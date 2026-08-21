/*
 * Block-wise gzip: the container that lets a compressed payload still resume,
 * and still seek.
 *
 * A gzip stream is one member — header, deflate stream, then a CRC and length
 * over everything before it. Appending to it requires the compressor state that
 * produced it, so a transfer that dies takes that state with it and the prefix
 * on disk decodes to nothing. That is the whole reason compression and
 * resumability were mutually exclusive, and why anything big enough to be worth
 * resuming was stored raw.
 *
 * Compressing each fixed-size block of plaintext into its own complete member
 * removes the dependency. A member is self-contained, so a file of members can
 * be extended at a member boundary by a process that knows nothing about the
 * ones already there — which is exactly what a resume is.
 *
 * Concatenated members are a valid gzip file to gzip(1), which finishes one
 * member and starts the next. Whether DecompressionStream("gzip") does the same
 * is not something to rely on: it is runtime-dependent — Bun's spans members,
 * Chromium's stops at the first and treats the rest as trailing junk — and
 * nothing in the Compression Streams spec promises either. So the boundaries are
 * recorded and each member is fed through its own DecompressionStream, which
 * works whichever way the runtime leans.
 *
 * Recording them is what also makes the file seekable, so the trailer below
 * would earn its place even on a runtime that decoded the whole concatenation
 * happily: a plaintext range touches only the blocks covering it.
 *
 * Blocks are a fixed plaintext size, so the trailer stores compressed lengths
 * only; where block i starts in the plaintext is i * blockSize by construction.
 */

/** "GZB1". Distinguishes our container from a plain gzip file of the same name. */
const MAGIC = 0x475a4231;

/** blockSize, lastBlockSize, count, magic — four uint32s after the length table. */
const FOOTER = 16;

/**
 * Plaintext bytes per block.
 *
 * Three things are traded here. Larger blocks compress better, because deflate's
 * window restarts at every member. Smaller blocks cost less to re-transfer after
 * a drop and less to decode when serving a short range. And the size must be a
 * multiple of 64 so that a block boundary is also a SHA-256 block boundary,
 * which is what makes the digest resumable — see sha256Stream.snapshot.
 */
export const BLOCK_SIZE = 1024 * 1024;

/**
 * One plaintext block, compressed into one complete gzip member.
 *
 * The member is collected rather than streamed because its length is what the
 * trailer records, and a length is only known once the member ends. Bounded by
 * BLOCK_SIZE, so this is the one place a payload is held in memory and it is
 * held a megabyte at a time regardless of how large the payload is.
 */
export async function gzipMember(bytes) {
  const gzip = new CompressionStream("gzip");
  const collected = new Response(gzip.readable).arrayBuffer();
  const writer = gzip.writable.getWriter();
  await writer.write(bytes);
  await writer.close();
  return new Uint8Array(await collected);
}

/**
 * Decode one member back to plaintext.
 *
 * The member is read whole and handed to the decoder rather than piped in from
 * the slice's own stream. Both end up holding a block, since the caller wants
 * the plaintext as a unit — but piping a slice means trusting the slice's end
 * bound to survive into its stream, and Bun's does not (Blob.slice().stream()
 * runs to the end of the blob there). Feeding the bytes leaves nothing to get
 * wrong, and an over-read here would silently decode the *following* blocks on
 * any runtime whose gzip decoder spans members.
 */
export async function gunzipMember(blob) {
  const gunzip = new DecompressionStream("gzip");
  const collected = new Response(gunzip.readable).arrayBuffer();
  const writer = gunzip.writable.getWriter();
  await writer.write(new Uint8Array(await blob.arrayBuffer()));
  await writer.close();
  return new Uint8Array(await collected);
}

/**
 * The trailer, written once when the object is complete.
 *
 * At the end rather than the front because the length table does not exist until
 * the last block has been compressed, and a header would mean either seeking
 * back over a file we may not have room to rewrite or buffering the whole
 * payload to learn its shape first.
 */
export function encodeTrailer(lengths, blockSize, lastBlockSize) {
  const out = new Uint8Array(lengths.length * 4 + FOOTER);
  const view = new DataView(out.buffer);
  let at = 0;
  for (const length of lengths) {
    view.setUint32(at, length);
    at += 4;
  }
  view.setUint32(at, blockSize);
  view.setUint32(at + 4, lastBlockSize);
  view.setUint32(at + 8, lengths.length);
  view.setUint32(at + 12, MAGIC);
  return out;
}

/**
 * Read the trailer off a File. Null when this is not one of our containers.
 *
 * Two small reads at the tail: File.slice is lazy, so neither touches the
 * payload itself. Returns cumulative member offsets — one more than the block
 * count, so offsets[i]..offsets[i+1] delimits block i without a special case at
 * the end.
 */
export async function readTrailer(file) {
  if (!file || file.size < FOOTER) return null;

  const foot = new DataView(await file.slice(file.size - FOOTER).arrayBuffer());
  if (foot.getUint32(12) !== MAGIC) return null;

  const blockSize = foot.getUint32(0);
  const lastBlockSize = foot.getUint32(4);
  const count = foot.getUint32(8);

  const tableAt = file.size - FOOTER - count * 4;
  if (tableAt < 0) return null;

  const table = new DataView(await file.slice(tableAt, file.size - FOOTER).arrayBuffer());
  const offsets = new Array(count + 1);
  offsets[0] = 0;
  for (let i = 0; i < count; i += 1) offsets[i + 1] = offsets[i] + table.getUint32(i * 4);

  // A torn file would have offsets running past where the table starts.
  if (offsets[count] !== tableAt) return null;

  return {
    offsets,
    blockSize,
    count,
    size: count ? (count - 1) * blockSize + lastBlockSize : 0,
  };
}

/**
 * A ReadableStream of plaintext bytes [start, end], inclusive.
 *
 * Pull-driven, and deliberately on the default queuing strategy: a high-water
 * mark of one chunk means desiredSize drops to zero after a single enqueue and
 * pull() is not called again until the consumer has taken it. So decoding a
 * whole file costs one block of memory rather than the file, and a consumer
 * that stops reading stops the decoder rather than filling a queue behind it.
 *
 * The first and last blocks are trimmed to the requested range, which is what
 * makes a Range request cost the blocks it covers instead of a decode from zero.
 */
export function readBlocks(file, { offsets, blockSize }, start, end) {
  let block = Math.floor(start / blockSize);
  const lastBlock = Math.floor(end / blockSize);

  return new ReadableStream({
    async pull(controller) {
      if (block > lastBlock) return controller.close();

      const plain = await gunzipMember(file.slice(offsets[block], offsets[block + 1]));
      const base = block * blockSize;
      const head = Math.max(0, start - base);
      const tail = Math.min(plain.length, end - base + 1);

      block += 1;
      if (tail > head) controller.enqueue(plain.subarray(head, tail));
    },
  });
}
