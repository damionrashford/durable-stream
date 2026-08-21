/*
 * Binary payload store, backed by the Origin Private File System.
 *
 * createWritable() returns a FileSystemWritableFileStream, which is a
 * WritableStream, so an incoming body reaches disk with backpressure and a file
 * larger than memory costs nothing to receive. getFile() returns a File, whose
 * .stream() is lazy and whose slices are transferable to a page.
 *
 * The API here is the async one. createSyncAccessHandle() is
 * [Exposed=DedicatedWorker], so it is unreachable from a service worker.
 *
 * Two directories, because a payload has two identities:
 *
 *   staging/<key>    bytes arriving, named by whoever asked for them
 *   objects/<sha256> bytes at rest, named by what they are
 *
 * Content addressing is what makes the same payload announced under three keys
 * cost one copy. It also means an object cannot be named until its last byte has
 * been hashed, which is why arrival needs a name of its own — and that name has
 * to be the caller's key, because a resume must find its own prefix and nothing
 * else has been established about the bytes yet.
 */

import { hashingStream } from "./sha256.js";
import { BLOCK_SIZE, gzipMember, encodeTrailer, readTrailer, readBlocks } from "./blocks.js";

const OBJECTS = "objects";
const STAGING = "staging";

/** Pre-content-addressing layout. Removed on open; the log can re-announce. */
const LEGACY = "blobs";

/** Suffix marking a block-wise gzip container. See blocks.js. */
const SUFFIX = ".gzb";
const ENCODING = "gzip-blocks";

/**
 * Payloads live in their own storage bucket where one is available.
 *
 * A bucket carries its own eviction policy. The log stays in the default bucket
 * and asks to be persisted; payloads go here and deliberately do not. Under
 * quota pressure the browser sheds this bucket first, so the derived data goes
 * and the source of truth stays — which retention alone cannot arrange, because
 * retention runs on our schedule and eviction runs on the browser's.
 *
 * "relaxed" durability suits payloads: they are announced by the log and any
 * loss shows up as a missing key, which is already a handled case.
 */
const BUCKET = "payloads";

async function rootDirectory() {
  if (navigator.storageBuckets) {
    try {
      const bucket = await navigator.storageBuckets.open(BUCKET, { durability: "relaxed" });
      return bucket.getDirectory();
    } catch {
      // Unsupported or refused; the default bucket still works.
    }
  }
  return navigator.storage.getDirectory();
}

/** Types where gzip pays for itself. Media and archives are already compressed. */
const COMPRESSIBLE =
  /^(text\/|application\/(json|xml|javascript|x-ndjson|wasm)|image\/svg\+xml)/;

/** getFileHandle throws NotFoundError rather than returning null. */
async function fileHandle(dir, name) {
  try {
    return await dir.getFileHandle(name);
  } catch {
    return null;
  }
}

export async function openBlobStore() {
  const root = await rootDirectory();
  await root.removeEntry(LEGACY, { recursive: true }).catch(() => {});

  let objects = await root.getDirectoryHandle(OBJECTS, { create: true });
  let staging = await root.getDirectoryHandle(STAGING, { create: true });

  /**
   * Give a finished staging file its content address.
   *
   * move() is the only way to do this without reading the payload back — it is
   * a directory-entry change, so a gigabyte costs what a byte costs. It is
   * neither universally implemented nor stable, hence the copy behind it, which
   * is correct and merely expensive.
   */
  async function promote(handle, name) {
    if (typeof handle.move === "function") {
      try {
        await handle.move(objects, name);
        return;
      } catch {
        // fall through to the portable path
      }
    }
    const source = await handle.getFile();
    const target = await objects.getFileHandle(name, { create: true });
    await source.stream().pipeTo(await target.createWritable());
    await staging.removeEntry(handle.name).catch(() => {});
  }

  const api = {
    /**
     * Consume a plaintext stream, store it, and return its content address.
     *
     * The stream is read once by hand rather than tee()'d, because hashing and
     * compressing both need every byte at the same rate and a tee whose branches
     * are consumed at different rates queues the difference without bound. One
     * reader, one megabyte held, and both consumers see each block in turn.
     *
     * Bytes reach disk a whole block at a time and never in part. That is what
     * `resume` needs from a failed attempt: a file whose length is a block
     * boundary, and — because a block is a multiple of 64 — a SHA-256 state that
     * can be picked up exactly where it stopped. `progress` is filled in as
     * blocks land, and is what a later call passes back as `resume`.
     */
    async put(key, stream, mime = "", { resume: from = null, progress = null, expect = null } = {}) {
      // Progress with no complete block is not a resume point — it is a start.
      // Honouring it anyway would adopt its encoding, and an empty record's
      // encoding is null, which would store a compressible payload raw.
      let resume = from?.blocks?.length ? from : null;

      // The log says where the last attempt stopped; only the file can say
      // whether those bytes are still here. A prefix can be swept between
      // attempts, and seeking past the end of a file that is shorter than
      // claimed pads it with NULs — which the hash would not notice, because it
      // continues from the saved state rather than from what is on disk. The
      // result would be a zero-prefixed file promoted under a valid content
      // address. Longer than claimed is fine: that is a short final block from
      // a clean early close, and the seek truncates it away.
      if (resume) {
        const staged = await fileHandle(staging, key);
        const have = staged ? (await staged.getFile()).size : 0;
        if (have < resume.blocks.reduce((n, v) => n + v, 0)) resume = null;
      }

      // A resumed write must use whatever the first attempt chose; a file cannot
      // be half raw and half compressed.
      const encoding = resume
        ? (resume.encoding ?? null)
        : COMPRESSIBLE.test(mime) && "CompressionStream" in globalThis
          ? ENCODING
          : null;

      const lengths = resume?.blocks ? [...resume.blocks] : [];
      const resuming = lengths.length > 0;
      // Only the final block is ever short, so complete blocks account for
      // exactly their nominal plaintext regardless of what the file holds.
      let plain = lengths.length * BLOCK_SIZE;
      let written = lengths.reduce((n, v) => n + v, 0);

      // Checkpointing costs the software SHA-256 rather than crypto.subtle,
      // which only whole-buffer digests can use. Callers that will never resume
      // pass no progress object and keep the fast path.
      const hasher = hashingStream({
        resumable: Boolean(progress || resume),
        from: resume?.hash ?? null,
      });

      const handle = await staging.getFileHandle(key, { create: true });
      const writable = await handle.createWritable({ keepExistingData: resuming });
      const writer = writable.getWriter();
      if (resuming) {
        // A previous attempt may have landed a short final block past the last
        // checkpoint. Cut back to the checkpoint before seeking to it, or those
        // bytes survive underneath everything written from here on.
        await writer.write({ type: "truncate", size: written });
        await writer.write({ type: "seek", position: written });
      }

      const emit = async (block) => {
        hasher.update(block);
        const member = encoding ? await gzipMember(block) : block;
        await writer.write(member);

        lengths.push(member.byteLength);
        written += member.byteLength;
        plain += block.byteLength;

        // Only a full block leaves the hash on a 64-byte boundary, and only a
        // full block can be followed by more — so a short one is never a resume
        // point and never needs to be one.
        if (!progress || block.byteLength !== BLOCK_SIZE) return;
        const hash = hasher.snapshot();
        // `plain` is what a resume asks the server for; `offset` is where on
        // disk to continue. Compression makes those two different numbers, and
        // the caller should not have to know the block size to derive either.
        if (hash) {
          Object.assign(progress, { plain, offset: written, blocks: [...lengths], hash, encoding });
        }
      };

      const reader = stream.getReader();
      const pending = new Uint8Array(BLOCK_SIZE);
      let held = 0;

      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;

          let chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
          while (chunk.length) {
            const take = Math.min(BLOCK_SIZE - held, chunk.length);
            pending.set(chunk.subarray(0, take), held);
            held += take;
            chunk = chunk.subarray(take);
            if (held < BLOCK_SIZE) continue;
            // Copied out: the sink may hold the buffer past write(), and the
            // next block reuses it.
            await emit(pending.slice(0, BLOCK_SIZE));
            held = 0;
          }
        }
        if (held) await emit(pending.slice(0, held));
        if (encoding) await writer.write(encodeTrailer(lengths, BLOCK_SIZE, held || BLOCK_SIZE));
        await writer.close();
      } catch (err) {
        // Nothing lands until close(), so aborting here would throw away every
        // complete block along with the broken one. Closing keeps them, which is
        // the whole resume story — a transfer that died at 60% costs 40% to
        // finish rather than 100%.
        await writer.close().catch(() => {});
        reader.cancel().catch(() => {});
        // With no progress object there is no resume path, so a prefix is only
        // garbage, and leaving it would mean a failed write grew the store.
        if (!progress) await staging.removeEntry(key).catch(() => {});
        throw err;
      }

      const { hash } = await hasher.digest();

      // The announced length is the only independent statement of what the bytes
      // should be. Checking it here rather than after promotion is what keeps a
      // truncated transfer from being content-addressed as though it were whole.
      if (expect !== null && expect !== undefined && plain !== expect) {
        throw new Error(`payload ${key}: expected ${expect} bytes, stored ${plain}`);
      }

      // Same content already at rest — under either encoding, since the address
      // is over the plaintext. Adopt what is there and drop what we just wrote;
      // the caller records the encoding it got rather than the one it chose.
      const existing = await api.resolve(hash);
      if (existing) {
        await staging.removeEntry(key).catch(() => {});
        return {
          sha256: hash,
          size: existing.size,
          stored: existing.file.size,
          encoding: existing.encoding,
          deduped: true,
        };
      }

      const name = encoding ? hash + SUFFIX : hash;
      await promote(handle, name);
      return {
        sha256: hash,
        size: plain,
        stored: (await (await objects.getFileHandle(name)).getFile()).size,
        encoding,
        deduped: false,
      };
    },

    /**
     * Locate an object by content address.
     *
     * Both encodings are probed because dedupe is over plaintext: the same bytes
     * may already be at rest in whichever form the first arrival chose. A
     * container whose trailer will not parse is treated as absent — it is a
     * write that never finished, and the log can announce the payload again.
     */
    async resolve(hash) {
      if (!hash) return null;

      for (const [name, encoding] of [
        [hash + SUFFIX, ENCODING],
        [hash, null],
      ]) {
        const handle = await fileHandle(objects, name);
        if (!handle) continue;

        const file = await handle.getFile();
        const index = encoding ? await readTrailer(file) : null;
        if (encoding && !index) continue;

        /**
         * Plaintext bytes [start, end], inclusive.
         *
         * Compressed payloads are seekable: a range decodes the blocks covering
         * it and no others, so a <video> element seeks in one block's worth of
         * work rather than a decode from zero. Range serving used to be offered
         * only for uncompressed files, which is most of why anything large had
         * to stay uncompressed.
         */
        const read = (start, end) =>
          index
            ? readBlocks(file, index, start, end)
            : file.slice(start, end + 1).stream();

        return { file, encoding, index, size: index ? index.size : file.size, read };
      }
      return null;
    },

    /** The whole payload as plaintext, whatever it is stored as. */
    async get(hash) {
      const found = await api.resolve(hash);
      return found ? found.read(0, found.size - 1) : null;
    },

    /** Abandon a partial transfer. */
    async discard(key) {
      await staging.removeEntry(key).catch(() => {});
    },

    /** One address, either encoding. Only one can exist, but neither is known. */
    async delete(hash) {
      await Promise.all([
        objects.removeEntry(hash + SUFFIX).catch(() => {}),
        objects.removeEntry(hash).catch(() => {}),
      ]);
    },

    /**
     * Address and size for every object.
     *
     * entries() yields [name, handle] pairs, so the directory is walked once —
     * keys() would mean a second getFileHandle() per file. The walk is serial
     * because the iterator is, but the getFile() calls it feeds are not: awaited
     * in the loop they would serialise a round trip per object, and this runs on
     * every maintenance pass over every payload we hold.
     */
    async list() {
      const pending = [];
      for await (const [name, handle] of objects.entries()) {
        if (handle.kind !== "file") continue;
        const hash = name.endsWith(SUFFIX) ? name.slice(0, -SUFFIX.length) : name;
        pending.push(handle.getFile().then((file) => ({ hash, stored: file.size })));
      }
      return Promise.all(pending);
    },

    /**
     * Drop what the log no longer references. Called after the log trims.
     *
     * Objects are refcounted by reachability rather than a counter: an address
     * survives while any live event still names it, so the last key referring to
     * a shared payload is what releases it, and a counter that could drift out
     * of step with the log never exists.
     *
     * Staging is swept by key on the same principle. A partial transfer whose
     * announcement has been trimmed away can never be resumed, because nothing
     * is left to say what the rest of it is.
     */
    async sweep(liveHashes, liveKeys = []) {
      const keepHashes = new Set(liveHashes);
      const keepKeys = new Set(liveKeys);
      let removed = 0;

      for (const { hash } of await api.list()) {
        if (keepHashes.has(hash)) continue;
        await api.delete(hash);
        removed += 1;
      }
      for await (const name of staging.keys()) {
        if (keepKeys.has(name)) continue;
        await staging.removeEntry(name).catch(() => {});
        removed += 1;
      }
      return removed;
    },

    /**
     * Enforce a byte budget, oldest first.
     *
     * A count-based retention says nothing about size, so a run of large
     * payloads reaches quota while the log is still well inside its limit. This
     * is the other half: hold total bytes under a budget, and drop from the
     * front of the log's own order rather than by size, so eviction stays
     * predictable instead of preferring whatever happens to be biggest.
     *
     * `oldestFirst` is ordered by each object's *newest* reference, which is not
     * the same as its oldest once payloads are shared. An object first seen long
     * ago and announced again this minute is live data wearing an old timestamp,
     * and evicting it would break the recent key as well as the ancient one.
     */
    async evictTo(budget, oldestFirst) {
      const sizes = new Map();
      for (const { hash, stored } of await api.list()) {
        sizes.set(hash, (sizes.get(hash) ?? 0) + stored);
      }
      let total = [...sizes.values()].reduce((n, v) => n + v, 0);
      const evicted = [];

      for (const hash of oldestFirst) {
        if (total <= budget) break;
        const size = sizes.get(hash);
        if (size === undefined) continue;
        await api.delete(hash);
        total -= size;
        evicted.push(hash);
      }
      return { evicted, total };
    },

    /**
     * Wipe everything.
     *
     * FileSystemHandle.remove({recursive:true}) does it in one call, but it is
     * experimental and non-standard, so it is a fast path with the portable
     * per-entry loop behind it. Removing a directory invalidates our handle,
     * hence the re-acquire.
     */
    async clear() {
      for (const name of [OBJECTS, STAGING]) {
        try {
          await root.removeEntry(name, { recursive: true });
        } catch {
          const dir = await root.getDirectoryHandle(name, { create: true });
          for await (const entry of dir.keys()) {
            await dir.removeEntry(entry, { recursive: true }).catch(() => {});
          }
        }
      }
      objects = await root.getDirectoryHandle(OBJECTS, { create: true });
      staging = await root.getDirectoryHandle(STAGING, { create: true });
    },
  };

  return api;
}

/**
 * OPFS, IndexedDB, and Cache Storage share one origin quota, so estimate() is a
 * combined number.
 *
 * `persisted` is the part that matters: a default bucket is "best-effort" and the
 * browser may clear it under pressure. Since the log is the source of truth,
 * best-effort means the source of truth is deletable.
 */
export async function storageStatus() {
  const { usage = 0, quota = 0, usageDetails: breakdown = null } =
    (await navigator.storage?.estimate?.()) ?? {};
  const persisted = (await navigator.storage?.persisted?.()) ?? false;

  // navigator.storage.estimate() covers the whole origin, other buckets
  // included, so the payload figure is a breakdown of `usage` and not an
  // addition to it. Separate buckets divide one allowance; they do not grant a
  // second one, and neither does spreading across OPFS, IndexedDB and Cache.
  let payloads = null;
  if (navigator.storageBuckets) {
    try {
      const bucket = await navigator.storageBuckets.open(BUCKET, { durability: "relaxed" });
      payloads = (await bucket.estimate()).usage ?? null;
    } catch {
      // No separate bucket; payload bytes are still inside `usage`.
    }
  }

  return { usage, quota, persisted, payloads, breakdown, pressure: quota ? usage / quota : 0 };
}
