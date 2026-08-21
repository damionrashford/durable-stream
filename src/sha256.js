/*
 * Streaming SHA-256.
 *
 * crypto.subtle only takes whole buffers — every method, including digest —
 * so hashing a payload through it means holding the payload. The algorithm has
 * no such limit: SHA-256 is Merkle-Damgard, absorbing 64-byte blocks into a
 * 256-bit state, so it streams by construction. This is that construction,
 * which costs a fixed 32 bytes of state regardless of payload size.
 *
 * The output is an ordinary SHA-256, so an upstream can verify it with any
 * standard tool. A tree of one-shot digests would also stream, but only this
 * program could check the result.
 */

const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

const rotr = (x, n) => ((x >>> n) | (x << (32 - n))) >>> 0;

const IV = [
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

/**
 * @param {{state: number[], length: number}|null} from
 *   A snapshot to continue from, as returned by `snapshot()`.
 */
export function sha256Stream(from = null) {
  const state = Int32Array.from(from?.state ?? IV);
  const buffer = new Uint8Array(64);
  const words = new Uint32Array(64);
  let buffered = 0;
  let length = from?.length ?? 0; // total bytes, for the length suffix

  const compress = (block, at) => {
    for (let i = 0; i < 16; i += 1) {
      words[i] =
        ((block[at + i * 4] << 24) |
          (block[at + i * 4 + 1] << 16) |
          (block[at + i * 4 + 2] << 8) |
          block[at + i * 4 + 3]) >>>
        0;
    }
    for (let i = 16; i < 64; i += 1) {
      const a = words[i - 15];
      const b = words[i - 2];
      const s0 = (rotr(a, 7) ^ rotr(a, 18) ^ (a >>> 3)) >>> 0;
      const s1 = (rotr(b, 17) ^ rotr(b, 19) ^ (b >>> 10)) >>> 0;
      words[i] = (words[i - 16] + s0 + words[i - 7] + s1) >>> 0;
    }

    let [h0, h1, h2, h3, h4, h5, h6, h7] = state;
    for (let i = 0; i < 64; i += 1) {
      const S1 = (rotr(h4, 6) ^ rotr(h4, 11) ^ rotr(h4, 25)) >>> 0;
      const ch = ((h4 & h5) ^ (~h4 & h6)) >>> 0;
      const t1 = (h7 + S1 + ch + K[i] + words[i]) >>> 0;
      const S0 = (rotr(h0, 2) ^ rotr(h0, 13) ^ rotr(h0, 22)) >>> 0;
      const maj = ((h0 & h1) ^ (h0 & h2) ^ (h1 & h2)) >>> 0;
      const t2 = (S0 + maj) >>> 0;
      h7 = h6; h6 = h5; h5 = h4; h4 = (h3 + t1) >>> 0;
      h3 = h2; h2 = h1; h1 = h0; h0 = (t1 + t2) >>> 0;
    }
    state[0] = (state[0] + h0) | 0; state[1] = (state[1] + h1) | 0;
    state[2] = (state[2] + h2) | 0; state[3] = (state[3] + h3) | 0;
    state[4] = (state[4] + h4) | 0; state[5] = (state[5] + h5) | 0;
    state[6] = (state[6] + h6) | 0; state[7] = (state[7] + h7) | 0;
  };

  return {
    update(chunk) {
      const bytes = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
      length += bytes.length;
      let at = 0;

      if (buffered) {
        const take = Math.min(64 - buffered, bytes.length);
        buffer.set(bytes.subarray(0, take), buffered);
        buffered += take;
        at = take;
        if (buffered < 64) return;
        compress(buffer, 0);
        buffered = 0;
      }
      // Whole blocks straight from the chunk; no copy.
      for (; at + 64 <= bytes.length; at += 64) compress(bytes, at);
      if (at < bytes.length) {
        buffer.set(bytes.subarray(at), 0);
        buffered = bytes.length - at;
      }
    },

    /** Applies the padding and length suffix, then renders the digest. */
    digest() {
      const bits = length * 8;
      const tail = new Uint8Array(buffered < 56 ? 64 : 128);
      tail.set(buffer.subarray(0, buffered));
      tail[buffered] = 0x80;
      // Length is 64-bit big-endian; the high half is written as a float-safe split.
      const view = new DataView(tail.buffer);
      view.setUint32(tail.length - 8, Math.floor(bits / 0x100000000));
      view.setUint32(tail.length - 4, bits >>> 0);
      for (let at = 0; at < tail.length; at += 64) compress(tail, at);

      let out = "";
      for (const word of state) out += ((word >>> 0) + 0x100000000).toString(16).slice(1);
      return out;
    },

    /**
     * Everything needed to continue this hash later, or null if we are mid-block.
     *
     * Merkle-Damgard holds all it knows in the 256-bit state plus a byte count,
     * so a hash in progress serialises to nine numbers — but only between
     * blocks. Mid-block there is an unabsorbed remainder as well, and rather
     * than serialise that too this refuses, because a caller that checkpoints on
     * a 64-byte boundary never sees the case. A 1 MiB block is 16384 of them.
     */
    snapshot() {
      return buffered ? null : { state: [...state], length };
    },

    get bytes() {
      return length;
    },
  };
}

/** Below this, buffering and using the native digest is worth it. */
const NATIVE_LIMIT = 8 * 1024 * 1024;

/**
 * SHA-256 over a stream, at native speed while that is affordable.
 *
 * The JS compression function runs around 43 MB/s against roughly 1800 MB/s for
 * crypto.subtle, so paying it for every payload would be wasteful when almost
 * all of them fit in memory comfortably. Chunks are held until they exceed the
 * limit; below it the whole payload goes to the native digest, and above it the
 * held chunks are replayed into the streaming implementation and memory returns
 * to constant. Either path produces the same standard digest.
 *
 * `resumable` gives that choice up and streams from the first byte. A digest
 * that has to survive a dropped connection needs `snapshot()`, and crypto.subtle
 * exposes no state at all — it only takes whole buffers and returns whole
 * digests, so the fast path is a dead end for anything that might be continued.
 */
export function hashingStream({ resumable = false, from = null } = {}) {
  let held = [];
  let size = 0;
  let streaming = resumable || from ? sha256Stream(from) : null;

  return {
    update(chunk) {
      const bytes = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
      if (streaming) return streaming.update(bytes);

      size += bytes.length;
      held.push(bytes);
      if (size <= NATIVE_LIMIT) return;

      // Too big to hold: replay what is buffered and keep going in constant space.
      streaming = sha256Stream();
      for (const part of held) streaming.update(part);
      held = [];
    },

    /** Null unless this hash was opened resumable; see sha256Stream.snapshot. */
    snapshot() {
      return streaming?.snapshot() ?? null;
    },

    async digest() {
      if (streaming) return { hash: streaming.digest(), via: "streaming" };
      const all = new Uint8Array(size);
      let at = 0;
      for (const part of held) {
        all.set(part, at);
        at += part.length;
      }
      held = [];
      const out = new Uint8Array(await crypto.subtle.digest("SHA-256", all));
      return {
        hash: [...out].map((b) => b.toString(16).padStart(2, "0")).join(""),
        via: "native",
      };
    },
  };
}
