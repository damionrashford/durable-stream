/*
 * Binary payload store, backed by the Origin Private File System.
 *
 * Why OPFS and not IndexedDB for bytes: createWritable() returns a
 * FileSystemWritableFileStream, which IS a WritableStream. An incoming body pipes
 * straight to disk with real backpressure and never lands in memory — even for a
 * file larger than RAM. Reading back, getFile() returns a File (a Blob), whose
 * .stream() is native and lazy, and which is transferable to a page.
 *
 * Not used here: createSyncAccessHandle(). It is [Exposed=DedicatedWorker] in the
 * spec, so it does not exist in a service worker. The async API is what we get.
 */

const DIR = "blobs";

/** Types where gzip pays for itself. Media and archives are already compressed. */
const COMPRESSIBLE =
  /^(text\/|application\/(json|xml|javascript|x-ndjson|wasm)|image\/svg\+xml)/;

async function dir() {
  const root = await navigator.storage.getDirectory();
  return root.getDirectoryHandle(DIR, { create: true });
}

export async function openBlobStore() {
  const store = await dir();

  const api = {
    /**
     * Pipe a ReadableStream to disk. Returns what was written and how.
     *
     * Compression happens in the pipe, so it costs no extra memory and the
     * uncompressed bytes never exist anywhere at once. The encoding is returned
     * rather than inferred later — get() has to know how to reverse it.
     */
    async put(key, stream, mime = "") {
      const encoding = COMPRESSIBLE.test(mime) && "CompressionStream" in globalThis ? "gzip" : null;

      // Count the bytes going in. Once compressed, the file size is the stored
      // size, not the payload's real length — and the real length is what a
      // caller means by "how big is this".
      let size = 0;
      const measured = stream.pipeThrough(
        new TransformStream({
          transform(chunk, controller) {
            size += chunk.byteLength ?? chunk.length ?? 0;
            controller.enqueue(chunk);
          },
        }),
      );
      const body = encoding ? measured.pipeThrough(new CompressionStream(encoding)) : measured;

      const file = await store.getFileHandle(key, { create: true });
      try {
        // pipeTo closes the destination on completion; createWritable() only
        // commits on close, so this is also what makes the write durable.
        await body.pipeTo(await file.createWritable());
      } catch (err) {
        // A half-written file is worse than none: the log would point at bytes
        // that do not decode.
        await api.delete(key);
        throw err;
      }
      return { size, stored: (await file.getFile()).size, encoding };
    },

    /**
     * A ReadableStream of the original bytes, decompressed if needed.
     *
     * Returns a stream rather than a File because a gzip'd file has to be piped
     * to be read, and callers should not care which case they got.
     */
    async get(key, { encoding = null } = {}) {
      let file;
      try {
        file = await (await store.getFileHandle(key)).getFile();
      } catch {
        return null; // NotFoundError
      }
      const stream = file.stream();
      return encoding === "gzip" ? stream.pipeThrough(new DecompressionStream("gzip")) : stream;
    },

    async delete(key) {
      await store.removeEntry(key).catch(() => {});
    },

    async keys() {
      const out = [];
      for await (const name of store.keys()) out.push(name);
      return out;
    },

    /** Drop payloads the log no longer references. Called after the log trims. */
    async sweep(live) {
      let removed = 0;
      for (const key of await api.keys()) {
        if (live.has(key)) continue;
        await api.delete(key);
        removed += 1;
      }
      return removed;
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
  const { usage = 0, quota = 0 } = (await navigator.storage?.estimate?.()) ?? {};
  const persisted = (await navigator.storage?.persisted?.()) ?? false;
  return { usage, quota, persisted, pressure: quota ? usage / quota : 0 };
}
