/*
 * Binary payload store, backed by the Origin Private File System.
 *
 * createWritable() returns a FileSystemWritableFileStream, which is a
 * WritableStream, so an incoming body pipes straight to disk with backpressure
 * and a file larger than memory costs nothing to receive. getFile() returns a
 * File, whose .stream() is lazy and whose slices are transferable to a page.
 *
 * The API here is the async one. createSyncAccessHandle() is
 * [Exposed=DedicatedWorker], so it is unreachable from a service worker.
 */

const DIR = "blobs";

/** Types where gzip pays for itself. Media and archives are already compressed. */
const COMPRESSIBLE =
  /^(text\/|application\/(json|xml|javascript|x-ndjson|wasm)|image\/svg\+xml)/;

async function openDir() {
  const root = await navigator.storage.getDirectory();
  return root.getDirectoryHandle(DIR, { create: true });
}

export async function openBlobStore() {
  let store = await openDir();

  const api = {
    /**
     * Pipe a ReadableStream to disk, optionally continuing a partial file.
     *
     * `offset > 0` resumes: the file is opened with keepExistingData (the default
     * truncates), the cursor is seeked past what is already there, and the new
     * bytes are appended. That is what makes a transfer that died at 60% cost
     * 40% to finish instead of 100%.
     *
     * Compression and resumability are mutually exclusive, so the caller says
     * up front which it wants and is told which it got.
     */
    async put(key, stream, mime = "", { offset = 0, resumable = false } = {}) {
      const resuming = offset > 0;
      // gzip is stateful: bytes appended to a stream whose compressor state is
      // gone do not decode. So anything that might have to resume is stored raw,
      // decided up front — not only once a resume is actually attempted.
      const encoding =
        !resuming && !resumable && COMPRESSIBLE.test(mime) && "CompressionStream" in globalThis
          ? "gzip"
          : null;

      // Count the bytes going in. Once compressed, the file size is the stored
      // size, not the payload's real length — and the real length is what a
      // caller means by "how big is this".
      let received = 0;
      const measured = stream.pipeThrough(
        new TransformStream({
          transform(chunk, controller) {
            received += chunk.byteLength ?? chunk.length ?? 0;
            controller.enqueue(chunk);
          },
        }),
      );
      const body = encoding ? measured.pipeThrough(new CompressionStream(encoding)) : measured;

      const file = await store.getFileHandle(key, { create: true });
      const writable = await file.createWritable({ keepExistingData: resuming });
      if (resuming) await writable.write({ type: "seek", position: offset });

      // Nothing reaches disk until close(); a failed pipeTo leaves the previous
      // contents intact, which is exactly what a resume needs.
      await body.pipeTo(writable);

      return { size: offset + received, stored: (await file.getFile()).size, encoding };
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

    /**
     * The File itself, for callers that need to slice it.
     *
     * Only meaningful for uncompressed payloads: a byte range of a gzip'd file
     * is not a byte range of its contents, so range serving is restricted to
     * those — which, by the resumable rule, are exactly the large ones.
     */
    async file(key) {
      try {
        return await (await store.getFileHandle(key)).getFile();
      } catch {
        return null;
      }
    },

    /** Bytes already on disk for this key. The resume point. */
    async sizeOf(key) {
      try {
        return (await (await store.getFileHandle(key)).getFile()).size;
      } catch {
        return 0;
      }
    },

    async delete(key) {
      await store.removeEntry(key).catch(() => {});
    },

    /**
     * Name and size for everything stored, in one pass.
     *
     * entries() yields [name, handle] pairs, so this costs one directory
     * iteration — keys() would mean a second getFileHandle() per file.
     */
    async list() {
      const out = [];
      for await (const [name, handle] of store.entries()) {
        if (handle.kind !== "file") continue;
        out.push({ key: name, stored: (await handle.getFile()).size });
      }
      return out;
    },

    /** Drop payloads the log no longer references. Called after the log trims. */
    async sweep(live) {
      let removed = 0;
      for (const { key } of await api.list()) {
        if (live.has(key)) continue;
        await api.delete(key);
        removed += 1;
      }
      return removed;
    },

    /**
     * Wipe everything.
     *
     * FileSystemHandle.remove({recursive:true}) does it in one call, but it is
     * experimental and non-standard, so it is a fast path with the portable
     * per-entry loop behind it. Removing the directory invalidates our handle,
     * hence the re-acquire.
     */
    async clear() {
      if (typeof store.remove === "function") {
        try {
          await store.remove({ recursive: true });
          store = await openDir();
          return;
        } catch {
          // fall through to the portable path
        }
      }
      for await (const name of store.keys()) await store.removeEntry(name, { recursive: true });
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
