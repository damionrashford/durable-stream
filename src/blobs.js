/*
 * Binary payload store, backed by the Origin Private File System.
 *
 * Why OPFS and not IndexedDB for bytes: createWritable() returns a
 * FileSystemWritableFileStream, which IS a WritableStream. That means an incoming
 * request body pipes straight to disk with real backpressure and never lands in
 * memory — even for a file larger than RAM. Reading back, getFile() returns a File
 * (a Blob), whose .stream() is native and lazy, and which is transferable to a page.
 *
 * Not used here: createSyncAccessHandle(). It is [Exposed=DedicatedWorker] in the
 * spec, so it does not exist in a service worker. The async API above is what we get.
 */

const DIR = "blobs";

async function dir() {
  const root = await navigator.storage.getDirectory();
  return root.getDirectoryHandle(DIR, { create: true });
}

export async function openBlobStore() {
  const store = await dir();

  return {
    /** Pipe a ReadableStream to disk. Returns the byte count actually written. */
    async put(key, stream) {
      const file = await store.getFileHandle(key, { create: true });
      // pipeTo closes the destination on completion; createWritable() only
      // commits on close, so this is also what makes the write durable.
      await stream.pipeTo(await file.createWritable());
      return (await file.getFile()).size;
    },

    /** A File (Blob). .stream() is native; the File itself is transferable. */
    async get(key) {
      try {
        return await (await store.getFileHandle(key)).getFile();
      } catch {
        return null; // NotFoundError
      }
    },

    async delete(key) {
      await store.removeEntry(key).catch(() => {});
    },

    async keys() {
      const out = [];
      for await (const name of store.keys()) out.push(name);
      return out;
    },

    async clear() {
      for (const name of await this.keys()) await this.delete(name);
    },
  };
}

/**
 * OPFS, IndexedDB, and Cache Storage share one origin quota, so estimate() is a
 * combined number. persist() asks the browser not to evict us under pressure.
 */
export async function storageStatus() {
  const { usage = 0, quota = 0 } = (await navigator.storage.estimate?.()) ?? {};
  const persisted = (await navigator.storage.persisted?.()) ?? false;
  return { usage, quota, persisted };
}
