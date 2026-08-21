/*
 * The append-only event log, backed by IndexedDB.
 *
 * This is the only durable layer. A service worker is terminated when idle and
 * its module state is not persisted across restarts — so anything held in a
 * variable is gone. Connections are disposable; the log is the source of truth.
 *
 * Event ids are the IndexedDB autoIncrement key: monotonic, gap-free, and exactly
 * what SSE's Last-Event-ID needs to resume without loss.
 */

const DB_NAME = "stream-log";
const DB_VERSION = 1;
const EVENTS = "events";
const META = "blob-meta";

function open() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(EVENTS)) {
        db.createObjectStore(EVENTS, { keyPath: "id", autoIncrement: true });
      }
      if (!db.objectStoreNames.contains(META)) {
        db.createObjectStore(META, { keyPath: "key" });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

const done = (tx) =>
  new Promise((resolve, reject) => {
    tx.oncomplete = resolve;
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });

const request = (req) =>
  new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });

export async function openLog() {
  const db = await open();

  // Live subscribers. In-memory on purpose: this set only has to outlive the
  // current worker instance, because reconnecting clients replay from the log.
  const subscribers = new Set();

  return {
    /** Append an event. Returns its id — the cursor clients resume from. */
    async append({ type, data }) {
      const tx = db.transaction(EVENTS, "readwrite");
      const id = await request(tx.objectStore(EVENTS).add({ type, data, at: Date.now() }));
      await done(tx);

      const event = { id, type, data };
      for (const fn of subscribers) {
        try {
          fn(event);
        } catch {
          subscribers.delete(fn);
        }
      }
      return id;
    },

    /**
     * Every event with id > after, in order. This is the replay path.
     *
     * Paged rather than cursor-based on purpose: an IndexedDB transaction
     * auto-commits once the microtask queue drains with no pending request, and
     * an async generator suspended at `yield` does exactly that. Each batch gets
     * its own short-lived transaction instead.
     */
    async *since(after = 0, batch = 200) {
      let cursor = after;
      while (true) {
        const tx = db.transaction(EVENTS, "readonly");
        // Exclusive lower bound: the client already has `cursor`.
        const range = IDBKeyRange.lowerBound(cursor, true);
        const rows = await request(tx.objectStore(EVENTS).getAll(range, batch));
        if (!rows.length) return;
        for (const row of rows) yield row;
        cursor = rows.at(-1).id;
        if (rows.length < batch) return;
      }
    },

    /** Highest id currently in the log. */
    async head() {
      const tx = db.transaction(EVENTS, "readonly");
      const cursor = await request(tx.objectStore(EVENTS).openCursor(null, "prev"));
      return cursor ? cursor.value.id : 0;
    },

    subscribe(fn) {
      subscribers.add(fn);
      return () => subscribers.delete(fn);
    },

    /**
     * Blob metadata only — the bytes live in OPFS. There is no transaction
     * spanning IndexedDB and OPFS, so the log is authoritative: a payload counts
     * as real once its `blob` event is committed here, never before.
     */
    async putMeta(key, meta) {
      const tx = db.transaction(META, "readwrite");
      tx.objectStore(META).put({ key, ...meta });
      await done(tx);
    },

    async getMeta(key) {
      const tx = db.transaction(META, "readonly");
      return request(tx.objectStore(META).get(key));
    },

    async clear() {
      const tx = db.transaction([EVENTS, META], "readwrite");
      tx.objectStore(EVENTS).clear();
      tx.objectStore(META).clear();
      await done(tx);
    },
  };
}
