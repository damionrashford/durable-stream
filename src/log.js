/*
 * The append-only event log, backed by IndexedDB.
 *
 * This is the only durable layer. A service worker is terminated when idle and
 * its module state is not persisted across restarts — so anything held in a
 * variable is gone. Connections are disposable; the log is the source of truth.
 *
 * Event ids are the IndexedDB autoIncrement key: monotonic, gap-free, and exactly
 * what a resume cursor needs.
 */

const DB_NAME = "stream-log";
const DB_VERSION = 2;
const EVENTS = "events";
const META = "blob-meta";
const OUTBOX = "outbox";

/** Events kept before the tail is trimmed. The log is not infinite storage. */
const RETENTION = 5000;
/** Trim to this when the origin is out of quota, not merely over retention. */
const PANIC_RETENTION = 500;

const FLOOR = "__floor";

function open() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(EVENTS)) {
        db.createObjectStore(EVENTS, { keyPath: "id", autoIncrement: true });
      }
      if (!db.objectStoreNames.contains(META)) db.createObjectStore(META, { keyPath: "key" });
      if (!db.objectStoreNames.contains(OUTBOX)) {
        db.createObjectStore(OUTBOX, { keyPath: "id", autoIncrement: true });
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

const isQuota = (err) =>
  err?.name === "QuotaExceededError" || err?.inner?.name === "QuotaExceededError";

export async function openLog() {
  const db = await open();

  // Live subscribers within this worker instance. In-memory on purpose: it only
  // has to outlive the current instance, because reconnecting clients replay.
  const subscribers = new Set();

  // Cross-context notification, independent of the service worker's lifetime.
  // Storage-partitioned rather than strictly origin-scoped — an embedded iframe
  // under a different top-level site is a different channel.
  const channel = new BroadcastChannel("stream-log");

  const api = {
    /** Append an event. Returns its id — the cursor clients resume from. */
    async append(entry, { retry = true } = {}) {
      let id;
      try {
        const tx = db.transaction(EVENTS, "readwrite");
        id = await request(
          tx.objectStore(EVENTS).add({ type: entry.type, data: entry.data ?? null, at: Date.now() }),
        );
        await done(tx);
      } catch (err) {
        // Out of space. Trim hard, then try once more; if it still fails the
        // caller has to deal with it rather than us silently dropping data.
        if (!retry || !isQuota(err)) throw err;
        await api.trim(PANIC_RETENTION);
        return api.append(entry, { retry: false });
      }

      const event = { id, type: entry.type, data: entry.data ?? null };
      for (const fn of subscribers) {
        try {
          fn(event);
        } catch {
          subscribers.delete(fn);
        }
      }
      // Tabs whose stream is dead still learn the log moved.
      channel.postMessage({ type: "append", id });

      if (id % 256 === 0) api.trim().catch(() => {});
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
        const range = IDBKeyRange.lowerBound(cursor, true);
        const rows = await request(tx.objectStore(EVENTS).getAll(range, batch));
        if (!rows.length) return;
        for (const row of rows) yield row;
        cursor = rows.at(-1).id;
        if (rows.length < batch) return;
      }
    },

    async head() {
      const tx = db.transaction(EVENTS, "readonly");
      const cursor = await request(tx.objectStore(EVENTS).openCursor(null, "prev"));
      return cursor ? cursor.value.id : 0;
    },

    /**
     * Drop the oldest events beyond a retention window.
     *
     * Ids are never reused, so a client resuming from a trimmed cursor simply
     * receives everything still present — it loses history, not its place.
     */
    async trim(keep = RETENTION) {
      const head = await api.head();
      const floor = head - keep;
      if (floor <= 0) return 0;

      const tx = db.transaction([EVENTS, META], "readwrite");
      tx.objectStore(EVENTS).delete(IDBKeyRange.upperBound(floor));
      tx.objectStore(META).put({ key: FLOOR, value: floor });
      await done(tx);
      channel.postMessage({ type: "trimmed", floor });
      return floor;
    },

    /** Oldest id still retained. Below this, history is gone. */
    async floor() {
      return (await api.getMeta(FLOOR))?.value ?? 0;
    },

    subscribe(fn) {
      subscribers.add(fn);
      return () => subscribers.delete(fn);
    },

    /** Announce to other contexts without going through the service worker. */
    onRemote(fn) {
      channel.addEventListener("message", (e) => fn(e.data));
      return () => channel.close();
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

    /* ── outbox: work that needs the network, deferred until there is some ── */

    async enqueue(op) {
      const tx = db.transaction(OUTBOX, "readwrite");
      const id = await request(tx.objectStore(OUTBOX).add({ ...op, at: Date.now() }));
      await done(tx);
      return id;
    },

    async pending() {
      const tx = db.transaction(OUTBOX, "readonly");
      return request(tx.objectStore(OUTBOX).getAll());
    },

    async dequeue(id) {
      const tx = db.transaction(OUTBOX, "readwrite");
      tx.objectStore(OUTBOX).delete(id);
      await done(tx);
    },

    async clear() {
      const tx = db.transaction([EVENTS, META, OUTBOX], "readwrite");
      for (const s of [EVENTS, META, OUTBOX]) tx.objectStore(s).clear();
      await done(tx);
    },
  };

  return api;
}
