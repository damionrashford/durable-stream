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
const DB_VERSION = 3;
const EVENTS = "events";
const META = "blob-meta";
const OUTBOX = "outbox";

/**
 * Event types that make a payload key reachable.
 *
 * `blob-partial` belongs here as much as the other two. It says a transfer
 * stopped with bytes on disk and records where to continue, so the key is
 * referenced even though no complete payload exists yet — and sweeping its
 * prefix while the log still points at it is what turns a resume into a write
 * over bytes that are no longer there.
 */
const BLOB_TYPES = ["blob", "blob-pending", "blob-partial"];

/**
 * Metadata keys belonging to the log itself rather than to a payload.
 *
 * The store is shared: `__floor` and `__upstream_cursor` live alongside one
 * record per payload key. Pruning has to be able to tell them apart, so the
 * prefix is reserved and payload keys must not use it.
 */
const RESERVED = /^__/;

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
      // Reachability is recomputed on every maintenance pass, and without this
      // that means reading every event to find the few that name a payload.
      // Created here rather than at first use because an index is schema.
      const events = req.transaction.objectStore(EVENTS);
      if (!events.indexNames.contains("type")) events.createIndex("type", "type");

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

/*
 * QuotaExceededError is now its own DOMException subclass carrying `quota` and
 * `requested`, but was a plain DOMException before that, and IndexedDB nests the
 * real error under `inner`. All three shapes have to be recognised.
 */
export const isQuota = (err) =>
  (typeof QuotaExceededError !== "undefined" && err instanceof QuotaExceededError) ||
  err?.name === "QuotaExceededError" ||
  err?.inner?.name === "QuotaExceededError";

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
        const room = await api.append(entry, { retry: false });
        // The subclass reports what was asked for and what the cap is; worth
        // recording, since hitting it again means trimming is not enough.
        if (err.quota !== undefined) {
          api.append({ type: "quota", data: { quota: err.quota, requested: err.requested } }).catch(
            () => {},
          );
        }
        return room;
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
     * What the log still references: payload keys, and the objects behind them.
     *
     * Order matters as much as membership. Anything absent is orphaned and can
     * go, and when space is short the front of these lists is what goes next.
     *
     * Keys keep their first appearance, because a key names one transfer and
     * that transfer's age is when it was announced. Objects take their *last*,
     * because content addressing means several keys can share one — and an
     * object announced again a minute ago is recent data no matter how long ago
     * some other key first introduced it. Re-inserting into a Map after deleting
     * moves the entry to the end, so one pass in log order produces exactly
     * that: oldest last-reference first.
     */
    async liveObjects() {
      const keys = [];
      const seen = new Set();
      const hashes = new Map();

      // One index range per blob type, all issued before the first await so the
      // transaction stays alive. An index yields its own key order, so the
      // three runs are merged back into log order — which is what decides
      // which reference to an object counts as its newest.
      const tx = db.transaction(EVENTS, "readonly");
      const index = tx.objectStore(EVENTS).index("type");
      const perType = await Promise.all(
        BLOB_TYPES.map((type) => request(index.getAll(IDBKeyRange.only(type)))),
      );

      for (const e of perType.flat().sort((a, b) => a.id - b.id)) {
        const key = e.data?.key;
        if (key && !seen.has(key)) {
          seen.add(key);
          keys.push(key);
        }

        const hash = e.data?.sha256;
        if (!hash) continue;
        hashes.delete(hash);
        hashes.set(hash, true);
      }
      return { keys, hashes: [...hashes.keys()] };
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

    /**
     * Drop metadata for keys no surviving event mentions.
     *
     * The log is append-only; this store is not, and that seam leaks. `trim`
     * removes the events but a key's metadata is written in place and outlives
     * them, so without this there is one record per payload key ever seen,
     * forever — the growth retention exists to prevent, reintroduced on the
     * mutable side.
     *
     * It also settles what a trimmed key means. Metadata alone would still
     * resolve an address whose object sweep has already collected, which reads
     * as a payload that exists until you ask for it. Removing both together
     * makes the answer simply "unknown key".
     *
     * One transaction: the keys are read and the deletes are issued in the same
     * synchronous continuation, so nothing can write a key in between and have
     * it collected by a decision made before it existed.
     */
    async pruneMeta(liveKeys) {
      const keep = new Set(liveKeys);
      const tx = db.transaction(META, "readwrite");
      const store = tx.objectStore(META);

      let removed = 0;
      for (const key of await request(store.getAllKeys())) {
        if (RESERVED.test(key) || keep.has(key)) continue;
        store.delete(key);
        removed += 1;
      }
      await done(tx);
      return removed;
    },

    /** Oldest id still retained. Below this, history is gone. */
    async floor() {
      return (await api.getMeta(FLOOR))?.value ?? 0;
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
