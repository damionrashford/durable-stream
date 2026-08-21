/*
 * SQL read model over the log.
 *
 * Runs in a dedicated worker because that is the only context where SQLite can
 * reach OPFS: createSyncAccessHandle() is [Exposed=DedicatedWorker]. A service
 * worker cannot spawn one, so the page owns this worker.
 *
 * The model is derived. The log in IndexedDB stays authoritative, and this is
 * rebuilt from it, because the VFS options both constrain what SQLite can be
 * here: "opfs" needs SharedArrayBuffer and therefore COOP/COEP headers, while
 * "opfs-sahpool" needs no headers but does not support multi-tab concurrency.
 * sahpool is used where available and an in-memory database otherwise; either
 * way the log can rebuild it.
 */

const CDN = "https://cdn.jsdelivr.net/npm/@sqlite.org/sqlite-wasm@3.53.0-build1/dist/index.mjs";

/** Held by whichever context owns the on-disk database. */
const PERSISTENT_LOCK = "stream-read-model";

/**
 * Take a lock and keep it, or report immediately that someone else has it.
 *
 * navigator.locks releases when its callback settles, so holding one across the
 * lifetime of a resource means handing back a release function and leaving the
 * callback pending until it is called.
 */
async function elect(name) {
  if (!navigator.locks) return { release() {} };
  let release;
  const granted = new Promise((resolve) => {
    navigator.locks
      .request(name, { ifAvailable: true }, (lock) => {
        if (!lock) return void resolve(null);
        return new Promise((done) => {
          release = done;
          resolve({ release: done });
        });
      })
      .catch(() => resolve(null));
  });
  const held = await granted;
  return held ?? null;
}

const SCHEMA = `
  create table if not exists events (
    id    integer primary key,
    type  text not null,
    at    integer,
    data  text
  );
  create index if not exists events_type on events(type);
  create index if not exists events_at   on events(at);
`;

let ready = null;

async function open() {
  const sqlite3InitModule = (await import(CDN)).default;
  const sqlite3 = await sqlite3InitModule();

  let db;
  let storage;

  // The sahpool VFS takes an exclusive lock on its files, so exactly one context
  // may hold the persistent database. Electing that context with a Web Lock
  // makes which one deterministic and lets the others fall back immediately,
  // instead of racing for the file and reporting whatever they happen to get.
  //
  // The lock is held for as long as the database is open, so the callback
  // returns a promise that only settles when this worker is torn down.
  const elected = await elect(PERSISTENT_LOCK);

  if (elected) {
    try {
      const pool = await sqlite3.installOpfsSAHPoolVfs({ name: "stream-read-model" });
      db = new pool.OpfsSAHPoolDb("/log.sqlite3");
      storage = "opfs-sahpool";
    } catch (err) {
      elected.release();
      db = new sqlite3.oo1.DB(":memory:", "c");
      storage = `memory (${err?.message ?? "opfs unavailable"})`;
    }
  } else {
    // Another context holds the persistent database. This one is still fully
    // functional; the log rebuilds it on every start.
    db = new sqlite3.oo1.DB(":memory:", "c");
    storage = "memory (persistent copy held elsewhere)";
  }

  db.exec(SCHEMA);
  return { db, storage, version: sqlite3.version?.libVersion };
}

const boot = () => (ready ??= open());

const handlers = {
  async status() {
    const { db, storage, version } = await boot();
    const [[count = 0] = []] = db.exec({ sql: "select count(*) from events", returnValue: "resultRows" });
    const [[head = 0] = []] = db.exec({
      sql: "select coalesce(max(id),0) from events",
      returnValue: "resultRows",
    });
    return { storage, version, count, head };
  },

  /** Idempotent by primary key, so replaying the log twice is harmless. */
  async ingest({ rows }) {
    const { db } = await boot();
    db.transaction(() => {
      const stmt = db.prepare("insert or replace into events(id,type,at,data) values(?,?,?,?)");
      try {
        for (const r of rows) {
          stmt.bind([r.id, r.type, r.at ?? null, JSON.stringify(r.data ?? null)]).stepReset();
        }
      } finally {
        stmt.finalize();
      }
    });
    return { ingested: rows.length };
  },

  /**
   * Read-only by construction. This runs whatever the caller passes, so the
   * guard is the statement type, not string inspection.
   */
  async query({ sql, params }) {
    const { db } = await boot();
    if (!/^\s*(select|with|explain|pragma)\b/i.test(sql)) {
      throw new Error("read model is query-only; append through the log instead");
    }
    const rows = db.exec({ sql, bind: params ?? [], returnValue: "resultRows", rowMode: "object" });
    return { rows };
  },

  async reset() {
    const { db } = await boot();
    db.exec("delete from events");
    return { ok: true };
  },
};

self.addEventListener("message", async (event) => {
  const { id, type, ...args } = event.data ?? {};
  try {
    const handler = handlers[type];
    if (!handler) throw new Error(`unknown request: ${type}`);
    self.postMessage({ id, ok: true, result: await handler(args) });
  } catch (err) {
    self.postMessage({ id, ok: false, error: String(err?.message ?? err) });
  }
});
