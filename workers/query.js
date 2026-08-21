/*
 * SQL read model over the log, in a dedicated worker.
 *
 * A dedicated worker, not the service worker, because that is the only place
 * SQLite can reach OPFS: createSyncAccessHandle() is [Exposed=DedicatedWorker],
 * and a service worker cannot spawn a Worker (`Worker` is
 * window_and_worker_except_service). So the page owns this, not sw.js.
 *
 * This is a DERIVED read model, never the source of truth. Two documented
 * constraints make that the only honest arrangement:
 *
 *   - The "opfs" VFS needs SharedArrayBuffer, which needs COOP/COEP response
 *     headers. A static host cannot send headers, so that VFS cannot load here.
 *   - The "opfs-sahpool" VFS works without those headers but, per SQLite's own
 *     docs, "does not support multi-tab concurrency" — and multi-tab is the
 *     whole point of the log.
 *
 * So persistence is attempted and memory is the fallback. Losing it costs
 * nothing: the log rebuilds it.
 */

const CDN = "https://cdn.jsdelivr.net/npm/@sqlite.org/sqlite-wasm@3.53.0-build1/dist/index.mjs";

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
  try {
    // Second and later tabs lose the exclusive file lock and land in memory.
    // That is the documented behaviour, not a failure to work around.
    const pool = await sqlite3.installOpfsSAHPoolVfs({ name: "stream-read-model" });
    db = new pool.OpfsSAHPoolDb("/log.sqlite3");
    storage = "opfs-sahpool";
  } catch (err) {
    db = new sqlite3.oo1.DB(":memory:", "c");
    storage = `memory (${err?.message ?? "opfs unavailable"})`;
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
