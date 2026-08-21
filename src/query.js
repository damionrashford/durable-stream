/*
 * Page-side client for the SQL read model.
 *
 * The page owns the worker because a service worker cannot spawn one. Everything
 * here is lazy: the worker, the ~1.4 MB of SQLite wasm, and the backfill all
 * happen on the first query and never before. Nothing in the app depends on it.
 */

const WORKER_URL = new URL("./query-worker.js", import.meta.url);

export function createQueryClient({ backfill, base }) {
  let worker = null;
  let seq = 0;
  let synced = null;
  const waiting = new Map();

  function spawn() {
    if (worker) return worker;
    worker = new Worker(WORKER_URL, { type: "module" });
    worker.addEventListener("message", (event) => {
      const { id, ok, result, error } = event.data ?? {};
      const pending = waiting.get(id);
      if (!pending) return;
      waiting.delete(id);
      ok ? pending.resolve(result) : pending.reject(new Error(error));
    });
    return worker;
  }

  function call(type, args = {}) {
    const w = spawn();
    const id = ++seq;
    return new Promise((resolve, reject) => {
      waiting.set(id, { resolve, reject });
      w.postMessage({ id, type, ...args });
    });
  }

  /** Replay the whole log into the read model once, then keep it current. */
  function sync() {
    synced ??= (async () => {
      const rows = await backfill();
      if (rows.length) await call("ingest", { rows });
      return rows.length;
    })();
    return synced;
  }

  return {
    /** Feed a live event. Cheap and idempotent; safe before the first query. */
    push(row) {
      if (!worker) return; // nothing spawned yet, so nothing to keep current
      call("ingest", { rows: [row] }).catch(() => {});
    },

    async query(sql, params) {
      await sync();
      const { rows } = await call("query", { sql, params });
      return rows;
    },

    async status() {
      await sync();
      return call("status");
    },

    base,
  };
}
