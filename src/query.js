/*
 * Page-side client for the SQL read model.
 *
 * The page owns the worker because a service worker cannot spawn one. Everything
 * here is lazy: the worker, the ~1.4 MB of SQLite wasm, and the backfill all
 * happen on the first query and never before. Nothing in the app depends on it.
 */

const WORKER_URL = new URL("../workers/query.js", import.meta.url);

/**
 * Load the worker from a blob built out of its own source.
 *
 * Under cross-origin isolation a module worker loaded from a URL fails to start
 * and reports an error with every field empty — the same source run from a blob
 * URL starts and works. Fetching once and constructing from the bytes sidesteps
 * whatever the URL path trips over, and costs one request that was going to
 * happen anyway. The import inside it is absolute, so it still resolves.
 */
async function workerURL() {
  if (!globalThis.crossOriginIsolated) return WORKER_URL;
  const source = await (await fetch(WORKER_URL)).text();
  return URL.createObjectURL(new Blob([source], { type: "text/javascript" }));
}

export function createQueryClient({ backfill }) {
  let worker = null;
  let seq = 0;
  let synced = null;
  const waiting = new Map();

  async function spawn() {
    if (worker) return worker;
    worker = new Worker(await workerURL(), { type: "module" });
    worker.addEventListener("message", (event) => {
      const { id, ok, result, error } = event.data ?? {};
      const pending = waiting.get(id);
      if (!pending) return;
      waiting.delete(id);
      ok ? pending.resolve(result) : pending.reject(new Error(error));
    });
    return worker;
  }

  async function call(type, args = {}) {
    const w = await spawn();
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

    /** Discard the model and replay the log. Used when the log has been trimmed. */
    async resync() {
      if (!worker) return;
      await call("reset");
      synced = null;
      await sync();
    },
  };
}
