/*
  * Service-worker implementation. Registered via the stub at the served root,
 * which is where scope requires it to be. Routes live in handler.js.
 *
 * Registered as a module worker so the imports work: register(url, {type:"module"}).
 *
 * Two lanes:
 *   control plane — SSE over fetch, handled by handler.js
 *   data plane    — transferable ReadableStreams over postMessage, handled here
 *
 * The data plane is worker-specific: transferring a stream needs a Client, which
 * only exists in a service worker. Bytes never become text, so no base64, no
 * chunk alignment, no reassembly.
 *
 * Events handled besides fetch:
 *   message                data plane — transfers a payload stream to a client
 *   sync                   flushes the outbox once the network returns
 *   backgroundfetchsuccess collects a payload downloaded outside this worker
 */

import { createHandler } from "../src/handler.js";
import { openLog } from "../src/log.js";
import { openBlobStore, storageStatus } from "../src/blobs.js";
import { ensureUpstream } from "../src/upstream.js";

/*
 * Origin to pull events from. Empty means local-only.
 *
 * This is outbound. The page's /api/events is resolved by this worker and is
 * unreachable from the network, so a server reaches the app by being connected
 * to, not by connecting. Frames that arrive are appended to the log every tab
 * reads, so one socket serves every tab.
 */
const UPSTREAM = "";
const UPSTREAM_HEADERS = {};

const OUTBOX_TAG = "outbox";

/*
 * App shell. The log survives offline in IndexedDB, but without the shell in
 * Cache Storage there is nothing to read it with — a reload with no network
 * fails before any of this runs. Bump VERSION to invalidate; entries are
 * served cache-first, so an edit is otherwise invisible to a controlled page.
 */
const VERSION = "shell-v5";
const SHELL = [
  "./",
  "./index.html",
  "./styles/main.css",
  "./src/app.js",
  "./src/render.js",
  "./src/query.js",
  "./src/handler.js",
  "./src/router.js",
  "./src/sse.js",
  "./src/log.js",
  "./src/blobs.js",
  "./src/transport.js",
  "./src/upstream.js",
  "./workers/query.js",
];

/** Minimum gap between retention passes. */
const MAINTENANCE_MS = 30_000;

self.addEventListener("install", (event) =>
  event.waitUntil(
    (async () => {
      const cache = await caches.open(VERSION);
      // Individually, so one missing entry cannot fail the whole install.
      await Promise.all(SHELL.map((path) => cache.add(path).catch(() => {})));
      await self.skipWaiting();
    })(),
  ),
);

self.addEventListener("activate", (event) =>
  event.waitUntil(
    (async () => {
      const stale = (await caches.keys()).filter((k) => k !== VERSION);
      await Promise.all(stale.map((k) => caches.delete(k)));
      // Lets the browser start fetching a navigation while this worker boots,
      // so a cold start costs no more than a warm one.
      await self.registration.navigationPreload?.enable().catch(() => {});
      await self.clients.claim();
    })(),
  ),
);

// Module state does not survive worker restarts — this is a per-instance cache,
// not storage. Everything durable is in IndexedDB or OPFS.
let booted = null;
const upstream = { running: false };

function boot() {
  booted ??= (async () => {
    const [log, blobs] = await Promise.all([openLog(), openBlobStore()]);
    const base = new URL(self.registration.scope).pathname;
    // Idempotent, and called from boot() rather than activate() because the worker
    // is killed when idle — this is what reconnects upstream when it wakes.
    ensureUpstream(upstream, {
      url: UPSTREAM,
      log,
      blobs,
      headers: UPSTREAM_HEADERS,
      registration: self.registration,
    });
    /*
     * Retention has two halves. Trimming the log drops old events; sweeping
     * drops the payloads those events were the only reference to. Without the
     * second half OPFS grows without bound no matter what the log does.
     *
     * Throttled rather than run per write: sweeping walks the whole log and the
     * whole directory, and neither changes meaningfully between two appends.
     */
    let lastMaintenance = 0;
    let maintaining = null;
    const maintain = () => {
      if (maintaining || Date.now() - lastMaintenance < MAINTENANCE_MS) return;
      maintaining = (async () => {
        try {
          if (await log.trim()) await blobs.sweep(await log.liveKeys());
        } finally {
          lastMaintenance = Date.now();
          maintaining = null;
        }
      })();
    };

    return {
      log,
      blobs,
      handle: createHandler(base, {
        log,
        blobs,
        storageStatus,
        upstream: UPSTREAM,
        registration: self.registration,
        outboxTag: OUTBOX_TAG,
        maintain,
      }),
    };
  })();
  return booted;
}

/* ── control plane ───────────────────────────────────────────────────────── */

const apiPrefix = () => new URL("api/", self.registration.scope).pathname;

/*
 * Enables cross-origin isolation by re-wrapping the document response with
 * COOP/COEP, which grants SharedArrayBuffer and the SQLite "opfs" VFS.
 *
 * A navigation from a controlled page passes through this handler, so the
 * headers can be added even where the host sends none. The first load is never
 * isolated: nothing controls it yet, so isolation starts on the load after
 * registration.
 *
 * Three conditions apply when enabled. Under require-corp every cross-origin
 * subresource must carry CORP or be fetched with CORS. A cross-origin document
 * embedded in an iframe needs more than that — CORP alone does not admit it; it
 * must also send Cross-Origin-Embedder-Policy itself — which rules out an
 * iframe relay against an origin that does not cooperate to that degree. And
 * the SQL read model's worker does not start under isolation, so query.js is
 * unavailable while this is on.
 */
const ISOLATE = false;

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  if (ISOLATE && event.request.mode === "navigate") {
    event.respondWith(
      (async () => {
        const res = await fetch(event.request);
        const headers = new Headers(res.headers);
        headers.set("cross-origin-opener-policy", "same-origin");
        headers.set("cross-origin-embedder-policy", "require-corp");
        return new Response(res.body, { status: res.status, statusText: res.statusText, headers });
      })(),
    );
    return;
  }

  if (url.origin !== self.location.origin) return;

  // Cheap synchronous test: respondWith must be called before we can await.
  if (url.pathname.startsWith(apiPrefix())) {
    event.respondWith(
      (async () => {
        const { handle } = await boot();
        return (await handle(event.request)) ?? new Response("no route", { status: 404 });
      })(),
    );
    return;
  }

  if (event.request.method !== "GET") return;
  event.respondWith(serveShell(event));
});

/*
 * Cache-first, but only for what SHELL named.
 *
 * Nothing is cached opportunistically. A response cached because it happened to
 * go past is a response that keeps being served after the file changes, and for
 * a module that means running code that no longer exists in the repo — a class
 * of bug that presents as the change simply not happening. What ships offline
 * is the explicit list, and everything else always comes from the network.
 *
 * A navigation with no network falls back to the entry document rather than a
 * browser error page.
 */
async function serveShell(event) {
  const cached = await caches.match(event.request, { ignoreSearch: true });
  if (cached) return cached;

  try {
    // Already in flight alongside worker boot, if navigation preload applied.
    return (await event.preloadResponse) || (await fetch(event.request));
  } catch (err) {
    if (event.request.mode === "navigate") {
      const shell = await caches.match("./index.html", { ignoreSearch: true });
      if (shell) return shell;
    }
    throw err;
  }
}

/* ── data plane ──────────────────────────────────────────────────────────── */

self.addEventListener("message", (event) => {
  if (event.data?.type === "pull") {
    event.waitUntil(transferBlob(event.data.key, event.source));
  }
});

async function transferBlob(key, client) {
  if (!client) return;
  const { log, blobs } = await boot();
  const meta = (await log.getMeta(key)) ?? {};
  const stream = await blobs.get(key, { encoding: meta.encoding });

  if (!stream) {
    client.postMessage({ type: "pull-miss", key, pending: Boolean(meta.pending) });
    return;
  }

  // The stream is lazy — a file larger than RAM never lands in memory here.
  // Transferring hands ownership to the page and backpressure survives the move,
  // so the page controls how fast bytes come off disk.
  client.postMessage({ type: "pull-ok", key, ...meta, stream }, [stream]);
}

/* ── deferred work ───────────────────────────────────────────────────────── */

/**
 * Fires when connectivity returns, whether or not a page is open. This is the
 * only path by which locally-produced events reach a server after being created
 * offline — a plain fetch at the time would simply have failed.
 */
self.addEventListener("sync", (event) => {
  if (event.tag !== OUTBOX_TAG) return;
  event.waitUntil(flushOutbox(event.lastChance));
});

async function flushOutbox(lastChance = false) {
  const { log } = await boot();

  for (const op of await log.pending()) {
    try {
      const res = await fetch(op.url, {
        method: op.method ?? "POST",
        headers: op.headers ?? { "content-type": "application/json" },
        body: op.body,
      });
      if (!res.ok) throw new Error(`outbox ${res.status}`);
      await log.dequeue(op.id);
      await log.append({ type: "outbox", data: { state: "sent", url: op.url } });
    } catch (err) {
      // Rejecting tells the browser to retry this sync later. On lastChance no
      // retry is coming, so record the loss rather than hide it.
      if (!lastChance) throw err;
      await log.dequeue(op.id);
      await log.append({
        type: "outbox",
        data: { state: "abandoned", url: op.url, error: String(err) },
      });
    }
  }
}

/**
 * A payload the browser finished downloading on our behalf. It kept going after
 * the tab closed and after this worker was killed; now it wakes us to collect.
 */
self.addEventListener("backgroundfetchsuccess", (event) => {
  event.waitUntil(collectBackgroundFetch(event.registration));
});

self.addEventListener("backgroundfetchfail", (event) => {
  event.waitUntil(
    boot().then(({ log }) =>
      log.append({
        type: "blob-failed",
        data: { key: event.registration.id, reason: event.registration.failureReason },
      }),
    ),
  );
});

async function collectBackgroundFetch(bgFetch) {
  const { log, blobs } = await boot();
  const key = bgFetch.id;
  const meta = (await log.getMeta(key)) ?? {};

  const [record] = await bgFetch.matchAll();
  if (!record) return;

  const response = await record.responseReady;
  if (!response.ok || !response.body) {
    await log.append({ type: "blob-failed", data: { key, status: response.status } });
    return;
  }

  const mime = meta.mime || response.headers.get("content-type") || "application/octet-stream";
  const { size, stored, encoding } = await blobs.put(key, response.body, mime);
  const settled = { name: meta.name ?? key, mime, size: meta.size ?? size, stored, encoding };

  await log.putMeta(key, settled);
  await log.append({ type: "blob", data: { key, ...settled } });
}
