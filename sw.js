/*
 * Service-worker mount. Thin on purpose — the routes live in handler.js.
 *
 * Registered as a module worker so the import works: register(url, {type:"module"}).
 *
 * Two lanes:
 *   control plane — SSE over fetch, handled by handler.js
 *   data plane    — transferable ReadableStreams over postMessage, handled here
 *
 * The data plane is worker-specific: transferring a stream needs a Client, which
 * only exists in a service worker. Bytes never become text, so no base64, no
 * chunk alignment, no reassembly.
 */

import { createHandler } from "./src/handler.js";
import { openLog } from "./src/log.js";
import { openBlobStore, storageStatus } from "./src/blobs.js";
import { ensureUpstream } from "./src/upstream.js";

/*
 * A real server to pull from. Empty means local-only.
 *
 * The page's /api/events is not this: it is resolved by this worker and is
 * unreachable from the network. A server cannot push into it. This is the
 * direction that works — the worker dials out and holds the socket, and whatever
 * arrives is appended to the log that every tab already reads.
 */
const UPSTREAM = "";
const UPSTREAM_HEADERS = {};

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

// Module state does not survive worker restarts — this is a per-instance cache,
// not storage. Everything durable is in IndexedDB.
let booted = null;
const upstream = { running: false };

function boot() {
  booted ??= (async () => {
    const [log, blobs] = await Promise.all([openLog(), openBlobStore()]);
    const base = new URL(self.registration.scope).pathname;
    // Idempotent, and called from boot() rather than activate() because the worker
    // is killed when idle — this is what reconnects upstream when it wakes.
    ensureUpstream(upstream, { url: UPSTREAM, log, headers: UPSTREAM_HEADERS });
    return { log, blobs, handle: createHandler(base, { log, blobs, storageStatus }) };
  })();
  return booted;
}

/* ── control plane ───────────────────────────────────────────────────────── */

const apiPrefix = () => new URL("api/", self.registration.scope).pathname;

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) return;
  // Cheap synchronous test: respondWith must be called before we can await.
  if (!url.pathname.startsWith(apiPrefix())) return;

  event.respondWith(
    (async () => {
      const { handle } = await boot();
      return (await handle(event.request)) ?? new Response("no route", { status: 404 });
    })(),
  );
});

/* ── data plane ──────────────────────────────────────────────────────────── */

self.addEventListener("message", (event) => {
  if (event.data?.type === "pull") {
    event.waitUntil(transferBlob(event.data.key, event.source));
  }
});

async function transferBlob(key, client) {
  if (!client) return;
  const { log, blobs } = await boot();
  const file = await blobs.get(key);

  if (!file) {
    client.postMessage({ type: "pull-miss", key });
    return;
  }

  const meta = (await log.getMeta(key)) ?? {};
  // File.stream() is native and lazy — a file larger than RAM never lands in
  // memory here. Transferring hands ownership to the page and backpressure
  // survives the move, so the page controls how fast bytes come off disk.
  const stream = file.stream();
  client.postMessage(
    { type: "pull-ok", key, size: file.size, ...meta, stream },
    [stream],
  );
}
