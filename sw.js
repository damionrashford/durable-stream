/*
 * Service-worker mount. Thin on purpose — the routes live in handler.js, which
 * also runs at the edge unchanged.
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

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

// Module state does not survive worker restarts — this is a per-instance cache,
// not storage. Everything durable is in IndexedDB.
let booted = null;
function boot() {
  booted ??= (async () => {
    const [log, blobs] = await Promise.all([openLog(), openBlobStore()]);
    const base = new URL(self.registration.scope).pathname;
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
