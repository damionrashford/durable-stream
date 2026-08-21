/*
 * Service-worker mount. Thin on purpose — the routes live in handler.js.
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
 * Three events besides fetch, all of which exist because this worker is not a
 * daemon and gets killed when idle:
 *   sync                   flush the outbox once there is a network again
 *   backgroundfetchsuccess collect a payload the browser downloaded for us
 *   message                serve the data plane
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

const OUTBOX_TAG = "outbox";

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

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
      }),
    };
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
