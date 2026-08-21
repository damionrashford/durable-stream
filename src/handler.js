/*
 * The request handler.
 *
 * Takes a Request, returns a Response, speaks ReadableStream — and touches no
 * service-worker globals, so the routes are testable on their own and the storage
 * layer is swappable. sw.js is a thin mount over this.
 *
 *   GET  /api/events?after=<id>   SSE. Replays from the cursor, then streams live.
 *   POST /api/events              Append {type, data}. Returns the new id.
 *   PUT  /api/blob/:key           Store a payload; announces it on the log.
 *   GET  /api/blob/:key           Stream it back.
 *   GET  /api/status              Log head/floor, outbox depth, storage durability.
 */

import { createRouter } from "./router.js";
import { encodeSSE, SSE_HEADERS } from "./sse.js";

const json = (body, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });

/**
 * Resume cursor.
 *
 * Last-Event-ID is the spec mechanism, and it works when a real server terminates
 * the stream — but Chrome does not forward it into a service worker's FetchEvent
 * request (it is a forbidden request header). So the cursor rides in the query
 * string, which the client controls and every runtime passes through untouched.
 */
function cursorOf(request) {
  const fromUrl = Number.parseInt(new URL(request.url).searchParams.get("after") ?? "", 10);
  if (Number.isFinite(fromUrl)) return fromUrl;
  return Number.parseInt(request.headers.get("last-event-id") ?? "", 10) || 0;
}

/**
 * The control plane: a durable, resumable event stream.
 *
 * Subscription happens *before* replay. Otherwise an event appended while we were
 * reading history would fall between the two and be lost forever — the exact gap
 * a resume cursor exists to close.
 */
/** Events the reader may fall behind by before we stop queueing for it. */
const HIGH_WATER = 64;

function streamEvents(request, { log }) {
  const resumeFrom = cursorOf(request);
  let unsubscribe = null;

  const source = new ReadableStream(
    {
      async start(controller) {
        const emit = (e) =>
          controller.enqueue({ id: e.id, data: JSON.stringify({ type: e.type, data: e.data }) });

        let live = false;
        let lagging = false;
        let last = resumeFrom;
        const buffered = [];

        // Subscribe *before* replaying. Otherwise an event appended while we were
        // reading history falls between the two and is lost — the exact gap a
        // resume cursor exists to close.
        unsubscribe = log.subscribe((e) => {
          if (!live) return void buffered.push(e);

          // desiredSize goes negative when the consumer can't keep up. Queueing
          // anyway would grow until the tab dies. Because the log is durable we
          // can drop instead and tell the client to resync from its cursor —
          // nothing is lost, and memory stays bounded.
          if (controller.desiredSize !== null && controller.desiredSize <= 0) {
            if (!lagging) {
              lagging = true;
              try {
                controller.enqueue({ data: JSON.stringify({ type: "lagged", data: { from: last } }) });
              } catch {
                unsubscribe?.();
              }
            }
            return;
          }

          lagging = false;
          try {
            emit(e);
            last = e.id;
          } catch {
            unsubscribe?.();
          }
        });

        for await (const e of log.since(resumeFrom)) {
          emit(e);
          last = e.id;
        }

        // Anything that landed during replay, minus what replay already covered.
        for (const e of buffered) if (e.id > last) emit(e);
        live = true;
      },
      cancel() {
        unsubscribe?.();
      },
    },
    new CountQueuingStrategy({ highWaterMark: HIGH_WATER }),
  );

  // Objects → wire strings → bytes. Every stage carries backpressure.
  return new Response(source.pipeThrough(encodeSSE()).pipeThrough(new TextEncoderStream()), {
    headers: SSE_HEADERS,
  });
}

/**
 * Append locally, then queue the same event for the server if one is configured.
 *
 * The local write always succeeds — it is IndexedDB, not the network — so the
 * page never has to care whether it is online. The upstream copy is deferred to
 * a `sync` event, which the browser fires once connectivity returns, page open
 * or not. Registration failing (unsupported, or permission denied) is not fatal:
 * the operation stays queued and the next flush picks it up.
 */
async function appendEvent(request, ctx) {
  const { log, upstream, registration, outboxTag } = ctx;
  const body = await request.json().catch(() => null);
  if (!body?.type) return json({ error: "type required" }, 400);

  const id = await log.append({ type: body.type, data: body.data ?? null });

  let queued = false;
  if (upstream) {
    await log.enqueue({
      url: upstream,
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ type: body.type, data: body.data ?? null }),
    });
    queued = true;
    await registration?.sync?.register(outboxTag).catch(() => {});
  }

  return json({ id, queued });
}

/**
 * The request body pipes straight to OPFS — never buffered, never stringified.
 *
 * Bytes land first, the log entry commits second. There is no transaction across
 * OPFS and IndexedDB, so the log is the authority: an orphaned file is recoverable
 * garbage, but a logged event with no bytes is a broken promise.
 */
async function putBlob(request, { log, blobs }, { key }) {
  const mime = request.headers.get("x-mime") || "application/octet-stream";
  const name = request.headers.get("x-name") ?? key;

  let size;
  let stored;
  let encoding;
  try {
    ({ size, stored, encoding } = await blobs.put(key, request.body, mime));
  } catch (err) {
    // Out of space. blobs.put() already removed the partial file, so the store
    // is consistent; the caller decides whether to trim and retry.
    if (err?.name === "QuotaExceededError") {
      return json({ error: "quota exceeded", key }, 507);
    }
    throw err;
  }

  const meta = { name, mime, size, stored, encoding };
  await log.putMeta(key, meta);
  await log.append({ type: "blob", data: { key, ...meta } });
  return json({ key, ...meta });
}

async function getBlob(_request, { log, blobs }, { key }) {
  const meta = (await log.getMeta(key)) ?? {};
  const stream = await blobs.get(key, { encoding: meta.encoding });
  if (!stream) return json({ error: meta.pending ? "still downloading" : "not found" }, 404);

  // Lazy: bytes come off disk as the consumer reads. No content-length when the
  // payload was compressed — the on-disk size is not the decoded size, and a
  // wrong one is worse than none.
  const headers = { "content-type": meta.mime || "application/octet-stream" };
  if (!meta.encoding && meta.size) headers["content-length"] = String(meta.size);
  return new Response(stream, { headers });
}

/**
 * Storage truth, because the log is only as durable as its bucket.
 *
 * A default bucket is "best-effort" and the browser may clear it under pressure.
 * `persisted: false` means the source of truth is deletable — worth surfacing
 * rather than assuming.
 */
async function status(_request, { log, blobs, storageStatus }) {
  const [head, floor, keys, storage] = await Promise.all([
    log.head(),
    log.floor(),
    blobs.keys(),
    storageStatus(),
  ]);
  return json({ head, floor, blobs: keys.length, outbox: (await log.pending()).length, storage });
}

export function createHandler(base, stores) {
  const match = createRouter(base, [
    { method: "GET", path: "/api/events", handler: streamEvents },
    { method: "POST", path: "/api/events", handler: appendEvent },
    { method: "PUT", path: "/api/blob/:key", handler: putBlob },
    { method: "GET", path: "/api/blob/:key", handler: getBlob },
    { method: "GET", path: "/api/status", handler: status },
  ]);

  /** Returns null when the route isn't ours, so the caller can fall through. */
  return function handle(request) {
    const hit = match(request);
    if (!hit) return null;
    return Promise.resolve(hit.handler(request, stores, hit.params));
  };
}
