/*
 * The request handler.
 *
 * Takes a Request, returns a Response, speaks ReadableStream — and touches no
 * service-worker globals, so the routes are testable on their own and the storage
 * layer is swappable. sw.js is a thin mount over this.
 *
 * Contract:
 *   GET    /api/events      SSE. Replays from Last-Event-ID, then streams live.
 *   POST   /api/events      Append {type, data}. Returns the new id.
 *   GET    /api/log         Whole log as JSON, for inspection.
 *   DELETE /api/log         Wipe.
 *   PUT    /api/blob/:key   Store a binary payload; announces it on the log.
 *   GET    /api/blob/:key   Stream it back over HTTP.
 *
 * The page never learns which runtime answered.
 */

import { createRouter } from "./router.js";
import { encodeSSE, SSE_HEADERS } from "./sse.js";

const json = (body, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });

/**
 * The control plane: a durable, resumable event stream.
 *
 * Subscription happens *before* replay. Otherwise an event appended while we were
 * reading history would fall between the two and be lost forever — the exact gap
 * Last-Event-ID exists to close.
 */
/**
 * Resume cursor.
 *
 * Last-Event-ID is the spec mechanism, and it works when a real server terminates
 * the stream — but Chrome does not forward it into a service worker's FetchEvent
 * request (it is a forbidden request header). Verified: on auto-reconnect the
 * worker sees no such header. So the cursor also rides in the query string, which
 * the client controls and every runtime passes through untouched.
 */
function cursorOf(request) {
  const fromUrl = Number.parseInt(new URL(request.url).searchParams.get("after") ?? "", 10);
  if (Number.isFinite(fromUrl)) return fromUrl;
  return Number.parseInt(request.headers.get("last-event-id") ?? "", 10) || 0;
}

function streamEvents(request, { log, open }) {
  const resumeFrom = cursorOf(request);

  let unsubscribe = null;
  let self = null;

  const source = new ReadableStream({
    async start(controller) {
      self = controller;
      open.add(controller);
      const emit = (e) =>
        controller.enqueue({ id: e.id, event: e.type, data: JSON.stringify(e.data) });

      let live = false;
      const buffered = [];
      unsubscribe = log.subscribe((e) => {
        if (live) {
          try {
            emit(e);
          } catch {
            unsubscribe?.();
          }
        } else {
          buffered.push(e);
        }
      });

      controller.enqueue({ retry: 2000 });
      controller.enqueue({ comment: "keep-alive" });

      let last = resumeFrom;
      for await (const e of log.since(resumeFrom)) {
        emit(e);
        last = e.id;
      }

      // Anything that landed during replay, minus what replay already covered.
      for (const e of buffered) if (e.id > last) emit(e);
      live = true;

      controller.enqueue({
        event: "caught-up",
        data: JSON.stringify({
          resumeFrom,
          through: last,
          // Diagnostic: whether the browser actually forwarded the resume cursor
          // into this worker's Request. Null on a first connect, and — worth
          // checking per engine — possibly null on reconnect too.
          sawHeader: request.headers.get("last-event-id"),
        }),
      });
    },
    cancel() {
      unsubscribe?.();
      open.delete(self);
    },
  });

  // Objects → wire strings → bytes. Every stage carries backpressure.
  return new Response(source.pipeThrough(encodeSSE()).pipeThrough(new TextEncoderStream()), {
    headers: SSE_HEADERS,
  });
}

async function appendEvent(request, { log }) {
  const body = await request.json().catch(() => null);
  if (!body?.type) return json({ error: "type required" }, 400);
  const id = await log.append({ type: body.type, data: body.data ?? null });
  return json({ id });
}

async function readLog(_request, { log }) {
  const events = [];
  for await (const e of log.since(0)) events.push(e);
  return json({ head: await log.head(), count: events.length, events });
}

async function clearLog(_request, { log }) {
  await log.clear();
  return json({ ok: true });
}

/**
 * The request body pipes straight to OPFS. It is never buffered, never stringified,
 * never base64'd — the bytes go from the network (or the page) to disk with
 * backpressure the whole way.
 *
 * Order matters: bytes land first, the log entry commits second. There is no
 * transaction across OPFS and IndexedDB, so the log is the authority — an orphaned
 * file is recoverable garbage, but a logged event with no bytes is a broken promise.
 */
async function putBlob(request, { log, blobs }, { key }) {
  const size = await blobs.put(key, request.body);
  const meta = {
    name: request.headers.get("x-name") ?? key,
    mime: request.headers.get("x-mime") || "application/octet-stream",
    sha256: request.headers.get("x-sha256") ?? null,
    size,
  };

  await log.putMeta(key, meta);
  await log.append({ type: "blob", data: { key, ...meta } });
  return json({ key, ...meta });
}

async function getBlob(_request, { log, blobs }, { key }) {
  const file = await blobs.get(key);
  if (!file) return json({ error: "not found" }, 404);
  const meta = (await log.getMeta(key)) ?? {};
  // A File is a Blob: this response body is lazy and streams off disk.
  return new Response(file, {
    headers: {
      "content-type": meta.mime || "application/octet-stream",
      "content-length": String(file.size),
    },
  });
}

/**
 * End every open stream from the server side. The browser then reconnects on its
 * own and replays its Last-Event-ID, which is the real resume path — closing the
 * EventSource from the page instead would discard the cursor and restart at zero.
 */
async function kick(_request, { open }) {
  const count = open.size;
  for (const controller of open) {
    try {
      controller.close();
    } catch {
      // already torn down
    }
  }
  open.clear();
  return json({ closed: count });
}

async function status(_request, { log, blobs, storageStatus }) {
  return json({
    head: await log.head(),
    blobs: await blobs.keys(),
    storage: await storageStatus(),
  });
}

export function createHandler(base, stores) {
  // Live stream controllers for this worker instance. Deliberately not persisted:
  // connections are disposable, the log is not.
  const ctx = { ...stores, open: new Set() };

  const match = createRouter(base, [
    { method: "GET", path: "/api/events", handler: streamEvents },
    { method: "POST", path: "/api/events", handler: appendEvent },
    { method: "GET", path: "/api/log", handler: readLog },
    { method: "DELETE", path: "/api/log", handler: clearLog },
    { method: "PUT", path: "/api/blob/:key", handler: putBlob },
    { method: "GET", path: "/api/blob/:key", handler: getBlob },
    { method: "GET", path: "/api/status", handler: status },
    { method: "POST", path: "/api/kick", handler: kick },
  ]);

  /** Returns null when the route isn't ours, so the caller can fall through. */
  return function handle(request) {
    const hit = match(request);
    if (!hit) return null;
    return Promise.resolve(hit.handler(request, ctx, hit.params));
  };
}
