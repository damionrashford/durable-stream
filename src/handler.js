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
function streamEvents(request, { log }) {
  const resumeFrom = cursorOf(request);
  let unsubscribe = null;

  const source = new ReadableStream({
    async start(controller) {
      const emit = (e) =>
        controller.enqueue({ id: e.id, data: JSON.stringify({ type: e.type, data: e.data }) });

      let live = false;
      const buffered = [];
      unsubscribe = log.subscribe((e) => {
        if (!live) return void buffered.push(e);
        try {
          emit(e);
        } catch {
          unsubscribe?.();
        }
      });

      let last = resumeFrom;
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
  });

  // Objects → wire strings → bytes. Every stage carries backpressure.
  return new Response(source.pipeThrough(encodeSSE()).pipeThrough(new TextEncoderStream()), {
    headers: SSE_HEADERS,
  });
}

async function appendEvent(request, { log }) {
  const body = await request.json().catch(() => null);
  if (!body?.type) return json({ error: "type required" }, 400);
  return json({ id: await log.append({ type: body.type, data: body.data ?? null }) });
}

/**
 * The request body pipes straight to OPFS — never buffered, never stringified.
 *
 * Bytes land first, the log entry commits second. There is no transaction across
 * OPFS and IndexedDB, so the log is the authority: an orphaned file is recoverable
 * garbage, but a logged event with no bytes is a broken promise.
 */
async function putBlob(request, { log, blobs }, { key }) {
  const size = await blobs.put(key, request.body);
  const meta = {
    name: request.headers.get("x-name") ?? key,
    mime: request.headers.get("x-mime") || "application/octet-stream",
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

export function createHandler(base, stores) {
  const match = createRouter(base, [
    { method: "GET", path: "/api/events", handler: streamEvents },
    { method: "POST", path: "/api/events", handler: appendEvent },
    { method: "PUT", path: "/api/blob/:key", handler: putBlob },
    { method: "GET", path: "/api/blob/:key", handler: getBlob },
  ]);

  /** Returns null when the route isn't ours, so the caller can fall through. */
  return function handle(request) {
    const hit = match(request);
    if (!hit) return null;
    return Promise.resolve(hit.handler(request, stores, hit.params));
  };
}
