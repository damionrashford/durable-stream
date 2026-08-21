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
 *   GET  /api/log                 Whole log as JSON, to backfill the read model.
 *   GET  /api/status              Log head/floor, outbox depth, storage durability.
 */

import { createRouter } from "./router.js";
import { isQuota } from "./log.js";
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

/** Fraction of the origin quota above which a write reclaims space first. */
const PRESSURE_LIMIT = 0.8;

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
                // A named event, and deliberately without an id: it is a signal
                // about the stream, not an entry in the log. An unnamed frame
                // would arrive carrying the previous id, and a client indexing
                // by id would overwrite a real event with it.
                controller.enqueue({ event: "lagged", data: JSON.stringify({ from: last }) });
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
  const { log, upstream, registration, outboxTag, maintain } = ctx;
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

  maintain?.();
  return json({ id, queued });
}

/**
 * The request body pipes straight to OPFS — never buffered, never stringified.
 *
 * Bytes land first, the log entry commits second. There is no transaction across
 * OPFS and IndexedDB, so the log is the authority: an orphaned file is recoverable
 * garbage, but a logged event with no bytes is a broken promise.
 */
async function putBlob(request, { log, blobs, maintain, storageStatus }, { key }) {
  const mime = request.headers.get("x-mime") || "application/octet-stream";
  const name = request.headers.get("x-name") ?? key;

  // Make room before writing rather than after failing. Retention runs on a
  // timer, so without this a run of large payloads reaches quota between passes
  // and the write fails for space that was about to be reclaimed anyway.
  const { pressure } = await storageStatus();
  if (pressure > PRESSURE_LIMIT) await maintain?.(true);

  let written;
  try {
    written = await blobs.put(key, request.body, mime);
  } catch (err) {
    if (!isQuota(err)) throw err;
    // blobs.put() removed the staged file, so the store is consistent. One
    // forced pass, then the caller is told for real.
    await maintain?.(true);
    return json({ error: "quota exceeded", key }, 507);
  }

  const { size, stored, encoding, sha256, deduped } = written;
  const meta = { name, mime, size, stored, encoding, sha256 };
  await log.putMeta(key, meta);
  await log.append({ type: "blob", data: { key, ...meta } });
  maintain?.();
  // `deduped` is not part of the payload's identity, so it stays out of the log
  // and the metadata — it describes this write, not these bytes.
  return json({ key, ...meta, deduped });
}

/**
 * The whole log as JSON.
 *
 * Exists to backfill the SQL read model in one request. Streaming it through
 * SSE would work but means holding a second connection open just to replay
 * history that is already complete.
 */
async function readLog(_request, { log }) {
  const events = [];
  for await (const e of log.since(0)) events.push(e);
  return json({ floor: await log.floor(), events });
}

/** `bytes=start-end`, single range only. Returns null when unusable. */
function parseRange(header, size) {
  const m = /^bytes=(\d*)-(\d*)$/.exec((header ?? "").trim());
  if (!m) return null;
  const [, rawStart, rawEnd] = m;

  // A suffix range ("bytes=-500") means the last N bytes.
  let start = rawStart === "" ? size - Number(rawEnd) : Number(rawStart);
  let end = rawStart === "" || rawEnd === "" ? size - 1 : Number(rawEnd);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  start = Math.max(0, start);
  end = Math.min(size - 1, end);
  if (start > end) return null;
  return { start, end };
}

/**
 * Serve a payload, honouring conditional and range requests.
 *
 * The key names a transfer; the object behind it is named by its content, so
 * this is where the two meet. A key with no address, or an address with nothing
 * at rest, is a 404 — the same case as a payload the log announced and the
 * browser then evicted.
 *
 * Range support is what makes a media element seekable — without 206 a <video>
 * can only play a payload from the start. It now applies to compressed payloads
 * too: the object is a sequence of independently decodable blocks, so a range
 * costs the blocks it covers instead of a decode from the beginning.
 *
 * The ETag is strong and is simply the content address. Two keys holding the
 * same bytes share one validator, which is correct — they are the same entity.
 */
async function getBlob(request, { log, blobs }, { key }) {
  const meta = (await log.getMeta(key)) ?? {};
  const object = await blobs.resolve(meta.sha256);
  if (!object) return json({ error: meta.pending ? "still downloading" : "not found" }, 404);

  const size = object.size;
  const etag = `"${meta.sha256}"`;

  // Nothing has changed and the client already has it.
  if (request.headers.get("if-none-match") === etag) {
    return new Response(null, { status: 304, headers: { etag, "cache-control": "no-cache" } });
  }

  const base = {
    etag,
    "content-type": meta.mime || "application/octet-stream",
    // Content-addressed and never rewritten, but revalidation is free and keeps
    // a deleted key from being served from cache forever.
    "cache-control": "no-cache",
    "accept-ranges": "bytes",
  };

  const range = parseRange(request.headers.get("range"), size);
  if (range) {
    const { start, end } = range;
    // Lazy either way: a slice is not read, and a block is not decoded, until
    // the body is consumed.
    return new Response(object.read(start, end), {
      status: 206,
      headers: {
        ...base,
        "content-range": `bytes ${start}-${end}/${size}`,
        "content-length": String(end - start + 1),
      },
    });
  }

  if (request.headers.get("range")) {
    // Syntactically present but unsatisfiable.
    return new Response(null, {
      status: 416,
      headers: { ...base, "content-range": `bytes */${size}` },
    });
  }

  // The plaintext length is known for both encodings now — the block trailer
  // records it — so a compressed payload no longer has to be served without a
  // content-length.
  return new Response(object.read(0, size - 1), {
    headers: { ...base, "content-length": String(size) },
  });
}

/**
 * Storage truth, because the log is only as durable as its bucket.
 *
 * A default bucket is "best-effort" and the browser may clear it under pressure.
 * `persisted: false` means the source of truth is deletable — worth surfacing
 * rather than assuming.
 */
async function status(_request, { log, blobs, storageStatus }) {
  const [head, floor, files, storage] = await Promise.all([
    log.head(),
    log.floor(),
    blobs.list(),
    storageStatus(),
  ]);
  return json({
    head,
    floor,
    blobs: files.length,
    bytes: files.reduce((n, f) => n + f.stored, 0),
    outbox: (await log.pending()).length,
    storage,
  });
}

export function createHandler(base, stores) {
  const match = createRouter(base, [
    { method: "GET", path: "/api/events", handler: streamEvents },
    { method: "POST", path: "/api/events", handler: appendEvent },
    { method: "PUT", path: "/api/blob/:key", handler: putBlob },
    { method: "GET", path: "/api/blob/:key", handler: getBlob },
    { method: "GET", path: "/api/log", handler: readLog },
    { method: "GET", path: "/api/status", handler: status },
  ]);

  /** Returns null when the route isn't ours, so the caller can fall through. */
  return async function handle(request) {
    const hit = match(request);
    if (!hit) return null;

    if (hit.allow) {
      return new Response(null, {
        status: 405,
        headers: { allow: hit.allow.join(", "), "cache-control": "no-store" },
      });
    }

    const response = await hit.handler(request, stores, hit.params);
    if (!hit.bodyless) return response;

    // HEAD: identical headers and status, body discarded. Cancelling rather than
    // ignoring it means the payload is never read off disk.
    response.body?.cancel().catch(() => {});
    return new Response(null, { status: response.status, headers: response.headers });
  };
}
