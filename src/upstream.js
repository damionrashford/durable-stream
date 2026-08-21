/*
 * The upstream leg.
 *
 * A server cannot dial a browser, so the worker dials out and holds the socket.
 * What arrives is appended to the local log, which every open tab is already
 * reading — one connection feeds N tabs, and pages never learn which events came
 * from a server and which were appended locally.
 *
 * Transport is chosen by capability in transport.js; this file only routes frames.
 *
 *   event → the log        small, ordered, cheap
 *   blob  → OPFS, then the log
 *
 * Buffering rule: an event is metadata and may be collected; a payload never is.
 * blobs.put() pipes the body straight to disk, so a file larger than memory costs
 * nothing to receive, and backpressure reaches the socket.
 */

import { openUpstream } from "./transport.js";

const CURSOR = "__upstream_cursor";

/** How often the resume cursor is written, at most. */
const CURSOR_FLUSH_MS = 1000;

/** Above this, a payload goes through Background Fetch so it survives the tab. */
const LARGE_PAYLOAD = 8 * 1024 * 1024;

/**
 * Above this, a payload is stored uncompressed so a failed transfer can resume
 * from its prefix. Below it, restarting is cheaper than giving up gzip.
 */
const RESUMABLE_ABOVE = 1024 * 1024;

/**
 * Idempotent. Safe to call on every request — a service worker is killed when
 * idle, so this is the hook that brings the connection back when it wakes.
 */
export function ensureUpstream(state, { url, log, blobs, headers = {}, registration }) {
  if (!url || state.running) return state;
  state.running = true;
  state.abort = new AbortController();

  (async () => {
    let backoff = 1000;

    while (state.running) {
      // Writing the cursor per frame is one IndexedDB transaction per event,
      // which becomes the bottleneck at any real rate. Replay is idempotent —
      // clients dedupe by id — so a stale cursor costs duplicates, never data.
      let pendingCursor = null;
      let lastFlush = 0;
      const flushCursor = async (force = false) => {
        if (pendingCursor === null) return;
        if (!force && Date.now() - lastFlush < CURSOR_FLUSH_MS) return;
        await log.putMeta(CURSOR, { value: pendingCursor });
        lastFlush = Date.now();
        pendingCursor = null;
      };

      try {
        const cursor = (await log.getMeta(CURSOR))?.value ?? null;
        const link = await openUpstream(url, { headers, cursor, signal: state.abort.signal });

        backoff = 1000;
        await log.append({ type: "upstream", data: { state: "connected", via: link.kind, url } });

        for await (const frame of link.frames()) {
          if (!state.running) break;

          if (frame.kind === "blob") {
            await receiveBlob(frame, { log, blobs, registration, signal: state.abort.signal });
          } else {
            await log.append({ type: frame.type, data: frame.data });
          }

          if (frame.id) {
            pendingCursor = frame.id;
            await flushCursor();
          }
        }
      } catch (err) {
        if (!state.running) return;
        await log.append({ type: "upstream", data: { state: "lost", error: String(err) } });
      } finally {
        // The connection is over either way; don't lose the last second of progress.
        await flushCursor(true).catch(() => {});
      }

      if (!state.running) return;
      await new Promise((r) => setTimeout(r, backoff));
      backoff = Math.min(backoff * 2, 30_000);
    }
  })();

  return state;
}

/**
 * Keys currently being written.
 *
 * createWritable() commits to a temp file and only lands on close(), so a second
 * transfer of the same key starting mid-write sees a stale size and resumes from
 * the wrong offset. One transfer per key at a time removes the question.
 */
const inFlight = new Set();

async function receiveBlob(frame, ctx) {
  const key = frame.key ?? crypto.randomUUID();
  if (inFlight.has(key)) {
    frame.body?.cancel().catch(() => {});
    return;
  }
  inFlight.add(key);
  try {
    await receiveBlobExclusive(key, frame, ctx);
  } finally {
    inFlight.delete(key);
  }
}

/**
 * Store an incoming payload.
 *
 * Big ones are handed to Background Fetch instead of being streamed inline: it
 * keeps downloading after the tab closes and after this worker is killed, shows
 * OS-level progress, and wakes us with `backgroundfetchsuccess` when it lands.
 * An inline stream dies with the worker.
 */
async function receiveBlobExclusive(key, frame, { log, blobs, registration, signal }) {
  const mime = frame.mime || "application/octet-stream";
  const name = frame.name ?? key;

  if (frame.href && frame.size > LARGE_PAYLOAD && registration?.backgroundFetch) {
    try {
      await registration.backgroundFetch.fetch(key, [frame.href], {
        title: name,
        downloadTotal: frame.size,
      });
      await log.putMeta(key, { name, mime, size: frame.size, pending: true });
      await log.append({ type: "blob-pending", data: { key, name, mime, size: frame.size } });
      return;
    } catch {
      // Duplicate id or unsupported — fall through to the inline path.
    }
  }

  // An upstream may re-announce a payload it has already delivered. Holding the
  // full announced length means there is nothing to fetch.
  if (frame.size && (await blobs.sizeOf(key)) >= frame.size && !(await log.getMeta(key))?.partial) {
    frame.body?.cancel().catch(() => {});
    return;
  }

  const resumable = (frame.size ?? 0) >= RESUMABLE_ABOVE;

  // WebTransport hands us the bytes; SSE hands us a reference to go get them.
  // Only the reference case can resume — a stream that already died is gone.
  let body = frame.body;
  let offset = 0;

  if (!body) {
    const have = resumable ? await blobs.sizeOf(key) : 0;
    const res = await fetch(frame.href, {
      signal,
      headers: have > 0 ? { range: `bytes=${have}-` } : {},
    });
    if (!res.ok || !res.body) throw new Error(`payload ${res.status}`);
    // 206 means the server honoured the range and is sending the remainder. A
    // plain 200 means it ignored it and is resending everything, so start over
    // rather than append a second copy onto the prefix we already hold.
    offset = res.status === 206 ? have : 0;
    body = res.body;
  }

  // Bytes land first, the log entry commits second. There is no transaction
  // across OPFS and IndexedDB, so the log is the authority: an orphaned file is
  // recoverable garbage, a logged event with no bytes is a broken promise.
  let result;
  try {
    result = await blobs.put(key, body, mime, { offset, resumable });
  } catch (err) {
    if (!resumable) {
      await blobs.delete(key); // no resume path, so a prefix is only garbage
      throw err;
    }
    // Keep the prefix and record how far it got. The next announcement of this
    // key picks up from there instead of starting at zero.
    await log.putMeta(key, { name, mime, size: frame.size, partial: await blobs.sizeOf(key) });
    await log.append({
      type: "blob-partial",
      data: { key, name, have: await blobs.sizeOf(key), of: frame.size ?? null },
    });
    throw err;
  }

  const meta = { name, mime, size: frame.size ?? result.size, stored: result.stored, encoding: result.encoding };
  await log.putMeta(key, meta);
  await log.append({ type: "blob", data: { key, ...meta } });
}
