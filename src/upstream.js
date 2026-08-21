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
        state.transport = link.kind;
        await log.append({ type: "upstream", data: { state: "connected", via: link.kind, url } });

        for await (const frame of link.frames()) {
          if (!state.running) break;

          if (frame.kind === "blob") {
            await receiveBlob(frame, { log, blobs, registration, url, signal: state.abort.signal });
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
 * Store an incoming payload.
 *
 * Big ones are handed to Background Fetch instead of being streamed inline: it
 * keeps downloading after the tab closes and after this worker is killed, shows
 * OS-level progress, and wakes us with `backgroundfetchsuccess` when it lands.
 * An inline stream dies with the worker.
 */
async function receiveBlob(frame, { log, blobs, registration, signal }) {
  const key = frame.key ?? crypto.randomUUID();
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

  // WebTransport hands us the bytes; SSE hands us a reference to go get them.
  let body = frame.body;
  if (!body) {
    const res = await fetch(frame.href, { signal });
    if (!res.ok || !res.body) throw new Error(`payload ${res.status}`);
    body = res.body;
  }

  // Bytes land first, the log entry commits second. There is no transaction
  // across OPFS and IndexedDB, so the log is the authority: an orphaned file is
  // recoverable garbage, a logged event with no bytes is a broken promise.
  const { size, stored, encoding } = await blobs.put(key, body, mime);
  const meta = { name, mime, size: frame.size ?? size, stored, encoding };
  await log.putMeta(key, meta);
  await log.append({ type: "blob", data: { key, ...meta } });
}

export function stopUpstream(state) {
  state.running = false;
  state.abort?.abort();
}
