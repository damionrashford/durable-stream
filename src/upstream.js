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

/**
 * Idempotent. Safe to call on every request — a service worker is killed when
 * idle, so this is the hook that brings the connection back when it wakes.
 */
export function ensureUpstream(state, { url, log, blobs, headers = {} }) {
  if (!url || state.running) return state;
  state.running = true;
  state.abort = new AbortController();

  (async () => {
    let backoff = 1000;

    while (state.running) {
      try {
        const cursor = (await log.getMeta(CURSOR))?.value ?? null;
        const link = await openUpstream(url, { headers, cursor, signal: state.abort.signal });

        backoff = 1000;
        state.transport = link.kind;
        await log.append({ type: "upstream", data: { state: "connected", via: link.kind, url } });

        for await (const frame of link.frames()) {
          if (!state.running) break;

          if (frame.kind === "blob") {
            const key = frame.key ?? crypto.randomUUID();
            // Straight to disk. Awaiting it is the backpressure.
            const size = await blobs.put(key, frame.body);
            const meta = {
              name: frame.name ?? key,
              mime: frame.mime || "application/octet-stream",
              size,
            };
            await log.putMeta(key, meta);
            await log.append({ type: "blob", data: { key, ...meta } });
          } else {
            await log.append({ type: frame.type, data: frame.data });
          }

          // Persist per frame: the worker can be killed at any point.
          if (frame.id) await log.putMeta(CURSOR, { value: frame.id });
        }
      } catch (err) {
        if (!state.running) return;
        await log.append({ type: "upstream", data: { state: "lost", error: String(err) } });
      }

      if (!state.running) return;
      await new Promise((r) => setTimeout(r, backoff));
      backoff = Math.min(backoff * 2, 30_000);
    }
  })();

  return state;
}

export function stopUpstream(state) {
  state.running = false;
  state.abort?.abort();
}
