/*
 * The upstream leg.
 *
 * A server cannot dial a browser, so the worker dials out and holds the socket.
 * Whatever arrives is appended to the local log, which every open tab is already
 * reading — so one connection feeds N tabs, and the pages never learn that some
 * events came from a server and others were appended locally.
 *
 * fetch() is used rather than EventSource because EventSource is GET-only and
 * cannot send headers; an upstream that needs Authorization is the normal case.
 */

import { decodeSSE } from "./sse.js";

const CURSOR = "__upstream_cursor";

/**
 * Idempotent. Safe to call on every request — a service worker is killed when
 * idle, so this is the hook that brings the connection back when it wakes.
 */
export function ensureUpstream(state, { url, log, headers = {} }) {
  if (!url || state.running) return state;
  state.running = true;

  (async () => {
    let backoff = 1000;

    while (state.running) {
      try {
        // Resume where the last connection died, not from the beginning.
        const cursor = (await log.getMeta(CURSOR))?.value ?? null;
        const res = await fetch(url, {
          headers: { accept: "text/event-stream", ...headers, ...(cursor ? { "last-event-id": cursor } : {}) },
        });
        if (!res.ok || !res.body) throw new Error(`upstream ${res.status}`);

        backoff = 1000;
        await log.append({ type: "upstream", data: { state: "connected", url } });

        const events = res.body.pipeThrough(new TextDecoderStream()).pipeThrough(decodeSSE());
        for await (const event of events) {
          if (!state.running) break;
          let data;
          try {
            data = JSON.parse(event.data);
          } catch {
            data = event.data; // upstream is not obliged to send JSON
          }
          await log.append({ type: event.event ?? "message", data });
          // Persist the cursor per event: the worker can be killed at any point.
          if (event.id) await log.putMeta(CURSOR, { value: event.id });
        }
      } catch (err) {
        await log.append({ type: "upstream", data: { state: "lost", error: String(err) } });
      }

      if (!state.running) return;
      await new Promise((r) => setTimeout(r, backoff));
      backoff = Math.min(backoff * 2, 30_000);
    }
  })();

  return state;
}
