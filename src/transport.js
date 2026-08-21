/*
 * Upstream transports, normalized to one shape.
 *
 * Two are implemented and picked by capability:
 *
 *   WebTransport  HTTP/3. Server opens a stream per message, so payloads are
 *                 native binary and independent — a large one does not block the
 *                 small ones behind it.
 *   SSE           HTTP/1.1 or /2 over fetch. Text only, one stream, ordered.
 *                 Binary has to be announced by reference and fetched separately.
 *
 * Both yield the same frames, so upstream.js never branches on transport:
 *
 *   { kind: "event", type, data }
 *   { kind: "blob",  key, name, mime, body }   body is a ReadableStream<Uint8Array>
 *
 * WebTransport framing: each unidirectional stream is one message — a single line
 * of JSON header, "\n", then the payload bytes. Cheap to parse, and the header
 * arrives before the body so the receiver can decide where the bytes go without
 * buffering them.
 */

import { decodeSSE } from "./sse.js";

const NEWLINE = 10;

function concat(chunks, total) {
  const out = new Uint8Array(total);
  let at = 0;
  for (const c of chunks) {
    out.set(c, at);
    at += c.length;
  }
  return out;
}

/**
 * Split "header line \n payload" without buffering the payload.
 *
 * Whatever of the payload arrived in the same chunk as the header is replayed
 * first, then the rest is pulled on demand — so backpressure reaches the socket.
 */
async function readFramed(stream) {
  const reader = stream.getReader();
  const chunks = [];
  let total = 0;
  let cut = -1;

  while (cut === -1) {
    const { value, done } = await reader.read();
    if (done) break;
    const idx = value.indexOf(NEWLINE);
    if (idx !== -1) cut = total + idx;
    chunks.push(value);
    total += value.length;
  }
  if (cut === -1) throw new Error("upstream stream ended before its header");

  const buffered = concat(chunks, total);
  const header = JSON.parse(new TextDecoder().decode(buffered.subarray(0, cut)));
  const leftover = buffered.subarray(cut + 1);

  const body = new ReadableStream({
    start(controller) {
      if (leftover.length) controller.enqueue(leftover);
    },
    async pull(controller) {
      const { value, done } = await reader.read();
      if (done) return controller.close();
      controller.enqueue(value);
    },
    cancel(reason) {
      return reader.cancel(reason);
    },
  });

  return { header, body };
}

/** How long a WebTransport handshake may take before we give up and use SSE. */
const HANDSHAKE_MS = 3000;

/**
 * Complete the handshake, and nothing else.
 *
 * Detection must not read a frame: against a genuine HTTP/3 server that simply
 * has nothing to say yet, the first read blocks forever, so the probe would
 * neither succeed nor fall back. `ready` settles on the connection alone.
 */
async function handshake(url, signal) {
  const transport = new WebTransport(url);
  let timer;
  try {
    await Promise.race([
      transport.ready,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error("webtransport handshake timed out")), HANDSHAKE_MS);
      }),
    ]);
  } catch (err) {
    try {
      transport.close();
    } catch {
      // never opened
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
  signal?.addEventListener("abort", () => transport.close(), { once: true });
  return transport;
}

async function* webTransportFrames(transport, signal) {
  const streams = transport.incomingUnidirectionalStreams.getReader();
  try {
    while (true) {
      const { value, done } = await streams.read();
      if (done || signal?.aborted) return;

      const { header, body } = await readFramed(value);
      if (header.kind === "blob") {
        yield { kind: "blob", key: header.key, name: header.name, mime: header.mime, body };
      } else {
        // Small by definition — an event is metadata, so collecting it is fine.
        const text = await new Response(body).text();
        yield { kind: "event", type: header.type ?? "message", data: safeJSON(text), id: header.id };
      }
    }
  } finally {
    try {
      transport.close();
    } catch {
      // already closed
    }
  }
}

async function* sseFrames(url, { headers, cursor, signal }) {
  const res = await fetch(url, {
    signal,
    headers: {
      accept: "text/event-stream",
      ...headers,
      ...(cursor ? { "last-event-id": cursor } : {}),
    },
  });
  if (!res.ok || !res.body) throw new Error(`upstream ${res.status}`);

  const events = res.body.pipeThrough(new TextDecoderStream()).pipeThrough(decodeSSE());
  for await (const event of events) {
    if (signal?.aborted) return;
    const data = safeJSON(event.data);

    // SSE cannot carry bytes, so a payload is announced by reference. The frame
    // carries `href` instead of `body`; the receiver decides how to pull it,
    // because a large one belongs in Background Fetch rather than this socket.
    if (event.event === "blob" && data?.url) {
      yield {
        kind: "blob",
        key: data.key,
        name: data.name,
        mime: data.mime,
        size: data.size,
        href: new URL(data.url, url).href,
        id: event.id,
      };
      continue;
    }
    yield { kind: "event", type: event.event ?? "message", data, id: event.id };
  }
}

function safeJSON(text) {
  try {
    return JSON.parse(text);
  } catch {
    return text; // an upstream is not obliged to send JSON
  }
}

/**
 * Try WebTransport, fall back to SSE.
 *
 * Detection is a real connection attempt, not a feature check: the API can exist
 * while the server speaks only HTTP/1.1, and https:// URLs are valid for both.
 * Returns which one won so it can be surfaced.
 */
export async function openUpstream(url, { headers = {}, cursor = null, signal } = {}) {
  if ("WebTransport" in globalThis) {
    try {
      const transport = await handshake(url, signal);
      return { kind: "webtransport", frames: () => webTransportFrames(transport, signal) };
    } catch {
      // Not HTTP/3, or it never answered. Either way, SSE.
    }
  }
  return { kind: "sse", frames: () => sseFrames(url, { headers, cursor, signal }) };
}
