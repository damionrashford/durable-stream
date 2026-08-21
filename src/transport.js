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
  // AbortSignal.timeout measures active time, so it does not fire against a
  // suspended worker or a bfcached document the way a bare timer would.
  const deadline = AbortSignal.timeout(HANDSHAKE_MS);
  try {
    await Promise.race([
      transport.ready,
      new Promise((_, reject) => deadline.addEventListener("abort", () => reject(deadline.reason))),
    ]);
  } catch (err) {
    try {
      transport.close();
    } catch {
      // never opened
    }
    throw err;
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
 * WebSocket frames.
 *
 * Worth having because CORS does not govern WebSocket: the server sees Origin
 * and decides for itself, so an upstream that will not set CORS headers is still
 * reachable. Being a different protocol, it also sits outside the six-connection
 * per-origin cap that HTTP/1.1 imposes on SSE.
 *
 * It carries binary natively, so a payload arrives on this socket instead of
 * needing the second HTTP request the SSE path has to make. Framing: a JSON text
 * message describes what follows, and if it declares a payload the binary
 * messages after it are its body, ending at the declared length.
 *
 * The cost is that WebSocket has no backpressure — messages arrive whether or
 * not anything is ready for them. Frames are queued with a bound and the socket
 * is closed once that bound is passed, which surfaces as a reconnect from the
 * stored cursor. Dropping and resyncing is what the rest of the system already
 * does with a reader that falls behind.
 */
const WS_QUEUE_LIMIT = 256;

async function* webSocketFrames(url, { cursor, signal }) {
  const socket = new WebSocket(url);
  socket.binaryType = "arraybuffer";

  const queue = [];
  let wake = null;
  let closed = null;

  const push = (frame) => {
    queue.push(frame);
    wake?.();
    wake = null;
  };
  const fail = (reason) => {
    closed ??= reason;
    wake?.();
    wake = null;
  };

  // Set while a payload header has been seen and its bytes are still arriving.
  let payload = null;

  socket.addEventListener("open", () => cursor && socket.send(JSON.stringify({ after: cursor })));
  socket.addEventListener("error", () => fail(new Error("websocket error")));
  socket.addEventListener("close", () => fail(null));
  signal?.addEventListener("abort", () => socket.close(), { once: true });

  socket.addEventListener("message", (event) => {
    if (typeof event.data !== "string") {
      // Body bytes for the payload the last header announced.
      if (!payload) return;
      payload.controller.enqueue(new Uint8Array(event.data));
      payload.received += event.data.byteLength;
      if (payload.received >= payload.size) {
        payload.controller.close();
        payload = null;
      }
      return;
    }

    let message;
    try {
      message = JSON.parse(event.data);
    } catch {
      return void push({ kind: "event", type: "message", data: event.data });
    }

    if (message.kind === "blob") {
      let controller;
      const body = new ReadableStream({ start: (c) => (controller = c) });
      payload = { controller, size: message.size ?? Infinity, received: 0 };
      push({ ...message, body });
      return;
    }

    push({ kind: "event", type: message.type ?? "message", data: message.data ?? message, id: message.id });
    if (queue.length > WS_QUEUE_LIMIT) {
      // Nothing can slow the sender, so stop reading and resume from the cursor.
      socket.close();
      fail(new Error("websocket queue overflow"));
    }
  });

  try {
    while (true) {
      while (queue.length) yield queue.shift();
      if (closed) throw closed;
      if (socket.readyState === WebSocket.CLOSED) return;
      await new Promise((resolve) => (wake = resolve));
    }
  } finally {
    payload?.controller.close();
    if (socket.readyState <= WebSocket.OPEN) socket.close();
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
  // Chosen by scheme rather than probed: ws:// and wss:// are unambiguous, and
  // an upstream that offers one has already decided which protocol it speaks.
  if (/^wss?:/.test(url)) {
    return { kind: "websocket", frames: () => webSocketFrames(url, { cursor, signal }) };
  }

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
