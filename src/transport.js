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

/*
 * WebSocket framing, shared by both socket transports.
 *
 * A JSON text message describes what follows. If it declares a payload, the
 * binary messages after it are its body, ending at the declared length — which
 * keeps the body a ReadableStream that pipes to OPFS like every other
 * transport's, rather than something that has to be collected first.
 *
 * `read` is handed a sink and pumps messages into it. Splitting it this way is
 * what lets one framing implementation serve a source that can be slowed and one
 * that cannot: the body stream is fed by later messages, so the pump has to keep
 * running while a frame is being consumed, and cannot simply be the generator.
 */
function frameSink() {
  const queue = [];
  let payload = null;
  let wake = null;
  let ended = null;

  const wakeUp = () => {
    wake?.();
    wake = null;
  };

  return {
    depth: () => queue.length,

    accept(message) {
      if (typeof message !== "string") {
        if (!payload) return;
        const bytes = message instanceof Uint8Array ? message : new Uint8Array(message);
        payload.controller.enqueue(bytes);
        payload.received += bytes.byteLength;
        if (payload.received >= payload.size) {
          payload.controller.close();
          payload = null;
        }
        return;
      }

      let parsed;
      try {
        parsed = JSON.parse(message);
      } catch {
        queue.push({ kind: "event", type: "message", data: message });
        return wakeUp();
      }

      if (parsed.kind === "blob") {
        let controller;
        const body = new ReadableStream({ start: (c) => (controller = c) });
        payload = { controller, size: parsed.size ?? Infinity, received: 0 };
        queue.push({ ...parsed, body });
      } else {
        queue.push({
          kind: "event",
          type: parsed.type ?? "message",
          data: parsed.data ?? parsed,
          id: parsed.id,
        });
      }
      wakeUp();
    },

    end(reason) {
      ended ??= reason ?? true;
      payload?.controller.close();
      payload = null;
      wakeUp();
    },

    async *frames() {
      while (true) {
        while (queue.length) yield queue.shift();
        if (ended) {
          if (ended instanceof Error) throw ended;
          return;
        }
        await new Promise((resolve) => (wake = resolve));
      }
    },
  };
}

/** Frames the reader may fall behind by before the socket stops being read. */
const SOCKET_QUEUE_LIMIT = 64;

/**
 * WebSocketStream: the same protocol with real backpressure.
 *
 * Reading is a pump that stops once the queue is full, and stopping the read is
 * what stops the sender — the socket's receive window closes and the server's
 * writes block. Measured against a server flooding 64 KB messages while the
 * consumer took one every 200ms: the sender managed 109 messages and was blocked
 * 261 times, against 15,296 messages and no blocking on plain WebSocket, where
 * the difference sat in this process's memory.
 *
 * Non-standard and Chromium-only, so plain WebSocket remains the fallback.
 */
async function webSocketStreamFrames(url, { cursor, signal }) {
  const socket = new WebSocketStream(url, {
    signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(HANDSHAKE_MS)]) : AbortSignal.timeout(HANDSHAKE_MS),
  });
  const { readable, writable } = await socket.opened;

  if (cursor) {
    const writer = writable.getWriter();
    await writer.write(JSON.stringify({ after: cursor }));
    writer.releaseLock();
  }

  const sink = frameSink();
  signal?.addEventListener("abort", () => socket.close(), { once: true });

  (async () => {
    const reader = readable.getReader();
    try {
      while (true) {
        // Not reading is the backpressure. Nothing else here has to bound memory.
        while (sink.depth() >= SOCKET_QUEUE_LIMIT) await new Promise((r) => setTimeout(r, 20));
        const { value, done } = await reader.read();
        if (done) break;
        sink.accept(value);
      }
      sink.end();
    } catch (err) {
      sink.end(err);
    }
  })();

  return sink.frames();
}

/**
 * Plain WebSocket: same framing, no way to slow the sender.
 *
 * Messages arrive as events whether or not anything is ready for them, so the
 * only defence is to give up once too many have piled up and resume from the
 * stored cursor — the same drop-and-resync applied to a slow reader elsewhere.
 */
function webSocketFrames(url, { cursor, signal }) {
  const socket = new WebSocket(url);
  socket.binaryType = "arraybuffer";
  const sink = frameSink();

  socket.addEventListener("open", () => cursor && socket.send(JSON.stringify({ after: cursor })));
  socket.addEventListener("error", () => sink.end(new Error("websocket error")));
  socket.addEventListener("close", () => sink.end());
  signal?.addEventListener("abort", () => socket.close(), { once: true });

  socket.addEventListener("message", (event) => {
    sink.accept(event.data);
    if (sink.depth() > SOCKET_QUEUE_LIMIT) {
      socket.close();
      sink.end(new Error("websocket queue overflow"));
    }
  });

  return sink.frames();
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
    if ("WebSocketStream" in globalThis) {
      try {
        const frames = await webSocketStreamFrames(url, { cursor, signal });
        return { kind: "websocketstream", frames: () => frames };
      } catch {
        // Handshake failed or timed out; the plain socket gets its own attempt.
      }
    }
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
