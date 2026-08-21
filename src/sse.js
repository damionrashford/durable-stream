/*
 * SSE framing and parsing as TransformStreams, so both directions compose with
 * pipeThrough() and inherit backpressure instead of being hand-rolled loops.
 *
 * Runtime-agnostic: no DOM, no service-worker globals. Runs in a page, a service
 * worker, a Cloudflare Worker, or Bun.
 */

/** Serialize one event to the wire format. A blank line terminates the message. */
export function frame({ comment, retry, id, event, data } = {}) {
  let out = "";
  if (comment !== undefined) out += `: ${comment}\n`;
  if (retry !== undefined) out += `retry: ${retry}\n`;
  if (id !== undefined) out += `id: ${id}\n`;
  if (event !== undefined) out += `event: ${event}\n`;
  // A literal newline would terminate the field, so multi-line payloads get one
  // `data:` per line. The receiver rejoins them with \n.
  if (data !== undefined) for (const line of String(data).split("\n")) out += `data: ${line}\n`;
  return `${out}\n`;
}

/** {id?, event?, data?} objects → wire-format strings. */
export function encodeSSE() {
  return new TransformStream({
    transform(event, controller) {
      controller.enqueue(frame(event));
    },
  });
}

function parseFrame(raw) {
  const out = { data: [] };
  for (const line of raw.split("\n")) {
    if (!line || line.startsWith(":")) continue; // blank or comment
    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    // Exactly one leading space is stripped, per spec. Two means one survives.
    const value = colon === -1 ? "" : line.slice(colon + 1).replace(/^ /, "");
    if (field === "data") out.data.push(value);
    else out[field] = value;
  }
  if (!out.data.length && out.event === undefined && out.id === undefined) return null;
  return { ...out, data: out.data.join("\n") };
}

/**
 * Wire-format strings → event objects.
 *
 * The spec allows CRLF, CR, or LF line endings. We normalize, but hold back a
 * trailing CR: it may be the first half of a CRLF split across two chunks, and
 * converting it early would fabricate a message boundary.
 */
export function decodeSSE() {
  let buffer = "";
  return new TransformStream({
    transform(chunk, controller) {
      buffer += chunk;

      let deferred = "";
      if (buffer.endsWith("\r")) {
        deferred = "\r";
        buffer = buffer.slice(0, -1);
      }
      buffer = buffer.replace(/\r\n?/g, "\n");

      let split;
      while ((split = buffer.indexOf("\n\n")) !== -1) {
        const event = parseFrame(buffer.slice(0, split));
        buffer = buffer.slice(split + 2);
        if (event) controller.enqueue(event);
      }

      buffer += deferred;
    },
  });
}

export const SSE_HEADERS = {
  "content-type": "text/event-stream; charset=utf-8",
  "cache-control": "no-cache, no-transform",
  // Tells nginx-style proxies not to buffer. Inert in-browser, correct at the edge.
  "x-accel-buffering": "no",
};
