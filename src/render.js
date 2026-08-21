/*
 * Rendering a pulled payload.
 *
 * How a payload should be consumed depends on what it is, and the difference is
 * not cosmetic — it decides whether the whole thing has to exist in memory:
 *
 *   text    rendered as it arrives, chunk by chunk. Nothing is ever fully held,
 *           so a log file bigger than RAM still displays.
 *   media   the element loads the payload URL itself. Its request carries Range,
 *           so playback can seek and only the played part is read.
 *   other   drained straight to a download link; never decoded, never inspected.
 *
 * Text and downloads consume the transferred stream. Media discards it and loads
 * the URL instead, because only an HTTP request can carry Range.
 */

const isText = (mime) =>
  mime.startsWith("text/") || /^application\/(json|xml|javascript|x-ndjson)/.test(mime);

const MEDIA = { "image/": "img", "video/": "video", "audio/": "audio" };
const mediaTag = (mime) => Object.entries(MEDIA).find(([p]) => mime.startsWith(p))?.[1];

export async function render(into, { stream, src, mime = "application/octet-stream", name, size }) {
  into.replaceChildren();

  if (isText(mime)) return renderText(into, stream);

  const tag = mediaTag(mime);
  if (tag) {
    // The element fetches for itself rather than consuming the transferred
    // stream: that request carries Range, so the payload is seekable and only
    // the played part is ever read. Consuming the stream would mean holding the
    // whole file in memory to produce a URL.
    stream.cancel().catch(() => {});
    return renderMedia(into, src, tag, name);
  }

  return renderDownload(into, stream, name, size);
}

/** Progressive: append each chunk as it lands, never hold the whole payload. */
async function renderText(into, stream) {
  const pre = document.createElement("pre");
  pre.className = "payload";
  into.append(pre);

  const reader = stream.pipeThrough(new TextDecoderStream()).getReader();
  let shown = 0;
  const LIMIT = 200_000; // stop painting long before the DOM becomes the bottleneck

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    if (shown < LIMIT) {
      pre.append(document.createTextNode(value));
      shown += value.length;
    } else if (shown !== Infinity) {
      pre.append(document.createTextNode("\n… truncated"));
      shown = Infinity;
      reader.cancel(); // stop pulling; backpressure reaches the worker
      break;
    }
  }
}

/** Streamed by the element itself, over a range-capable URL. */
function renderMedia(into, src, tag, name) {
  const el = document.createElement(tag);
  el.src = src;
  if (tag === "img") el.alt = name ?? "";
  else el.controls = true;
  el.className = "payload";
  into.append(el);
}

async function renderDownload(into, stream, name, size) {
  const blob = await new Response(stream).blob();
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = name ?? "payload";
  link.textContent = `${size ?? blob.size} B · download`;
  link.className = "link";
  into.append(link);
}
