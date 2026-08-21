/*
 * Rendering a pulled payload.
 *
 * How a payload should be consumed depends on what it is, and the difference is
 * not cosmetic — it decides whether the whole thing has to exist in memory:
 *
 *   text    rendered as it arrives, chunk by chunk. Nothing is ever fully held,
 *           so a log file bigger than RAM still displays.
 *   media   an <img>/<video>/<audio> needs a URL, and a URL needs a Blob, so the
 *           stream is collected first. Bounded by the file size — the one case
 *           where buffering is unavoidable without MSE.
 *   other   drained straight to a download link; never decoded, never inspected.
 *
 * Every path takes the transferred ReadableStream, so the bytes are never text on
 * the wire regardless of how they end up on screen.
 */

const isText = (mime) =>
  mime.startsWith("text/") || /^application\/(json|xml|javascript|x-ndjson)/.test(mime);

const MEDIA = { "image/": "img", "video/": "video", "audio/": "audio" };
const mediaTag = (mime) => Object.entries(MEDIA).find(([p]) => mime.startsWith(p))?.[1];

export async function render(into, { stream, mime = "application/octet-stream", name, size }) {
  into.replaceChildren();

  if (isText(mime)) return renderText(into, stream);

  const tag = mediaTag(mime);
  if (tag) return renderMedia(into, stream, tag, name);

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

/** Buffered by necessity: a media element needs an addressable URL. */
async function renderMedia(into, stream, tag, name) {
  const blob = await new Response(stream).blob();
  const url = URL.createObjectURL(blob);

  const el = document.createElement(tag);
  el.src = url;
  if (tag === "img") el.alt = name ?? "";
  else el.controls = true;
  el.className = "payload";
  into.append(el);

  // The object URL owns the blob until revoked; release once it's decoded.
  el.addEventListener(tag === "img" ? "load" : "loadeddata", () => URL.revokeObjectURL(url), {
    once: true,
  });
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
