/*
 * Page side. It talks to /api/* and never touches the worker directly, except on
 * the data plane — where the worker transfers a live ReadableStream over
 * postMessage rather than encoding bytes into the text stream.
 */

import { render } from "./render.js";

const $ = (id) => document.getElementById(id);
const api = (path) => new URL(path, document.baseURI).href;

const feed = $("feed");
const statusEl = $("status");

function setStatus(state, text) {
  statusEl.dataset.state = state;
  statusEl.textContent = text;
}

// Replay can redeliver ids the feed already has. The log is the truth; the view is
// idempotent over it.
const rendered = new Set();

function row(id, type) {
  if (rendered.has(id)) return null;
  rendered.add(id);

  const li = document.createElement("li");
  for (const [cls, text] of [["id", id], ["name", type]]) {
    const span = document.createElement("span");
    span.className = cls;
    span.textContent = text;
    li.append(span);
  }
  const body = document.createElement("span");
  body.className = "body";
  li.append(body);
  feed.append(li);
  li.scrollIntoView({ block: "nearest" });
  return body;
}

/* ── control plane ───────────────────────────────────────────────────────── */

let es = null;
let cursor = 0; // highest id this tab has seen; the resume point
let retry = null;

/**
 * We drive reconnection ourselves rather than letting EventSource do it.
 *
 * Its built-in retry always re-requests the same URL and relies on Last-Event-ID,
 * which the worker never receives — so an automatic reconnect would silently
 * replay the whole log every time. Reopening with ?after=<cursor> resumes exactly.
 */
function connect(from = cursor) {
  clearTimeout(retry);
  es?.close();

  const url = api(`api/events?after=${from}`);
  $("ep-get").textContent = url;
  es = new EventSource(url);

  es.addEventListener("open", () => setStatus("live", "live"));

  es.onmessage = (e) => {
    const id = Number.parseInt(e.lastEventId, 10);
    if (Number.isFinite(id) && id > cursor) cursor = id;

    const { type, data } = JSON.parse(e.data);

    // The worker dropped events rather than queue them for a reader that fell
    // behind. Nothing is lost — the log has them — so resync from our cursor.
    if (type === "lagged") return void connect(cursor);

    const body = row(e.lastEventId, type);
    if (!body) return;

    if (type === "blob") {
      const btn = document.createElement("button");
      btn.className = "link";
      btn.textContent = `${data.name} · ${data.size} B · pull`;
      btn.addEventListener("click", () => pull(data, body));
      body.append(btn);
    } else {
      body.textContent = JSON.stringify(data);
    }
  };

  es.onerror = () => {
    setStatus("wait", `reconnecting from ${cursor}`);
    es.close(); // stop the built-in retry; ours carries the cursor
    retry = setTimeout(() => connect(cursor), 800);
  };
}

/* ── data plane ──────────────────────────────────────────────────────────── */

const pending = new Map();

navigator.serviceWorker.addEventListener("message", async (event) => {
  const msg = event.data;
  const body = pending.get(msg?.key);
  if (!body) return;
  pending.delete(msg.key);

  if (msg.type !== "pull-ok") {
    body.textContent = msg.pending ? "still downloading in the background" : "missing";
    return;
  }

  // msg.stream is a LIVE ReadableStream transferred out of the worker. render()
  // decides whether to consume it progressively or collect it, by MIME.
  await render(body, msg);
});

function pull(meta, body) {
  body.replaceChildren();
  body.textContent = "pulling…";
  pending.set(meta.key, body);
  navigator.serviceWorker.controller?.postMessage({ type: "pull", key: meta.key });
}

/* ── boot ────────────────────────────────────────────────────────────────── */

$("ep-post").textContent = api("api/events");
$("ep-blob").textContent = api("api/blob/:key");

for (const id of ["ep-get", "ep-post", "ep-blob"]) {
  $(id).addEventListener("click", async (e) => {
    await navigator.clipboard.writeText(e.currentTarget.textContent).catch(() => {});
    e.currentTarget.dataset.copied = "true";
    setTimeout(() => delete e.currentTarget.dataset.copied, 1200);
  });
}

let durability = "unknown";

async function refreshStatus() {
  try {
    const { head, floor, outbox, storage } = await (await fetch(api("api/status"))).json();
    const pct = storage.quota ? Math.round((storage.usage / storage.quota) * 100) : 0;
    $("meta").textContent =
      `head ${head}${floor ? ` (from ${floor})` : ""} · ${pct}% of quota · ${durability}` +
      (outbox ? ` · ${outbox} queued` : "");
  } catch {
    // The stream will show the real problem; don't fight over the status line.
  }
}

/*
 * Cross-tab notification that does not depend on the service worker being alive.
 * If this tab's stream is dead but another tab appended, we still learn the log
 * moved and can resync. Note this is scoped to the storage partition, not
 * strictly the origin.
 */
if ("BroadcastChannel" in window) {
  const channel = new BroadcastChannel("stream-log");
  channel.addEventListener("message", (e) => {
    if (e.data?.type === "append" && e.data.id > cursor && es?.readyState !== EventSource.OPEN) {
      connect(cursor);
    }
    if (e.data?.type === "trimmed") refreshStatus();
  });
}

// clients.claim() can land before we attach the listener, so poll alongside it —
// waiting on the event alone hangs when activation is fast.
function awaitController(timeout = 3000) {
  if (navigator.serviceWorker.controller) return Promise.resolve(true);
  return new Promise((resolve) => {
    const settle = () => {
      clearInterval(poll);
      clearTimeout(cap);
      resolve(Boolean(navigator.serviceWorker.controller));
    };
    navigator.serviceWorker.addEventListener("controllerchange", settle, { once: true });
    const poll = setInterval(() => navigator.serviceWorker.controller && settle(), 60);
    const cap = setTimeout(settle, timeout);
  });
}

(async () => {
  if (!("serviceWorker" in navigator)) {
    setStatus("error", "no service worker support");
    return;
  }
  try {
    // updateViaCache:"none" — the worker imports handler.js and friends; without
    // this the HTTP cache is consulted for those imports and edits go unnoticed.
    // No update() call here: firing it while the first install is still in flight
    // aborts that install with an AbortError.
    await navigator.serviceWorker.register(api("sw.js"), {
      type: "module",
      updateViaCache: "none",
    });
    await navigator.serviceWorker.ready;

    if (!(await awaitController())) {
      // Activated too early to ever claim this page. One reload guarantees control.
      if (!sessionStorage.getItem("sw-reload")) {
        sessionStorage.setItem("sw-reload", "1");
        location.reload();
        return;
      }
      setStatus("error", "worker never took control");
      return;
    }
    sessionStorage.removeItem("sw-reload");

    // The log is the source of truth, so whether its bucket is evictable is not
    // a detail. A default bucket is "best-effort" and the browser may clear it
    // under pressure; persist() asks to be exempt. Chromium decides silently
    // from engagement history, Firefox prompts — so the answer matters and is
    // reported rather than assumed.
    const persisted =
      (await navigator.storage?.persisted?.().catch(() => false)) ||
      (await navigator.storage?.persist?.().catch(() => false));
    durability = persisted ? "persisted" : "evictable";

    setStatus("wait", "connecting");
    connect();
    refreshStatus();
    setInterval(refreshStatus, 5000);
  } catch (err) {
    setStatus("error", String(err));
  }
})();
