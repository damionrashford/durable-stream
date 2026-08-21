/*
 * Page side. It talks to /api/* and never touches the worker directly, except on
 * the data plane — where the worker transfers a live ReadableStream over
 * postMessage rather than encoding bytes into the text stream.
 */

import { render } from "./render.js";
import { createQueryClient } from "./query.js";

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

/** Rows kept in the DOM. The log retains far more; this is a viewport, not storage. */
const MAX_ROWS = 500;

/*
 * Replaying a log delivers hundreds of events in a burst, one message at a time.
 * Appending each one directly would mean a layout pass per event. Rows are staged
 * in a DocumentFragment and inserted once per frame instead — a fragment is not
 * part of the document, so building it costs nothing.
 */
let staged = null;
let scheduled = 0;

/*
 * requestAnimationFrame does not fire in a hidden tab, so scheduling on it alone
 * means a backgrounded tab renders nothing and stages rows forever. Frames when
 * visible, a timer when not, and a flush the moment visibility returns.
 */
function schedule() {
  if (scheduled) return;
  scheduled =
    document.visibilityState === "visible" ? requestAnimationFrame(flush) : setTimeout(flush, 250);
}

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") flush();
});

function flush() {
  scheduled = 0;
  if (!staged) return;
  feed.append(staged);
  staged = null;

  // Trim from the front. Ids only grow, so the oldest rows are the first ones.
  while (feed.childElementCount > MAX_ROWS) feed.firstElementChild.remove();

  // Once per flush rather than once per row: scrollIntoView forces layout.
  feed.lastElementChild?.scrollIntoView({ block: "nearest" });
}

function row(id, type) {
  if (rendered.has(id)) return null;
  rendered.add(id);
  // Bounded alongside the DOM: replay only moves forward from a cursor, so an id
  // old enough to be forgotten here is old enough never to arrive again.
  if (rendered.size > MAX_ROWS * 4) {
    for (const old of rendered) {
      rendered.delete(old);
      if (rendered.size <= MAX_ROWS * 2) break;
    }
  }

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

  staged ??= new DocumentFragment();
  staged.append(li);
  schedule();
  return body;
}

/*
 * One listener for every payload button, on the container.
 *
 * A listener per row would mean a closure per row held alive by the DOM; with the
 * feed trimming and refilling, delegation keeps that at exactly one.
 */
feed.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-pull]");
  if (!button) return;
  pull(button.dataset.pull, button.closest("li").querySelector(".body"));
});

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
    sqlClient.push({ id, type, data });

    // The worker dropped events rather than queue them for a reader that fell
    // behind. Nothing is lost — the log has them — so resync from our cursor.
    if (type === "lagged") return void connect(cursor);

    const body = row(e.lastEventId, type);
    if (!body) return;

    if (type === "blob") {
      const btn = document.createElement("button");
      btn.className = "link";
      btn.dataset.pull = data.key; // delegated; no per-row listener
      btn.textContent = `${data.name} · ${data.size} B · pull`;
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

function pull(key, body) {
  body.replaceChildren();
  body.textContent = "pulling…";
  pending.set(key, body);
  navigator.serviceWorker.controller?.postMessage({ type: "pull", key });
}

/* ── boot ────────────────────────────────────────────────────────────────── */

/*
 * SQL over the log, on demand.
 *
 *   await sql("select type, count(*) n from events group by type order by n desc")
 *
 * Derived, never authoritative: SQLite reaches OPFS only through a dedicated
 * worker, and the service worker cannot spawn one, so the log stays in IndexedDB
 * where the worker can serve it. Nothing loads until the first call.
 */
const sqlClient = createQueryClient({
  backfill: async () => (await (await fetch(api("api/log"))).json()).events,
  base: api("api/log"),
});
globalThis.sql = (text, params) => sqlClient.query(text, params);
globalThis.sql.status = () => sqlClient.status();

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
