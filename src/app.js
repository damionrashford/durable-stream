/*
 * Page side. It talks to /api/* and never learns which runtime answered — service
 * worker today, edge worker later, same code.
 *
 * Control plane: EventSource over /api/events. Text, resumable, auto-reconnecting.
 * Data plane:    transferable ReadableStreams over postMessage. Binary, backpressured.
 */

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

function row(kind, id, name, body) {
  if (id) {
    if (rendered.has(id)) return null;
    rendered.add(id);
  }
  const li = document.createElement("li");
  li.dataset.kind = kind;
  for (const [cls, text] of [["id", id], ["name", name]]) {
    const span = document.createElement("span");
    span.className = cls;
    span.textContent = text;
    li.append(span);
  }
  const slot = document.createElement("span");
  slot.className = "body";
  if (body instanceof Node) slot.append(body);
  else slot.textContent = body ?? "";
  li.append(slot);
  feed.append(li);
  li.scrollIntoView({ block: "nearest" });
  return slot;
}

const bytes = (n) =>
  n < 1024 ? `${n} B` : n < 1048576 ? `${(n / 1024).toFixed(1)} KB` : `${(n / 1048576).toFixed(1)} MB`;

async function sha256(buffer) {
  const digest = await crypto.subtle.digest("SHA-256", buffer);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/* ── control plane ───────────────────────────────────────────────────────── */

let es = null;
let cursor = 0; // highest id this tab has seen; the resume point
let retry = null;

const advance = (e) => {
  const id = Number.parseInt(e.lastEventId, 10);
  if (Number.isFinite(id) && id > cursor) cursor = id;
};

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
  es = new EventSource(api(`api/events?after=${from}`));

  es.addEventListener("open", () => setStatus("live", "live"));

  es.addEventListener("caught-up", (e) => {
    const { resumeFrom, through, sawHeader } = JSON.parse(e.data);
    row(
      "meta",
      "",
      "caught-up",
      `resumed at ${resumeFrom} → ${through} · Last-Event-ID header: ${sawHeader ?? "absent"}`,
    );
    setStatus("live", "live");
  });

  es.addEventListener("note", (e) => {
    advance(e);
    row("event", e.lastEventId, "note", JSON.parse(e.data)?.text);
  });

  es.addEventListener("blob", (e) => {
    advance(e);
    const meta = JSON.parse(e.data);
    const slot = row("blob", e.lastEventId, "blob", "");
    if (!slot) return; // already rendered
    const btn = document.createElement("button");
    btn.className = "link";
    btn.textContent = `${meta.name} · ${bytes(meta.size)} · pull`;
    btn.addEventListener("click", () => pull(meta, slot));
    slot.append(btn);
  });

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
  const slot = pending.get(msg?.key);
  if (!slot) return;
  pending.delete(msg.key);

  if (msg.type !== "pull-ok") {
    slot.textContent = "missing";
    return;
  }

  // msg.stream is a LIVE ReadableStream that was transferred out of the worker.
  // Response() drains it; the bytes were never text.
  const blob = await new Response(msg.stream).blob();
  const ok = msg.sha256 ? (await sha256(await blob.arrayBuffer())) === msg.sha256 : null;

  slot.replaceChildren();
  const verdict = document.createElement("span");
  verdict.className = ok === false ? "bad" : "good";
  verdict.textContent = ok === null ? `${bytes(blob.size)}` : ok ? `${bytes(blob.size)} · hash ok` : "HASH MISMATCH";
  slot.append(verdict);

  const url = URL.createObjectURL(blob);
  if ((msg.mime ?? "").startsWith("image/")) {
    const img = document.createElement("img");
    img.src = url;
    img.alt = msg.name ?? "";
    slot.append(img);
  } else {
    const a = document.createElement("a");
    a.href = url;
    a.download = msg.name ?? msg.key;
    a.textContent = "download";
    a.className = "link";
    slot.append(a);
  }
});

function pull(meta, slot) {
  slot.replaceChildren();
  slot.textContent = "pulling…";
  pending.set(meta.key, slot);
  navigator.serviceWorker.controller?.postMessage({ type: "pull", key: meta.key });
}

/* ── producing ───────────────────────────────────────────────────────────── */

async function upload(file) {
  const key = crypto.randomUUID();
  // SubtleCrypto has no streaming digest, so this one read is unavoidable if we
  // want an end-to-end integrity check. The upload itself still streams.
  const hash = await sha256(await file.arrayBuffer());

  // A File is a Blob, so fetch streams it as the body — no duplex flag needed and
  // nothing is buffered on the way to OPFS.
  await fetch(api(`api/blob/${key}`), {
    method: "PUT",
    headers: { "x-name": file.name, "x-mime": file.type || "application/octet-stream", "x-sha256": hash },
    body: file,
  });
  // No local render: the event comes back over the stream, same as every other tab.
}

$("file").addEventListener("change", (e) => {
  for (const file of e.target.files) upload(file);
  e.target.value = "";
});

const drop = $("drop");
for (const type of ["dragenter", "dragover"]) {
  drop.addEventListener(type, (e) => {
    e.preventDefault();
    drop.dataset.over = "true";
  });
}
for (const type of ["dragleave", "drop"]) {
  drop.addEventListener(type, () => delete drop.dataset.over);
}
drop.addEventListener("drop", (e) => {
  e.preventDefault();
  for (const file of e.dataTransfer.files) upload(file);
});

$("ping").addEventListener("click", () =>
  fetch(api("api/events"), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ type: "note", data: { text: `hello from a tab at ${new Date().toLocaleTimeString()}` } }),
  }),
);

$("replay").addEventListener("click", () => {
  feed.replaceChildren();
  rendered.clear();
  cursor = 0;
  connect(0);
});

// Killed from the worker side. Ending the stream at the source is the realistic
// failure — a dropped connection, not a deliberate close — and it exercises the
// resume path rather than the shutdown path.
$("kill").addEventListener("click", async () => {
  row("meta", "", "killed", "stream ended at the source — watch it resume from its cursor");
  await fetch(api("api/kick"), { method: "POST" });
});

$("wipe").addEventListener("click", async () => {
  await fetch(api("api/log"), { method: "DELETE" });
  feed.replaceChildren();
  rendered.clear();
  cursor = 0;
  connect(0);
  refreshStorage();
});

/* ── boot ────────────────────────────────────────────────────────────────── */

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

async function refreshStorage() {
  const res = await fetch(api("api/status"));
  const { head, storage } = await res.json();
  $("storage").textContent =
    `head ${head} · ${bytes(storage.usage)} of ${bytes(storage.quota)} · ` +
    (storage.persisted ? "persisted" : "evictable");
}

(async () => {
  if (!("serviceWorker" in navigator)) {
    setStatus("error", "no service worker support");
    return;
  }
  try {
    // Module worker: sw.js imports handler.js, the same file the edge runs.
    // updateViaCache:"none" — the worker imports handler.js and friends; without
    // this the HTTP cache is consulted for those imports and edits go unnoticed.
    const reg = await navigator.serviceWorker.register(api("sw.js"), {
      type: "module",
      updateViaCache: "none",
    });
    reg.update().catch(() => {});
    await navigator.serviceWorker.ready;

    // clients.claim() can land before we attach the listener, so poll alongside it
    // rather than waiting on the event alone — otherwise a fast activation hangs here.
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

    // Ask the browser not to evict the log under storage pressure.
    await navigator.storage?.persist?.().catch(() => {});

    setStatus("wait", "connecting");
    connect();
    await refreshStorage();
    setInterval(refreshStorage, 5000);
  } catch (err) {
    setStatus("error", String(err));
  }
})();
