# Durable stream

A static site with no backend. A service worker answers `/api/*` from inside the
browser, and the app talks to it as if it were a remote server.

The point isn't streaming. It's the inversion underneath: **the log is the source of
truth and the connection is disposable.** Reconnects, offline, multiple tabs, and the
worker being killed all stop being error cases.

```
┌─ page ─────────────────────────────────────────────┐
│  new EventSource("api/events")   fetch("api/blob/…") │
│  never names a transport                            │
└────────────────────────┬────────────────────────────┘
┌─ service worker ───────▼────────────────────────────┐
│  router     URLPattern → route                      │
│  log        IndexedDB, append-only, monotonic ids   │
│  blobs      OPFS, streamed in and out               │
│  fan-out    one log → every open tab                │
└─────────────────────────────────────────────────────┘
```

## Two lanes

Bytes do not belong in a text protocol. They're split:

| | Control plane | Data plane |
|---|---|---|
| Transport | SSE (`EventSource`) | transferable `ReadableStream` via `postMessage` |
| Carries | events, metadata, cursors | raw `Uint8Array` |
| Encoding | UTF-8 text | native binary |

`ReadableStream` is a [transferable object](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Transferable_objects)
and [`Client.postMessage`](https://developer.mozilla.org/en-US/docs/Web/API/Client/postMessage)
takes a transfer list, so the worker hands the page a *live* stream. No base64, no
33% overhead, no 3-byte chunk alignment, no reassembly, no sequence numbers.
Backpressure survives the transfer — the page controls how fast bytes come off disk.

## Storage

Neither store alone is right, and there is no transaction spanning them, so the log
is authoritative: a payload counts as real once its `blob` event commits.

| Data | Store | Why |
|---|---|---|
| Event log | IndexedDB | the autoIncrement key *is* the monotonic cursor; range queries |
| Binary payloads | OPFS | `createWritable()` is a `WritableStream`, so uploads `pipeTo` disk with backpressure and never buffer |
| UI prefs | localStorage | page-only — it doesn't exist in workers |

`FileSystemSyncAccessHandle` (the fast path) is `[Exposed=DedicatedWorker]` in the
spec, so it is unavailable here. The async OPFS API is what a service worker gets.

## Two findings worth knowing

**Chrome does not forward `Last-Event-ID` into a service worker's request.** It's a
forbidden request header. Verified in this app — the `caught-up` event reports what
the worker actually saw, and on auto-reconnect it's `absent`. The spec mechanism
works against a real server; it does not work when the worker serves the stream. So
the cursor rides in the query string (`?after=<id>`) and the client drives its own
reconnect. `EventSource`'s built-in retry is deliberately disabled — it would re-request
the same URL and silently replay the entire log on every drop.

**The worker is not a daemon.** `ServiceWorkerGlobalScope` state "is not persisted
across the termination/restart cycle." Every module-level variable — including the
set of open connections — evaporates. `waitUntil()` delays termination; it doesn't
prevent it. That's survivable only because the log is durable.

## Layout

```
index.html            entry — must stay at root for Pages
sw.js                 worker mount — must stay at root; scope is its own directory
src/
  app.js              page: consumes the contract, drives reconnection
  handler.js          routes; no worker globals, so it's testable standalone
  router.js           URLPattern matching
  sse.js              framing + parsing as TransformStreams
  log.js              IndexedDB append-only log
  blobs.js            OPFS blob store
styles/main.css
scripts/dev-server.ts
```

## Routes

| | |
|---|---|
| `GET /api/events?after=<id>` | SSE — replays from the cursor, then streams live |
| `POST /api/events` | append `{type, data}` |
| `GET /api/log` | whole log as JSON |
| `DELETE /api/log` | wipe |
| `PUT /api/blob/:key` | store a payload; announces it on the log |
| `GET /api/blob/:key` | stream it back over HTTP |
| `POST /api/kick` | end every open stream, to exercise resume |
| `GET /api/status` | head, blob keys, quota |

## Run

```
bun scripts/dev-server.ts
```

Then <http://localhost:4321>. The page is a viewer; produce from the console:

```js
stream.append("note", { text: "hi" })
stream.put(new File(["hello"], "a.txt", { type: "text/plain" }))
```

Open a second tab — one worker, one log, both live. Hit **drop connection** and watch
it resume at its cursor instead of replaying.

## Deploying to GitHub Pages

Works as-is: `.nojekyll` is present, all paths are relative, and `index.html` and
`sw.js` sit at the root so the worker's scope covers the site. Nothing here needs a
custom HTTP header — every response header that matters is set on a `Response`
object in [src/handler.js](src/handler.js), which is exactly why a header-less static
host can serve it.

## Verified

Run against Chrome headless, not asserted:

- worker takes control, serves `/api/*`
- append → event appears in every open tab
- `POST /api/kick` ends the stream → client resumes at its cursor, no gap, no replay
- 20 KB binary: page → OPFS → transferred stream → reassembled → **SHA-256 matches**
- two tabs share one log; an append in one appears in the other

## Known limits

- `crypto.subtle` has no streaming digest, so the integrity check reads the file
  once on each side. The transfer itself still streams.
- Log growth is unbounded. A real deployment needs compaction or a retention window.
- No auth. Any script on the origin can read the log.
