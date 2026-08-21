# Durable stream

A static site with no backend. A service worker answers `/api/*` from inside the
browser, and the app talks to it as if it were a remote server.

The point isn't streaming. It's the inversion underneath: **the log is the source of
truth and the connection is disposable.** Reconnects, offline, multiple tabs, and the
worker being killed all stop being error cases.

Live: <https://damionrashford.github.io/durable-stream/>

```
┌─ page ──────────────────────────────────────────────┐
│  EventSource("api/events")   fetch("api/blob/…")     │
│  sql("select …")             never names a transport │
└─────────────────────────┬────────────────────────────┘
┌─ service worker ────────▼────────────────────────────┐
│  shell      Cache Storage — the app loads with no net  │
│  router     URLPattern → route, HEAD, 405            │
│  log        IndexedDB, append-only, monotonic ids    │
│  blobs      OPFS, content-addressed, block-gzip'd    │
│  upstream   dials out; WebTransport, WS(Stream), SSE  │
│  deferred   sync + backgroundfetch                   │
└──────────────────────────────────────────────────────┘
```

## Two lanes

Bytes do not belong in a text protocol, so they're split:

| | Control plane | Data plane |
|---|---|---|
| Transport | SSE (`EventSource`) | transferable `ReadableStream` via `postMessage` |
| Carries | events, metadata, cursors | raw `Uint8Array` |

`ReadableStream` is a [transferable object](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Transferable_objects)
and [`Client.postMessage`](https://developer.mozilla.org/en-US/docs/Web/API/Client/postMessage)
takes a transfer list, so the worker hands the page a *live* stream. No base64, no 33%
overhead, no chunk alignment, no reassembly. Backpressure survives the transfer.

## Rules that aren't obvious

Each of these came from something that broke.

**Compression and resumability are mutually exclusive.** gzip is stateful, so bytes
appended after the compressor state is gone don't decode. The choice is made before
the first byte: payloads ≥1 MB are stored raw and can resume, smaller ones are
compressed because restarting them is cheap.

**Seek is useless without `Range`.** OPFS can write at byte 6,000,000, but the server
has to send from there. A `200` instead of `206` means the range was ignored, so the
offset resets rather than appending a second copy onto the prefix.

**Chrome does not forward `Last-Event-ID` into a service worker's request** — it's a
forbidden request header. The spec resume path silently doesn't apply when the worker
serves the stream, so the cursor rides in `?after=<id>` and the client drives its own
reconnect. `EventSource`'s built-in retry is disabled deliberately; it would replay the
whole log on every drop.

**`requestAnimationFrame` doesn't fire in a hidden tab.** Batching DOM writes on frames
alone means a backgrounded tab renders nothing forever. Frames when visible, a timer
when not.

**A slow reader gets dropped, not queued.** `desiredSize` going negative means the
consumer can't keep up; because the log is durable we drop and send a `lagged` marker,
and the client resyncs from its cursor. Memory stays bounded and nothing is lost.

**The worker is not a daemon.** `ServiceWorkerGlobalScope` state "is not persisted
across the termination/restart cycle." Every module-level variable evaporates.
`waitUntil()` delays termination; it doesn't prevent it.

## Storage

No transaction spans IndexedDB and OPFS, so the log is authoritative: a payload counts
as real once its `blob` event commits.

| Data | Store | Why |
|---|---|---|
| Event log | IndexedDB | the autoIncrement key *is* the cursor; range queries; reachable from the worker |
| Payloads | OPFS | `createWritable()` is a `WritableStream`, so uploads reach disk without buffering |
| Identity | streaming SHA-256 | standard digest, so an upstream can verify it with any tool — and the address a payload is stored under |
| Read model | SQLite wasm | derived, disposable, rebuilt from the log |
| App shell | Cache Storage | so there is something to read the log *with* offline |

Payloads are stored by content hash, so the same bytes announced under three keys cost
one copy. Keys map to addresses in the log; objects are refcounted by reachability
rather than a counter, so the last live key referring to a payload is what releases it.

Anything compressible is stored as a sequence of independently decodable 1 MiB gzip
members with their boundaries recorded, which is what lets a compressed payload both
resume from its prefix and serve a byte range — see `src/blocks.js`. A block boundary is
also a SHA-256 block boundary, so a transfer that dies can carry its digest state
forward and still arrive at the standard hash of the whole payload.

Retention trims the log to 5000 events, prunes the metadata those events justified,
sweeps payloads no surviving event references,
and evicts oldest-first to hold payloads under a byte budget — a count says nothing
about size, so both are enforced. A write above 80% of quota reclaims space before
trying, which turns a failure between scheduled passes into a slower write that
succeeds. `QuotaExceededError` still triggers a harder trim and one retry.

## Layout

```
index.html                 entry
sw.js                      registration stub — must be at the root (see below)
workers/
  service-worker.js        routes, data plane, sync, background fetch
  query.js                 SQLite read model (dedicated worker)
src/
  app.js                   page: consumes the contract, drives reconnection
  handler.js               routes; no worker globals, so testable alone
  router.js                URLPattern, HEAD dispatch, 405 + Allow
  sse.js                   framing and parsing as TransformStreams
  log.js                   IndexedDB log, retention, outbox, BroadcastChannel
  blobs.js                 OPFS store: content addressing, dedupe, resumable writes
  blocks.js                block-wise gzip container: resumable *and* seekable
  transport.js             upstream: WebTransport with SSE fallback
  upstream.js              frame routing, cursor, Background Fetch
  render.js                by MIME: streaming text, ranged media, download
  query.js                 page-side client for the read model
styles/main.css
```

`sw.js` stays at the root because a worker's scope defaults to its own directory and
widening it needs `Service-Worker-Allowed`, which a static host cannot send.

## Routes

| | |
|---|---|
| `GET /api/events?after=<id>` | SSE — replays from the cursor, then streams live |
| `POST /api/events` | append `{type, data}`; queues upstream if one is configured |
| `PUT /api/blob/:key` | store a payload; announces it on the log |
| `GET /api/blob/:key` | stream it back; `Range`, `ETag`, `304`, `416` |
| `GET /api/log` | whole log as JSON, to backfill the read model |
| `GET /api/status` | head, floor, outbox depth, storage durability |

`HEAD` works on any `GET`. A known path with an unknown method returns `405` with
`Allow`.

## Run

```
python3 -m http.server 4321
```

Then <http://localhost:4321>. The page is a viewer; produce from the console:

```js
await fetch("api/events", { method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ type: "note", data: { text: "hi" } }) })

await sql("select type, count(*) n from events group by type order by n desc")
```

Open a second tab — one worker, one log, both live.

## Connecting a real server

Set `UPSTREAM` in [workers/service-worker.js](workers/service-worker.js). The page's
`/api/events` is resolved by the worker and unreachable from the network; a server
cannot push into it. The worker dials out instead and appends what arrives to the log
every tab already reads — one socket feeding N tabs.

A `ws://` or `wss://` URL selects the WebSocket transport; otherwise WebTransport is
attempted and SSE takes over if the handshake fails or times out.

WebSocket is worth reaching for when an upstream will not set CORS headers, because
CORS does not govern it — the server sees `Origin` and decides. It also carries binary,
so a payload arrives on the same socket instead of needing the second HTTP request the
SSE path makes, and it sits outside the six-connection per-origin cap.

`WebSocketStream` is used where it exists, because it adds the one thing plain
WebSocket lacks: not reading stops the sender. Against a server flooding 64 KB messages
while the consumer took one every 200 ms, the sender managed 109 messages and was
blocked 261 times; on plain WebSocket it sent 15,296 and was never blocked, and the
difference sat in the tab's memory. It is non-standard and Chromium-only, so plain
WebSocket stays as the fallback and bounds its queue instead, closing the socket past
the bound so the reconnect resumes from the stored cursor.

## Cross-origin isolation

`ISOLATE` in [workers/service-worker.js](workers/service-worker.js), **off by default.**

COOP/COEP are document response headers and a static host sends none — but a controlled
page's navigation passes through the worker, so the response can be re-wrapped.
Measured with it on: `crossOriginIsolated === true` and `SharedArrayBuffer` available
on GitHub Pages. The first load is never isolated; it begins on the next one.

Both things isolation used to exclude are handled. A module worker loaded from a URL
fails to start under it, reporting an error with every field empty, so the SQL worker is
built from a blob of its own source instead. And a cross-origin iframe is refused unless
it carries COEP of its own, so a relay needs the `credentialless` attribute, which lifts
that requirement in exchange for an ephemeral, cookie-less context.

It stays off because nothing here needs `SharedArrayBuffer`, not because anything is
blocked by it.

## Verified, not asserted

Run against headless Chrome:

- resume: a server dying at 40% of 2 MB → retry requests `bytes=838860-`, final file
  2,097,152 bytes byte-correct across the seam
- compression: 10 KB text stored as 82 B, round-tripped exactly; binary left alone
- data plane: payload → OPFS → transferred stream → rendered; media loads from
  the ranged URL instead
- HTTP: `206`, suffix ranges, `416`, `304`, `HEAD` with empty body, `405` with `Allow`
- log: trim at head 300 leaving floor 206; outbox queued with the sync tag registered
- multi-tab: a broadcast crossing two tabs; one log, two readers
- SQLite: `opfs-sahpool`, 3.53.0, `GROUP BY` over the log

## Known limits

- No auth. The origin is the only boundary; any script on it can read the log.
- A payload above the free quota cannot be stored at all, and there is no way to
  ask for more: OPFS, IndexedDB, Cache Storage and every storage bucket draw on
  one origin allowance, and `estimate()` reports the whole of it. Reaching the
  user's own disk needs the File System Access API and a user gesture.
- Retention is enforced two ways — a count on the log and a byte budget on
  payloads — but a single payload larger than the remaining quota still cannot
  be stored, and answers `507`.
- Cross-origin isolation is off by default because nothing here needs
  `SharedArrayBuffer`, not because anything is excluded by it.

Working on this? See [AGENTS.md](AGENTS.md).
