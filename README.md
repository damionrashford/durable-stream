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
│  router     URLPattern → route, HEAD, 405            │
│  log        IndexedDB, append-only, monotonic ids    │
│  blobs      OPFS, streamed, gzip'd, resumable        │
│  upstream   dials out; WebTransport or SSE           │
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
| Payloads | OPFS | `createWritable()` is a `WritableStream`, so uploads `pipeTo` disk and never buffer |
| Read model | SQLite wasm | derived, disposable, rebuilt from the log |

Retention trims the log to 5000 events and sweeps the payloads those events were the
only reference to, so both halves stay bounded. `QuotaExceededError` triggers a harder
trim and one retry.

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
  blobs.js                 OPFS store: compression, ranges, resumable writes
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

Transport is picked by a real connection attempt, not a feature check: WebTransport
first, SSE if the handshake fails or times out.

## Cross-origin isolation

`ISOLATE` in [workers/service-worker.js](workers/service-worker.js), **off by default.**

COOP/COEP are document response headers and a static host sends none — but a controlled
page's navigation passes through the worker, so the response can be re-wrapped.
Measured with it on: `crossOriginIsolated === true` and `SharedArrayBuffer` available
on GitHub Pages. The first load is never isolated; it begins on the next one.

It's off because the SQL worker fails to start under isolation, and nothing here needs
`SharedArrayBuffer` yet. Turn it on when something does, and fix that first.

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
- Retention is a count, not a size. A workload of large payloads can reach quota
  between passes, which surfaces as `507` rather than data loss.
- Cross-origin isolation and the SQL read model cannot both be on.

Working on this? See [AGENTS.md](AGENTS.md).
