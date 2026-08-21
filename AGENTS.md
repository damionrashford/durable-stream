# AGENTS.md

Operating notes for changing this repo. The README explains what the system is;
this explains how to work on it without breaking invariants that are not
obvious from a single file.

## Invariants

Breaking any of these breaks the system silently rather than loudly.

1. **`sw.js` stays at the served root.** A service worker's scope is the
   directory it is served from, and widening it needs the
   `Service-Worker-Allowed` response header, which a static host cannot send.
   The file is a stub; the implementation is `workers/service-worker.js`.

2. **The log is the only source of truth.** Anything in a module-scope variable
   is lost when the worker is reaped. OPFS payloads and the SQL read model are
   both derived and both rebuildable from it.

3. **A payload is real only once its `blob` event commits.** No transaction
   spans OPFS and IndexedDB. Write bytes first, log second. An orphaned file is
   collectable garbage; a logged event with no bytes is unrecoverable.

4. **`respondWith()` must be called synchronously** in the `fetch` handler. Test
   the path first, then pass a promise.

5. **Payloads reach disk a whole block at a time, never in part.** Nothing a
   writable accepts lands until `close()`, so the error path closes rather than
   aborts — aborting would discard every complete block along with the broken
   one, and resume would never make progress. A landed file is therefore always
   block-aligned, which is what makes both the resume offset and the SHA-256
   snapshot in `src/blocks.js` well-defined.

6. **A key names a transfer; a hash names the bytes.** `blobs` is addressed by
   content, and the key → address mapping lives in the log's metadata. Anything
   reading a payload resolves `meta.sha256` first, and one object may back
   several keys — so nothing may delete by key, and `evictTo` orders objects by
   their *newest* reference, not their oldest.

7. **Do not rely on one `DecompressionStream` spanning concatenated gzip
   members.** It is runtime-dependent — Bun's does, Chromium's does not — and
   the spec promises neither. Members are decoded one at a time against recorded
   boundaries. Bun also truncates `Blob.slice().stream()` to the end of the blob,
   so slices destined for a decoder are read via `arrayBuffer()`.

8. **Events on the wire carry their id; signals do not.** A frame without `id:`
   inherits the previous `lastEventId`. Anything indexed by id must arrive as a
   *named* event (see `lagged`) so it never reaches `onmessage`.

9. **`requestAnimationFrame` does not fire in a hidden tab.** Anything scheduling
   render work needs a timer fallback.

10. **Adding a page module means adding it to `SHELL`, and bumping `VERSION`.**
   Nothing is cached opportunistically, on purpose: a response cached because it
   went past keeps being served after the file changes, and for a module that
   means running code no longer in the repo. It presents as the edit simply not
   happening. Anything absent from `SHELL` always comes from the network and so
   is unavailable offline.

11. **Cross-context coordination uses Web Locks, not a module-scope Set.** A Set
   lives exactly as long as the worker instance holding it, and every tab has
   its own. `payload:<key>` serialises payload writes; `stream-read-model`
   elects the one context allowed to open the on-disk SQLite database.

## Layers

```
page        src/app.js, src/render.js, src/query.js
contract    src/handler.js, src/router.js, src/sse.js     no worker globals
storage     src/log.js (IndexedDB), src/blobs.js + src/blocks.js (OPFS)
upstream    src/transport.js, src/upstream.js
mounts      sw.js → workers/service-worker.js, workers/query.js
```

`src/handler.js` and everything it imports must not touch `self`,
`registration`, `clients`, or `caches`. Platform access arrives through the
context object built in `workers/service-worker.js`. Keeping that boundary is
what makes the routes testable without a worker.

## Running and testing

```
python3 -m http.server 4321      # any static server; no build step
```

There is no test suite. Verification is done by driving a real browser, because
every interesting behaviour here is a browser behaviour. When changing anything
below, exercise it rather than reasoning about it:

| Change | Exercise |
|---|---|
| resume / `Range` | kill a transfer mid-flight, confirm the retry requests `bytes=<have>-` and that the finished payload's `sha256` matches an uninterrupted transfer of the same bytes |
| compression | round-trip a compressible and an incompressible payload; check `size` vs `stored`, and seek a compressed one |
| dedupe | store identical bytes under two keys; confirm one file in `objects/`, and that deleting one key leaves the other readable |
| retention | append past `RETENTION`, confirm `floor` advances and orphaned payloads are swept |
| streaming | confirm the first chunk arrives before the last is produced |
| DOM | test with the tab hidden; `visibilityState` is `hidden` in headless |
| shell cache | stop the server, reload, confirm the app still boots |

**Service worker updates are the main time sink.** Chrome frequently keeps
serving an old worker after imported modules change. The reliable escape is a
different origin — serve on a new port. `unregister()` plus reload is not
dependable, and a wedged profile survives `launch.ts nuke`.

## Conventions

- Plain ES modules, no build step, no dependencies. The only external asset is
  SQLite wasm, loaded lazily from a CDN on the first `sql()` call.
- Comments document the code and the constraint behind it. Not history, not what
  was tried, not what is absent.
- Configuration is a named constant at the top of the file that uses it —
  `UPSTREAM`, `ISOLATE`, `RETENTION`, `RESUMABLE_ABOVE`, `LARGE_PAYLOAD`.
- Experimental APIs are capability-detected with a working fallback:
  Background Fetch, Background Sync, `FileSystemHandle.remove()`, WebTransport,
  `CompressionStream`, SQLite's OPFS VFS.
- Prefer a stream to a buffer. If a payload has to be collected, the reason
  belongs in a comment.

## Where behaviour is decided

| Question | File |
|---|---|
| What a route returns | `src/handler.js` |
| Method matching, HEAD, 405 | `src/router.js` |
| Log retention count and quota response | `src/log.js` |
| Payload byte budget, pressure limit | `workers/service-worker.js`, `src/handler.js` |
| Compression threshold, dedupe, promotion | `src/blobs.js` |
| Block size, container format, range decoding | `src/blocks.js` |
| Resumable threshold, Background Fetch threshold | `src/upstream.js` |
| Transport choice, handshake timeout, WS queue bound | `src/transport.js` |
| Reconnect policy and cursor | `src/app.js` |
| DOM batching and row cap | `src/app.js` |
| Retention scheduling, isolation, upstream config | `workers/service-worker.js` |
| Shell cache contents and version | `workers/service-worker.js` (`SHELL`, `VERSION`) |
