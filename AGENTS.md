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

5. **Compression and resumability are mutually exclusive.** gzip is stateful, so
   bytes appended after the compressor state is gone will not decode. `blobs.put`
   decides from `resumable` before the first byte, never retroactively.

6. **Events on the wire carry their id; signals do not.** A frame without `id:`
   inherits the previous `lastEventId`. Anything indexed by id must arrive as a
   *named* event (see `lagged`) so it never reaches `onmessage`.

7. **`requestAnimationFrame` does not fire in a hidden tab.** Anything scheduling
   render work needs a timer fallback.

8. **Adding a page module means adding it to `SHELL`, and bumping `VERSION`.**
   Nothing is cached opportunistically, on purpose: a response cached because it
   went past keeps being served after the file changes, and for a module that
   means running code no longer in the repo. It presents as the edit simply not
   happening. Anything absent from `SHELL` always comes from the network and so
   is unavailable offline.

9. **Cross-context coordination uses Web Locks, not a module-scope Set.** A Set
   lives exactly as long as the worker instance holding it, and every tab has
   its own. `payload:<key>` serialises payload writes; `stream-read-model`
   elects the one context allowed to open the on-disk SQLite database.

## Layers

```
page        src/app.js, src/render.js, src/query.js
contract    src/handler.js, src/router.js, src/sse.js     no worker globals
storage     src/log.js (IndexedDB), src/blobs.js (OPFS)
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
| resume / `Range` | kill a transfer mid-flight, confirm the retry requests `bytes=<have>-` and the result is byte-correct |
| compression | round-trip a compressible and an incompressible payload; check `size` vs `stored` |
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
| Retention size and quota response | `src/log.js` |
| Compression threshold, range slicing | `src/blobs.js` |
| Resumable threshold, Background Fetch threshold | `src/upstream.js` |
| Transport choice, handshake timeout, WS queue bound | `src/transport.js` |
| Reconnect policy and cursor | `src/app.js` |
| DOM batching and row cap | `src/app.js` |
| Retention scheduling, isolation, upstream config | `workers/service-worker.js` |
| Shell cache contents and version | `workers/service-worker.js` (`SHELL`, `VERSION`) |
