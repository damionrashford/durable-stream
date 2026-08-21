// Local dev server. `bun scripts/dev-server.ts` → http://localhost:4321
// Static files only, no /api routes — the service worker provides those.
const ROOT = new URL("../", import.meta.url).pathname;

const TYPES: Record<string, string> = {
  html: "text/html; charset=utf-8",
  css: "text/css; charset=utf-8",
  js: "text/javascript; charset=utf-8",
  json: "application/json; charset=utf-8",
};

Bun.serve({
  port: 4321,
  async fetch(req) {
    const { pathname } = new URL(req.url);
    const rel = pathname === "/" ? "index.html" : pathname.slice(1);
    const file = Bun.file(ROOT + rel);
    if (!(await file.exists())) return new Response("not found", { status: 404 });

    const ext = rel.includes(".") ? rel.split(".").pop()! : "html";
    // Bun.file() as a body adds Content-Disposition, which makes Chrome download
    // the page instead of rendering it. Send bytes instead.
    return new Response(await file.bytes(), {
      headers: {
        "content-type": TYPES[ext] ?? "application/octet-stream",
        // Never cache during development; stale sw.js is the #1 time sink here.
        "cache-control": "no-store",
      },
    });
  },
});

console.log("http://localhost:4321");
