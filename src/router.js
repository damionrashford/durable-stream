/*
 * URLPattern routing. Available in workers, so the same table works wherever the
 * handler is mounted — only the base path differs ("/repo-name/" on a project
 * page, "/" everywhere else).
 *
 * Two HTTP details the router owns rather than each handler:
 *
 *   HEAD is GET without a body. A response to HEAD "must" carry the headers GET
 *   would have sent, so it is dispatched to the GET handler and the body is
 *   dropped afterwards — never a separate implementation that can drift.
 *
 *   405 needs an Allow header listing what the path does support. That is only
 *   knowable here, where the whole table is visible.
 */

export function createRouter(base, routes) {
  const prefix = base.replace(/\/$/, "");
  const compiled = routes.map((route) => ({
    ...route,
    pattern: new URLPattern({ pathname: prefix + route.path }),
  }));

  return function match(request) {
    const url = new URL(request.url);
    // HEAD is answered by the GET handler; the caller strips the body.
    const method = request.method === "HEAD" ? "GET" : request.method;

    let pathMatched = null;
    for (const route of compiled) {
      const hit = route.pattern.exec(url);
      if (!hit) continue;
      pathMatched ??= hit;
      if (route.method === method) {
        return {
          handler: route.handler,
          params: hit.pathname.groups,
          bodyless: request.method === "HEAD",
        };
      }
    }

    // The path exists but not for this method — that is a 405, not a 404, and it
    // must say what is allowed.
    if (pathMatched) {
      const allow = compiled
        .filter((r) => r.pattern.exec(url))
        .flatMap((r) => (r.method === "GET" ? ["GET", "HEAD"] : [r.method]));
      return { allow: [...new Set(allow)] };
    }
    return null;
  };
}
