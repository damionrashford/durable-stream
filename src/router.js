/*
 * URLPattern routing. Available in workers, so the same table works in a service
 * worker and at the edge — only the base path differs ("/repo-name/" on GitHub
 * Pages, "/" everywhere else).
 */

export function createRouter(base, routes) {
  const compiled = routes.map((route) => ({
    ...route,
    pattern: new URLPattern({ pathname: base.replace(/\/$/, "") + route.path }),
  }));

  return function match(request) {
    const url = new URL(request.url);
    for (const route of compiled) {
      if (route.method !== request.method) continue;
      const hit = route.pattern.exec(url);
      if (hit) return { handler: route.handler, params: hit.pathname.groups, url };
    }
    return null;
  };
}
