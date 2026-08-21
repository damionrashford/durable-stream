/*
 * Registration stub.
 *
 * A service worker's scope defaults to the directory it is served from, and
 * widening it needs the Service-Worker-Allowed response header, which a static
 * host cannot send. So this file has to sit at the served root to control the
 * whole site — but the implementation belongs with the other workers.
 */
import "./workers/service-worker.js";
