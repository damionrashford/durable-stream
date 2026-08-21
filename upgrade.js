(async () => {
  const reg = await navigator.serviceWorker.getRegistration();
  await reg.update();
  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    const caches_ = await caches.keys();
    if (caches_.includes("shell-v15")) break;
    await new Promise(r => setTimeout(r, 300));
  }
  return { caches: await caches.keys(), waiting: Boolean(reg.waiting), active: reg.active?.state };
})()
