// A Service Worker that serves Swarm disks to the page through swarmblk's
// engine, compiled to WebAssembly (vendor/swarmblk.wasm, from
// cmd/swarmblk-wasm). The page reads /swarm-disk/<snapshot>?bee=<Bee API URL>
// with HTTP range requests, as any emulator reads a disk image over HTTP; the
// engine fetches the chunks from Bee, checks each against its address, and
// keeps them in Cache Storage. /swarm-disk/stats answers with the engine's
// counters, and /swarm-disk/trace/<snapshot> with the guest's reads so far,
// as a boot trace.
importScripts("vendor/wasm_exec.js");

let engine = null; // a promise that the engine is running

function startEngine() {
  if (!engine) {
    engine = (async () => {
      const go = new Go();
      const wasm = await (await fetch("vendor/swarmblk.wasm", { cache: "no-cache" })).arrayBuffer();
      const { instance } = await WebAssembly.instantiate(wasm, go.importObject);
      go.run(instance); // runs until the worker stops
      while (!self.swarmblkRead) await new Promise(r => setTimeout(r, 10));
    })();
  }
  return engine;
}

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", e => e.waitUntil(self.clients.claim()));

self.addEventListener("fetch", e => {
  const u = new URL(e.request.url);
  if (u.origin === location.origin && u.pathname.startsWith("/swarm-disk/")) {
    e.respondWith(serve(e.request, u).catch(err =>
      new Response(String(err), { status: 502, headers: { "Content-Type": "text/plain" } })));
  }
});

async function serve(req, u) {
  await startEngine();
  if (u.pathname === "/swarm-disk/stats") {
    return Response.json(self.swarmblkStats());
  }
  if (u.pathname.startsWith("/swarm-disk/trace/")) {
    const trace = self.swarmblkTrace(u.pathname.split("/")[3]);
    if (!trace) return new Response("that disk is not open", { status: 404 });
    return new Response(trace, { headers: { "Content-Type": "application/octet-stream" } });
  }
  const snapshot = u.pathname.split("/")[2];
  const bee = u.searchParams.get("bee") || "http://localhost:1633";
  const disk = await self.swarmblkOpen(bee, snapshot);
  let start = 0, end = disk.size - 1;
  const m = /^bytes=(\d+)-(\d*)$/.exec(req.headers.get("Range") || "");
  if (m) {
    start = Number(m[1]);
    if (m[2]) end = Math.min(Number(m[2]), disk.size - 1);
  }
  if (start > end) {
    return new Response(null, { status: 416, headers: { "Content-Range": `bytes */${disk.size}` } });
  }
  const headers = { "Content-Type": "application/octet-stream", "Accept-Ranges": "bytes" };
  if (req.method === "HEAD") {
    headers["Content-Length"] = String(disk.size);
    return new Response(null, { headers });
  }
  const data = await self.swarmblkRead(snapshot, start, end - start + 1);
  headers["Content-Length"] = String(data.byteLength);
  if (m) headers["Content-Range"] = `bytes ${start}-${end}/${disk.size}`;
  return new Response(data, { status: m ? 206 : 200, headers });
}
