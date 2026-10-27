// A Service Worker that serves Swarm disks to the page through swarmblk's
// engine, compiled to WebAssembly (vendor/swarmblk.wasm, from
// cmd/swarmblk-wasm). Disks are named by the page:
//
//   GET  /swarm-disk/NAME?snapshot=REF&bee=URL[&rw=1]  the disk, with HTTP ranges, as any
//                                                      emulator reads a disk image over HTTP;
//                                                      REF is a reference or a lineage
//   PUT  /swarm-disk/NAME, X-Offset: BYTES             a write, answered once the engine holds it
//   GET  /swarm-disk/info/NAME?snapshot=REF&bee=URL[&rw=1]  opens the disk: its size, snapshot,
//                                                      lineage and writes since its checkpoint
//   POST /swarm-disk/checkpoint/NAME?batch=ID          checkpoints a writable disk
//   POST /swarm-disk/reset/NAME                        forgets a writable disk's local state
//   GET  /swarm-disk/trace/NAME                        the guest's reads so far, a boot trace
//   GET  /swarm-disk/stats                             the engine's counters
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
      new Response(String(err.message || err), { status: 502, headers: { "Content-Type": "text/plain" } })));
  }
});

// open opens the disk that a URL's path ends in, with its query's snapshot,
// Bee and writability.
function open(u) {
  const name = u.pathname.split("/").pop();
  const bee = u.searchParams.get("bee") || "http://localhost:1633";
  return self.swarmblkOpen(bee, name, u.searchParams.get("snapshot") || "", u.searchParams.get("rw") === "1");
}

async function serve(req, u) {
  await startEngine();
  const [, , route, name] = u.pathname.split("/");
  switch (route) {
    case "stats":
      return Response.json(await self.swarmblkStats());
    case "trace": {
      const trace = await self.swarmblkTrace(name);
      if (!trace) return new Response("that disk is not open", { status: 404 });
      return new Response(trace, { headers: { "Content-Type": "application/octet-stream" } });
    }
    case "info":
      return Response.json(await open(u));
    case "checkpoint":
      return Response.json(await self.swarmblkCheckpoint(name, u.searchParams.get("batch") || ""));
    case "reset":
      await self.swarmblkReset(name);
      return new Response(null, { status: 204 });
  }
  if (req.method === "PUT") {
    const data = new Uint8Array(await req.arrayBuffer());
    await self.swarmblkWrite(route, Number(req.headers.get("X-Offset")), data);
    return new Response(null, { status: 204 });
  }
  const disk = await open(u);
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
  const data = await self.swarmblkRead(route, start, end - start + 1);
  headers["Content-Length"] = String(data.byteLength);
  if (m) headers["Content-Range"] = `bytes ${start}-${end}/${disk.size}`;
  return new Response(data, { status: m ? 206 : 200, headers });
}
