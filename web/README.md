# swarmblk in a browser tab

A web page that boots a swarmblk snapshot's disk in a PC emulated in the browser by [v86](https://github.com/copy/v86). swarmblk's engine, compiled to WebAssembly (`cmd/swarmblk-wasm`), runs in a Service Worker (`sw.js`). It reads the disk chunk by chunk from a Bee node, checks every chunk against its address, and keeps chunks in the browser's cache, so that a disk once read boots without the node. First results: [docs/results/phase2-browser-2026-10-27.md](../docs/results/phase2-browser-2026-10-27.md).

## Run it

```sh
web/build.sh            # fetches v86 and xterm.js, and builds the engine, into web/vendor/
python3 web/serve.py    # serves web/ on http://127.0.0.1:8099
```

Then open `http://127.0.0.1:8099/?snapshot=<reference>` in a browser on the same computer as Bee.

- Bee must accept the page's origin: its `cors-allowed-origins` setting, which Swarm Desktop sets to `*`.
- v86 emulates a 32-bit PC, so 64-bit guests do not boot. A bootable ISO serves well, padded to a power of two and imported: `cp alpine.iso alpine.raw && truncate -s 64M alpine.raw && swarmblk import alpine.raw`. Alpine Linux's x86 virt ISO boots to a login prompt in about two minutes; log in as root.
- Writes stay in the tab, and are gone when it closes.

## Parameters

| Parameter | Meaning |
|---|---|
| `snapshot` | the snapshot's reference |
| `bee` | the Bee API, by default `http://localhost:1633` |
| `drive` | `cdrom`, the default, or `hda` for a hard disk image |
| `direct=1` | read the disk from Bee's `/bzz` instead of through the engine |
| `local=FILE` | boot a file in `web/` instead, without Swarm |
| `report=1` | test mode: post the screen, the serial console and the disk's read times to `serve.py` every 2 s |
| `type=A;B` | with `report`, type these lines at the serial console's login prompt |
| `record=1` | post the boot trace to `serve.py` once the serial console shows a login prompt |

## Boot traces

The engine records the blocks a boot reads. The page's link downloads them as a boot trace (SPEC §6). Store the trace with the snapshot, and a browser that opens the new snapshot fetches those blocks in parallel from the start:

```sh
swarmblk restore <snapshot> trace.qcow2
swarmblk checkpoint -prefetch <snapshot>.trace trace.qcow2
```

## Tests

`web/coldrun.sh NAME SNAPSHOT` boots a snapshot in headless Chromium with an empty profile and reports when the login prompt came, where the chunks came from, and how long the machine waited for its disk. `serve.py` must be running.

## Licenses

The files here are under swarmblk's license. `build.sh` fetches v86 (BSD-2-Clause), SeaBIOS and its VGA BIOS (LGPL-3.0) and xterm.js (MIT), with their license files, into `web/vendor/`.
