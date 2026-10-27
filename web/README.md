# swarmblk in a browser tab

A web page that boots swarmblk snapshots' disks in a PC emulated in the browser by [v86](https://github.com/copy/v86). swarmblk's engine, compiled to WebAssembly (`cmd/swarmblk-wasm`), runs in a Service Worker (`sw.js`). It reads the disks chunk by chunk from a Bee node, checks every chunk against its address, and keeps chunks in the browser's cache, so that a disk once read boots without the node. A writable hard disk keeps the machine's writes in the browser, and a checkpoint puts them on Swarm as the next snapshot of a lineage. Results: [docs/results/phase2-browser-2026-10-27.md](../docs/results/phase2-browser-2026-10-27.md).

## Run it

```sh
web/build.sh            # fetches v86 and xterm.js, and builds the engine, into web/vendor/
python3 web/serve.py    # serves web/ on http://127.0.0.1:8099
```

Then open `http://127.0.0.1:8099/?snapshot=<reference>` in a browser on the same computer as Bee.

- Bee must accept the page's origin: its `cors-allowed-origins` setting, which Swarm Desktop sets to `*`.
- v86 emulates a 32-bit PC, so 64-bit guests do not boot. A bootable ISO serves well, padded to a power of two and imported: `cp alpine.iso alpine.raw && truncate -s 64M alpine.raw && swarmblk import alpine.raw`. Alpine Linux's x86 virt ISO boots to a login prompt in about two minutes; log in as root.
- Writes to the CD, or to any disk not given as `hda`, stay in the tab, and are gone when it closes.

## A writable disk and checkpoints

`&hda=SNAPSHOT` adds a hard disk through the engine, writable, beside the CD; a snapshot or a lineage, `0x<owner>/<topic>`, names where it starts. Any filesystem image will do, such as one made with `mkfs.ext4 -d dir data.raw 64M` and imported. In Alpine: `mount /dev/sda /mnt`, write, `umount /mnt`.

- **The writes stay in the browser.** The page passes each of v86's writes to the engine, in order, and the guest's write completes once the engine holds it. The engine keeps the writes in memory and in an ordered log in Cache Storage, and a reload replays the log. A tab that closes loses at most its last few writes, never one before another it kept.
- **A checkpoint puts them on Swarm.** With `&batch=ID` the page's button checkpoints the disk: the engine rebuilds the snapshot's tree with the writes, uploads the chunks that changed and the snapshot's manifest, stamped from the batch, waits until Bee has synced them, and publishes the snapshot as the next update of the disk's lineage (SPEC §8). The machine keeps running. The first checkpoint starts a lineage, and the next ones continue it.
- **The browser is the lineage's one writer.** The engine makes the lineage's key and keeps it, with the disk's state, in this browser profile's Cache Storage, for this page's origin. Clearing the site's data loses the key and the writes since the last checkpoint, but not the snapshots. Another browser that opens the lineage reads its latest update, and its own checkpoints start a lineage of their own.
- **The batch must be the Bee node's own.** Bee stamps the uploads, so a batch is used from one node only (SPEC §9, P2).
- **One tab at a time.** All the page's tabs in a browser share one engine, so a page holds a browser lock on its writable disk while it lives (the Web Locks API). A second tab with the same disk waits 5 s, time for a page that a reload replaces to let go, then says the disk is in use and does not start.
- **The disk remembers where it is.** Once the browser holds a disk, the page opens it from its own state, whatever `hda` says. The page's link forgets the browser's copy, writes and all, and starts again from `hda`.

## Parameters

| Parameter | Meaning |
|---|---|
| `snapshot` | the snapshot's reference |
| `bee` | the Bee API, by default `http://localhost:1633` |
| `drive` | `cdrom`, the default, or `hda` for a hard disk image, read-only |
| `hda` | a writable hard disk through the engine, beside the CD: a snapshot or a lineage |
| `batch` | the postage batch that checkpoints stamp with, one of the Bee node's own |
| `direct=1` | read the disk from Bee's `/bzz` instead of through the engine |
| `local=FILE` | boot a file in `web/` instead, without Swarm |
| `report=1` | test mode: post the screen, the serial console and the disk's read times to `serve.py` every 2 s |
| `type=A;B` | with `report`, type these lines at the serial console's login prompt |
| `record=1` | post the boot trace to `serve.py` once the serial console shows a login prompt |
| `checkpoint=1` | with `type`, checkpoint the hard disk 10 s after the last line |
| `tab=NAME` | with `report`, names the tab in its reports |
| `twin=1` | with `report`, open a second tab of the page once this one holds the hard disk, which must find it in use (headless Chromium needs `--disable-popup-blocking`) |

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
