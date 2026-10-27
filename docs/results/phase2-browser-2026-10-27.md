# A Swarm disk in a browser tab — 27 October 2026

First runs of a swarmblk disk under an emulator in a web page, on laptop A with node A's Bee, in headless Chromium 156 and by hand in Brave, on 27 and 28 October.

## Setup

- **Emulator:** v86 0.5.472 (BSD-2-Clause), which emulates a 32-bit PC in WebAssembly. 64-bit guests do not run on it.
- **Disks:** v86's test Linux (`linux4.iso`, 7.7 MB) and Alpine Linux 3.24.3's 32-bit virt ISO (51 MB), each padded to 64 MiB and imported with `swarmblk import`: snapshots `4c0c744d…` (1,828 chunks) and `ad40e39c…` (12,180 chunks).
- **Two ways to the disk:**
  - *direct:* v86 reads `/bzz/<snapshot>/disk.raw` from Bee with HTTP range requests.
  - *engine:* `cmd/swarmblk-wasm`, swarmblk's engine compiled to WebAssembly, runs in a Service Worker. v86 reads `/swarm-disk/<snapshot>` from the page's own origin, and the engine answers from the disk tree: it fetches chunks with `GET /chunks`, checks each against its address, and keeps them in the browser's Cache Storage and in memory. It reads ahead of sequential reads, prefetches from the snapshot's boot trace, and records the guest's reads as a new trace.
- The page shows the guest's serial console in xterm.js and its screen. The page and the Service Worker are in [`web/`](../../web/README.md), whose `build.sh` fetches v86 and xterm.js.

## Results

| Disk | Path | Browser cache | Prompt after | Notes |
|---|---|---|---|---|
| test Linux | direct | — | 12 s | 126 range reads, median 4 ms |
| test Linux | engine | cold | 22 s | 1,818 chunks fetched and checked |
| Alpine | engine | cold | not within 240 s | about 1 chunk in 10 came from the network through Bee, in about 300 ms |
| Alpine | engine | warm | 132 s | 9,538 chunks from the cache, 28 from Bee; the disk waits added up to 16 s |
| Alpine | engine | warm, Bee unreachable | 130 s | every chunk from the cache |

A warm boot takes about as long as booting the ISO from a local file. The emulated CPU sets its pace, not the disk.

**Reads.** v86 reads one piece at a time, so each read's latency adds to the boot. The engine's first version took a median of 856 ms per 64 KiB read, mostly writing to Cache Storage and checking it for chunks it did not have. Writing in the background, keeping recent chunks in memory and a list of cached ones, and reading ahead after two sequential reads brought the median to 23–65 ms.

**Direct reads need a workaround.** v86 sends `X-Accept-Encoding: identity` with range requests. Across origins, the browser first asks Bee whether it may (a CORS preflight), and Bee does not allow that header, so every read failed until the page dropped it. Through the engine, the requests stay on the page's origin.

## Boot traces

The engine recorded Alpine's boot in the browser: 52 runs, 8,802 blocks (34.4 MiB). `swarmblk checkpoint -prefetch` stored the trace with the same disk, uploading 7 chunks, as snapshot `e396f350…`, update 0 of a new lineage.

Cold boots in fresh browser profiles, with Bee by then holding the chunks: a sample of 40 came in 34–47 ms each, where before one in ten had taken about 300 ms.

| Snapshot | Prompt after | Time the disk waited | Median read |
|---|---|---|---|
| with the trace | 158 s | 18.4 s | 23 ms |
| without | 166 s | 47.1 s | 65 ms |

The trace cut the disk waits by 61%, but the boot by only 8 s: the emulator sets the pace here, and the prefetch's own work competes with it for this laptop's CPU. Where Bee must retrieve chunks from the network, a trace should matter more, as it did natively (Alpine through node B: 17–18 min without, 3.5 min with). In a browser, though, a page reaches one host over at most six connections at once, which caps how fast chunks arrive from a node that retrieves them from the network. Freedom's `bzz://` handler, which serves pages from its node through Electron rather than the page's HTTP connections, might lift that cap, and so might several host names for one Bee. Neither was tried.

## Checkpoints from the tab

On 28 October the engine got writable disks and checkpoints. The page booted Alpine from the traced CD and added a 64 MiB ext4 disk as `hda`, imported as snapshot `ab55c826…` (22 chunks). The page passes each of v86's writes to the engine in order, and the guest's write completes once the engine holds it; the engine keeps the writes in memory and in an ordered log in Cache Storage. A checkpoint button runs SPEC §8 in the browser: rebuild, upload through node A's Bee with the test batch, wait for sync, publish. The lineage key is made in the browser and kept there.

Three runs in one browser profile, each typing at the serial console:

| Run | In the guest | Checkpoint |
|---|---|---|
| 1 | mount the disk, write `hello.txt`, `sync`, unmount | 1,039 blocks written, 19 chunks uploaded in 7.9 s: snapshot `f3d0cbdb…`, update 0 of a new lineage `0x393d2750…/b1561b05…` |
| 2 | mount, write `second.txt`, `sync`, unmount; the browser is then killed | none: 16 blocks only in the browser |
| 3 | mount: `second.txt` is there, replayed from the log; unmount | 20 blocks, 23 chunks in 4.0 s: snapshot `0bed6599…`, update 1 |

Most of run 1's writes were zeros or blocks already on Swarm, which add no chunks. `swarmblk export` of the lineage, natively and with an empty state directory, gave update 1. Its filesystem passed `e2fsck -fn`, held both files with the guest's text, and its `meta.json` named update 0 as its parent.
