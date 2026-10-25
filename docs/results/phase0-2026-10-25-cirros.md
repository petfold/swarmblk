# Phase 0 dry run — 25 October 2026 (CirrOS)

A dry run of the [runbook](../PHASE0.md) with a 128 MiB CirrOS image instead of the 8 GiB Ubuntu image, to test the tools and the setup before buying the Phase 0 batch. The numbers are indicative only. The image is small, there is one sample per variant (two for cold-la and cold-nola), and the same chunks were read many times through both nodes, so later runs could meet warmer caches along the retrieval paths.

## Setup

- **Node A:** Bee 2.8.3 light node on the machine that runs QEMU, with Gnosis RPC `rpc.gnosischain.com`.
- **Node B:** Bee 2.8.3 light node on a second laptop on the same Wi-Fi, reached through an SSH tunnel.
- **Guest:** QEMU 8.2.2 with KVM; CirrOS 0.6.3 x86_64 as a raw image resized to 128 MiB (3 intermediate levels, root fan-out 2); 512 MiB of RAM, 2 vCPUs, a user-mode NIC and a NoCloud seed with JSON meta-data. A boot ends at the login prompt.
- **Overlays** as in §5: readahead 64k unless noted, timeout 60 s. beeproxy and chunkprobe as committed today.
- **Batch:** depth 19, immutable, one day, 0.177 xBZZ.

**Upload** (node A, not pinned): 128 MiB in 27.5 s. The tag showed split 33,027 and seen 27,478, mostly zero blocks; all 5,549 distinct chunks were synced about a minute later. The root's span is exactly 134,217,728 bytes with 2 references, so the tree is at redundancy level 0.

## Boots

| Run | Node | To login prompt (s) | GET requests | MiB fetched | Request latency p50 / p99 (ms) | Overlay after boot (MiB) |
|---|---|---|---|---|---|---|
| cold-la-1 | B | 394 | 332 | 40.1 | 1453 / 2202 | 49.1 |
| cold-nola-1 | B | not reached in 900 | 193 | 25.1 | 4512 / 7190 | — |
| cold-la-2 | B | 387 | 335 | 40.3 | 1461 / 2334 | 49.2 |
| cold-nola-2 | B | not reached in 900 | 194 | 25.2 | 4442 / 7376 | 21.7 when stopped |
| cold-tree | B, tree cached | 227 | 337 | 40.6 | 718 / 1466 | 49.1 |
| cold-tree-ra | B, tree cached, readahead 1M | 157 | 167 | 170.1 | 1635 / 14566 | 49.1 |
| warm | A, chunks in Bee's cache | 22.6 | 332 | 40.1 | 9.6 / 147 | |
| hot | A, overlay reused | 9.6 | 2 | 0.2 | 41 | 49.3 after warm and hot |

## Chunk probe

Node B, `Swarm-Cache: false`, 500 leaves per level, run after the four cold boots:

| Concurrency | p50 (ms) | p90 | p99 | max | chunks/s | failed |
|---|---|---|---|---|---|---|
| 1 | 341 | 488 | 680 | 819 | 3 | 0 |
| 8 | 283 | 292 | 335 | 498 | 28 | 0 |
| 32 | 286 | 377 | 643 | 853 | 101 | 0 |
| 128 | 405 | 645 | 933 | 3099 | 121 | 0 |

Node A, 200 leaves per level, all served from Bee's cache after the warm boots:

| Concurrency | p50 (ms) | p90 | p99 | max | chunks/s | failed |
|---|---|---|---|---|---|---|
| 1 | 1.7 | 2.3 | 3.1 | 37.3 | 533 | 0 |
| 8 | 1.9 | 2.9 | 3.8 | 4.2 | 3828 | 0 |
| 32 | 5.3 | 9.1 | 10.6 | 11.9 | 5238 | 0 |
| 128 | 20.3 | 28.5 | 30.8 | 31.2 | 5806 | 0 |

**Intermediate walk:** 57 chunks and 202 zero subtrees skipped. Node B: 1.16 s at concurrency 128, and 21.0 s at concurrency 1 (0.37 s per chunk). Node A: 2.48 s.

**Boot working set:** about 9,500 distinct 4 KiB blocks (37 MiB) in each cold-la and cold-tree log, at QEMU's request granularity, which includes its 64 KiB readahead.

## Observations

1. **A cold chunk from a light node takes about 0.3 s, and throughput levels off at 100–120 chunks/s (about 0.45 MB/s) from 32 requests in flight.** At that rate, prefetching a 300 MB working set would take more than ten minutes, so for large images bandwidth, not latency, may set the cold-boot time.
2. **Caching the tree cuts the cold boot by 42%.** With `Swarm-Cache: false` node B keeps nothing, so every `/bytes` request re-fetches the intermediate chunks above its leaves: a 128 KiB request took 1.45 s against 0.34 s for one chunk. With the tree cached on node B (cold-tree), the median request fell to 0.72 s and the boot from about 390 s to 227 s. cold-la therefore overstates what the engine, with its tree cache, would see.
3. **The boot loader dominates.** In cold-la, about 5 of the 6.5 minutes went to BIOS and GRUB reading one 128 KiB request at a time, about 35 a minute; the kernel then read 13 MiB in parallel within a minute. A 1 MiB readahead (cold-tree-ra) cut the boot to 157 s with half as many requests, but it fetched four times the data (170 MiB), and its slowest requests took up to 16 s. Prefetching from a recorded trace should give the speed without the waste.
4. **Neither lookahead setting suits a block device.** With lookahead off, Bee serves a range in 32 KiB pieces, each waiting for its slowest chunk, so requests were three times slower and neither cold-nola boot finished within 15 minutes. With it on, a 128 KiB request fetches up to 1 MiB and discards the rest.
5. **An unpinned upload does not stay warm on the uploading node.** Bee deleted node A's chunks once they were synced, so node A's first boot fetched them from the network (p50 157 ms, p99 1.4 s per request). The runbook now pins the upload.
6. **Warm and hot boots took 22.6 s and 9.6 s:** the floor for this image on this machine.
7. **Bandwidth cost:** node B's chequebook paid 0.0149 xBZZ for everything here. That is 0.0094 xBZZ for the four cold boots and the probe, and 0.0055 xBZZ for caching the tree and the two cold-tree boots.

## Gate and decision

None; this is a dry run. For the Ubuntu runs:

- **Buy the Phase 0 batch.** It needs Peter's decision on depth 22's duration; at today's price, about 4.2 xBZZ for 3 days or 19.6 xBZZ for 14.
- **Follow the updated runbook:** pin the upload, keep the run order of §5, and add cold-tree and cold-tree-ra.
- **Expect the boot loader to dominate cold boots,** since Ubuntu's kernel and initrd are several times larger than CirrOS's. Watch the tail latency in cold-tree-ra against QEMU's 60 s request timeout.
