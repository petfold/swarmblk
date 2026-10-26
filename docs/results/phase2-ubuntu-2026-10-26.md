# Ubuntu boots through the engine — 26 October 2026

In Phase 0, stock QEMU could not cold-boot Ubuntu from Swarm through a light node. With the runbook's settings every cold boot fell into emergency mode at 14–30 minutes, when systemd's 90 s wait for the boot partitions ran out. Only one tuned setting booted, in 22.2 minutes ([results](phase0-2026-10-25.md)). Since then the engine's cold boot was measured with Alpine ([results](phase2-cold-2026-10-26.md)). These runs boot Phase 0's Ubuntu through the engine.

## Setup

- **Disk:** Phase 0's Ubuntu 24.04 cloud image as an 8 GiB disk, root `8ebdfc89…`, uploaded with `/bytes` and pinned on node A. `swarmblk serve -root` exports it read-only, and QEMU boots a qcow2 overlay on the export.
- **Guest:** as in Phase 0. QEMU 8.2.2 with KVM, 4 GiB of RAM, 2 vCPUs, QEMU's default network, and the same password-only NoCloud seed.
- **Timing:** from QEMU's start to the console's `Cloud-init v. … finished`, Phase 0's end of a boot. Each run starts the engine with an empty local store. The cold runs go through node B with `serve -nocache`.
- **Trace:** recorded with `serve -record` in the warm run: 60,630 blocks (237 MiB) in 4,367 runs. That is what the guest read, less than Phase 0's 78,614 blocks at QEMU's request granularity.

## Runs

| Run | Bee node | Prefetch | Login prompt | Cloud-init finished | Chunks fetched | Node B paid |
|---|---|---|---|---|---|---|
| warm | A, holding every chunk | none, recording the trace | 75 s | 85 s | 59,678 | — |
| warm | A | the trace | — | 64 s | 59,876 | — |
| cold | B | the trace | 7.4 min | **9.7 min** | 60,314 | 0.045 xBZZ |
| cold | B | none | 32.7 min | 37.4 min | 59,993 | 0.059 xBZZ |

For comparison, Phase 0 with stock QEMU reading through Bee's HTTP API:

| Run | Cloud-init finished |
|---|---|
| warm | 59.5 s |
| cold, the runbook's settings | failed: emergency mode at 14–30 minutes |
| cold, tree cached on node B, lookahead equal to the request size | 22.2 min |

## Observations

1. **The engine cold-boots Ubuntu through one light node.** It took 9.7 minutes with the trace and 37 minutes without. No run timed out: the engine's reads were fast enough for systemd's waits, and nothing had to be cached on node B first.
2. **Prefetch makes the cold boot four times faster.** With the trace, the boot is bound by throughput: 60,314 chunks in 9.7 minutes, about 100 chunks a second. That is close to the Alpine runs' 88 a second, and to Phase 0's estimate of seven or eight minutes for this working set.
3. **Without prefetch, half the cold boot is the boot loader.** The kernel started 18.5 minutes after QEMU, because GRUB reads the kernel and initrd one block at a time and each read was a cold retrieval. Phase 0's first stock boot spent about 15 minutes there.
4. **Warm boots need the trace too.** Without it, the engine fetches one chunk per request in the order the guest reads, and the warm boot took 85 s against Phase 0's 59.5 s, whose HTTP reads fetched 128 KiB at a time. With the trace it took 64 s.
5. **Cost:** node B paid 0.104 xBZZ for the two cold boots, about 0.23 xBZZ per GiB retrieved, a little more than the Alpine runs' 0.18.
