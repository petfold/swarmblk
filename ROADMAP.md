# swarmblk roadmap

Each phase ends at a gate that decides whether the next is worth building. Phase 0 is days of work, not weeks. Details: [docs/DESIGN.md](docs/DESIGN.md), [docs/SPEC.md](docs/SPEC.md).

**Current phase: 2.**

## Phase 0 — Measure with stock tools

Boot an unmodified image from Swarm through QEMU's HTTP block driver and measure. Runbook: [docs/PHASE0.md](docs/PHASE0.md).

- [x] Bee ≥ 2.8.2 on node A; a second node B (light) on another machine; immutable test batch (depth 21 sufficed)
- [x] 8 GiB raw Ubuntu cloud image and cloud-init seed
- [x] Upload at redundancy level 0; record reference, upload time and tag counters
- [x] `tools/beeproxy`: logging reverse proxy that can inject `Swarm-Lookahead-Buffer-Size` and `Swarm-Cache`
- [x] `cmd/chunkprobe`: intermediate walk plus random-leaf latency at several concurrency levels
- [x] Boot runs: cold with and without lookahead, warm, hot (cold boots fail with stock tools)
- [x] Chunk probe on nodes A and B
- [x] `docs/results/phase0-<date>.md` from the template: [2026-10-25](docs/results/phase0-2026-10-25.md)

**Gate:** cold boot within a bound Peter sets after seeing the numbers, and a boot working set small enough to prefetch.

Decided 2026-10-25: go on to Phase 1. With stock tools the best cold boot took 22 minutes and the default settings never completed, while the working set (307 MiB) fetched in 7 minutes engine-style. The cold-boot bound is set when an engine prototype can be measured.

## Phase 1 — Offline checkpointer (Go)

Prove the data model: overlay changes become a new Swarm tree that restores elsewhere, with roots identical to Bee's.

- [x] Module `github.com/petfold/swarmblk`; depend on `github.com/ethersphere/bee/v2` at the latest 2.8.x (v2.8.3 at writing); CI running `go test ./...`
- [x] `internal/tree`: geometry, path arithmetic, zero subtrees, incremental rebuild along dirty paths, tree diff; test vectors against Bee's `pkg/file/pipeline/builder` (SPEC §14, I1)
- [x] `internal/bee`: HTTP client for `/chunks`, `/chunks/stream`, `/tags`, `/soc`, `/feeds`, `/stamps`, `/stamps/{id}/buckets`
- [x] `internal/store`: content-addressed chunk store (tree cache and chunk cache), verified on write
- [x] `internal/snapshot`: Mantaray manifest and `meta.json` (SPEC §5)
- [x] `internal/lineage`: lineage feed read and write (SPEC §7)
- [x] `swarmblk import <image.raw>`: chunk-level upload at redundancy 0, producing the same root as Phase 0's `/bytes` upload
- [x] `swarmblk checkpoint <overlay.qcow2>`: the overlay's own blocks from `qemu-img map --output=json` (zero clusters become Z_0), read from the qcow2 file at the mapped offsets, rehash, rebuild, upload what differs from the parent, confirm by tag, rebase the overlay, publish (SPEC §8)
- [x] `swarmblk restore <lineage|manifest>`: a fresh qcow2 overlay on the HTTP backing, or `swarmblk export` to a raw file with verified chunk fetches
- [x] `swarmblk diff <a> <b>`: changed block ranges between two snapshots
- [x] Discard check: with QEMU `discard=unmap,detect-zeroes=unmap`, guest discards on the overlay appear as zero clusters, not holes onto the backing image ([gate run](docs/results/phase1-gate-2026-10-26.md))

**Gate:** roots match Bee's pipeline for the same bytes, and a restore on a second host is byte-identical (SPEC §14, I7).

Met 2026-10-26 ([results](docs/results/phase1-gate-2026-10-26.md)): a CirrOS guest changed a disk restored from Swarm, laptop A checkpointed it, and laptop B exported the lineage through its own Bee node with the same sha256 as the guest's disk; QEMU read and booted it through node B too.

## Phase 2 — Block engine over NBD

- [x] NBD server: evaluate an existing Go library (for example pojntfx/go-nbd) for structured replies, block status, flush, trim and write-zeroes; otherwise implement that subset. Neither library checked had them, so `internal/nbd` implements the subset, tested against QEMU's client (DESIGN §12)
- [x] Write epochs with replayable, checksummed index; flush and FUA durability (SPEC §11): `internal/epoch`, `engine.Disk` and `swarmblk serve -disk`
- [x] Tree and chunk caches; zero short-circuit; holes through block status; parallel verified fetches: `internal/engine`, exported read-only by `swarmblk serve`. The chunk cache is bounded since the hydration policy
- [x] Trace-driven prefetch (`prefetch.bin`, SPEC §6): `serve -record`, `checkpoint -prefetch`, prefetch ahead of the guest ([first runs](docs/results/phase2-prefetch-2026-10-26.md))
- [x] Cold-boot measurement through the engine, on a disk no node has fetched, to set the bound Phase 0 left open: Alpine booted cold through node B in 17–18 min without prefetch and in 3.4–3.5 min with its trace ([results](docs/results/phase2-cold-2026-10-26.md)), and Phase 0's Ubuntu, which stock QEMU could not boot, in 37 min without and 9.7 min with its trace ([results](docs/results/phase2-ubuntu-2026-10-26.md)); Peter sets the bound
- [x] Hydration as a per-disk policy: lazy, hydrate, or re-stamp only (DESIGN §3): `serve -disk … -policy`. Lazy disks share a chunk cache that `serve -cache` bounds. A hydrating disk keeps a local image that checkpoints move on: a CirrOS disk booted from it with no Bee running, and the image matched the exported snapshot byte for byte. Re-stamping is tested against the fake Bee only, since it needs a second batch to show anything live
- [x] Filesystem round trip without root or a VM: an ext4 image built with `mkfs.ext4 -d`, changed with `debugfs`, checkpointed, and exported on another node; `e2fsck -fn` must be clean and every file's checksum equal (`TestFilesystem`, and `TestLiveFilesystem` through node B)
- [x] Crash consistency with a real guest: files written with checksums and `fsync`, QEMU killed at random points, the overlay checkpointed and restored on another node; `fsck` must be clean and every file that was `fsync`ed intact, over many runs. With the guest's own driver writing, the same workload on ext4, XFS, Btrfs and ZFS compares what checkpoints upload (DESIGN §9), and so what a native ZFSwarm would save. `tools/crashtest` killed QEMU or the engine under a writable disk 80 times and checked 24 snapshots through node B, on all four filesystems, without losing an fsynced operation. For the same workload, XFS uploaded 1.2 times the data written, ext4 2.2, ZFS 2.3 and Btrfs 3.5 ([results](docs/results/phase2-crash-2026-10-26.md))
- [x] Live checkpoints from an atomic cut; guest-agent freeze for application consistency: `serve -disk` checkpoints the disk every `-every` or when `checkpoint -disk` asks, freezing the guest's filesystems with `-qga` (SPEC §8). A checkpoint taken while CirrOS wrote was crash-consistent, and the last one matched the disk byte for byte ([results](docs/results/phase2-live-2026-10-26.md)). With Ubuntu's real agent, the frozen snapshot's ext4 needed no journal recovery ([results](docs/results/phase2-qga-2026-10-26.md))
- [x] Failure policy: block the guest and raise an alarm after retries: `serve -failure block`, the default, makes a read that Swarm cannot serve wait and retry, backing off to 30 s, and raises an alarm in serve's log and through `-alarm`; `-failure error` returns I/O errors
- [x] `PR_SET_IO_FLUSHER` and locked memory for the engine and Bee: `serve -iolock`, and `swarmblk iolock -- bee start`, for hosts whose kernel mounts a disk. Only the refusal without capabilities is tested here; the rest needs root
- [x] Disk card with VM definition, UEFI variables and vTPM state (SPEC §12): `swarmblk card` and `swarmblk boot`. A rehearsal of the gate on one laptop killed the machine's host and booted it from its card in a fresh state directory, with every write of its last checkpoint, and booted a UEFI card ([results](docs/results/phase2-dr-2026-10-27.md)). The vTPM path is tested against a fake swtpm only

**Gate:** a disaster-recovery drill — kill a host, then boot its VM on another machine from the disk card. Rehearsed on one laptop; the drill itself needs a second machine with QEMU, and swtpm for a vTPM.

## Phase 3 — Retention manager

- [ ] Coverage index (address → batches) and TTL forecasts with a margin (SPEC §9, P5)
- [ ] Bucket watch: new batch or dilution when the fullest bucket passes 90% (P4)
- [ ] Rotation by deduplicated re-stamping (P6); adoption of shared bases; batch binding per host (P2, P3)
- [ ] Scrub and repair from the local chunk store; pin the latest snapshot
- [ ] Cost report per disk and per retention class

**Gate:** a simulated 90-day history keeps every retained snapshot restorable within budget.

## Phase 4 — Privacy and branching

- [ ] LUKS guidance: aes-xts-plain64, no random-IV integrity mode, discards as a per-disk choice
- [ ] ACT sharing of snapshot manifests, with Swarm ID grantees
- [ ] Disk registry with handoff, takeover and the fencing check (SPEC §7)
- [ ] Optional journal mode for short recovery points

**Gate:** two branches of one base run independently, and a revoked grantee cannot read new snapshots.

## Stretch

- vhost-user-blk and ublk exports
- Suspend to Swarm and resume elsewhere (QEMU migration stream, lazy page loading)
- Engine-side stamp signing, so Bee can be a remote gateway; engine-side encryption

## Later projects

- **ZFSwarm:** ZFS with Swarm underneath natively, instead of through a block device. ZFS is a Merkle tree: each block pointer carries its child's checksum up to the uberblock, which maps onto Swarm's trees and feeds. A "swarm" checksum would make every block pointer hold its block's Swarm reference, and OpenZFS's user-space stack (`libzpool`) behind FUSE or virtiofs would serve it with this engine's caches, write log, prefetch and feeds. First measure ZFS on swarmblk (the crash and cost test above), then prototype in `libzpool`; the hard part is ZFS's location-based allocation.
