# swarmblk roadmap

Each phase ends at a gate that decides whether the next is worth building. Phase 0 is days of work, not weeks. Details: [docs/DESIGN.md](docs/DESIGN.md), [docs/SPEC.md](docs/SPEC.md).

**Current phase: 1.**

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
- [ ] Discard check: with QEMU `discard=unmap,detect-zeroes=unmap`, guest discards on the overlay appear as zero clusters, not holes onto the backing image

**Gate:** roots match Bee's pipeline for the same bytes, and a restore on a second host is byte-identical (SPEC §14, I7).

## Phase 2 — Block engine over NBD

- [ ] NBD server: evaluate an existing Go library (for example pojntfx/go-nbd) for structured replies, block status, flush, trim and write-zeroes; otherwise implement that subset
- [ ] Write epochs with replayable, checksummed index; flush and FUA durability (SPEC §11)
- [ ] Tree and chunk caches; zero short-circuit; holes through block status; parallel verified fetches
- [ ] Trace-driven prefetch (`prefetch.bin`, SPEC §6), and hydration as a per-disk policy: lazy, hydrate, or re-stamp only (DESIGN §3)
- [ ] Live checkpoints from an atomic cut; guest-agent freeze for application consistency
- [ ] Failure policy: block the guest and raise an alarm after retries
- [ ] `PR_SET_IO_FLUSHER` and locked memory for the engine and Bee
- [ ] Disk card with VM definition, UEFI variables and vTPM state (SPEC §12)

**Gate:** a disaster-recovery drill — kill a host, then boot its VM on another machine from the disk card.

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
