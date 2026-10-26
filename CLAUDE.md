# CLAUDE.md — swarmblk

swarmblk is a virtual disk whose snapshots live on Ethereum Swarm. Each snapshot is an ordinary Swarm file, so its chunk tree is the block map. A local engine will serve the disk over NBD, keep writes local, and publish checkpoints as new trees that share every unchanged chunk with the last one.

## Read first

1. [docs/DESIGN.md](docs/DESIGN.md) — architecture and the reasons for it
2. [docs/SPEC.md](docs/SPEC.md) — formats and invariants you must not break
3. [ROADMAP.md](ROADMAP.md) — phases, gates, current tasks
4. [docs/PHASE0.md](docs/PHASE0.md) — the measurement runbook

## Where we are

**Phase 2: the block engine over NBD,** started on 2026-10-26 after Phase 1's gate was met ([results](docs/results/phase1-gate-2026-10-26.md)). Phase 1 built the offline tools on `internal/tree`, `internal/store`, `internal/bee`, `internal/snapshot` and `internal/lineage`: `swarmblk import`, `checkpoint` (a stopped VM's qcow2 overlay into the next snapshot of a lineage), `restore`, `export` and `diff`. `internal/nbd` serves block devices over NBD with structured replies, block status, flush, FUA, trim and write-zeroes, tested against QEMU's client. `internal/engine` has the read path: a snapshot's disk read from its tree, with every intermediate chunk cached, zero subtrees answered without fetches and reported as holes, and parallel verified fetches through the local store. `swarmblk serve` exports a snapshot or a lineage's head read-only over NBD, and QEMU runs overlays on it. It records boot traces and prefetches from them ahead of the guest (SPEC §6): a cold boot of Alpine through a node that had never seen it took 3.5 minutes with its trace and 17–18 without ([results](docs/results/phase2-cold-2026-10-26.md)), and Phase 0's Ubuntu, which stock QEMU could not boot, 9.7 minutes with its trace and 37 without ([results](docs/results/phase2-ubuntu-2026-10-26.md)). A filesystem round trip (ext4 changed with debugfs, checkpointed, exported through another node, checked with e2fsck) passes. The write path is in: `internal/epoch` keeps a disk's writes in a checksummed, crash-recoverable log (SPEC §11), `engine.Disk` puts it on top of a snapshot, and `swarmblk serve -disk NAME` serves the disk writable, so a VM runs straight on it. Live checkpoints work (SPEC §8): serve cuts the writes atomically, optionally freezing the guest's filesystems through the guest agent, and turns the frozen epoch into the next snapshot while the guest keeps writing, every `-every` or when `swarmblk checkpoint -disk NAME` asks ([results](docs/results/phase2-live-2026-10-26.md)). `tools/crashtest` killed QEMU or the engine under ext4, XFS, Btrfs and ZFS 92 times without losing an fsynced write, and measured what each filesystem's checkpoints upload ([results](docs/results/phase2-crash-2026-10-26.md)). Each writable disk has a hydration policy: lazy disks read through a chunk cache that `serve -cache` bounds, a hydrating disk keeps a whole local copy that runs it with no network, and a re-stamping disk stamps its snapshot again under the host's batch. A read that Swarm cannot serve waits and raises an alarm (`serve -failure`, `-alarm`), and `serve -iolock` readies the engine for a host whose kernel mounts the disk. `swarmblk card` makes a disk card (SPEC §12, `internal/card`): the disk's lineage, and the machine's definition, UEFI variables and TPM state, encrypted on Swarm. `swarmblk boot` runs the machine from it with QEMU, on a new hydrating disk. A rehearsal on one laptop killed a machine's host and booted the machine from its card ([results](docs/results/phase2-dr-2026-10-27.md)). Left for Phase 2's gate: the drill on a second machine with QEMU.

## Hard rules

- **Redundancy level 0, always.** Every upload sets `Swarm-Redundancy-Level: 0`. Bee's default for `/bytes` is medium erasure coding, which changes the tree geometry.
- **Trees are Bee's trees.** Build and hash with Bee's Go packages; never reimplement BMT, chunk addressing or the tree builder. Every tree function is tested against Bee's pipeline (SPEC §14, I1).
- **Immutable batches only.** Never create or stamp with a mutable batch.
- **One stamp issuer per batch.** A batch is bound to one host and one Bee node. Never stamp from it anywhere else. If a node loses its state, its batches take no new chunks.
- **Root last.** No lineage feed update until the upload tag shows every new chunk synced. A `/chunks/stream` ack only means stored locally.
- **One writer per lineage.** Never write a lineage feed from two hosts.
- **No secrets in the repo.** It is public. Lineage keys, disk keys and disk cards never go into code, logs, fixtures or commits.
- **Ask before spending.** Buying, topping up or diluting batches spends xBZZ; ask the owner (Peter) first.

## Stack

- Go, at the version Bee's `go.mod` requires. Module path and public repo: `github.com/petfold/swarmblk`.
- `github.com/ethersphere/bee/v2` at the latest 2.8.x (v2.8.3 at writing), used as a library. Ethersphere's beekeeper does the same.

| Bee package | Use |
|---|---|
| `pkg/swarm` | constants, `Address`, `Chunk` |
| `pkg/cac`, `pkg/bmt` | chunk construction and addresses |
| `pkg/file/pipeline/builder` | reference tree builder (import, tests) |
| `pkg/file/joiner` | reference reader (tests) |
| `pkg/manifest` | Mantaray snapshot manifests |
| `pkg/feeds`, `pkg/soc` | lineage feed updates |

- Tools: QEMU with KVM, `qemu-img`, `qemu-nbd`, libnbd (`nbdinfo`, `nbdcopy`), `jq`.

## Environment

| Variable | Meaning |
|---|---|
| `SWARMBLK_BEE_URL` | Bee API, default `http://localhost:1633` (Bee ≥ 2.8.2) |
| `SWARMBLK_BATCH` | immutable batch for integration tests, bound to this host |
| `SWARMBLK_BEE_URL_B` | optional second Bee node, which integration tests restore through |
| `SWARMBLK_HOME` | local state, default `~/.local/share/swarmblk` (SPEC §10) |

Unit tests need no network. Integration tests need a running Bee node and a funded immutable batch.

## Layout (planned)

```
cmd/swarmblk/        CLI: import, checkpoint, restore, export, diff (Phase 1); serve, card, boot, iolock (Phase 2)
cmd/chunkprobe/      Phase 0 latency probe
tools/beeproxy/      Phase 0 logging proxy
tools/crashtest/     Phase 2 crash and cost test with a real guest
internal/tree/       geometry, paths, zero subtrees, rebuild, diff
internal/bee/        minimal Bee HTTP client
internal/beetest/    fake Bee node for unit tests
internal/store/      content-addressed chunk store
internal/snapshot/   manifest and meta.json
internal/lineage/    lineage feed (registry in Phase 4)
internal/nbd/        NBD server (Phase 2)
internal/engine/     block engine: snapshots read-only, disks writable (Phase 2)
internal/epoch/      write epochs: the local write log (Phase 2)
internal/card/       disk cards and VM definitions (Phase 2)
docs/                GUIDE, REFERENCE, DESIGN, SPEC, PHASE0, results/
```

## Commands

```sh
go build ./...
go test ./...                       # unit tests, no network
go test -tags integration ./...     # needs Bee and SWARMBLK_BATCH
```

## Testing

- Tree code: random images at the sizes SPEC §2 allows, random block mutations, incremental rebuild compared with a full Bee rebuild.
- Integration: small disks (64 MiB to 1 GiB), redundancy 0, the test batch.
- Restores: sha256 of `swarmblk export` must equal the guest-visible disk at checkpoint (I7).
- Never use the public gateway in tests.

## Conventions

- Keep docs short and plain. When a decision changes, update docs/DESIGN.md and add a row to its decision log.
- Formats are frozen per SPEC version: anything that changes a root or a byte on Swarm needs a new format version.
- Small commits with imperative subjects.
