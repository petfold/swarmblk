# CLAUDE.md — swarmblk

swarmblk is a virtual disk whose snapshots live on Ethereum Swarm. Each snapshot is an ordinary Swarm file, so its chunk tree is the block map. A local engine will serve the disk over NBD, keep writes local, and publish checkpoints as new trees that share every unchanged chunk with the last one.

## Read first

1. [docs/DESIGN.md](docs/DESIGN.md) — architecture and the reasons for it
2. [docs/SPEC.md](docs/SPEC.md) — formats and invariants you must not break
3. [ROADMAP.md](ROADMAP.md) — phases, gates, current tasks
4. [docs/PHASE0.md](docs/PHASE0.md) — the measurement runbook

## Where we are

**Phase 1: the offline checkpointer.** Phase 0 is done ([results](docs/results/phase0-2026-10-25.md)), and its gate was decided on 2026-10-25: go on. `internal/tree` has geometry, path arithmetic, zero subtrees, incremental rebuild and diff, `internal/store` keeps verified chunks on disk, and `internal/bee` uploads chunks (single or streamed), tracks sync by tag, reads batches and buckets, and publishes and reads feed updates. `swarmblk import` builds an image's tree with Bee's pipeline into the store, wraps it in a snapshot manifest (`internal/snapshot`) and uploads both, root last; the disk root matches Phase 0's `/bytes` upload, and stock Bee serves the snapshot under `/bzz`. `internal/lineage` creates lineage keys, signs updates byte-identical to Bee's sequence feeds, and publishes only on top of the head it expects. `swarmblk checkpoint` turns a stopped VM's qcow2 overlay into the next snapshot of a lineage and rebases the overlay onto it (SPEC §8). Next are `restore`, `export` and `diff`, then the gate's restore on a second host. Do not start the NBD engine (Phase 2) before Phase 1's gate passes.

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
| `SWARMBLK_HOME` | local state, default `~/.local/share/swarmblk` (SPEC §10) |

Unit tests need no network. Integration tests need a running Bee node and a funded immutable batch.

## Layout (planned)

```
cmd/swarmblk/        CLI: import, checkpoint, restore, export, diff (Phase 1)
cmd/chunkprobe/      Phase 0 latency probe
tools/beeproxy/      Phase 0 logging proxy
internal/tree/       geometry, paths, zero subtrees, rebuild, diff
internal/bee/        minimal Bee HTTP client
internal/beetest/    fake Bee node for unit tests
internal/store/      content-addressed chunk store
internal/snapshot/   manifest and meta.json
internal/lineage/    lineage feed (registry in Phase 4)
docs/                DESIGN, SPEC, PHASE0, results/
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
