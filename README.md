# swarmblk

A virtual disk backed by Ethereum Swarm.

Each snapshot is a standard Swarm file, so its chunk tree is the block map. Unchanged blocks are shared between snapshots, zero regions cost nothing, and any Swarm tool can fetch a snapshot as a raw image and verify it by its root.

**Status:** design done; Phase 0 (measurement) next. See [ROADMAP.md](ROADMAP.md).

## What it is for

1. **VM disaster recovery with live restore.** Boot the last snapshot on any provider at once, and hydrate the disk in the background.
2. **Golden images.** One publisher, many readers, free branches for per-user copies.

## How it works

- **Reads** go to a local engine that caches the tree's upper levels, fetches each missing 4 KiB chunk once, verifies it by hash, and prefetches from a recorded boot trace.
- **Writes** land on local storage first. A checkpoint uploads only the blocks whose content changed, rebuilds the path above them, and publishes the new root through a Swarm feed.
- **Postage** is managed explicitly: immutable batches, one stamp issuer per batch, and rotation that re-stamps only live chunks.
- **Interfaces:** NBD for QEMU and for Linux hosts, with ublk and vhost-user-blk later.

Details: [docs/DESIGN.md](docs/DESIGN.md) (architecture) and [docs/SPEC.md](docs/SPEC.md) (formats).

## Try it without swarmblk code

Upload a raw image with `Swarm-Redundancy-Level: 0`, then boot it straight from Bee through QEMU's HTTP block driver with a local overlay. The full runbook, including the pitfalls, is in [docs/PHASE0.md](docs/PHASE0.md).

```sh
qemu-img create -f qcow2 -F raw \
  -b 'json:{"file.driver":"http","file.url":"http://localhost:1633/bytes/<ref>","file.readahead":"64k","file.timeout":60}' \
  overlay.qcow2
qemu-system-x86_64 -enable-kvm -m 4G -drive file=overlay.qcow2,if=virtio,copy-on-read=on
```

Requires Bee 2.8.2 or later.

## License

To be decided. Bee, which swarmblk uses as a library, is BSD-licensed.
