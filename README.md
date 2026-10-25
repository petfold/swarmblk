# swarmblk

A virtual disk backed by Ethereum Swarm.

Each snapshot is a standard Swarm file, so its chunk tree is the block map. Unchanged blocks are shared between snapshots, zero regions cost nothing, and any Swarm tool can fetch a snapshot as a raw image and verify it by its root.

**Status:** Phase 0 (measurement) done; Phase 1 (the offline checkpointer) under way. See [ROADMAP.md](ROADMAP.md).

## Why a block device

The block layer is the one interface that every operating system, filesystem and application already uses. Put Swarm underneath it and nothing above needs to know about Swarm. The guest boots its own kernel, mounts its own filesystem and runs unmodified software. A snapshot captures everything at one instant: boot loader, partition table, permissions, database files and half-finished package installs. So you get a whole machine, not a selection of files. Each snapshot is crash-consistent by construction, which copying thousands of changing files rarely achieves.

The Swarm tree adds two things block devices usually lack. Every snapshot shares its unchanged 4 KiB blocks with the previous one. And any snapshot can be branched, or restored on any machine, without copying the disk first.

## How it differs from the alternatives

- **Files or objects** ([swarmfs](https://github.com/petfold/swarmfs), [s3warm](https://github.com/petfold/s3warm)): Swarm sees names and files. You can share or fetch a single file, and others can browse and index the content. But each application must use that interface, and changes are handled per file. A block device sees only opaque blocks that mean something to the guest's filesystem. Pulling one file out of a snapshot means reading that filesystem, with a tool like libguestfs.
- **Storage built for one application** (brash for Iceberg tables, Yjs feeds for documents): fastest and richest in meaning, but it takes a new design per application. The block device is generic and knows nothing about what it stores.
- **Downloading the whole image**: simple, but you wait for every byte. The block device starts at once and fetches only the blocks the guest touches.

## Where it is fast enough

Every cold read costs a Swarm retrieval, which is far slower than a local SSD. Writes never wait for Swarm, because they land on local storage first. So speed depends on how many reads are cold. It works well when:

- **The working set is small and predictable.** A boot reads a small part of its image, and containers read only [6.4%](https://usenix.org/node/194431) of the data they pull. A recorded boot trace can prefetch that set in parallel.
- **Data is read again after the first time.** Cached and hydrated blocks run at local-disk speed.
- **Being able to run at all matters more than latency**, as in disaster recovery.
- **Many machines read the same image.** Popular chunks get cached along Swarm's retrieval paths.

It is too slow for cold random reads over large data, such as a database scanning tables nobody has touched or analytics over terabytes. It also cannot serve several writers on one disk.

## Examples

- A server's host dies, and its VM boots on another provider from the [disk card](docs/DESIGN.md#7-writers-lineages-and-handoff) while the rest of the disk streams in.
- A fleet of CI runners boots a standard build image, and each job gets a throwaway branch.
- An instructor publishes a lab VM, thirty students each run a branch, and only their own changes are stored.
- Kiosks or edge devices update by fetching only the blocks that changed between two snapshots.
- An investigator mounts an evidence image read-only and fetches only the regions they examine.
- A legacy application in an old OS image stays runnable for as long as you keep paying its postage, with no installation media needed.

Disaster recovery is the first target, and golden images for CI and labs the second ([DESIGN §10](docs/DESIGN.md#10-use-cases-ranked)).

## How it works

- **Reads** go to a local engine that caches the tree's upper levels, fetches each missing 4 KiB chunk once, verifies it by hash, and prefetches from a recorded boot trace.
- **Writes** land on local storage first. A checkpoint uploads only the blocks whose content changed, rebuilds the path above them, and publishes the new root through a Swarm feed.
- **Postage** is managed explicitly: immutable batches, one stamp issuer per batch, and rotation that re-stamps only live chunks.
- **Interfaces:** NBD for QEMU and for Linux hosts, with ublk and vhost-user-blk later.
- **Filesystems:** any, from ext4 and XFS to Btrfs, ZFS and NTFS. The choice affects snapshot size and read cost ([DESIGN §9](docs/DESIGN.md#9-filesystems-on-the-disk)).

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

[BSD 3-Clause](LICENSE), the same as Bee, which swarmblk uses as a library.
