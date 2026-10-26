# swarmblk reference

This manual describes every swarmblk command, option, file and message, as of Phase 2. For a walkthrough, see the [user guide](GUIDE.md); for the on-Swarm formats in full, [SPEC.md](SPEC.md); for the reasons behind them, [DESIGN.md](DESIGN.md).

## Commands

| Command | Does |
|---|---|
| [`import`](#import) | uploads a raw disk image as a first snapshot |
| [`serve`](#serve) | serves a snapshot read-only, or a disk writable, over NBD |
| [`checkpoint`](#checkpoint) | turns a disk's or an overlay's changes into the next snapshot |
| [`restore`](#restore) | makes a qcow2 overlay that reads a snapshot from Bee over HTTP |
| [`export`](#export) | writes a snapshot's disk to a raw image file |
| [`diff`](#diff) | lists the blocks that differ between two snapshots |

## Conventions

**Options come before arguments:** `swarmblk serve -disk vm1 <snapshot>`, not `swarmblk serve <snapshot> -disk vm1`. Every command takes:

| Option | Default | Meaning |
|---|---|---|
| `-bee url` | `$SWARMBLK_BEE_URL`, else `http://localhost:1633` | the Bee node's API; Bee 2.8.2 or newer |
| `-home dir` | `$SWARMBLK_HOME`, else `~/.local/share/swarmblk` | the [local state](#local-state) |

Commands that upload also take `-batch id`, by default `$SWARMBLK_BATCH`: the [postage batch](#postage) to stamp with.

**Naming snapshots.** A snapshot is named by its reference: 64 hexadecimal digits, the address of its manifest. A lineage is named `0x<owner>/<topic>`, 40 and 64 lowercase hex digits, and stands for its latest snapshot, its **head**. swarmblk finds the head through Bee's feed lookup, which can lag a fresh update by a minute or so.

**Disk names** are letters, digits, dots, dashes and underscores, starting with a letter or digit.

**Output.** A command's result goes to standard output as one line: a snapshot reference, or for `serve` an NBD URI. Progress and errors go to standard error, errors prefixed `swarmblk:`. The exit status is 0 on success, 1 on an error, and 2 for a usage error, after which the usage is printed.

**Blocks.** Disks are divided into 4 KiB blocks, numbered from 0. Each block is one Swarm chunk; a block of zeros is the shared zero chunk and costs nothing.

## import

```
swarmblk import [-bee url] [-batch id] [-home dir] image.raw
```

Uploads the raw disk image `image.raw` as a first snapshot and prints its reference.

- The image's size must be a power of two from 64 MiB to 1 TiB (2^26 to 2^40 bytes). Resize it with `qemu-img resize -f raw`.
- import builds the disk's tree locally with Bee's pipeline, at redundancy level 0, and keeps its chunks in the local store. It writes the snapshot's manifest, checks the batch ([Postage](#postage)), and then uploads every distinct chunk once, over one stream under a new tag, with the manifest's root last. It returns once Bee's tag shows every chunk synced to the network.
- The snapshot starts no lineage: its `parent` and `lineage` are null. The first checkpoint of a disk or overlay on it starts one.

Progress:

```
tree: 128 MiB disk, 33027 chunks, 5549 distinct, root 1e38c8e7…
snapshot: 9c7a4d0b…, 5511 blocks with data, 5554 chunks to upload
upload: 5554 chunks stored under tag 731813 in 9s; waiting for sync
sync: 5554 of 5554 synced after 34s
```

## serve

```
swarmblk serve [options] snapshot|lineage                 read-only, a snapshot's disk
swarmblk serve [options] -root root                       read-only, a bare disk tree
swarmblk serve [options] -disk name [snapshot|lineage]    writable, a named disk
```

Serves a disk over NBD on a Unix socket until it gets SIGINT or SIGTERM, and prints the URI for QEMU:

```
nbd+unix:///<export>?socket=<path>
```

The export is named after the snapshot's reference, the root, or the disk's name.

| Option | Default | Meaning |
|---|---|---|
| `-socket path` | `nbd.sock` in the state directory | the Unix socket to listen on, mode 0600 |
| `-root root` | | serve the disk tree under this 64-hex-digit root, such as a `/bytes` upload at redundancy level 0 |
| `-disk name` | | serve this [writable disk](#writable-disks); a snapshot or lineage is needed only when the disk is new |
| `-batch id` | `$SWARMBLK_BATCH` | the batch that the disk's checkpoints stamp with |
| `-every interval` | off | checkpoint the disk at this interval, if it changed; a Go duration such as `30s`, `15m`, `2h` |
| `-qga socket` | off | freeze the guest's filesystems around each checkpoint's cut through the QEMU guest agent on this socket |
| `-record file` | off | when serve stops, write the blocks the guest read from the snapshot to this file as a [boot trace](#boot-traces) |
| `-prefetch file` | the snapshot's trace | prefetch from the boot trace in this file |
| `-window blocks` | 16384 | prefetch at most this many blocks of the trace ahead of the guest |
| `-nocache` | off | ask Bee not to cache what it retrieves (`Swarm-Cache: false`), so that measurements stay cold |

`-every` and `-qga` need `-disk`, and `-every` needs a batch. serve refuses a socket that another server listens on, and replaces one that nobody does.

**The NBD export.** Fixed-newstyle handshake, structured replies, and block status through the `base:allocation` context, with zero blocks reported as holes. Block sizes: 512 bytes minimum, 4096 preferred, 32 MiB the largest request. Flush and FUA are supported. A writable export also takes trim and write-zeroes; a read-only one refuses writes.

**Reads.** serve fetches each 4 KiB chunk from Bee once, up to 64 at a time, checks it against its address, and keeps it in the local chunk store. It keeps every intermediate chunk of the tree in memory, and answers zero subtrees without fetching. A failed fetch is tried four times in all, after 0.5, 1 and 2 s; a chunk that fails its check is not tried again. A read that still fails returns an I/O error to the guest.

**Prefetch.** If the snapshot carries a boot trace, or `-prefetch` names one, serve fetches its blocks in trace order in the background, at most `-window` blocks ahead of the furthest trace block the guest has read, with up to half of the 64 fetches.

**Writable disks.** With `-disk`, serve opens the disk's [epochs](#writable-disks), recovering them after a crash, and locks the disk. A new disk needs the snapshot or lineage it starts from. For an existing disk, a snapshot given must be the one it sits on, so it is best left out. Writes go to the active epoch; a flush, or a write with FUA, returns once the epoch is durable. serve also listens on the disk's control socket for [checkpoint requests](#control-socket). If a checkpoint stopped part way and a batch is set, serve finishes it at start.

Its messages, from several runs:

```
disk vm1: new, on snapshot 9c7a4d0b…
serve: prefetching 5926 blocks of the boot trace, up to 16384 ahead of the guest
serve: a 128 MiB disk, writable, epoch 1 holding 0 blocks, on snapshot 9c7a4d0b…, as vm1
serve: stopped after 12m49s; 0 chunks fetched from Bee
serve: prefetched 5926 of 5926 trace blocks, 0 failed
serve: wrote a boot trace of 5926 blocks in 120 runs to boot.trace
```

## checkpoint

```
swarmblk checkpoint [-bee url] [-batch id] [-home dir] -disk name
swarmblk checkpoint [-bee url] [-batch id] [-home dir] [-branch] [-prefetch file] overlay.qcow2
```

Turns the changes of a writable disk, or of a stopped VM's qcow2 overlay, into the next snapshot, and prints its reference. With nothing changed, it uploads nothing and prints the snapshot the disk or overlay already sits on.

**A disk** (`-disk`). If a serve holds the disk, checkpoint asks it over the control socket, and that serve's batch and guest agent are used. Otherwise checkpoint opens the disk itself, with `-batch` and no guest agent. The checkpoint runs while the guest keeps writing:

1. If an earlier checkpoint left a frozen epoch, it is finished first.
2. With `-qga`, the guest agent freezes the guest's filesystems.
3. **The cut:** the writes in flight finish, new ones wait, the active epoch is synced and frozen, and a new epoch takes the writes. Reads never wait, and writes wait only for the writes in flight and one fsync.
4. The guest's filesystems are thawed, even if the freeze failed.
5. The frozen epoch's blocks, as they were at the cut, are rebuilt into the parent's tree. Only the chunks that differ from the parent's at their place are uploaded, the manifest's root last, and the checkpoint waits until all are synced.
6. The frozen epoch records the uploaded snapshot, which is then published as the lineage's next update.
7. The disk moves onto the new snapshot, and the frozen epoch is removed.

The snapshot's consistency is `application` if the freeze succeeded, and `crash` otherwise: a live snapshot then needs the journal recovery that a power cut would.

**An overlay.** The overlay's backing file must name its parent snapshot as `…/bzz/<snapshot>/disk.raw`, as `restore` makes it, and its VM must be stopped: qemu-img refuses an overlay that QEMU holds. checkpoint reads the clusters the overlay itself holds from the qcow2 file, uploads as above, rebases the overlay onto the new snapshot, and publishes it.

| Option | Meaning |
|---|---|
| `-disk name` | checkpoint this writable disk |
| `-branch` | start a new lineage, even if this host holds the parent's (overlays) |
| `-prefetch file` | store the boot trace in this file with the snapshot (overlays); otherwise it keeps its parent's |

**Lineage.** A snapshot continues its parent's lineage if this host holds the lineage's key and the parent is the lineage's head. If the host lacks the key, or the parent is in no lineage, the snapshot starts a new lineage with a new key. If the host has the key but Bee finds another head, checkpoint stops with `not the lineage's head`: Bee may lag, so retry, or for an overlay start a branch with `-branch`.

Progress:

```
disk vm1: froze epoch 1 with 6611 blocks
disk vm1: 6611 blocks on snapshot 9c7a4d0b…, rebuilt in 2.419s
lineage 0x1bc0467a…/219d107a…: new, from snapshot 9c7a4d0b…
snapshot: e55a07f6…, 6593 blocks changed, 6387 chunks to upload
upload: 6387 chunks stored under tag 731814 in 19s; waiting for sync
sync: 6387 of 6387 synced after 49s
lineage 0x1bc0467a…/219d107a…: update 0 is snapshot e55a07f6…
```

Other lines: `disk vm1: no writes since snapshot …` and `no changes since snapshot …` when nothing changed; `disk vm1: taking up the checkpoint of epoch …` when finishing an earlier one; `lineage …: publishing update …, which an earlier checkpoint left unpublished`; `lineage …: another host writes it` when the host lacks the parent lineage's key; and `disk vm1: the guest agent did not freeze the filesystems, so the snapshot is crash-consistent: …`.

## restore

```
swarmblk restore [-bee url] [-home dir] snapshot|lineage overlay.qcow2
```

Makes a new qcow2 overlay, which must not exist yet, on the snapshot, and prints the snapshot's reference. QEMU reads the disk through the overlay's backing file, `GET /bzz/<snapshot>/disk.raw` from Bee over HTTP, with requests timing out after 60 s and reading 64 KiB ahead. QEMU needs its HTTP block driver, in Ubuntu's `qemu-block-extra` package. The VM's writes stay in the overlay until `checkpoint overlay.qcow2`.

## export

```
swarmblk export [-bee url] [-home dir] snapshot|lineage image.raw
```

Writes the snapshot's disk to a new raw image file, which must not exist yet, and prints the snapshot's reference. export walks the tree itself, fetches 32 chunks at a time, checks each, and retries failed fetches as serve does. Zero regions become holes in a sparse file. It reports the image's SHA-256, which is the same wherever the snapshot is exported:

```
export vm1.raw: snapshot ea64ec80…, 128 MiB, 12072 blocks with data, sha256 c7742d80…, in 11.752s
```

## diff

```
swarmblk diff [-bee url] [-home dir] snapshot|lineage snapshot|lineage
```

Prints the blocks whose contents differ between two snapshots' disks, one run per line as its first block and its number of blocks, then a summary to standard error. Both disks must be the same size. Only the tree chunks on differing paths are read.

```
0 1
2304 2
2312 59
diff: 6638 blocks (26552 KiB) in 123 runs differ between snapshots 9c7a4d0b… and 3cd3aa9c…
```

## Writable disks

A writable disk is a directory, `disks/<name>` in the state directory, of numbered **write epochs** on the snapshot the disk sits on. The newest epoch is **active** and takes the writes. During a checkpoint, the epoch before it is **frozen** under it. Reads look in the active epoch, then in the frozen one, then in the snapshot. At most one epoch is frozen at a time.

Each epoch is a directory with three files: an append-only `data` file, an `index` of 36-byte checksummed records, and `super.json`. A record holds a whole block, a block of zeros, or a patch: part of a block whose contents were not at hand. A write that covers a whole block is recorded whole, and a zero write, trim or discard as a zero block. A flush fsyncs `data` and then `index`. When the disk opens, each epoch replays its index up to the first record that fails its checksum, so a crash loses only writes that no flush covered. Opening also removes epochs whose checkpoint had finished, and what an epoch created at the moment of a crash left. Details are in [SPEC §11](SPEC.md#11-write-epochs-phase-2).

A disk is locked while a serve or a checkpoint has it open: a second one stops with `disk … is in use`.

### Control socket

While serve holds a disk, it listens on `disks/<name>/control.sock`, mode 0600. A client sends one JSON line:

```json
{"request":"checkpoint"}
```

and receives the checkpoint's progress as `{"log":"…"}` lines, then `{"snapshot":"<64 hex>"}` or `{"error":"…"}`. Checkpoints run one at a time; a request made during one waits for it.

### Guest agent

`-qga` names the host end of the QEMU guest agent's channel, which QEMU makes with:

```
-chardev socket,path=<socket>,server=on,wait=off,id=qga0
-device virtio-serial
-device virtserialport,chardev=qga0,name=org.qemu.guest_agent.0
```

The guest must run `qemu-guest-agent`. Before each command, swarmblk sends a 0xFF byte and `guest-sync-delimited` with a random ID, and ignores everything before the matching answer. It then sends `guest-fsfreeze-freeze` before the cut and `guest-fsfreeze-thaw` after it, with a minute's timeout each.

## Lineages and keys

A lineage is a Bee sequence feed: update *n* names snapshot *n*'s manifest, and Bee's `GET /feeds/<owner>/<topic>` resolves the feed to its latest one. Its owner is the address of a secp256k1 key, created with the lineage and kept in `keys/<owner>.key`, in hex, mode 0600. swarmblk refuses a key file that other users have any access to. Without the key, a lineage can still be read and branched, but not continued. Only one host may hold a lineage's key, since Swarm cannot stop two writers from overwriting each other's updates.

## Postage

swarmblk stamps uploads with the batch given, and checks it first:

- The batch must be **usable** and **immutable**.
- Counting every chunk to upload as new, no bucket of the batch may overflow, or the upload is refused. A warning comes first when a bucket would pass 90%.

Every new chunk of a snapshot is stamped, including its manifest and the feed update that publishes it. Chunks identical to ones already stamped cost no new slots. A snapshot's `meta.json` lists the batches that stamp it and its ancestors. Its data stays on Swarm while those batches are funded.

## Boot traces

A boot trace lists, in first-read order, the blocks a boot reads, as runs of consecutive blocks. `serve -record` writes one, `serve -prefetch` uses one, and `checkpoint -prefetch` stores one with a snapshot as `prefetch.bin`. The file is little-endian: the magic `SBPF`, version 1, three zero bytes, then records of a uint64 first block and a uint32 count ([SPEC §6](SPEC.md#6-prefetch-trace-prefetchbin)).

## Snapshot format

A snapshot is a Mantaray manifest whose root names `meta.json` as its index:

| Path | Content |
|---|---|
| `disk.raw` | the disk, as Bee's chunk tree at redundancy level 0 |
| `meta.json` | the metadata below |
| `prefetch.bin` | the boot trace, if any |

Any Swarm tool can read it: `GET /bzz/<snapshot>/disk.raw` is the raw disk, and `GET /bzz/<snapshot>/` the metadata.

| `meta.json` field | Meaning |
|---|---|
| `format` | `swarmblk/1` |
| `size` | the disk's size in bytes |
| `logical_sector`, `physical_sector` | 512 and 4096 |
| `redundancy`, `encryption` | 0, and `none` |
| `disk_root` | the root of `disk.raw` |
| `parent` | the snapshot this one was made from, or null |
| `lineage` | `owner`, `topic` and `index` of its place in a lineage, or null |
| `created` | when it was made, in UTC |
| `consistency` | `crash`, or `application` after a guest-agent freeze |
| `batches` | the batches that stamp it and its ancestors |
| `prefetch` | `prefetch.bin`, or null |
| `stats` | `changed_blocks` since the parent, and `new_chunks` the parent lacked |

## Local state

| Path, in the state directory | Holds |
|---|---|
| `chunks/` | every chunk swarmblk has handled, checked on write, under two levels of directories named by its address; it grows without bound, and can be deleted while nothing runs |
| `disks/<name>/` | a writable disk: `lock`, `control.sock` while served, and its epochs `1/`, `2/`, …; deleting it loses the writes since its last snapshot |
| `keys/<owner>.key` | lineage keys; back them up, and never share them |
| `nbd.sock` | serve's default socket |

The state directory and `keys/` are created with mode 0700.

## Messages

| Message | Cause |
|---|---|
| `batch … is not usable` | the batch is new (usable a few minutes after purchase) or expired |
| `batch … is mutable; swarmblk stamps only with immutable batches` | buy batches with the `immutable: true` header |
| `…: bucket … would need … slots of …` | the upload could overflow the batch; use a deeper batch, or dilute this one |
| `disk size … is not a power of two`, `disk size 2^… is outside 2^26 to 2^40 bytes` | resize the image |
| `disk name …: use letters, digits, dots, dashes and underscores` | rename the disk |
| `disk … is new: name the snapshot or lineage it starts from` | give the first `serve -disk` a starting point |
| `disk … is on snapshot …, not …` | leave out the snapshot for an existing disk |
| `disk … is in use` | another serve or checkpoint has the disk open |
| `…: another server listens there` | another serve uses this socket; stop it, or pass `-socket` |
| `checkpoints need a batch to stamp with` | set `SWARMBLK_BATCH`, or pass `-batch` |
| `checkpoints are for a writable disk: serve it with -disk` | `-every` and `-qga` need `-disk` |
| `lineage …: not the lineage's head: …` | Bee does not see the latest update yet, or the lineage moved on |
| `… names neither a snapshot (64 hex digits) nor a lineage (0x<owner>/<topic>)` | check the argument |
| `… has mode …; a lineage key must be private to its user (chmod 600)` | `chmod 600` the key file |
| `Failed to get shared "write" lock` | the overlay's VM is still running |

## Limits

- Disks of 64 MiB to 1 TiB, a power of two in size.
- No encryption: anyone with a reference can read the disk. Encrypt inside the guest if needed.
- One writer per lineage, and one serve or checkpoint per disk at a time.
- The chunk store is unbounded, and there is no failure policy yet: a block that cannot be fetched is an I/O error in the guest.
- NBD over Unix sockets only, on Linux, with Bee 2.8.2 or newer.

## Developer tools

| Tool | Does |
|---|---|
| `tools/beeproxy` | a logging HTTP proxy in front of Bee, used in Phase 0 ([PHASE0.md](PHASE0.md)) |
| `cmd/chunkprobe` | measures chunk retrieval latency and throughput through a Bee node |
| `tools/crashtest` | the crash-consistency and cost test with a real guest ([results](results/phase2-crash-2026-10-26.md)) |

Each prints its own usage. `go test ./...` runs the unit tests, which need no network.
