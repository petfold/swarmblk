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
| [`card`](#card) | makes a disk card, which restores a machine from the bare network |
| [`boot`](#boot) | runs a disk card's machine with QEMU, on a writable disk |
| [`iolock`](#iolock) | runs a command, such as Bee, as an I/O flusher |

## Conventions

**Options come before arguments:** `swarmblk serve -disk vm1 <snapshot>`, not `swarmblk serve <snapshot> -disk vm1`. Every command takes:

| Option | Default | Meaning |
|---|---|---|
| `-bee url` | `$SWARMBLK_BEE_URL`, else `http://localhost:1633` | the Bee node's API; Bee 2.8.2 or newer |
| `-home dir` | `$SWARMBLK_HOME`, else `~/.local/share/swarmblk` | the [local state](#local-state) |

Commands that upload also take `-batch id`, by default `$SWARMBLK_BATCH`: the [postage batch](#postage) to stamp with.

**Naming snapshots.** A snapshot is named by its reference: 64 hexadecimal digits, the address of its manifest. A lineage is named `0x<owner>/<topic>`, 40 and 64 lowercase hex digits, and stands for its latest snapshot, its **head**. swarmblk finds the head through Bee's feed lookup, which can lag a fresh update by a minute or so.

**Disk names** are letters, digits, dots, dashes and underscores, starting with a letter or digit.

**Output.** A command's result goes to standard output as one line: a snapshot reference, for `serve` an NBD URI, and for `card` the card's file name. `boot` gives standard output to QEMU. Progress and errors go to standard error, errors prefixed `swarmblk:`. The exit status is 0 on success, 1 on an error, and 2 for a usage error, after which the usage is printed.

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
| `-cache size` | `4G` | bound the local chunk cache: at start and every 5 minutes, remove the leaves used least recently; `0` for no bound. Sizes take `K`, `M`, `G` or `T` |
| `-policy policy` | the disk's own, else `lazy` | the disk's [hydration policy](#hydration-policies): `lazy`, `hydrate` or `restamp`; it is recorded with the disk |
| `-failure policy` | `block` | when Swarm cannot deliver a block: `block` the read and raise an alarm, or return an I/O `error` ([Failures](#failures)) |
| `-alarm command` | none | a shell command to run when an alarm is raised or cleared ([Failures](#failures)) |
| `-iolock` | off | run as an I/O flusher with locked memory, for a host whose kernel mounts the disk ([iolock](#iolock)) |

`-every`, `-qga` and `-policy` need `-disk`, and `-every` needs a batch. serve refuses a socket that another server listens on, and replaces one that nobody does.

**The NBD export.** Fixed-newstyle handshake, structured replies, and block status through the `base:allocation` context, with zero blocks reported as holes. Block sizes: 512 bytes minimum, 4096 preferred, 32 MiB the largest request. Flush and FUA are supported. A writable export also takes trim and write-zeroes; a read-only one refuses writes.

**Reads.** serve fetches each 4 KiB chunk from Bee once, up to 64 at a time, checks it against its address, and keeps it in the local chunk store. It keeps every intermediate chunk of the tree in memory, and answers zero subtrees without fetching. A failed fetch is tried four times in all, after 0.5, 1 and 2 s; a chunk that fails its check is not tried again. A read that still fails waits and tries again, or fails, as `-failure` says ([Failures](#failures)).

**Prefetch.** If the snapshot carries a boot trace, or `-prefetch` names one, serve fetches its blocks in trace order in the background, at most `-window` blocks ahead of the furthest trace block the guest has read, with up to half of the 64 fetches.

**Writable disks.** With `-disk`, serve opens the disk's [epochs](#writable-disks), recovering them after a crash, and locks the disk. A new disk needs the snapshot or lineage it starts from. For an existing disk, a snapshot given must be the one it sits on, so it is best left out. Writes go to the active epoch; a flush, or a write with FUA, returns once the epoch is durable. serve also listens on the disk's control socket for [checkpoint requests](#control-socket). If a checkpoint stopped part way and a batch is set, serve finishes it at start.

Its messages, from several runs:

```
disk vm1: new, on snapshot 9c7a4d0b…
serve: prefetching 5926 blocks of the boot trace, up to 16384 ahead of the guest
serve: a 128 MiB disk, writable, epoch 1 holding 0 blocks, on snapshot 9c7a4d0b…, policy lazy, as vm1
serve: chunk cache: removed 11298 leaves (44 MiB) used least recently; 3 MiB left
serve: stopped after 12m49s; 0 chunks fetched from Bee
serve: prefetched 5926 of 5926 trace blocks, 0 failed
serve: wrote a boot trace of 5926 blocks in 120 runs to boot.trace
```

## checkpoint

```
swarmblk checkpoint [-bee url] [-batch id] [-home dir] -disk name
swarmblk checkpoint [-bee url] [-batch id] [-home dir] [-branch] [-prefetch file] overlay.qcow2
```

Turns the changes of a writable disk, or of a stopped VM's qcow2 overlay, into the next snapshot, and prints its reference. With nothing changed, it uploads nothing and prints the snapshot the disk or overlay already sits on, unless `-prefetch` names a boot trace the snapshot lacks: then the next snapshot has the same disk and the new trace, a few chunks to upload.

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

## card

```
swarmblk card [-bee url] [-batch id] [-home dir] [options] -disk name | lineage | snapshot
```

Makes a **disk card**, a small JSON file that restores a machine from the bare network ([Disk cards](#disk-cards)), and prints its file's name. The card names the disk's lineage or snapshot, and the machine's definition, UEFI variables and TPM state, which card uploads encrypted, stamped with the batch, and waits until Bee has synced.

| Option | Default | Meaning |
|---|---|---|
| `-disk name` | | the card is for this writable disk, and follows the lineage its checkpoints continue |
| `-name name` | the disk's name | the machine's name, which `boot` gives its disk |
| `-vm file` | the disk's `vm/vm.json` | the [VM definition](#vm-definitions) |
| `-nvram file` | the disk's `vm/nvram.fd`, if any | the machine's UEFI variables, such as libvirt's `/var/lib/libvirt/qemu/nvram/<machine>_VARS.fd` |
| `-vtpm dir` | the disk's `vm/tpm`, if any | the machine's swtpm state directory, such as libvirt's `/var/lib/libvirt/swtpm/<uuid>/tpm2` |
| `-o file` | `<name>.card` | where to write the card, mode 0600 |

The defaults in the disk's `vm/` directory are what [`boot`](#boot) left there, so a machine that boot runs needs only `swarmblk card -disk name`.

**What the card follows.** Given a lineage, the card follows it from its head. Given a snapshot, it stays on that snapshot. Given `-disk`, it follows the disk's lineage if this host holds the lineage's key; otherwise the disk's next checkpoint starts a lineage, and the card stays on the disk's snapshot until it is made again after that checkpoint:

```
card: disk vm1 sits on snapshot …, in no lineage this host writes, so the card stays on it; its next checkpoint starts a lineage, and a card made after that follows it
```

A card that follows a lineage stays good as the disk moves on: boot takes the lineage's head. Make a new card when the machine's definition, firmware settings or TPM state change, ideally while the machine is stopped, since card copies the files as they are. Progress:

```
card: uploading the machine's definition, UEFI variables, 137 chunks
upload: 137 chunks stored under tag 731824 in 0s; waiting for sync
sync: 137 of 137 synced after 5s
card: machine dr-uefi, following lineage 0xcdfeac70…/cab06b02… from update 1, snapshot 2a3e915e…
```

Without UEFI variables a UEFI machine starts from the firmware's defaults, and without TPM state a machine gets a new TPM, which cannot unseal what the old one sealed; card warns of both. **Keep cards private and offline, like keys:** whoever holds one can boot the machine and read its TPM's secrets.

## boot

```
swarmblk boot [-bee url] [-home dir] [options] card [-- qemu-arguments]
```

Runs a disk card's machine with QEMU until the machine stops, on a writable disk that boot serves itself. Arguments after the card, after `--`, go to QEMU last, such as `-drive file=seed.img,if=virtio,format=raw,readonly=on` or `-vnc :1`.

| Option | Default | Meaning |
|---|---|---|
| `-disk name` | the card's name | the writable disk to run the machine on |
| `-snapshot snapshot\|lineage` | the card's | where a new disk starts, instead of the card's lineage head or snapshot |
| `-policy policy` | the disk's own, `hydrate` for a new disk | the disk's [hydration policy](#hydration-policies) |
| `-batch id` | `$SWARMBLK_BATCH` | the batch that the disk's checkpoints stamp with; without one, there are none |
| `-every interval` | off | checkpoint the disk at this interval, as serve does |
| `-cache size`, `-failure policy`, `-alarm command` | as for serve | as for [serve](#serve) |
| `-qemu binary` | `qemu-system-<arch>` | QEMU, for the definition's `arch` |
| `-firmware file` | OVMF's | UEFI firmware code, for a UEFI machine |
| `-swtpm binary` | `swtpm` | swtpm, for a machine with a TPM |
| `-display how` | `serial` | `serial`: the machine's serial console on this terminal, through QEMU's `-nographic`; otherwise a QEMU `-display` value, such as `gtk`, `vnc=:1` or `none` |
| `-nic backend` | `user` | QEMU's network backend for the machine's NIC: `user` is QEMU's own NAT; a bridge is `bridge,br=br0` |

In turn, boot:

1. reads the card, and fetches and decrypts the VM definition;
2. makes the disk, if it is new, on the head of the card's lineage, which must be at the card's update or later, or on the card's snapshot; a disk that exists runs as it is, with the writes it holds;
3. writes the machine's UEFI variables and TPM state to the disk's `vm/` directory from the card, the first time only, since the machine changes them as it runs. A UEFI machine without variables starts from OVMF's template, and a machine with a TPM but no state gets a new TPM;
4. serves the disk on `disks/<name>/nbd.sock`, as `serve -disk` would, and starts swtpm for a TPM;
5. runs QEMU, with its console and messages on boot's standard output and error. boot's own messages, and serve's, go to standard error;
6. when the machine stops by itself and boot has a batch, checkpoints the disk.

On SIGINT or SIGTERM, boot presses the machine's power button through QEMU's monitor, waits up to 2 minutes for it to shut down, and then stops QEMU. A disk from a card starts a new lineage at its first checkpoint, since the card holds no lineage key. A UEFI machine needs OVMF (Ubuntu's `ovmf` package): boot looks for it in `/usr/share/OVMF`, `/usr/share/edk2/ovmf`, `/usr/share/edk2/x64` and `/usr/share/qemu`, and takes the build whose variables have the size of the machine's. A machine with a TPM needs swtpm (Ubuntu's `swtpm` package).

The QEMU command line comes from the [VM definition](#vm-definitions):

| From | QEMU arguments |
|---|---|
| always | `-name`, `-machine`, `-accel kvm` with `-cpu host` where KVM can run the machine, else `-accel tcg` with `-cpu max`, `-smp`, `-m`, and `-qmp unix:vm/qmp.sock,server=on,wait=off` |
| `firmware: "uefi"` | `-drive if=pflash` for the firmware code, read-only, and for `vm/nvram.fd` |
| `secure_boot` | `smm=on` on the machine, and `-global driver=cfi.pflash01,property=secure,value=on` |
| `disk` | the NBD export as drive `disk0`, with `discard=unmap,detect-zeroes=unmap`, on `virtio-blk-pci`, on `scsi-hd` behind `virtio-scsi-pci`, or on `ide-hd` for SATA; first in the boot order |
| `nic` | `-nic <backend>,model=…,mac=…`, or `-nic none` |
| `tpm` | `-chardev socket` to swtpm's `vm/swtpm.sock`, `-tpmdev emulator`, and the TPM device |
| `agent` | the guest agent's channel on `vm/qga.sock`, which boot's checkpoints use to freeze the guest |
| `args` | appended, then boot's own QEMU arguments |

From the rehearsal ([results](results/phase2-dr-2026-10-27.md)):

```
lineage 0xcdfeac70…/cab06b02…: the head is update 1, snapshot 2a3e915e…
disk dr: new, on snapshot 2a3e915e…
serve: a 128 MiB disk, writable, epoch 1 holding 0 blocks, on snapshot 2a3e915e…, policy hydrate, as dr
boot: qemu-system-x86_64 -name dr -machine q35 -accel kvm -cpu host -smp 1 -m 512 -drive file=nbd+unix:///dr?socket=…
disk dr: hydrated, all 32768 blocks local; 10115 fetched in 12s
boot: the machine stopped after 22s
boot: checkpointing disk dr
```

Other lines: `boot: disk … exists, so the machine runs on it as it is`; `boot: the card holds no UEFI variables, so the machine starts from …`; `boot: the card holds no TPM state, so the machine gets a new TPM`; `boot: asked the machine to shut down; waiting up to 2m0s`; `boot: the checkpoint failed, so the writes stay in the disk's epochs until the next one: …`.

## iolock

```
swarmblk iolock -- command [arguments]
```

Runs a command as an **I/O flusher** (`PR_SET_IO_FLUSHER`, Linux 5.6 or newer): the kernel lets it allocate memory without waiting for writeback. This matters only when the host's own kernel mounts a swarmblk disk, through its NBD client (`nbd-client`, or `qemu-nbd -c`). The engine and Bee are then in the kernel's writeback path, and under memory pressure, writeback can wait for them while they wait for memory. VMs need none of this: QEMU's NBD client runs in user space.

Use it to start Bee, for example in a systemd unit:

```
ExecStart=/usr/local/bin/swarmblk iolock -- /usr/local/bin/bee start --config /etc/bee/bee.yaml
MemoryMin=1G
```

`serve -iolock` makes serve an I/O flusher too, and locks its memory (`mlockall`), so that none of it is ever swapped out. serve starts itself again to do it, since the flag belongs to a thread and Go starts threads before it can be set. Memory locks end when a process starts another program, so a Bee started through `iolock` keeps the flag but not locked memory: protect it with its service manager, as `MemoryMin=` does above. Never swap onto a swarmblk disk.

The flag needs `CAP_SYS_RESOURCE`, and locked memory `CAP_IPC_LOCK`. Give them to the binary, or to the service with systemd's `AmbientCapabilities=CAP_SYS_RESOURCE CAP_IPC_LOCK`:

```sh
sudo setcap cap_sys_resource,cap_ipc_lock+ep /usr/local/bin/swarmblk
```

Without them, both refuse: `-iolock needs CAP_SYS_RESOURCE and CAP_IPC_LOCK: sudo setcap cap_sys_resource,cap_ipc_lock+ep …`.

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

## Hydration policies

Each writable disk has one, recorded in `disks/<name>/policy` and set with `serve -disk … -policy` ([DESIGN §3](DESIGN.md#3-read-path), [SPEC §11](SPEC.md#11-write-epochs-phase-2)):

| Policy | The disk | Suits |
|---|---|---|
| `lazy` | reads what the guest asks for, and its trace, through the shared, bounded chunk cache | golden images, CI and forensics, where most blocks are never read |
| `hydrate` | keeps a whole local copy in `disks/<name>/image/`, filled in the background | disaster recovery: a complete copy needs no network, nor the batch |
| `restamp` | stamps every chunk of its snapshot again under serve's batch, once, keeping none of them | adopting a disk whose snapshot someone else's batch pays for |

**hydrate.** The copy is a sparse file of the disk's size with a map of the blocks it holds. Reads take a block from it first and put what they fetch in it. A background hydration fetches the rest in disk order, 16 blocks at a time, without passing them through the cache, and tries again a minute after a run that left blocks out. A checkpoint writes its blocks into the copy before it moves the disk on, so the copy never fetches them again. A complete copy also answers block status, so the disk starts and runs with Bee unreachable; only checkpoints need it. Turning the policy off deletes the copy. Messages:

```
disk dr: hydrating, … of 32768 blocks local (…%), … failed
disk dr: hydrated, all 32768 blocks local; 5191 fetched in 7s
disk dr: hydration left … of 32768 blocks out, … could not be fetched; trying again in a minute
disk dr: policy lazy; removing its local copy
```

**restamp.** serve lists every distinct chunk of the disk's snapshot: its manifest, metadata, boot trace and disk tree. It uploads them all under its batch over one stream, fetching each from the cache or else from Bee without keeping it, and waits until Bee has synced them. It then records the snapshot in `disks/<name>/restamp.json`. A checkpoint moves the record on to its new snapshot, whose new chunks it stamped itself, so the job runs once per disk. Uploading chunks under a second batch only matters when they carry stamps from another batch, such as a snapshot someone else imported. Messages:

```
disk vm: re-stamping the … chunks of snapshot … with batch …
disk vm: re-stamped snapshot … in …
```

## Failures

When Swarm cannot deliver a block after a fetch's four tries, the read follows serve's `-failure` policy:

- `block`, the default: the read waits and tries again after 1, 2, 4, 8 and 16 seconds, and then every 30 seconds, until Swarm delivers the block or the guest's client disconnects. Other reads go on meanwhile. The guest sees a slow disk rather than an I/O error, which a filesystem such as ext4 answers by remounting read-only.
- `error`: the read fails with an I/O error.

A hydrated disk's local copy serves its blocks either way. Under `block`, serve raises an **alarm** when the first read starts waiting, repeats it every minute while reads wait, and clears it when none waits any more:

```
serve: disk vm1: ALARM: block 1030 cannot be fetched from Swarm: …; the guest waits until it can
serve: disk vm1: ALARM: 2 reads have waited 1m0s for Swarm: …
serve: disk vm1: reads go through again, after 1m42s
serve: disk vm1: no read waits for Swarm any more, since their clients are gone, after 3m5s
```

With `-alarm command`, serve also runs `sh -c command` for each of these lines, in the background, with the line in `$SWARMBLK_ALARM` and `raised` or `cleared` in `$SWARMBLK_ALARM_STATE`; for example `-alarm 'notify-send swarmblk "$SWARMBLK_ALARM"'`. A command that fails is reported as `serve: the alarm command failed: …`.

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

## Disk cards

A disk card is a JSON file, mode 0600, that names a machine's disk and holds what boot needs to run it ([SPEC §12](SPEC.md#12-disk-card)):

| Field | Meaning |
|---|---|
| `format` | `swarmblk-card/1` |
| `name` | the machine, and its disk's name |
| `created` | when the card was made, in UTC |
| `lineage` | `owner`, `topic`, and the `index` of `snapshot` in it; null for a card that stays on its snapshot |
| `snapshot` | the disk's snapshot when the card was made |
| `batches` | the batches that stamp the snapshot, its ancestors and the card's files |
| `vm` | `definition`, `nvram` and `vtpm`: the machine's files, or null for those it lacks |

The machine's files are on Swarm under Bee's chunk encryption. Each reference is 128 hexadecimal digits: the address and then the key, so `GET /bytes/<reference>` returns the file decrypted, and the card is as secret as its machine's TPM. A card is not signed, and holds no lineage key.

### VM definitions

A VM definition is a JSON file that describes the machine to QEMU. Fields left out take their defaults, which card fills in before upload:

| Field | Default | Meaning |
|---|---|---|
| `format` | `swarmblk-vm/1` | |
| `arch` | `x86_64` | QEMU's target: boot runs `qemu-system-<arch>` |
| `machine` | `q35` | QEMU's machine type |
| `cpu` | `host` with KVM, else `max` | QEMU's CPU model |
| `cpus` | 1 | virtual CPUs |
| `memory` | 1024 | memory in MiB |
| `firmware` | `bios` | `bios`, or `uefi` with OVMF |
| `secure_boot` | false | UEFI with Secure Boot |
| `tpm` | none | QEMU's TPM device, such as `tpm-crb` or `tpm-tis`, backed by swtpm |
| `disk` | `virtio` | the disk's bus: `virtio`, `scsi` or `sata` |
| `nic` | none | `{"model": "virtio-net-pci", "mac": "52:54:00:12:34:56"}`: QEMU's NIC model and its MAC address, which some guests' network settings match on |
| `agent` | false | a channel for the QEMU guest agent |
| `args` | none | more QEMU arguments |

A small Linux server:

```json
{"memory": 2048, "cpus": 2, "firmware": "uefi", "nic": {"model": "virtio-net-pci", "mac": "52:54:00:12:34:56"}, "agent": true}
```

## Local state

| Path, in the state directory | Holds |
|---|---|
| `chunks/` | every chunk swarmblk has handled, checked on write, under two levels of directories named by its address; `serve -cache` bounds it, and it can be deleted while nothing runs |
| `disks/<name>/` | a writable disk: `lock`, `control.sock` while served, its epochs `1/`, `2/`, …, its `policy`, and `image/` or `restamp.json` as that policy keeps them; deleting it loses the writes since its last snapshot |
| `disks/<name>/vm/` | the machine that `boot` runs on the disk: `vm.json`, `nvram.fd`, `tpm/`, `swtpm.log`, and the sockets `qmp.sock`, `qga.sock` and `swtpm.sock`; boot's NBD socket is `disks/<name>/nbd.sock` |
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
| `policy …: use lazy, hydrate or restamp` | check the policy's name |
| `hydration policies are for a writable disk: serve it with -disk` | `-policy` needs `-disk` |
| `disk …: re-stamping needs a batch to stamp with` | set `SWARMBLK_BATCH`, or pass `-batch` |
| `size …: use bytes, or a number with K, M, G or T` | check `-cache` |
| `failure policy …: use block or error` | check `-failure` |
| `… needs CAP_SYS_RESOURCE and CAP_IPC_LOCK: sudo setcap …` | give the binary the capabilities, as the message says ([iolock](#iolock)) |
| `name the machine with -name`, `name the VM definition with -vm` | card needs both, unless `-disk` names a disk that boot ran |
| `vm definition: …` | the [VM definition](#vm-definitions) has a field format 1 does not allow |
| `UEFI variables are for a machine with UEFI firmware`, `TPM state is for a machine with a TPM` | set `firmware` or `tpm` in the definition, or leave out `-nvram` or `-vtpm` |
| `card: …` | the card file is damaged, or of another format |
| `… has mode …, but a card holds keys: keep it private (chmod 600)` | a warning: `chmod 600` the card |
| `lineage …: the head reads as update …, older than the card's update …` | Bee's lookup lags: try again, or boot the card's own snapshot with `-snapshot` |
| `disk … exists: -snapshot is for a new disk` | leave out `-snapshot`, or boot with another `-disk` name |
| `no OVMF firmware here: install OVMF, or name its code file with -firmware` | install OVMF (Ubuntu's `ovmf` package), or pass `-firmware` |
| `no OVMF build here reads UEFI variables of … bytes` | install the OVMF build the machine used, or pass its code file with `-firmware` |
| `the machine has a TPM: exec: "swtpm": …` | install swtpm, or pass `-swtpm` |

## Limits

- Disks of 64 MiB to 1 TiB, a power of two in size.
- No encryption: anyone with a reference can read the disk. Encrypt inside the guest if needed.
- One writer per lineage, and one serve or checkpoint per disk at a time.
- A disk card is not signed and holds its keys in the clear: keep it like a private key.
- NBD over Unix sockets only, on Linux, with Bee 2.8.2 or newer.

## Developer tools

| Tool | Does |
|---|---|
| `tools/beeproxy` | a logging HTTP proxy in front of Bee, used in Phase 0 ([PHASE0.md](PHASE0.md)) |
| `cmd/chunkprobe` | measures chunk retrieval latency and throughput through a Bee node |
| `tools/crashtest` | the crash-consistency and cost test with a real guest ([results](results/phase2-crash-2026-10-26.md)) |
| `cmd/swarmblk-wasm` | the engine for browsers, built with `GOOS=js GOARCH=wasm`: in a Service Worker, it serves a snapshot's disk to an emulator in the page by HTTP range requests, checking every chunk and keeping chunks in the browser's cache, and records the guest's reads as a boot trace ([results](results/phase2-browser-2026-10-27.md)) |
| `web/` | the page and Service Worker that boot a snapshot's disk in a browser tab with v86, through `cmd/swarmblk-wasm` ([README](../web/README.md)) |

Each prints its own usage. `go test ./...` runs the unit tests, which need no network.
