# swarmblk user guide

This guide walks you through swarmblk from scratch. You put a small Linux virtual machine's disk on Swarm, boot it from there, run it with snapshots taken while it works, and bring it back on another computer. It assumes you are at home on a Linux command line, but not that you know how filesystems or Swarm work inside. Each idea is explained when you first need it.

Allow an hour or so. Most of it goes into installing and funding a Bee node, if you do not have one yet. The outputs shown are from a real run of this guide.

> **Before you start.** swarmblk is experimental: it is in Phase 2 of its [roadmap](../ROADMAP.md). Snapshots are not encrypted, so anyone who learns a snapshot's reference can read the whole disk. Keep secrets, and anything you cannot afford to lose, off it.

## 1. The ideas in brief

A disk, to the computer using it, is a long row of numbered blocks. swarmblk works in 4 KiB blocks. Swarm, a peer-to-peer storage network, stores data in 4 KiB pieces called **chunks**. Each chunk's address is a hash of its contents, so nobody can change a chunk without changing its address.

swarmblk stores each block of a disk as one chunk, and builds a tree of chunks above them that lists their addresses. The chunk at the top, the **root**, therefore names the whole disk: its address identifies every byte. Two blocks with the same contents are one chunk, so they are stored once. All blocks of zeros are the same chunk, so empty space costs nothing.

- A **snapshot** is a disk at one moment, with a little metadata, all on Swarm. Its **reference**, 64 hex digits, is all anyone needs to read it.
- A **lineage** is the series of snapshots of one disk over time, like a document's version history. It has an address of its own, written `0x<owner>/<topic>`, which leads to its latest snapshot. Only the computer that holds the lineage's key can add to it.
- **Bee** is the program that connects your computer to Swarm. To upload, it needs a **postage batch**: prepaid storage, bought with the xBZZ token. Your data stays on Swarm while the batch lasts.
- The **engine**, `swarmblk serve`, sits between the virtual machine and Swarm. It fetches the blocks the machine reads, checks each against its hash, and keeps the machine's writes on your own disk. A **checkpoint** turns the writes since the last snapshot into the next snapshot. It uploads only the blocks that changed, while the machine keeps running.

```
virtual machine (QEMU) ──NBD──> swarmblk serve ──> Bee ──> Swarm
                                      │
                                      └── your disk: writes since the last
                                          snapshot, and a cache of chunks
```

NBD, the Network Block Device protocol, is how QEMU, the virtual machine program, talks to the engine. You only ever copy the address that `serve` prints.

## 2. What you need

- **A Linux computer with KVM,** the processor's support for virtual machines. If `ls /dev/kvm` shows the file, you have it.
- **Some packages.** On Ubuntu or Debian:
  ```sh
  sudo apt install qemu-system-x86 qemu-utils qemu-block-extra cloud-image-utils jq curl bc
  ```
- **Go 1.26 or newer,** to build swarmblk ([go.dev/dl](https://go.dev/dl/)).
- **A Bee node, version 2.8.2 or newer,** running on this computer and funded with xDAI and xBZZ. [Swarm Desktop](https://github.com/ethersphere/swarm-desktop) sets one up for you; [docs.ethswarm.org](https://docs.ethswarm.org) explains running Bee yourself.

Build swarmblk, and put it somewhere on your `PATH`:

```sh
git clone https://github.com/petfold/swarmblk
cd swarmblk
go build -o ~/bin/swarmblk ./cmd/swarmblk
swarmblk          # prints the commands it knows
```

Tell swarmblk where Bee listens, and check Bee's version:

```sh
export SWARMBLK_BEE_URL=http://localhost:1633
curl -s $SWARMBLK_BEE_URL/health | jq -r .version
```

## 3. Buy a postage batch

Every chunk uploaded to Swarm carries a postage stamp from a batch. A batch has a **depth**, which sets how many chunks it can stamp, and a **balance**, which sets how long they stay on Swarm. swarmblk only uses **immutable** batches, which never overwrite old stamps when they fill up.

Depth 20 is a good size to start with. It holds a few hundred thousand chunks, a gigabyte or so of changed blocks, enough for this guide and several small disks. Smaller batches fill up unevenly and run out early. Work out the price for a week first:

```sh
PRICE=$(curl -s $SWARMBLK_BEE_URL/chainstate | jq -r .currentPrice)
DEPTH=20 DAYS=7
AMOUNT=$(( PRICE * 17280 * DAYS ))       # Gnosis Chain makes 17,280 blocks a day
echo "cost: $(echo "$AMOUNT * 2^$DEPTH / 10^16" | bc -l) xBZZ"
```

The price changes; in October 2026 this came to 2.49 xBZZ. If you are happy with the cost, buy the batch. This spends real xBZZ:

```sh
curl -s -X POST -H "immutable: true" $SWARMBLK_BEE_URL/stamps/$AMOUNT/$DEPTH | jq -r .batchID
export SWARMBLK_BATCH=<the batch ID it printed>
```

The batch becomes usable after a few minutes. Check with:

```sh
curl -s $SWARMBLK_BEE_URL/stamps/$SWARMBLK_BATCH | jq '{usable, batchTTL}'
```

`batchTTL` is the batch's remaining life in seconds. Swarm may drop your chunks once it runs out, so top the batch up in time (section 11).

## 4. Put a disk on Swarm

You need a disk image to start from. This guide uses CirrOS, a tiny Linux made for testing virtual machines, which downloads in seconds. Any raw disk image works the same way, for example an Ubuntu cloud image.

```sh
mkdir ~/swarmblk-tutorial && cd ~/swarmblk-tutorial
curl -LO https://download.cirros-cloud.net/0.6.3/cirros-0.6.3-x86_64-disk.img
qemu-img convert -O raw cirros-0.6.3-x86_64-disk.img cirros.raw
qemu-img resize -f raw cirros.raw 128M
```

The disk's size must be a power of two between 64 MiB and 1 TiB: 64M, 128M, 256M, and so on up to 1T. Resizing only adds empty space at the end, which costs nothing; CirrOS grows its filesystem into it when it first boots.

Now **import** it:

```sh
swarmblk import cirros.raw
```

swarmblk builds the tree on your computer, uploads each distinct chunk once, with the root last, and waits until the network has every one. For CirrOS that takes under a minute:

```
tree: 128 MiB disk, 33027 chunks, 5549 distinct, root 1e38c8e7…
snapshot: 9c7a4d0b…, 5511 blocks with data, 5554 chunks to upload
upload: 5554 chunks stored under tag 731813 in 9s; waiting for sync
sync: 5554 of 5554 synced after 34s
9c7a4d0b1bdb84ee7932adb9712e4226b7bc1bf39048fd0878f2e340532bf748
```

Of the disk's 32,768 blocks, only 5,511 hold any data, and only their chunks, with the tree above them, went up. The last line is the new snapshot's reference. Keep it:

```sh
SNAP=<the reference>
```

A snapshot is an ordinary Swarm upload, so plain Bee can show you its metadata:

```sh
curl -s $SWARMBLK_BEE_URL/bzz/$SNAP/meta.json | jq
```

```json
{
  "format": "swarmblk/1",
  "size": 134217728,
  "disk_root": "1e38c8e7…",
  "parent": null,
  "lineage": null,
  "created": "2026-10-26T17:10:44Z",
  "consistency": "crash",
  "batches": ["…"],
  "stats": { "changed_blocks": 5511, "new_chunks": 5549 },
  …
}
```

## 5. Boot it from Swarm

CirrOS, like most cloud images, reads its settings at boot from a small extra disk called a **seed**. Make one that just names the machine:

```sh
echo '{"instance-id": "tutorial-1", "local-hostname": "tutorial"}' > meta-data
touch user-data
cloud-localds seed.img user-data meta-data
```

Start the engine on the snapshot. It runs until you stop it with Ctrl-C:

```sh
swarmblk serve $SNAP
```

It prints the disk's address for QEMU:

```
nbd+unix:///9c7a4d0b…?socket=/home/you/.local/share/swarmblk/nbd.sock
```

This disk is read-only, so the virtual machine needs somewhere to put its own writes. A qcow2 **overlay** file does that: it reads through to the snapshot and keeps every change on your computer. In a second terminal:

```sh
URI='<the address serve printed>'
qemu-img create -f qcow2 -F raw -b "$URI" overlay.qcow2
qemu-system-x86_64 -enable-kvm -m 512 -nographic \
  -drive file=overlay.qcow2,if=virtio \
  -drive file=seed.img,if=virtio,format=raw,readonly=on
```

The machine boots in your terminal, in about 20 seconds:

```
login as 'cirros' user. default password: 'gocubsgo'. use 'sudo' for root.
tutorial login:
```

Log in, look around, and shut it down with `sudo poweroff`. Ctrl-A then X stops QEMU if you need to. Then stop `serve` with Ctrl-C.

The boot was quick because your computer still has every chunk from the import. A computer that has never seen the disk fetches each block from Swarm the first time the machine reads it, which is slower; section 8 shows how to speed that up.

## 6. Run a writable disk with live snapshots

This is swarmblk's main use. The virtual machine runs straight on a swarmblk disk, with no overlay. Its writes stay on your computer until a checkpoint turns them into the next snapshot, which you can take at any time while the machine runs.

Create a disk named `vm1`, starting from the snapshot. A name can use letters, digits, dots, dashes and underscores.

```sh
swarmblk serve -disk vm1 $SNAP
```

```
disk vm1: new, on snapshot 9c7a4d0b…
serve: a 128 MiB disk, writable, epoch 1 holding 0 blocks, on snapshot 9c7a4d0b…, policy lazy, as vm1
nbd+unix:///vm1?socket=/home/you/.local/share/swarmblk/nbd.sock
```

Boot the machine on it, in the second terminal, with the address `serve` printed:

```sh
URI='nbd+unix:///vm1?socket=/home/you/.local/share/swarmblk/nbd.sock'
qemu-system-x86_64 -enable-kvm -m 512 -nographic \
  -drive file="$URI",if=virtio,format=raw,discard=unmap \
  -drive file=seed.img,if=virtio,format=raw,readonly=on
```

`discard=unmap` lets the machine tell the disk which blocks it no longer needs; section 11 explains why that saves money. Log in and change something:

```sh
echo "hello from $(date)" | sudo tee /root/hello.txt
sync
```

Now take a **checkpoint** from a third terminal, while the machine keeps running:

```sh
swarmblk checkpoint -disk vm1
```

```
disk vm1: froze epoch 1 with 6611 blocks
disk vm1: 6611 blocks on snapshot 9c7a4d0b…, rebuilt in 2.419s
lineage 0x1bc0467a…/219d107a…: new, from snapshot 9c7a4d0b…
snapshot: e55a07f6…, 6593 blocks changed, 6387 chunks to upload
upload: 6387 chunks stored under tag 731814 in 19s; waiting for sync
sync: 6387 of 6387 synced after 49s
lineage 0x1bc0467a…/219d107a…: update 0 is snapshot e55a07f6…
e55a07f6e864564a53c795c50263b52569a51228a88b99ad95a91fc125595369
```

Here is what happened:

1. **The cut.** The engine held new writes for an instant and set aside every write so far, as an **epoch**. Every write the machine had finished is in it, and none is half in. New writes went on into a fresh epoch at once.
2. **The upload.** swarmblk rebuilt the tree with the changed blocks, and uploaded only the chunks that differ from the last snapshot's. Here most of them come from CirrOS's first boot, which grows its filesystem and sets itself up. It then waited until the network had all of them; until then, nobody could read the snapshot anyway.
3. **The lineage.** This was the disk's first checkpoint, so swarmblk created a lineage for it, with a new key kept on your computer, and published the snapshot as the lineage's update 0.

The lineage address in those lines, in full, is your disk's permanent name on Swarm:

```sh
LINEAGE=0x…/…
```

Change something more in the machine and checkpoint again. The next snapshot becomes update 1, and costs only what changed:

```
disk vm1: froze epoch 2 with 19 blocks
…
snapshot: ea64ec80…, 19 blocks changed, 24 chunks to upload
…
lineage 0x1bc0467a…/219d107a…: update 1 is snapshot ea64ec80…
```

Shutting the machine down writes a little more, so a checkpoint after `sudo poweroff` made update 2. One more after that had nothing new, and uploaded nothing:

```
disk vm1: no writes since snapshot 3cd3aa9c…
```

**Stopping and resuming.** Shut the machine down before you stop `serve`: without it, the machine's disk is gone, and its reads and writes fail. Ctrl-C then stops `serve` cleanly, and `swarmblk serve -disk vm1` later carries on from the same state. Once a disk exists, leave out the snapshot argument. If your computer crashes, every write the machine had flushed to disk is kept, and a checkpoint that was cut short is finished by the next one.

**Checkpoints on a schedule.** Start `serve` with `-every` to checkpoint the disk at that interval, whenever it has changed:

```sh
swarmblk serve -disk vm1 -every 15m
```

**Clean snapshots with the guest agent.** A live checkpoint captures the disk as if the power had been cut at that instant. Linux filesystems recover from that by replaying their journal when they next start, just as after a power cut. If you would rather have snapshots that need no recovery, let the QEMU guest agent pause the machine's filesystems around the cut. CirrOS has no agent, but Ubuntu and most cloud images can install it as the `qemu-guest-agent` package. Give QEMU a channel to the agent by adding these options:

```sh
  -chardev socket,path=/tmp/vm1-agent.sock,server=on,wait=off,id=qga0 \
  -device virtio-serial \
  -device virtserialport,chardev=qga0,name=org.qemu.guest_agent.0
```

and tell `serve` where it is:

```sh
swarmblk serve -disk vm1 -qga /tmp/vm1-agent.sock
```

Such snapshots say `"consistency": "application"` in their `meta.json`.

## 7. Bring it back on another computer

On a second computer, set up Bee and swarmblk as in section 2. Reading needs no batch. You can:

- **Boot the latest snapshot,** as in section 5, with the lineage in place of a snapshot reference:
  ```sh
  swarmblk serve $LINEAGE
  ```
- **Get an ordinary image file,** to use anywhere:
  ```sh
  swarmblk export $LINEAGE vm1.raw
  ```
  ```
  lineage 0x1bc0467a…/219d107a…: the head is update 1, snapshot ea64ec80…
  export vm1.raw: snapshot ea64ec80…, 128 MiB, 12072 blocks with data, sha256 c7742d80…, in 11.752s
  ```
  The same snapshot always gives the same SHA-256 hash, wherever you export it.
- **Carry on working there,** with a batch of its own:
  ```sh
  swarmblk serve -disk vm1 $LINEAGE
  ```
  Its first checkpoint starts a new lineage, because the original lineage's key stays on the first computer. You now have two independent **branches** of the disk.

**Keep a whole copy.** A disk normally fetches only the blocks its machine reads. For disaster recovery you may want all of it on the computer, so that it keeps working when the network does not. Give the disk the `hydrate` policy:

```sh
swarmblk serve -disk vm1 -policy hydrate $LINEAGE
```

`serve` then copies the whole disk in the background while the machine runs, fetching what the machine reads first:

```
disk vm1: hydrated, all 32768 blocks local; … fetched in …
```

From then on the disk starts and runs even with Bee unreachable; only checkpoints need Bee. Checkpoints keep the copy up to date, so nothing is fetched twice. The disk remembers its policy, and `-policy lazy` deletes the copy.

**Bring back the whole machine with a disk card.** The commands above bring back the disk. A **disk card** brings back the machine: a small file that names the disk's lineage and holds what QEMU needs to run it, such as its memory and network card, and its firmware settings and TPM, if it has them. Describe the machine in a file, `vm.json`:

```json
{"memory": 512, "nic": {"model": "virtio-net-pci"}}
```

and make the card on the first computer:

```sh
swarmblk card -disk vm1 -vm vm.json
```

```
card: uploading the machine's definition, 1 chunks
upload: 1 chunks stored under tag 731825 in 0s; waiting for sync
sync: 1 of 1 synced after 5s
card: machine vm1, following lineage 0x1bc0467a…/219d107a… from update 2, snapshot 3cd3aa9c…
vm1.card
```

The card follows the lineage, so it stays good as you take more snapshots; make a new one when the machine's description changes. Keep it off this computer, somewhere safe, such as a password manager: it is all a second computer needs, and whoever holds it can start the machine. On the second computer, with a batch of its own in `SWARMBLK_BATCH`:

```sh
swarmblk boot vm1.card -- -drive file=seed.img,if=virtio,format=raw,readonly=on
```

`boot` makes the disk on the lineage's latest snapshot, with the `hydrate` policy, and runs the machine in your terminal. Anything after `--` goes to QEMU, here the seed. When the machine shuts down, boot takes a checkpoint. It starts a new lineage, since the lineage's key stays on the first computer:

```
lineage 0x1bc0467a…/219d107a…: the head is update 2, snapshot 3cd3aa9c…
disk vm1: new, on snapshot 3cd3aa9c…
serve: a 128 MiB disk, writable, epoch 1 holding 0 blocks, on snapshot 3cd3aa9c…, policy hydrate, as vm1
boot: qemu-system-x86_64 -name vm1 -machine q35 -accel kvm -cpu host -smp 1 -m 512 …
disk vm1: hydrated, all 32768 blocks local; 11765 fetched in 2s
boot: the machine stopped after 14s
boot: checkpointing disk vm1
…
lineage 0x02422cf5…/7d0a4187…: update 0 is snapshot 1978769f…
```

Run the same command again to carry on with the disk as the machine left it. For a machine with UEFI firmware or a TPM, give `card` its firmware settings with `-nvram` and its TPM with `-vtpm` ([reference](REFERENCE.md#card)).

Bee finds a lineage's latest update by asking the network. Right after a checkpoint it may still find the one before for a minute or so, as in the export above, which ran just after update 2 was published. A snapshot's reference never changes, so use it when you need that exact snapshot.

**One writer per lineage.** Swarm has no locks, so if two computers published to the same lineage, each would overwrite the other's history. swarmblk keeps each lineage's key on the computer that created it, in `~/.local/share/swarmblk/keys`. Back that folder up, keep it private, and never copy a key to a second computer to run both at once.

## 8. Faster boots on a new computer

A computer that has never seen a disk must fetch every block from Swarm as the machine first asks for it. A boot reads thousands of blocks, mostly one after another, so a first boot is slow. Ubuntu took 37 minutes through a single light Bee node.

A **boot trace** is the list of blocks a boot reads. With one, the engine fetches those blocks in parallel, ahead of the machine, and the same Ubuntu boot took 9.7 minutes. Record a trace during any boot, best a fast one on the computer that made the disk:

```sh
swarmblk serve -record boot.trace $SNAP
```

Boot the machine as in section 5. When you stop `serve`, it writes the trace:

```
serve: wrote a boot trace of 5926 blocks in 120 runs to boot.trace
```

To use it on another computer:

```sh
swarmblk serve -prefetch boot.trace $LINEAGE
```

A snapshot can also carry a trace, which `serve` then uses without being asked. A disk's checkpoints keep the trace of the snapshot the disk started from, and the overlay checkpoints of the next section can store a new one.

## 9. Snapshots without the engine

There is a simpler way to work, without the engine, if you can stop the machine for each snapshot. `restore` makes an overlay that reads the snapshot straight from Bee, with QEMU's own HTTP client:

```sh
swarmblk restore $SNAP work.qcow2
qemu-system-x86_64 -enable-kvm -m 512 -nographic \
  -drive file=work.qcow2,if=virtio \
  -drive file=seed.img,if=virtio,format=raw,readonly=on
```

Once the machine has shut down, a checkpoint uploads what the overlay holds and moves the overlay onto the new snapshot:

```sh
swarmblk checkpoint work.qcow2
```

```
overlay: 7328 blocks on snapshot 9c7a4d0b…, rebuilt in 1.028s
lineage 0xb9fd078d…/a312b9df…: new, from snapshot 9c7a4d0b…
snapshot: 24b214a2…, 6608 blocks changed, 6401 chunks to upload
…
lineage 0xb9fd078d…/a312b9df…: update 0 is snapshot 24b214a2…
```

The first checkpoint starts a lineage, and later ones continue it; `-branch` starts a new one instead. `-prefetch boot.trace` stores a boot trace with the snapshot. While the machine runs, the checkpoint refuses, because QEMU holds the file. Cold boots this way are slower than through the engine, which fetches many blocks at once.

## 10. See what changed

`diff` lists the blocks that differ between two snapshots, each run of blocks as its first block number and its length, and sums them up:

```sh
swarmblk diff $SNAP $LINEAGE
```

```
lineage 0x1bc0467a…/219d107a…: the head is update 2, snapshot 3cd3aa9c…
0 1
2304 2
2312 59
…
diff: 6638 blocks (26552 KiB) in 123 runs differ between snapshots 9c7a4d0b… and 3cd3aa9c…
```

## 11. What it costs

**Storage.** A checkpoint uploads only the blocks that changed, plus a few tree chunks above them. Blocks of zeros, and blocks identical to ones already on Swarm, cost nothing. How much a given amount of work changes depends on the filesystem inside the machine. With the same workload of small files and frequent syncs, XFS uploaded about 1.2 times the data written, ext4 2.2 times, ZFS 2.3 times, and Btrfs 3.5 times ([details](results/phase2-crash-2026-10-26.md)).

**Deleted files.** A filesystem does not erase a deleted file's blocks, so they keep their old contents, and the next snapshot keeps paying for them. Run `sudo fstrim -av` in the machine from time to time. With `discard=unmap` in QEMU, the freed blocks then become zeros, which are free.

**The batch.** Check how full it is and how long it has left:

```sh
curl -s $SWARMBLK_BEE_URL/stamps/$SWARMBLK_BATCH | jq '{utilization, batchTTL}'
```

`utilization` counts the chunks in the batch's fullest bucket; a depth-20 batch's buckets hold 16 each. Once one is full, uploads that land in it fail, so buy a new batch before that. To add time:

```sh
curl -s -X PATCH $SWARMBLK_BEE_URL/stamps/topup/$SWARMBLK_BATCH/$AMOUNT
```

**Reading.** Bee pays other nodes for the chunks it fetches beyond a free allowance. In October 2026 a light node paid about 0.2 xBZZ for each GiB it read.

## 12. Safety

- **Snapshots are public.** They are not encrypted, and a reference or lineage address is all it takes to read the whole disk. Encrypt inside the machine, for example with LUKS full-disk encryption, if you need privacy.
- **Keys are the lineage.** Without a lineage's key, you can still read and branch it, but not add to it. Back up `~/.local/share/swarmblk/keys`.
- **Data lasts as long as its postage.** When a batch runs out, Swarm may drop its chunks.
- **If Swarm cannot deliver a block,** the machine waits for it and seems to freeze, and `serve` prints an `ALARM` line every minute until the block arrives. `serve -alarm` also runs a command of yours, such as one that sends you a message, and `-failure error` makes such reads fail instead.
- **Disk cards are keys.** Whoever holds a card can start the machine and read what its TPM keeps. Store cards like passwords.

## 13. When something goes wrong

| Message | Meaning | What to do |
|---|---|---|
| `batch … is not usable` | The batch is brand new, or has run out | Wait a few minutes after buying, or buy a new one |
| `batch … is mutable` | swarmblk only stamps with immutable batches | Buy one with the `immutable: true` header |
| `disk size … is not a power of two` | The image has the wrong size | `qemu-img resize -f raw` it to 64M, 128M, 256M … |
| `disk vm1 is new: name the snapshot or lineage it starts from` | The first `serve -disk` needs a starting point | Add the snapshot or lineage |
| `disk vm1 is on snapshot …, not …` | An existing disk was given a different snapshot | Leave the argument out |
| `disk vm1 is in use` | Another `serve` or `checkpoint` has the disk open | Use that one, or stop it |
| `…/nbd.sock: another server listens there` | Another `serve` is still running on the same socket | Stop it, or give this one `-socket other.sock` |
| `checkpoints need a batch to stamp with` | No batch was given | Set `SWARMBLK_BATCH`, or pass `-batch` |
| `Failed to get shared "write" lock` | The overlay's machine is still running | Shut it down, then checkpoint |
| `lineage …: not the lineage's head: …` | Bee does not see the latest update yet, or the lineage moved on | Try again in a minute, or use `-branch` |
| `serve: disk vm1: ALARM: block … cannot be fetched from Swarm: …` | Swarm cannot deliver a block the machine needs, and the machine waits for it | Check that Bee runs and has peers, and that the batch is funded; the machine carries on when the block arrives |
| `lineage …: the head reads as update …, older than the card's update …` | Bee has not found the lineage's latest update yet | Try `boot` again in a minute |

**The machine starts slowly** on a computer that has not seen the disk before: record and use a boot trace (section 8).

## Where swarmblk keeps things

All its local state is in `~/.local/share/swarmblk`, or wherever `SWARMBLK_HOME` points:

| Path | Holds |
|---|---|
| `chunks/` | a cache of the chunks swarmblk has handled, checked against their addresses; `serve` keeps it under 4 GiB, or the size `-cache` gives; safe to delete while nothing runs |
| `disks/<name>/` | a writable disk's writes since its last snapshot, its local copy if it has one, and the settings of a machine that `boot` runs; deleting it loses the writes |
| `keys/` | your lineage keys; back it up |
| `nbd.sock` | where `serve` listens unless told otherwise |

## Words used here

| Word | Meaning |
|---|---|
| block | 4 KiB of a disk, the unit swarmblk stores and reads |
| chunk | 4 KiB on Swarm, named by the hash of its contents |
| root | the chunk at the top of a disk's tree; its address names the whole disk |
| snapshot | a disk at one moment, with its metadata, on Swarm |
| reference | a snapshot's 64-hex-digit address |
| lineage | the series of snapshots of one disk; its address `0x<owner>/<topic>` leads to the latest |
| epoch | the writes a disk has collected on your computer since its last cut |
| checkpoint | turning the writes since the last snapshot into the next one |
| boot trace | the blocks a boot reads, for fetching them ahead of time |
| disk card | a small, secret file that brings a whole machine back from Swarm |
| postage batch | prepaid storage that stamps uploads |
| Bee | the program that connects a computer to Swarm |
| NBD | the protocol QEMU uses to reach the engine |

Every command, option and message is in the [reference](REFERENCE.md). For how swarmblk works inside, see [DESIGN.md](DESIGN.md), and for its formats, [SPEC.md](SPEC.md).
