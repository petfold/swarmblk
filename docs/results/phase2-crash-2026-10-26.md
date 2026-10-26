# Crash consistency with a real guest — 26 October 2026

The crash test of ROADMAP Phase 2, run with `tools/crashtest` on node A. The guest is Ubuntu 24.04 (kernel 6.8.0-147, ZFS 2.2.2, xfsprogs 6.6.0, btrfs-progs 6.6.3, e2fsprogs 1.47.0) with 2 vCPUs and 2 GiB. Its second disk is a 1 GiB disk that `swarmblk serve -disk` serves writable, with discards passed through. Each filesystem was made on it with its defaults, aligned to 4 KiB: `mkfs.ext4`, `mkfs.xfs`, `mkfs.btrfs`, and `zpool create -o ashift=12` on the whole disk. Each base holds a 16 MiB file of random data and the workload's log, and was checkpointed once, for 4,255 to 4,701 chunks.

## Method

- **Workload.** Each iteration does one of three operations at random: create a file of 4–64 KiB (50%), overwrite a 64 KiB slot of the 16 MiB file in place (35%), or delete a file (15%). It fsyncs the data and the directory, then commits the operation in a log, also fsynced, and reports it on the serial console. Overwrites and deletes record their intent first, so a crash in between leaves only that slot or file in doubt.
- **Quick runs.** A kill comes 1–15 s into the workload. It is either QEMU alone, a power cut to the guest, or the engine (`serve`, SIGKILL) under the running guest followed by QEMU. A checker VM then reads the disk through the engine; after an engine kill, a new `serve` recovers the epochs first. The checker mounts the disk, which replays the journal or log, and verifies every committed operation and that nothing else is there. Then it runs the filesystem's own checker: `e2fsck -fn`, `xfs_repair -n`, `btrfs scrub` and `btrfs check --readonly`, or `zpool scrub` and `zpool status`.
- **Full runs.** A live checkpoint comes 2–10 s into the workload, while the guest writes. A kill follows 1–8 s later: QEMU, QEMU and the engine, or the engine. Then a second checkpoint takes what is left, through the running `serve` or, after an engine kill, by `checkpoint -disk` opening the disk itself. Both snapshots are exported through node B from an empty store and checked in the same way.
- **Pass.** A disk passes when its checker is clean, nothing is out of place, and the log holds at least every operation the guest reported before the cut or the kill.

## Results

| | ext4 | XFS | Btrfs | ZFS |
|---|---|---|---|---|
| Quick runs passed: QEMU killed, engine killed | 10/10, 10/10 | 10/10, 10/10 | 10/10, 10/10 | 10/10, 10/10 |
| Operations verified in quick runs | 5,883 | 6,993 | 3,814 | 5,551 |
| Full runs passed, both snapshots | 3/3 | 3/3 | 3/3 | 3/3 |
| Operations verified in full runs' snapshots | 598 | 859 | 732 | 838 |

All 80 quick runs and all 24 snapshots of the 12 full runs passed: no operation that the guest had reported fsynced was lost, and every checker was clean. In 14 quick runs and one full run, the kill cut an overwrite in half; the slots around it verified. In 12 quick runs, the log held operations that the console had not yet shown. When the engine was killed under the guest, the guest's next fsync failed with an I/O error, and everything it had reported before was in the recovered epoch. The campaign took 1 hour 56 minutes: quick runs 45 s each (90 s with ZFS, whose checker scrubs the pool), and full runs 3–7 minutes.

## What checkpoints upload

The same workload, 200 iterations with one seed, ran twice on each filesystem. A checkpoint followed each round, with the guest powered off.

| | ext4 | XFS | Btrfs | ZFS |
|---|---|---|---|---|
| Blocks changed, rounds 1 and 2 | 4,081, 4,015 | 2,107, 2,287 | 11,484, 14,262 | 4,423, 4,657 |
| Chunks uploaded | 4,069, 4,019 | 2,157, 2,342 | 6,316, 6,705 | 4,114, 4,328 |
| Chunks per block of data written | 2.2 | 1.2 | 3.5 | 2.3 |

The workload wrote 1,874 and 1,814 blocks of data in the two rounds: created files of 850 and 854 blocks, and 64 and 60 slots of 16 blocks. Everything a checkpoint uploads beyond that is the filesystem's own writing.

1. **XFS uploads little beyond the data**, ext4 about as much again. Both overwrite in place, but ext4's journal writes whole metadata blocks at every commit, and this workload fsyncs three times an iteration.
2. **Btrfs costs most.** Copy-on-write rewrites the metadata above every change, and an fsync also writes its log tree. On one disk Btrfs keeps two copies of its metadata (DUP), so it changed about twice the blocks it uploaded: identical copies are one chunk.
3. **ZFS pays for its record size.** At the default 128 KiB, each 64 KiB overwrite rewrites a whole record, which puts the workload's floor at 2,706 and 2,646 blocks. Its upload exceeds that by 1,400–1,700 chunks a round, from the intent log, metadata and freed blocks that are never trimmed (`autotrim` is off). A native ZFSwarm would keep the records and the block pointers above them, but not the intent log or the space maps. So it could save at most 34–39% of ZFS's upload here, while a smaller record size would save the record overhead in any design.

## Limits

- Killing QEMU or the engine leaves the host's page cache intact, so a host power cut that loses the engine's own unsynced writes is not tested here. `internal/epoch`'s recovery test covers epoch files cut at any point.
- Writes reach the epoch in the order they complete, so a disk cache that reorders unflushed writes is not modelled either.
- One guest kernel, default mount options, and 20 quick and 3 full runs per filesystem.
- A second disk on the same base snapshot cannot continue the base's lineage, whose head has moved on. So each run served its disk from its own state directory. With no lineage key there, each run's checkpoints started a lineage of their own.
