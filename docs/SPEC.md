# swarmblk — specification (format 1)

*25 October 2026. This file fixes formats and invariants; [DESIGN.md](DESIGN.md) explains why. Sections marked "draft" are filled in during the phase named.*

Format 1 covers unencrypted trees at redundancy level 0. Anything that would change a root or a byte on Swarm needs a new format version.

## 1. Constants

From Bee's `pkg/swarm`:

| Name | Value |
|---|---|
| Chunk payload (`ChunkSize`) | 4096 bytes |
| Span (`SpanSize`) | 8 bytes, little-endian uint64 |
| Reference (`HashSize`) | 32 bytes |
| References per intermediate chunk (`Branches`) | 128 |
| Postage bucket depth | 16 (65,536 buckets) |

A chunk's address is the BMT hash of its span and payload, as computed by Bee's `pkg/cac` and `pkg/bmt`. Never reimplement it.

## 2. Tree geometry

Disk size *S* = 2^*s* bytes with 26 ≤ *s* ≤ 40 (64 MiB to 1 TiB). Every upload uses `Swarm-Redundancy-Level: 0` and no encryption.

```
Leaf count            N = S / 4096 = 2^m,        m = s − 12      (14 ≤ m ≤ 28)
Intermediate levels   L = ceil(m / 7)                               (2 ≤ L ≤ 4)
Root fan-out          F = 2^(m − 7·(L − 1))                         (2 ≤ F ≤ 128)
Span of a level-j chunk, 1 ≤ j < L:   4096 · 128^j bytes
Span of the root (level L):            S
```

Leaf *b* holds disk bytes [4096·*b*, 4096·(*b*+1)). Its path from the root down:

```
root slot        c_L = floor(b / 128^(L−1))              0 ≤ c_L < F
level-j slot     c_j = floor(b / 128^(j−1)) mod 128      1 ≤ j < L
level-j chunk on the path is number floor(b / 128^j) within level j
```

Worked values:

| Size | m | L | F | Fetches per cold read, nothing cached |
|---|---|---|---|---|
| 64 MiB | 14 | 2 | 128 | 3 |
| 1 GiB | 18 | 3 | 16 | 4 |
| 8 GiB | 21 | 3 | 128 | 4 |
| 64 GiB | 24 | 4 | 8 | 5 |
| 1 TiB | 28 | 4 | 128 | 5 |

Because every level below the root is full for these sizes, Bee's carry-over rule (a lone reference moves up a level unwrapped) never applies. The root always has *F* ≥ 2 references and is wrapped.

## 3. Zero subtrees

```
Z_0 = address(span = 4096,           payload = 4096 zero bytes)
Z_j = address(span = 4096 · 128^j,   payload = Z_(j−1) repeated 128 times),   1 ≤ j < L
empty-disk root = address(span = S,  payload = Z_(L−1) repeated F times)
```

The engine computes Z_0 … Z_3 at start-up. A reference equal to Z_j (at level j) reads as zeros without a fetch and is reported as a hole through NBD block status. Discarded or zeroed blocks are written as Z_0.

## 4. Sectors and partial writes

- The NBD export advertises 512-byte logical and 4096-byte physical blocks (512e), with minimum 512, preferred 4096, and at most 32 MiB per request.
- A write covering whole 4 KiB blocks replaces those blocks.
- A write covering part of a block whose base is local merges at once.
- A write covering part of a block whose base is not yet local is logged as (block, offset, length, bytes) and merged when the base arrives or at freeze. It never waits for a Swarm read.

## 5. Snapshot manifest

A snapshot is a Mantaray manifest built with Bee's `pkg/manifest` and uploaded chunk by chunk:

| Path | Target | Content-Type |
|---|---|---|
| `disk.raw` | disk tree root (§2) | `application/octet-stream` |
| `meta.json` | JSON document below | `application/json` |
| `prefetch.bin` | optional boot trace (§6) | `application/octet-stream` |

Each entry also carries its name as `Filename` metadata, and the root path `/` names `meta.json` as its index document (`website-index-document`). Anyone can then fetch the image with `GET /bzz/<manifest>/disk.raw`, and `GET /bzz/<manifest>/` shows the metadata. Every chunk is at redundancy level 0, and the manifest's root is uploaded last.

`meta.json`, format 1:

```json
{
  "format": "swarmblk/1",
  "size": 8589934592,
  "logical_sector": 512,
  "physical_sector": 4096,
  "redundancy": 0,
  "encryption": "none",
  "disk_root": "<64 hex>",
  "parent": "<64 hex manifest reference, or null for a first snapshot>",
  "lineage": { "owner": "0x<40 hex>", "topic": "<64 hex>", "index": 12 },
  "created": "2026-10-25T07:00:00Z",
  "consistency": "crash",
  "batches": ["<64 hex batch id>"],
  "prefetch": "prefetch.bin",
  "stats": { "changed_blocks": 1234, "new_chunks": 1290 }
}
```

| Field | Meaning |
|---|---|
| `encryption` | `none` or `guest-luks` (informational; the host never sees keys) |
| `consistency` | `crash`, or `application` when the guest filesystem was frozen through the guest agent |
| `batches` | every batch the writer knows stamps chunks reachable from this snapshot: its own plus the parent's list |
| `parent` | the parent snapshot's manifest, or null for a first snapshot |
| `lineage` | the snapshot's place in a lineage feed (§7), or null for a snapshot outside any lineage, such as a plain import |
| `prefetch` | path of the trace, or null |
| `stats` | `changed_blocks`: blocks that differ from the parent, or from an empty disk for a first snapshot; `new_chunks`: distinct chunks of the disk tree that the parent's tree lacks, or all of them for a first snapshot |

Readers must ignore unknown fields. Writers must not change the meaning of a field within format 1.

## 6. Prefetch trace (`prefetch.bin`)

Little-endian binary:

```
magic    4 bytes  "SBPF"
version  1 byte   1
reserved 3 bytes  0
records  repeated until EOF:
         start  uint64   first 4 KiB block of the run
         count  uint32   number of consecutive blocks
```

Records are in first-touch order and do not repeat a block. The engine prefetches them in order, with a bounded window ahead of the guest's current position in the trace. It records a trace from a guest's reads (`swarmblk serve -record`), and `swarmblk checkpoint -prefetch` stores one with the next snapshot; a checkpoint without one keeps its parent's trace, which names blocks by position and so still applies.

## 7. Feeds

Feed updates are built with Bee's `pkg/feeds` and `pkg/soc` and published with `POST /soc/{owner}/{id}?sig=…`, so that `GET /feeds/{owner}/{topic}` resolves them. Only sequence feeds exist in Bee.

**Lineage feed.**

- Owner: the lineage key, a secp256k1 key held by exactly one host.
- Topic: `keccak256(utf8("swarmblk/lineage/") ‖ lineage_id)`, where `lineage_id` is 16 random bytes chosen at lineage creation.
- Update *n* carries the reference of snapshot *n*'s manifest. Index 0 is the lineage's first snapshot.
- Payload, the content of the chunk that the update wraps: 40 bytes, the snapshot's `created` time as an 8-byte big-endian count of Unix seconds, then the 32-byte manifest reference. This is Bee's legacy feed payload, so `GET /feeds` and `/bzz` resolve it to the manifest. A retried publish signs the same bytes.
- A writer publishes index *n*+1 only after reading its own head as *n*. If it reads *n*+1 naming the same manifest, an earlier attempt succeeded.
- Commands name a lineage as `0x<owner>/<topic>`, in lowercase hex, and take it to mean its head.
- Readers take the head's index from `GET /feeds`, then fetch the update with `GET /chunks/{address}` and check its signature themselves.

**Disk registry (draft, Phase 4).**

- Owner: the person's or organisation's key.
- Topic: `keccak256(utf8("swarmblk/registry/") ‖ utf8(disk_name))`.
- Each update references a JSON record:

```json
{ "format": "swarmblk/1", "lineage": { "owner": "0x…", "topic": "…" },
  "base": "<manifest reference the lineage starts from>", "epoch": 3, "state": "active" }
```

- `state` is `active` or `sealed`. A writer must stop publishing if the latest registry record names another lineage.

## 8. Checkpoint protocol

Normative order. Steps 3–6 may run concurrently with the guest.

1. **Freeze** an atomic cut of the write log (all completed writes in, no in-flight write half in). Optionally freeze the guest filesystem first through the QEMU guest agent and record `consistency: "application"`.
2. **Hash** each block of the frozen epoch with `pkg/cac`. Skip a block whose address equals the parent's leaf or is in the coverage index (§10) for a live batch. The zero chunks Z_j enter the index when first uploaded to each batch.
3. **Upload** remaining leaves through `/chunks/stream` (or `POST /chunks`) with `Swarm-Postage-Batch-Id` of this host's current batch and one `Swarm-Tag`.
4. **Rebuild** intermediate chunks bottom-up along affected paths; upload them, root last.
5. **Upload** `meta.json`, `prefetch.bin` if any, and the manifest chunks.
6. **Confirm** sync: poll `GET /tags/{uid}` until every chunk counted under the tag is synced, which is when `synced` = `split` − `seen`. Bee counts each uploaded chunk as split and each already-present one as seen. `internal/bee`'s integration test pins this rule for Bee 2.8.3; check it again before moving to another Bee version. Then fetch a random sample of new chunks with `Swarm-Cache: false` from the network.
7. **Publish** lineage feed update *n*+1.
8. **Retire** the epoch: move its blocks into the chunk store under their addresses and record coverage.

A crash before step 7 leaves the previous snapshot current; the uploaded chunks are orphaned but harmless.

**Offline checkpoints (Phase 1).** The disk is a qcow2 overlay whose backing file names its parent snapshot as `…/bzz/<snapshot>/disk.raw`, which QEMU reads from Bee over HTTP, as a plain URL or in a `json:` file name with QEMU's HTTP options. `swarmblk restore` makes such an overlay, with a `json:` name that sets Phase 0's options (`file.timeout` 60 s, `file.readahead` 64 KiB). With the VM stopped, `swarmblk checkpoint`:

- reads the blocks the overlay itself holds from the qcow2 file, at the offsets that `qemu-img map` reports with the backing file left out; zero clusters become Z_0;
- rebuilds the parent's tree with them and uploads the chunks that differ from the parent's at their place (steps 2–6);
- rebases the overlay onto the new snapshot (`qemu-img rebase -u`), which is safe because the two disks differ only where the overlay holds blocks, then publishes (step 7).

The snapshot continues the parent's lineage when this host holds its key and the parent is its head; otherwise it starts a new lineage, which `-branch` also forces. A checkpoint that stops between the rebase and the publish leaves the overlay on an unpublished snapshot, and the next checkpoint publishes it first.

**Live checkpoints (Phase 2).** A writable disk (§11) is checkpointed while its guest runs. `swarmblk serve -disk` holds the disk and runs its checkpoints, one at a time: every `-every`, when `swarmblk checkpoint -disk` asks on the disk's control socket, and at start when one stopped part way. With no serve holding the disk, `checkpoint -disk` opens it and runs the checkpoint itself.

- Step 1 waits for the writes in flight, holds off new ones, fsyncs the active epoch, marks it `frozen` and puts a new epoch on top, on the same parent. Reads never wait. With `-qga`, the QEMU guest agent freezes the guest's filesystems (`guest-fsfreeze-freeze`) before the cut and thaws them after it, also after a failed freeze, which may have frozen some. The snapshot records `consistency: "application"` only when the freeze succeeded. The client syncs first: a 0xFF byte, then `guest-sync-delimited` with a random ID, ignoring everything before its answer.
- Steps 2–6 take the frozen epoch's blocks as they were at the cut, patches applied to what lies under them, and rebuild and upload as offline checkpoints do.
- Before step 7, the frozen epoch records `state: uploading` and the new `snapshot`. A checkpoint that stops after that only publishes the next time.
- Step 8 moves the epochs above onto the new snapshot (their `parent` and `disk_root`), marks the frozen epoch `retired` and removes it, closing it when no read uses it any more.
- At most one epoch is frozen: a checkpoint first finishes one that an earlier checkpoint left.

The control socket is `disks/<name>/control.sock` (§10). A client sends `{"request":"checkpoint"}` as one JSON line and gets the checkpoint's log as `{"log":"…"}` lines, then `{"snapshot":"<hex>"}` or `{"error":"…"}`. A checkpoint with no writes since the last returns the disk's snapshot and uploads nothing.

## 9. Postage rules

- **P1.** Buy batches with the `immutable: true` header. Never stamp disk data with a mutable batch.
- **P2.** Each batch is bound to one host and one Bee node. The engine records the binding and refuses to stamp with a batch bound elsewhere.
- **P3.** If a Bee node loses its state directory, its batches take no new chunks. Their existing chunks stay valid until expiry, and top-ups are still allowed.
- **P4.** Before each checkpoint, read `GET /stamps/{id}/buckets`. When the fullest bucket passes 90% (configurable), new chunks go to a new batch, or the batch is diluted.
- **P5.** Re-check every batch's TTL at least daily. Alert when TTL < retention target + margin (default margin 30 days).
- **P6.** Rotation re-stamps the deduplicated set of addresses reachable from retained snapshots, chunk by chunk. Never use `PUT /stewardship` for whole disks: it visits every reference.

## 10. Local state

Under `$SWARMBLK_HOME` (default `~/.local/share/swarmblk`), mode 0700:

| Item | Content |
|---|---|
| `chunks/` | content-addressed chunk store: tree cache (all intermediates of known snapshots) and chunk cache (leaves), verified on write. `serve -cache` bounds it, 4 GiB by default: a sweep removes the leaves used least recently, read or written, and never intermediate chunks or manifests. A writer holds `chunks/.lock` shared from putting chunks until Bee has synced them; a sweep takes it alone, or skips |
| `state.db` | embedded key-value store (e.g. bbolt): lineages (id, owner, topic, head index, head manifest), batches (id, bound host, depth, TTL forecast, fullest bucket), coverage (address → batch ids), epochs (Phase 2) |
| `disks/` | writable disks, each a directory of write epochs (§11), a `lock` file that the process holding the disk keeps an exclusive `flock` on, and, while `serve` holds it, `control.sock` (§8). Also its hydration policy and what that keeps (§11): `policy`, `image/` and `restamp.json` |
| `keys/` | lineage keys, one file `<owner>.key` each, holding the private key in hex, mode 0600; never logged or committed. A key file that other users can read is refused |

## 11. Write epochs (Phase 2)

A writable disk keeps the blocks written since its last snapshot in a write epoch, a directory `$SWARMBLK_HOME/disks/<name>/<n>` for epoch *n*:

- `super.json`: `format` (`swarmblk-epoch/1`), `epoch`, `parent` (the snapshot's manifest), `disk_root`, `size`, `state`, and from `uploading` on, `snapshot`, the manifest of the snapshot that holds the epoch's blocks. It is written after `data` and `index` exist, and replaced atomically. A directory without it and with no records is what an interrupted create left; it is removed when the disk opens.
- States: `active` takes the writes; `frozen` is cut for a checkpoint; `uploading` has its snapshot uploaded and synced, perhaps not yet published; `retired` is in a published snapshot that the epochs above sit on, and is removed.
- `data`: the written bytes, appended.
- `index`: 36-byte little-endian records, appended after their data: kind (1 byte: 1 a whole block, 2 a block of zeros with no data, 3 a patch), 3 zero bytes, `pos` (uint32, a patch's offset in the block), `length` (uint32: 4096, 0, or a patch's length), `block` (uint64), `offset` (uint64, where the data starts in `data`), the data's CRC32C (uint32), and the CRC32C of the 32 bytes before it (uint32).

Reads take a block's latest whole record, or else its contents in the snapshot, and apply the patches after it in order. A write covering part of a block is recorded whole when the block's contents are at hand, and as a patch otherwise (§4); a whole block of zeros, a discard and a zero-write are zero blocks. A guest FLUSH, or a write with FUA, completes only after `data` and then `index` are fsynced, so a durable record never points at data that is not. Recovery replays the index and drops the first record whose checksum, data or position fails, and everything after it, then truncates both files. An open epoch holds an exclusive `flock` on its index, so no second process writes it.

A disk is its epochs, oldest first, on the snapshot under the oldest: the newest is active and the others are frozen. When the disk opens, a `retired` epoch, and a frozen one whose `snapshot` is the parent of the epochs above it, are removed, since their checkpoint finished; the epochs left must share one parent (§8).

**Hydration policy** (DESIGN §3). A disk's `policy` file holds `lazy`, `hydrate` or `restamp`, and a disk without one is lazy. A lazy disk reads through the bounded chunk cache (§10). A hydrating disk keeps a local copy in `image/`:

- `disk.raw`: the disk under the epochs, a sparse file of its size. Zero blocks are holes, and block status is answered from them.
- `present`: a bitmap, bit *b* mod 8 of byte *b*/8 set when block *b* is in `disk.raw`. Data is fsynced before its bits are written.
- `root`: the root of the disk tree the copy holds, in hex, replaced atomically.

Reads take a block from the copy first and put what they fetch in it, and a background hydration fetches the rest in disk order. A checkpoint moves the copy at the start of step 8: it writes the frozen epoch's blocks, syncs, then replaces `root`, before the epochs above move. Meanwhile reads may use the copy, since the frozen epoch above holds every block that changes. When the disk opens, a copy whose root is neither that of the disk's snapshot nor that of a frozen epoch's recorded `snapshot` starts again empty. A disk that is no longer hydrating loses its copy.

A re-stamping disk uploads every distinct chunk of its snapshot again under the batch, as one stream, once. It then records `restamp.json`, `{"snapshot": …, "batches": […]}`: every chunk of that snapshot carries a stamp from one of the batches. A checkpoint moves the record to the new snapshot and adds its batch.

## 12. Disk card (draft, Phase 2)

A JSON document that restores a machine from the bare network. It is secret when it holds keys.

```json
{
  "format": "swarmblk-card/1",
  "name": "web-01",
  "registry": { "owner": "0x…", "topic": "…" },
  "lineage": { "owner": "0x…", "topic": "…" },
  "snapshot": "<manifest reference at card time>",
  "batches": ["…"],
  "disk_key": "<LUKS key material or ACT reference, optional>",
  "vm": { "definition": "<reference>", "nvram": "<reference>", "vtpm": "<reference>" }
}
```

The `vm` blobs are encrypted before upload. Signature scheme and key wrapping are decided in Phase 2.

## 13. Bee API usage

Bee ≥ 2.8.2. All requests go to `$SWARMBLK_BEE_URL` (default `http://localhost:1633`).

| Endpoint | Used for | Notes |
|---|---|---|
| `POST /bytes` | Phase 0 image upload | Set `Swarm-Redundancy-Level: 0` and `Swarm-Postage-Batch-Id`; the response carries `swarm-tag` |
| `GET`/`HEAD /bytes/{ref}` | Phase 0 boot through QEMU | `HEAD` advertises ranges from 2.8.2; `Swarm-Lookahead-Buffer-Size: 0` turns off Bee's lookahead; `Swarm-Cache: false` skips caching |
| `GET /chunks/{address}` | engine reads, feed updates | Returns span + payload; verify locally; honours `Swarm-Cache`. The BMT hash pads with zeros, so a valid payload may lack trailing zeros: read a leaf as span bytes, zero-padded. A single-owner chunk comes back whole: ID, signature, span and payload |
| `POST /chunks` | single uploads | `Swarm-Postage-Batch-Id` or pre-signed `Swarm-Postage-Stamp`; `Swarm-Tag` |
| `GET /chunks/stream` (WebSocket) | bulk uploads | One ack per chunk means stored locally, not synced; `Swarm-Tag`; batch id or pre-signed stamps |
| `POST /tags`, `GET /tags/{uid}` | sync tracking | Counters `split`, `seen`, `stored`, `sent`, `synced` |
| `POST /soc/{owner}/{id}?sig=` | feed updates | Built with `pkg/feeds` and `pkg/soc`; the body is the wrapped chunk (span + payload). With `Swarm-Deferred-Upload: false` and no `Swarm-Tag`, Bee pushes the chunk and answers after a receipt |
| `GET /feeds/{owner}/{topic}` | latest snapshot | `Swarm-Feed-Index` gives the index; `Swarm-Only-Root-Chunk: true` skips the content. A feed without updates gets 404 `no update found`. The lookup drops any probe slower than 1 s, so the index can lag |
| `GET /stamps`, `/stamps/{id}`, `/stamps/{id}/buckets` | batch status, TTL, bucket fill | |
| `POST /stamps/{amount}/{depth}` | buying a batch | Header `immutable: true`; an operator action, not automatic |
| `PATCH /stamps/topup/{id}/{amount}`, `/stamps/dilute/{id}/{depth}` | retention manager | |
| `GET /stewardship/{ref}` | retrievability check on small trees | Visits every reference; for disks, sample `GET /chunks` instead |
| `POST /pins/{ref}` | pinning the latest snapshot locally | |

## 14. Invariants and their tests

| Id | Invariant | Test |
|---|---|---|
| I1 | A snapshot's root equals Bee's root for the same bytes at redundancy 0 | Build random images (sizes per §2) with Bee's `pkg/file/pipeline/builder`; mutate random blocks; compare incremental rebuild with a full Bee rebuild |
| I2 | No feed update before every new chunk is synced | Fault-injection test: kill the uploader between steps 3 and 7; the previous snapshot stays current |
| I3 | Every chunk reachable from a retained snapshot is covered past its target date | Retention simulation over generated histories |
| I4 | A batch stamps from exactly one host | Engine refuses a batch bound elsewhere; unit test on the binding table |
| I5 | Each Z_j is uploaded at most once per batch | Count uploads in an all-zero import |
| I6 | A writer never publishes to a lineage the registry no longer names | Registry race test (Phase 4) |
| I7 | Export of a snapshot equals the guest-visible disk at freeze | sha256 of `swarmblk export` against the image read through the overlay at checkpoint time |
