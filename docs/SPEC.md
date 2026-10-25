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

- The NBD export advertises 512-byte logical and 4096-byte physical blocks (512e), with minimum 512, preferred 4096.
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

Anyone can then fetch the image with `GET /bzz/<manifest>/disk.raw`.

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
| `prefetch` | path of the trace, or null |

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

Records are in first-touch order and do not repeat a block. The engine prefetches them in order, with a bounded window ahead of the guest's current position in the trace.

## 7. Feeds

Feed updates are built with Bee's `pkg/feeds` and `pkg/soc` and published with `POST /soc/{owner}/{id}?sig=…`, so that `GET /feeds/{owner}/{topic}` resolves them. Only sequence feeds exist in Bee.

**Lineage feed.**

- Owner: the lineage key, a secp256k1 key held by exactly one host.
- Topic: `keccak256(utf8("swarmblk/lineage/") ‖ lineage_id)`, where `lineage_id` is 16 random bytes chosen at lineage creation.
- Update *n* carries the reference of snapshot *n*'s manifest. Index 0 is the lineage's first snapshot.
- A writer publishes index *n*+1 only after reading its own head as *n*.

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
| `chunks/` | content-addressed chunk store: tree cache (all intermediates of known snapshots) and chunk cache (leaves), verified on write |
| `state.db` | embedded key-value store (e.g. bbolt): lineages (id, owner, topic, head index, head manifest), batches (id, bound host, depth, TTL forecast, fullest bucket), coverage (address → batch ids), epochs (Phase 2) |
| `keys/` | lineage keys, mode 0600; never logged or committed |

## 11. Write epochs (draft, Phase 2)

- Append-only data segments of 4 KiB blocks, plus an index of block → (segment, offset, CRC32C).
- Partial-write records as in §4.
- Superblock: epoch number, parent snapshot root, state (`active`, `frozen`, `uploading`, `retired`).
- A guest FLUSH or FUA write completes only after data and index are fsynced.
- Recovery replays the index and drops records after the last good checksum.

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
| `GET /chunks/{address}` | engine reads | Returns span + payload; verify locally; honours `Swarm-Cache`. The BMT hash pads with zeros, so a valid payload may lack trailing zeros: read a leaf as span bytes, zero-padded |
| `POST /chunks` | single uploads | `Swarm-Postage-Batch-Id` or pre-signed `Swarm-Postage-Stamp`; `Swarm-Tag` |
| `GET /chunks/stream` (WebSocket) | bulk uploads | One ack per chunk means stored locally, not synced; `Swarm-Tag`; batch id or pre-signed stamps |
| `POST /tags`, `GET /tags/{uid}` | sync tracking | Counters `split`, `seen`, `stored`, `sent`, `synced` |
| `POST /soc/{owner}/{id}?sig=` | feed updates | Built with `pkg/feeds` and `pkg/soc` |
| `GET /feeds/{owner}/{topic}` | latest snapshot | Returns `swarm-feed-index` |
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
