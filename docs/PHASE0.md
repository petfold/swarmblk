# Phase 0 — measure with stock tools

*Runbook, 25 October 2026. Goal: learn whether booting from Swarm is fast enough to be worth an engine, before writing one.*

Phase 0 boots an unmodified cloud image from a Bee `/bytes` URL through QEMU's HTTP block driver, with a local qcow2 overlay for writes. The only code is two small tools: a logging proxy in front of Bee and a chunk-latency probe.

**Gate.** Peter sets the cold-boot bound after seeing the numbers. The boot working set must also be small enough to prefetch.

## What you need

- Bee ≥ 2.8.2 on this machine (node A, `http://localhost:1633`), funded. The latest release at writing is v2.8.3.
- A second Bee node on another machine, ideally a light node (node B). Cold reads must come through a node that has never held the chunks, and a light node has no reserve that could hold them.
- QEMU with KVM, `qemu-img`, `cloud-image-utils` (for `cloud-localds`), `jq`, `moreutils` (for `ts`), Go.

## 1. Buy a test batch

An 8 GiB image with 2–3 GB of real data fits a depth-22 batch (7.7 GB effective). Check the cost before buying.

```sh
BEE=http://localhost:1633
curl -s $BEE/health | jq -r .version          # must be ≥ 2.8.2
PRICE=$(curl -s $BEE/chainstate | jq -r .currentPrice)
DAYS=14
AMOUNT=$(( PRICE * 17280 * DAYS ))            # 17280 Gnosis blocks per day
echo "cost: $(echo "$AMOUNT * 2^22 / 10^16" | bc -l) xBZZ"
curl -s -X POST -H "immutable: true" "$BEE/stamps/$AMOUNT/22" | jq
export BATCH=<batchID from the reply>          # usable after 2–3 minutes
```

## 2. Build the test image

```sh
wget https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img
qemu-img convert -f qcow2 -O raw noble-server-cloudimg-amd64.img disk.raw
qemu-img resize -f raw disk.raw 8G             # power of two, as SPEC §2 requires

cat > user-data <<'EOF'
#cloud-config
password: swarmblk
chpasswd: { expire: false }
ssh_pwauth: true
EOF
cloud-localds seed.img user-data
```

## 3. Upload at redundancy level 0

`-T` streams the file; `--data-binary @file` would load 8 GiB into memory. `Swarm-Pin: true` keeps every chunk in node A's local store for the warm runs. Without it, Bee deletes uploaded chunks once they are synced, and node A fetches them back from the network.

```sh
curl -s -X POST "$BEE/bytes" \
  -H "Swarm-Postage-Batch-Id: $BATCH" \
  -H "Swarm-Redundancy-Level: 0" \
  -H "Swarm-Pin: true" \
  -H "Content-Type: application/octet-stream" \
  -T disk.raw -D upload-headers.txt | tee upload.json
REF=$(jq -r .reference upload.json)
TAG=$(awk 'tolower($1)=="swarm-tag:"{print $2}' upload-headers.txt | tr -d '\r')
while sleep 30; do date +%T; curl -s "$BEE/tags/$TAG" | jq -c '{split,seen,stored,sent,synced}'; done
```

Record the upload wall time, the tag counters, and when `synced` stops rising.

## 4. Write the two tools

**`tools/beeproxy`** — a reverse proxy in front of a Bee node (Go `net/http/httputil`).

- Flags: `-listen` (default `127.0.0.1:1636`; Bee's p2p port is 1634), `-bee` (upstream URL, default `$SWARMBLK_BEE_URL` or `http://localhost:1633`), `-lookahead N` (when set, adds `Swarm-Lookahead-Buffer-Size: N`; 0 turns lookahead off), `-nocache` (adds `Swarm-Cache: false`), `-log` (CSV path, appended to; default stdout).
- Passes `HEAD` and `GET` through unchanged apart from the injected headers, and refuses other methods, so nothing can upload or buy stamps through it. QEMU's probe needs `Accept-Ranges` on `HEAD`.
- Logs one CSV row per request under the header `start_ns,method,path,range,status,bytes,duration_ms`: start time (Unix ns), method, path, `Range` header, status, response bytes, duration (ms). Rows are written as requests finish, so sort by `start_ns` for the access order.
- The log is both the latency record and the boot access trace, at QEMU's readahead granularity: each request covers the read plus `file.readahead`, so a 64 KiB read fetches 128 KiB.

**`cmd/chunkprobe`** — measures raw chunk retrieval, which QEMU's 8-request limit hides.

- Flags: `-bee` (default `$SWARMBLK_BEE_URL` or `http://localhost:1633`), `-ref` (the root from §3), `-n` (leaves per level, default 2000), `-c` (concurrency levels, default `1,8,32,128`), `-nocache` (adds `Swarm-Cache: false`), `-seed` (0 picks one; the report prints it).
- Walks the tree one level at a time with `GET /chunks/{address}` at the highest concurrency, fetching each distinct intermediate chunk once and skipping zero subtrees (SPEC §3), as the engine will. Verifies every chunk with Bee's `pkg/cac`, and refuses trees that are not format 1, such as uploads without `Swarm-Redundancy-Level: 0`. Reports the walk time.
- Fetches `n` random distinct leaves that are not Z_0 at each concurrency level, a fresh set per level while the image has enough.
- Prints the §7 table: p50, p90, p99 and max latency, chunks per second, and failed fetches, per level.
- Its tree and Bee code is in `internal/tree` and `internal/bee`, the seeds of Phase 1.

## 5. Boot runs

Start the proxy in a second terminal for each run, with that run's flags from the table below and a log of its own:

```sh
go run ./tools/beeproxy -bee http://<node B>:1633 -nocache -log cold-la-1.csv
```

Create a fresh overlay per run, pointing at the proxy (`http://127.0.0.1:1636`):

```sh
mkoverlay () {   # $1 = overlay file, $2 = proxy URL
  qemu-img create -f qcow2 -F raw \
    -b "json:{\"file.driver\":\"http\",\"file.url\":\"$2/bytes/$REF\",\"file.readahead\":\"64k\",\"file.timeout\":60}" \
    "$1"
}
boot () {        # $1 = overlay file, $2 = log name
  qemu-system-x86_64 -enable-kvm -cpu host -m 4G -smp 2 -nographic \
    -drive file="$1",if=virtio,copy-on-read=on \
    -drive file=seed.img,if=virtio,format=raw,readonly=on \
    2>&1 | ts -s '%.s' | tee "$2.log"
}
```

The boot is done when the console prints `Cloud-init v. … finished`. Note the elapsed time on that line, then shut the guest down.

| Run | Bee node | Proxy flags | Overlay | What it shows |
|---|---|---|---|---|
| cold-la | B (light) | `-nocache` | fresh | first boot, Bee lookahead on (512 KiB for files over 10 MB) |
| cold-nola | B (light) | `-nocache -lookahead 0` | fresh | first boot, lookahead off |
| warm | A | none | fresh | chunks pinned in node A's local store (§3) |
| hot | any | any | reused from a previous run | copy-on-read has hydrated the overlay |

Run the two cold cases more than once and alternate them. Forwarding nodes cache chunks along retrieval paths, so later runs can get faster.

After each cold boot, measure what the overlay holds (bytes read through copy-on-read plus bytes the guest wrote):

```sh
qemu-img map --output=json overlay.qcow2 | jq '[.[] | select(.data and .depth == 0) | .length] | add'
```

Keep every proxy log; Phase 2 turns one into the first `prefetch.bin`.

## 6. Chunk probe

```sh
go run ./cmd/chunkprobe -bee http://<node B>:1633 -ref $REF -nocache
go run ./cmd/chunkprobe -bee $BEE -ref $REF            # node A, for comparison
```

## 7. Record results

Write `docs/results/phase0-<date>.md`:

```markdown
# Phase 0 results — <date>

Setup: Bee versions and modes (A, B), locations, QEMU version, image, size, batch depth.
Upload: wall time; tag split / seen / synced; time until synced stopped rising.

| Run | Boot to cloud-init finished (s) | Requests | Bytes fetched | Request latency p50 / p99 (ms) | Overlay after boot |
|---|---|---|---|---|---|
| cold-la | | | | | |
| cold-nola | | | | | |
| warm | | | | | |
| hot | | | | | |

Chunk probe (node B, Swarm-Cache: false):

| Concurrency | p50 (ms) | p90 | p99 | max | chunks/s | failed |
|---|---|---|---|---|---|---|
| 1 | | | | | | |
| 8 | | | | | | |
| 32 | | | | | | |
| 128 | | | | | | |

Intermediate walk time:
Boot working set (unique 4 KiB blocks in the proxy log):
Observations:
Proposed gate and decision:
```

## Pitfalls

- **QEMU's default request timeout is 5 seconds.** A slower cold read becomes a guest I/O error, so always set `file.timeout`.
- **QEMU keeps at most 8 HTTP requests in flight.** Phase 0 boot times understate what parallel prefetch could do; chunkprobe measures that.
- **Bee adds a 512 KiB lookahead** to every read of a file over 10 MB unless the request turns it off.
- **Bee before 2.8.2 returns wrong `HEAD` headers**, and QEMU refuses the backing file unless the newer `file.force-range=on` option is set.
- **Forgetting `Swarm-Redundancy-Level: 0`** gives an erasure-coded, 119-ary tree. It still boots, but it is not the swarmblk format, and the chunk counts will be off.
- **A full node may already hold some chunks** in its reserve, so take network numbers from a light node.
