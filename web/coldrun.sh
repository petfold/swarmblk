#!/bin/sh
# coldrun.sh NAME SNAPSHOT [SECONDS] boots SNAPSHOT through the engine in
# headless Chromium with a new, empty profile, so that the browser's cache
# starts cold, for SECONDS (300 by default). Then it reports when the serial
# console showed a login prompt, what the engine served, and how long the
# machine waited for its disk. web/serve.py must be serving on port 8099.
# $CHROMIUM names the browser (default chromium), and $PROFILES where the
# profile goes (default ~/snap/chromium/common, where snap's Chromium may
# write). The page's reports go to web/out/log-NAME.jsonl.
set -eu
web=$(cd "$(dirname "$0")" && pwd)
name=$1 snapshot=$2 seconds=${3:-300}
profile=${PROFILES:-$HOME/snap/chromium/common}/swarmblk-$name
rm -rf "$profile" "$web/out/log.jsonl"
mkdir -p "$profile" "$web/out"
timeout "$seconds" "${CHROMIUM:-chromium}" --headless=new --no-first-run --no-default-browser-check --disable-gpu \
	--user-data-dir="$profile" "http://127.0.0.1:8099/?snapshot=$snapshot&report=1" > "$web/out/chromium-$name.out" 2>&1 || true
mv "$web/out/log.jsonl" "$web/out/log-$name.jsonl"
python3 - "$web/out/log-$name.jsonl" "$name" <<'EOF'
import json, statistics, sys
rows = [json.loads(l) for l in open(sys.argv[1])]
first = next((d for d in rows if "login:" in d["serial"]), None)
d = first or rows[-1]
t = d["timings"] or [[0, 0, 0]]
when = f"{first['t']} s" if first else f"not within {rows[-1]['t']} s"
print(f"{sys.argv[2]}: login prompt {when}; engine {d['engine']}; {len(t)} reads, the disk waited {sum(x[1] for x in t) / 1000:.1f} s, median {statistics.median(x[1] for x in t)} ms")
EOF
