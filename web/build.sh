#!/bin/sh
# build.sh fetches the third-party files that the page needs, at pinned
# versions checked by SHA-256, and builds swarmblk's engine for browsers,
# all into web/vendor/, which git ignores. It needs curl, tar, sha256sum
# and Go.
set -eu
web=$(cd "$(dirname "$0")" && pwd)
vendor=$web/vendor
mkdir -p "$vendor"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# fetch URL SHA256 NAME downloads URL to $tmp/NAME and checks it.
fetch() {
	curl -sfL --retry 3 -o "$tmp/$3" "$1"
	echo "$2  $tmp/$3" | sha256sum -c --quiet -
}

# v86 0.5.472 (BSD-2-Clause), the emulator.
fetch https://registry.npmjs.org/v86/-/v86-0.5.472.tgz 4698fae3f518ad0a6f78d31e8f6f8802ff272a86f07b603b95f6c9fdd428e4fc v86.tgz
tar -xzf "$tmp/v86.tgz" -C "$tmp" package/build/libv86.js package/build/v86.wasm package/LICENSE
cp "$tmp/package/build/libv86.js" "$tmp/package/build/v86.wasm" "$vendor/"
cp "$tmp/package/LICENSE" "$vendor/v86-LICENSE"
rm -r "$tmp/package"

# SeaBIOS 1.16.2 and its VGA BIOS (LGPL-3.0), as v86 builds them, from the
# commit of v86's repository that was tested with this page.
v86=https://raw.githubusercontent.com/copy/v86/cc37f35c43bf14865d74ad76879ecf9410f69eb1/bios
fetch $v86/seabios.bin 73e3f359102e3a9982c35fce98eb7cd08f18303ac7f1ba6ebfbe6cdc1c244d98 seabios.bin
fetch $v86/vgabios.bin a4bc0d80cc3ca028c73dafa8fee396b8d054ce87ebd8abfbd31b06b437607880 vgabios.bin
fetch $v86/COPYING.LESSER a853c2ffec17057872340eee242ae4d96cbf2b520ae27d903e1b2fef1a5f9d1c COPYING.LESSER
cp "$tmp/seabios.bin" "$tmp/vgabios.bin" "$vendor/"
cp "$tmp/COPYING.LESSER" "$vendor/seabios-COPYING.LESSER"

# xterm.js 6.0.0 (MIT), the serial console, which answers the guest's
# terminal queries.
fetch https://registry.npmjs.org/@xterm/xterm/-/xterm-6.0.0.tgz 908e66e04af6c8dc6b00dd3b54de088e2e81e5ed866284fd6c2fb3c2d1c7a3f6 xterm.tgz
tar -xzf "$tmp/xterm.tgz" -C "$tmp" package/lib/xterm.js package/css/xterm.css package/LICENSE
cp "$tmp/package/lib/xterm.js" "$tmp/package/css/xterm.css" "$vendor/"
cp "$tmp/package/LICENSE" "$vendor/xterm-LICENSE"

# swarmblk's engine, and Go's loader for it, which must come from the same Go.
cd "$web/.."
GOOS=js GOARCH=wasm go build -trimpath -ldflags='-s -w' -o "$vendor/swarmblk.wasm" ./cmd/swarmblk-wasm
cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" "$vendor/"
echo "web/vendor is ready. Run python3 web/serve.py and open http://127.0.0.1:8099/?snapshot=<reference>"
