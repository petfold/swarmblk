package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/lineage"
	"github.com/petfold/swarmblk/internal/snapshot"
	"github.com/petfold/swarmblk/internal/tree"
)

// startServe runs serve for arg or root with an empty store, as on another
// host, and returns the URI it prints and a function that stops it and
// returns its error and log.
func (cp *checkpointer) startServe(t *testing.T, arg, root string) (string, func() (error, string)) {
	t.Helper()
	return cp.startServeWith(t, serveConfig{arg: arg, root: root})
}

// startServeWith is startServe with more of serve's configuration; it picks
// the socket.
func (cp *checkpointer) startServeWith(t *testing.T, cfg serveConfig) (string, func() (error, string)) {
	t.Helper()
	cfg.sock = filepath.Join(t.TempDir(), "nbd.sock")
	pr, pw := io.Pipe()
	var log bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	s := newStore(t)
	go func() {
		done <- serve(ctx, cp.c, s, cfg, pw, &log)
		pw.Close()
	}()
	line, err := bufio.NewReader(pr).ReadString('\n')
	if err != nil {
		cancel()
		t.Fatalf("serve: %v\n%s", <-done, log.String())
	}
	go io.Copy(io.Discard, pr)
	return strings.TrimSpace(line), func() (error, string) {
		cancel()
		err := <-done
		return err, log.String()
	}
}

// readAll reads the disk at uri, of format f, with qemu-img convert.
func readAll(t *testing.T, f, uri string) []byte {
	t.Helper()
	out := filepath.Join(t.TempDir(), "read.raw")
	if b, err := exec.Command("qemu-img", "convert", "-f", f, "-O", "raw", uri, out).CombinedOutput(); err != nil {
		t.Fatalf("qemu-img convert %s: %v\n%s", uri, err, b)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestServe(t *testing.T) {
	needQEMU(t)
	cp := newCheckpointer(t)
	snap, img := cp.importDisk(t)
	uri, stop := cp.startServe(t, snap.String(), "")
	if !strings.HasPrefix(uri, "nbd+unix:///"+snap.String()+"?socket=") {
		t.Errorf("URI %q", uri)
	}
	if !bytes.Equal(readAll(t, "raw", uri), img) {
		t.Error("QEMU read the served snapshot different")
	}
	sock := uri[strings.Index(uri, "socket=")+len("socket="):]
	if fi, err := os.Stat(sock); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("socket %s: %v, %v; want mode 0600", sock, fi.Mode(), err)
	}
	err, log := stop()
	if err != nil || !strings.Contains(log, "chunks fetched from Bee") {
		t.Errorf("serve ended with %v:\n%s", err, log)
	}
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Errorf("the socket outlived serve: %v", err)
	}

	root := mustMeta(t, cp, snap).DiskRoot
	uri, stop = cp.startServe(t, "", root.String())
	if !strings.Contains(uri, root.String()) || !bytes.Equal(readAll(t, "raw", uri), img) {
		t.Errorf("serving the disk root %s as %s read different", root, uri)
	}
	stop()
}

// TestServeLineage serves a lineage's head and runs an overlay on it, as a
// VM would.
func TestServeLineage(t *testing.T) {
	needQEMU(t)
	cp := newCheckpointer(t)
	snap0, img := cp.importDisk(t)
	o := newOverlay(t, snap0, img)
	o.write(t, "write -P 0x31 1M 64k")
	snap1, err := cp.run(t, o, false)
	if err != nil {
		t.Fatal(err)
	}
	l, _, err := lineage.FromMeta(*mustMeta(t, cp, snap1).Lineage)
	if err != nil {
		t.Fatal(err)
	}
	uri, stop := cp.startServe(t, l.String(), "")
	defer stop()
	if !strings.Contains(uri, snap1.String()) || !bytes.Equal(readAll(t, "raw", uri), o.disk) {
		t.Fatalf("lineage %s served as %s, which reads different from the checkpointed disk", l, uri)
	}
	ov := filepath.Join(t.TempDir(), "vm.qcow2")
	run(t, "qemu-img", "create", "-q", "-f", "qcow2", "-F", "raw", "-b", uri, ov)
	run(t, "qemu-io", "-f", "qcow2", "-c", "write -P 0x32 2M 4k", ov)
	want := bytes.Clone(o.disk)
	copy(want[2<<20:], bytes.Repeat([]byte{0x32}, 4096))
	if !bytes.Equal(readAll(t, "qcow2", ov), want) {
		t.Error("an overlay on the served disk reads different")
	}
}

func TestServeSocket(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "stale.sock")
	l, err := net.Listen("unix", stale)
	if err != nil {
		t.Fatal(err)
	}
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	l.Close()
	if l, err = listen(stale); err != nil {
		t.Fatalf("a stale socket: %v", err)
	}
	if _, err := listen(stale); err == nil || !strings.Contains(err.Error(), "another server") {
		t.Errorf("a socket in use: %v", err)
	}
	l.Close()

	cp := newCheckpointer(t)
	for _, root := range []string{"swarmblk", strings.Repeat("ab", 32)} {
		if err := serve(context.Background(), cp.c, newStore(t), serveConfig{root: root, sock: filepath.Join(dir, "x.sock")}, io.Discard, io.Discard); err == nil {
			t.Errorf("serve took the root %q", root)
		}
	}
}

// TestServeTrace records a boot trace while QEMU reads a served snapshot,
// stores it with a checkpoint, keeps it in the next one, and prefetches
// from it when the lineage is served.
func TestServeTrace(t *testing.T) {
	needQEMU(t)
	ctx := context.Background()
	cp := newCheckpointer(t)
	snap0, img := cp.importDisk(t)
	trace := filepath.Join(t.TempDir(), "boot.trace")
	uri, stop := cp.startServeWith(t, serveConfig{arg: snap0.String(), record: trace})
	run(t, "qemu-io", "-r", "-f", "raw", "-c", "read 1M 64k", "-c", "read 0 4k", "-c", "read 1032k 4k", uri)
	if err, log := stop(); err != nil || !strings.Contains(log, "wrote a boot trace of 17 blocks in 2 runs") {
		t.Fatalf("serve ended with %v:\n%s", err, log)
	}
	b, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	want := []tree.Range{{Start: 256, Count: 16}, {Start: 0, Count: 1}}
	if runs, err := snapshot.DecodeTrace(b); err != nil || !reflect.DeepEqual(runs, want) {
		t.Fatalf("the trace holds %v, %v; want %v", runs, err, want)
	}

	o := newOverlay(t, snap0, img)
	o.write(t, "write -P 0x41 4M 4k")
	snap1, err := cp.runWith(t, o, false, b)
	if err != nil {
		t.Fatal(err)
	}
	o.write(t, "write -P 0x42 5M 4k")
	snap2, err := cp.run(t, o, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range []swarm.Address{snap1, snap2} {
		s, err := snapshot.Read(ctx, cp.bee.Store, ref)
		if err != nil {
			t.Fatal(err)
		}
		if runs, err := snapshot.ReadTrace(ctx, cp.bee.Store, s); err != nil || !reflect.DeepEqual(runs, want) {
			t.Errorf("snapshot %s has the trace %v, %v; want %v", ref, runs, err, want)
		}
	}

	l, _, _ := lineage.FromMeta(*mustMeta(t, cp, snap2).Lineage)
	uri, stop = cp.startServe(t, l.String(), "")
	if !bytes.Equal(readAll(t, "raw", uri), o.disk) {
		t.Error("the served lineage reads different")
	}
	if err, log := stop(); err != nil || !strings.Contains(log, "prefetching 17 blocks") || !strings.Contains(log, "prefetched 17 of 17 trace blocks, 0 failed") {
		t.Errorf("serve ended with %v:\n%s", err, log)
	}

	bad := filepath.Join(t.TempDir(), "bad.trace")
	if err := os.WriteFile(bad, []byte("not a trace"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := serve(ctx, cp.c, newStore(t), serveConfig{arg: snap0.String(), prefetch: bad, sock: filepath.Join(t.TempDir(), "x.sock")}, io.Discard, io.Discard); err == nil {
		t.Error("serve took a bad trace")
	}
}

// TestServeDisk writes to a disk served writable, restarts serve, and reads
// the writes back from the recovered epoch.
func TestServeDisk(t *testing.T) {
	needQEMU(t)
	ctx := context.Background()
	cp := newCheckpointer(t)
	snap0, img := cp.importDisk(t)
	disks := filepath.Join(t.TempDir(), "disks")
	uri, stop := cp.startServeWith(t, serveConfig{disk: "vm", arg: snap0.String(), disks: disks})
	if !strings.HasPrefix(uri, "nbd+unix:///vm?socket=") {
		t.Errorf("URI %q", uri)
	}
	run(t, "qemu-io", "-f", "raw", "-c", "write -P 0x61 0 64k", "-c", "write -P 0x62 1M 512", "-c", "write -z 2M 64k",
		"-c", "discard 4M 8k", "-c", "write -f -P 0x63 63M 4k", "-c", "flush", uri)
	if err, log := stop(); err != nil || !strings.Contains(log, "disk vm: new, on snapshot "+snap0.String()) || !strings.Contains(log, "writable, epoch 1 holding 0 blocks") {
		t.Fatalf("serve ended with %v:\n%s", err, log)
	}
	want := bytes.Clone(img)
	for _, w := range []struct {
		off, n int
		v      byte
	}{{0, 64 << 10, 0x61}, {1 << 20, 512, 0x62}, {2 << 20, 64 << 10, 0}, {4 << 20, 8 << 10, 0}, {63 << 20, 4 << 10, 0x63}} {
		copy(want[w.off:], bytes.Repeat([]byte{w.v}, w.n))
	}

	uri, stop = cp.startServeWith(t, serveConfig{disk: "vm", disks: disks})
	if !bytes.Equal(readAll(t, "raw", uri), want) {
		t.Error("the restarted disk reads different from what was written")
	}
	sock := filepath.Join(t.TempDir(), "again.sock")
	if err := serve(ctx, cp.c, newStore(t), serveConfig{disk: "vm", disks: disks, sock: sock}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Errorf("a second serve of the same disk: %v", err)
	}
	if err, log := stop(); err != nil || !strings.Contains(log, "writable, epoch 1 holding") {
		t.Errorf("serve ended with %v:\n%s", err, log)
	}

	other := cp.importBytes(t, make([]byte, 64<<20))
	for name, c := range map[string]struct {
		cfg  serveConfig
		want string
	}{
		"another snapshot":      {serveConfig{disk: "vm", arg: other.String(), disks: disks}, "is on snapshot"},
		"a new disk alone":      {serveConfig{disk: "fresh", disks: disks}, "is new"},
		"a bad name":            {serveConfig{disk: "../vm", arg: snap0.String(), disks: disks}, "disk name"},
		"checkpoints read-only": {serveConfig{arg: snap0.String(), every: time.Minute}, "-disk"},
		"no batch":              {serveConfig{disk: "vm", disks: disks, every: time.Minute}, "batch"},
	} {
		c.cfg.sock = filepath.Join(t.TempDir(), "x.sock")
		if err := serve(ctx, cp.c, newStore(t), c.cfg, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error about %q", name, err, c.want)
		}
	}
	uri, stop = cp.startServeWith(t, serveConfig{disk: "vm", arg: snap0.String(), disks: disks})
	if !bytes.Equal(readAll(t, "raw", uri), want) {
		t.Error("naming the disk's own snapshot changed what it reads")
	}
	stop()
}
