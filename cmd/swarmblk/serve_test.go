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
	"strings"
	"testing"

	"github.com/petfold/swarmblk/internal/lineage"
)

// startServe runs serve for arg or root with an empty store, as on another
// host, and returns the URI it prints and a function that stops it and
// returns its error and log.
func (cp *checkpointer) startServe(t *testing.T, arg, root string) (string, func() (error, string)) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "nbd.sock")
	pr, pw := io.Pipe()
	var log bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	s := newStore(t)
	go func() {
		done <- serve(ctx, cp.c, s, arg, root, sock, pw, &log)
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
		if err := serve(context.Background(), cp.c, newStore(t), "", root, filepath.Join(dir, "x.sock"), io.Discard, io.Discard); err == nil {
			t.Errorf("serve took the root %q", root)
		}
	}
}
