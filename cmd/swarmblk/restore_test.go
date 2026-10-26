package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/lineage"
	"github.com/petfold/swarmblk/internal/snapshot"
)

// exportFresh exports what arg names with an empty store, as on another
// host, and returns the image and the log.
func (cp *checkpointer) exportFresh(t *testing.T, arg string) (swarm.Address, []byte, string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "disk.raw")
	var log bytes.Buffer
	snap, err := export(context.Background(), cp.c, newStore(t), arg, out, &log)
	t.Log("\n" + log.String())
	if err != nil {
		t.Fatal(err)
	}
	img, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return snap, img, log.String()
}

func TestExport(t *testing.T) {
	cp := newCheckpointer(t)
	snap0, img := cp.importDisk(t)
	got, disk, log := cp.exportFresh(t, snap0.String())
	if !got.Equal(snap0) || !bytes.Equal(disk, img) {
		t.Fatalf("export of %s gave %s and %d bytes that differ from the image", snap0, got, len(disk))
	}
	if sum := fmt.Sprintf("sha256 %x", sha256.Sum256(img)); !strings.Contains(log, sum) {
		t.Errorf("export reported %q, want %q", log, sum)
	}

	ctx := context.Background()
	out := filepath.Join(t.TempDir(), "disk.raw")
	if err := os.WriteFile(out, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	l, _ := testLineageName(t)
	for name, c := range map[string]struct{ arg, out, want string }{
		"an existing file": {snap0.String(), out, "exists"},
		"a bad name":       {"swarmblk", filepath.Join(t.TempDir(), "x.raw"), "neither a snapshot"},
		"a disk root":      {mustMeta(t, cp, snap0).DiskRoot.String(), filepath.Join(t.TempDir(), "y.raw"), "snapshot"},
		"a new lineage":    {l, filepath.Join(t.TempDir(), "z.raw"), "no update"},
	} {
		if _, err := export(ctx, cp.c, newStore(t), c.arg, c.out, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error about %q", name, err, c.want)
		}
		if name != "an existing file" {
			if _, err := os.Stat(c.out); !os.IsNotExist(err) {
				t.Errorf("%s: export left %s behind", name, c.out)
			}
		}
	}
	if b, err := os.ReadFile(out); err != nil || string(b) != "keep" {
		t.Errorf("export changed an existing file: %q, %v", b, err)
	}
}

// testLineageName returns a lineage that has no updates.
func testLineageName(t *testing.T) (string, lineage.Lineage) {
	t.Helper()
	l, _, err := lineage.Create(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return l.String(), l
}

func mustMeta(t *testing.T, cp *checkpointer, snap swarm.Address) snapshot.Meta {
	t.Helper()
	s, err := snapshot.Read(context.Background(), cp.bee.Store, snap)
	if err != nil {
		t.Fatal(err)
	}
	return s.Meta
}

// TestRestoreLineage checkpoints an overlay, exports and restores its
// lineage elsewhere, and checkpoints the restored overlay, which continues
// the lineage on this host.
func TestRestoreLineage(t *testing.T) {
	needQEMU(t)
	ctx := context.Background()
	cp := newCheckpointer(t)
	snap0, img := cp.importDisk(t)
	o := newOverlay(t, snap0, img)
	o.write(t, "write -P 0x21 0 64k", "write -z 2M 1M")
	snap1, err := cp.run(t, o, false)
	if err != nil {
		t.Fatal(err)
	}
	l, _, err := lineage.FromMeta(*mustMeta(t, cp, snap1).Lineage)
	if err != nil {
		t.Fatal(err)
	}

	got, disk, log := cp.exportFresh(t, l.String())
	if !got.Equal(snap1) || !bytes.Equal(disk, o.disk) || !strings.Contains(log, "update 0") {
		t.Fatalf("export of lineage %s gave snapshot %s, want %s, and its disk must be the VM's", l, got, snap1)
	}

	path := filepath.Join(t.TempDir(), "restored.qcow2")
	var rlog bytes.Buffer
	if got, err = restore(ctx, cp.c, newStore(t), "http://bee.invalid/", l.String(), path, &rlog); err != nil || !got.Equal(snap1) {
		t.Fatalf("restore: %s, %v; want %s", got, err, snap1)
	}
	ov, err := openOverlay(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if !ov.Snapshot.Equal(snap1) || ov.Size != int64(len(img)) || ov.Backing != testBacking(t, snap1) {
		t.Errorf("restored overlay: %+v, want one on %s with backing %s", ov, snap1, testBacking(t, snap1))
	}

	r := &testOverlay{path: path, disk: o.disk}
	r.write(t, "write -P 0x22 4M 4k")
	n := len(cp.bee.Received())
	snap2, err := cp.run(t, r, false)
	if err != nil {
		t.Fatal(err)
	}
	cp.check(t, r, snap2, snap1, 1, cp.since(n))

	if _, err := restore(ctx, cp.c, cp.s, "http://bee.invalid", snap0.String(), path, &rlog); err == nil || !strings.Contains(err.Error(), "exists") {
		t.Errorf("restore over an existing file: %v", err)
	}
	if ov, err := openOverlay(ctx, path); err != nil || !ov.Snapshot.Equal(snap2) {
		t.Errorf("after the refused restore the overlay is %+v, %v; want it on %s", ov, err, snap2)
	}
	for _, api := range []string{"localhost:1633", "ftp://bee", "http://"} {
		if _, err := restore(ctx, cp.c, cp.s, api, snap0.String(), filepath.Join(t.TempDir(), "o.qcow2"), &rlog); err == nil {
			t.Errorf("restore made an overlay for the Bee API %q", api)
		}
	}
}
