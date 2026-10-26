package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// importBytes imports img as a disk and returns its snapshot.
func (cp *checkpointer) importBytes(t *testing.T, img []byte) swarm.Address {
	t.Helper()
	path := filepath.Join(t.TempDir(), "disk.raw")
	if err := os.WriteFile(path, img, 0o600); err != nil {
		t.Fatal(err)
	}
	snap, err := importImage(context.Background(), cp.c, cp.s, testBatch, path, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestDiff(t *testing.T) {
	ctx := context.Background()
	cp := newCheckpointer(t)
	a, img := cp.importDisk(t)
	changed := slices.Clone(img)
	fill := func(first, count int, b byte) {
		copy(changed[first*swarm.ChunkSize:], bytes.Repeat([]byte{b}, count*swarm.ChunkSize))
	}
	fill(0, 1, 0x11)     // data over data
	fill(100, 4, 0)      // zeros over data
	fill(5000, 1, 0x22)  // data over zeros
	fill(16383, 1, 0x33) // the last block
	fill(9000, 2, 0)     // zeros over zeros: no change
	b := cp.importBytes(t, changed)

	var out, log bytes.Buffer
	if err := diff(ctx, cp.c, newStore(t), a.String(), b.String(), &out, &log); err != nil {
		t.Fatal(err)
	}
	if want := "0 1\n100 4\n5000 1\n16383 1\n"; out.String() != want {
		t.Errorf("diff printed\n%swant\n%s", out.String(), want)
	}
	if !strings.Contains(log.String(), "7 blocks (28 KiB) in 4 runs") {
		t.Errorf("diff reported %q", log.String())
	}

	out.Reset()
	if err := diff(ctx, cp.c, cp.s, b.String(), b.String(), &out, &log); err != nil || out.Len() != 0 {
		t.Errorf("a snapshot against itself: %q, %v", out.String(), err)
	}
	large := cp.importBytes(t, make([]byte, 128<<20))
	if err := diff(ctx, cp.c, cp.s, a.String(), large.String(), &out, &log); err == nil || !strings.Contains(err.Error(), "cannot be compared") {
		t.Errorf("disks of two sizes: %v", err)
	}
}
