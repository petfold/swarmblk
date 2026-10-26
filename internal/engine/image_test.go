package engine

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

func fill(v byte) []byte { return bytes.Repeat([]byte{v}, swarm.ChunkSize) }

func testRoot(c string) swarm.Address { return swarm.MustParseHexAddress(strings.Repeat(c, 64)) }

// TestImage puts blocks in an image, drops those for another root, and
// reopens it as a crash would leave it: what was synced is there, and what
// was not is not. Advance then moves it to the next disk.
func TestImage(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "image")
	m, err := OpenImage(dir, 64*swarm.ChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	if n, blocks := m.Present(); n != 0 || blocks != 64 || !m.Root().IsZero() {
		t.Fatalf("a new image holds %d of %d blocks, root %s", n, blocks, m.Root())
	}
	a, b, c := testRoot("a"), testRoot("b"), testRoot("c")
	if err := m.Put(a, 1, fill(1)); err != nil || m.Has(1) {
		t.Fatalf("a block for a root the image does not hold: %v", err)
	}
	if err := m.Reset(a); err != nil {
		t.Fatal(err)
	}
	for blk, data := range map[int64][]byte{1: fill(1), 2: fill(2), 3: fill(0)} {
		if err := m.Put(a, blk, data); err != nil {
			t.Fatal(err)
		}
	}
	m.Put(a, 1, fill(9)) // the image has block 1 already
	if err := m.PutZeros(a, 8, 20); err != nil {
		t.Fatal(err)
	}
	if err := m.Sync(); err != nil {
		t.Fatal(err)
	}
	m.Put(a, 40, fill(40)) // never synced
	if got, ok := m.Block(1); !ok || !bytes.Equal(got, fill(1)) {
		t.Error("block 1 reads different")
	}

	// A crash: the files are opened again without a Sync or a Close.
	m2, err := OpenImage(dir, 64*swarm.ChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := m2.Present(); n != 23 || !m2.Root().Equal(a) || m2.Has(40) {
		t.Errorf("after a crash the image holds %d blocks, root %s, block 40 %v; want 23 and %s", n, m2.Root(), m2.Has(40), a)
	}
	for blk, want := range map[int64][]byte{2: fill(2), 3: fill(0), 10: fill(0)} {
		if got, ok := m2.Block(blk); !ok || !bytes.Equal(got, want) {
			t.Errorf("block %d after a crash: %v", blk, ok)
		}
	}

	// Advance applies the next disk's changes and drops what is fetched
	// meanwhile.
	err = m2.Advance(a, b, func(put func(int64, []byte) error) error {
		m2.Put(a, 5, fill(5))
		if err := put(2, fill(22)); err != nil {
			return err
		}
		return put(10, nil)
	})
	if err != nil || !m2.Root().Equal(b) || m2.Has(5) {
		t.Fatalf("Advance: %v, root %s, block 5 %v", err, m2.Root(), m2.Has(5))
	}
	if got, _ := m2.Block(2); !bytes.Equal(got, fill(22)) {
		t.Error("Advance did not change block 2")
	}
	if m2.Put(a, 6, fill(6)); m2.Has(6) {
		t.Error("a block for the old root went in after Advance")
	}
	if err := m2.Advance(a, b, nil); err != nil || !m2.Root().Equal(b) {
		t.Errorf("a repeated Advance: %v", err)
	}
	if err := m2.Advance(a, c, nil); err != nil || !m2.Root().Equal(c) {
		t.Errorf("an Advance from a root the image does not hold: %v", err)
	}
	if n, _ := m2.Present(); n != 0 {
		t.Errorf("an image moved from elsewhere kept %d blocks", n)
	}
	if err := m2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestHydrate reads part of the test disk while its image hydrates, and
// then reads all of it with every fetch failing: the image alone must
// hold it.
func TestHydrate(t *testing.T) {
	img, root, s := testDisk(t)
	ctx := context.Background()
	cache := inmemchunkstore.New() // the tree's chunks stay here
	through := storage.GetterFunc(func(ctx context.Context, a swarm.Address) (swarm.Chunk, error) {
		if ch, err := cache.Get(ctx, a); err == nil {
			return ch, nil
		}
		ch, err := s.Get(ctx, a)
		if err == nil {
			cache.Put(ctx, ch)
		}
		return ch, err
	})
	snap, err := Open(ctx, through, root, 8)
	if err != nil {
		t.Fatal(err)
	}
	m, err := OpenImage(filepath.Join(t.TempDir(), "image"), snap.Size())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Reset(root); err != nil {
		t.Fatal(err)
	}
	snap.SetImage(m, swarm.ZeroAddress)
	h, err := snap.Hydrate(ctx, s, 4)
	if err != nil {
		t.Fatal(err)
	}
	part := make([]byte, 1<<20)
	if err := snap.ReadAt(ctx, part, 4<<20); err != nil || !bytes.Equal(part, img[4<<20:5<<20]) {
		t.Fatalf("a read during hydration: %v", err)
	}
	<-h.Done()
	if fetched, failed, present, blocks := h.Progress(); h.Err() != nil || failed != 0 || present != blocks {
		t.Fatalf("hydration ended with %v: %d fetched, %d failed, %d of %d present", h.Err(), fetched, failed, present, blocks)
	}

	off, err := Open(ctx, through, root, 8)
	if err != nil {
		t.Fatal(err)
	}
	off.get = storage.GetterFunc(func(context.Context, swarm.Address) (swarm.Chunk, error) {
		return nil, errors.New("no network")
	})
	off.SetImage(m, swarm.ZeroAddress)
	got := make([]byte, off.Size())
	if err := off.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, img) {
		t.Fatalf("reading the hydrated disk without a network: %v", err)
	}
	if blk, ok := off.LocalBlock(ctx, 1024); !ok || !bytes.Equal(blk, img[1024*swarm.ChunkSize:1025*swarm.ChunkSize]) {
		t.Error("LocalBlock does not find a hydrated block")
	}
	// Block status from the image's holes, without the tree, is the tree's.
	want, err := snap.Extents(ctx, 0, snap.Size())
	if err != nil {
		t.Fatal(err)
	}
	snap.SetImage(nil, swarm.ZeroAddress)
	tree, err := snap.Extents(ctx, 0, snap.Size())
	if err != nil {
		t.Fatal(err)
	}
	got2, err := off.Extents(ctx, 0, off.Size())
	if err != nil || !slices.Equal(got2, tree) || !slices.Equal(want, tree) {
		t.Errorf("block status from the image differs from the tree's: %v", err)
	}
}

// TestImageAhead gives a snapshot an image of another disk: reads must
// use it only when told that the image is ahead at that disk.
func TestImageAhead(t *testing.T) {
	_, root, s := testDisk(t)
	ctx := context.Background()
	snap, err := Open(ctx, s, root, 8)
	if err != nil {
		t.Fatal(err)
	}
	m, err := OpenImage(filepath.Join(t.TempDir(), "image"), snap.Size())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	next := testRoot("d")
	if err := m.Reset(next); err != nil {
		t.Fatal(err)
	}
	m.Put(next, 1030, fill(7)) // block 1030 has random data in the test disk
	snap.SetImage(m, swarm.ZeroAddress)
	if blk, ok := snap.LocalBlock(ctx, 1030); ok && bytes.Equal(blk, fill(7)) {
		t.Error("a snapshot used an image of another disk")
	}
	snap.SetImage(m, next)
	if blk, ok := snap.LocalBlock(ctx, 1030); !ok || !bytes.Equal(blk, fill(7)) {
		t.Error("a snapshot did not use the image ahead of it")
	}
	leaf, _, err := snap.leaf(ctx, 1031)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snap.block(ctx, 1031, leaf); err != nil || m.Has(1031) {
		t.Errorf("a block fetched for the snapshot's own disk went into the image ahead: %v", err)
	}
}
