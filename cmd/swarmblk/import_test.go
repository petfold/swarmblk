package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/beetest"
	"github.com/petfold/swarmblk/internal/snapshot"
	"github.com/petfold/swarmblk/internal/store"
)

const testBatch = "c7af5ddacf81eaa13304ea78b497c8d719b2f5c0bf53b8ab01654cb4c65edecb"

// testImage writes a 64 MiB disk image of mostly zeros, with random data in
// its first 2 MiB and one random block near the end, and returns its path.
func testImage(t *testing.T) (string, []byte) {
	t.Helper()
	img := make([]byte, 64<<20)
	src := rand.NewChaCha8([32]byte{5})
	src.Read(img[:2<<20])
	src.Read(img[60<<20 : 60<<20+swarm.ChunkSize])
	path := filepath.Join(t.TempDir(), "disk.raw")
	if err := os.WriteFile(path, img, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, img
}

// newStore opens a store in a new temporary directory.
func newStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// startImport runs importImage against b, with a fresh store.
func startImport(t *testing.T, b *beetest.Bee, path string) (swarm.Address, *store.Store, error) {
	t.Helper()
	s := newStore(t)
	root, err := importImage(context.Background(), b.Client(t), s, testBatch, path, io.Discard)
	return root, s, err
}

func TestImport(t *testing.T) {
	path, img := testImage(t)
	ctx := context.Background()
	distinct := map[string]bool{}
	var changed int64
	diskRoot, err := builder.FeedPipeline(ctx, builder.NewPipelineBuilder(ctx, putterFunc(func(_ context.Context, ch swarm.Chunk) error {
		distinct[ch.Address().ByteString()] = true
		return nil
	}), false, redundancy.NONE), bytes.NewReader(img))
	if err != nil {
		t.Fatal(err)
	}
	for b := 0; b < len(img); b += swarm.ChunkSize {
		if !bytes.Equal(img[b:b+swarm.ChunkSize], make([]byte, swarm.ChunkSize)) {
			changed++
		}
	}

	b := beetest.New(testBatch)
	ref, s, err := startImport(t, b, path)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := snapshot.Read(ctx, s, ref)
	if err != nil {
		t.Fatal(err)
	}
	if m := snap.Meta; !m.DiskRoot.Equal(diskRoot) || m.Size != int64(len(img)) || m.Parent != nil || m.Lineage != nil ||
		len(m.Batches) != 1 || m.Batches[0] != testBatch || m.Stats.ChangedBlocks != changed || m.Stats.NewChunks != int64(len(distinct)) {
		t.Errorf("meta.json %+v; want disk root %s, %d changed blocks, %d new chunks", m, diskRoot, changed, len(distinct))
	}

	got := b.Received()
	if !got[len(got)-1].Equal(ref) {
		t.Errorf("the last chunk sent is %s, want the manifest's root %s", got[len(got)-1], ref)
	}
	sent := map[string]bool{}
	for _, a := range got {
		if sent[a.ByteString()] {
			t.Fatalf("chunk %s sent twice", a)
		}
		sent[a.ByteString()] = true
		if _, err := s.Get(ctx, a); err != nil {
			t.Fatalf("the local store lacks chunk %s: %v", a, err)
		}
	}
	for k := range distinct {
		if !sent[k] {
			t.Fatalf("chunk %s of the disk tree was not sent", swarm.NewAddress([]byte(k)))
		}
	}
	if extra := len(got) - len(distinct); extra < 2 || extra > 10 {
		t.Errorf("%d chunks besides the disk tree's, want the manifest's few", extra)
	}
}

func TestImportRefuses(t *testing.T) {
	path, _ := testImage(t)
	odd := filepath.Join(t.TempDir(), "odd.raw")
	if err := os.WriteFile(odd, make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		set  func(*beetest.Bee)
		path string
		want string
	}{
		{"mutable batch", func(b *beetest.Bee) { b.Immutable = false }, path, "mutable"},
		{"full buckets", func(b *beetest.Bee) { b.Capacity, b.Fill = 4, 4 }, path, "not enough room"},
		{"odd size", func(*beetest.Bee) {}, odd, "outside"},
	} {
		b := beetest.New(testBatch)
		c.set(b)
		_, _, err := startImport(t, b, c.path)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error about %q", c.name, err, c.want)
		}
		if b.Streams() != 0 {
			t.Errorf("%s: %d streams opened before refusing", c.name, b.Streams())
		}
		if c.name == "full buckets" && !errors.Is(err, errNoRoom) {
			t.Errorf("full buckets: %v, want errNoRoom", err)
		}
	}
}
