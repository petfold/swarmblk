package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/file/joiner"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

var src = rand.NewChaCha8([32]byte{3})

func random(n int) []byte {
	b := make([]byte, n)
	src.Read(b)
	return b
}

// testChunks returns a full leaf, a short data chunk, a full intermediate
// chunk, and a root with two references. The root's stored file has the
// same length as the short chunk's.
func testChunks(t *testing.T) []swarm.Chunk {
	t.Helper()
	leaf, err := cac.New(random(swarm.ChunkSize))
	if err != nil {
		t.Fatal(err)
	}
	short, err := cac.New(random(72))
	if err != nil {
		t.Fatal(err)
	}
	mid, err := cac.NewWithDataSpan(append(binary.LittleEndian.AppendUint64(nil, 512<<10), random(swarm.ChunkSize)...))
	if err != nil {
		t.Fatal(err)
	}
	root, err := cac.NewWithDataSpan(append(binary.LittleEndian.AppendUint64(nil, 128<<20), random(2*swarm.HashSize)...))
	if err != nil {
		t.Fatal(err)
	}
	return []swarm.Chunk{leaf, short, mid, root}
}

func TestPutGet(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	chunks := testChunks(t)
	for _, ch := range chunks {
		for range 2 { // a second Put of the same chunk is a no-op
			if err := s.Put(ctx, ch); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i, want := range []int64{4096, 72, 4104, 72} { // payloads alone, spans kept
		ch := chunks[i]
		got, err := s.Get(ctx, ch.Address())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Data(), ch.Data()) {
			t.Errorf("chunk %d: Get returned different data", i)
		}
		fi, err := os.Stat(s.path(ch.Address()))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size() != want || fi.Mode().Perm() != 0o600 {
			t.Errorf("chunk %d: file of %d bytes, mode %v; want %d bytes, mode 0600", i, fi.Size(), fi.Mode().Perm(), want)
		}
	}
}

func TestPutRefusesWrongData(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	chunks := testChunks(t)
	forged := swarm.NewChunk(chunks[0].Address(), chunks[1].Data())
	if err := s.Put(ctx, forged); !errors.Is(err, storage.ErrInvalidChunk) {
		t.Errorf("Put of a forged chunk: %v, want storage.ErrInvalidChunk", err)
	}
	if _, err := s.Get(ctx, chunks[0].Address()); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Get after the refused Put: %v, want storage.ErrNotFound", err)
	}
}

func TestGetRemovesDamagedFile(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ch := testChunks(t)[0]
	if err := s.Put(ctx, ch); err != nil {
		t.Fatal(err)
	}
	p := s.path(ch.Address())
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[100] ^= 1
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, ch.Address()); !errors.Is(err, ErrCorrupt) {
		t.Errorf("Get of a damaged chunk: %v, want ErrCorrupt", err)
	}
	if _, err := s.Get(ctx, ch.Address()); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Get after removal: %v, want storage.ErrNotFound", err)
	}
}

func TestThrough(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	chunks := testChunks(t)
	fetches := 0
	g := s.Through(func(_ context.Context, addr swarm.Address) (swarm.Chunk, error) {
		fetches++
		for _, ch := range chunks {
			if ch.Address().Equal(addr) {
				return ch, nil
			}
		}
		return chunks[1], nil // a node returning the wrong chunk
	})
	for range 2 {
		got, err := g.Get(ctx, chunks[0].Address())
		if err != nil || !bytes.Equal(got.Data(), chunks[0].Data()) {
			t.Fatalf("Get: %v", err)
		}
	}
	if fetches != 1 {
		t.Errorf("%d fetches for two Gets, want 1: the second must come from the store", fetches)
	}
	missing := swarm.MustParseHexAddress("ab" + chunks[0].Address().String()[2:])
	if _, err := g.Get(ctx, missing); !errors.Is(err, storage.ErrInvalidChunk) {
		t.Errorf("Get answered with the wrong chunk: %v, want storage.ErrInvalidChunk", err)
	}
	if _, err := s.Get(ctx, missing); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("the wrong chunk was stored: %v", err)
	}
}

// TestBeeRoundTrip builds a file's tree with Bee's pipeline straight into the
// store and reads it back with Bee's joiner. The odd size gives a short last
// leaf next to full leaves and intermediate chunks.
func TestBeeRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := random(5<<20 + 100)
	root, err := builder.FeedPipeline(ctx, builder.NewPipelineBuilder(ctx, s, false, redundancy.NONE), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	j, size, err := joiner.New(ctx, s, s, root, redundancy.NONE)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, size)
	if _, err := io.ReadFull(j, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Error("Bee's joiner read back different bytes")
	}
}

// TestSweep fills a store with leaves of known ages and other chunks, and
// sweeps it below its size: the oldest leaves go first, a leaf read lately
// stays, the other chunks all stay, and a writer's hold stops the sweep.
func TestSweep(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	others := testChunks(t)[1:] // a short chunk, an intermediate and a root
	for _, ch := range others {
		if err := s.Put(ctx, ch); err != nil {
			t.Fatal(err)
		}
	}
	var leaves []swarm.Chunk
	start := time.Now().Add(-48 * time.Hour)
	for i := range 20 {
		ch, err := cac.New(random(swarm.ChunkSize))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, ch); err != nil {
			t.Fatal(err)
		}
		used := start.Add(time.Duration(i) * time.Hour) // leaf 0 is the oldest
		if err := os.Chtimes(s.path(ch.Address()), used, used); err != nil {
			t.Fatal(err)
		}
		leaves = append(leaves, ch)
	}
	if _, err := s.Get(ctx, leaves[0].Address()); err != nil { // used now
		t.Fatal(err)
	}

	if sw, err := s.Sweep(1 << 30); err != nil || sw.Removed != 0 {
		t.Fatalf("a sweep under the limit: %+v, %v", sw, err)
	}
	release, err := s.Hold()
	if err != nil {
		t.Fatal(err)
	}
	if sw, err := s.Sweep(1); err != nil || !sw.Skipped || sw.Removed != 0 {
		t.Fatalf("a sweep while a writer holds the store: %+v, %v", sw, err)
	}
	release()

	limit := int64(12 * swarm.ChunkSize) // the leaves alone take 20 blocks
	sw, err := s.Sweep(limit)
	if err != nil {
		t.Fatal(err)
	}
	if sw.Bytes > limit/10*9 || sw.Removed == 0 || sw.Freed != sw.Removed*swarm.ChunkSize {
		t.Fatalf("swept to %+v, want at most %d bytes", sw, limit/10*9)
	}
	for i, ch := range leaves {
		_, err := s.Get(ctx, ch.Address())
		// Leaf 0 was read lately, so leaves 1 to Removed went.
		if gone := i >= 1 && int64(i) <= sw.Removed; gone != errors.Is(err, storage.ErrNotFound) {
			t.Errorf("leaf %d: %v, removed %d", i, err, sw.Removed)
		}
	}
	for _, ch := range others {
		if _, err := s.Get(ctx, ch.Address()); err != nil {
			t.Errorf("chunk %s, not a leaf, was removed: %v", ch.Address(), err)
		}
	}
}
