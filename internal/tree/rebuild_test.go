package tree

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// mutate changes blocks of img, a sparseImage, in place and returns the
// changes as Rebuild takes them: random data anywhere (mostly into zero
// regions), zeros over the random 1 MiB region, one block written three
// times, and the first and last blocks.
func mutate(img []byte, r *rand.Rand, src *rand.ChaCha8, n int) map[int64][]byte {
	blocks := map[int64][]byte{}
	set := func(b int64, data []byte) {
		block := img[b*swarm.ChunkSize : (b+1)*swarm.ChunkSize]
		if data == nil {
			clear(block)
		} else {
			copy(block, data)
		}
		blocks[b] = data
	}
	random := func() []byte {
		data := make([]byte, swarm.ChunkSize)
		src.Read(data)
		return data
	}
	last := int64(len(img)/swarm.ChunkSize) - 1
	for range n {
		set(r.Int64N(last+1), random())
	}
	for range n / 4 {
		set(1024+r.Int64N(256), nil) // sparseImage's random region is blocks 1024 to 1279
	}
	again := random()
	for range 3 {
		set(r.Int64N(last+1), again)
	}
	set(0, random())
	set(last, random())
	return blocks
}

// changedRanges compares two images block by block.
func changedRanges(a, b []byte) []Range {
	var out []Range
	for i := int64(0); i < int64(len(a)/swarm.ChunkSize); i++ {
		lo, hi := i*swarm.ChunkSize, (i+1)*swarm.ChunkSize
		if bytes.Equal(a[lo:hi], b[lo:hi]) {
			continue
		}
		if n := len(out); n > 0 && out[n-1].Start+out[n-1].Count == i {
			out[n-1].Count++
		} else {
			out = append(out, Range{i, 1})
		}
	}
	return out
}

func TestRebuildMatchesBee(t *testing.T) {
	t.Parallel()
	for _, size := range []int{64 << 20, 128 << 20} { // L = 2 and F = 128; L = 3 and F = 2
		t.Run(fmt.Sprintf("%dMiB", size>>20), func(t *testing.T) {
			t.Parallel()
			rebuildRounds(t, size)
		})
	}
}

// rebuildRounds checks two rounds of changes to a sparse image of size bytes.
func rebuildRounds(t *testing.T, size int) {
	ctx := context.Background()
	img := sparseImage(size)
	root, store := build(t, img, redundancy.NONE)
	src := rand.NewChaCha8([32]byte{byte(size >> 20)})
	r := rand.New(src)
	for round := range 2 {
		before := slices.Clone(img)
		blocks := mutate(img, r, src, 50)
		if round == 1 { // zero a whole 512 KiB subtree that held data, so it becomes Z_1
			for b := int64(1024); b < 1024+swarm.Branches; b++ {
				blocks[b] = nil
				clear(img[b*swarm.ChunkSize : (b+1)*swarm.ChunkSize])
			}
		}

		got, chunks, err := Rebuild(ctx, store, root, blocks)
		if err != nil {
			t.Fatal(err)
		}
		if want, _ := build(t, img, redundancy.NONE); !got.Equal(want) {
			t.Fatalf("round %d: Rebuild gave root %s, Bee builds %s", round, got, want)
		}
		if n := len(chunks); n == 0 || !chunks[n-1].Address().Equal(got) {
			t.Errorf("round %d: the root is not the last of %d chunks", round, n)
		}
		seen := map[string]bool{}
		for _, ch := range chunks {
			if !cac.Valid(ch) || seen[ch.Address().ByteString()] {
				t.Errorf("round %d: chunk %s is invalid or repeated", round, ch.Address())
			}
			seen[ch.Address().ByteString()] = true
			if err := store.Put(ctx, ch); err != nil {
				t.Fatal(err)
			}
		}

		ranges, err := Diff(ctx, store, root, got)
		if err != nil {
			t.Fatal(err)
		}
		if want := changedRanges(before, img); !slices.Equal(ranges, want) {
			t.Errorf("round %d: Diff found %d runs, want %d", round, len(ranges), len(want))
		}
		root = got
	}
	if unchanged, chunks, err := Rebuild(ctx, store, root, nil); err != nil || !unchanged.Equal(root) || len(chunks) != 0 {
		t.Errorf("Rebuild with no blocks gave %s and %d chunks (%v)", unchanged, len(chunks), err)
	}
}

// emptyRoot stores and returns the root of an all-zero disk of size bytes
// (SPEC §3): Z_(L−1) repeated F times under the disk's span.
func emptyRoot(t *testing.T, store *inmemchunkstore.ChunkStore, size int64) swarm.Address {
	t.Helper()
	g, err := New(size)
	if err != nil {
		t.Fatal(err)
	}
	data := binary.LittleEndian.AppendUint64(nil, uint64(size))
	for range g.Fanout {
		data = append(data, Zero(g.Levels-1).Bytes()...)
	}
	ch, err := cac.NewWithDataSpan(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	return ch.Address()
}

func TestRebuildRejectsBadBlocks(t *testing.T) {
	ctx := context.Background()
	store := inmemchunkstore.New()
	root := emptyRoot(t, store, 64<<20)
	for name, blocks := range map[string]map[int64][]byte{
		"past the end": {16384: nil},
		"negative":     {-1: nil},
		"short block":  {7: make([]byte, 100)},
	} {
		if _, _, err := Rebuild(ctx, store, root, blocks); err == nil {
			t.Errorf("%s: Rebuild accepted it", name)
		}
	}
}

func TestDiffNeedsEqualSizes(t *testing.T) {
	ctx := context.Background()
	store := inmemchunkstore.New()
	small, large := emptyRoot(t, store, 64<<20), emptyRoot(t, store, 128<<20)
	if _, err := Diff(ctx, store, small, large); err == nil {
		t.Error("Diff compared disks of different sizes")
	}
	if ranges, err := Diff(ctx, store, large, large); err != nil || len(ranges) != 0 {
		t.Errorf("Diff of a tree with itself: %v, %v", ranges, err)
	}
}

func TestPathArithmetic(t *testing.T) {
	for _, size := range []int64{64 << 20, 1 << 30, 64 << 30, 1 << 40} {
		g, err := New(size)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range []int64{0, 1, 127, 128, g.Leaves() / 3, g.Leaves() - 1} {
			if g.Index(b, g.Levels) != 0 || g.Slot(b, g.Levels) >= g.Fanout {
				t.Errorf("size %d, block %d: root index %d, slot %d", size, b, g.Index(b, g.Levels), g.Slot(b, g.Levels))
			}
			// Each step up the path divides the chunk number by 128, and the
			// slot is the lower chunk's place within its parent.
			for j := 1; j < g.Levels; j++ {
				if g.Index(b, j) != g.Index(b, j-1)/swarm.Branches || int64(g.Slot(b, j)) != g.Index(b, j-1)%swarm.Branches {
					t.Errorf("size %d, block %d, level %d: index %d, slot %d", size, b, j, g.Index(b, j), g.Slot(b, j))
				}
			}
		}
	}
}
