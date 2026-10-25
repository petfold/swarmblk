package tree

import (
	"bytes"
	"context"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// build stores data as the tree that Bee's pipeline makes for POST /bytes at
// the given redundancy level, and returns its root.
func build(t *testing.T, data []byte, level redundancy.Level) (swarm.Address, *inmemchunkstore.ChunkStore) {
	t.Helper()
	ctx := context.Background()
	store := inmemchunkstore.New()
	root, err := builder.FeedPipeline(ctx, builder.NewPipelineBuilder(ctx, store, false, level), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return root, store
}

// sparseImage returns a disk image of mostly zeros: a random 1 MiB region,
// random blocks scattered over the first 32 MiB and one block repeated, so
// that its tree has zero subtrees and repeated leaves.
func sparseImage(size int) []byte {
	img := make([]byte, size)
	src := rand.NewChaCha8([32]byte{7})
	r := rand.New(src)
	src.Read(img[4<<20 : 5<<20])
	for range 40 {
		b := r.IntN(32<<20/swarm.ChunkSize) * swarm.ChunkSize
		src.Read(img[b : b+swarm.ChunkSize])
	}
	for _, b := range []int{10, 2000, 7000} {
		copy(img[b*swarm.ChunkSize:], bytes.Repeat([]byte("swarmblk"), swarm.ChunkSize/8))
	}
	return img
}

func TestGeometry(t *testing.T) {
	// The worked values in SPEC §2.
	for _, c := range []struct {
		size           int64
		levels, fanout int
	}{
		{64 << 20, 2, 128},
		{1 << 30, 3, 16},
		{8 << 30, 3, 128},
		{64 << 30, 4, 8},
		{1 << 40, 4, 128},
	} {
		g, err := New(c.size)
		if err != nil {
			t.Fatal(err)
		}
		if g.Levels != c.levels || g.Fanout != c.fanout || g.Leaves() != c.size/swarm.ChunkSize {
			t.Errorf("New(%d) = %+v with %d leaves, want L %d and F %d", c.size, g, g.Leaves(), c.levels, c.fanout)
		}
		if got := int64(g.Fanout) * g.Span(g.Levels-1); got != g.Span(g.Levels) {
			t.Errorf("size %d: the root's references span %d bytes", c.size, got)
		}
	}
	for _, size := range []int64{0, -1 << 30, 32 << 20, 2 << 40, 64<<20 + swarm.ChunkSize} {
		if _, err := New(size); err == nil {
			t.Errorf("New(%d) accepted a size that format 1 does not allow", size)
		}
	}
}

func TestZerosMatchBee(t *testing.T) {
	// Z_2 and Z_3 span 64 MiB and 8 GiB. TestWalkMatchesBee meets Z_2 in
	// Bee's trees, and Z_3 comes from the same loop.
	for j := range 2 {
		n := swarm.ChunkSize << (7 * j)
		if root, _ := build(t, make([]byte, n), redundancy.NONE); !root.Equal(Zero(j)) {
			t.Errorf("Z_%d = %s, but Bee's root for %d zero bytes is %s", j, Zero(j), n, root)
		}
	}
}

// TestWalkMatchesBee walks trees built by Bee's pipeline using only the
// geometry. Every leaf must hold the block of the image that SPEC §2 puts
// there, and every all-zero subtree must be Z_j.
func TestWalkMatchesBee(t *testing.T) {
	ctx := context.Background()
	for _, size := range []int{64 << 20, 128 << 20} { // L = 2 and F = 128; L = 3 and F = 2
		img := sparseImage(size)
		root, store := build(t, img, redundancy.NONE)
		rootCh, err := store.Get(ctx, root)
		if err != nil {
			t.Fatal(err)
		}
		g, refs, err := Root(rootCh)
		if err != nil {
			t.Fatal(err)
		}
		if g.Size != int64(size) {
			t.Fatalf("Root read size %d, want %d", g.Size, size)
		}

		var block int64 // the next leaf in disk order
		var walk func(j int, refs []swarm.Address)
		walk = func(j int, refs []swarm.Address) { // refs point to chunks at level j
			for _, ref := range refs {
				region := img[block*swarm.ChunkSize : block*swarm.ChunkSize+g.Span(j)]
				zero := allZero(region)
				switch {
				case zero != ref.Equal(Zero(j)):
					t.Fatalf("size %d: level-%d reference %s at block %d, Z_%d %s, region all zero: %t", size, j, ref, block, j, Zero(j), zero)
				case zero:
					block += g.Span(j) / swarm.ChunkSize
				case j == 0:
					ch, err := cac.New(region)
					if err != nil {
						t.Fatal(err)
					}
					if !ref.Equal(ch.Address()) {
						t.Fatalf("size %d: leaf %d is %s, want %s", size, block, ref, ch.Address())
					}
					block++
				default:
					ch, err := store.Get(ctx, ref)
					if err != nil {
						t.Fatal(err)
					}
					kids, err := g.Children(j, ch)
					if err != nil {
						t.Fatal(err)
					}
					walk(j-1, kids)
				}
			}
		}
		walk(g.Levels-1, refs)
		if block != g.Leaves() {
			t.Errorf("size %d: walked %d leaves, want %d", size, block, g.Leaves())
		}

		// The root read as a level-1 chunk has the wrong span (64 MiB) or the
		// wrong number of references (128 MiB).
		if _, err := g.Children(1, rootCh); err == nil {
			t.Errorf("size %d: Children accepted the root as a level-1 chunk", size)
		}
	}
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

func TestRootRejectsOtherTrees(t *testing.T) {
	for _, c := range []struct {
		name  string
		size  int
		level redundancy.Level
		want  string
	}{
		{"redundancy", 1 << 20, redundancy.MEDIUM, "Swarm-Redundancy-Level: 0"},
		{"odd size", 4<<20 + 1000, redundancy.NONE, "not a power of two"},
		{"too small", 1 << 20, redundancy.NONE, "outside"},
	} {
		root, store := build(t, make([]byte, c.size), c.level)
		ch, err := store.Get(context.Background(), root)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := Root(ch); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Root returned %v, want an error mentioning %q", c.name, err, c.want)
		}
	}
}
