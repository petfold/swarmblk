package tree

import (
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// load returns the references in the level-j chunk at addr. A zero subtree is
// known without a fetch: it holds Z_(j−1) in every slot.
func (g Geometry) load(ctx context.Context, get storage.Getter, j int, addr swarm.Address) ([]swarm.Address, error) {
	if j < g.Levels && addr.Equal(Zero(j)) {
		refs := make([]swarm.Address, swarm.Branches)
		for i := range refs {
			refs[i] = Zero(j - 1)
		}
		return refs, nil
	}
	ch, err := get.Get(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("level-%d chunk %s: %w", j, addr, err)
	}
	return g.Children(j, ch)
}

// open fetches a root chunk and returns the disk's geometry and the root's
// references.
func open(ctx context.Context, get storage.Getter, root swarm.Address) (Geometry, []swarm.Address, error) {
	ch, err := get.Get(ctx, root)
	if err != nil {
		return Geometry{}, nil, fmt.Errorf("root %s: %w", root, err)
	}
	return Root(ch)
}

// Rebuild returns the root of the tree that the tree under root becomes when
// the given blocks change, together with every chunk it computed: the new
// leaves and each intermediate chunk on their paths, distinct, the root last.
// blocks maps block numbers to their new 4096-byte contents; nil stands for
// zeros. get supplies the old tree's chunks, but never a zero subtree's.
//
// Chunks come from Bee's pkg/cac, so the root equals Bee's full rebuild of the
// changed image (SPEC §14, I1). Callers upload only the returned chunks that
// are not already covered (SPEC §8).
func Rebuild(ctx context.Context, get storage.Getter, root swarm.Address, blocks map[int64][]byte) (swarm.Address, []swarm.Chunk, error) {
	g, rootRefs, err := open(ctx, get, root)
	if err != nil {
		return swarm.ZeroAddress, nil, err
	}
	if len(blocks) == 0 {
		return root, nil, nil
	}

	var out []swarm.Chunk
	seen := map[string]bool{}
	add := func(ch swarm.Chunk) {
		if k := ch.Address().ByteString(); !seen[k] {
			seen[k] = true
			out = append(out, ch)
		}
	}

	// The new leaves: changed[i] is the new address of child i at the level
	// below the one being rebuilt, starting with the blocks.
	zero := make([]byte, swarm.ChunkSize)
	changed := map[int64]swarm.Address{}
	for _, b := range slices.Sorted(maps.Keys(blocks)) {
		data := blocks[b]
		if b < 0 || b >= g.Leaves() {
			return swarm.ZeroAddress, nil, fmt.Errorf("block %d is outside a disk of %d blocks", b, g.Leaves())
		}
		if data == nil {
			data = zero
		}
		if len(data) != swarm.ChunkSize {
			return swarm.ZeroAddress, nil, fmt.Errorf("block %d has %d bytes, want %d", b, len(data), swarm.ChunkSize)
		}
		ch, err := cac.New(data)
		if err != nil {
			return swarm.ZeroAddress, nil, err
		}
		changed[b] = ch.Address()
		add(ch)
	}

	// The old chunks on the changed paths, top-down: old[j][i] holds the
	// references of chunk i at level j.
	old := make([]map[int64][]swarm.Address, g.Levels+1)
	old[g.Levels] = map[int64][]swarm.Address{0: rootRefs}
	for j := g.Levels - 1; j >= 1; j-- {
		old[j] = map[int64][]swarm.Address{}
		for b := range changed {
			i := g.Index(b, j)
			if _, ok := old[j][i]; ok {
				continue
			}
			refs, err := g.load(ctx, get, j, old[j+1][g.Index(b, j+1)][g.Slot(b, j+1)])
			if err != nil {
				return swarm.ZeroAddress, nil, err
			}
			old[j][i] = refs
		}
	}

	// New chunks bottom-up, each a copy of the old one with its changed
	// references replaced.
	span := make([]byte, swarm.SpanSize)
	for j := 1; j <= g.Levels; j++ {
		parents := map[int64][]swarm.Address{}
		for _, c := range slices.Sorted(maps.Keys(changed)) {
			p, slot := c/swarm.Branches, int(c%swarm.Branches)
			if j == g.Levels {
				p, slot = 0, int(c)
			}
			refs, ok := parents[p]
			if !ok {
				refs = slices.Clone(old[j][p])
				parents[p] = refs
			}
			refs[slot] = changed[c]
		}
		changed = map[int64]swarm.Address{}
		binary.LittleEndian.PutUint64(span, uint64(g.Span(j)))
		for _, p := range slices.Sorted(maps.Keys(parents)) {
			data := slices.Clone(span)
			for _, r := range parents[p] {
				data = append(data, r.Bytes()...)
			}
			ch, err := cac.NewWithDataSpan(data)
			if err != nil {
				return swarm.ZeroAddress, nil, err
			}
			changed[p] = ch.Address()
			add(ch)
		}
	}
	return changed[0], out, nil
}

// Range is a run of consecutive blocks.
type Range struct {
	Start, Count int64
}

// Diff returns the blocks whose contents differ between the trees under a and
// b, as sorted runs. Both trees must describe disks of the same size. Diff
// skips every subtree whose address the two trees share, so it reads about
// one chunk per changed region per level.
func Diff(ctx context.Context, get storage.Getter, a, b swarm.Address) ([]Range, error) {
	ga, ra, err := open(ctx, get, a)
	if err != nil {
		return nil, err
	}
	gb, rb, err := open(ctx, get, b)
	if err != nil {
		return nil, err
	}
	if ga != gb {
		return nil, fmt.Errorf("disks of %d and %d bytes cannot be compared", ga.Size, gb.Size)
	}
	var out []Range
	var walk func(j int, first int64, xs, ys []swarm.Address) error
	walk = func(j int, first int64, xs, ys []swarm.Address) error { // xs and ys: a level-j chunk's references
		for i := range xs {
			if xs[i].Equal(ys[i]) {
				continue
			}
			c := first + int64(i) // the child's number at level j−1
			if j == 1 {
				if n := len(out); n > 0 && out[n-1].Start+out[n-1].Count == c {
					out[n-1].Count++
				} else {
					out = append(out, Range{c, 1})
				}
				continue
			}
			cx, err := ga.load(ctx, get, j-1, xs[i])
			if err != nil {
				return err
			}
			cy, err := ga.load(ctx, get, j-1, ys[i])
			if err != nil {
				return err
			}
			if err := walk(j-1, c*swarm.Branches, cx, cy); err != nil {
				return err
			}
		}
		return nil
	}
	return out, walk(ga.Levels, 0, ra, rb)
}
