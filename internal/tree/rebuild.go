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
// the given blocks change, together with every chunk that differs from the
// old tree at its place: the changed leaves and each intermediate chunk on
// their paths, distinct, the root last. blocks maps block numbers to their
// new 4096-byte contents; nil stands for zeros. A block whose contents did
// not change adds nothing, and if none changed, Rebuild returns root and no
// chunks. get supplies the old tree's chunks, but never a zero subtree's.
//
// Chunks come from Bee's pkg/cac, so the root equals Bee's full rebuild of the
// changed image (SPEC §14, I1). Callers upload only the returned chunks that
// are not already covered (SPEC §8).
func Rebuild(ctx context.Context, get storage.Getter, root swarm.Address, blocks map[int64][]byte) (swarm.Address, []swarm.Chunk, error) {
	g, rootRefs, err := open(ctx, get, root)
	if err != nil {
		return swarm.ZeroAddress, nil, err
	}
	for b, data := range blocks {
		if b < 0 || b >= g.Leaves() {
			return swarm.ZeroAddress, nil, fmt.Errorf("block %d is outside a disk of %d blocks", b, g.Leaves())
		}
		if data != nil && len(data) != swarm.ChunkSize {
			return swarm.ZeroAddress, nil, fmt.Errorf("block %d has %d bytes, want %d", b, len(data), swarm.ChunkSize)
		}
	}
	if len(blocks) == 0 {
		return root, nil, nil
	}

	// The old chunks on the blocks' paths, top-down: old[j][i] holds the
	// references of chunk i at level j.
	old := make([]map[int64][]swarm.Address, g.Levels+1)
	old[g.Levels] = map[int64][]swarm.Address{0: rootRefs}
	for j := g.Levels - 1; j >= 1; j-- {
		old[j] = map[int64][]swarm.Address{}
		for b := range blocks {
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

	var out []swarm.Chunk
	seen := map[string]bool{}
	add := func(ch swarm.Chunk) {
		if k := ch.Address().ByteString(); !seen[k] {
			seen[k] = true
			out = append(out, ch)
		}
	}

	// The new leaves that differ from the old ones: changed[i] is the new
	// address of child i at the level below the one being rebuilt, starting
	// with the blocks.
	zero := make([]byte, swarm.ChunkSize)
	changed := map[int64]swarm.Address{}
	for _, b := range slices.Sorted(maps.Keys(blocks)) {
		data := blocks[b]
		if data == nil {
			data = zero
		}
		ch, err := cac.New(data)
		if err != nil {
			return swarm.ZeroAddress, nil, err
		}
		if ch.Address().Equal(old[1][g.Index(b, 1)][g.Slot(b, 1)]) {
			continue
		}
		changed[b] = ch.Address()
		add(ch)
	}
	if len(changed) == 0 {
		return root, nil, nil
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

// Changes returns the chunks of the tree under b that differ from the tree
// under a at their place, distinct, each after its children and b's root
// last: what a writer uploads when b follows a (SPEC §8). Like Diff, it
// skips every subtree whose address the two trees share. get must hold the
// chunks of both trees on the differing paths, except zero subtrees'.
func Changes(ctx context.Context, get storage.Getter, a, b swarm.Address) ([]swarm.Address, error) {
	if a.Equal(b) {
		return nil, nil
	}
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
	var out []swarm.Address
	seen := map[string]bool{}
	add := func(addr swarm.Address) {
		if k := addr.ByteString(); !seen[k] {
			seen[k] = true
			out = append(out, addr)
		}
	}
	var walk func(j int, xs, ys []swarm.Address) error
	walk = func(j int, xs, ys []swarm.Address) error { // xs and ys: a level-j chunk's references
		for i := range ys {
			if xs[i].Equal(ys[i]) {
				continue
			}
			if j > 1 {
				cx, err := ga.load(ctx, get, j-1, xs[i])
				if err != nil {
					return err
				}
				cy, err := ga.load(ctx, get, j-1, ys[i])
				if err != nil {
					return err
				}
				if err := walk(j-1, cx, cy); err != nil {
					return err
				}
			}
			add(ys[i])
		}
		return nil
	}
	if err := walk(ga.Levels, ra, rb); err != nil {
		return nil, err
	}
	add(b)
	return out, nil
}

// Walk calls fn for each run of blocks of the disk under root, in order:
// count blocks from first on, whose leaves are all leaf. A zero subtree is a
// single run of Z_0, known without a fetch; elsewhere a run is consecutive
// blocks with the same leaf. get supplies the tree's intermediate chunks,
// never a zero subtree's, and Walk returns the disk's geometry.
func Walk(ctx context.Context, get storage.Getter, root swarm.Address, fn func(first, count int64, leaf swarm.Address) error) (Geometry, error) {
	g, refs, err := open(ctx, get, root)
	if err != nil {
		return Geometry{}, err
	}
	var run struct {
		first, count int64
		leaf         swarm.Address
	}
	emit := func(first, count int64, leaf swarm.Address) error {
		if run.count > 0 && run.first+run.count == first && run.leaf.Equal(leaf) {
			run.count += count
			return nil
		}
		if run.count > 0 {
			if err := fn(run.first, run.count, run.leaf); err != nil {
				return err
			}
		}
		run.first, run.count, run.leaf = first, count, leaf
		return nil
	}
	var walk func(j int, first int64, refs []swarm.Address) error
	walk = func(j int, first int64, refs []swarm.Address) error { // refs: a level-j chunk's references
		per := int64(1) << (7 * (j - 1)) // blocks under each child
		for i, r := range refs {
			b := first + int64(i)*per
			switch {
			case j == 1:
				err = emit(b, 1, r)
			case r.Equal(Zero(j - 1)):
				err = emit(b, per, Zero(0))
			default:
				var children []swarm.Address
				if children, err = g.load(ctx, get, j-1, r); err == nil {
					err = walk(j-1, b, children)
				}
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(g.Levels, 0, refs); err != nil {
		return g, err
	}
	return g, fn(run.first, run.count, run.leaf)
}
