// Package engine serves swarmblk disks as block devices (DESIGN §3, §8): a
// snapshot's disk, read from its chunk tree, and a writable disk on top of
// one, with an optional local copy of the snapshot's disk.
package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/petfold/swarmblk/internal/nbd"
	"github.com/petfold/swarmblk/internal/tree"
)

// Fetchers is how many chunks a Snapshot fetches at once by default.
const Fetchers = 64

// Snapshot is a read-only block device over a snapshot's disk tree
// (DESIGN §3). It keeps every intermediate chunk it reads, decoded, so that
// a block costs one fetch once its path is known. Zero subtrees and zero
// leaves are known by their addresses: they read as zeros without a fetch,
// and block status reports them as holes. A request's blocks are fetched in
// parallel, at most a set number at a time across all requests, and a
// chunk that several requests need at once is fetched once.
type Snapshot struct {
	g      tree.Geometry
	addr   swarm.Address   // the disk tree's root
	root   []swarm.Address // its references
	get    storage.Getter
	sem    chan struct{}
	leaves singleflight.Group
	tree   singleflight.Group

	mu    sync.RWMutex
	nodes map[string][]swarm.Address // intermediate chunks' references, by address

	rec atomic.Pointer[Recorder] // notes reads for a boot trace, if set
	pf  atomic.Pointer[Prefetch] // follows a boot trace ahead of the guest, if set

	local storage.Getter           // chunks at hand without a fetch, if set
	img   atomic.Pointer[imageRef] // a local copy that reads look in first and fill, if set
}

// imageRef is a Snapshot's image, and a root that it may hold instead of
// the Snapshot's.
type imageRef struct {
	m     *Image
	ahead swarm.Address
}

// SetImage gives the Snapshot a local copy of its disk, which reads look in
// first and put what they fetch in. Reads use the image while it holds this
// disk, or ahead, if not zero: the next snapshot's disk, when a frozen
// epoch above holds every block in which the two differ. A nil image
// removes it.
func (s *Snapshot) SetImage(m *Image, ahead swarm.Address) {
	if m == nil {
		s.img.Store(nil)
		return
	}
	s.img.Store(&imageRef{m, ahead})
}

// Root returns the root of the disk tree.
func (s *Snapshot) Root() swarm.Address { return s.addr }

// SetLocal tells the Snapshot where chunks are at hand without a fetch,
// such as the local store, for LocalBlock. Call it before use.
func (s *Snapshot) SetLocal(local storage.Getter) { s.local = local }

// Open opens the disk under root. get supplies chunks after checking them,
// typically from the local store reading through Bee; fetchers bounds the
// gets in flight, and 0 means Fetchers.
func Open(ctx context.Context, get storage.Getter, root swarm.Address, fetchers int) (*Snapshot, error) {
	ch, err := get.Get(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("disk root %s: %w", root, err)
	}
	g, refs, err := tree.Root(ch)
	if err != nil {
		return nil, err
	}
	if fetchers <= 0 {
		fetchers = Fetchers
	}
	return &Snapshot{g: g, addr: root, root: refs, get: get, sem: make(chan struct{}, fetchers), nodes: map[string][]swarm.Address{}}, nil
}

// Size returns the disk's size in bytes.
func (s *Snapshot) Size() int64 { return s.g.Size }

// fetch gets the chunk at addr, with at most the set number of gets in
// flight.
func (s *Snapshot) fetch(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-s.sem }()
	return s.get.Get(ctx, addr)
}

// chunk gets the leaf at addr, once for all who ask at the same time.
func (s *Snapshot) chunk(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	v, err, _ := s.leaves.Do(addr.ByteString(), func() (any, error) { return s.fetch(ctx, addr) })
	if err != nil {
		return nil, err
	}
	return v.(swarm.Chunk), nil
}

// cached returns the references of the intermediate chunk at k, if known.
func (s *Snapshot) cached(k string) ([]swarm.Address, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	refs, ok := s.nodes[k]
	return refs, ok
}

// node returns the references in the level-j chunk at addr, which is not a
// zero subtree. It fetches the chunk once: whoever comes while it is being
// fetched waits for it, and whoever comes after finds it cached.
func (s *Snapshot) node(ctx context.Context, j int, addr swarm.Address) ([]swarm.Address, error) {
	k := addr.ByteString()
	if refs, ok := s.cached(k); ok {
		return refs, nil
	}
	v, err, _ := s.tree.Do(k, func() (any, error) {
		if refs, ok := s.cached(k); ok {
			return refs, nil
		}
		ch, err := s.fetch(ctx, addr)
		if err != nil {
			return nil, fmt.Errorf("level-%d chunk %s: %w", j, addr, err)
		}
		refs, err := s.g.Children(j, ch)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.nodes[k] = refs
		s.mu.Unlock()
		return refs, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]swarm.Address), nil
}

// leaf returns block b's leaf and how many blocks from b on certainly share
// it: the rest of a zero subtree, whose leaves are all Z_0, or else 1.
func (s *Snapshot) leaf(ctx context.Context, b int64) (swarm.Address, int64, error) {
	refs := s.root
	for j := s.g.Levels; ; j-- {
		child := refs[s.g.Slot(b, j)]
		if j == 1 {
			return child, 1, nil
		}
		if child.Equal(tree.Zero(j - 1)) {
			per := int64(1) << (7 * (j - 1)) // blocks under the child
			return tree.Zero(0), per - b%per, nil
		}
		var err error
		if refs, err = s.node(ctx, j-1, child); err != nil {
			return swarm.ZeroAddress, 0, err
		}
	}
}

// LocalBlock returns block b's contents if they are at hand without
// waiting for the network: in the image, in a zero subtree or a zero leaf,
// or with the leaf and its path in the tree cache or the local getter.
func (s *Snapshot) LocalBlock(ctx context.Context, b int64) ([]byte, bool) {
	if b < 0 || b >= s.g.Leaves() {
		return nil, false
	}
	if block, ok := s.imageBlock(b); ok {
		return block, true
	}
	refs := s.root
	for j := s.g.Levels; ; j-- {
		child := refs[s.g.Slot(b, j)]
		if child.Equal(tree.Zero(j - 1)) {
			return make([]byte, swarm.ChunkSize), true
		}
		if j == 1 {
			if s.local == nil {
				return nil, false
			}
			ch, err := s.local.Get(ctx, child)
			if err != nil || binary.LittleEndian.Uint64(ch.Data()) != swarm.ChunkSize {
				return nil, false
			}
			block := make([]byte, swarm.ChunkSize) // a payload may lack trailing zeros
			copy(block, ch.Data()[swarm.SpanSize:])
			return block, true
		}
		next, ok := s.cached(child.ByteString())
		if !ok {
			if s.local == nil {
				return nil, false
			}
			ch, err := s.local.Get(ctx, child)
			if err != nil {
				return nil, false
			}
			if next, err = s.g.Children(j-1, ch); err != nil {
				return nil, false
			}
		}
		refs = next
	}
}

// ReadAt fills p with the disk's bytes from off on.
func (s *Snapshot) ReadAt(ctx context.Context, p []byte, off int64) error {
	end := off + int64(len(p))
	if off < 0 || end > s.g.Size {
		return nbd.EINVAL
	}
	if rec := s.rec.Load(); rec != nil {
		rec.touch(off/swarm.ChunkSize, (end-1)/swarm.ChunkSize)
	}
	pf := s.pf.Load()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(cap(s.sem))
	for first := off - off%swarm.ChunkSize; first < end; first += swarm.ChunkSize {
		if pf != nil {
			pf.read(first / swarm.ChunkSize)
		}
		lo, hi := max(first, off), min(first+swarm.ChunkSize, end)
		dst := p[lo-off : hi-off]
		g.Go(func() error {
			b := first / swarm.ChunkSize
			if block, ok := s.imageBlock(b); ok {
				copy(dst, block[lo-first:hi-first])
				return nil
			}
			leaf, _, err := s.leaf(gctx, b)
			if err != nil {
				return err
			}
			clear(dst)
			if leaf.Equal(tree.Zero(0)) {
				return nil
			}
			block, err := s.block(gctx, b, leaf)
			if err != nil {
				return err
			}
			copy(dst, block[lo-first:hi-first])
			return nil
		})
	}
	return g.Wait()
}

// image returns the Snapshot's image if it holds this disk, or the one
// ahead.
func (s *Snapshot) image() *Image {
	r := s.img.Load()
	if r == nil {
		return nil
	}
	if root := r.m.Root(); !root.Equal(s.addr) && (r.ahead.IsZero() || !root.Equal(r.ahead)) {
		return nil
	}
	return r.m
}

// imageBlock returns block b from the image, if the Snapshot has one that
// holds this disk, or the one ahead, and the block.
func (s *Snapshot) imageBlock(b int64) ([]byte, bool) {
	if m := s.image(); m != nil {
		return m.Block(b)
	}
	return nil, false
}

// block fetches block b, whose leaf is leaf, and puts it in the image.
func (s *Snapshot) block(ctx context.Context, b int64, leaf swarm.Address) ([]byte, error) {
	ch, err := s.chunk(ctx, leaf)
	if err != nil {
		return nil, fmt.Errorf("block %d: leaf %s: %w", b, leaf, err)
	}
	block, err := leafBlock(ch)
	if err != nil {
		return nil, fmt.Errorf("block %d: %w", b, err)
	}
	if r := s.img.Load(); r != nil {
		r.m.Put(s.addr, b, block) // a block that cannot be kept is fetched again
	}
	return block, nil
}

// leafBlock returns a leaf's block: its payload, which may lack trailing
// zeros (SPEC §13), padded to 4 KiB.
func leafBlock(ch swarm.Chunk) ([]byte, error) {
	if span := binary.LittleEndian.Uint64(ch.Data()); span != swarm.ChunkSize {
		return nil, fmt.Errorf("leaf %s spans %d bytes; format 1 has only full leaves", ch.Address(), span)
	}
	block := make([]byte, swarm.ChunkSize)
	copy(block, ch.Data()[swarm.SpanSize:])
	return block, nil
}

// Extents describes the bytes from off on in the base:allocation context:
// blocks with zero leaves, in zero subtrees or not, are holes that read as
// zeros, and all others are data. An image that holds all those blocks
// answers without the tree. If a chunk cannot be fetched part way, it
// describes the bytes up to there.
func (s *Snapshot) Extents(ctx context.Context, off, length int64) ([]nbd.Extent, error) {
	if m := s.image(); m != nil {
		if ext, ok := m.Extents(off, min(length, s.g.Size-off)); ok {
			return ext, nil
		}
	}
	end := min(off+length, s.g.Size)
	var out []nbd.Extent
	for pos := off; pos < end; {
		b := pos / swarm.ChunkSize
		leaf, run, err := s.leaf(ctx, b)
		if err != nil {
			if len(out) > 0 {
				return out, nil
			}
			return nil, err
		}
		next := min((b+run)*swarm.ChunkSize, end)
		hole := leaf.Equal(tree.Zero(0))
		if k := len(out); k > 0 && out[k-1].Hole == hole {
			out[k-1].Length += next - pos
		} else {
			out = append(out, nbd.Extent{Length: next - pos, Hole: hole, Zero: hole})
		}
		pos = next
	}
	return out, nil
}

// Flush does nothing: a Snapshot holds no writes.
func (s *Snapshot) Flush(context.Context) error { return nil }

// WriteAt refuses: a Snapshot is read-only.
func (s *Snapshot) WriteAt(context.Context, []byte, int64, bool) error { return nbd.EPERM }

// Trim refuses: a Snapshot is read-only.
func (s *Snapshot) Trim(context.Context, int64, int64, bool) error { return nbd.EPERM }

// WriteZeroes refuses: a Snapshot is read-only.
func (s *Snapshot) WriteZeroes(context.Context, int64, int64, bool, bool) error { return nbd.EPERM }

var _ nbd.Device = (*Snapshot)(nil)
