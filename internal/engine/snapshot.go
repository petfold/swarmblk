// Package engine serves swarmblk disks as block devices (DESIGN §3, §8). So
// far it has the read path: a snapshot's disk, read from its chunk tree.
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
	root   []swarm.Address
	get    storage.Getter
	sem    chan struct{}
	leaves singleflight.Group
	tree   singleflight.Group

	mu    sync.RWMutex
	nodes map[string][]swarm.Address // intermediate chunks' references, by address

	rec atomic.Pointer[Recorder] // notes reads for a boot trace, if set
	pf  atomic.Pointer[Prefetch] // follows a boot trace ahead of the guest, if set
}

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
	return &Snapshot{g: g, root: refs, get: get, sem: make(chan struct{}, fetchers), nodes: map[string][]swarm.Address{}}, nil
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
			leaf, _, err := s.leaf(gctx, b)
			if err != nil {
				return err
			}
			clear(dst)
			if leaf.Equal(tree.Zero(0)) {
				return nil
			}
			ch, err := s.chunk(gctx, leaf)
			if err != nil {
				return fmt.Errorf("block %d: leaf %s: %w", b, leaf, err)
			}
			if span := binary.LittleEndian.Uint64(ch.Data()); span != swarm.ChunkSize {
				return fmt.Errorf("block %d: leaf %s spans %d bytes; format 1 has only full leaves", b, leaf, span)
			}
			// A payload may lack trailing zeros (SPEC §13).
			if payload := ch.Data()[swarm.SpanSize:]; lo-first < int64(len(payload)) {
				copy(dst, payload[lo-first:min(hi-first, int64(len(payload)))])
			}
			return nil
		})
	}
	return g.Wait()
}

// Extents describes the bytes from off on in the base:allocation context:
// blocks with zero leaves, in zero subtrees or not, are holes that read as
// zeros, and all others are data. If a chunk cannot be fetched part way, it
// describes the bytes up to there.
func (s *Snapshot) Extents(ctx context.Context, off, length int64) ([]nbd.Extent, error) {
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
