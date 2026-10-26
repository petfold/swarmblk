package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/epoch"
	"github.com/petfold/swarmblk/internal/nbd"
)

// Disk is a writable block device: a snapshot's disk with write epochs on
// top (DESIGN §4). The active epoch takes the writes; while a checkpoint
// runs, the epoch it freezes stays under the active one until the new
// snapshot replaces both. Reads look in the active epoch, then in the
// frozen ones, newest first, then in the snapshot. Writes never wait for
// Swarm: a write that covers part of a block merges with the block's
// contents if they are at hand, and is recorded as a patch otherwise
// (SPEC §4). A flush, or a write with FUA, returns once the active epoch is
// durable.
type Disk struct {
	cut    sync.RWMutex // writes hold it shared; Freeze holds it alone
	mu     sync.Mutex   // serializes changes of layers
	swap   sync.RWMutex // held shared to start using the layers, alone to replace them
	layers *layers
	locks  [64]sync.Mutex // serialize the partial writes to a block, by block number

	// An epoch that Promote replaces is closed once nothing uses the layers
	// that held it: those replaced since the last Promote, and the older
	// ones, which the closing before waits for.
	old     []*layers
	retired chan struct{} // closed when the last Promote's epoch is
}

// layers is what a Disk reads: a snapshot, the frozen epochs on it (oldest
// first) and the active epoch on top. A read or write keeps the layers it
// started with, even if they are replaced while it runs.
type layers struct {
	snap   *Snapshot
	frozen []*epoch.Epoch
	active *epoch.Epoch

	uses sync.WaitGroup // the reads and writes that use these layers
}

// down returns the epochs from the top down.
func (l *layers) down() []*epoch.Epoch {
	eps := []*epoch.Epoch{l.active}
	for i := len(l.frozen) - 1; i >= 0; i-- {
		eps = append(eps, l.frozen[i])
	}
	return eps
}

// NewDisk puts epochs on top of snap, which must be the parent of the
// first. The last epoch is the active one, and the others are frozen,
// oldest first.
func NewDisk(snap *Snapshot, epochs ...*epoch.Epoch) (*Disk, error) {
	if len(epochs) == 0 {
		return nil, errors.New("a disk needs an epoch")
	}
	for _, ep := range epochs {
		if s := ep.Super(); s.Size != snap.Size() {
			return nil, fmt.Errorf("an epoch for a disk of %d bytes on a snapshot of %d", s.Size, snap.Size())
		}
	}
	n := len(epochs) - 1
	return &Disk{layers: &layers{snap: snap, frozen: slices.Clone(epochs[:n]), active: epochs[n]}}, nil
}

// current returns the disk's layers.
func (d *Disk) current() *layers {
	d.swap.RLock()
	defer d.swap.RUnlock()
	return d.layers
}

// use returns the disk's layers for a read or write, which must call
// l.uses.Done when it ends.
func (d *Disk) use() *layers {
	d.swap.RLock()
	defer d.swap.RUnlock()
	d.layers.uses.Add(1)
	return d.layers
}

// replace puts l in place of the disk's layers. d.mu must be held.
func (d *Disk) replace(l *layers) {
	d.swap.Lock()
	defer d.swap.Unlock()
	d.old = append(d.old, d.layers)
	d.layers = l
}

// Size returns the disk's size in bytes.
func (d *Disk) Size() int64 { return d.current().snap.Size() }

// Snapshot returns the snapshot under the disk's epochs.
func (d *Disk) Snapshot() *Snapshot { return d.current().snap }

// Active returns the epoch that takes the writes.
func (d *Disk) Active() *epoch.Epoch { return d.current().active }

// Frozen returns the frozen epochs, oldest first.
func (d *Disk) Frozen() []*epoch.Epoch { return slices.Clone(d.current().frozen) }

var zeros = make([]byte, swarm.ChunkSize)

// contents returns block b as the epochs eps, from the top down, and the
// snapshot under them hold it, and whether it could tell. With local, it
// gives up rather than fetch from the network.
func (l *layers) contents(ctx context.Context, b int64, eps []*epoch.Epoch, local bool) ([]byte, bool, error) {
	for i, ep := range eps {
		blk, ok := ep.Get(b)
		if !ok {
			continue
		}
		var base []byte
		if !blk.Whole {
			var found bool
			var err error
			if base, found, err = l.contents(ctx, b, eps[i+1:], local); err != nil || !found {
				return nil, found, err
			}
		}
		data, err := ep.Read(blk, base)
		return data, err == nil, err
	}
	if local {
		data, ok := l.snap.LocalBlock(ctx, b)
		return data, ok, nil
	}
	data := make([]byte, swarm.ChunkSize)
	err := l.snap.ReadAt(ctx, data, b*swarm.ChunkSize)
	return data, err == nil, err
}

// held reports whether any epoch holds block b, and if the topmost one that
// does holds a plain zero block.
func (l *layers) held(b int64) (held, zero bool) {
	for _, ep := range l.down() {
		if blk, ok := ep.Get(b); ok {
			return true, blk.Whole && blk.Zero && len(blk.Patches) == 0
		}
	}
	return false, false
}

// ReadAt fills p with the disk's bytes from off on. Runs of blocks that no
// epoch holds are read from the snapshot in one request, so that they are
// fetched in parallel.
func (d *Disk) ReadAt(ctx context.Context, p []byte, off int64) error {
	l := d.use()
	defer l.uses.Done()
	end := off + int64(len(p))
	if off < 0 || end > l.snap.Size() {
		return nbd.EINVAL
	}
	run := off // the start of the bytes not yet read, all in no epoch
	flush := func(to int64) error {
		if to <= run {
			return nil
		}
		return l.snap.ReadAt(ctx, p[run-off:to-off], run)
	}
	eps := l.down()
	for first := off - off%swarm.ChunkSize; first < end; first += swarm.ChunkSize {
		b := first / swarm.ChunkSize
		if held, _ := l.held(b); !held {
			continue
		}
		data, _, err := l.contents(ctx, b, eps, false)
		if err != nil {
			return err
		}
		lo, hi := max(first, off), min(first+swarm.ChunkSize, end)
		if err := flush(lo); err != nil {
			return err
		}
		copy(p[lo-off:hi-off], data[lo-first:hi-first])
		run = hi
	}
	return flush(end)
}

// WriteAt writes p at off: whole blocks as they are, or as zero blocks if
// they hold only zeros, and parts of blocks through patch.
func (d *Disk) WriteAt(ctx context.Context, p []byte, off int64, fua bool) error {
	return d.write(ctx, p, off, int64(len(p)), fua)
}

// write writes p at off, or zeros if p is nil, over length bytes, to the
// active epoch.
func (d *Disk) write(ctx context.Context, p []byte, off, length int64, fua bool) error {
	d.cut.RLock()
	defer d.cut.RUnlock()
	l := d.use()
	defer l.uses.Done()
	end := off + length
	if off < 0 || end > l.snap.Size() {
		return nbd.EINVAL
	}
	for first := off - off%swarm.ChunkSize; first < end; first += swarm.ChunkSize {
		b := first / swarm.ChunkSize
		lo, hi := max(first, off), min(first+swarm.ChunkSize, end)
		part := zeros[:hi-lo]
		if p != nil {
			part = p[lo-off : hi-off]
		}
		var err error
		switch {
		case hi-lo < swarm.ChunkSize:
			err = d.patch(ctx, l, b, int(lo-first), part)
		case bytes.Equal(part, zeros):
			err = l.active.Zero(b)
		default:
			err = l.active.Put(b, part)
		}
		if err != nil {
			return err
		}
	}
	if fua {
		return l.active.Sync()
	}
	return nil
}

// patch writes data at pos within block b. If the block's contents are at
// hand, it records the merged block; otherwise it records a patch, to be
// applied when the block is read.
func (d *Disk) patch(ctx context.Context, l *layers, b int64, pos int, data []byte) error {
	mu := &d.locks[b%int64(len(d.locks))]
	mu.Lock()
	defer mu.Unlock()
	cur, ok, err := l.contents(ctx, b, l.down(), true)
	switch {
	case err != nil:
		return err
	case !ok:
		return l.active.Patch(b, pos, data)
	}
	copy(cur[pos:], data)
	if bytes.Equal(cur, zeros) {
		return l.active.Zero(b)
	}
	return l.active.Put(b, cur)
}

// Flush makes every write so far durable: those before the last Freeze
// are.
func (d *Disk) Flush(context.Context) error {
	l := d.use()
	defer l.uses.Done()
	return l.active.Sync()
}

// Trim discards length bytes from off, which then read as zeros: whole
// blocks become zero blocks, which cost nothing in a snapshot (DESIGN §4).
func (d *Disk) Trim(ctx context.Context, off, length int64, fua bool) error {
	return d.write(ctx, nil, off, length, fua)
}

// WriteZeroes makes length bytes from off read as zeros, as Trim does.
func (d *Disk) WriteZeroes(ctx context.Context, off, length int64, fua, _ bool) error {
	return d.write(ctx, nil, off, length, fua)
}

// Extents describes the bytes from off on: zero blocks in the epochs are
// holes, other blocks they hold are data, and the rest is as the snapshot
// describes it.
func (d *Disk) Extents(ctx context.Context, off, length int64) ([]nbd.Extent, error) {
	l := d.use()
	defer l.uses.Done()
	end := min(off+length, l.snap.Size())
	var out []nbd.Extent
	add := func(e nbd.Extent) {
		if k := len(out); k > 0 && out[k-1].Hole == e.Hole && out[k-1].Zero == e.Zero {
			out[k-1].Length += e.Length
		} else {
			out = append(out, e)
		}
	}
	for pos := off; pos < end; {
		// The run of blocks from pos that no epoch holds.
		next := pos
		for next < end {
			if held, _ := l.held(next / swarm.ChunkSize); held {
				break
			}
			next = min(next-next%swarm.ChunkSize+swarm.ChunkSize, end)
		}
		if next > pos {
			ext, err := l.snap.Extents(ctx, pos, next-pos)
			if err != nil {
				if len(out) > 0 {
					return out, nil
				}
				return nil, err
			}
			for _, e := range ext {
				add(e)
				pos += e.Length
			}
			if pos < next { // the snapshot described only part of the run
				return out, nil
			}
			continue
		}
		_, hole := l.held(pos / swarm.ChunkSize)
		n := min(pos-pos%swarm.ChunkSize+swarm.ChunkSize, end) - pos
		add(nbd.Extent{Length: n, Hole: hole, Zero: hole})
		pos += n
	}
	return out, nil
}

// Freeze cuts the disk's writes atomically (SPEC §8, step 1): every write
// that has returned is in, and none still running is half in. It waits for
// the running writes, holding off new ones, syncs the active epoch and
// marks it frozen, and puts next on top as the new active epoch; then the
// writes go on. Reads never wait. It returns the frozen epoch. Only one
// epoch is frozen at a time: until Promote replaces it, Freeze refuses.
func (d *Disk) Freeze(next *epoch.Epoch) (*epoch.Epoch, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cut.Lock()
	defer d.cut.Unlock()
	l := d.current()
	if len(l.frozen) > 0 {
		return nil, errors.New("an epoch is frozen already; its checkpoint must finish first")
	}
	if s := next.Super(); s.Size != l.snap.Size() {
		return nil, fmt.Errorf("an epoch for a disk of %d bytes on one of %d", s.Size, l.snap.Size())
	}
	if err := l.active.Sync(); err != nil {
		return nil, err
	}
	if err := l.active.SetState(epoch.Frozen); err != nil {
		return nil, err
	}
	d.replace(&layers{snap: l.snap, frozen: []*epoch.Epoch{l.active}, active: next})
	return l.active, nil
}

// Blocks passes the blocks of the frozen epoch ep to fn, in order and at
// most window at a time, each with its contents as the disk held them at
// the cut, or nil for zeros: patches are applied to what lies under them,
// which may need a fetch (SPEC §8, step 2).
func (d *Disk) Blocks(ctx context.Context, ep *epoch.Epoch, window int, fn func(map[int64][]byte) error) error {
	l := d.use()
	defer l.uses.Done()
	i := slices.Index(l.frozen, ep)
	if i < 0 {
		return errors.New("the epoch is not frozen in this disk")
	}
	eps := []*epoch.Epoch{ep}
	for j := i - 1; j >= 0; j-- {
		eps = append(eps, l.frozen[j])
	}
	batch := map[int64][]byte{}
	for _, b := range ep.Blocks() {
		data, _, err := l.contents(ctx, b, eps, false)
		if err != nil {
			return fmt.Errorf("block %d: %w", b, err)
		}
		if bytes.Equal(data, zeros) {
			data = nil
		}
		batch[b] = data
		if len(batch) == window {
			if err := fn(batch); err != nil {
				return err
			}
			batch = map[int64][]byte{}
		}
	}
	if len(batch) == 0 {
		return nil
	}
	return fn(batch)
}

// Promote puts the disk on snap, the snapshot of the frozen epoch ep and
// what lay under it (SPEC §8, step 8): the active epoch now sits on snap,
// and ep leaves the disk. Reads and writes that started before may still
// use ep, so it is closed once they end.
func (d *Disk) Promote(snap *Snapshot, ep *epoch.Epoch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	l := d.current()
	if len(l.frozen) == 0 || l.frozen[0] != ep {
		return errors.New("the epoch is not the oldest frozen one")
	}
	if snap.Size() != l.snap.Size() {
		return fmt.Errorf("a snapshot of %d bytes for a disk of %d", snap.Size(), l.snap.Size())
	}
	d.replace(&layers{snap: snap, frozen: slices.Clone(l.frozen[1:]), active: l.active})
	old, before, done := d.old, d.retired, make(chan struct{})
	d.old, d.retired = nil, done
	go func() {
		defer close(done)
		if before != nil {
			<-before
		}
		for _, l := range old {
			l.uses.Wait()
		}
		ep.Close() // its blocks are in snap
	}()
	return nil
}

// Close syncs and closes every epoch of the disk, once nothing uses those
// that Promote replaced.
func (d *Disk) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.retired != nil {
		<-d.retired
	}
	l := d.current()
	var err error
	for _, ep := range append(slices.Clone(l.frozen), l.active) {
		if cerr := ep.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

var _ nbd.Device = (*Disk)(nil)
