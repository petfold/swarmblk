package engine

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/epoch"
	"github.com/petfold/swarmblk/internal/nbd"
)

// Disk is a writable block device: a snapshot's disk with a write epoch on
// top (DESIGN §4). Reads look in the epoch, then in the snapshot. Writes
// go to the epoch and never wait for Swarm: a write that covers part of a
// block merges with the block's contents if they are at hand, and is
// recorded as a patch otherwise (SPEC §4). A flush, or a write with FUA,
// returns once the epoch is durable.
type Disk struct {
	snap  *Snapshot
	ep    *epoch.Epoch
	locks [64]sync.Mutex // serialize the partial writes to a block, by block number
}

// NewDisk puts the epoch ep on top of snap, which must be its parent.
func NewDisk(snap *Snapshot, ep *epoch.Epoch) (*Disk, error) {
	if s := ep.Super(); s.Size != snap.Size() {
		return nil, fmt.Errorf("an epoch for a disk of %d bytes on a snapshot of %d", s.Size, snap.Size())
	}
	return &Disk{snap: snap, ep: ep}, nil
}

// Size returns the disk's size in bytes.
func (d *Disk) Size() int64 { return d.snap.Size() }

var zeros = make([]byte, swarm.ChunkSize)

// block returns block b's contents from the epoch, if it holds the block,
// reading the snapshot underneath when the epoch holds only patches.
func (d *Disk) block(ctx context.Context, b int64) ([]byte, bool, error) {
	blk, ok := d.ep.Get(b)
	if !ok {
		return nil, false, nil
	}
	var base []byte
	if !blk.Whole {
		base = make([]byte, swarm.ChunkSize)
		if err := d.snap.ReadAt(ctx, base, b*swarm.ChunkSize); err != nil {
			return nil, true, err
		}
	}
	data, err := d.ep.Read(blk, base)
	return data, true, err
}

// ReadAt fills p with the disk's bytes from off on. Runs of blocks that the
// epoch does not hold are read from the snapshot in one request, so that
// they are fetched in parallel.
func (d *Disk) ReadAt(ctx context.Context, p []byte, off int64) error {
	end := off + int64(len(p))
	if off < 0 || end > d.Size() {
		return nbd.EINVAL
	}
	run := off // the start of the bytes not yet read, all outside the epoch
	flush := func(to int64) error {
		if to <= run {
			return nil
		}
		return d.snap.ReadAt(ctx, p[run-off:to-off], run)
	}
	for first := off - off%swarm.ChunkSize; first < end; first += swarm.ChunkSize {
		data, ok, err := d.block(ctx, first/swarm.ChunkSize)
		if err != nil {
			return err
		}
		if !ok {
			continue
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

// write writes p at off, or zeros if p is nil, over length bytes.
func (d *Disk) write(ctx context.Context, p []byte, off, length int64, fua bool) error {
	end := off + length
	if off < 0 || end > d.Size() {
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
			err = d.patch(ctx, b, int(lo-first), part)
		case bytes.Equal(part, zeros):
			err = d.ep.Zero(b)
		default:
			err = d.ep.Put(b, part)
		}
		if err != nil {
			return err
		}
	}
	if fua {
		return d.ep.Sync()
	}
	return nil
}

// patch writes data at pos within block b. If the block's contents are at
// hand, in the epoch or locally under it, it records the merged block;
// otherwise it records a patch, to be applied when the block is read.
func (d *Disk) patch(ctx context.Context, b int64, pos int, data []byte) error {
	mu := &d.locks[b%int64(len(d.locks))]
	mu.Lock()
	defer mu.Unlock()
	blk, ok := d.ep.Get(b)
	var cur []byte
	var err error
	switch {
	case ok && blk.Whole:
		cur, err = d.ep.Read(blk, nil)
	default:
		if under, local := d.snap.LocalBlock(ctx, b); local {
			cur, err = d.ep.Read(blk, under) // blk is empty if !ok
		}
	}
	if err != nil {
		return err
	}
	if cur == nil {
		return d.ep.Patch(b, pos, data)
	}
	copy(cur[pos:], data)
	if bytes.Equal(cur, zeros) {
		return d.ep.Zero(b)
	}
	return d.ep.Put(b, cur)
}

// Flush makes every write so far durable.
func (d *Disk) Flush(context.Context) error { return d.ep.Sync() }

// Trim discards length bytes from off, which then read as zeros: whole
// blocks become zero blocks, which cost nothing in a snapshot (DESIGN §4).
func (d *Disk) Trim(ctx context.Context, off, length int64, fua bool) error {
	return d.write(ctx, nil, off, length, fua)
}

// WriteZeroes makes length bytes from off read as zeros, as Trim does.
func (d *Disk) WriteZeroes(ctx context.Context, off, length int64, fua, _ bool) error {
	return d.write(ctx, nil, off, length, fua)
}

// Extents describes the bytes from off on: zero blocks in the epoch are
// holes, other blocks it holds are data, and the rest is as the snapshot
// describes it.
func (d *Disk) Extents(ctx context.Context, off, length int64) ([]nbd.Extent, error) {
	end := min(off+length, d.Size())
	var out []nbd.Extent
	add := func(e nbd.Extent) {
		if k := len(out); k > 0 && out[k-1].Hole == e.Hole && out[k-1].Zero == e.Zero {
			out[k-1].Length += e.Length
		} else {
			out = append(out, e)
		}
	}
	for pos := off; pos < end; {
		// The run of blocks from pos that the epoch does not hold.
		next := pos
		for next < end {
			if _, ok := d.ep.Get(next / swarm.ChunkSize); ok {
				break
			}
			next = min(next-next%swarm.ChunkSize+swarm.ChunkSize, end)
		}
		if next > pos {
			ext, err := d.snap.Extents(ctx, pos, next-pos)
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
		blk, _ := d.ep.Get(pos / swarm.ChunkSize)
		hole := blk.Whole && blk.Zero && len(blk.Patches) == 0
		n := min(pos-pos%swarm.ChunkSize+swarm.ChunkSize, end) - pos
		add(nbd.Extent{Length: n, Hole: hole, Zero: hole})
		pos += n
	}
	return out, nil
}

var _ nbd.Device = (*Disk)(nil)
