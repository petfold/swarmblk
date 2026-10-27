//go:build js && wasm

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	"golang.org/x/sync/singleflight"

	"github.com/petfold/swarmblk/internal/engine"
	"github.com/petfold/swarmblk/internal/lineage"
	"github.com/petfold/swarmblk/internal/snapshot"
)

const (
	window    = 16384   // blocks of the boot trace to prefetch ahead of the guest, as serve's default
	ahead     = 1 << 20 // the most bytes to read ahead of sequential reads
	aheadGets = 4       // chunks that reads ahead fetch at once, leaving the rest of the browser's connections to the guest's reads
)

// disk is an open disk: a snapshot's, and for a writable disk the blocks
// written on it since, which reads take first. Reads ahead go through
// their own engine, which fetches fewer chunks at once, and the guest's
// reads find what they fetched in memory.
type disk struct {
	name     string
	writable bool
	get      *getter
	rec      *engine.Recorder // the guest's reads
	trace    int64            // blocks of the snapshot's boot trace

	wmu sync.Mutex // one write at a time; a checkpoint's cut falls between two
	seq uint64     // the last write's number, under wmu

	mu         sync.Mutex
	base       snapshot.Snapshot
	s, ra      *engine.Snapshot  // for reads, and reads ahead
	writes     map[int64]written // blocks written since base
	state      diskState         // what the browser keeps of a writable disk
	next, warm int64             // where the last read ended, and how far reads ahead reach
	streak     int               // sequential reads in a row
	cp         sync.Mutex        // one checkpoint at a time
	store      cache             // writable disks' state and logs
	log        chan logEntry     // writes, in order, for the log in store
}

// written is a block's contents after the write numbered seq.
type written struct {
	data []byte
	seq  uint64
}

type logEntry struct {
	seq uint64
	off int64
	p   []byte
}

// diskState is what the browser keeps of a writable disk beside its log:
// the snapshot under its writes, the last write that snapshot holds, and the
// lineage that its checkpoints continue, with the lineage's key.
type diskState struct {
	Snapshot swarm.Address     `json:"snapshot"`
	Cut      uint64            `json:"cut"`
	Lineage  *snapshot.Lineage `json:"lineage"` // nil until the first checkpoint starts one
	Key      string            `json:"key"`     // hex
}

var errReadOnly = errors.New("the disk is read-only")

// open opens the disk name on snapshot ref, a reference or a lineage, through
// the Bee node at beeURL, and starts prefetching from its boot trace. A
// writable disk that the browser holds goes on from its state instead, with
// the writes its log kept.
func open(ctx context.Context, beeURL, name, ref string, writable bool) (*disk, error) {
	mu.Lock()
	d, ok := disks[name]
	mu.Unlock()
	if ok {
		return d, nil
	}
	v, err, _ := opening.Do(name, func() (any, error) { return openDisk(ctx, beeURL, name, ref, writable) })
	if err != nil {
		return nil, err
	}
	return v.(*disk), nil
}

// opening lets the requests that open one disk at once share the work.
var opening singleflight.Group

// openDisk is open, for the first request.
func openDisk(ctx context.Context, beeURL, name, ref string, writable bool) (*disk, error) {
	get, err := getterFor(beeURL)
	if err != nil {
		return nil, err
	}
	d := &disk{name: name, writable: writable, get: get, writes: map[int64]written{}}
	if writable {
		if d.store, err = openCache("swarmblk-disks"); err != nil {
			return nil, fmt.Errorf("the browser's cache: %w", err)
		}
		b, _, ok, err := d.store.get(d.key("state"), "")
		if err != nil {
			return nil, err
		}
		if ok {
			if err := json.Unmarshal(b, &d.state); err != nil {
				return nil, fmt.Errorf("disk %s: %w", name, err)
			}
		}
	}
	addr := d.state.Snapshot
	if addr.IsZero() {
		if addr, err = resolve(ctx, get, ref); err != nil {
			return nil, err
		}
		d.state.Snapshot = addr
	}
	if d.base, err = snapshot.Read(ctx, get, addr); err != nil {
		return nil, err
	}
	if d.s, err = engine.Open(ctx, get, d.base.Meta.DiskRoot, engine.Fetchers); err != nil {
		return nil, err
	}
	if d.ra, err = engine.Open(ctx, get, d.base.Meta.DiskRoot, aheadGets); err != nil {
		return nil, err
	}
	d.rec = d.s.Record()
	trace, err := snapshot.ReadTrace(ctx, get, d.base)
	if err != nil {
		return nil, err
	}
	if len(trace) > 0 {
		d.trace = d.s.Prefetch(ctx, trace, window).Blocks()
	}
	if writable {
		if err := d.replay(ctx); err != nil {
			return nil, err
		}
		if err := d.saveState(); err != nil {
			return nil, err
		}
		d.log = make(chan logEntry, 1024)
		go d.keepLog(d.log)
	}
	mu.Lock()
	disks[name] = d
	mu.Unlock()
	return d, nil
}

// resolve returns the snapshot that ref names: a reference, or a lineage's
// latest update.
func resolve(ctx context.Context, get *getter, ref string) (swarm.Address, error) {
	if strings.Contains(ref, "/") {
		l, err := lineage.Parse(ref)
		if err != nil {
			return swarm.ZeroAddress, err
		}
		head, err := lineage.Head(ctx, get.bee, l)
		if err != nil {
			return swarm.ZeroAddress, fmt.Errorf("lineage %s: %w", l, err)
		}
		return head.Snapshot, nil
	}
	addr, err := swarm.ParseHexAddress(ref)
	if err != nil || len(addr.Bytes()) != swarm.HashSize {
		return swarm.ZeroAddress, fmt.Errorf("snapshot %q is neither 64 hex digits nor a lineage", ref)
	}
	return addr, nil
}

func opened(name string) (*disk, error) {
	mu.Lock()
	defer mu.Unlock()
	if d, ok := disks[name]; ok {
		return d, nil
	}
	return nil, fmt.Errorf("disk %s is not open", name)
}

// reset forgets the writable disk name: its state, its log and its writes
// since the last checkpoint. The page opens it afresh after.
func reset(name string) error {
	mu.Lock()
	d, ok := disks[name]
	delete(disks, name)
	mu.Unlock()
	if ok && d.writable {
		d.wmu.Lock() // after any write in progress
		if d.log != nil {
			close(d.log)
			d.log = nil
		}
		d.wmu.Unlock()
	}
	store, err := openCache("swarmblk-disks")
	if err != nil {
		return err
	}
	keys, err := store.keys()
	if err != nil {
		return err
	}
	prefix := "/swarmblk/disks/" + name + "/"
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			if err := store.remove(k); err != nil {
				return err
			}
		}
	}
	return nil
}

// key is where the disk keeps rel in the cache.
func (d *disk) key(rel string) string { return "/swarmblk/disks/" + d.name + "/" + rel }

func (d *disk) info() map[string]any {
	d.mu.Lock()
	defer d.mu.Unlock()
	l, index := "", uint64(0)
	if m := d.state.Lineage; m != nil {
		if ll, i, err := lineage.FromMeta(*m); err == nil {
			l, index = ll.String(), i
		}
	}
	return map[string]any{"size": d.s.Size(), "snapshot": d.base.Ref.String(), "lineage": l, "index": index, "writes": len(d.writes), "trace": d.trace}
}

// pending returns the number of blocks written since the last checkpoint.
func (d *disk) pending() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.writes)
}

// read reads n bytes at off: the snapshot's, with the blocks written since
// on top.
func (d *disk) read(ctx context.Context, off int64, n int) ([]byte, error) {
	d.mu.Lock()
	s := d.s
	if off < 0 || off+int64(n) > s.Size() {
		d.mu.Unlock()
		return nil, fmt.Errorf("a read of %d bytes at %d, past the disk's end", n, off)
	}
	first, last := off/swarm.ChunkSize, (off+int64(n)-1)/swarm.ChunkSize
	over := map[int64][]byte{}
	for b := first; b <= last; b++ {
		if w, ok := d.writes[b]; ok {
			over[b] = w.data
		}
	}
	d.mu.Unlock()
	p := make([]byte, n)
	if int64(len(over)) < last-first+1 {
		if err := s.ReadAt(ctx, p, off); err != nil {
			return nil, err
		}
		d.readAhead(off, int64(n))
	}
	for b, data := range over {
		lo, hi := max(off, b*swarm.ChunkSize), min(off+int64(n), (b+1)*swarm.ChunkSize)
		copy(p[lo-off:hi-off], data[lo-b*swarm.ChunkSize:hi-b*swarm.ChunkSize])
	}
	return p, nil
}

// readAhead reads past sequential reads in the background, so that the
// guest, which reads one piece at a time, finds the next pieces fetched.
// It starts after two sequential reads in a row with 64 KiB and doubles
// while they go on, up to ahead bytes.
func (d *disk) readAhead(off, n int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	end := off + n
	if off != d.next {
		d.next, d.warm, d.streak = end, end, 0
		return
	}
	d.next = end
	d.streak++
	if d.streak < 2 {
		return
	}
	size := min(int64(64<<10)<<min(d.streak-2, 4), ahead)
	from, to := max(d.warm, end), min(end+size, d.s.Size())
	if from >= to {
		return
	}
	d.warm = to
	go d.ra.ReadAt(context.Background(), make([]byte, to-from), from)
}

// write writes p at off. The disk holds it in memory once write returns,
// and keeps it in the log soon after, in order.
func (d *disk) write(ctx context.Context, off int64, p []byte) error {
	if !d.writable {
		return errReadOnly
	}
	d.wmu.Lock()
	defer d.wmu.Unlock()
	if d.log == nil {
		return fmt.Errorf("disk %s is closed", d.name)
	}
	seq := d.seq + 1
	if err := d.apply(ctx, seq, off, p); err != nil {
		return err
	}
	d.seq = seq
	d.log <- logEntry{seq, off, p}
	return nil
}

// apply puts write seq, p at off, into the written blocks. A write to part
// of a block takes the rest from what the guest sees of it now.
func (d *disk) apply(ctx context.Context, seq uint64, off int64, p []byte) error {
	d.mu.Lock()
	size := d.s.Size()
	d.mu.Unlock()
	if off < 0 || off+int64(len(p)) > size {
		return fmt.Errorf("a write of %d bytes at %d, past the disk's end", len(p), off)
	}
	for len(p) > 0 {
		b := off / swarm.ChunkSize
		lo := int(off % swarm.ChunkSize)
		n := min(len(p), swarm.ChunkSize-lo)
		var blk []byte
		if n == swarm.ChunkSize {
			blk = slices.Clone(p[:n])
		} else {
			old, err := d.read(ctx, b*swarm.ChunkSize, swarm.ChunkSize)
			if err != nil {
				return err
			}
			blk = old
			copy(blk[lo:], p[:n])
		}
		d.mu.Lock()
		d.writes[b] = written{blk, seq}
		d.mu.Unlock()
		off += int64(n)
		p = p[n:]
	}
	return nil
}

// keepLog keeps the disk's writes in its log, one after another, so that a
// reload that finds a write in the log finds every write before it there
// too.
func (d *disk) keepLog(log chan logEntry) {
	for e := range log {
		err := d.store.put(d.key(fmt.Sprintf("log/%d", e.seq)), e.p, map[string]any{"X-Offset": strconv.FormatInt(e.off, 10)})
		if err != nil {
			fmt.Printf("swarmblk: disk %s: keeping write %d: %v\n", d.name, e.seq, err)
		}
	}
}

// replay applies the writes in the log after the state's cut, in order, up
// to the first one missing: what the log kept before the page went.
func (d *disk) replay(ctx context.Context) error {
	keys, err := d.store.keys()
	if err != nil {
		return err
	}
	var seqs []uint64
	for _, k := range keys {
		if s, ok := strings.CutPrefix(k, d.key("log/")); ok {
			if n, err := strconv.ParseUint(s, 10, 64); err == nil && n > d.state.Cut {
				seqs = append(seqs, n)
			}
		}
	}
	slices.Sort(seqs)
	d.seq = d.state.Cut
	for _, n := range seqs {
		if n != d.seq+1 {
			break
		}
		p, off, ok, err := d.store.get(d.key(fmt.Sprintf("log/%d", n)), "X-Offset")
		if err != nil || !ok {
			break
		}
		o, err := strconv.ParseInt(off, 10, 64)
		if err != nil {
			break
		}
		if err := d.apply(ctx, n, o, p); err != nil {
			return err
		}
		d.seq = n
	}
	return nil
}

func (d *disk) saveState() error {
	d.mu.Lock()
	b, err := json.Marshal(d.state)
	d.mu.Unlock()
	if err != nil {
		return err
	}
	return d.store.put(d.key("state"), b, nil)
}

// dropLog removes the log's entries up to write cut, which a snapshot holds.
func (d *disk) dropLog(cut uint64) {
	keys, err := d.store.keys()
	if err != nil {
		return
	}
	for _, k := range keys {
		if s, ok := strings.CutPrefix(k, d.key("log/")); ok {
			if n, err := strconv.ParseUint(s, 10, 64); err == nil && n <= cut {
				d.store.remove(k)
			}
		}
	}
}
