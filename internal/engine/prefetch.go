package engine

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/petfold/swarmblk/internal/tree"
)

// Recorder notes the blocks that a guest reads, in first-touch order: a
// boot trace (SPEC §6).
type Recorder struct {
	mu   sync.Mutex
	seen map[int64]bool
	runs []tree.Range
}

// touch notes a read of blocks first to last, in order.
func (r *Recorder) touch(first, last int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for b := first; b <= last; b++ {
		if r.seen[b] {
			continue
		}
		r.seen[b] = true
		if n := len(r.runs); n > 0 && r.runs[n-1].Start+r.runs[n-1].Count == b {
			r.runs[n-1].Count++
		} else {
			r.runs = append(r.runs, tree.Range{Start: b, Count: 1})
		}
	}
}

// Runs returns the trace so far.
func (r *Recorder) Runs() []tree.Range {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]tree.Range(nil), r.runs...)
}

// Record starts noting the blocks that reads touch, and returns the trace.
func (s *Snapshot) Record() *Recorder {
	r := &Recorder{seen: map[int64]bool{}}
	s.rec.Store(r)
	return r
}

// Prefetch follows a boot trace ahead of the guest.
type Prefetch struct {
	blocks  int64           // in the trace, within the disk, each once
	order   map[int64]int64 // a block's place in the trace
	pos     atomic.Int64    // one past the furthest place the guest has read
	moved   chan struct{}
	fetched atomic.Int64 // blocks done, whether fetched or found
	failed  atomic.Int64
	done    chan struct{}
}

// Blocks returns how many blocks the trace holds.
func (p *Prefetch) Blocks() int64 { return p.blocks }

// Fetched returns how many of the trace's blocks are in place so far, and
// how many could not be fetched.
func (p *Prefetch) Fetched() (int64, int64) { return p.fetched.Load(), p.failed.Load() }

// Done is closed when prefetching ends, at the end of the trace or of its
// context.
func (p *Prefetch) Done() <-chan struct{} { return p.done }

// read notes that the guest read block b.
func (p *Prefetch) read(b int64) {
	i, ok := p.order[b]
	if !ok {
		return
	}
	for {
		cur := p.pos.Load()
		if i < cur {
			return
		}
		if p.pos.CompareAndSwap(cur, i+1) {
			select {
			case p.moved <- struct{}{}:
			default:
			}
			return
		}
	}
}

// Prefetch fetches the leaves of the blocks in runs, a boot trace, in
// trace order and in the background, so that the guest's reads find them
// in the store underneath or in flight (DESIGN §3). It stays at most window
// blocks of the trace ahead of the furthest trace block the guest has
// read, and uses at most half of the Snapshot's fetches, leaving the rest
// to the guest. Blocks outside the disk, and repeats, are skipped. It
// returns at once; prefetching ends with the trace or with ctx.
func (s *Snapshot) Prefetch(ctx context.Context, runs []tree.Range, window int64) *Prefetch {
	p := &Prefetch{order: map[int64]int64{}, moved: make(chan struct{}, 1), done: make(chan struct{})}
	var trace []int64
	for _, r := range runs {
		for b := max(r.Start, 0); b < min(r.Start+r.Count, s.g.Leaves()); b++ {
			if _, ok := p.order[b]; !ok {
				p.order[b] = int64(len(trace))
				trace = append(trace, b)
			}
		}
	}
	p.blocks = int64(len(trace))
	s.pf.Store(p)

	work := make(chan int64)
	go func() {
		defer close(work)
		for i, b := range trace {
			for int64(i) >= p.pos.Load()+window {
				select {
				case <-p.moved:
				case <-ctx.Done():
					return
				}
			}
			select {
			case work <- b:
			case <-ctx.Done():
				return
			}
		}
	}()
	var wg sync.WaitGroup
	for range max(1, cap(s.sem)/2) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range work {
				leaf, _, err := s.leaf(ctx, b)
				if err == nil && !leaf.Equal(tree.Zero(0)) {
					_, err = s.chunk(ctx, leaf)
				}
				if err != nil {
					p.failed.Add(1)
					continue
				}
				p.fetched.Add(1)
			}
		}()
	}
	go func() {
		wg.Wait()
		close(p.done)
	}()
	return p
}
