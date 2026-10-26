package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/tree"
)

// Hydration fills a Snapshot's image in the background.
type Hydration struct {
	img     *Image
	fetched atomic.Int64 // blocks fetched and put in the image
	failed  atomic.Int64 // blocks that could not be fetched
	done    chan struct{}
	err     error
}

// Done is closed when the hydration ends: at the end of the disk, or of
// its context.
func (h *Hydration) Done() <-chan struct{} { return h.done }

// Err returns why the hydration ended early, once Done is closed: nil if it
// went through the whole disk, even if some blocks could not be fetched.
func (h *Hydration) Err() error { return h.err }

// Progress returns how many blocks this hydration fetched, how many it
// could not, and how many of the disk's blocks the image holds.
func (h *Hydration) Progress() (fetched, failed, present, blocks int64) {
	present, blocks = h.img.Present()
	return h.fetched.Load(), h.failed.Load(), present, blocks
}

// hydrateSync is how often a hydration syncs the image.
const hydrateSync = 2 * time.Second

// Hydrate fills the image the Snapshot holds this disk in (DESIGN §3): it
// walks the disk in block order, marks zero subtrees without a fetch, and
// fetches the other blocks the image lacks with get, workers at a time. get
// should go to the network without keeping the chunks, which the image
// keeps instead. The guest's reads go on meanwhile, and put what they fetch
// in the image first. Hydrate returns at once.
func (s *Snapshot) Hydrate(ctx context.Context, get storage.Getter, workers int) (*Hydration, error) {
	r := s.img.Load()
	if r == nil || !r.m.Root().Equal(s.addr) {
		return nil, errors.New("the snapshot has no image of its disk")
	}
	h := &Hydration{img: r.m, done: make(chan struct{})}
	type job struct {
		first, count int64
		leaf         swarm.Address
	}
	jobs := make(chan job, 4*workers)
	var wg sync.WaitGroup
	for range max(1, workers) {
		wg.Go(func() {
			for j := range jobs {
				ch, err := get.Get(ctx, j.leaf)
				var block []byte
				if err == nil {
					block, err = leafBlock(ch)
				}
				if err != nil {
					h.failed.Add(j.count)
					continue
				}
				for b := j.first; b < j.first+j.count; b++ {
					if err := h.img.Put(s.addr, b, block); err != nil {
						h.failed.Add(1)
						continue
					}
					h.fetched.Add(1)
				}
			}
		})
	}
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(hydrateSync)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				h.img.Sync()
			}
		}
	}()
	go func() {
		_, err := tree.Walk(ctx, s.get, s.addr, func(first, count int64, leaf swarm.Address) error {
			if leaf.Equal(tree.Zero(0)) {
				return h.img.PutZeros(s.addr, first, count)
			}
			for b := first; b < first+count; {
				if h.img.Has(b) {
					b++
					continue
				}
				e := b + 1
				for e < first+count && !h.img.Has(e) {
					e++
				}
				select {
				case jobs <- job{b, e - b, leaf}:
				case <-ctx.Done():
					return ctx.Err()
				}
				b = e
			}
			return nil
		})
		close(jobs)
		wg.Wait()
		close(stop)
		if serr := h.img.Sync(); err == nil {
			err = serr
		}
		h.err = err
		close(h.done)
	}()
	return h, nil
}
