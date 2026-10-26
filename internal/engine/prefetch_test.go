package engine

import (
	"bytes"
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/tree"
)

func TestRecord(t *testing.T) {
	_, snap, _ := open(t)
	rec := snap.Record()
	ctx := context.Background()
	for _, r := range []struct{ off, n int64 }{
		{1 << 20, 64 << 10}, // blocks 256 to 271
		{0, 4096},           // block 0
		{258 << 12, 4096},   // block 258 again
		{512, 8192},         // blocks 0 to 2, of which 1 and 2 are new
		{272 << 12, 4096},   // block 272, after 271 but not right after it
	} {
		if err := snap.ReadAt(ctx, make([]byte, r.n), r.off); err != nil {
			t.Fatal(err)
		}
	}
	want := []tree.Range{{Start: 256, Count: 16}, {Start: 0, Count: 3}, {Start: 272, Count: 1}}
	if got := rec.Runs(); !reflect.DeepEqual(got, want) {
		t.Errorf("trace %v, want %v", got, want)
	}
}

// network stands for Bee: each get takes a while, and it notes how many are
// in flight at most and how often each chunk is fetched.
type network struct {
	storage.Getter
	delay time.Duration

	mu        sync.Mutex
	now, most int
	gets      map[string]int
	total     int
}

func (n *network) Get(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	n.mu.Lock()
	n.now++
	n.most = max(n.most, n.now)
	n.gets[addr.ByteString()]++
	n.total++
	n.mu.Unlock()
	time.Sleep(n.delay)
	n.mu.Lock()
	n.now--
	n.mu.Unlock()
	return n.Getter.Get(ctx, addr)
}

func (n *network) stats() (most, total int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.most, n.total
}

// cached keeps what it gets from the network, as the local store does.
type cached struct {
	net   storage.Getter
	local *inmemchunkstore.ChunkStore
}

func (c cached) Get(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	if ch, err := c.local.Get(ctx, addr); err == nil {
		return ch, nil
	}
	ch, err := c.net.Get(ctx, addr)
	if err == nil {
		err = c.local.Put(ctx, ch)
	}
	return ch, err
}

// waitFor polls until cond holds, for up to ten seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("waited 10 s for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPrefetch(t *testing.T) {
	img, root, s := testDisk(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &network{Getter: s, delay: 10 * time.Millisecond, gets: map[string]int{}}
	snap, err := Open(ctx, cached{n, inmemchunkstore.New()}, root, 8)
	if err != nil {
		t.Fatal(err)
	}
	// 64 blocks of the random region, a zero block, a repeat, and blocks
	// past the end of the disk.
	runs := []tree.Range{{Start: 1024, Count: 40}, {Start: 9000, Count: 1}, {Start: 1030, Count: 2}, {Start: 1064, Count: 24}, {Start: snap.g.Leaves() - 1, Count: 5}}
	p := snap.Prefetch(ctx, runs, 16)
	if p.Blocks() != 40+1+24+1 {
		t.Fatalf("the trace holds %d blocks, want 66", p.Blocks())
	}

	// Ahead of a guest that has read nothing, it fetches the window's 16
	// blocks and waits.
	waitFor(t, "16 blocks", func() bool { done, _ := p.Fetched(); return done == 16 })
	time.Sleep(100 * time.Millisecond)
	if done, failed := p.Fetched(); done != 16 || failed != 0 {
		t.Fatalf("%d blocks fetched and %d failed beyond the window", done, failed)
	}
	if most, _ := n.stats(); most > 4 {
		t.Errorf("%d fetches in flight; prefetching may use half of 8", most)
	}

	// The guest reads the trace's 11th block: the window moves.
	read := func(b int64) {
		t.Helper()
		got := make([]byte, swarm.ChunkSize)
		if err := snap.ReadAt(ctx, got, b*swarm.ChunkSize); err != nil || !bytes.Equal(got, img[b*swarm.ChunkSize:(b+1)*swarm.ChunkSize]) {
			t.Fatalf("block %d: %v", b, err)
		}
	}
	read(1034)
	waitFor(t, "27 blocks", func() bool { done, _ := p.Fetched(); return done == 27 })

	// Reading the last block lets it finish; the guest then finds every
	// block in place, and no chunk was fetched twice.
	read(1087)
	select {
	case <-p.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("prefetching did not finish")
	}
	if done, failed := p.Fetched(); done != p.Blocks() || failed != 0 {
		t.Errorf("%d of %d blocks fetched, %d failed", done, p.Blocks(), failed)
	}
	_, before := n.stats()
	for _, r := range runs[:4] {
		for b := r.Start; b < r.Start+r.Count; b++ {
			read(b)
		}
	}
	if _, after := n.stats(); after != before {
		t.Errorf("the guest fetched %d chunks after prefetching", after-before)
	}
	for k, c := range n.gets {
		if c > 1 {
			t.Errorf("chunk %x fetched %d times", k, c)
		}
	}
}
