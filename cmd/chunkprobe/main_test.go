package main

import (
	"bytes"
	"context"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/tree"
)

// testImage returns a disk image of size bytes, 64 MiB or more, that is zero
// apart from a random 1 MiB region at 4 MiB and one random block repeated at
// blocks 100 and 9000. Its tree has 257 distinct leaves other than Z_0 and
// four distinct level-1 chunks other than Z_1, all in the first 64 MiB.
func testImage(size int) []byte {
	img := make([]byte, size)
	src := rand.NewChaCha8([32]byte{1})
	src.Read(img[4<<20 : 5<<20])
	src.Read(img[100*swarm.ChunkSize : 101*swarm.ChunkSize])
	copy(img[9000*swarm.ChunkSize:], img[100*swarm.ChunkSize:101*swarm.ChunkSize])
	return img
}

// testLeaves returns the distinct leaves of img other than Z_0.
func testLeaves(t *testing.T, img []byte) []swarm.Address {
	t.Helper()
	s := set{}
	zero := make([]byte, swarm.ChunkSize)
	for b := 0; b < len(img); b += swarm.ChunkSize {
		if bytes.Equal(img[b:b+swarm.ChunkSize], zero) {
			continue
		}
		ch, err := cac.New(img[b : b+swarm.ChunkSize])
		if err != nil {
			t.Fatal(err)
		}
		s[[swarm.HashSize]byte(ch.Address().Bytes())] = struct{}{}
	}
	return s.list()
}

// handlerTransport answers requests with a handler in the same process, so
// the tests need no network.
type handlerTransport struct{ http.Handler }

func (h handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec.Result(), nil
}

// build stores img as the tree that Bee's pipeline makes for POST /bytes at
// redundancy level 0, and returns its root.
func build(img []byte) (*inmemchunkstore.ChunkStore, swarm.Address, error) {
	ctx := context.Background()
	store := inmemchunkstore.New()
	root, err := builder.FeedPipeline(ctx, builder.NewPipelineBuilder(ctx, store, false, redundancy.NONE), bytes.NewReader(img))
	return store, root, err
}

// builtImage is a disk image with the tree that Bee's pipeline built for it.
type builtImage struct {
	img   []byte
	store *inmemchunkstore.ChunkStore
	root  swarm.Address
}

// testTree is testImage(128 MiB), built once for all the tests. Its tree has
// three intermediate levels; the root holds one level-2 chunk and Z_2.
var testTree = sync.OnceValue(func() builtImage {
	img := testImage(128 << 20)
	store, root, err := build(img)
	if err != nil {
		panic(err)
	}
	return builtImage{img, store, root}
})

// fakeBee serves GET /chunks/{address} from testTree, and counts the
// requests for each address and their Swarm-Cache headers.
type fakeBee struct {
	mu    sync.Mutex
	gets  map[string]int
	cache map[string]int
}

func newFakeBee() *fakeBee {
	return &fakeBee{gets: map[string]int{}, cache: map[string]int{}}
}

func (b *fakeBee) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	addr, err := swarm.ParseHexAddress(strings.TrimPrefix(r.URL.Path, "/chunks/"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	b.mu.Lock()
	b.gets[addr.String()]++
	b.cache[r.Header.Get("Swarm-Cache")]++
	b.mu.Unlock()
	ch, err := testTree().store.Get(r.Context(), addr)
	if err != nil {
		http.Error(w, `{"code":404,"message":"chunk not found"}`, http.StatusNotFound)
		return
	}
	w.Write(ch.Data())
}

func (b *fakeBee) client() *bee.Client {
	return &bee.Client{URL: "http://bee", HTTP: &http.Client{Transport: handlerTransport{b}}}
}

func TestWalk(t *testing.T) {
	b := newFakeBee()
	w, err := walk(context.Background(), b.client(), testTree().root, 8)
	if err != nil {
		t.Fatal(err)
	}
	if w.geo.Size != 128<<20 || w.geo.Levels != 3 {
		t.Errorf("geometry %+v", w.geo)
	}
	// Fetched: the root, one level-2 chunk and four level-1 chunks. Not
	// fetched: Z_2 in the root and 124 Z_1 in the level-2 chunk.
	if w.fetched != 6 || w.zeros != 125 {
		t.Errorf("fetched %d and skipped %d zero subtrees, want 6 and 125", w.fetched, w.zeros)
	}
	if want := testLeaves(t, testTree().img); !slices.EqualFunc(w.leaves, want, swarm.Address.Equal) {
		t.Errorf("walk found %d leaves, want the %d distinct non-zero blocks", len(w.leaves), len(want))
	}
	if len(b.gets) != w.fetched {
		t.Errorf("Bee got requests for %d addresses, want %d", len(b.gets), w.fetched)
	}
	for addr, n := range b.gets {
		if n != 1 {
			t.Errorf("%s fetched %d times", addr, n)
		}
	}
	for j := range 3 {
		if b.gets[tree.Zero(j).String()] != 0 {
			t.Errorf("Z_%d was fetched", j)
		}
	}
}

func TestProbe(t *testing.T) {
	b := newFakeBee()
	c := b.client()
	c.NoCache = true
	w, err := walk(context.Background(), c, testTree().root, 8)
	if err != nil {
		t.Fatal(err)
	}
	missing := swarm.MustParseHexAddress(strings.Repeat("ab", 32))
	leaves := append(pick(w.leaves, 0, 300), missing)

	res := probe(context.Background(), c, leaves, 4)
	if len(res.lat) != 300 || res.failed != 1 || !strings.Contains(res.err.Error(), "404") {
		t.Errorf("%d fetches and %d failures (%v), want 300 and the missing chunk's 404", len(res.lat), res.failed, res.err)
	}
	if b.cache["false"] != w.fetched+301 || len(b.cache) != 1 {
		t.Errorf("Swarm-Cache headers %v, want false on every request", b.cache)
	}
	if row := res.row(4); !strings.HasPrefix(row, "| 4 | ") || !strings.HasSuffix(row, " | 1 |") {
		t.Errorf("row %q", row)
	}
}

func TestRowPercentiles(t *testing.T) {
	r := result{took: time.Second}
	for i := 100; i >= 1; i-- {
		r.lat = append(r.lat, time.Duration(i)*time.Millisecond)
	}
	if got, want := r.row(8), "| 8 | 50.0 | 90.0 | 99.0 | 100.0 | 100 | 0 |"; got != want {
		t.Errorf("row = %q, want %q", got, want)
	}
	if got, want := (result{failed: 3}).row(1), "| 1 | – | – | – | – | 0 | 3 |"; got != want {
		t.Errorf("row = %q, want %q", got, want)
	}
}

func TestPickWraps(t *testing.T) {
	a1, a2, a3 := swarm.NewAddress([]byte{1}), swarm.NewAddress([]byte{2}), swarm.NewAddress([]byte{3})
	if got := pick([]swarm.Address{a1, a2, a3}, 2, 4); !slices.EqualFunc(got, []swarm.Address{a3, a1, a2, a3}, swarm.Address.Equal) {
		t.Errorf("pick = %v", got)
	}
}

func TestParseLevels(t *testing.T) {
	if got, err := parseLevels("1,8, 32"); err != nil || !slices.Equal(got, []int{1, 8, 32}) {
		t.Errorf("parseLevels = %v, %v", got, err)
	}
	for _, bad := range []string{"", "0", "8,x", "-1"} {
		if _, err := parseLevels(bad); err == nil {
			t.Errorf("parseLevels(%q) succeeded", bad)
		}
	}
}
