package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/gorilla/websocket"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/store"
)

const testBatch = "c7af5ddacf81eaa13304ea78b497c8d719b2f5c0bf53b8ab01654cb4c65edecb"

// fakeBee serves what import uses: a batch, its buckets, one tag and the
// chunk stream, which checks and records every chunk it gets.
type fakeBee struct {
	immutable bool
	capacity  int // bucket capacity; every bucket starts with fill chunks
	fill      int

	mu      sync.Mutex
	got     []swarm.Address
	streams int
}

func (b *fakeBee) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method + " " + r.URL.Path {
	case "GET /stamps/" + testBatch:
		fmt.Fprintf(w, `{"batchID":%q,"depth":21,"bucketDepth":16,"immutableFlag":%t,"usable":true,"exists":true}`, testBatch, b.immutable)
	case "GET /stamps/" + testBatch + "/buckets":
		var list []string
		for i := range 1 << 16 {
			list = append(list, fmt.Sprintf(`{"bucketID":%d,"collisions":%d}`, i, b.fill))
		}
		fmt.Fprintf(w, `{"depth":21,"bucketDepth":16,"bucketUpperBound":%d,"buckets":[%s]}`, b.capacity, strings.Join(list, ","))
	case "POST /tags":
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"uid":1}`)
	case "GET /tags/1":
		b.mu.Lock()
		n := len(b.got)
		b.mu.Unlock()
		fmt.Fprintf(w, `{"uid":1,"split":%d,"seen":0,"synced":%d}`, n, n)
	case "GET /chunks/stream":
		b.stream(w, r)
	default:
		http.Error(w, `{"code":404,"message":"not found"}`, http.StatusNotFound)
	}
}

func (b *fakeBee) stream(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Swarm-Postage-Batch-Id") != testBatch || r.Header.Get("Swarm-Tag") != "1" {
		http.Error(w, "wrong headers", http.StatusBadRequest)
		return
	}
	conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	b.mu.Lock()
	b.streams++
	b.mu.Unlock()
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		ch, err := cac.NewWithDataSpan(msg)
		if err != nil {
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "invalid chunk data"))
			return
		}
		b.mu.Lock()
		b.got = append(b.got, ch.Address())
		b.mu.Unlock()
		conn.WriteMessage(websocket.BinaryMessage, nil)
	}
}

// testImage writes a 64 MiB disk image of mostly zeros, with random data in
// its first 2 MiB and one random block near the end, and returns its path.
func testImage(t *testing.T) (string, []byte) {
	t.Helper()
	img := make([]byte, 64<<20)
	src := rand.NewChaCha8([32]byte{5})
	src.Read(img[:2<<20])
	src.Read(img[60<<20 : 60<<20+swarm.ChunkSize])
	path := filepath.Join(t.TempDir(), "disk.raw")
	if err := os.WriteFile(path, img, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, img
}

// startImport runs importImage against bee, with a fresh store.
func startImport(t *testing.T, b *fakeBee, path string) (swarm.Address, *store.Store, error) {
	t.Helper()
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)
	s, err := store.Open(filepath.Join(t.TempDir(), "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := importImage(context.Background(), bee.New(srv.URL, 4), s, testBatch, path, io.Discard)
	return root, s, err
}

func TestImport(t *testing.T) {
	path, img := testImage(t)
	ctx := context.Background()
	distinct := map[string]bool{}
	want, err := builder.FeedPipeline(ctx, builder.NewPipelineBuilder(ctx, putterFunc(func(_ context.Context, ch swarm.Chunk) error {
		distinct[ch.Address().ByteString()] = true
		return nil
	}), false, redundancy.NONE), bytes.NewReader(img))
	if err != nil {
		t.Fatal(err)
	}

	b := &fakeBee{immutable: true, capacity: 32}
	root, s, err := startImport(t, b, path)
	if err != nil {
		t.Fatal(err)
	}
	if !root.Equal(want) {
		t.Errorf("import gave root %s; Bee's pipeline builds %s", root, want)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.got) != len(distinct) || !b.got[len(b.got)-1].Equal(root) {
		t.Errorf("Bee got %d chunks, want the %d distinct ones with the root last", len(b.got), len(distinct))
	}
	sent := map[string]bool{}
	for _, a := range b.got {
		if !distinct[a.ByteString()] || sent[a.ByteString()] {
			t.Fatalf("chunk %s sent twice or not part of the tree", a)
		}
		sent[a.ByteString()] = true
		if _, err := s.Get(ctx, a); err != nil {
			t.Fatalf("the local store lacks chunk %s: %v", a, err)
		}
	}
}

func TestImportRefuses(t *testing.T) {
	path, _ := testImage(t)
	odd := filepath.Join(t.TempDir(), "odd.raw")
	if err := os.WriteFile(odd, make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		bee  *fakeBee
		path string
		want string
	}{
		{"mutable batch", &fakeBee{immutable: false, capacity: 32}, path, "mutable"},
		{"full buckets", &fakeBee{immutable: true, capacity: 4, fill: 4}, path, "not enough room"},
		{"odd size", &fakeBee{immutable: true, capacity: 32}, odd, "outside"},
	} {
		_, _, err := startImport(t, c.bee, c.path)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error about %q", c.name, err, c.want)
		}
		if c.bee.streams != 0 {
			t.Errorf("%s: %d streams opened before refusing", c.name, c.bee.streams)
		}
		if c.name == "full buckets" && !errors.Is(err, errNoRoom) {
			t.Errorf("full buckets: %v, want errNoRoom", err)
		}
	}
}
