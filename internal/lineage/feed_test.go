package lineage

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/feeds"
	"github.com/ethersphere/bee/v2/pkg/feeds/sequence"
	"github.com/ethersphere/bee/v2/pkg/soc"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
)

const testBatch = "c7af5ddacf81eaa13304ea78b497c8d719b2f5c0bf53b8ab01654cb4c65edecb"

// fakeBee serves feeds from an in-memory store as Bee does: GET /feeds finds
// the head with Bee's sequence finder, GET /chunks returns stored chunks, and
// POST /soc rebuilds and checks a single-owner chunk before storing it.
// Chunks in hidden are not found, like updates Bee cannot retrieve yet.
type fakeBee struct {
	store *inmemchunkstore.ChunkStore

	mu     sync.Mutex
	hidden map[string]bool
	posts  int
}

func newFakeBee(t *testing.T) (*fakeBee, *bee.Client) {
	b := &fakeBee{store: inmemchunkstore.New(), hidden: map[string]bool{}}
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)
	return b, bee.New(srv.URL, 2)
}

func (b *fakeBee) get(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	b.mu.Lock()
	hidden := b.hidden[addr.ByteString()]
	b.mu.Unlock()
	if hidden {
		return nil, storage.ErrNotFound
	}
	return b.store.Get(ctx, addr)
}

func (b *fakeBee) hide(t *testing.T, l Lineage, i uint64, hidden bool) {
	t.Helper()
	addr, err := l.Address(i)
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hidden[addr.ByteString()] = hidden
}

func notFound(w http.ResponseWriter, msg string) {
	w.WriteHeader(http.StatusNotFound)
	fmt.Fprintf(w, `{"code":404,"message":%q}`, msg)
}

func (b *fakeBee) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	switch {
	case r.Method == http.MethodGet && len(parts) == 3 && parts[0] == "feeds":
		owner, _ := hex.DecodeString(parts[1])
		topic, _ := hex.DecodeString(parts[2])
		finder := sequence.NewFinder(storage.GetterFunc(b.get), feeds.New(topic, common.BytesToAddress(owner)))
		ch, cur, _, err := finder.At(ctx, time.Now().Unix(), 0)
		if err != nil || ch == nil {
			notFound(w, "no update found")
			return
		}
		i, _ := cur.MarshalBinary()
		w.Header().Set("Swarm-Feed-Index", hex.EncodeToString(i))
		w.Write(ch.Data())
	case r.Method == http.MethodGet && len(parts) == 2 && parts[0] == "chunks":
		ch, err := b.get(ctx, swarm.MustParseHexAddress(parts[1]))
		if err != nil {
			notFound(w, "chunk not found")
			return
		}
		w.Write(ch.Data())
	case r.Method == http.MethodPost && len(parts) == 3 && parts[0] == "soc":
		if r.Header.Get("Swarm-Postage-Batch-Id") != testBatch {
			http.Error(w, "no batch", http.StatusBadRequest)
			return
		}
		owner, _ := hex.DecodeString(parts[1])
		id, _ := hex.DecodeString(parts[2])
		sig, _ := hex.DecodeString(r.URL.Query().Get("sig"))
		data, _ := io.ReadAll(r.Body)
		wc, err := cac.NewWithDataSpan(data)
		if err != nil {
			http.Error(w, "chunk data error", http.StatusBadRequest)
			return
		}
		s, err := soc.NewSigned(id, wc, owner, sig)
		if err != nil {
			http.Error(w, "invalid address", http.StatusUnauthorized)
			return
		}
		ch, err := s.Chunk()
		if err != nil || !soc.Valid(ch) {
			http.Error(w, "invalid chunk", http.StatusUnauthorized)
			return
		}
		b.store.Put(ctx, ch)
		b.mu.Lock()
		b.posts++
		b.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"reference":%q}`, ch.Address())
	default:
		notFound(w, "not found")
	}
}

func TestPublish(t *testing.T) {
	ctx := context.Background()
	b, c := newFakeBee(t)
	l, key := testLineage(t)
	u0, u1, u2 := testUpdate(0), testUpdate(1), testUpdate(2)
	posts := func(want int) {
		t.Helper()
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.posts != want {
			t.Errorf("%d updates posted, want %d", b.posts, want)
		}
	}
	head := func(want Update) {
		t.Helper()
		if got, err := Head(ctx, c, l); err != nil || !same(got, want) {
			t.Errorf("Head = %+v, %v; want %+v", got, err, want)
		}
	}

	if _, err := Head(ctx, c, l); !errors.Is(err, bee.ErrNoUpdate) {
		t.Errorf("Head of a new lineage: %v, want ErrNoUpdate", err)
	}
	if err := Publish(ctx, c, testBatch, l, key, &u0, u1); !errors.Is(err, ErrNotHead) {
		t.Errorf("update 1 to a lineage without updates: %v, want ErrNotHead", err)
	}
	if err := Publish(ctx, c, testBatch, l, key, nil, u0); err != nil {
		t.Fatal(err)
	}
	head(u0)
	// A retry after an update arrived does nothing.
	if err := Publish(ctx, c, testBatch, l, key, nil, u0); err != nil {
		t.Errorf("publishing update 0 again: %v", err)
	}
	posts(1)
	if err := Publish(ctx, c, testBatch, l, key, &u0, u1); err != nil {
		t.Fatal(err)
	}
	head(u1)
	posts(2)

	other := testUpdate(9)
	other.Index = 1
	for name, prev := range map[string]*Update{"no update": nil, "update 0": &u0} {
		next := other
		if prev == nil {
			next.Index = 0
		}
		if err := Publish(ctx, c, testBatch, l, key, prev, next); !errors.Is(err, ErrNotHead) {
			t.Errorf("after %s, while the head is update 1: %v, want ErrNotHead", name, err)
		}
	}
	posts(2)

	// Bee cannot find update 1 yet: the writer must wait.
	b.hide(t, l, 1, true)
	if err := Publish(ctx, c, testBatch, l, key, &u1, u2); !errors.Is(err, ErrNotHead) || !strings.Contains(err.Error(), "finds update 0") {
		t.Errorf("with update 1 out of sight: %v, want ErrNotHead naming update 0", err)
	}
	b.hide(t, l, 1, false)
	if err := Publish(ctx, c, testBatch, l, key, &u1, u2); err != nil {
		t.Fatal(err)
	}
	head(u2)
	posts(3)

	_, otherKey := testLineage(t)
	u3 := testUpdate(3)
	if err := Publish(ctx, c, testBatch, l, otherKey, &u2, u3); err == nil {
		t.Error("Publish signed with another lineage's key")
	}
	if err := Publish(ctx, c, testBatch, l, key, &u1, u3); err == nil || errors.Is(err, ErrNotHead) {
		t.Errorf("update 3 after update 1: %v, want a refusal before any request", err)
	}
	posts(3)
}
