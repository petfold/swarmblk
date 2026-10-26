// Package beetest is a fake Bee node for tests. It serves the parts of the
// Bee API that swarmblk uses from an in-memory chunk store, and checks what
// it receives as Bee does: content-addressed chunks against their addresses,
// single-owner chunks against their signatures. It knows one batch, and one
// tag, numbered 1.
package beetest

import (
	"context"
	"encoding/hex"
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
	"github.com/gorilla/websocket"

	"github.com/petfold/swarmblk/internal/bee"
)

// Bee is a fake Bee node.
type Bee struct {
	Batch     string // the batch it knows, in hex
	Immutable bool
	Capacity  int // bucket capacity; every bucket starts with Fill chunks
	Fill      int

	Store *inmemchunkstore.ChunkStore

	mu        sync.Mutex
	received  []swarm.Address
	streams   int
	hidden    map[string]bool
	refuseSOC bool
}

// New returns a fake Bee with an immutable batch whose buckets hold 32
// chunks each, all empty.
func New(batch string) *Bee {
	return &Bee{Batch: batch, Immutable: true, Capacity: 32, Store: inmemchunkstore.New(), hidden: map[string]bool{}}
}

// Client serves b until the test ends and returns a client for it, which
// keeps as many connections open as the engine fetches at once, so that
// parallel fetches reuse them instead of opening new ones.
func (b *Bee) Client(t testing.TB) *bee.Client {
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)
	return bee.New(srv.URL, 64)
}

// Received returns the addresses of the chunks uploaded so far, in order.
func (b *Bee) Received() []swarm.Address {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]swarm.Address(nil), b.received...)
}

// Streams returns the number of chunk streams opened.
func (b *Bee) Streams() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.streams
}

// Hide makes the chunk at addr unfindable, as one that Bee cannot retrieve
// yet, or findable again.
func (b *Bee) Hide(addr swarm.Address, hidden bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hidden[addr.ByteString()] = hidden
}

// RefuseSOC makes uploads of single-owner chunks fail, or work again.
func (b *Bee) RefuseSOC(refuse bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refuseSOC = refuse
}

func (b *Bee) get(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	b.mu.Lock()
	hidden := b.hidden[addr.ByteString()]
	b.mu.Unlock()
	if hidden {
		return nil, storage.ErrNotFound
	}
	return b.Store.Get(ctx, addr)
}

func (b *Bee) put(ctx context.Context, ch swarm.Chunk) error {
	if err := b.Store.Put(ctx, ch); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.received = append(b.received, ch.Address())
	return nil
}

func reply(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"code":%d,"message":%q}`, status, msg)
}

func (b *Bee) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	path := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	route := r.Method + " " + path[0]
	switch {
	case route == "GET stamps" && len(path) == 2 && path[1] == b.Batch:
		fmt.Fprintf(w, `{"batchID":%q,"depth":21,"bucketDepth":16,"immutableFlag":%t,"usable":true,"exists":true}`, b.Batch, b.Immutable)
	case route == "GET stamps" && len(path) == 3 && path[1] == b.Batch && path[2] == "buckets":
		list := make([]string, 1<<16)
		for i := range list {
			list[i] = fmt.Sprintf(`{"bucketID":%d,"collisions":%d}`, i, b.Fill)
		}
		fmt.Fprintf(w, `{"depth":21,"bucketDepth":16,"bucketUpperBound":%d,"buckets":[%s]}`, b.Capacity, strings.Join(list, ","))
	case route == "POST tags" && len(path) == 1:
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"uid":1}`)
	case route == "GET tags" && len(path) == 2 && path[1] == "1":
		n := len(b.Received())
		fmt.Fprintf(w, `{"uid":1,"split":%d,"seen":0,"synced":%d}`, n, n)
	case route == "GET chunks" && len(path) == 2 && path[1] == "stream":
		b.stream(w, r)
	case route == "GET chunks" && len(path) == 2:
		addr, err := swarm.ParseHexAddress(path[1])
		if err != nil {
			reply(w, http.StatusBadRequest, "invalid address")
			return
		}
		ch, err := b.get(ctx, addr)
		if err != nil {
			reply(w, http.StatusNotFound, "chunk not found")
			return
		}
		w.Write(ch.Data())
	case route == "POST soc" && len(path) == 3:
		b.postSOC(w, r, path[1], path[2])
	case route == "GET feeds" && len(path) == 3:
		owner, err1 := hex.DecodeString(path[1])
		topic, err2 := hex.DecodeString(path[2])
		if err1 != nil || err2 != nil || len(owner) != common.AddressLength {
			reply(w, http.StatusBadRequest, "invalid path params")
			return
		}
		finder := sequence.NewFinder(storage.GetterFunc(b.get), feeds.New(topic, common.BytesToAddress(owner)))
		ch, cur, _, err := finder.At(ctx, time.Now().Unix(), 0)
		if err != nil || ch == nil {
			reply(w, http.StatusNotFound, "no update found")
			return
		}
		i, _ := cur.MarshalBinary()
		w.Header().Set("Swarm-Feed-Index", hex.EncodeToString(i))
		w.Write(ch.Data())
	default:
		reply(w, http.StatusNotFound, "not found")
	}
}

// stream serves GET /chunks/stream: a WebSocket on which each binary
// message is a chunk, answered with an empty message once stored.
func (b *Bee) stream(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Swarm-Postage-Batch-Id") != b.Batch || r.Header.Get("Swarm-Tag") != "1" {
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
		if err == nil {
			err = b.put(r.Context(), ch)
		}
		if err != nil {
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "invalid chunk data"))
			return
		}
		conn.WriteMessage(websocket.BinaryMessage, nil)
	}
}

// postSOC serves POST /soc/{owner}/{id}?sig=, rebuilding and checking the
// single-owner chunk as Bee does.
func (b *Bee) postSOC(w http.ResponseWriter, r *http.Request, owner, id string) {
	if r.Header.Get("Swarm-Postage-Batch-Id") != b.Batch {
		reply(w, http.StatusBadRequest, "invalid batch id")
		return
	}
	b.mu.Lock()
	refuse := b.refuseSOC
	b.mu.Unlock()
	if refuse {
		reply(w, http.StatusInternalServerError, "chunk write error")
		return
	}
	o, err1 := hex.DecodeString(owner)
	i, err2 := hex.DecodeString(id)
	sig, err3 := hex.DecodeString(r.URL.Query().Get("sig"))
	data, err4 := io.ReadAll(r.Body)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		reply(w, http.StatusBadRequest, "invalid request")
		return
	}
	wc, err := cac.NewWithDataSpan(data)
	if err != nil {
		reply(w, http.StatusBadRequest, "chunk data error")
		return
	}
	s, err := soc.NewSigned(i, wc, o, sig)
	if err != nil {
		reply(w, http.StatusUnauthorized, "invalid address")
		return
	}
	ch, err := s.Chunk()
	if err != nil || !soc.Valid(ch) {
		reply(w, http.StatusUnauthorized, "invalid chunk")
		return
	}
	if err := b.put(r.Context(), ch); err != nil {
		reply(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, `{"reference":%q}`, ch.Address())
}
