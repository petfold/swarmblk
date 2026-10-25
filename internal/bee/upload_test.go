package bee

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/gorilla/websocket"
)

const testBatch = "c7af5ddacf81eaa13304ea78b497c8d719b2f5c0bf53b8ab01654cb4c65edecb"

// handlerClient returns a client whose requests h answers in this process.
func handlerClient(h http.HandlerFunc) *Client {
	return &Client{URL: "http://bee", HTTP: &http.Client{Transport: handlerTransport{h}}}
}

func testChunks(t *testing.T, n int) []swarm.Chunk {
	t.Helper()
	var chunks []swarm.Chunk
	for i := range n {
		ch, err := cac.New([]byte(fmt.Sprintf("swarmblk chunk %d", i)))
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, ch)
	}
	return chunks
}

func TestPutChunk(t *testing.T) {
	ch := testChunks(t, 1)[0]
	status, reference := http.StatusCreated, ch.Address().String()
	var got *http.Request
	var body []byte
	c := handlerClient(func(w http.ResponseWriter, r *http.Request) {
		got = r
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"reference":%q,"message":"batch is overissued","code":%d}`, reference, status)
	})
	ctx := context.Background()

	if err := c.PutChunk(ctx, testBatch, 7, ch); err != nil {
		t.Fatal(err)
	}
	if got.Method != http.MethodPost || got.URL.Path != "/chunks" || got.Header.Get("Swarm-Postage-Batch-Id") != testBatch ||
		got.Header.Get("Swarm-Tag") != "7" || !slices.Equal(body, ch.Data()) {
		t.Errorf("request %s %s, batch %q, tag %q, %d bytes", got.Method, got.URL.Path, got.Header.Get("Swarm-Postage-Batch-Id"), got.Header.Get("Swarm-Tag"), len(body))
	}
	if err := c.PutChunk(ctx, testBatch, 0, ch); err != nil || got.Header.Get("Swarm-Tag") != "" {
		t.Errorf("without a tag: %v, Swarm-Tag %q", err, got.Header.Get("Swarm-Tag"))
	}

	status = http.StatusPaymentRequired
	if err := c.PutChunk(ctx, testBatch, 0, ch); !errors.Is(err, ErrOverissued) {
		t.Errorf("402: %v, want ErrOverissued", err)
	}
	status, reference = http.StatusCreated, strings.Repeat("ab", 32)
	if err := c.PutChunk(ctx, testBatch, 0, ch); err == nil {
		t.Error("PutChunk accepted a response naming another chunk")
	}
}

func TestTagsAndBatches(t *testing.T) {
	c := handlerClient(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /tags":
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"uid":7,"split":0,"startedAt":"2026-10-26T10:00:00Z"}`)
		case "GET /tags/7":
			fmt.Fprint(w, `{"uid":7,"split":10,"seen":2,"stored":0,"sent":8,"synced":8}`)
		case "GET /stamps/" + testBatch:
			fmt.Fprintf(w, `{"batchID":%q,"label":"swarmblk-live-tests","depth":21,"bucketDepth":16,"immutableFlag":true,"usable":true,"exists":true,"utilization":19,"batchTTL":3600}`, testBatch)
		case "GET /stamps/" + testBatch + "/buckets":
			fmt.Fprint(w, `{"depth":21,"bucketDepth":16,"bucketUpperBound":32,"buckets":[{"bucketID":0,"collisions":3},{"bucketID":65535,"collisions":19}]}`)
		default:
			http.Error(w, `{"code":404,"message":"not found"}`, http.StatusNotFound)
		}
	})
	ctx := context.Background()

	if tag, err := c.CreateTag(ctx); err != nil || tag.UID != 7 {
		t.Errorf("CreateTag: %+v, %v", tag, err)
	}
	if tag, err := c.Tag(ctx, 7); err != nil || tag.Split != 10 || tag.Seen != 2 || tag.Synced != 8 {
		t.Errorf("Tag: %+v, %v", tag, err)
	}
	if b, err := c.Batch(ctx, testBatch); err != nil || b.Depth != 21 || !b.Immutable || !b.Usable || b.Utilization != 19 || b.TTL != 3600 {
		t.Errorf("Batch: %+v, %v", b, err)
	}
	b, err := c.Buckets(ctx, testBatch)
	if err != nil || b.Capacity != 32 || len(b.Fill) != 1<<16 || b.Fill[0] != 3 || b.Fill[65535] != 19 || b.Fill[1] != 0 {
		t.Errorf("Buckets: depth %d, capacity %d, %d buckets, %v", b.Depth, b.Capacity, len(b.Fill), err)
	}
	var e *Error
	if _, err := c.Tag(ctx, 8); !errors.As(err, &e) || e.Status != http.StatusNotFound || e.Message != "not found" {
		t.Errorf("missing tag: %v", err)
	}
}

// streamBee serves /chunks/stream: it checks each message is a chunk,
// acknowledges the first ack of them, then closes with reason.
type streamBee struct {
	ack    int
	reason string
	mu     sync.Mutex
	got    []swarm.Address
	header http.Header
}

func (b *streamBee) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	b.header = r.Header.Clone()
	b.mu.Unlock()
	conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
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
		n := len(b.got)
		if n < b.ack {
			b.got = append(b.got, ch.Address())
		}
		b.mu.Unlock()
		if n >= b.ack {
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, b.reason))
			return
		}
		conn.WriteMessage(websocket.BinaryMessage, nil)
	}
}

func TestStream(t *testing.T) {
	chunks := testChunks(t, 20)
	bee := &streamBee{ack: len(chunks)}
	srv := httptest.NewServer(bee)
	defer srv.Close()
	c := New(srv.URL, 1)

	s, err := c.Stream(context.Background(), testBatch, 9, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range chunks {
		if err := s.Put(ch); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.Acked() != int64(len(chunks)) {
		t.Errorf("%d acknowledged, want %d", s.Acked(), len(chunks))
	}
	bee.mu.Lock()
	defer bee.mu.Unlock()
	if bee.header.Get("Swarm-Postage-Batch-Id") != testBatch || bee.header.Get("Swarm-Tag") != "9" {
		t.Errorf("headers %v", bee.header)
	}
	for i, ch := range chunks {
		if !bee.got[i].Equal(ch.Address()) {
			t.Fatalf("chunk %d arrived out of order", i)
		}
	}
}

func TestStreamOverissued(t *testing.T) {
	chunks := testChunks(t, 10)
	srv := httptest.NewServer(&streamBee{ack: 3, reason: "batch is overissued"})
	defer srv.Close()

	s, err := New(srv.URL, 1).Stream(context.Background(), testBatch, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	var putErr error
	for _, ch := range chunks {
		if putErr = s.Put(ch); putErr != nil {
			break
		}
	}
	if err := s.Close(); !errors.Is(err, ErrOverissued) || putErr != nil && !errors.Is(putErr, ErrOverissued) {
		t.Errorf("Close: %v; Put: %v; want ErrOverissued", err, putErr)
	}
	if s.Acked() != 3 {
		t.Errorf("%d acknowledged, want 3", s.Acked())
	}
}

func TestWaitSynced(t *testing.T) {
	polls, stuck := 0, false
	c := handlerClient(func(w http.ResponseWriter, r *http.Request) {
		polls++
		synced := min(4*(polls-1), 8) // 0, 4, 8: done at 8, since 2 of the 10 were already present
		if stuck {
			synced = 4
		}
		fmt.Fprintf(w, `{"uid":7,"split":10,"seen":2,"synced":%d}`, synced)
	})
	tag, err := c.WaitSynced(context.Background(), 7, time.Millisecond)
	if err != nil || !tag.Done() || polls != 3 {
		t.Errorf("WaitSynced: %+v after %d polls, %v", tag, polls, err)
	}

	stuck = true
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.WaitSynced(ctx, 7, time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("WaitSynced on a stuck tag: %v, want context.DeadlineExceeded", err)
	}
}
