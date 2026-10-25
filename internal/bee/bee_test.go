package bee

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// handlerTransport answers requests with a handler in the same process, so
// the tests need no network.
type handlerTransport struct{ http.Handler }

func (h handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec.Result(), nil
}

// fakeBee answers every request with status and body, and keeps the requests.
func fakeBee(status int, body []byte) (*Client, *[]*http.Request) {
	var seen []*http.Request
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r)
		w.WriteHeader(status)
		w.Write(body)
	})
	return &Client{URL: "http://bee", HTTP: &http.Client{Transport: handlerTransport{h}}}, &seen
}

func TestChunk(t *testing.T) {
	ch, err := cac.New([]byte("swarmblk"))
	if err != nil {
		t.Fatal(err)
	}
	c, seen := fakeBee(http.StatusOK, ch.Data())
	for _, nocache := range []bool{false, true} {
		c.NoCache = nocache
		got, err := c.Chunk(context.Background(), ch.Address())
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got.Data(), ch.Data()) {
			t.Errorf("Chunk returned %x, want %x", got.Data(), ch.Data())
		}
		r := (*seen)[len(*seen)-1]
		if r.Method != http.MethodGet || r.URL.Path != "/chunks/"+ch.Address().String() {
			t.Errorf("request %s %s", r.Method, r.URL.Path)
		}
		var want []string
		if nocache {
			want = []string{"false"}
		}
		if v := r.Header.Values("Swarm-Cache"); !slices.Equal(v, want) {
			t.Errorf("NoCache %v: Swarm-Cache %q, want %q", nocache, v, want)
		}
	}
}

func TestChunkRejectsWrongData(t *testing.T) {
	ch, err := cac.New([]byte("swarmblk"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := cac.New([]byte("something else"))
	if err != nil {
		t.Fatal(err)
	}
	flipped := slices.Clone(ch.Data())
	flipped[swarm.SpanSize] ^= 1
	// Trailing zeros past the span would still hash to ch's address, as in Bee.
	for _, body := range [][]byte{other.Data(), flipped, make([]byte, swarm.ChunkWithSpanSize+1), ch.Data()[:4]} {
		c, _ := fakeBee(http.StatusOK, body)
		if _, err := c.Chunk(context.Background(), ch.Address()); !errors.Is(err, ErrInvalidChunk) {
			t.Errorf("body %x: error %v, want ErrInvalidChunk", body, err)
		}
	}
}

func TestChunkReportsStatus(t *testing.T) {
	c, _ := fakeBee(http.StatusNotFound, []byte(`{"code":404,"message":"chunk not found"}`))
	_, err := c.Chunk(context.Background(), swarm.MustParseHexAddress(strings.Repeat("ab", 32)))
	if err == nil || !strings.Contains(err.Error(), "404") || !strings.Contains(err.Error(), "chunk not found") {
		t.Errorf("error %v, want one with the status and Bee's message", err)
	}
}
