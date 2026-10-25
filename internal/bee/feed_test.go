package bee

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/soc"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// testSOC returns a single-owner chunk wrapping payload, signed with a new key.
func testSOC(t *testing.T, payload string) (swarm.Chunk, *soc.SOC) {
	t.Helper()
	key, err := crypto.GenerateSecp256k1Key()
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := cac.New([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	ch, err := soc.New(make([]byte, swarm.HashSize), wrapped).Sign(crypto.NewDefaultSigner(key))
	if err != nil {
		t.Fatal(err)
	}
	s, err := soc.FromChunk(ch)
	if err != nil {
		t.Fatal(err)
	}
	return ch, s
}

func TestSOC(t *testing.T) {
	ch, _ := testSOC(t, "swarmblk")
	c, seen := fakeBee(http.StatusOK, ch.Data())
	got, err := c.SOC(context.Background(), ch.Address())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Data(), ch.Data()) || (*seen)[0].URL.Path != "/chunks/"+ch.Address().String() {
		t.Errorf("SOC returned %x from %s", got.Data(), (*seen)[0].URL.Path)
	}

	flipped := slices.Clone(ch.Data())
	flipped[len(flipped)-1] ^= 1
	other, _ := testSOC(t, "swarmblk")
	plain, err := cac.New([]byte("swarmblk"))
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{"tampered": flipped, "another owner's": other.Data(), "content-addressed": plain.Data(), "long": append(slices.Clone(ch.Data()), make([]byte, swarm.ChunkSize)...)} {
		c, _ := fakeBee(http.StatusOK, body)
		if _, err := c.SOC(context.Background(), ch.Address()); !errors.Is(err, ErrInvalidChunk) {
			t.Errorf("%s chunk: error %v, want ErrInvalidChunk", name, err)
		}
	}
	if err := (&Client{}).PutSOC(context.Background(), testBatch, plain); err == nil {
		t.Error("PutSOC accepted a content-addressed chunk")
	}
}

func TestPutSOC(t *testing.T) {
	ch, s := testSOC(t, "swarmblk")
	status, reference := http.StatusCreated, ch.Address().String()
	var got *http.Request
	var body []byte
	c := handlerClient(func(w http.ResponseWriter, r *http.Request) {
		got = r
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"reference":%q,"message":"invalid chunk","code":%d}`, reference, status)
	})
	ctx := context.Background()

	if err := c.PutSOC(ctx, testBatch, ch); err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("/soc/%x/%x", s.OwnerAddress(), s.ID()); got.Method != http.MethodPost || got.URL.Path != want {
		t.Errorf("request %s %s, want POST %s", got.Method, got.URL.Path, want)
	}
	if sig := got.URL.Query().Get("sig"); sig != hex.EncodeToString(s.Signature()) {
		t.Errorf("sig=%s, want %x", sig, s.Signature())
	}
	if !slices.Equal(body, s.WrappedChunk().Data()) {
		t.Errorf("body %x, want the wrapped chunk %x", body, s.WrappedChunk().Data())
	}
	// No tag: Bee pushes the chunk at once and answers after the receipt.
	if b, d, tag := got.Header.Get("Swarm-Postage-Batch-Id"), got.Header.Get("Swarm-Deferred-Upload"), got.Header.Get("Swarm-Tag"); b != testBatch || d != "false" || tag != "" {
		t.Errorf("batch %q, deferred %q, tag %q", b, d, tag)
	}

	status = http.StatusUnauthorized
	if err := c.PutSOC(ctx, testBatch, ch); err == nil || !strings.Contains(err.Error(), "invalid chunk") {
		t.Errorf("401: %v, want Bee's message", err)
	}
	status, reference = http.StatusCreated, strings.Repeat("ab", 32)
	if err := c.PutSOC(ctx, testBatch, ch); err == nil {
		t.Error("PutSOC accepted a response naming another chunk")
	}
}

func TestFeedIndex(t *testing.T) {
	owner := common.HexToAddress("0x" + strings.Repeat("12", 20))
	topic := make([]byte, swarm.HashSize)
	topic[0] = 0xab
	status, index, message := http.StatusOK, "000000000000002a", ""
	var got *http.Request
	c := handlerClient(func(w http.ResponseWriter, r *http.Request) {
		got = r
		w.Header().Set("Swarm-Feed-Index", index)
		w.WriteHeader(status)
		if message != "" {
			fmt.Fprintf(w, `{"code":%d,"message":%q}`, status, message)
		}
	})
	ctx := context.Background()

	i, err := c.FeedIndex(ctx, owner, topic)
	if err != nil || i != 42 {
		t.Fatalf("FeedIndex = %d, %v; want 42", i, err)
	}
	if want := fmt.Sprintf("/feeds/%x/%x", owner[:], topic); got.Method != http.MethodGet || got.URL.Path != want || got.Header.Get("Swarm-Only-Root-Chunk") != "true" {
		t.Errorf("request %s %s, Swarm-Only-Root-Chunk %q; want GET %s", got.Method, got.URL.Path, got.Header.Get("Swarm-Only-Root-Chunk"), want)
	}
	for _, bad := range []string{"", "2a", "xyz", "00000000000000002a"} {
		index = bad
		if _, err := c.FeedIndex(ctx, owner, topic); err == nil {
			t.Errorf("Swarm-Feed-Index %q accepted", bad)
		}
	}

	status = http.StatusNotFound
	for msg, want := range map[string]bool{"no update found": true, "lookup at failed": false, "wrapped chunk cannot be retrieved": false} {
		message = msg
		if _, err := c.FeedIndex(ctx, owner, topic); err == nil || errors.Is(err, ErrNoUpdate) != want {
			t.Errorf("404 %q: %v; ErrNoUpdate should match: %v", msg, err, want)
		}
	}
}
