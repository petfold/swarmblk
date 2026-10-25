//go:build integration

package lineage

// Live test against a running Bee node: $SWARMBLK_BEE_URL (default
// http://localhost:1633), stamping with the immutable batch in
// $SWARMBLK_BATCH, which must belong to that node. Run it with
//
//	go test -tags integration ./internal/lineage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
)

// TestLivePublish starts a lineage with a new key, publishes two updates,
// and reads them back through Bee: the head rule, a retried publish, and
// Bee's own resolution of GET /feeds to the snapshot named. Each run takes
// two new batch slots, for the updates; the two chunks standing in for
// snapshots are the same every run.
func TestLivePublish(t *testing.T) {
	batch := os.Getenv("SWARMBLK_BATCH")
	if batch == "" {
		t.Fatal("SWARMBLK_BATCH is not set")
	}
	url := os.Getenv("SWARMBLK_BEE_URL")
	if url == "" {
		url = "http://localhost:1633"
	}
	c := bee.New(url, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	var snaps []swarm.Chunk
	for i := range 2 {
		ch, err := cac.New([]byte(fmt.Sprintf("swarmblk lineage live test: snapshot %d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.PutChunk(ctx, batch, 0, ch); err != nil {
			t.Fatal(err)
		}
		snaps = append(snaps, ch)
	}
	l, key := testLineage(t)
	t.Logf("lineage %s", l)
	now := time.Now().UTC().Truncate(time.Second)
	u0 := Update{Index: 0, Snapshot: snaps[0].Address(), Time: now}
	u1 := Update{Index: 1, Snapshot: snaps[1].Address(), Time: now.Add(time.Second)}

	start := time.Now()
	if _, err := Head(ctx, c, l); !errors.Is(err, bee.ErrNoUpdate) {
		t.Fatalf("Head of a new lineage: %v, want ErrNoUpdate", err)
	}
	t.Logf("no update found in %v", time.Since(start).Round(time.Millisecond))

	publish := func(prev *Update, u Update) {
		t.Helper()
		start := time.Now()
		if err := Publish(ctx, c, batch, l, key, prev, u); err != nil {
			t.Fatal(err)
		}
		t.Logf("update %d published in %v", u.Index, time.Since(start).Round(time.Millisecond))
	}
	// waitHead polls until Bee's lookup finds u, which can lag.
	waitHead := func(u Update) {
		t.Helper()
		start := time.Now()
		for {
			h, err := Head(ctx, c, l)
			if err == nil && same(h, u) {
				t.Logf("head is update %d after %v", u.Index, time.Since(start).Round(time.Millisecond))
				return
			}
			if time.Since(start) > 3*time.Minute {
				t.Fatalf("head %+v, %v after %v; want update %d", h, err, time.Since(start).Round(time.Second), u.Index)
			}
			time.Sleep(2 * time.Second)
		}
	}

	publish(nil, u0)
	waitHead(u0)
	publish(&u0, u1)
	waitHead(u1)
	publish(&u0, u1) // a retry finds update 1 in place and does nothing
	other := u1
	other.Snapshot = snaps[0].Address()
	if err := Publish(ctx, c, batch, l, key, &u0, other); !errors.Is(err, ErrNotHead) {
		t.Errorf("another update 1: %v, want ErrNotHead", err)
	}

	// Bee reads the payload as a legacy one and serves the snapshot it names.
	resp, err := http.Get(fmt.Sprintf("%s/feeds/%x/%x", url, l.Owner[:], l.Topic[:]))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /feeds: %s, %v", resp.Status, err)
	}
	if v := resp.Header.Get("Swarm-Feed-Resolved-Version"); !bytes.Equal(body, snaps[1].Data()[swarm.SpanSize:]) || v != "v1" {
		t.Errorf("GET /feeds served %q as %q; want snapshot 1's chunk, %q, as v1", body, v, snaps[1].Data()[swarm.SpanSize:])
	}
}
