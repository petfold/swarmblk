//go:build integration

package bee

// Live tests against a running Bee node: $SWARMBLK_BEE_URL (default
// http://localhost:1633), uploading with the immutable batch in
// $SWARMBLK_BATCH, which must belong to that node. Run them with
//
//	go test -tags integration ./internal/bee

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

func liveClient(t *testing.T) (*Client, string) {
	t.Helper()
	batch := os.Getenv("SWARMBLK_BATCH")
	if batch == "" {
		t.Fatal("SWARMBLK_BATCH is not set")
	}
	url := os.Getenv("SWARMBLK_BEE_URL")
	if url == "" {
		url = "http://localhost:1633"
	}
	return New(url, 16), batch
}

// TestLiveUpload streams chunks under a tag, reads them back, uploads one
// more without a tag, and reads the batch's state. The chunks are the same
// every run, so reruns take no new batch slots.
func TestLiveUpload(t *testing.T) {
	ctx := context.Background()
	c, batch := liveClient(t)
	var chunks []swarm.Chunk
	for i := range 51 {
		ch, err := cac.New(bytes.Repeat([]byte(fmt.Sprintf("swarmblk live chunk %3d ", i)), 160))
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, ch)
	}

	tag, err := c.CreateTag(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s, err := c.Stream(ctx, batch, tag.UID, 16)
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range chunks[:50] {
		if err := s.Put(ch); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.Acked() != 50 {
		t.Errorf("%d chunks acknowledged, want 50", s.Acked())
	}
	// Every acknowledged chunk is counted as split, and is pushed later; the
	// tag is done when synced reaches split minus seen (SPEC §8, step 6).
	wctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	start := time.Now()
	if tag, err = c.WaitSynced(wctx, tag.UID, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if tag.Split != 50 {
		t.Errorf("tag counted %d chunks as split, want 50", tag.Split)
	}
	t.Logf("tag %d synced after %v: %+v", tag.UID, time.Since(start).Round(time.Second), tag)

	for _, ch := range chunks[:50] {
		got, err := c.Chunk(ctx, ch.Address())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Data(), ch.Data()) {
			t.Fatalf("chunk %s read back different", ch.Address())
		}
	}
	if err := c.PutChunk(ctx, batch, 0, chunks[50]); err != nil {
		t.Fatal(err)
	}

	b, err := c.Batch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if !b.Usable || !b.Immutable || b.TTL <= 0 {
		t.Errorf("batch %+v", b)
	}
	bk, err := c.Buckets(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	var used, fullest uint32
	for _, n := range bk.Fill {
		used += n
		fullest = max(fullest, n)
	}
	if bk.Capacity != 1<<(b.Depth-b.BucketDepth) || fullest != b.Utilization || used < 51 {
		t.Errorf("buckets: capacity %d at depth %d, fullest %d against utilization %d, %d used", bk.Capacity, b.Depth, fullest, b.Utilization, used)
	}
	t.Logf("batch: depth %d, %d chunks stamped, fullest bucket %d of %d, TTL %d s", b.Depth, used, fullest, bk.Capacity, b.TTL)
}
