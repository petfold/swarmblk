//go:build integration

package main

// Live tests against a running Bee node: $SWARMBLK_BEE_URL (default
// http://localhost:1633), uploading with the immutable batch in
// $SWARMBLK_BATCH, which must belong to that node. Run them with
//
//	go test -tags integration ./cmd/chunkprobe

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
)

// upload stores data with POST /bytes at redundancy level 0, as PHASE0.md §3
// does, and returns its root.
func upload(t *testing.T, beeURL string, data []byte) swarm.Address {
	t.Helper()
	batch := os.Getenv("SWARMBLK_BATCH")
	if batch == "" {
		t.Fatal("SWARMBLK_BATCH is not set")
	}
	req, err := http.NewRequest(http.MethodPost, beeURL+"/bytes", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Swarm-Postage-Batch-Id", batch)
	req.Header.Set("Swarm-Redundancy-Level", "0")
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var r struct{ Reference swarm.Address }
	if resp.StatusCode != http.StatusCreated || json.Unmarshal(body, &r) != nil {
		t.Fatalf("upload: %s: %s", resp.Status, body)
	}
	return r.Reference
}

// TestLiveProbe uploads a 64 MiB image, checks that Bee's root is the one its
// pipeline builds here, then walks the tree and probes leaves on the node.
func TestLiveProbe(t *testing.T) {
	ctx := context.Background()
	img := testImage(64 << 20)
	_, want, err := build(img)
	if err != nil {
		t.Fatal(err)
	}
	c := bee.New(envOr("SWARMBLK_BEE_URL", "http://localhost:1633"), 8)
	c.NoCache = true
	root := upload(t, c.URL, img)
	if !root.Equal(want) {
		t.Fatalf("Bee's root is %s, but its pipeline builds %s at redundancy 0", root, want)
	}

	w, err := walk(ctx, c, root, 8)
	if err != nil {
		t.Fatal(err)
	}
	// The root, four level-1 chunks and 124 references to Z_1 (see testImage).
	if w.fetched != 5 || w.zeros != 124 || len(w.leaves) != 257 {
		t.Errorf("walk fetched %d chunks, skipped %d zero subtrees and found %d leaves, want 5, 124 and 257", w.fetched, w.zeros, len(w.leaves))
	}
	t.Logf("walk took %v", w.took)
	for i, conc := range []int{1, 8} {
		res := probe(ctx, c, pick(w.leaves, i*50, 50), conc)
		if res.failed > 0 {
			t.Errorf("concurrency %d: %d fetches failed; the first: %v", conc, res.failed, res.err)
		}
		t.Log(res.row(conc))
	}
}
