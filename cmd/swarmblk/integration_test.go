//go:build integration

package main

// Live tests against a running Bee node: $SWARMBLK_BEE_URL (default
// http://localhost:1633), uploading with the immutable batch in
// $SWARMBLK_BATCH, which must belong to that node. Run them with
//
//	go test -tags integration ./cmd/swarmblk

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/snapshot"
	"github.com/petfold/swarmblk/internal/store"
)

// TestLiveImportMatchesBytes imports an image chunk by chunk, then uploads
// the same bytes with POST /bytes at redundancy level 0, as Phase 0 did, and
// requires the same disk root (ROADMAP, Phase 1). Stock Bee must also serve
// the snapshot: the image as /bzz/<snapshot>/disk.raw, and meta.json both by
// name and as the index document (SPEC §5).
func TestLiveImportMatchesBytes(t *testing.T) {
	batch := os.Getenv("SWARMBLK_BATCH")
	if batch == "" {
		t.Fatal("SWARMBLK_BATCH is not set")
	}
	url := envOr("SWARMBLK_BEE_URL", "http://localhost:1633")
	ctx := context.Background()
	path, img := testImage(t)
	s, err := store.Open(filepath.Join(t.TempDir(), "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	var progress bytes.Buffer
	ref, err := importImage(ctx, bee.New(url, 64), s, batch, path, &progress)
	t.Log("\n" + progress.String())
	if err != nil {
		t.Fatal(err)
	}
	snap, err := snapshot.Read(ctx, s, ref)
	if err != nil {
		t.Fatal(err)
	}
	root := snap.Meta.DiskRoot

	req, err := http.NewRequest(http.MethodPost, url+"/bytes", bytes.NewReader(img))
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
	var r struct{ Reference swarm.Address }
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /bytes: %s, %v", resp.Status, err)
	}
	if !root.Equal(r.Reference) {
		t.Errorf("import gave disk root %s; POST /bytes gave %s", root, r.Reference)
	}

	get := func(path string) []byte {
		t.Helper()
		resp, err := http.Get(url + "/bzz/" + ref.String() + "/" + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /bzz/%s/%s: %s, %v", ref, path, resp.Status, err)
		}
		return b
	}
	if !bytes.Equal(get("disk.raw"), img) {
		t.Error("/bzz/<snapshot>/disk.raw differs from the image")
	}
	var m snapshot.Meta
	if err := json.Unmarshal(get("meta.json"), &m); err != nil || !m.DiskRoot.Equal(root) {
		t.Errorf("/bzz/<snapshot>/meta.json: %+v, %v", m, err)
	}
	if index := get(""); !bytes.Contains(index, []byte(`"format":"swarmblk/1"`)) {
		t.Errorf("/bzz/<snapshot>/ serves %.80q, want meta.json", index)
	}
}
