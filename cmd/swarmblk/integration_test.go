//go:build integration

package main

// Live tests against a running Bee node: $SWARMBLK_BEE_URL (default
// http://localhost:1633), uploading with the immutable batch in
// $SWARMBLK_BATCH, which must belong to that node. Run them with
//
//	go test -tags integration ./cmd/swarmblk

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/lineage"
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

// TestLiveCheckpoint imports the test image, writes to a qcow2 overlay on it
// through QEMU, which reads the snapshot from Bee as its backing file, and
// checkpoints the overlay twice. Each time Bee must serve the new snapshot's
// disk.raw with the same bytes as the VM's disk (SPEC §14, I7, on one host),
// and the lineage's head must move to it. Then the lineage is exported,
// restored and served over NBD, each with an empty store, and all must read
// as the VM's disk. The lineage key is a new one in a temporary directory.
func TestLiveCheckpoint(t *testing.T) {
	needQEMU(t)
	batch := os.Getenv("SWARMBLK_BATCH")
	if batch == "" {
		t.Fatal("SWARMBLK_BATCH is not set")
	}
	url := envOr("SWARMBLK_BEE_URL", "http://localhost:1633")
	ctx := context.Background()
	c := bee.New(url, 64)
	s, err := store.Open(filepath.Join(t.TempDir(), "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	keys := filepath.Join(t.TempDir(), "keys")
	path, _ := testImage(t)
	var progress bytes.Buffer
	snap, err := importImage(ctx, c, s, batch, path, &progress)
	if err != nil {
		t.Fatal(err)
	}

	ov := filepath.Join(t.TempDir(), "overlay.qcow2")
	run(t, "qemu-img", "create", "-q", "-u", "-f", "qcow2", "-F", "raw", "-b",
		`json:{"file.driver":"http","file.url":"`+url+`/bzz/`+snap.String()+`/disk.raw","file.timeout":60}`, ov, "64M")
	sum := func(r io.Reader) [32]byte {
		h := sha256.New()
		if _, err := io.Copy(h, r); err != nil {
			t.Fatal(err)
		}
		return [32]byte(h.Sum(nil))
	}
	var want [32]byte
	var l lineage.Lineage
	for round, cmds := range [][]string{
		{"write -P 0xab 4096 4096", "write -z 1M 64k", "write -P 0x5a 63M 1M"},
		{"write -P 0x77 8M 4k"},
	} {
		args := []string{"-f", "qcow2"}
		for _, cmd := range cmds {
			args = append(args, "-c", cmd)
		}
		run(t, "qemu-io", append(args, ov)...)
		raw := filepath.Join(t.TempDir(), "disk.raw")
		run(t, "qemu-img", "convert", "-f", "qcow2", "-O", "raw", ov, raw)
		f, err := os.Open(raw)
		if err != nil {
			t.Fatal(err)
		}
		want = sum(f)
		f.Close()

		progress.Reset()
		start := time.Now()
		next, err := checkpoint(ctx, c, s, keys, batch, ov, false, &progress)
		t.Logf("round %d, %v:\n%s", round, time.Since(start).Round(time.Millisecond), progress.String())
		if err != nil {
			t.Fatal(err)
		}
		got, err := snapshot.Read(ctx, s, next)
		if err != nil {
			t.Fatal(err)
		}
		if !got.Meta.Parent.Equal(snap) || got.Meta.Lineage.Index != uint64(round) {
			t.Errorf("round %d: parent %s, lineage %+v; want %s and update %d", round, got.Meta.Parent, got.Meta.Lineage, snap, round)
		}
		resp, err := http.Get(url + "/bzz/" + next.String() + "/disk.raw")
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK || sum(resp.Body) != want {
			t.Errorf("round %d: Bee serves /bzz/%s/disk.raw (%s) with other bytes than the VM's disk", round, next, resp.Status)
		}
		resp.Body.Close()
		if l, _, err = lineage.FromMeta(*got.Meta.Lineage); err != nil {
			t.Fatal(err)
		}
		if head, err := lineage.Head(ctx, c, l); err != nil || head.Index != uint64(round) || !head.Snapshot.Equal(next) {
			t.Errorf("round %d: lineage head %+v, %v; want snapshot %s", round, head, err, next)
		}
		snap = next
	}

	// The lineage restores with nothing cached: exported, and as a new
	// overlay that QEMU reads from Bee.
	fresh, err := store.Open(filepath.Join(t.TempDir(), "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	raw := filepath.Join(t.TempDir(), "export.raw")
	progress.Reset()
	if got, err := export(ctx, c, fresh, l.String(), raw, &progress); err != nil || !got.Equal(snap) {
		t.Fatalf("export of lineage %s: %s, %v; want %s", l, got, err, snap)
	}
	t.Log("\n" + progress.String())
	f, err := os.Open(raw)
	if err != nil {
		t.Fatal(err)
	}
	if sum(f) != want {
		t.Error("the exported image differs from the VM's disk")
	}
	f.Close()
	restored := filepath.Join(t.TempDir(), "restored.qcow2")
	if got, err := restore(ctx, c, fresh, url, l.String(), restored, &progress); err != nil || !got.Equal(snap) {
		t.Fatalf("restore of lineage %s: %s, %v; want %s", l, got, err, snap)
	}
	converted := filepath.Join(t.TempDir(), "restored.raw")
	run(t, "qemu-img", "convert", "-f", "qcow2", "-O", "raw", restored, converted)
	if f, err = os.Open(converted); err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if sum(f) != want {
		t.Error("QEMU reads the restored overlay different from the VM's disk")
	}

	// The engine serves the lineage over NBD, again with an empty store.
	served, err := store.Open(filepath.Join(t.TempDir(), "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	sctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- serve(sctx, c, served, l.String(), "", filepath.Join(t.TempDir(), "nbd.sock"), pw, io.Discard)
		pw.Close()
	}()
	line, err := bufio.NewReader(pr).ReadString('\n')
	if err != nil {
		cancel()
		t.Fatalf("serve: %v", <-done)
	}
	go io.Copy(io.Discard, pr)
	viaNBD := filepath.Join(t.TempDir(), "nbd.raw")
	run(t, "qemu-img", "convert", "-f", "raw", "-O", "raw", strings.TrimSpace(line), viaNBD)
	cancel()
	if err := <-done; err != nil {
		t.Error(err)
	}
	g, err := os.Open(viaNBD)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if sum(g) != want {
		t.Error("QEMU reads the served lineage different from the VM's disk")
	}
}
