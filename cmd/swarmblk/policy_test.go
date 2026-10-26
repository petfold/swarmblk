package main

import (
	"bytes"
	"context"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/snapshot"
)

// waitFor polls cond every 100 ms for up to a minute.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(time.Minute); !cond(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("waited a minute for %s", what)
		}
	}
}

// hydrated reports whether the disk's local copy holds all its blocks, as
// far as its map on disk shows.
func hydrated(dir string, blocks int) bool {
	b, err := os.ReadFile(filepath.Join(dir, imageDir, "present"))
	if err != nil {
		return false
	}
	n := 0
	for _, x := range b {
		n += bits.OnesCount8(x)
	}
	return n == blocks
}

// hideAll makes the fake Bee refuse every chunk it holds now.
func hideAll(t *testing.T, cp *checkpointer) {
	t.Helper()
	cp.bee.Store.Iterate(context.Background(), func(ch swarm.Chunk) (bool, error) {
		cp.bee.Hide(ch.Address(), true)
		return false, nil
	})
}

// TestDiskHydrate serves a disk with the hydrate policy until its local
// copy is complete, then hides every chunk from Bee: the disk must still
// read whole, take a checkpoint that moves the copy on, and start again
// from the copy alone. Turning the policy off removes the copy.
func TestDiskHydrate(t *testing.T) {
	needQEMU(t)
	cp, h := diskCheckpointer(t)
	snap0, img := cp.importDisk(t)
	dir := filepath.Join(h.home, "disks", "vm")
	cfg := serveConfig{disk: "vm", arg: snap0.String(), disks: filepath.Join(h.home, "disks"), batch: testBatch, keys: cp.keys, policy: policyHydrate}
	uri, stop := cp.startServeWith(t, cfg)
	waitFor(t, "the local copy", func() bool { return hydrated(dir, len(img)/swarm.ChunkSize) })
	hideAll(t, cp)
	if !bytes.Equal(readAll(t, "raw", uri), img) {
		t.Fatal("the hydrated disk reads different with Bee holding nothing")
	}

	run(t, "qemu-io", "-f", "raw", "-c", "write -P 0x51 0 64k", "-c", "write -P 0x52 6M 512", "-c", "write -z 8M 64k", uri)
	want := slices.Clone(img)
	set(want, 0, 64<<10, 0x51)
	set(want, 6<<20, 512, 0x52)
	set(want, 8<<20, 64<<10, 0)
	snap1, log, err := h.checkpoint(t, "vm")
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if !bytes.Equal(readAll(t, "raw", uri), want) {
		t.Error("the disk reads different after a checkpoint")
	}
	if err, log := stop(); err != nil || !strings.Contains(log, "policy hydrate") || !strings.Contains(log, "disk vm: hydrated, all 16384 blocks local") {
		t.Fatalf("serve ended with %v:\n%s", err, log)
	}

	cfg.arg, cfg.policy = "", "" // the disk keeps its policy
	uri, stop = cp.startServeWith(t, cfg)
	if !bytes.Equal(readAll(t, "raw", uri), want) {
		t.Error("the restarted disk reads different from its local copy")
	}
	if err, log := stop(); err != nil || !strings.Contains(log, "on snapshot "+snap1.String()+", policy hydrate") {
		t.Errorf("serve ended with %v:\n%s", err, log)
	}

	cfg.policy = policyLazy
	_, stop = cp.startServeWith(t, cfg)
	if err, log := stop(); err != nil || !strings.Contains(log, "removing its local copy") {
		t.Errorf("serve ended with %v:\n%s", err, log)
	}
	if _, err := os.Stat(filepath.Join(dir, imageDir)); !os.IsNotExist(err) {
		t.Errorf("a lazy disk kept its local copy: %v", err)
	}
}

// TestDiskRestamp serves a disk with the restamp policy: every chunk of its
// snapshot must reach Bee again, a checkpoint must carry the record on to
// the next snapshot, and a restart must not re-stamp again.
func TestDiskRestamp(t *testing.T) {
	needQEMU(t)
	ctx := context.Background()
	cp, h := diskCheckpointer(t)
	snap0, _ := cp.importDisk(t)
	var chunks []swarm.Address
	if err := snapshot.Chunks(ctx, cp.bee.Store, snap0, func(a swarm.Address) error {
		chunks = append(chunks, a)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(h.home, "disks", "vm")
	record := func() restampRecord {
		r, _ := (&diskState{dir: dir}).readRestamp()
		return r
	}
	n := len(cp.bee.Received())
	cfg := serveConfig{disk: "vm", arg: snap0.String(), disks: filepath.Join(h.home, "disks"), batch: testBatch, keys: cp.keys, policy: policyRestamp}
	uri, stop := cp.startServeWith(t, cfg)
	waitFor(t, "the re-stamp", func() bool { return record().Snapshot.Equal(snap0) })
	again := map[string]bool{}
	for _, a := range cp.since(n) {
		again[a.ByteString()] = true
	}
	for _, a := range chunks {
		if !again[a.ByteString()] {
			t.Fatalf("chunk %s of the snapshot was not stamped again", a)
		}
	}

	run(t, "qemu-io", "-f", "raw", "-c", "write -P 0x61 0 8k", uri)
	snap1, log, err := h.checkpoint(t, "vm")
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if r := record(); !r.Snapshot.Equal(snap1) || !slices.Equal(r.Batches, []string{testBatch}) {
		t.Errorf("after a checkpoint the record is %+v, want snapshot %s", r, snap1)
	}
	if err, log := stop(); err != nil || !strings.Contains(log, "re-stamped snapshot "+snap0.String()) {
		t.Fatalf("serve ended with %v:\n%s", err, log)
	}

	n = len(cp.bee.Received())
	cfg.arg = ""
	_, stop = cp.startServeWith(t, cfg)
	time.Sleep(time.Second)
	if err, log := stop(); err != nil || strings.Contains(log, "re-stamping") {
		t.Errorf("a restart re-stamped again: %v\n%s", err, log)
	}
	if got := len(cp.since(n)); got != 0 {
		t.Errorf("a restart uploaded %d chunks", got)
	}
}

// TestCacheSweep serves a snapshot with a small cache and reads it whole:
// serve must sweep the leaves it fetched, and the disk must still read.
func TestCacheSweep(t *testing.T) {
	needQEMU(t)
	defer func(d time.Duration) { sweepInterval = d }(sweepInterval)
	sweepInterval = 50 * time.Millisecond
	cp := newCheckpointer(t)
	snap0, img := cp.importDisk(t)
	uri, stop := cp.startServeWith(t, serveConfig{arg: snap0.String(), cache: 64 << 10})
	for range 2 {
		if !bytes.Equal(readAll(t, "raw", uri), img) {
			t.Fatal("the disk reads different")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err, log := stop(); err != nil || !strings.Contains(log, "chunk cache: removed") {
		t.Errorf("serve ended with %v:\n%s", err, log)
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{"0": 0, "4096": 4096, "512K": 512 << 10, "4G": 4 << 30, "1t": 1 << 40, " 2M ": 2 << 20} {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "G", "-1", "1.5G", "4P", "99999999999T"} {
		if _, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) took it", in)
		}
	}
}
