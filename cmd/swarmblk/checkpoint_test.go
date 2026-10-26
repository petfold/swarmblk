package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/file/joiner"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/soc"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/beetest"
	"github.com/petfold/swarmblk/internal/lineage"
	"github.com/petfold/swarmblk/internal/snapshot"
	"github.com/petfold/swarmblk/internal/store"
)

// needQEMU skips a test that needs QEMU's tools where they are missing,
// except in CI, which installs them.
func needQEMU(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"qemu-img", "qemu-io"} {
		if _, err := exec.LookPath(tool); err != nil {
			if os.Getenv("CI") != "" {
				t.Fatalf("%s is missing", tool)
			}
			t.Skipf("%s is missing; the overlay tests need QEMU's tools", tool)
		}
	}
}

func run(t *testing.T, name string, args ...string) {
	t.Helper()
	if out, err := exec.Command(name, args...).CombinedOutput(); err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
}

// testBacking names snap's disk as restore does, on a host that tests never
// contact.
func testBacking(t *testing.T, snap swarm.Address) string {
	t.Helper()
	b, err := backingName("http://bee.invalid", snap)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// testOverlay is a qcow2 overlay, and the disk that a VM sees through it.
type testOverlay struct {
	path string
	disk []byte
}

// newOverlay makes an empty overlay on snapshot snap, whose disk is disk.
func newOverlay(t *testing.T, snap swarm.Address, disk []byte) *testOverlay {
	t.Helper()
	o := &testOverlay{path: filepath.Join(t.TempDir(), "overlay.qcow2"), disk: disk}
	run(t, "qemu-img", "create", "-q", "-u", "-f", "qcow2", "-F", "raw", "-b", testBacking(t, snap), o.path, fmt.Sprint(len(disk)))
	return o
}

// write runs qemu-io commands on the overlay as a VM's writes. For their
// duration the backing file is a local copy of the disk, so that partial
// writes copy the right data into the overlay; then the overlay gets its own
// backing file name back.
func (o *testOverlay) write(t *testing.T, cmds ...string) {
	t.Helper()
	ov, err := openOverlay(context.Background(), o.path)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	local, out := filepath.Join(dir, "disk.raw"), filepath.Join(dir, "out.raw")
	if err := os.WriteFile(local, o.disk, 0o600); err != nil {
		t.Fatal(err)
	}
	run(t, "qemu-img", "rebase", "-u", "-f", "qcow2", "-F", "raw", "-b", local, o.path)
	args := []string{"-f", "qcow2"}
	for _, c := range cmds {
		args = append(args, "-c", c)
	}
	run(t, "qemu-io", append(args, o.path)...)
	run(t, "qemu-img", "convert", "-f", "qcow2", "-O", "raw", o.path, out)
	if o.disk, err = os.ReadFile(out); err != nil {
		t.Fatal(err)
	}
	run(t, "qemu-img", "rebase", "-u", "-f", "qcow2", "-F", "raw", "-b", ov.Backing, o.path)
}

// checkpointer runs checkpoints of overlays against one fake Bee, with one
// local store and key directory.
type checkpointer struct {
	bee  *beetest.Bee
	c    *bee.Client
	s    *store.Store
	keys string
}

func newCheckpointer(t *testing.T) *checkpointer {
	b := beetest.New(testBatch)
	return &checkpointer{bee: b, c: b.Client(t), s: newStore(t), keys: filepath.Join(t.TempDir(), "keys")}
}

func (cp *checkpointer) run(t *testing.T, o *testOverlay, branch bool) (swarm.Address, error) {
	t.Helper()
	return cp.runWith(t, o, branch, nil)
}

// runWith checkpoints o, storing the boot trace prefetch with the snapshot
// if it is not nil.
func (cp *checkpointer) runWith(t *testing.T, o *testOverlay, branch bool, prefetch []byte) (swarm.Address, error) {
	t.Helper()
	var log bytes.Buffer
	snap, err := checkpoint(context.Background(), cp.c, cp.s, cp.keys, testBatch, o.path, branch, prefetch, &log)
	t.Log("\n" + log.String())
	return snap, err
}

// importDisk imports the test image and returns its snapshot and contents.
func (cp *checkpointer) importDisk(t *testing.T) (swarm.Address, []byte) {
	t.Helper()
	path, img := testImage(t)
	snap, err := importImage(context.Background(), cp.c, cp.s, testBatch, path, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return snap, img
}

// check verifies the snapshot that a checkpoint of o made, from what the
// fake Bee received alone: its metadata, that its tree is Bee's tree of the
// disk that the VM sees and that all of it was uploaded, the root of the
// manifest last, before the lineage update. The overlay must sit on it, and
// the lineage head must be it.
func (cp *checkpointer) check(t *testing.T, o *testOverlay, snap, parent swarm.Address, index uint64, received []swarm.Address) snapshot.Snapshot {
	t.Helper()
	ctx := context.Background()
	got, err := snapshot.Read(ctx, cp.bee.Store, snap)
	if err != nil {
		t.Fatal(err)
	}
	m := got.Meta
	if m.Parent == nil || !m.Parent.Equal(parent) || m.Lineage == nil || m.Lineage.Index != index || m.Batches[0] != testBatch {
		t.Errorf("meta.json %+v, want parent %s and lineage update %d", m, parent, index)
	}
	root, err := builder.FeedPipeline(ctx, builder.NewPipelineBuilder(ctx, putterFunc(func(context.Context, swarm.Chunk) error { return nil }), false, redundancy.NONE), bytes.NewReader(o.disk))
	if err != nil {
		t.Fatal(err)
	}
	if !m.DiskRoot.Equal(root) {
		t.Fatalf("snapshot %s has disk root %s; Bee builds %s for the VM's disk", snap, m.DiskRoot, root)
	}
	j, _, err := joiner.New(ctx, cp.bee.Store, putterFunc(func(context.Context, swarm.Chunk) error { return nil }), root, redundancy.NONE)
	if err != nil {
		t.Fatal(err)
	}
	if disk, err := io.ReadAll(j); err != nil || !bytes.Equal(disk, o.disk) {
		t.Fatalf("the uploaded tree reads back different: %v", err)
	}

	n := len(received)
	if n < 2 || !received[n-2].Equal(snap) {
		t.Fatalf("%d chunks uploaded; the manifest's root %s must come last before the update", n, snap)
	}
	if ch, err := cp.bee.Store.Get(ctx, received[n-1]); err != nil || !soc.Valid(ch) {
		t.Errorf("the last chunk uploaded is not a lineage update: %v", err)
	}
	if extra := n - 1 - int(m.Stats.NewChunks); extra < 2 || extra > 10 {
		t.Errorf("%d chunks uploaded for %d new ones in the disk tree; want a few for the manifest", n-1, m.Stats.NewChunks)
	}

	ov, err := openOverlay(ctx, o.path)
	if err != nil {
		t.Fatal(err)
	}
	if !ov.Snapshot.Equal(snap) || !strings.Contains(ov.Backing, `"file.timeout":60`) {
		t.Errorf("the overlay's backing file is %s, want %s with the options kept", ov.Backing, testBacking(t, snap))
	}
	l, i, err := lineage.FromMeta(*m.Lineage)
	if err != nil {
		t.Fatal(err)
	}
	if head, err := lineage.Head(ctx, cp.c, l); err != nil || head.Index != i || !head.Snapshot.Equal(snap) {
		t.Errorf("lineage head %+v, %v; want update %d, snapshot %s", head, err, i, snap)
	}
	return got
}

// since returns the chunks the fake Bee received after the first n.
func (cp *checkpointer) since(n int) []swarm.Address {
	return cp.bee.Received()[n:]
}

func TestCheckpoint(t *testing.T) {
	needQEMU(t)
	cp := newCheckpointer(t)
	snap0, img := cp.importDisk(t)
	o := newOverlay(t, snap0, img)

	// Partial writes, a zeroed cluster, a discard, and zeros over data.
	o.write(t, "write -P 0xab 4096 4096", "write -z 1M 64k", "write -P 0xcd 2M 128k", "discard 2M 64k", "write -z 60M 64k", "write -P 0x5a 63M 1M")
	n := len(cp.bee.Received())
	snap1, err := cp.run(t, o, false)
	if err != nil {
		t.Fatal(err)
	}
	s1 := cp.check(t, o, snap1, snap0, 0, cp.since(n))
	if s1.Meta.Stats.ChangedBlocks != 1+16+16+1+256 {
		t.Errorf("%d blocks changed, want 290", s1.Meta.Stats.ChangedBlocks)
	}

	// The overlay keeps its blocks: those unchanged since the last
	// checkpoint upload nothing.
	o.write(t, "write -P 0xab 4096 4096", "write -P 0x77 8M 4k")
	n = len(cp.bee.Received())
	snap2, err := cp.run(t, o, false)
	if err != nil {
		t.Fatal(err)
	}
	s2 := cp.check(t, o, snap2, snap1, 1, cp.since(n))
	if s2.Meta.Stats.ChangedBlocks != 1 || s2.Meta.Stats.NewChunks != 3 {
		t.Errorf("stats %+v, want one changed block and three new chunks: the leaf, its parent and the root", s2.Meta.Stats)
	}

	n = len(cp.bee.Received())
	if same, err := cp.run(t, o, false); err != nil || !same.Equal(snap2) || len(cp.since(n)) != 0 {
		t.Errorf("without changes: %s, %v, %d chunks uploaded; want %s and none", same, err, len(cp.since(n)), snap2)
	}

	// A publish that fails leaves the overlay on the new snapshot; the next
	// checkpoint publishes it.
	o.write(t, "write -P 0x33 12M 4k")
	cp.bee.RefuseSOC(true)
	snap3, err := cp.run(t, o, false)
	if err == nil {
		t.Fatalf("checkpoint published %s while Bee refused updates", snap3)
	}
	cp.bee.RefuseSOC(false)
	n = len(cp.bee.Received())
	if snap3, err = cp.run(t, o, false); err != nil {
		t.Fatal(err)
	}
	if got := cp.since(n); len(got) != 1 {
		t.Errorf("the retry uploaded %d chunks, want the update alone", len(got))
	}
	s3, err := snapshot.Read(context.Background(), cp.bee.Store, snap3)
	if err != nil || s3.Meta.Lineage.Index != 2 || !s3.Meta.Parent.Equal(snap2) {
		t.Fatalf("the snapshot left unpublished: %+v, %v", s3.Meta, err)
	}
	l, _, _ := lineage.FromMeta(*s3.Meta.Lineage)
	if head, err := lineage.Head(context.Background(), cp.c, l); err != nil || head.Index != 2 || !head.Snapshot.Equal(snap3) {
		t.Errorf("after the retry the head is %+v, %v; want update 2, snapshot %s", head, err, snap3)
	}

	// A branch starts a new lineage from the overlay's snapshot.
	o.write(t, "write -P 0x44 16M 4k")
	n = len(cp.bee.Received())
	snap4, err := cp.run(t, o, true)
	if err != nil {
		t.Fatal(err)
	}
	s4 := cp.check(t, o, snap4, snap3, 0, cp.since(n))
	if s4.Meta.Lineage.Topic == s3.Meta.Lineage.Topic {
		t.Error("the branch kept the lineage")
	}
}

// TestCheckpointElsewhere checkpoints overlays on another host's lineage:
// without its key, the snapshot starts a lineage of this host's. With the
// key but a parent that is not the head, the writer must branch.
func TestCheckpointElsewhere(t *testing.T) {
	needQEMU(t)
	cp := newCheckpointer(t)
	snap0, img := cp.importDisk(t)
	o := newOverlay(t, snap0, img)
	o.write(t, "write -P 0x01 0 4k")
	snap1, err := cp.run(t, o, false)
	if err != nil {
		t.Fatal(err)
	}
	disk1 := o.disk
	o.write(t, "write -P 0x02 0 4k")
	if _, err := cp.run(t, o, false); err != nil {
		t.Fatal(err)
	}

	// Another overlay on snap1, now behind the head.
	behind := newOverlay(t, snap1, disk1)
	behind.write(t, "write -P 0x03 4M 4k")
	if _, err := cp.run(t, behind, false); !errors.Is(err, lineage.ErrNotHead) || !strings.Contains(err.Error(), "-branch") {
		t.Errorf("an overlay behind the head: %v, want ErrNotHead and advice to branch", err)
	}

	other := &checkpointer{bee: cp.bee, c: cp.c, s: newStore(t), keys: filepath.Join(t.TempDir(), "keys")}
	n := len(cp.bee.Received())
	snap, err := other.run(t, behind, false)
	if err != nil {
		t.Fatal(err)
	}
	s := other.check(t, behind, snap, snap1, 0, other.since(n))
	s1, _ := snapshot.Read(context.Background(), cp.bee.Store, snap1)
	if s.Meta.Lineage.Owner == s1.Meta.Lineage.Owner {
		t.Error("a host without the lineage's key continued it")
	}
}

func TestCheckpointRefuses(t *testing.T) {
	needQEMU(t)
	cp := newCheckpointer(t)
	snap0, img := cp.importDisk(t)
	dir := t.TempDir()
	raw := filepath.Join(dir, "disk.raw")
	if err := os.WriteFile(raw, img, 0o600); err != nil {
		t.Fatal(err)
	}
	overlay := func(name, backing, size string) string {
		path := filepath.Join(dir, name)
		args := []string{"create", "-q", "-u", "-f", "qcow2"}
		if backing != "" {
			args = append(args, "-F", "raw", "-b", backing)
		}
		run(t, "qemu-img", append(args, path, size)...)
		return path
	}
	for name, c := range map[string]struct {
		path, want string
	}{
		"raw image":       {raw, "not a qcow2"},
		"no backing":      {overlay("plain.qcow2", "", "64M"), "no raw backing"},
		"a disk root":     {overlay("bytes.qcow2", "http://bee.invalid/bytes/"+snap0.String(), "64M"), "does not name one snapshot"},
		"another size":    {overlay("large.qcow2", testBacking(t, snap0), "128M"), "134217728-byte disk"},
		"unknown":         {overlay("unknown.qcow2", testBacking(t, swarm.MustParseHexAddress(strings.Repeat("ab", 32))), "64M"), "not found"},
		"a missing image": {filepath.Join(dir, "missing.qcow2"), "missing.qcow2"},
	} {
		n := len(cp.bee.Received())
		if _, err := cp.run(t, &testOverlay{path: c.path}, false); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error about %q", name, err, c.want)
		}
		if len(cp.since(n)) != 0 {
			t.Errorf("%s: chunks uploaded before refusing", name)
		}
	}

	cp.bee.Immutable = false
	o := newOverlay(t, snap0, img)
	o.write(t, "write -P 0x01 0 4k")
	if _, err := cp.run(t, o, false); err == nil || !strings.Contains(err.Error(), "mutable") {
		t.Errorf("a mutable batch: %v", err)
	}
}

var _ storage.Putter = putterFunc(nil)
