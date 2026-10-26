package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/epoch"
	"github.com/petfold/swarmblk/internal/snapshot"
)

// diskHome is the local state directory of a test's disks.
type diskHome struct {
	cp   *checkpointer
	home string
}

func newDiskHome(t *testing.T, cp *checkpointer) *diskHome {
	return &diskHome{cp: cp, home: t.TempDir()}
}

// serve serves disk name writable, checkpointing with the test batch.
func (h *diskHome) serve(t *testing.T, name, arg string) (string, func() (error, string)) {
	t.Helper()
	return h.cp.startServeWith(t, serveConfig{disk: name, arg: arg, disks: filepath.Join(h.home, "disks"), batch: testBatch, keys: h.cp.keys})
}

// checkpoint asks for a checkpoint of disk name, through its serve or here.
func (h *diskHome) checkpoint(t *testing.T, name string) (swarm.Address, string, error) {
	t.Helper()
	var log bytes.Buffer
	snap, err := checkpointNamedDisk(context.Background(), h.cp.c, h.cp.s, h.home, name, testBatch, &log)
	return snap, log.String(), err
}

// epochs lists the disk's epoch directories.
func (h *diskHome) epochs(t *testing.T, name string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(h.home, "disks", name))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if _, err := strconv.Atoi(e.Name()); err == nil {
			out = append(out, e.Name())
		}
	}
	return out
}

// expect checks a disk's snapshot: its parent and lineage index, and that
// its disk, exported through another store, is want.
func (h *diskHome) expect(t *testing.T, snap, parent swarm.Address, index uint64, want []byte) {
	t.Helper()
	s, err := snapshot.Read(context.Background(), h.cp.bee.Store, snap)
	if err != nil {
		t.Fatal(err)
	}
	if s.Meta.Parent == nil || !s.Meta.Parent.Equal(parent) || s.Meta.Lineage == nil || s.Meta.Lineage.Index != index {
		t.Errorf("snapshot %s: parent %v, lineage %+v; want %s and update %d", snap, s.Meta.Parent, s.Meta.Lineage, parent, index)
	}
	if _, img, _ := h.cp.exportFresh(t, snap.String()); !bytes.Equal(img, want) {
		t.Errorf("snapshot %s holds other bytes than the disk at its cut", snap)
	}
}

// diskCheckpointer gives a test a checkpointer whose keys live in a disk
// home, where checkpointNamedDisk looks for them.
func diskCheckpointer(t *testing.T) (*checkpointer, *diskHome) {
	cp := newCheckpointer(t)
	h := newDiskHome(t, cp)
	cp.keys = filepath.Join(h.home, "keys")
	return cp, h
}

func set(b []byte, off, n int, v byte) { copy(b[off:], bytes.Repeat([]byte{v}, n)) }

func TestDiskCheckpoint(t *testing.T) {
	needQEMU(t)
	cp, h := diskCheckpointer(t)
	snap0, img := cp.importDisk(t)
	uri, stop := h.serve(t, "vm", snap0.String())
	run(t, "qemu-io", "-f", "raw", "-c", "write -P 0x71 0 64k", "-c", "write -P 0x72 1M 512", "-c", "write -z 2M 64k", "-c", "flush", uri)
	want1 := slices.Clone(img)
	set(want1, 0, 64<<10, 0x71)
	set(want1, 1<<20, 512, 0x72)
	set(want1, 2<<20, 64<<10, 0)

	snap1, log, err := h.checkpoint(t, "vm")
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if !strings.Contains(log, "froze epoch 1") {
		t.Errorf("the checkpoint's log:\n%s", log)
	}
	h.expect(t, snap1, snap0, 0, want1)

	run(t, "qemu-io", "-f", "raw", "-c", "write -P 0x73 3M 4k", "-c", "write -P 0x74 1M 512", uri)
	want2 := slices.Clone(want1)
	set(want2, 3<<20, 4<<10, 0x73)
	set(want2, 1<<20, 512, 0x74)
	snap2, log, err := h.checkpoint(t, "vm")
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	h.expect(t, snap2, snap1, 1, want2)

	n := len(cp.bee.Received())
	if same, log, err := h.checkpoint(t, "vm"); err != nil || !same.Equal(snap2) || len(cp.since(n)) != 0 || !strings.Contains(log, "no writes since") {
		t.Errorf("a checkpoint without writes: %s, %v, %d chunks uploaded\n%s", same, err, len(cp.since(n)), log)
	}
	if got := h.epochs(t, "vm"); len(got) != 1 || got[0] != "3" {
		t.Errorf("the disk keeps epochs %v, want only the active one, 3", got)
	}
	if !bytes.Equal(readAll(t, "raw", uri), want2) {
		t.Error("the disk reads different after its checkpoints")
	}
	if err, log := stop(); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}

	uri, stop = h.serve(t, "vm", "")
	if !bytes.Equal(readAll(t, "raw", uri), want2) {
		t.Error("the restarted disk reads different")
	}
	if err, log := stop(); err != nil || !strings.Contains(log, "on snapshot "+snap2.String()) {
		t.Errorf("the restarted disk: %v\n%s", err, log)
	}

	// With no serve running the disk, checkpoint opens it itself.
	if snap, log, err := h.checkpoint(t, "vm"); err != nil || !snap.Equal(snap2) {
		t.Errorf("an offline checkpoint without writes: %s, %v\n%s", snap, err, log)
	}
}

// TestDiskCheckpointResume stops checkpoints at each stage: a publish that
// fails, an upload that fails, and an epoch left after the disk moved on.
// Each must be finished by the next checkpoint or the next open.
func TestDiskCheckpointResume(t *testing.T) {
	needQEMU(t)
	ctx := context.Background()
	cp, h := diskCheckpointer(t)
	snap0, img := cp.importDisk(t)
	uri, stop := h.serve(t, "vm", snap0.String())
	run(t, "qemu-io", "-f", "raw", "-c", "write -P 0x81 0 8k", uri)
	want := slices.Clone(img)
	set(want, 0, 8<<10, 0x81)

	// The publish fails: the snapshot is uploaded and recorded.
	cp.bee.RefuseSOC(true)
	if _, log, err := h.checkpoint(t, "vm"); err == nil {
		t.Fatalf("a checkpoint succeeded while Bee refused updates\n%s", log)
	}
	cp.bee.RefuseSOC(false)
	frozen, err := epoch.Open(filepath.Join(h.home, "disks", "vm", "1"))
	if err == nil {
		frozen.Close()
		t.Fatal("the frozen epoch is not held by serve")
	}
	if !bytes.Equal(readAll(t, "raw", uri), want) {
		t.Error("the disk reads different after a failed checkpoint")
	}
	stop()
	ep, err := epoch.Open(filepath.Join(h.home, "disks", "vm", "1"))
	if err != nil {
		t.Fatal(err)
	}
	s := ep.Super()
	ep.Close()
	if s.State != epoch.Uploading || s.Snapshot == nil {
		t.Fatalf("after the failed publish epoch 1 is %+v", s)
	}
	uploaded := *s.Snapshot

	// serve finishes it at start; a checkpoint asked for waits for that.
	n := len(cp.bee.Received())
	uri, stop = h.serve(t, "vm", "")
	snap1, log, err := h.checkpoint(t, "vm")
	if err != nil || !snap1.Equal(uploaded) {
		t.Fatalf("after a restart: %s, %v; want the uploaded %s\n%s", snap1, err, uploaded, log)
	}
	if got := cp.since(n); len(got) != 1 {
		t.Errorf("finishing the checkpoint uploaded %d chunks, want the update alone", len(got))
	}
	h.expect(t, snap1, snap0, 0, want)

	// The upload fails: the frozen epoch stays, and the next checkpoint
	// finishes it, then the writes after it.
	run(t, "qemu-io", "-f", "raw", "-c", "write -P 0x82 4M 4k", uri)
	set(want, 4<<20, 4<<10, 0x82)
	cp.bee.RefuseChunks(true)
	if _, log, err := h.checkpoint(t, "vm"); err == nil {
		t.Fatalf("a checkpoint succeeded while Bee refused chunks\n%s", log)
	}
	cp.bee.RefuseChunks(false)
	atCut := slices.Clone(want)
	run(t, "qemu-io", "-f", "raw", "-c", "write -P 0x83 5M 4k", uri)
	set(want, 5<<20, 4<<10, 0x83)
	snap3, log, err := h.checkpoint(t, "vm")
	if err != nil || !strings.Contains(log, "taking up the checkpoint of epoch") {
		t.Fatalf("%v\n%s", err, log)
	}
	h.expect(t, snap3, mustParent(t, cp, snap3), 2, want)
	h.expect(t, mustParent(t, cp, snap3), snap1, 1, atCut)
	if err, log := stop(); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}

	// An epoch whose checkpoint had moved the disk on is removed on open.
	other := cp.importBytes(t, make([]byte, 64<<20))
	dir := filepath.Join(h.home, "disks", "moved")
	os.MkdirAll(dir, 0o700)
	old, err := epoch.Create(filepath.Join(dir, "1"), epoch.Super{Epoch: 1, Parent: snap0, Root: mustMeta(t, cp, snap0).DiskRoot, Size: 64 << 20, State: epoch.Uploading, Snapshot: &other})
	if err != nil {
		t.Fatal(err)
	}
	old.Close()
	top, err := epoch.Create(filepath.Join(dir, "2"), epoch.Super{Epoch: 2, Parent: other, Root: mustMeta(t, cp, other).DiskRoot, Size: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	top.Close()
	os.Mkdir(filepath.Join(dir, "3"), 0o700) // as a crash in epoch.Create leaves it
	open := func() (*diskState, error) {
		return openDisk(ctx, cp.c, cp.s, cp.s.Through(cp.c.Chunk), filepath.Join(h.home, "disks"), "moved", "", "", &bytes.Buffer{})
	}
	ds, err := open()
	if err != nil {
		t.Fatal(err)
	}
	if got := h.epochs(t, "moved"); !ds.base.Ref.Equal(other) || len(ds.disk.Frozen()) != 0 || !slices.Equal(got, []string{"2"}) {
		t.Errorf("the replaced or abandoned epoch was kept: base %s, %d frozen, epochs %v", ds.base.Ref, len(ds.disk.Frozen()), got)
	}
	if _, err := open(); err == nil || !strings.Contains(err.Error(), "disk moved is in use") {
		t.Errorf("a disk was opened twice: %v", err)
	}
	ds.close()
}

func mustParent(t *testing.T, cp *checkpointer, snap swarm.Address) swarm.Address {
	t.Helper()
	m := mustMeta(t, cp, snap)
	if m.Parent == nil {
		t.Fatalf("snapshot %s has no parent", snap)
	}
	return *m.Parent
}

// fakeAgent answers as a QEMU guest agent would, after bytes that an
// earlier client left, and notes the commands it gets.
type fakeAgent struct {
	failFreeze bool

	mu       sync.Mutex
	commands []string
}

func (a *fakeAgent) serve(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "qga.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go a.answer(conn)
		}
	}()
	return path
}

func (a *fakeAgent) answer(conn net.Conn) {
	defer conn.Close()
	reply := func(prefix string, v any) {
		b, _ := json.Marshal(v)
		conn.Write([]byte(prefix + string(b) + "\n"))
	}
	conn.Write([]byte(`{"return": 9`))           // cut short
	reply("\xff", map[string]any{"return": 999}) // another client's sync
	r := bufio.NewReader(conn)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		if i := bytes.LastIndexByte(line, 0xff); i >= 0 { // resets the parser
			line = line[i+1:]
		}
		var req struct {
			Execute   string
			Arguments struct{ ID int64 }
		}
		if err := json.Unmarshal(line, &req); err != nil {
			reply("", map[string]any{"error": map[string]string{"class": "GenericError", "desc": err.Error()}})
			continue
		}
		switch {
		case req.Execute == "guest-sync-delimited":
			reply("\xff", map[string]any{"return": req.Arguments.ID})
			continue
		case req.Execute == "guest-fsfreeze-freeze" && a.failFreeze:
			reply("", map[string]any{"error": map[string]string{"class": "GenericError", "desc": "failed to freeze /"}})
		case req.Execute == "guest-fsfreeze-freeze", req.Execute == "guest-fsfreeze-thaw":
			reply("", map[string]any{"return": 1})
		default:
			reply("", map[string]any{"error": map[string]string{"class": "CommandNotFound", "desc": "no " + req.Execute}})
		}
		a.mu.Lock()
		a.commands = append(a.commands, req.Execute)
		a.mu.Unlock()
	}
}

// got returns the commands the agent got and forgets them.
func (a *fakeAgent) got() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	c := a.commands
	a.commands = nil
	return c
}

func TestGuestAgent(t *testing.T) {
	ctx := context.Background()
	a := &fakeAgent{}
	path := a.serve(t)
	if err := guestFreeze(ctx, path); err != nil {
		t.Fatal(err)
	}
	if err := guestThaw(ctx, path); err != nil {
		t.Fatal(err)
	}
	if _, err := guestAgent(ctx, path, "guest-unknown"); err == nil || !strings.Contains(err.Error(), "CommandNotFound") {
		t.Errorf("an unknown command: %v", err)
	}
	if got := a.got(); !slices.Equal(got, []string{"guest-fsfreeze-freeze", "guest-fsfreeze-thaw", "guest-unknown"}) {
		t.Errorf("the guest agent got %v", got)
	}

	// A checkpoint with the guest agent is consistent for applications; one
	// whose freeze fails is consistent after a crash, and still thaws.
	needQEMU(t)
	cp, h := diskCheckpointer(t)
	snap0, _ := cp.importDisk(t)
	cfg := serveConfig{disk: "vm", arg: snap0.String(), disks: filepath.Join(h.home, "disks"), batch: testBatch, keys: cp.keys, qga: path}
	uri, stop := cp.startServeWith(t, cfg)
	defer stop()
	for i, c := range []struct {
		fail bool
		want string
	}{{false, "application"}, {true, "crash"}} {
		a.failFreeze = c.fail
		run(t, "qemu-io", "-f", "raw", "-c", fmt.Sprintf("write -P 0x%x 0 4k", 0x91+i), uri)
		snap, log, err := h.checkpoint(t, "vm")
		if err != nil {
			t.Fatalf("%v\n%s", err, log)
		}
		if m := mustMeta(t, cp, snap); m.Consistency != c.want {
			t.Errorf("consistency %q, want %s\n%s", m.Consistency, c.want, log)
		}
		if got := a.got(); !slices.Equal(got, []string{"guest-fsfreeze-freeze", "guest-fsfreeze-thaw"}) {
			t.Errorf("the guest agent got %v", got)
		}
	}
}
