package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/epoch"
	"github.com/petfold/swarmblk/internal/nbd"
	"github.com/petfold/swarmblk/internal/tree"
)

// newDisk opens the test disk with a new epoch on top. get supplies the
// snapshot's chunks, and local those at hand, if not nil.
func newDisk(t *testing.T, get, local storage.Getter) (*Disk, *epoch.Epoch, string, []byte) {
	t.Helper()
	img, root, _ := testDisk(t)
	snap, err := Open(context.Background(), get, root, 8)
	if err != nil {
		t.Fatal(err)
	}
	if local != nil {
		snap.SetLocal(local)
	}
	dir := filepath.Join(t.TempDir(), "1")
	ep, err := epoch.Create(dir, epoch.Super{Epoch: 1, Root: root, Size: snap.Size()})
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDisk(snap, ep)
	if err != nil {
		t.Fatal(err)
	}
	return d, ep, dir, slices.Clone(img)
}

// readAll reads the whole disk.
func readAll(t *testing.T, d *Disk) []byte {
	t.Helper()
	b := make([]byte, d.Size())
	if err := d.ReadAt(context.Background(), b, 0); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDiskWrites(t *testing.T) {
	_, _, s := testDisk(t)
	d, ep, dir, model := newDisk(t, s, s)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(5, 6))
	for i := range 400 {
		// Writes cluster in the first 8 MiB, over data and zeros alike.
		off := r.Int64N(8 << 20)
		n := 1 + r.Int64N(64<<10)
		if r.IntN(3) > 0 {
			off, n = off-off%512, max(512, n-n%512)
		}
		var err error
		switch r.IntN(6) {
		case 0:
			err = d.Trim(ctx, off, n, false)
			clear(model[off : off+n])
		case 1:
			err = d.WriteZeroes(ctx, off, n, r.IntN(2) == 0, false)
			clear(model[off : off+n])
		default:
			p := make([]byte, n)
			for j := range p {
				p[j] = byte(r.Uint32())
			}
			err = d.WriteAt(ctx, p, off, r.IntN(4) == 0)
			copy(model[off:], p)
		}
		if err != nil {
			t.Fatal(err)
		}
		if i%50 == 0 {
			if err := d.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, 9<<20)
			if err := d.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, model[:9<<20]) {
				t.Fatalf("after %d writes the disk reads different: %v", i+1, err)
			}
		}
	}
	if !bytes.Equal(readAll(t, d), model) {
		t.Fatal("the disk reads different from the model")
	}

	// The epoch survives closing and reopening.
	if err := ep.Close(); err != nil {
		t.Fatal(err)
	}
	ep, err := epoch.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Close()
	if d, err = NewDisk(d.Snapshot(), ep); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAll(t, d), model) {
		t.Fatal("the reopened disk reads different")
	}
}

// TestDiskPatch writes parts of blocks whose contents are not at hand: no
// write may fetch anything, and reads must apply the patches to the fetched
// contents. Once a block is at hand, a partial write merges with it.
func TestDiskPatch(t *testing.T) {
	_, _, s := testDisk(t)
	n := &network{Getter: s, gets: map[string]int{}}
	local := inmemchunkstore.New()
	d, _, _, model := newDisk(t, cached{n, local}, nil)
	ctx := context.Background()
	b := int64(4<<20/swarm.ChunkSize + 7) // in the random region
	_, before := n.stats()
	for _, w := range []struct {
		pos int
		p   string
	}{{0, "swarmblk"}, {512, "patch"}, {4090, "edge!!"}, {3, "over"}} {
		off := b*swarm.ChunkSize + int64(w.pos)
		if err := d.WriteAt(ctx, []byte(w.p), off, false); err != nil {
			t.Fatal(err)
		}
		copy(model[off:], w.p)
	}
	if _, after := n.stats(); after != before {
		t.Errorf("the partial writes fetched %d chunks", after-before)
	}
	if blk, _ := d.Active().Get(b); blk.Whole || len(blk.Patches) != 4 {
		t.Errorf("the block is %+v, want four patches", blk)
	}
	got := make([]byte, 3*swarm.ChunkSize)
	if err := d.ReadAt(ctx, got, (b-1)*swarm.ChunkSize); err != nil || !bytes.Equal(got, model[(b-1)*swarm.ChunkSize:(b+2)*swarm.ChunkSize]) {
		t.Fatalf("the patched block reads different: %v", err)
	}

	// The read left the block's base in the local store: with it at hand, a
	// partial write merges.
	d.Snapshot().SetLocal(local)
	off := b*swarm.ChunkSize + 1000
	if err := d.WriteAt(ctx, []byte("merged"), off, false); err != nil {
		t.Fatal(err)
	}
	copy(model[off:], "merged")
	if blk, _ := d.Active().Get(b); !blk.Whole || len(blk.Patches) != 0 {
		t.Errorf("after a local merge the block is %+v, want a whole block", blk)
	}
	// A zero subtree is at hand too, when its path is: 60 MiB is zeros
	// under the level-2 chunk that the reads above fetched.
	z := int64(60 << 20 / swarm.ChunkSize)
	if err := d.WriteAt(ctx, []byte("zero"), z*swarm.ChunkSize+8, false); err != nil {
		t.Fatal(err)
	}
	copy(model[z*swarm.ChunkSize+8:], "zero")
	if blk, _ := d.Active().Get(z); !blk.Whole {
		t.Errorf("a partial write over zeros left %+v", blk)
	}
	if !bytes.Equal(readAll(t, d), model) {
		t.Error("the disk reads different from the model")
	}
}

// TestDiskConcurrent writes every sector of a few blocks at once, from
// many goroutines, over contents at hand and not: none may be lost.
func TestDiskConcurrent(t *testing.T) {
	_, _, s := testDisk(t)
	for _, local := range []storage.Getter{nil, s} {
		d, _, _, model := newDisk(t, s, local)
		ctx := context.Background()
		var wg sync.WaitGroup
		for _, b := range []int64{0, 1030, 9000} { // data, random data, zeros
			for sector := range int64(8) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					p := bytes.Repeat([]byte{byte(b) ^ byte(sector+1)}, 512)
					if err := d.WriteAt(ctx, p, b*swarm.ChunkSize+sector*512, false); err != nil {
						t.Error(err)
					}
				}()
				copy(model[b*swarm.ChunkSize+sector*512:], bytes.Repeat([]byte{byte(b) ^ byte(sector+1)}, 512))
			}
		}
		wg.Wait()
		if !bytes.Equal(readAll(t, d), model) {
			t.Errorf("with local %v: concurrent partial writes were lost", local != nil)
		}
	}
}

func TestDiskExtents(t *testing.T) {
	_, _, s := testDisk(t)
	d, _, _, _ := newDisk(t, s, s)
	ctx := context.Background()
	b := int64(4 << 20 / swarm.ChunkSize) // the random region: data
	d.Trim(ctx, b*swarm.ChunkSize, 2*swarm.ChunkSize, false)
	d.WriteAt(ctx, bytes.Repeat([]byte{1}, swarm.ChunkSize), 9000*swarm.ChunkSize, false) // over zeros
	got, err := d.Extents(ctx, b*swarm.ChunkSize, 4*swarm.ChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	want := []nbd.Extent{{Length: 8192, Hole: true, Zero: true}, {Length: 8192}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("extents %v, want %v", got, want)
	}
	if got, err = d.Extents(ctx, 8999*swarm.ChunkSize, 3*swarm.ChunkSize); err != nil || fmt.Sprint(got) != fmt.Sprint([]nbd.Extent{{Length: 4096, Hole: true, Zero: true}, {Length: 4096}, {Length: 4096, Hole: true, Zero: true}}) {
		t.Errorf("extents around a block written over zeros: %v, %v", got, err)
	}
}

// TestDiskNBD writes to a Disk through QEMU's NBD client, with every kind
// of write, and reads it back.
func TestDiskNBD(t *testing.T) {
	for _, tool := range []string{"qemu-io", "qemu-img"} {
		if _, err := exec.LookPath(tool); err != nil {
			if os.Getenv("CI") != "" {
				t.Fatalf("%s is missing", tool)
			}
			t.Skipf("%s is missing", tool)
		}
	}
	_, _, s := testDisk(t)
	d, ep, _, model := newDisk(t, s, s)
	sock := filepath.Join(t.TempDir(), "nbd.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- (&nbd.Server{Exports: []nbd.Export{{Name: "disk", Device: d}}}).Serve(ctx, l) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	u := "nbd+unix:///disk?socket=" + sock
	out, err := exec.Command("qemu-io", "-f", "raw",
		"-c", "write -P 0x11 0 64k", "-c", "write -P 0x22 4M 4k", "-c", "write -P 0x33 1027k 2k",
		"-c", "write -P 0x44 100M 512", "-c", "write -z 4100k 16k", "-c", "discard 64k 64k",
		"-c", "write -f -P 0x55 8M 8k", "-c", "flush", "-c", "read -P 0x33 1027k 2k", u).CombinedOutput()
	if err != nil || strings.Contains(string(out), "fail") {
		t.Fatalf("qemu-io: %v\n%s", err, out)
	}
	set := func(off, n int64, v byte) { copy(model[off:], bytes.Repeat([]byte{v}, int(n))) }
	set(0, 64<<10, 0x11)
	set(4<<20, 4<<10, 0x22)
	set(1027<<10, 2<<10, 0x33)
	set(100<<20, 512, 0x44)
	set(4100<<10, 16<<10, 0)
	set(64<<10, 64<<10, 0)
	set(8<<20, 8<<10, 0x55)

	raw := filepath.Join(t.TempDir(), "disk.raw")
	start := time.Now()
	if out, err := exec.Command("qemu-img", "convert", "-f", "raw", "-O", "raw", u, raw).CombinedOutput(); err != nil {
		t.Fatalf("qemu-img convert: %v\n%s", err, out)
	}
	if got, err := os.ReadFile(raw); err != nil || !bytes.Equal(got, model) {
		t.Fatalf("QEMU reads other bytes than it wrote: %v", err)
	}
	t.Logf("read 128 MiB through NBD in %v; the epoch holds %d blocks", time.Since(start).Round(time.Millisecond), len(ep.Blocks()))
}

// TestFreezeCut freezes a disk while writers write two blocks each in turn,
// A then B, numbering their writes. The frozen epoch must be an atomic cut:
// in it a writer's A is its B or one ahead, never behind, and every write
// that returned before the cut is in.
func TestFreezeCut(t *testing.T) {
	_, _, s := testDisk(t)
	d, _, dir, _ := newDisk(t, s, s)
	ctx := context.Background()
	const writers = 8
	block := func(w, i int64) int64 { return 20000 + w*2 + i } // zeros in the image
	write := func(w, i int64, seq uint64) error {
		p := make([]byte, swarm.ChunkSize)
		binary.LittleEndian.PutUint64(p, seq)
		return d.WriteAt(ctx, p, block(w, i)*swarm.ChunkSize, false)
	}
	var acked [writers]atomic.Uint64 // the last write of B that returned
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range int64(writers) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := uint64(1); ; seq++ {
				select {
				case <-stop:
					return
				default:
				}
				if write(w, 0, seq) != nil || write(w, 1, seq) != nil {
					t.Error("a write failed")
					return
				}
				acked[w].Store(seq)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	var before [writers]uint64
	for w := range writers {
		before[w] = acked[w].Load()
	}
	next, err := epoch.Create(filepath.Join(filepath.Dir(dir), "2"), epoch.Super{Epoch: 2, Size: d.Size()})
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := d.Freeze(next)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	if s := frozen.Super(); s.State != epoch.Frozen {
		t.Errorf("the frozen epoch's state is %q", s.State)
	}
	seen := map[int64]uint64{}
	if err := d.Blocks(ctx, frozen, 1000, func(m map[int64][]byte) error {
		for b, p := range m {
			seen[b] = binary.LittleEndian.Uint64(p)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for w := range int64(writers) {
		a, b := seen[block(w, 0)], seen[block(w, 1)]
		if a != b && a != b+1 || b < before[w] {
			t.Errorf("writer %d: A %d and B %d in the cut, B %d acknowledged before it", w, a, b, before[w])
		}
		if final := readAll(t, d)[block(w, 1)*swarm.ChunkSize:]; binary.LittleEndian.Uint64(final) < b {
			t.Errorf("writer %d: the disk went back after the cut", w)
		}
	}
	if _, err := d.Freeze(next); err == nil {
		t.Error("a second Freeze before the checkpoint finished was accepted")
	}
}

// TestFreezePromote freezes a disk with whole blocks, zeros and patches,
// rebuilds the frozen blocks into a new tree with Bee's code, and promotes
// it: the disk must read the same before and after, and take writes.
func TestFreezePromote(t *testing.T) {
	_, root, s := testDisk(t)
	ctx := context.Background()
	store := inmemchunkstore.New()
	both := storage.GetterFunc(func(ctx context.Context, a swarm.Address) (swarm.Chunk, error) {
		if ch, err := store.Get(ctx, a); err == nil {
			return ch, nil
		}
		return s.Get(ctx, a)
	})
	d, _, dir, model := newDisk(t, both, nil) // no contents at hand: partial writes stay patches
	for _, w := range []struct {
		off int64
		p   []byte
	}{
		{0, bytes.Repeat([]byte{1}, 8192)},
		{4<<20 + 100, []byte("a patch over data")},
		{30 << 20, make([]byte, 4096)},
		{60<<20 + 7, []byte("a patch over zeros")},
	} {
		if err := d.WriteAt(ctx, w.p, w.off, false); err != nil {
			t.Fatal(err)
		}
		copy(model[w.off:], w.p)
	}
	next, err := epoch.Create(filepath.Join(filepath.Dir(dir), "2"), epoch.Super{Epoch: 2, Size: d.Size()})
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := d.Freeze(next)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.WriteAt(ctx, []byte("after the cut"), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	copy(model[1<<20:], "after the cut")

	newRoot := root
	if err := d.Blocks(ctx, frozen, 2, func(blocks map[int64][]byte) error {
		r, chunks, err := tree.Rebuild(ctx, both, newRoot, blocks)
		for _, ch := range chunks {
			store.Put(ctx, ch)
		}
		newRoot = r
		return err
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := Open(ctx, both, newRoot, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Promote(snap, frozen); err != nil {
		t.Fatal(err)
	}
	if len(d.Frozen()) != 0 || d.Snapshot() != snap {
		t.Error("Promote kept the frozen epoch or the old snapshot")
	}
	if !bytes.Equal(readAll(t, d), model) {
		t.Fatal("the disk reads different after Promote")
	}
	// The new snapshot alone holds the disk as it was at the cut.
	atCut := slices.Clone(model)
	copy(atCut[1<<20:], readSnap(t, both, root)[1<<20:1<<20+13])
	got := make([]byte, snap.Size())
	if err := snap.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, atCut) {
		t.Errorf("the promoted snapshot differs from the disk at the cut: %v", err)
	}
	if err := d.WriteAt(ctx, []byte("on top"), 2<<20, true); err != nil {
		t.Fatal(err)
	}
	if err := d.Promote(snap, frozen); err == nil {
		t.Error("a second Promote was accepted")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

// readSnap reads a whole snapshot.
func readSnap(t *testing.T, get storage.Getter, root swarm.Address) []byte {
	t.Helper()
	snap, err := Open(context.Background(), get, root, 8)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, snap.Size())
	if err := snap.ReadAt(context.Background(), b, 0); err != nil {
		t.Fatal(err)
	}
	return b
}

// TestPromoteRead promotes twice while a read waits for the snapshot under
// a patch in the first epoch: the epoch must stay open until the read ends,
// and the read must return what the disk held when it started.
func TestPromoteRead(t *testing.T) {
	_, root, s := testDisk(t)
	ctx := context.Background()
	store := inmemchunkstore.New()
	var slow atomic.Pointer[swarm.Address] // the next fetch of it waits
	waiting, release := make(chan struct{}), make(chan struct{})
	get := storage.GetterFunc(func(ctx context.Context, a swarm.Address) (swarm.Chunk, error) {
		if p := slow.Load(); p != nil && p.Equal(a) && slow.CompareAndSwap(p, nil) {
			close(waiting)
			<-release
		}
		if ch, err := store.Get(ctx, a); err == nil {
			return ch, nil
		}
		return s.Get(ctx, a)
	})
	d, first, dir, model := newDisk(t, get, nil)
	const b = 1030 // random data
	off := int64(b*swarm.ChunkSize + 10)
	if err := d.WriteAt(ctx, []byte("a patch"), off, false); err != nil {
		t.Fatal(err)
	}
	copy(model[off:], "a patch")
	want := slices.Clone(model[b*swarm.ChunkSize : (b+1)*swarm.ChunkSize])
	leaf, _, err := d.Snapshot().leaf(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	slow.Store(&leaf)
	got := make([]byte, swarm.ChunkSize)
	read := make(chan error)
	go func() { read <- d.ReadAt(ctx, got, b*swarm.ChunkSize) }()
	<-waiting

	for n := uint64(2); n <= 3; n++ {
		next, err := epoch.Create(filepath.Join(filepath.Dir(dir), fmt.Sprint(n)), epoch.Super{Epoch: n, Size: d.Size()})
		if err != nil {
			t.Fatal(err)
		}
		frozen, err := d.Freeze(next)
		if err != nil {
			t.Fatal(err)
		}
		blocks := map[int64][]byte{}
		for _, blk := range frozen.Blocks() {
			blocks[blk] = model[blk*swarm.ChunkSize : (blk+1)*swarm.ChunkSize]
		}
		r, chunks, err := tree.Rebuild(ctx, get, root, blocks)
		if err != nil {
			t.Fatal(err)
		}
		for _, ch := range chunks {
			store.Put(ctx, ch)
		}
		root = r
		snap, err := Open(ctx, get, root, 8)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Promote(snap, frozen); err != nil {
			t.Fatal(err)
		}
		w := bytes.Repeat([]byte{byte(n)}, swarm.ChunkSize)
		if err := d.WriteAt(ctx, w, 8*swarm.ChunkSize, false); err != nil {
			t.Fatal(err)
		}
		copy(model[8*swarm.ChunkSize:], w)
	}
	if err := first.Sync(); err != nil {
		t.Errorf("the first epoch was closed while a read used it: %v", err)
	}
	close(release)
	if err := <-read; err != nil || !bytes.Equal(got, want) {
		t.Errorf("a read across two promotes: %v, same bytes %v", err, bytes.Equal(got, want))
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Sync(); err == nil {
		t.Error("the first epoch is still open after the disk closed")
	}
}
