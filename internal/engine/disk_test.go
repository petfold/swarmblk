package engine

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/epoch"
	"github.com/petfold/swarmblk/internal/nbd"
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
	if d, err = NewDisk(d.snap, ep); err != nil {
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
	if blk, _ := d.ep.Get(b); blk.Whole || len(blk.Patches) != 4 {
		t.Errorf("the block is %+v, want four patches", blk)
	}
	got := make([]byte, 3*swarm.ChunkSize)
	if err := d.ReadAt(ctx, got, (b-1)*swarm.ChunkSize); err != nil || !bytes.Equal(got, model[(b-1)*swarm.ChunkSize:(b+2)*swarm.ChunkSize]) {
		t.Fatalf("the patched block reads different: %v", err)
	}

	// The read left the block's base in the local store: with it at hand, a
	// partial write merges.
	d.snap.SetLocal(local)
	off := b*swarm.ChunkSize + 1000
	if err := d.WriteAt(ctx, []byte("merged"), off, false); err != nil {
		t.Fatal(err)
	}
	copy(model[off:], "merged")
	if blk, _ := d.ep.Get(b); !blk.Whole || len(blk.Patches) != 0 {
		t.Errorf("after a local merge the block is %+v, want a whole block", blk)
	}
	// A zero subtree is at hand too, when its path is: 60 MiB is zeros
	// under the level-2 chunk that the reads above fetched.
	z := int64(60 << 20 / swarm.ChunkSize)
	if err := d.WriteAt(ctx, []byte("zero"), z*swarm.ChunkSize+8, false); err != nil {
		t.Fatal(err)
	}
	copy(model[z*swarm.ChunkSize+8:], "zero")
	if blk, _ := d.ep.Get(z); !blk.Whole {
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
