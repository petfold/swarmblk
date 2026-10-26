package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/nbd"
	"github.com/petfold/swarmblk/internal/tree"
)

// testDisk returns a 128 MiB image and its root and chunks, built once with
// Bee's pipeline: a random 1 MiB region, random blocks scattered over the
// first 32 MiB, one block repeated, and the rest zeros. Tests only read
// them.
func testDisk(t *testing.T) ([]byte, swarm.Address, *inmemchunkstore.ChunkStore) {
	t.Helper()
	d, err := disk()
	if err != nil {
		t.Fatal(err)
	}
	return d.img, d.root, d.store
}

var disk = sync.OnceValues(func() (struct {
	img   []byte
	root  swarm.Address
	store *inmemchunkstore.ChunkStore
}, error) {
	img := make([]byte, 128<<20)
	src := rand.NewChaCha8([32]byte{11})
	r := rand.New(src)
	src.Read(img[4<<20 : 5<<20])
	for range 40 {
		b := r.IntN(32<<20/swarm.ChunkSize) * swarm.ChunkSize
		src.Read(img[b : b+swarm.ChunkSize])
	}
	for _, b := range []int{10, 2000, 30000} {
		copy(img[b*swarm.ChunkSize:], bytes.Repeat([]byte("swarmblk"), swarm.ChunkSize/8))
	}
	ctx := context.Background()
	s := inmemchunkstore.New()
	root, err := builder.FeedPipeline(ctx, builder.NewPipelineBuilder(ctx, s, false, redundancy.NONE), bytes.NewReader(img))
	return struct {
		img   []byte
		root  swarm.Address
		store *inmemchunkstore.ChunkStore
	}{img, root, s}, err
})

// counter counts the gets of each chunk, and refuses those of zero
// subtrees and zero leaves, which a Snapshot must never fetch.
type counter struct {
	*inmemchunkstore.ChunkStore
	fail swarm.Address // a chunk to fail, if not zero

	mu   sync.Mutex
	gets map[string]int
}

func (c *counter) Get(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	for j := range 4 {
		if addr.Equal(tree.Zero(j)) {
			return nil, fmt.Errorf("fetched Z_%d", j)
		}
	}
	if addr.Equal(c.fail) {
		return nil, errors.New("unavailable")
	}
	c.mu.Lock()
	c.gets[addr.ByteString()]++
	c.mu.Unlock()
	return c.ChunkStore.Get(ctx, addr)
}

func open(t *testing.T) ([]byte, *Snapshot, *counter) {
	t.Helper()
	img, root, s := testDisk(t)
	c := &counter{ChunkStore: s, gets: map[string]int{}}
	snap, err := Open(context.Background(), c, root, 8)
	if err != nil {
		t.Fatal(err)
	}
	return img, snap, c
}

func TestReadAt(t *testing.T) {
	img, snap, c := open(t)
	if snap.Size() != int64(len(img)) {
		t.Fatalf("size %d, want %d", snap.Size(), len(img))
	}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 1))
			for range 200 {
				off := r.Int64N(int64(len(img)) - 1)
				n := 1 + r.Int64N(min(int64(len(img))-off, 2<<20))
				if r.IntN(2) == 0 { // sector-aligned, as most clients send
					off -= off % 512
					n = max(512, n-n%512)
					n = min(n, int64(len(img))-off)
				}
				p := make([]byte, n)
				if err := snap.ReadAt(context.Background(), p, off); err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(p, img[off:off+n]) {
					t.Errorf("%d bytes from %d read back different", n, off)
					return
				}
			}
		}()
	}
	wg.Wait()

	all := make([]byte, len(img))
	if err := snap.ReadAt(context.Background(), all, 0); err != nil || !bytes.Equal(all, img) {
		t.Fatalf("the whole disk reads back different: %v", err)
	}
	for k, n := range c.gets {
		ch, _ := c.ChunkStore.Get(context.Background(), swarm.NewAddress([]byte(k)))
		if binary.LittleEndian.Uint64(ch.Data()) > swarm.ChunkSize && n != 1 {
			t.Errorf("intermediate chunk %x fetched %d times", k, n)
		}
	}
	for _, bad := range []struct{ off, n int64 }{{-512, 512}, {int64(len(img)) - 512, 1024}} {
		if err := snap.ReadAt(context.Background(), make([]byte, bad.n), bad.off); err == nil {
			t.Errorf("read of %d bytes from %d accepted", bad.n, bad.off)
		}
	}
	if err := snap.WriteAt(context.Background(), make([]byte, 512), 0, false); !errors.Is(err, nbd.EPERM) {
		t.Errorf("write: %v, want EPERM", err)
	}
}

// zeroRuns describes img in the base:allocation context, as Extents should:
// zero blocks are holes, the rest data.
func zeroRuns(img []byte, off, end int64) []nbd.Extent {
	var out []nbd.Extent
	zero := make([]byte, swarm.ChunkSize)
	for pos := off; pos < end; {
		first := pos - pos%swarm.ChunkSize
		next := min(first+swarm.ChunkSize, end)
		hole := bytes.Equal(img[first:first+swarm.ChunkSize], zero)
		if k := len(out); k > 0 && out[k-1].Hole == hole {
			out[k-1].Length += next - pos
		} else {
			out = append(out, nbd.Extent{Length: next - pos, Hole: hole, Zero: hole})
		}
		pos = next
	}
	return out
}

func TestExtents(t *testing.T) {
	img, snap, c := open(t)
	for _, r := range []struct{ off, n int64 }{{0, int64(len(img))}, {512, 3 << 20}, {4<<20 - 1024, 2 << 20}, {64 << 20, 64 << 20}} {
		got, err := snap.Extents(context.Background(), r.off, r.n)
		if err != nil {
			t.Fatal(err)
		}
		if want := zeroRuns(img, r.off, r.off+r.n); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("extents of %d bytes from %d:\n%v\nwant\n%v", r.n, r.off, got, want)
		}
	}
	for k := range c.gets {
		if ch, _ := c.ChunkStore.Get(context.Background(), swarm.NewAddress([]byte(k))); binary.LittleEndian.Uint64(ch.Data()) == swarm.ChunkSize {
			t.Fatal("block status fetched a leaf")
		}
	}
}

func TestUnavailable(t *testing.T) {
	img, root, s := testDisk(t)
	ctx := context.Background()
	// A level-1 chunk over the random region, and a leaf in it.
	full, err := Open(ctx, s, root, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := int64(4<<20/swarm.ChunkSize + 100)
	leaf, _, err := full.leaf(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	refs := full.root
	for j := full.g.Levels; j > 2; j-- {
		refs, _ = full.node(ctx, j-1, refs[full.g.Slot(b, j)])
	}
	level1 := refs[full.g.Slot(b, 2)]

	c := &counter{ChunkStore: s, fail: leaf, gets: map[string]int{}}
	snap, err := Open(ctx, c, root, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := snap.ReadAt(ctx, make([]byte, 8192), b*swarm.ChunkSize); err == nil {
		t.Error("a read of an unavailable leaf succeeded")
	}
	p := make([]byte, 4096)
	if err := snap.ReadAt(ctx, p, (b+1)*swarm.ChunkSize); err != nil || !bytes.Equal(p, img[(b+1)*swarm.ChunkSize:(b+2)*swarm.ChunkSize]) {
		t.Errorf("the next block: %v", err)
	}

	c = &counter{ChunkStore: s, fail: level1, gets: map[string]int{}}
	if snap, err = Open(ctx, c, root, 0); err != nil {
		t.Fatal(err)
	}
	from := int64(0) // up to the unavailable level-1 chunk, block status answers
	got, err := snap.Extents(ctx, from, b*swarm.ChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	var covered int64
	for _, e := range got {
		covered += e.Length
	}
	if want := (b - b%swarm.Branches) * swarm.ChunkSize; covered != want {
		t.Errorf("block status covered %d bytes before the unavailable chunk, want %d", covered, want)
	}
	if _, err := snap.Extents(ctx, b*swarm.ChunkSize, 4096); err == nil {
		t.Error("block status of an unavailable chunk succeeded")
	}
}

// TestNBD serves a Snapshot over NBD to QEMU's client.
func TestNBD(t *testing.T) {
	for _, tool := range []string{"qemu-img"} {
		if _, err := exec.LookPath(tool); err != nil {
			if os.Getenv("CI") != "" {
				t.Fatalf("%s is missing", tool)
			}
			t.Skipf("%s is missing", tool)
		}
	}
	img, snap, _ := open(t)
	sock := filepath.Join(t.TempDir(), "nbd.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		done <- (&nbd.Server{Exports: []nbd.Export{{Name: "snap", Device: snap, ReadOnly: true}}}).Serve(ctx, l)
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	u := "nbd+unix:///snap?socket=" + sock

	raw := filepath.Join(t.TempDir(), "disk.raw")
	if out, err := exec.Command("qemu-img", "convert", "-f", "raw", "-O", "raw", u, raw).CombinedOutput(); err != nil {
		t.Fatalf("qemu-img convert: %v\n%s", err, out)
	}
	if got, err := os.ReadFile(raw); err != nil || !bytes.Equal(got, img) {
		t.Fatalf("QEMU read other bytes: %v", err)
	}
	out, err := exec.Command("qemu-img", "map", "-f", "raw", "--output=json", u).Output()
	if err != nil {
		t.Fatal(err)
	}
	var extents []struct {
		Start, Length int64
		Data, Zero    bool
	}
	if err := json.Unmarshal(out, &extents); err != nil {
		t.Fatal(err)
	}
	var got []nbd.Extent
	for _, e := range extents {
		got = append(got, nbd.Extent{Length: e.Length, Hole: !e.Data, Zero: e.Zero})
	}
	if want := zeroRuns(img, 0, int64(len(img))); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("qemu-img map:\n%v\nwant\n%v", got, want)
	}
}
