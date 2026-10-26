package epoch

import (
	"bytes"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

const blocks = 16

func testSuper() Super {
	return Super{Epoch: 1, Parent: swarm.NewAddress(bytes.Repeat([]byte{1}, 32)), Root: swarm.NewAddress(bytes.Repeat([]byte{2}, 32)), Size: blocks * swarm.ChunkSize}
}

// op is one write to an epoch, and to a model of the disk.
type op struct {
	kind  byte
	block int64
	pos   int
	data  []byte
}

func (o op) do(e *Epoch) error {
	switch o.kind {
	case kindBlock:
		return e.Put(o.block, o.data)
	case kindZero:
		return e.Zero(o.block)
	default:
		return e.Patch(o.block, o.pos, o.data)
	}
}

func (o op) model(disk []byte) {
	blk := disk[o.block*swarm.ChunkSize : (o.block+1)*swarm.ChunkSize]
	switch o.kind {
	case kindBlock:
		copy(blk, o.data)
	case kindZero:
		clear(blk)
	default:
		copy(blk[o.pos:], o.data)
	}
}

// ops returns n random writes: whole blocks, zeros and patches.
func ops(n int) []op {
	r := rand.New(rand.NewPCG(1, 2))
	var out []op
	for range n {
		o := op{kind: byte(1 + r.IntN(3)), block: r.Int64N(blocks)}
		switch o.kind {
		case kindBlock:
			o.data = make([]byte, swarm.ChunkSize)
		case kindPatch:
			o.pos = r.IntN(swarm.ChunkSize - 1)
			o.data = make([]byte, 1+r.IntN(swarm.ChunkSize-o.pos))
		}
		for i := range o.data {
			o.data[i] = byte(r.Uint32())
		}
		out = append(out, o)
	}
	return out
}

// base is the disk underneath the epoch.
func base() []byte {
	b := make([]byte, blocks*swarm.ChunkSize)
	for i := range b {
		b[i] = byte(i / swarm.ChunkSize)
	}
	return b
}

// contents reads every block through the epoch, over base.
func contents(t *testing.T, e *Epoch) []byte {
	t.Helper()
	under := base()
	out := slices.Clone(under)
	for _, b := range e.Blocks() {
		blk, _ := e.Get(b)
		data, err := e.Read(blk, under[b*swarm.ChunkSize:(b+1)*swarm.ChunkSize])
		if err != nil {
			t.Fatal(err)
		}
		copy(out[b*swarm.ChunkSize:], data)
	}
	return out
}

func TestEpoch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "1")
	e, err := Create(dir, testSuper())
	if err != nil {
		t.Fatal(err)
	}
	model := base()
	for _, o := range ops(200) {
		if err := o.do(e); err != nil {
			t.Fatal(err)
		}
		o.model(model)
	}
	if !bytes.Equal(contents(t, e), model) {
		t.Fatal("the epoch reads different from the model")
	}
	if err := e.SetState(Frozen); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	if e, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := Open(dir); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Errorf("an epoch was opened twice: %v", err)
	}
	if s := e.Super(); s.State != Frozen || s.Format != Format || s.Size != blocks*swarm.ChunkSize || !s.Parent.Equal(testSuper().Parent) {
		t.Errorf("superblock %+v", s)
	}
	if !bytes.Equal(contents(t, e), model) {
		t.Fatal("the reopened epoch reads different")
	}
	for name, err := range map[string]error{
		"a short block":  e.Put(0, make([]byte, 100)),
		"outside":        e.Put(blocks, make([]byte, swarm.ChunkSize)),
		"an empty patch": e.Patch(0, 0, nil),
		"a long patch":   e.Patch(0, 4000, make([]byte, 100)),
	} {
		if err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if _, err := Create(dir, testSuper()); err == nil {
		t.Error("Create replaced an epoch")
	}
}

// TestRecovery stands for crashes: it rebuilds the files as a crash could
// leave them, cut at any point or with the last record's data damaged.
// Recovery must keep exactly the writes before the cut, and the epoch must
// take new writes after it.
func TestRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "1")
	e, err := Create(dir, testSuper())
	if err != nil {
		t.Fatal(err)
	}
	all := ops(60)
	var dataEnd []int64 // the data file's length after each write
	for _, o := range all {
		if err := o.do(e); err != nil {
			t.Fatal(err)
		}
		dataEnd = append(dataEnd, e.end)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, dataFile))
	index, _ := os.ReadFile(filepath.Join(dir, indexFile))
	super, _ := os.ReadFile(filepath.Join(dir, superFile))

	crash := func(name string, index, data []byte, kept int) {
		t.Helper()
		d := filepath.Join(t.TempDir(), "1")
		os.Mkdir(d, 0o700)
		os.WriteFile(filepath.Join(d, superFile), super, 0o600)
		os.WriteFile(filepath.Join(d, dataFile), data, 0o600)
		os.WriteFile(filepath.Join(d, indexFile), index, 0o600)
		e, err := Open(d)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		defer e.Close()
		model := base()
		for _, o := range all[:kept] {
			o.model(model)
		}
		if !bytes.Equal(contents(t, e), model) || e.count != int64(kept) {
			t.Fatalf("%s: recovered %d records, want %d, or other contents", name, e.count, kept)
		}
		more := op{kind: kindBlock, block: 3, data: bytes.Repeat([]byte{0xee}, swarm.ChunkSize)}
		if err := more.do(e); err != nil {
			t.Fatal(err)
		}
		more.model(model)
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		if e, err = Open(d); err != nil || !bytes.Equal(contents(t, e), model) {
			t.Fatalf("%s: a write after recovery did not survive: %v", name, err)
		}
	}
	for k := 0; k <= len(all); k++ {
		end := int64(0)
		if k > 0 {
			end = dataEnd[k-1]
		}
		// Cut after k records: in the index alone, or in both files.
		crash("index cut", index[:k*recordSize], data, k)
		crash("both cut", index[:k*recordSize], data[:end], k)
		if k < len(all) {
			// A torn record, its data written or not.
			crash("torn record", index[:k*recordSize+recordSize/2], data, k)
			kept := k // a zero record needs no data, so it survives
			if all[k].kind == kindZero {
				kept++
			}
			crash("data missing", index[:(k+1)*recordSize], data[:end], kept)
			if dataEnd[k] > end {
				damaged := slices.Clone(data[:dataEnd[k]])
				damaged[dataEnd[k]-1] ^= 0xff
				crash("data damaged", index[:(k+1)*recordSize], damaged, k)
			}
		}
	}
	// A damaged record in the middle drops everything after it.
	bad := slices.Clone(index)
	bad[10*recordSize+13] ^= 1
	crash("record damaged", bad, data, 10)
}
