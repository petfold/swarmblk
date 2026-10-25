// Package tree describes the chunk tree of a format-1 disk image: its geometry
// (SPEC §2) and its zero subtrees (SPEC §3). Chunk addresses always come from
// Bee's pkg/cac.
package tree

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"sync"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// Format 1 allows disks of 2^MinShift to 2^MaxShift bytes (64 MiB to 1 TiB).
const (
	MinShift = 26
	MaxShift = 40
)

// Geometry is the shape of a disk's tree. Leaves are level 0 and the root is
// level Levels.
type Geometry struct {
	Size   int64 // S, the disk size in bytes
	Levels int   // L, the number of intermediate levels, root included
	Fanout int   // F, the number of references in the root
}

// New returns the geometry of a disk of size bytes, which must be a power of
// two from 2^MinShift to 2^MaxShift.
func New(size int64) (Geometry, error) {
	if size <= 0 || size&(size-1) != 0 {
		return Geometry{}, fmt.Errorf("disk size %d is not a power of two", size)
	}
	s := bits.TrailingZeros64(uint64(size))
	if s < MinShift || s > MaxShift {
		return Geometry{}, fmt.Errorf("disk size 2^%d is outside 2^%d to 2^%d bytes", s, MinShift, MaxShift)
	}
	m := s - 12 // the leaf count is 2^m
	levels := (m + 6) / 7
	return Geometry{Size: size, Levels: levels, Fanout: 1 << (m - 7*(levels-1))}, nil
}

// Leaves returns the number of 4 KiB leaves, N.
func (g Geometry) Leaves() int64 { return g.Size / swarm.ChunkSize }

// Span returns the span of a chunk at level j: 4096·128^j below the root and
// the disk size at the root.
func (g Geometry) Span(j int) int64 {
	if j == g.Levels {
		return g.Size
	}
	return swarm.ChunkSize << (7 * j)
}

// Refs returns the number of references in a chunk at level j ≥ 1: 128 below
// the root and F at the root.
func (g Geometry) Refs(j int) int {
	if j == g.Levels {
		return g.Fanout
	}
	return swarm.Branches
}

// Index returns the number, within level j, of the chunk on leaf b's path
// (SPEC §2): floor(b / 128^j). Level 0 is the leaf itself, and the root is
// always number 0.
func (g Geometry) Index(b int64, j int) int64 { return b >> (7 * j) }

// Slot returns which reference of the level-j chunk (j ≥ 1) on leaf b's path
// leads towards b (SPEC §2): floor(b / 128^(j−1)) mod 128 below the root, and
// floor(b / 128^(L−1)) at the root.
func (g Geometry) Slot(b int64, j int) int {
	if j == g.Levels {
		return int(g.Index(b, j-1))
	}
	return int(g.Index(b, j-1) % swarm.Branches)
}

// Root reads a disk's geometry from the span of its root chunk and returns it
// with the root's references.
func Root(ch swarm.Chunk) (Geometry, []swarm.Address, error) {
	data := ch.Data()
	if len(data) < swarm.SpanSize {
		return Geometry{}, nil, fmt.Errorf("root %s is shorter than a span", ch.Address())
	}
	if redundancy.IsLevelEncoded(data[:swarm.SpanSize]) {
		return Geometry{}, nil, fmt.Errorf("root %s has a redundancy level in its span; format 1 needs uploads with Swarm-Redundancy-Level: 0", ch.Address())
	}
	g, err := New(int64(binary.LittleEndian.Uint64(data)))
	if err != nil {
		return Geometry{}, nil, fmt.Errorf("root %s: %w", ch.Address(), err)
	}
	refs, err := g.Children(g.Levels, ch)
	return g, refs, err
}

// Children returns the references in ch, a chunk at level j ≥ 1, after
// checking its span and length against g. The references share ch's data.
func (g Geometry) Children(j int, ch swarm.Chunk) ([]swarm.Address, error) {
	data := ch.Data()
	n := g.Refs(j)
	if len(data) != swarm.SpanSize+n*swarm.HashSize {
		return nil, fmt.Errorf("level-%d chunk %s holds %d bytes, want %d references", j, ch.Address(), len(data)-swarm.SpanSize, n)
	}
	if span := binary.LittleEndian.Uint64(data); span != uint64(g.Span(j)) {
		return nil, fmt.Errorf("level-%d chunk %s has span %d, want %d", j, ch.Address(), span, g.Span(j))
	}
	refs := make([]swarm.Address, n)
	for i := range refs {
		off := swarm.SpanSize + i*swarm.HashSize
		refs[i] = swarm.NewAddress(data[off : off+swarm.HashSize])
	}
	return refs, nil
}

// Zero returns Z_j, the address of an all-zero subtree at level j, for
// 0 ≤ j ≤ 3: every level below the root of a format-1 disk.
func Zero(j int) swarm.Address { return zeros()[j] }

var zeros = sync.OnceValue(func() (z [4]swarm.Address) {
	data := make([]byte, swarm.SpanSize+swarm.ChunkSize) // Z_0's payload is 4096 zero bytes
	for j := range z {
		binary.LittleEndian.PutUint64(data, swarm.ChunkSize<<(7*j))
		for i := 0; j > 0 && i < swarm.Branches; i++ {
			copy(data[swarm.SpanSize+i*swarm.HashSize:], z[j-1].Bytes())
		}
		ch, err := cac.NewWithDataSpan(data)
		if err != nil {
			panic(err) // data always holds a span and one chunk's payload
		}
		z[j] = ch.Address()
	}
	return z
})
