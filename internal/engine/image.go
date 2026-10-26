package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/nbd"
)

// Image is a local copy of a disk, for the hydrate policy (DESIGN §3): a
// sparse file of the disk's size, a map of the blocks it holds, and the
// root of the disk tree they come from. Reads put the blocks they fetch in
// it, and Hydrate fetches the rest, so that a complete image needs no
// network. Blocks fetched for any other root, or while Advance moves the
// image to the next one, are dropped, so the file never mixes two disks. A
// block's data is synced before the map says it is there, so after a crash
// the map may lack blocks the file holds, but never the other way round.
type Image struct {
	dir    string
	data   *os.File
	mapf   *os.File
	blocks int64

	mu      sync.RWMutex
	root    swarm.Address // the disk tree held
	moving  bool          // Advance is moving the image to the next root
	present []byte        // a bit per block
	dirty   map[int]bool  // bytes of present changed since the last Sync
	count   int64         // blocks present
	written bool          // data written since the last Sync
}

// Files in an image's directory.
const (
	imageData = "disk.raw"
	imageMap  = "present"
	imageRoot = "root"
)

// OpenImage opens the image in dir for a disk of size bytes, and makes it
// if there is none. A new image holds no blocks and no root.
func OpenImage(dir string, size int64) (*Image, error) {
	if size <= 0 || size%swarm.ChunkSize != 0 {
		return nil, fmt.Errorf("image: disk size %d", size)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	m := &Image{dir: dir, blocks: size / swarm.ChunkSize, dirty: map[int]bool{}}
	m.present = make([]byte, (m.blocks+7)/8)
	var err error
	if m.data, err = os.OpenFile(filepath.Join(dir, imageData), os.O_RDWR|os.O_CREATE, 0o600); err != nil {
		return nil, err
	}
	if m.mapf, err = os.OpenFile(filepath.Join(dir, imageMap), os.O_RDWR|os.O_CREATE, 0o600); err != nil {
		m.data.Close()
		return nil, err
	}
	fail := func(err error) (*Image, error) {
		m.data.Close()
		m.mapf.Close()
		return nil, err
	}
	b, err := os.ReadFile(filepath.Join(dir, imageRoot))
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return fail(err)
	default:
		if m.root, err = swarm.ParseHexAddress(strings.TrimSpace(string(b))); err != nil {
			return fail(fmt.Errorf("image %s: root: %w", dir, err))
		}
	}
	fi, err := m.data.Stat()
	if err != nil {
		return fail(err)
	}
	if fi.Size() != size { // new
		if err := m.reset(swarm.ZeroAddress); err != nil {
			return fail(err)
		}
		return m, nil
	}
	if _, err := m.mapf.ReadAt(m.present, 0); err != nil && !errors.Is(err, io.EOF) {
		return fail(err)
	}
	for _, x := range m.present {
		m.count += int64(bits.OnesCount8(x))
	}
	return m, nil
}

// Root returns the root of the disk tree the image holds.
func (m *Image) Root() swarm.Address {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.root
}

// Present returns how many of the disk's blocks the image holds, and how
// many the disk has.
func (m *Image) Present() (int64, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count, m.blocks
}

// Has reports whether the image holds block b.
func (m *Image) Has(b int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.has(b)
}

func (m *Image) has(b int64) bool { return m.present[b/8]&(1<<(b%8)) != 0 }

// set marks block b present. m.mu must be held.
func (m *Image) set(b int64) {
	if !m.has(b) {
		m.present[b/8] |= 1 << (b % 8)
		m.dirty[int(b/8)] = true
		m.count++
	}
}

// Block returns block b's contents if the image holds it.
func (m *Image) Block(b int64) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if b < 0 || b >= m.blocks || !m.has(b) {
		return nil, false
	}
	p := make([]byte, swarm.ChunkSize)
	if _, err := m.data.ReadAt(p, b*swarm.ChunkSize); err != nil {
		return nil, false
	}
	return p, true
}

var zeroBlock = make([]byte, swarm.ChunkSize)

// write puts data at block b, or makes it a hole if data is nil or zeros,
// and marks it present. m.mu must be held.
func (m *Image) write(b int64, data []byte) error {
	var err error
	if data == nil || bytes.Equal(data, zeroBlock) {
		err = punch(m.data, b*swarm.ChunkSize, swarm.ChunkSize)
	} else {
		_, err = m.data.WriteAt(data, b*swarm.ChunkSize)
	}
	if err != nil {
		return err
	}
	m.written = true
	m.set(b)
	return nil
}

// Put puts block b of the disk under root in the image, unless the image
// holds another disk or already has the block.
func (m *Image) Put(root swarm.Address, b int64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.moving || !root.Equal(m.root) || m.root.IsZero() || b < 0 || b >= m.blocks || m.has(b) {
		return nil
	}
	return m.write(b, data)
}

// PutZeros marks count blocks from first on, a zero subtree of the disk
// under root, as present: holes in the file.
func (m *Image) PutZeros(root swarm.Address, first, count int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.moving || !root.Equal(m.root) || m.root.IsZero() || first < 0 || first+count > m.blocks {
		return nil
	}
	if err := punch(m.data, first*swarm.ChunkSize, count*swarm.ChunkSize); err != nil {
		return err
	}
	m.written = true
	for b := first; b < first+count; {
		if b%8 == 0 && b+8 <= first+count { // a whole byte at once
			if x := m.present[b/8]; x != 0xff {
				m.count += int64(8 - bits.OnesCount8(x))
				m.present[b/8] = 0xff
				m.dirty[int(b/8)] = true
			}
			b += 8
			continue
		}
		m.set(b)
		b++
	}
	return nil
}

// Sync makes the image durable: first the data written so far, then the
// map's changed bytes.
func (m *Image) Sync() error {
	m.mu.Lock()
	idx := slices.Sorted(maps.Keys(m.dirty))
	vals := make([]byte, len(idx))
	for k, i := range idx {
		vals[k] = m.present[i]
	}
	written := m.written
	m.dirty, m.written = map[int]bool{}, false
	m.mu.Unlock()

	err := func() error {
		if written {
			if err := m.data.Sync(); err != nil {
				return err
			}
		}
		if len(idx) == 0 {
			return nil
		}
		for k := 0; k < len(idx); {
			e := k + 1
			for e < len(idx) && idx[e] == idx[e-1]+1 {
				e++
			}
			if _, err := m.mapf.WriteAt(vals[k:e], int64(idx[k])); err != nil {
				return err
			}
			k = e
		}
		return m.mapf.Sync()
	}()
	if err != nil { // try again next time
		m.mu.Lock()
		for _, i := range idx {
			m.dirty[i] = true
		}
		m.written = m.written || written
		m.mu.Unlock()
	}
	return err
}

// Advance moves the image from the disk under from to the disk under to,
// which differs only in the blocks that apply passes to put: a checkpoint's
// frozen epoch, nil for zeros. Until it returns, the image stays at from
// for readers, who find the frozen epoch's blocks above it, and Put drops
// what it gets. An image already at to stays, and one at any other root
// starts again, empty, at to.
func (m *Image) Advance(from, to swarm.Address, apply func(put func(b int64, data []byte) error) error) error {
	m.mu.Lock()
	switch {
	case m.root.Equal(to):
		m.mu.Unlock()
		return nil
	case !m.root.Equal(from):
		defer m.mu.Unlock()
		return m.reset(to)
	}
	m.moving = true
	m.mu.Unlock()

	err := apply(func(b int64, data []byte) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		if b < 0 || b >= m.blocks {
			return fmt.Errorf("image: block %d is outside the disk", b)
		}
		return m.write(b, data)
	})
	if err == nil {
		err = m.Sync()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.moving = false
	if err != nil {
		// The blocks written are the frozen epoch's, which readers find
		// first until a later Advance completes.
		return err
	}
	m.root = to
	return m.writeRoot()
}

// Reset empties the image and makes it hold the disk under root.
func (m *Image) Reset(root swarm.Address) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reset(root)
}

func (m *Image) reset(root swarm.Address) error {
	size := m.blocks * swarm.ChunkSize
	for _, f := range []struct {
		f    *os.File
		size int64
	}{{m.data, size}, {m.mapf, int64(len(m.present))}} {
		if err := f.f.Truncate(0); err != nil {
			return err
		}
		if err := f.f.Truncate(f.size); err != nil {
			return err
		}
		if err := f.f.Sync(); err != nil {
			return err
		}
	}
	clear(m.present)
	m.dirty, m.count, m.written = map[int]bool{}, 0, false
	m.root = root
	return m.writeRoot()
}

// writeRoot records m.root durably: a new file, synced, renamed over the
// old one, and the directory synced.
func (m *Image) writeRoot() error {
	tmp := filepath.Join(m.dir, imageRoot+".new")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(f, m.root)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, filepath.Join(m.dir, imageRoot))
	}
	if err != nil {
		return err
	}
	d, err := os.Open(m.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Close syncs the image and closes its files.
func (m *Image) Close() error {
	err := m.Sync()
	if cerr := m.data.Close(); err == nil {
		err = cerr
	}
	if cerr := m.mapf.Close(); err == nil {
		err = cerr
	}
	return err
}

// writeZeros writes n zero bytes to f from off on.
func writeZeros(f *os.File, off, n int64) error {
	for n > 0 {
		k := min(n, int64(len(zeroBlock)))
		if _, err := f.WriteAt(zeroBlock[:k], off); err != nil {
			return err
		}
		off, n = off+k, n-k
	}
	return nil
}

// Extents describes length bytes from off on as data and holes, if the
// image holds every block there: zero blocks are holes in the file, which
// the filesystem reports. It reports false otherwise.
func (m *Image) Extents(off, length int64) ([]nbd.Extent, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	end := min(off+length, m.blocks*swarm.ChunkSize)
	for b := off / swarm.ChunkSize; b*swarm.ChunkSize < end; b++ {
		if !m.has(b) {
			return nil, false
		}
	}
	var out []nbd.Extent
	for pos := off; pos < end; {
		hole, next := holes(m.data, pos, end)
		if k := len(out); k > 0 && out[k-1].Hole == hole {
			out[k-1].Length += next - pos
		} else {
			out = append(out, nbd.Extent{Length: next - pos, Hole: hole, Zero: hole})
		}
		pos = next
	}
	return out, true
}
