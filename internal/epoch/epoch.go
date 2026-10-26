// Package epoch keeps a write epoch (SPEC §11): the blocks written to a disk
// since its last snapshot, on local storage, so that they survive a crash.
// Data goes to an append-only file and is described by an index of
// checksummed records, appended in order; a superblock names the snapshot
// underneath. Sync makes everything written so far durable, and Open
// recovers an epoch after a crash by replaying its index up to the first
// record that fails a checksum. An open epoch holds a lock, so that no other
// process or Open writes it at the same time.
package epoch

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// Format names the epoch format this package writes and reads.
const Format = "swarmblk-epoch/1"

// States of an epoch (SPEC §11).
const (
	Active    = "active"
	Frozen    = "frozen"
	Uploading = "uploading"
	Retired   = "retired"
)

// Super is an epoch's superblock, super.json.
type Super struct {
	Format string        `json:"format"`
	Epoch  uint64        `json:"epoch"`
	Parent swarm.Address `json:"parent"`    // the snapshot the writes go on top of
	Root   swarm.Address `json:"disk_root"` // the parent's disk root
	Size   int64         `json:"size"`
	State  string        `json:"state"`
}

// Files in an epoch's directory.
const (
	superFile = "super.json"
	dataFile  = "data"
	indexFile = "index"
)

// An index record, 36 bytes, little-endian:
//
//	kind     1 byte   kindBlock, kindZero or kindPatch
//	reserved 3 bytes  0
//	pos      uint32   a patch's offset within the block, else 0
//	length   uint32   bytes of data: 4096, 0, or a patch's length
//	block    uint64
//	offset   uint64   where the data starts in the data file
//	data     uint32   CRC32C of the data
//	check    uint32   CRC32C of the 32 bytes before it
const (
	recordSize = 36
	kindBlock  = 1 // a whole block of data
	kindZero   = 2 // a whole block of zeros, without data
	kindPatch  = 3 // part of a block, applied to what the block held before
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type record struct {
	kind        byte
	pos, length uint32
	block       uint64
	offset      uint64
	data        uint32
}

func (r record) encode() []byte {
	b := make([]byte, 0, recordSize)
	b = append(b, r.kind, 0, 0, 0)
	b = binary.LittleEndian.AppendUint32(b, r.pos)
	b = binary.LittleEndian.AppendUint32(b, r.length)
	b = binary.LittleEndian.AppendUint64(b, r.block)
	b = binary.LittleEndian.AppendUint64(b, r.offset)
	b = binary.LittleEndian.AppendUint32(b, r.data)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// decode parses a record and reports whether it is well formed.
func decode(b []byte) (record, bool) {
	r := record{
		kind:   b[0],
		pos:    binary.LittleEndian.Uint32(b[4:]),
		length: binary.LittleEndian.Uint32(b[8:]),
		block:  binary.LittleEndian.Uint64(b[12:]),
		offset: binary.LittleEndian.Uint64(b[20:]),
		data:   binary.LittleEndian.Uint32(b[28:]),
	}
	ok := crc32.Checksum(b[:32], castagnoli) == binary.LittleEndian.Uint32(b[32:]) && b[1] == 0 && b[2] == 0 && b[3] == 0
	switch r.kind {
	case kindBlock:
		ok = ok && r.pos == 0 && r.length == swarm.ChunkSize
	case kindZero:
		ok = ok && r.pos == 0 && r.length == 0
	case kindPatch:
		ok = ok && r.length > 0 && r.pos+r.length <= swarm.ChunkSize
	default:
		ok = false
	}
	return r, ok
}

// Block is what an epoch holds for one block: a whole block of data or
// zeros, or nothing, and then the patches written after it, in order. A
// block with patches but no whole record is read by applying them to the
// block's contents in the snapshot underneath.
type Block struct {
	Whole   bool // there is a whole record
	Zero    bool // it holds zeros
	offset  int64
	Patches []Patch
}

// Patch is a partial write: Length bytes at Pos within the block.
type Patch struct {
	Pos, Length int
	offset      int64
}

// Epoch is an open write epoch. Its methods may be called concurrently.
type Epoch struct {
	dir string

	mu     sync.RWMutex
	super  Super
	data   *os.File
	index  *os.File
	end    int64 // the data file's length
	count  int64 // records in the index
	blocks map[int64]*Block
}

// Create makes a new epoch in dir, which must not exist yet.
func Create(dir string, s Super) (*Epoch, error) {
	if s.Size <= 0 || s.Size%swarm.ChunkSize != 0 {
		return nil, fmt.Errorf("epoch: disk size %d", s.Size)
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, err
	}
	s.Format = Format
	if s.State == "" {
		s.State = Active
	}
	if err := writeSuper(dir, s); err != nil {
		return nil, err
	}
	e := &Epoch{dir: dir, super: s, blocks: map[int64]*Block{}}
	var err error
	if e.data, err = os.OpenFile(filepath.Join(dir, dataFile), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600); err != nil {
		return nil, err
	}
	if e.index, err = os.OpenFile(filepath.Join(dir, indexFile), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600); err != nil {
		e.data.Close()
		return nil, err
	}
	if err := e.lock(); err != nil {
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		e.Close()
		return nil, err
	}
	return e, nil
}

// Open opens the epoch in dir and recovers it: it replays the index and
// drops the first record that fails a checksum, or whose data does, and
// everything after it, which no Sync covered. It truncates both files to
// what it kept.
func Open(dir string) (*Epoch, error) {
	b, err := os.ReadFile(filepath.Join(dir, superFile))
	if err != nil {
		return nil, err
	}
	var s Super
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("epoch %s: %w", dir, err)
	}
	if s.Format != Format || s.Size <= 0 || s.Size%swarm.ChunkSize != 0 {
		return nil, fmt.Errorf("epoch %s: format %q, size %d", dir, s.Format, s.Size)
	}
	e := &Epoch{dir: dir, super: s, blocks: map[int64]*Block{}}
	if e.data, err = os.OpenFile(filepath.Join(dir, dataFile), os.O_RDWR, 0); err != nil {
		return nil, err
	}
	if e.index, err = os.OpenFile(filepath.Join(dir, indexFile), os.O_RDWR, 0); err != nil {
		e.data.Close()
		return nil, err
	}
	if err := e.lock(); err != nil {
		return nil, err
	}
	if err := e.replay(); err != nil {
		e.Close()
		return nil, err
	}
	return e, nil
}

// lock takes the epoch's lock, an exclusive flock on its index, which
// closing the index releases. It closes both files if the epoch is in use.
func (e *Epoch) lock() error {
	if err := syscall.Flock(int(e.index.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		e.data.Close()
		e.index.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return fmt.Errorf("epoch %s is in use", e.dir)
		}
		return err
	}
	return nil
}

// replay rebuilds the blocks from the index, keeping records up to the first
// bad one, and truncates the files after them.
func (e *Epoch) replay() error {
	idx, err := io.ReadAll(e.index)
	if err != nil {
		return err
	}
	fi, err := e.data.Stat()
	if err != nil {
		return err
	}
	var buf [swarm.ChunkSize]byte
	for len(idx) >= recordSize {
		r, ok := decode(idx[:recordSize])
		ok = ok && r.block < uint64(e.super.Size/swarm.ChunkSize) && int64(r.offset) == e.end && e.end+int64(r.length) <= fi.Size()
		if ok && r.length > 0 {
			data := buf[:r.length]
			_, err := e.data.ReadAt(data, int64(r.offset))
			ok = err == nil && crc32.Checksum(data, castagnoli) == r.data
		}
		if !ok {
			break
		}
		e.apply(r)
		e.end += int64(r.length)
		e.count++
		idx = idx[recordSize:]
	}
	if err := e.index.Truncate(e.count * recordSize); err != nil {
		return err
	}
	if err := e.data.Truncate(e.end); err != nil {
		return err
	}
	if _, err := e.index.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

// apply adds a record to the blocks.
func (e *Epoch) apply(r record) {
	b := int64(r.block)
	switch r.kind {
	case kindBlock, kindZero:
		e.blocks[b] = &Block{Whole: true, Zero: r.kind == kindZero, offset: int64(r.offset)}
	case kindPatch:
		blk, ok := e.blocks[b]
		if !ok {
			blk = &Block{}
			e.blocks[b] = blk
		}
		blk.Patches = append(blk.Patches, Patch{Pos: int(r.pos), Length: int(r.length), offset: int64(r.offset)})
	}
}

// Super returns the epoch's superblock.
func (e *Epoch) Super() Super {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.super
}

// SetState records a new state in the superblock, durably.
func (e *Epoch) SetState(state string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.super
	s.State = state
	if err := writeSuper(e.dir, s); err != nil {
		return err
	}
	e.super = s
	return nil
}

// append writes data, if any, and then its record.
func (e *Epoch) append(r record, data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if r.block >= uint64(e.super.Size/swarm.ChunkSize) {
		return fmt.Errorf("epoch: block %d is outside the disk", r.block)
	}
	r.offset = uint64(e.end)
	r.length = uint32(len(data))
	if len(data) > 0 {
		r.data = crc32.Checksum(data, castagnoli)
		if _, err := e.data.WriteAt(data, e.end); err != nil {
			return err
		}
	}
	if _, err := e.index.Write(r.encode()); err != nil {
		return err
	}
	e.apply(r)
	e.end += int64(len(data))
	e.count++
	return nil
}

// Put records a whole block.
func (e *Epoch) Put(b int64, data []byte) error {
	if len(data) != swarm.ChunkSize {
		return fmt.Errorf("epoch: block %d of %d bytes", b, len(data))
	}
	return e.append(record{kind: kindBlock, block: uint64(b)}, data)
}

// Zero records a block of zeros.
func (e *Epoch) Zero(b int64) error {
	return e.append(record{kind: kindZero, block: uint64(b)}, nil)
}

// Patch records data written at pos within block b.
func (e *Epoch) Patch(b int64, pos int, data []byte) error {
	if len(data) == 0 || pos < 0 || pos+len(data) > swarm.ChunkSize {
		return fmt.Errorf("epoch: patch of %d bytes at %d", len(data), pos)
	}
	return e.append(record{kind: kindPatch, pos: uint32(pos), block: uint64(b)}, data)
}

// Sync makes every record so far durable: the data first, then the index,
// so that a durable record never points at data that is not.
func (e *Epoch) Sync() error {
	if err := e.data.Sync(); err != nil {
		return err
	}
	return e.index.Sync()
}

// Get returns what the epoch holds for block b, if anything.
func (e *Epoch) Get(b int64) (Block, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	blk, ok := e.blocks[b]
	if !ok {
		return Block{}, false
	}
	c := *blk
	c.Patches = slices.Clone(blk.Patches)
	return c, true
}

// Read returns block blk's contents: its whole record, or else base, the
// block's contents underneath, with the patches applied. base is not
// changed.
func (e *Epoch) Read(blk Block, base []byte) ([]byte, error) {
	out := make([]byte, swarm.ChunkSize)
	switch {
	case blk.Whole && !blk.Zero:
		if _, err := e.data.ReadAt(out, blk.offset); err != nil {
			return nil, err
		}
	case !blk.Whole:
		if len(base) != swarm.ChunkSize {
			return nil, errors.New("epoch: a patched block needs its base")
		}
		copy(out, base)
	}
	for _, p := range blk.Patches {
		if _, err := e.data.ReadAt(out[p.Pos:p.Pos+p.Length], p.offset); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Blocks returns the blocks that the epoch holds, in order.
func (e *Epoch) Blocks() []int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return slices.Sorted(maps.Keys(e.blocks))
}

// Close syncs and closes the epoch.
func (e *Epoch) Close() error {
	err := e.Sync()
	if cerr := e.data.Close(); err == nil {
		err = cerr
	}
	if cerr := e.index.Close(); err == nil {
		err = cerr
	}
	return err
}

// writeSuper replaces the superblock atomically: a new file, synced, renamed
// over the old one, and the directory synced.
func writeSuper(dir string, s Super) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, superFile+".new")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(b, '\n'))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, filepath.Join(dir, superFile))
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
