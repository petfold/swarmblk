// Package store keeps chunks on local disk under their Swarm addresses: the
// tree cache and chunk cache of SPEC §10. Every chunk is checked against its
// address with Bee's pkg/cac when it is written and when it is read, so a
// damaged or half-written file is never returned. Sweep bounds the cache by
// removing the leaves read least recently, which can be fetched again.
package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// ErrCorrupt means that a stored chunk no longer matched its address. The
// store has removed it, so fetching the chunk again repairs it.
var ErrCorrupt = errors.New("stored chunk does not match its address")

// Store is a directory of chunks, one file per chunk. It is safe for
// concurrent use, and as a storage.Putter it can take the output of Bee's
// pipeline builder directly.
type Store struct {
	dir string
}

var (
	_ storage.Getter = (*Store)(nil)
	_ storage.Putter = (*Store)(nil)
)

// Open returns the store in dir, creating the directory if needed.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

// path returns the file for the chunk at addr, under two levels of
// directories named by the address's first two bytes, so that no directory
// grows large.
func (s *Store) path(addr swarm.Address) string {
	h := addr.String()
	return filepath.Join(s.dir, h[:2], h[2:4], h)
}

// Put checks ch against its address and stores it, unless the store already
// holds that address: a stored chunk was checked when it was written. A data
// chunk, whose span equals its payload length, is stored as the payload
// alone, so a full leaf takes one 4 KiB block on disk; other chunks keep
// their span.
func (s *Store) Put(_ context.Context, ch swarm.Chunk) error {
	p := s.path(ch.Address())
	if _, err := os.Stat(p); err == nil {
		return nil // the same address always holds the same chunk
	}
	if !cac.Valid(ch) {
		return fmt.Errorf("chunk %s: %w", ch.Address(), storage.ErrInvalidChunk)
	}
	data := ch.Data()
	if binary.LittleEndian.Uint64(data) == uint64(len(data)-swarm.SpanSize) {
		data = data[swarm.SpanSize:]
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(p), ".put-*")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), p)
	}
	if err != nil {
		os.Remove(f.Name())
	}
	return err
}

// Get returns the chunk at addr after checking it. It returns an error
// wrapping storage.ErrNotFound if the store does not have the chunk, and one
// wrapping ErrCorrupt if the stored file no longer matches. Reading a leaf
// marks it used, for Sweep.
func (s *Store) Get(_ context.Context, addr swarm.Address) (swarm.Chunk, error) {
	p := s.path(addr)
	b, err := readUsed(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("chunk %s: %w", addr, storage.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	// The file holds either a payload alone or a span and payload; only the
	// right reading hashes to addr.
	if len(b) <= swarm.ChunkSize {
		data := binary.LittleEndian.AppendUint64(make([]byte, 0, swarm.SpanSize+len(b)), uint64(len(b)))
		if ch := swarm.NewChunk(addr, append(data, b...)); cac.Valid(ch) {
			return ch, nil
		}
	}
	if ch := swarm.NewChunk(addr, b); cac.Valid(ch) {
		return ch, nil
	}
	os.Remove(p)
	return nil, fmt.Errorf("chunk %s: %w", addr, ErrCorrupt)
}

// Through returns a getter that reads from s and, when s lacks a chunk or
// holds a damaged copy, gets it with fetch and keeps it in s once checked.
func (s *Store) Through(fetch func(context.Context, swarm.Address) (swarm.Chunk, error)) storage.Getter {
	return through{s, fetch}
}

type through struct {
	s     *Store
	fetch func(context.Context, swarm.Address) (swarm.Chunk, error)
}

func (t through) Get(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	ch, err := t.s.Get(ctx, addr)
	if err == nil || !errors.Is(err, storage.ErrNotFound) && !errors.Is(err, ErrCorrupt) {
		return ch, err
	}
	if ch, err = t.fetch(ctx, addr); err != nil {
		return nil, err
	}
	if !ch.Address().Equal(addr) {
		return nil, fmt.Errorf("asked for chunk %s, got %s: %w", addr, ch.Address(), storage.ErrInvalidChunk)
	}
	if err := t.s.Put(ctx, ch); err != nil {
		return nil, err
	}
	return ch, nil
}

// A leaf is stored as its payload alone, and in format 1 every leaf is a
// whole block, so a file of this size is a leaf: it can be fetched again,
// and Sweep may remove it. Intermediate chunks keep their span, and so are
// longer, and manifests' small files are shorter.
const leafSize = swarm.ChunkSize

// usedAfter is how stale a leaf's time must be before a read renews it, so
// that most reads cost no extra write.
const usedAfter = time.Hour

// readUsed reads the file at p and, if it is a leaf whose time is stale,
// sets its time to now, which is when Sweep takes it to have been used.
func readUsed(p string) ([]byte, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(f)
	if err == nil && fi.Size() == leafSize && time.Since(fi.ModTime()) > usedAfter {
		now := time.Now()
		os.Chtimes(p, now, now)
	}
	return b, err
}

// lockFile is the file whose flock writers share and Sweep takes alone.
const lockFile = ".lock"

// Hold keeps Sweep away until release is called. A writer holds the store
// from putting chunks until Bee has synced them, since before that they
// exist nowhere else. Any number may hold it at once.
func (s *Store) Hold() (release func(), err error) {
	f, err := os.OpenFile(filepath.Join(s.dir, lockFile), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		f.Close()
		return nil, err
	}
	return func() { f.Close() }, nil
}

// Swept is what a sweep found and did.
type Swept struct {
	Skipped        bool  // a writer held the store
	Bytes          int64 // in the store's files after the sweep
	Removed, Freed int64 // leaves removed, and their bytes
}

// Sweep bounds the store: if its files take more than limit bytes, it
// removes leaves, the least recently used first, until they take at most
// nine tenths of limit. Only leaves are removed, so the tree's intermediate
// chunks and the snapshots' manifests stay. Sweep skips its work while a
// writer holds the store, and does nothing if limit is not positive.
func (s *Store) Sweep(limit int64) (Swept, error) {
	var sw Swept
	if limit <= 0 {
		return sw, nil
	}
	f, err := os.OpenFile(filepath.Join(s.dir, lockFile), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return sw, err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); errors.Is(err, syscall.EWOULDBLOCK) {
		sw.Skipped = true
		return sw, nil
	} else if err != nil {
		return sw, err
	}
	type leaf struct {
		path string
		used time.Time
	}
	var leaves []leaf
	err = filepath.WalkDir(s.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasPrefix(d.Name(), ".") {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return nil // removed meanwhile
		}
		sw.Bytes += fi.Size()
		if fi.Size() == leafSize {
			leaves = append(leaves, leaf{p, fi.ModTime()})
		}
		return nil
	})
	if err != nil || sw.Bytes <= limit {
		return sw, err
	}
	slices.SortFunc(leaves, func(a, b leaf) int { return a.used.Compare(b.used) })
	for _, l := range leaves {
		if sw.Bytes <= limit/10*9 {
			break
		}
		if os.Remove(l.path) == nil {
			sw.Bytes -= leafSize
			sw.Removed++
			sw.Freed += leafSize
		}
	}
	return sw, nil
}
