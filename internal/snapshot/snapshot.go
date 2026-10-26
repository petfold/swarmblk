// Package snapshot writes and reads snapshot manifests (SPEC §5): Mantaray
// manifests, built with Bee's pkg/manifest, that name a disk tree
// (disk.raw), its metadata (meta.json) and an optional boot trace
// (prefetch.bin). Any Swarm tool can then fetch the image as
// /bzz/<manifest>/disk.raw.
package snapshot

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ethersphere/bee/v2/pkg/file/joiner"
	"github.com/ethersphere/bee/v2/pkg/file/loadsave"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/manifest"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/tree"
)

// Format names the snapshot format this package writes and reads.
const Format = "swarmblk/1"

// Paths in a snapshot manifest.
const (
	DiskPath     = "disk.raw"
	MetaPath     = "meta.json"
	PrefetchPath = "prefetch.bin"
)

// Meta is meta.json, format 1 (SPEC §5).
type Meta struct {
	Format         string         `json:"format"`
	Size           int64          `json:"size"`
	LogicalSector  int            `json:"logical_sector"`
	PhysicalSector int            `json:"physical_sector"`
	Redundancy     int            `json:"redundancy"`
	Encryption     string         `json:"encryption"`  // "none" or "guest-luks"
	DiskRoot       swarm.Address  `json:"disk_root"`   // the disk tree's root (SPEC §2)
	Parent         *swarm.Address `json:"parent"`      // the parent snapshot's manifest; nil for a first snapshot
	Lineage        *Lineage       `json:"lineage"`     // nil for a snapshot outside any lineage
	Created        time.Time      `json:"created"`     // UTC
	Consistency    string         `json:"consistency"` // "crash" or "application"
	Batches        []string       `json:"batches"`     // every batch known to stamp chunks reachable from this snapshot
	Prefetch       *string        `json:"prefetch"`    // PrefetchPath, or nil
	Stats          Stats          `json:"stats"`
}

// Lineage places a snapshot in a lineage feed (SPEC §7).
type Lineage struct {
	Owner string `json:"owner"` // 0x and 40 hex digits
	Topic string `json:"topic"` // 64 hex digits
	Index uint64 `json:"index"`
}

// Stats compares a snapshot with its parent, or with an empty disk.
type Stats struct {
	ChangedBlocks int64 `json:"changed_blocks"`
	NewChunks     int64 `json:"new_chunks"`
}

// NewMeta returns the metadata of a new, crash-consistent, unencrypted
// snapshot of a disk of size bytes whose tree has the given root.
func NewMeta(size int64, root swarm.Address) Meta {
	return Meta{
		Format:         Format,
		Size:           size,
		LogicalSector:  512,
		PhysicalSector: 4096,
		Encryption:     "none",
		DiskRoot:       root,
		Created:        time.Now().UTC().Truncate(time.Second),
		Consistency:    "crash",
	}
}

// check reports what in m format 1 does not allow.
func (m Meta) check() error {
	if _, err := tree.New(m.Size); err != nil {
		return err
	}
	switch {
	case m.Format != Format:
		return fmt.Errorf("format %q, want %q", m.Format, Format)
	case m.LogicalSector != 512 || m.PhysicalSector != 4096:
		return fmt.Errorf("sectors %d/%d, want 512/4096", m.LogicalSector, m.PhysicalSector)
	case m.Redundancy != 0:
		return fmt.Errorf("redundancy level %d, want 0", m.Redundancy)
	case m.Encryption != "none" && m.Encryption != "guest-luks":
		return fmt.Errorf("encryption %q", m.Encryption)
	case m.Consistency != "crash" && m.Consistency != "application":
		return fmt.Errorf("consistency %q", m.Consistency)
	case m.DiskRoot.IsZero() || len(m.DiskRoot.Bytes()) != swarm.HashSize:
		return errors.New("no disk root")
	}
	return nil
}

// pipelineFn returns Bee's pipeline at redundancy level 0, unencrypted,
// writing to put.
func pipelineFn(ctx context.Context, put storage.Putter) func() pipeline.Interface {
	return func() pipeline.Interface { return builder.NewPipelineBuilder(ctx, put, false, redundancy.NONE) }
}

// Write builds the snapshot manifest for meta and returns its reference. It
// writes meta.json, prefetch.bin if prefetch is not nil, and the manifest's
// nodes through put, the manifest's root last. The disk tree itself must
// already exist; Write only names meta.DiskRoot.
func Write(ctx context.Context, put storage.Putter, get storage.Getter, meta Meta, prefetch []byte) (swarm.Address, error) {
	meta.Prefetch = nil
	if prefetch != nil {
		p := PrefetchPath
		meta.Prefetch = &p
	}
	if err := meta.check(); err != nil {
		return swarm.ZeroAddress, fmt.Errorf("meta.json: %w", err)
	}
	js, err := json.Marshal(meta)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	newPipeline := pipelineFn(ctx, put)
	metaRef, err := builder.FeedPipeline(ctx, newPipeline(), bytes.NewReader(js))
	if err != nil {
		return swarm.ZeroAddress, err
	}

	m, err := manifest.NewDefaultManifest(loadsave.New(get, put, newPipeline, redundancy.NONE), false)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	type entry struct {
		path string
		ref  swarm.Address
		meta map[string]string
	}
	entries := []entry{
		{manifest.RootPath, swarm.ZeroAddress, map[string]string{manifest.WebsiteIndexDocumentSuffixKey: MetaPath}},
		{DiskPath, meta.DiskRoot, fileMeta(DiskPath, "application/octet-stream")},
		{MetaPath, metaRef, fileMeta(MetaPath, "application/json")},
	}
	if prefetch != nil {
		ref, err := builder.FeedPipeline(ctx, newPipeline(), bytes.NewReader(prefetch))
		if err != nil {
			return swarm.ZeroAddress, err
		}
		entries = append(entries, entry{PrefetchPath, ref, fileMeta(PrefetchPath, "application/octet-stream")})
	}
	for _, e := range entries {
		if err := m.Add(ctx, e.path, manifest.NewEntry(e.ref, e.meta)); err != nil {
			return swarm.ZeroAddress, fmt.Errorf("manifest entry %s: %w", e.path, err)
		}
	}
	return m.Store(ctx)
}

// fileMeta is the metadata Bee's /bzz needs to serve a manifest entry.
func fileMeta(name, contentType string) map[string]string {
	return map[string]string{
		manifest.EntryMetadataContentTypeKey: contentType,
		manifest.EntryMetadataFilenameKey:    name,
	}
}

// Snapshot is a snapshot manifest that Read has checked.
type Snapshot struct {
	Ref      swarm.Address // the manifest
	Meta     Meta
	Prefetch swarm.Address // the boot trace's root, or the zero address
}

// Read loads the snapshot manifest at ref and checks that its meta.json is
// format 1 and names the same disk root as its disk.raw entry. Unknown
// fields in meta.json are ignored, as SPEC §5 requires.
func Read(ctx context.Context, get storage.Getter, ref swarm.Address) (Snapshot, error) {
	m, err := manifest.NewDefaultManifestReference(ref, loadsave.NewReadonly(get, discard{}, redundancy.NONE))
	if err != nil {
		return Snapshot{}, fmt.Errorf("snapshot %s: %w", ref, err)
	}
	lookup := func(path string) (swarm.Address, error) {
		e, err := m.Lookup(ctx, path)
		if err != nil {
			return swarm.ZeroAddress, fmt.Errorf("snapshot %s: %s: %w", ref, path, err)
		}
		return e.Reference(), nil
	}
	metaRef, err := lookup(MetaPath)
	if err != nil {
		return Snapshot{}, err
	}
	j, size, err := joiner.New(ctx, get, discard{}, metaRef, redundancy.NONE)
	if err != nil {
		return Snapshot{}, fmt.Errorf("snapshot %s: %s: %w", ref, MetaPath, err)
	}
	if size > 1<<20 {
		return Snapshot{}, fmt.Errorf("snapshot %s: %s has %d bytes", ref, MetaPath, size)
	}
	js, err := io.ReadAll(j)
	if err != nil {
		return Snapshot{}, fmt.Errorf("snapshot %s: %s: %w", ref, MetaPath, err)
	}
	s := Snapshot{Ref: ref}
	if err := json.Unmarshal(js, &s.Meta); err != nil {
		return Snapshot{}, fmt.Errorf("snapshot %s: %s: %w", ref, MetaPath, err)
	}
	if err := s.Meta.check(); err != nil {
		return Snapshot{}, fmt.Errorf("snapshot %s: %s: %w", ref, MetaPath, err)
	}
	disk, err := lookup(DiskPath)
	if err != nil {
		return Snapshot{}, err
	}
	if !disk.Equal(s.Meta.DiskRoot) {
		return Snapshot{}, fmt.Errorf("snapshot %s: %s is %s but %s names %s", ref, DiskPath, disk, MetaPath, s.Meta.DiskRoot)
	}
	if s.Meta.Prefetch != nil {
		if s.Prefetch, err = lookup(*s.Meta.Prefetch); err != nil {
			return Snapshot{}, err
		}
	}
	return s, nil
}

// discard is a putter for reading, which writes nothing.
type discard struct{}

func (discard) Put(context.Context, swarm.Chunk) error { return nil }

// Chunks calls fn once for each distinct chunk of the snapshot at ref: its
// manifest's nodes, and the chunk trees of disk.raw, meta.json and
// prefetch.bin, the zero subtrees' chunks once each. It fetches with get
// only the chunks that have children; a leaf's address comes from its
// parent. Re-stamping a snapshot (DESIGN §5) uploads exactly these.
func Chunks(ctx context.Context, get storage.Getter, ref swarm.Address, fn func(swarm.Address) error) error {
	m, err := manifest.NewDefaultManifestReference(ref, loadsave.NewReadonly(get, discard{}, redundancy.NONE))
	if err != nil {
		return fmt.Errorf("snapshot %s: %w", ref, err)
	}
	seen := map[string]bool{}
	var walk func(addr swarm.Address, leaf bool) error
	walk = func(addr swarm.Address, leaf bool) error {
		k := addr.ByteString()
		if seen[k] {
			return nil
		}
		seen[k] = true
		if err := fn(addr); err != nil || leaf {
			return err
		}
		ch, err := get.Get(ctx, addr)
		if err != nil {
			return fmt.Errorf("snapshot %s: chunk %s: %w", ref, addr, err)
		}
		span := binary.LittleEndian.Uint64(ch.Data())
		if span <= swarm.ChunkSize {
			return nil // a leaf after all
		}
		refs := ch.Data()[swarm.SpanSize:]
		if len(refs)%swarm.HashSize != 0 {
			return fmt.Errorf("snapshot %s: chunk %s spans %d bytes but holds %d bytes of references", ref, addr, span, len(refs))
		}
		// Each child covers at most a 128th of the span, rounded up to a
		// block, so under a chunk of at most 128 blocks they are leaves.
		leaves := span <= swarm.Branches*swarm.ChunkSize
		for i := 0; i < len(refs); i += swarm.HashSize {
			if err := walk(swarm.NewAddress(refs[i:i+swarm.HashSize]), leaves); err != nil {
				return err
			}
		}
		return nil
	}
	return m.IterateAddresses(ctx, func(addr swarm.Address) error { return walk(addr, false) })
}
