package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/file/joiner"
	"github.com/ethersphere/bee/v2/pkg/file/loadsave"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/manifest"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/tree"
)

// recorder stores chunks and remembers the order they came in.
type recorder struct {
	*inmemchunkstore.ChunkStore
	order []swarm.Address
}

func (r *recorder) Put(ctx context.Context, ch swarm.Chunk) error {
	r.order = append(r.order, ch.Address())
	return r.ChunkStore.Put(ctx, ch)
}

func testMeta() Meta {
	m := NewMeta(64<<20, tree.Zero(2)) // Z_2 is the root of an empty 64 MiB disk
	parent := tree.Zero(1)
	m.Parent = &parent
	m.Lineage = &Lineage{Owner: "0x" + strings.Repeat("ab", 20), Topic: strings.Repeat("cd", 32), Index: 12}
	m.Batches = []string{strings.Repeat("ef", 32)}
	m.Stats = Stats{ChangedBlocks: 1234, NewChunks: 1290}
	return m
}

func readAll(t *testing.T, get storage.Getter, ref swarm.Address) []byte {
	t.Helper()
	j, _, err := joiner.New(context.Background(), get, discard{}, ref, redundancy.NONE)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(j)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestWriteRead(t *testing.T) {
	ctx := context.Background()
	trace := make([]byte, 10000) // three leaves and their parent
	rand.NewChaCha8([32]byte{9}).Read(trace)
	for _, prefetch := range [][]byte{trace, nil} {
		s := &recorder{ChunkStore: inmemchunkstore.New()}
		meta := testMeta()
		ref, err := Write(ctx, s, s, meta, prefetch)
		if err != nil {
			t.Fatal(err)
		}
		if last := s.order[len(s.order)-1]; !last.Equal(ref) {
			t.Errorf("the manifest root %s was not written last (%s was)", ref, last)
		}

		got, err := Read(ctx, s, ref)
		if err != nil {
			t.Fatal(err)
		}
		if prefetch != nil {
			p := PrefetchPath
			meta.Prefetch = &p
		}
		want, _ := json.Marshal(meta)
		have, _ := json.Marshal(got.Meta)
		if !bytes.Equal(have, want) {
			t.Errorf("meta.json read back as\n%s\nwant\n%s", have, want)
		}
		switch {
		case prefetch == nil && !got.Prefetch.IsZero():
			t.Errorf("a snapshot without a trace has prefetch %s", got.Prefetch)
		case prefetch != nil && !bytes.Equal(readAll(t, s, got.Prefetch), prefetch):
			t.Error("prefetch.bin read back different")
		}
	}
}

func TestWriteRefusesBadMeta(t *testing.T) {
	ctx := context.Background()
	s := inmemchunkstore.New()
	for name, change := range map[string]func(*Meta){
		"odd size":    func(m *Meta) { m.Size = 100 << 20 },
		"redundancy":  func(m *Meta) { m.Redundancy = 2 },
		"encryption":  func(m *Meta) { m.Encryption = "aes" },
		"consistency": func(m *Meta) { m.Consistency = "eventual" },
		"no root":     func(m *Meta) { m.DiskRoot = swarm.ZeroAddress },
		"format":      func(m *Meta) { m.Format = "swarmblk/2" },
	} {
		meta := testMeta()
		change(&meta)
		if _, err := Write(ctx, s, s, meta, nil); err == nil {
			t.Errorf("%s: Write accepted it", name)
		}
	}
}

// rawManifest builds a manifest with the given entries, bypassing Write's
// checks, as a broken or foreign writer might.
func rawManifest(t *testing.T, s *inmemchunkstore.ChunkStore, entries map[string]swarm.Address) swarm.Address {
	t.Helper()
	ctx := context.Background()
	m, err := manifest.NewDefaultManifest(loadsave.New(s, s, pipelineFn(ctx, s), redundancy.NONE), false)
	if err != nil {
		t.Fatal(err)
	}
	for path, ref := range entries {
		if err := m.Add(ctx, path, manifest.NewEntry(ref, fileMeta(path, "application/octet-stream"))); err != nil {
			t.Fatal(err)
		}
	}
	ref, err := m.Store(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

// file stores b with Bee's pipeline and returns its root.
func file(t *testing.T, s *inmemchunkstore.ChunkStore, b []byte) swarm.Address {
	t.Helper()
	ctx := context.Background()
	ref, err := builder.FeedPipeline(ctx, pipelineFn(ctx, s)(), bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestReadRefusesOtherManifests(t *testing.T) {
	ctx := context.Background()
	s := inmemchunkstore.New()
	meta := testMeta()
	good, _ := json.Marshal(meta)
	foreign := meta
	foreign.Format = "something/1"
	other, _ := json.Marshal(foreign)
	for name, entries := range map[string]map[string]swarm.Address{
		"no meta.json":      {DiskPath: meta.DiskRoot},
		"foreign format":    {DiskPath: meta.DiskRoot, MetaPath: file(t, s, other)},
		"not JSON":          {DiskPath: meta.DiskRoot, MetaPath: file(t, s, []byte("swarmblk"))},
		"disk.raw mismatch": {DiskPath: tree.Zero(1), MetaPath: file(t, s, good)},
		"no disk.raw":       {MetaPath: file(t, s, good)},
	} {
		if _, err := Read(ctx, s, rawManifest(t, s, entries)); err == nil {
			t.Errorf("%s: Read accepted it", name)
		}
	}
	// Readers ignore fields they do not know (SPEC §5).
	extra := strings.Replace(string(good), `"format"`, `"future_field":[1,2],"format"`, 1)
	if _, err := Read(ctx, s, rawManifest(t, s, map[string]swarm.Address{DiskPath: meta.DiskRoot, MetaPath: file(t, s, []byte(extra))})); err != nil {
		t.Errorf("an unknown field: %v", err)
	}
}
