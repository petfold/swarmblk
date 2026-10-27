package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/lineage"
	"github.com/petfold/swarmblk/internal/snapshot"
	"github.com/petfold/swarmblk/internal/store"
	"github.com/petfold/swarmblk/internal/tree"
)

func cmdCheckpoint(args []string) {
	fs := flag.NewFlagSet("checkpoint", flag.ExitOnError)
	beeURL := fs.String("bee", envOr("SWARMBLK_BEE_URL", "http://localhost:1633"), "Bee API `url` ($SWARMBLK_BEE_URL if set)")
	batch := fs.String("batch", os.Getenv("SWARMBLK_BATCH"), "immutable postage batch `id` to stamp with ($SWARMBLK_BATCH if set)")
	dir := fs.String("home", home(), "local state `directory` ($SWARMBLK_HOME if set)")
	branch := fs.Bool("branch", false, "start a new lineage, even if this host holds the parent's")
	prefetch := fs.String("prefetch", "", "store the boot trace in this `file` with the snapshot, rather than the parent's")
	disk := fs.String("disk", "", "checkpoint the writable disk with this `name` instead: through the serve that runs it, or here if none does")
	fs.Parse(args)
	if *disk != "" {
		if fs.NArg() != 0 {
			usage()
		}
		s, err := store.Open(filepath.Join(*dir, "chunks"))
		if err != nil {
			log.Fatal(err)
		}
		snap, err := checkpointNamedDisk(context.Background(), bee.New(*beeURL, 64), s, *dir, *disk, *batch, os.Stderr)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(snap)
		return
	}
	if fs.NArg() != 1 || *batch == "" {
		usage()
	}
	var trace []byte
	if *prefetch != "" {
		var err error
		if trace, err = os.ReadFile(*prefetch); err != nil {
			log.Fatal(err)
		}
		if _, err := snapshot.DecodeTrace(trace); err != nil {
			log.Fatalf("%s: %v", *prefetch, err)
		}
	}
	s, err := store.Open(filepath.Join(*dir, "chunks"))
	if err != nil {
		log.Fatal(err)
	}
	snap, err := checkpoint(context.Background(), bee.New(*beeURL, 64), s, filepath.Join(*dir, "keys"), *batch, fs.Arg(0), *branch, trace, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(snap)
}

// window is how many blocks checkpoint rebuilds at a time: 64 MiB.
const window = 16384

// checkpoint turns the overlay at path into the next snapshot after the one
// it sits on (SPEC §8), with the VM stopped: commit rebuilds the parent's
// tree with the overlay's blocks and uploads the snapshot, and before it is
// published the overlay is pointed at it, so that the next checkpoint
// follows it. Lineage keys live in keys. The snapshot gets the boot trace
// prefetch, if not nil, or else keeps its parent's (SPEC §6). It reports
// progress to w and returns the snapshot's reference, or the parent's if
// the overlay changes nothing and prefetch is nil or the parent's trace.
func checkpoint(ctx context.Context, c *bee.Client, s *store.Store, keys, batch, path string, branch bool, prefetch []byte, w io.Writer) (swarm.Address, error) {
	if err := checkBatch(ctx, c, batch); err != nil {
		return swarm.ZeroAddress, err
	}
	o, err := openOverlay(ctx, path)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	get := s.Through(c.Chunk)
	parent, err := snapshot.Read(ctx, get, o.Snapshot)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	if parent.Meta.Size != o.Size {
		return swarm.ZeroAddress, fmt.Errorf("%s is a %d-byte disk on a snapshot of %d bytes", path, o.Size, parent.Meta.Size)
	}
	return commit(ctx, c, s, get, keys, batch, commitSpec{
		parent:   parent,
		what:     "overlay",
		blocks:   func(fn func(map[int64][]byte) error) (int64, error) { return o.blocks(ctx, window, fn) },
		branch:   branch,
		prefetch: prefetch,
		// The overlay moves to the new snapshot before the update is
		// published: if the publish fails, the next checkpoint finds the
		// snapshot under the overlay and publishes it first.
		uploaded: func(snap swarm.Address) error {
			if err := o.rebase(ctx, snap); err != nil {
				return fmt.Errorf("snapshot %s is uploaded but %s still sits on %s: %w", snap, path, parent.Ref, err)
			}
			return nil
		},
	}, w)
}

// commitSpec says what commit turns into a snapshot.
type commitSpec struct {
	parent      snapshot.Snapshot
	what        string                                            // where the blocks come from, for reports
	blocks      func(func(map[int64][]byte) error) (int64, error) // passes the changed blocks, nil for zeros, a window at a time
	branch      bool                                              // start a new lineage
	prefetch    []byte                                            // a boot trace, or nil to keep the parent's
	consistency string                                            // "application" if the guest's filesystems were frozen
	uploaded    func(swarm.Address) error                         // runs once the snapshot is synced, before it is published
}

// commit turns blocks that changed on spec.parent into the next snapshot
// (SPEC §8, steps 2 to 7). It rebuilds the parent's tree with the blocks,
// writes a snapshot manifest, uploads every chunk that differs from the
// parent's, the manifest's root last, and waits until all are synced. Then
// it runs spec.uploaded and publishes the snapshot as the next update of the
// parent's lineage, or of a new one (SPEC §7). Chunks go to s, and get
// supplies the parent's. It reports progress to w and returns the snapshot's
// reference, or the parent's if the blocks change nothing and spec.prefetch
// is nil or the parent's trace: a new trace alone makes a new snapshot.
func commit(ctx context.Context, c *bee.Client, s *store.Store, get storage.Getter, keys, batch string, spec commitSpec, w io.Writer) (swarm.Address, error) {
	release, err := s.Hold() // the new chunks exist only in s until synced
	if err != nil {
		return swarm.ZeroAddress, err
	}
	defer release()
	parent := spec.parent
	start := time.Now()
	root := parent.Meta.DiskRoot
	n, err := spec.blocks(func(blocks map[int64][]byte) error {
		r, chunks, err := tree.Rebuild(ctx, get, root, blocks)
		if err != nil {
			return err
		}
		for _, ch := range chunks {
			if err := s.Put(ctx, ch); err != nil {
				return err
			}
		}
		root = r
		return nil
	})
	if err != nil {
		return swarm.ZeroAddress, err
	}
	fmt.Fprintf(w, "%s: %d blocks on snapshot %s, rebuilt in %v\n", spec.what, n, parent.Ref, time.Since(start).Round(time.Millisecond))

	next, err := followLineage(ctx, c, keys, batch, parent, spec.branch, w)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	if root.Equal(parent.Meta.DiskRoot) {
		same, err := sameTrace(ctx, get, parent, spec.prefetch)
		if err != nil {
			return swarm.ZeroAddress, err
		}
		if same {
			fmt.Fprintf(w, "no changes since snapshot %s\n", parent.Ref)
			return parent.Ref, nil
		}
		fmt.Fprintf(w, "%s: no blocks changed, but the boot trace is new\n", spec.what)
	}
	if next.key == nil {
		if next.l, next.key, err = lineage.Create(keys); err != nil {
			return swarm.ZeroAddress, err
		}
		fmt.Fprintf(w, "lineage %s: new, from snapshot %s\n", next.l, parent.Ref)
	}
	index := uint64(0)
	if next.prev != nil {
		index = next.prev.Index + 1
	}

	ranges, err := tree.Diff(ctx, get, parent.Meta.DiskRoot, root)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	order, err := tree.Changes(ctx, get, parent.Meta.DiskRoot, root)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	meta := snapshot.NewMeta(parent.Meta.Size, root)
	meta.Parent = &parent.Ref
	meta.Lineage = next.l.Meta(index)
	meta.Encryption = parent.Meta.Encryption
	if spec.consistency != "" {
		meta.Consistency = spec.consistency
	}
	meta.Batches = []string{batch}
	for _, b := range parent.Meta.Batches {
		if !slices.Contains(meta.Batches, b) {
			meta.Batches = append(meta.Batches, b)
		}
	}
	for _, r := range ranges {
		meta.Stats.ChangedBlocks += r.Count
	}
	meta.Stats.NewChunks = int64(len(order))
	prefetch := spec.prefetch
	if prefetch == nil {
		runs, err := snapshot.ReadTrace(ctx, get, parent)
		if err != nil {
			return swarm.ZeroAddress, err
		}
		if runs != nil {
			prefetch = snapshot.EncodeTrace(runs)
		}
	}

	var mu sync.Mutex // the manifest's nodes are saved concurrently
	seen := map[string]bool{}
	for _, a := range order {
		seen[a.ByteString()] = true
	}
	snap, err := snapshot.Write(ctx, putterFunc(func(ctx context.Context, ch swarm.Chunk) error {
		mu.Lock()
		if !seen[ch.Address().ByteString()] {
			seen[ch.Address().ByteString()] = true
			order = append(order, ch.Address())
		}
		mu.Unlock()
		return s.Put(ctx, ch)
	}), s, meta, prefetch)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	if i := slices.IndexFunc(order, snap.Equal); i >= 0 && i < len(order)-1 { // the manifest's root last
		order = append(slices.Delete(order, i, i+1), snap)
	}
	fmt.Fprintf(w, "snapshot: %s, %d blocks changed, %d chunks to upload\n", snap, meta.Stats.ChangedBlocks, len(order))
	if err := upload(ctx, c, s, batch, order, w); err != nil {
		return swarm.ZeroAddress, err
	}
	if spec.uploaded != nil {
		if err := spec.uploaded(snap); err != nil {
			return swarm.ZeroAddress, err
		}
	}
	u := lineage.Update{Index: index, Snapshot: snap, Time: meta.Created}
	if err := lineage.Publish(ctx, c, batch, next.l, next.key, next.prev, u); err != nil {
		return swarm.ZeroAddress, fmt.Errorf("snapshot %s is uploaded, but publishing it failed; checkpoint again to retry: %w", snap, err)
	}
	fmt.Fprintf(w, "lineage %s: update %d is snapshot %s\n", next.l, index, snap)
	return snap, nil
}

// sameTrace reports whether the boot trace prefetch, if not nil, is the one
// that parent has, so that a snapshot with the same disk would add nothing.
func sameTrace(ctx context.Context, get storage.Getter, parent snapshot.Snapshot, prefetch []byte) (bool, error) {
	if prefetch == nil {
		return true, nil
	}
	runs, err := snapshot.ReadTrace(ctx, get, parent)
	if err != nil {
		return false, err
	}
	return runs != nil && bytes.Equal(snapshot.EncodeTrace(runs), prefetch), nil
}

// successor says where the snapshot after a parent goes: after update prev
// of lineage l, signed with key, or, with no key, at update 0 of a new
// lineage.
type successor struct {
	l    lineage.Lineage
	key  *ecdsa.PrivateKey
	prev *lineage.Update
}

// followLineage decides where the snapshot after parent goes. It continues
// parent's lineage if this host holds the lineage's key and parent is its
// head (SPEC §7), and otherwise starts a new lineage, unless the head is
// elsewhere in a lineage this host writes: that needs branch. A parent that
// an interrupted checkpoint uploaded but did not publish is published first,
// when the head is its own parent.
func followLineage(ctx context.Context, c *bee.Client, keys, batch string, parent snapshot.Snapshot, branch bool, w io.Writer) (successor, error) {
	m := parent.Meta.Lineage
	if branch || m == nil {
		return successor{}, nil
	}
	l, i, err := lineage.FromMeta(*m)
	if err != nil {
		return successor{}, fmt.Errorf("snapshot %s: %w", parent.Ref, err)
	}
	key, err := lineage.LoadKey(keys, l.Owner)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(w, "lineage %s: another host writes it\n", l)
		return successor{}, nil
	}
	if err != nil {
		return successor{}, err
	}
	self, err := publishedAt(ctx, c, batch, l, key, i, parent, w)
	if err != nil {
		return successor{}, fmt.Errorf("%w; retry if Bee lags, or checkpoint with -branch to start a new lineage", err)
	}
	return successor{l, key, &self}, nil
}

// publishedAt makes sure that snapshot snap is update i of lineage l, the
// lineage's head. If a checkpoint uploaded snap but stopped before
// publishing it, snap's own parent is the head, and publishedAt publishes
// snap, signed with key. It returns the update.
func publishedAt(ctx context.Context, c *bee.Client, batch string, l lineage.Lineage, key *ecdsa.PrivateKey, i uint64, snap snapshot.Snapshot, w io.Writer) (lineage.Update, error) {
	self := lineage.Update{Index: i, Snapshot: snap.Ref, Time: snap.Meta.Created}
	head, err := lineage.Head(ctx, c, l)
	none := errors.Is(err, bee.ErrNoUpdate)
	switch {
	case err != nil && !none:
		return lineage.Update{}, err
	case !none && head.Index == i && head.Snapshot.Equal(snap.Ref):
		return head, nil
	case none && i == 0, !none && i > 0 && head.Index == i-1 && snap.Meta.Parent != nil && head.Snapshot.Equal(*snap.Meta.Parent):
		var prev *lineage.Update
		if !none {
			prev = &head
		}
		fmt.Fprintf(w, "lineage %s: publishing update %d, snapshot %s, which an earlier checkpoint left unpublished\n", l, i, snap.Ref)
		if err := lineage.Publish(ctx, c, batch, l, key, prev, self); err != nil {
			return lineage.Update{}, err
		}
		return self, nil
	}
	found := "no update"
	if !none {
		found = fmt.Sprintf("update %d (snapshot %s)", head.Index, head.Snapshot)
	}
	return lineage.Update{}, fmt.Errorf("lineage %s: %w: Bee finds %s, but snapshot %s is update %d", l, lineage.ErrNotHead, found, snap.Ref, i)
}
