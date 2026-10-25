package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/snapshot"
	"github.com/petfold/swarmblk/internal/store"
	"github.com/petfold/swarmblk/internal/tree"
)

func cmdImport(args []string) {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	beeURL := fs.String("bee", envOr("SWARMBLK_BEE_URL", "http://localhost:1633"), "Bee API `url` ($SWARMBLK_BEE_URL if set)")
	batch := fs.String("batch", os.Getenv("SWARMBLK_BATCH"), "immutable postage batch `id` to stamp with ($SWARMBLK_BATCH if set)")
	dir := fs.String("home", home(), "local state `directory` ($SWARMBLK_HOME if set)")
	fs.Parse(args)
	if fs.NArg() != 1 || *batch == "" {
		usage()
	}
	s, err := store.Open(filepath.Join(*dir, "chunks"))
	if err != nil {
		log.Fatal(err)
	}
	snap, err := importImage(context.Background(), bee.New(*beeURL, 64), s, *batch, fs.Arg(0), os.Stderr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(snap)
}

// putterFunc lets a function serve as Bee's storage.Putter.
type putterFunc func(context.Context, swarm.Chunk) error

func (f putterFunc) Put(ctx context.Context, ch swarm.Chunk) error { return f(ctx, ch) }

// importImage turns the raw disk image at path into a first snapshot: it
// builds the format-1 tree with Bee's pipeline into s, adds a snapshot
// manifest (SPEC §5), checks that batch has room, uploads every distinct
// chunk over one stream under a new tag with the manifest's root last, and
// waits until all of them are synced (SPEC §8). It reports progress to w
// and returns the snapshot's reference.
func importImage(ctx context.Context, c *bee.Client, s *store.Store, batch, path string, w io.Writer) (swarm.Address, error) {
	f, err := os.Open(path)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return swarm.ZeroAddress, err
	}
	g, err := tree.New(fi.Size())
	if err != nil {
		return swarm.ZeroAddress, fmt.Errorf("%s: %w", path, err)
	}
	b, err := c.Batch(ctx, batch)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	switch {
	case !b.Exists || !b.Usable:
		return swarm.ZeroAddress, fmt.Errorf("batch %s is not usable", batch)
	case !b.Immutable:
		return swarm.ZeroAddress, fmt.Errorf("batch %s is mutable; swarmblk stamps only with immutable batches (SPEC §9, P1)", batch)
	}

	// Build the tree into the store, keeping each distinct chunk once.
	start := time.Now()
	var (
		mu      sync.Mutex
		total   int
		changed int64 // blocks that are not all zero
		seen    = map[string]bool{}
		order   []swarm.Address
	)
	put := putterFunc(func(ctx context.Context, ch swarm.Chunk) error {
		if err := s.Put(ctx, ch); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		total++
		if binary.LittleEndian.Uint64(ch.Data()) == swarm.ChunkSize && !ch.Address().Equal(tree.Zero(0)) {
			changed++ // a leaf: format 1 has only full leaves, and no other chunk has this span
		}
		if k := ch.Address().ByteString(); !seen[k] {
			seen[k] = true
			order = append(order, ch.Address())
		}
		return nil
	})
	root, err := builder.FeedPipeline(ctx, builder.NewPipelineBuilder(ctx, put, false, redundancy.NONE), bufio.NewReaderSize(f, 1<<20))
	if err != nil {
		return swarm.ZeroAddress, err
	}
	fmt.Fprintf(w, "tree: %d MiB disk, %d chunks, %d distinct, root %s, built in %v\n", g.Size>>20, total, len(order), root, time.Since(start).Round(time.Second))

	meta := snapshot.NewMeta(g.Size, root)
	meta.Batches = []string{batch}
	meta.Stats = snapshot.Stats{ChangedBlocks: changed, NewChunks: int64(len(order))}
	snap, err := snapshot.Write(ctx, put, s, meta, nil)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	for i, a := range order { // the manifest's root last, so the snapshot is complete once it arrives
		if a.Equal(snap) {
			order = append(append(order[:i:i], order[i+1:]...), snap)
			break
		}
	}
	fmt.Fprintf(w, "snapshot: %s, %d blocks with data, %d chunks to upload\n", snap, changed, len(order))

	if err := checkRoom(ctx, c, batch, order, w); err != nil {
		return swarm.ZeroAddress, err
	}

	start = time.Now()
	tag, err := c.CreateTag(ctx)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	st, err := c.Stream(ctx, batch, tag.UID, 64)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	for i, a := range order {
		ch, err := s.Get(ctx, a)
		if err == nil {
			err = st.Put(ch)
		}
		if err != nil {
			st.Close()
			return swarm.ZeroAddress, fmt.Errorf("uploading chunk %d of %d: %w", i+1, len(order), err)
		}
		if (i+1)%max(len(order)/10, 1) == 0 {
			fmt.Fprintf(w, "upload: %d of %d chunks sent\n", i+1, len(order))
		}
	}
	if err := st.Close(); err != nil {
		return swarm.ZeroAddress, fmt.Errorf("upload: %d of %d chunks stored: %w", st.Acked(), len(order), err)
	}
	fmt.Fprintf(w, "upload: %d chunks stored under tag %d in %v; waiting for sync\n", len(order), tag.UID, time.Since(start).Round(time.Second))

	for last := time.Now(); ; {
		t, err := c.Tag(ctx, tag.UID)
		if err != nil {
			return swarm.ZeroAddress, err
		}
		if t.Done() {
			fmt.Fprintf(w, "sync: %d of %d synced after %v\n", t.Synced, t.Split-t.Seen, time.Since(start).Round(time.Second))
			return snap, nil
		}
		if time.Since(last) >= time.Minute {
			fmt.Fprintf(w, "sync: %d of %d synced\n", t.Synced, t.Split-t.Seen)
			last = time.Now()
		}
		select {
		case <-ctx.Done():
			return swarm.ZeroAddress, ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// errNoRoom means a batch's buckets could overflow with the chunks to upload.
var errNoRoom = errors.New("not enough room in the batch")

// checkRoom refuses an upload that could overflow a bucket of the batch, and
// warns when it would fill one past 90% (SPEC §9, P4). It counts every chunk
// as new, so it overestimates when some are already stamped in the batch.
func checkRoom(ctx context.Context, c *bee.Client, batch string, addrs []swarm.Address, w io.Writer) error {
	bk, err := c.Buckets(ctx, batch)
	if err != nil {
		return err
	}
	need := map[uint32]uint32{}
	for _, a := range addrs {
		need[binary.BigEndian.Uint32(a.Bytes())>>(32-bk.BucketDepth)]++
	}
	var fullest, at uint32
	for i, n := range need {
		if n+bk.Fill[i] > fullest {
			fullest, at = n+bk.Fill[i], i
		}
	}
	if fullest > bk.Capacity {
		return fmt.Errorf("%w: bucket %d would need %d slots of %d; dilute the batch or use a deeper one (SPEC §9, P4)", errNoRoom, at, fullest, bk.Capacity)
	}
	if fullest*10 > bk.Capacity*9 {
		fmt.Fprintf(w, "warning: bucket %d will be %d of %d full (SPEC §9, P4)\n", at, fullest, bk.Capacity)
	}
	return nil
}
