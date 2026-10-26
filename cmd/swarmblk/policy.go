package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/engine"
	"github.com/petfold/swarmblk/internal/epoch"
	"github.com/petfold/swarmblk/internal/snapshot"
	"github.com/petfold/swarmblk/internal/store"
)

// A writable disk's hydration policy (DESIGN §3), recorded in its
// directory's policy file:
const (
	policyLazy    = "lazy"    // reads fetch what they need, into the bounded cache
	policyHydrate = "hydrate" // a local copy of the disk, filled in the background
	policyRestamp = "restamp" // every chunk stamped again under this host's batch
)

// Files in a disk's directory for its policy.
const (
	policyFile  = "policy"
	imageDir    = "image"
	restampFile = "restamp.json"
)

// diskPolicy returns the policy of the disk in dir: arg if not empty, which
// it records, or else the recorded one, and lazy if none is. A disk that no
// longer hydrates loses its local copy.
func diskPolicy(dir, name, arg string, w io.Writer) (string, error) {
	policy := arg
	b, err := os.ReadFile(filepath.Join(dir, policyFile))
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return "", err
	case policy == "":
		policy = strings.TrimSpace(string(b))
	}
	if policy == "" {
		policy = policyLazy
	}
	if !slices.Contains([]string{policyLazy, policyHydrate, policyRestamp}, policy) {
		return "", fmt.Errorf("policy %q: use lazy, hydrate or restamp", policy)
	}
	if strings.TrimSpace(string(b)) != policy {
		if err := os.WriteFile(filepath.Join(dir, policyFile), []byte(policy+"\n"), 0o600); err != nil {
			return "", err
		}
	}
	if policy != policyHydrate {
		if _, err := os.Stat(filepath.Join(dir, imageDir)); err == nil {
			fmt.Fprintf(w, "disk %s: policy %s; removing its local copy\n", name, policy)
			if err := os.RemoveAll(filepath.Join(dir, imageDir)); err != nil {
				return "", err
			}
		}
	}
	return policy, nil
}

// openImage opens the local copy of a hydrating disk on base, with epochs
// eps on it, and gives it to snap. A copy of another disk starts again,
// except one that a checkpoint moved to the snapshot of the frozen epoch
// eps[0] before it stopped: reads may use it, since that epoch holds every
// block in which it differs from base.
func openImage(ctx context.Context, get storage.Getter, dir string, base snapshot.Snapshot, eps []*epoch.Epoch, snap *engine.Snapshot) (*engine.Image, error) {
	img, err := engine.OpenImage(filepath.Join(dir, imageDir), base.Meta.Size)
	if err != nil {
		return nil, err
	}
	root := img.Root()
	if root.Equal(base.Meta.DiskRoot) {
		snap.SetImage(img, swarm.ZeroAddress)
		return img, nil
	}
	if len(eps) > 1 && eps[0].Super().Snapshot != nil {
		next, err := snapshot.Read(ctx, get, *eps[0].Super().Snapshot)
		if err == nil && root.Equal(next.Meta.DiskRoot) {
			snap.SetImage(img, root)
			return img, nil
		}
	}
	if err := img.Reset(base.Meta.DiskRoot); err != nil {
		img.Close()
		return nil, err
	}
	snap.SetImage(img, swarm.ZeroAddress)
	return img, nil
}

// hydrateWorkers is how many blocks a disk's hydration fetches at once.
const hydrateWorkers = 16

// keepHydrated fills the disk's local copy until ctx ends (DESIGN §3): it
// hydrates the disk's snapshot, starts again on the next one after a
// checkpoint, and tries again a minute after a run that left blocks out. get
// fetches from Bee without keeping chunks in the cache. It reports progress
// to w every minute, and when the copy is complete.
func (ds *diskState) keepHydrated(ctx context.Context, get storage.Getter, w io.Writer) {
	for ctx.Err() == nil {
		run, cancel := context.WithCancel(ctx)
		start := time.Now()
		h, err := ds.disk.Snapshot().Hydrate(run, get, hydrateWorkers)
		if err != nil {
			cancel()
			fmt.Fprintf(w, "disk %s: hydration: %v\n", ds.name, err)
			select {
			case <-ds.moved:
			case <-ctx.Done():
			}
			continue
		}
		tick := time.NewTicker(time.Minute)
		moved := false
	wait:
		for {
			select {
			case <-h.Done():
				break wait
			case <-ds.moved:
				moved = true
				cancel()
			case <-tick.C:
				_, failed, present, blocks := h.Progress()
				fmt.Fprintf(w, "disk %s: hydrating, %d of %d blocks local (%.1f%%), %d failed\n", ds.name, present, blocks, 100*float64(present)/float64(blocks), failed)
			}
		}
		tick.Stop()
		cancel()
		fetched, failed, present, blocks := h.Progress()
		switch {
		case moved:
			continue
		case ctx.Err() != nil:
			return
		case present == blocks:
			if fetched > 0 {
				fmt.Fprintf(w, "disk %s: hydrated, all %d blocks local; %d fetched in %v\n", ds.name, blocks, fetched, time.Since(start).Round(time.Second))
			}
			select {
			case <-ds.moved:
			case <-ctx.Done():
			}
		default:
			fmt.Fprintf(w, "disk %s: hydration left %d of %d blocks out, %d could not be fetched; trying again in a minute\n", ds.name, blocks-present, blocks, failed)
			select {
			case <-time.After(time.Minute):
			case <-ds.moved:
			case <-ctx.Done():
			}
		}
	}
}

// restampRecord is what a disk has re-stamped: a snapshot every chunk of
// which carries a stamp from one of batches.
type restampRecord struct {
	Snapshot swarm.Address `json:"snapshot"`
	Batches  []string      `json:"batches"`
}

func (ds *diskState) readRestamp() (restampRecord, bool) {
	var r restampRecord
	b, err := os.ReadFile(filepath.Join(ds.dir, restampFile))
	return r, err == nil && json.Unmarshal(b, &r) == nil
}

func (ds *diskState) writeRestamp(r restampRecord) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(ds.dir, restampFile+".new")
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(ds.dir, restampFile))
}

// restamp stamps every chunk of the disk's snapshot again under batch
// (DESIGN §5, adoption), unless the disk's record shows them covered. It
// fetches each distinct chunk once, from the store if there or else from
// Bee without keeping it, uploads them over one stream, and waits until Bee
// has synced them; then it records the snapshot. get supplies the
// manifests and the tree's intermediate chunks. Checkpoints with the same
// batch keep the record up to date (promote).
func (ds *diskState) restamp(ctx context.Context, c *bee.Client, s *store.Store, get storage.Getter, batch string, w io.Writer) error {
	ds.mu.Lock()
	snap := ds.base.Ref
	ds.mu.Unlock()
	if r, ok := ds.readRestamp(); ok && r.Snapshot.Equal(snap) && slices.Contains(r.Batches, batch) {
		return nil
	}
	if err := checkBatch(ctx, c, batch); err != nil {
		return err
	}
	start := time.Now()
	var addrs []swarm.Address
	if err := snapshot.Chunks(ctx, get, snap, func(a swarm.Address) error {
		addrs = append(addrs, a)
		return nil
	}); err != nil {
		return err
	}
	fmt.Fprintf(w, "disk %s: re-stamping the %d chunks of snapshot %s with batch %s\n", ds.name, len(addrs), snap, batch)
	fetch := retry(c.Chunk)
	once := storage.GetterFunc(func(ctx context.Context, a swarm.Address) (swarm.Chunk, error) {
		if ch, err := s.Get(ctx, a); err == nil {
			return ch, nil
		}
		return fetch(ctx, a)
	})
	if err := uploadFrom(ctx, c, once, batch, addrs, w); err != nil {
		return err
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()
	// A checkpoint meanwhile stamped its new chunks with batch too.
	if err := ds.writeRestamp(restampRecord{Snapshot: ds.base.Ref, Batches: []string{batch}}); err != nil {
		return err
	}
	fmt.Fprintf(w, "disk %s: re-stamped snapshot %s in %v\n", ds.name, snap, time.Since(start).Round(time.Second))
	return nil
}

// parseSize reads a size in bytes, with an optional K, M, G or T suffix
// for powers of 1024.
func parseSize(arg string) (int64, error) {
	s := strings.TrimSpace(strings.ToUpper(arg))
	shift := 0
	if n := len(s); n > 0 {
		if i := strings.IndexByte("KMGT", s[n-1]); i >= 0 {
			shift = 10 * (i + 1)
			s = s[:n-1]
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 0 || v > (1<<62)>>shift {
		return 0, fmt.Errorf("size %q: use bytes, or a number with K, M, G or T", arg)
	}
	return v << shift, nil
}
