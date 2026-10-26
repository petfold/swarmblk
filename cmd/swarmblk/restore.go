package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"golang.org/x/sync/errgroup"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/lineage"
	"github.com/petfold/swarmblk/internal/snapshot"
	"github.com/petfold/swarmblk/internal/store"
	"github.com/petfold/swarmblk/internal/tree"
)

func cmdRestore(args []string) {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	beeURL := fs.String("bee", envOr("SWARMBLK_BEE_URL", "http://localhost:1633"), "Bee API `url` ($SWARMBLK_BEE_URL if set)")
	dir := fs.String("home", home(), "local state `directory` ($SWARMBLK_HOME if set)")
	fs.Parse(args)
	if fs.NArg() != 2 {
		usage()
	}
	s, err := store.Open(filepath.Join(*dir, "chunks"))
	if err != nil {
		log.Fatal(err)
	}
	snap, err := restore(context.Background(), bee.New(*beeURL, 4), s, *beeURL, fs.Arg(0), fs.Arg(1), os.Stderr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(snap)
}

func cmdExport(args []string) {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	beeURL := fs.String("bee", envOr("SWARMBLK_BEE_URL", "http://localhost:1633"), "Bee API `url` ($SWARMBLK_BEE_URL if set)")
	dir := fs.String("home", home(), "local state `directory` ($SWARMBLK_HOME if set)")
	fs.Parse(args)
	if fs.NArg() != 2 {
		usage()
	}
	s, err := store.Open(filepath.Join(*dir, "chunks"))
	if err != nil {
		log.Fatal(err)
	}
	snap, err := export(context.Background(), bee.New(*beeURL, fetchers), s, fs.Arg(0), fs.Arg(1), os.Stderr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(snap)
}

// resolve returns the snapshot that arg names: a manifest reference, or a
// lineage as 0x<owner>/<topic>, whose head it finds through Bee.
func resolve(ctx context.Context, c *bee.Client, get storage.Getter, arg string, w io.Writer) (snapshot.Snapshot, error) {
	if !strings.Contains(arg, "/") {
		ref, err := swarm.ParseHexAddress(arg)
		if err != nil || len(ref.Bytes()) != swarm.HashSize {
			return snapshot.Snapshot{}, fmt.Errorf("%q names neither a snapshot (64 hex digits) nor a lineage (0x<owner>/<topic>)", arg)
		}
		return snapshot.Read(ctx, get, ref)
	}
	l, err := lineage.Parse(arg)
	if err != nil {
		return snapshot.Snapshot{}, err
	}
	head, err := lineage.Head(ctx, c, l)
	if err != nil {
		return snapshot.Snapshot{}, fmt.Errorf("lineage %s: %w", l, err)
	}
	snap, err := snapshot.Read(ctx, get, head.Snapshot)
	if err != nil {
		return snapshot.Snapshot{}, err
	}
	if m := snap.Meta.Lineage; m == nil {
		return snapshot.Snapshot{}, fmt.Errorf("lineage %s: update %d names snapshot %s, which is in no lineage", l, head.Index, snap.Ref)
	} else if ml, i, err := lineage.FromMeta(*m); err != nil || ml != l || i != head.Index {
		return snapshot.Snapshot{}, fmt.Errorf("lineage %s: update %d names snapshot %s, whose meta.json places it elsewhere", l, head.Index, snap.Ref)
	}
	fmt.Fprintf(w, "lineage %s: the head is update %d, snapshot %s\n", l, head.Index, snap.Ref)
	return snap, nil
}

// backingName is the backing file name of an overlay on snap: QEMU reads the
// snapshot's disk.raw from the Bee API at api over HTTP, with Phase 0's
// options (PHASE0.md): requests time out after 60 s rather than QEMU's 5,
// and each reads 64 KiB ahead.
func backingName(api string, snap swarm.Address) (string, error) {
	u, err := url.Parse(api)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("Bee API %q is not an http or https URL", api)
	}
	opts, err := json.Marshal(map[string]any{
		"file.driver":    u.Scheme,
		"file.url":       strings.TrimSuffix(api, "/") + "/bzz/" + snap.String() + "/disk.raw",
		"file.readahead": "64k",
		"file.timeout":   60,
	})
	return "json:" + string(opts), err
}

// restore makes a qcow2 overlay at out on the snapshot that arg names, for a
// VM to run on: QEMU reads the snapshot's disk from Bee at api, and the
// VM's writes stay in the overlay until a checkpoint (SPEC §8). It returns
// the snapshot's reference.
func restore(ctx context.Context, c *bee.Client, s *store.Store, api, arg, out string, w io.Writer) (swarm.Address, error) {
	snap, err := resolve(ctx, c, s.Through(c.Chunk), arg, w)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	backing, err := backingName(api, snap.Ref)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	f, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	f.Close()
	if _, err := qemuImg(ctx, "create", "-q", "-u", "-f", "qcow2", "-F", "raw", "-b", backing, out, fmt.Sprint(snap.Meta.Size)); err != nil {
		os.Remove(out)
		return swarm.ZeroAddress, err
	}
	fmt.Fprintf(w, "overlay %s: a %d MiB disk on snapshot %s, read from %s\n", out, snap.Meta.Size>>20, snap.Ref, api)
	return snap.Ref, nil
}

// fetchers is how many chunks export fetches at once.
const fetchers = 32

// export writes the disk of the snapshot that arg names to a new raw image
// at out, and reports its SHA-256 (SPEC §14, I7). It walks the tree itself,
// fetching every chunk with GET /chunks and checking it, and leaves zero
// subtrees as holes. The tree's intermediate chunks stay in s; leaves are
// read from s when it has them, and are not kept. It returns the snapshot's
// reference.
func export(ctx context.Context, c *bee.Client, s *store.Store, arg, out string, w io.Writer) (swarm.Address, error) {
	start := time.Now()
	get := s.Through(c.Chunk)
	snap, err := resolve(ctx, c, get, arg, w)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	f, err := os.OpenFile(out, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	sum, blocks, err := exportTo(ctx, c, s, get, snap, f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(out)
		return swarm.ZeroAddress, fmt.Errorf("exporting snapshot %s: %w", snap.Ref, err)
	}
	fmt.Fprintf(w, "export %s: snapshot %s, %d MiB, %d blocks with data, sha256 %x, in %v\n", out, snap.Ref, snap.Meta.Size>>20, blocks, sum, time.Since(start).Round(time.Millisecond))
	return snap.Ref, nil
}

// exportTo writes snap's disk to f and returns its SHA-256 and the number of
// blocks that hold data.
func exportTo(ctx context.Context, c *bee.Client, s *store.Store, get storage.Getter, snap snapshot.Snapshot, f *os.File) ([32]byte, int64, error) {
	if err := f.Truncate(snap.Meta.Size); err != nil {
		return [32]byte{}, 0, err
	}
	type run struct {
		first, count int64
		leaf         swarm.Address
	}
	runs := make(chan run, fetchers)
	var blocks atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(runs)
		geo, err := tree.Walk(gctx, get, snap.Meta.DiskRoot, func(first, count int64, leaf swarm.Address) error {
			if leaf.Equal(tree.Zero(0)) {
				return nil // a hole
			}
			select {
			case runs <- run{first, count, leaf}:
				return nil
			case <-gctx.Done():
				return gctx.Err()
			}
		})
		if err == nil && geo.Size != snap.Meta.Size {
			err = fmt.Errorf("the disk tree describes %d bytes, meta.json %d", geo.Size, snap.Meta.Size)
		}
		return err
	})
	for range fetchers {
		g.Go(func() error {
			for r := range runs {
				ch, err := s.Get(gctx, r.leaf)
				if err != nil {
					ch, err = c.Chunk(gctx, r.leaf)
				}
				if err != nil {
					return err
				}
				if span := binary.LittleEndian.Uint64(ch.Data()); span != swarm.ChunkSize {
					return fmt.Errorf("leaf %s spans %d bytes; format 1 has only full leaves", r.leaf, span)
				}
				block := make([]byte, swarm.ChunkSize) // a payload may lack trailing zeros (SPEC §13)
				copy(block, ch.Data()[swarm.SpanSize:])
				for b := r.first; b < r.first+r.count; b++ {
					if _, err := f.WriteAt(block, b*swarm.ChunkSize); err != nil {
						return err
					}
				}
				blocks.Add(r.count)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return [32]byte{}, 0, err
	}
	if err := f.Sync(); err != nil {
		return [32]byte{}, 0, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, snap.Meta.Size)); err != nil {
		return [32]byte{}, 0, err
	}
	return [32]byte(h.Sum(nil)), blocks.Load(), nil
}
