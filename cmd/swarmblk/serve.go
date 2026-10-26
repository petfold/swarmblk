package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/engine"
	"github.com/petfold/swarmblk/internal/epoch"
	"github.com/petfold/swarmblk/internal/nbd"
	"github.com/petfold/swarmblk/internal/snapshot"
	"github.com/petfold/swarmblk/internal/store"
	"github.com/petfold/swarmblk/internal/tree"
)

func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	beeURL := fs.String("bee", envOr("SWARMBLK_BEE_URL", "http://localhost:1633"), "Bee API `url` ($SWARMBLK_BEE_URL if set)")
	dir := fs.String("home", home(), "local state `directory` ($SWARMBLK_HOME if set)")
	var cfg serveConfig
	fs.StringVar(&cfg.sock, "socket", "", "Unix `socket` to listen on (default nbd.sock in the local state directory)")
	fs.StringVar(&cfg.root, "root", "", "serve the disk tree under this `root` instead of a snapshot")
	fs.StringVar(&cfg.disk, "disk", "", "serve the disk with this `name`, writable, from its write epoch, made on the snapshot named if new")
	fs.StringVar(&cfg.record, "record", "", "write the guest's reads to this `file` as a boot trace when serve stops")
	fs.StringVar(&cfg.prefetch, "prefetch", "", "prefetch from the boot trace in this `file` instead of the snapshot's")
	fs.Int64Var(&cfg.window, "window", 16384, "prefetch at most this many `blocks` of the trace ahead of the guest")
	nocache := fs.Bool("nocache", false, "ask Bee not to keep the chunks it retrieves (Swarm-Cache: false), so that measurements stay cold")
	fs.Parse(args)
	if fs.NArg() > 1 || cfg.root != "" && (fs.NArg() == 1 || cfg.disk != "") || cfg.root == "" && cfg.disk == "" && fs.NArg() == 0 {
		usage()
	}
	cfg.arg = fs.Arg(0)
	cfg.disks = filepath.Join(*dir, "disks")
	s, err := store.Open(filepath.Join(*dir, "chunks"))
	if err != nil {
		log.Fatal(err)
	}
	if cfg.sock == "" {
		cfg.sock = filepath.Join(*dir, "nbd.sock")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c := bee.New(*beeURL, engine.Fetchers)
	c.NoCache = *nocache
	if err := serve(ctx, c, s, cfg, os.Stdout, os.Stderr); err != nil {
		log.Fatal(err)
	}
}

// serveConfig says what serve exports and how.
type serveConfig struct {
	arg, root string // the snapshot or lineage, or else the disk root
	disk      string // a writable disk's name
	disks     string // the directory of writable disks
	sock      string // the Unix socket to listen on
	record    string // where to write the boot trace, if anywhere
	prefetch  string // a boot trace to prefetch from instead of the snapshot's
	window    int64  // blocks of the trace to prefetch ahead of the guest
}

// fetch gets a chunk from Bee and checks it.
type fetch func(context.Context, swarm.Address) (swarm.Chunk, error)

// retry tries each chunk up to four times, waiting 0.5, 1 and 2 seconds in
// between, since a retrieval from Swarm can fail and succeed soon after. A
// chunk that comes back wrong is not tried again.
func retry(f fetch) fetch {
	return func(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
		wait := 500 * time.Millisecond
		for attempt := 1; ; attempt++ {
			ch, err := f(ctx, addr)
			if err == nil || attempt == 4 || errors.Is(err, bee.ErrInvalidChunk) || ctx.Err() != nil {
				return ch, err
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
			wait *= 2
		}
	}
}

// serve exports a disk over NBD on a Unix socket until ctx ends (DESIGN §8):
// read-only, the disk of the snapshot that cfg.arg names or the disk tree
// under cfg.root; or writable, the disk cfg.disk, from its write epoch
// (SPEC §11). The export is named after the snapshot's reference, the root
// or the disk, and serve prints its URI for QEMU to out:
// nbd+unix:///<name>?socket=<socket>. It prefetches from the snapshot's boot
// trace, or from cfg.prefetch, ahead of the guest (SPEC §6), and can record
// a new trace. It reports to w.
func serve(ctx context.Context, c *bee.Client, s *store.Store, cfg serveConfig, out, w io.Writer) error {
	var fetched atomic.Int64
	get := s.Through(retry(func(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
		ch, err := c.Chunk(ctx, addr)
		if err == nil {
			fetched.Add(1)
		}
		return ch, err
	}))
	var disk swarm.Address
	var name, desc string
	var trace []tree.Range
	var ep *epoch.Epoch
	if cfg.disk != "" {
		var snap snapshot.Snapshot
		var err error
		if ep, snap, err = openDisk(ctx, c, get, cfg, w); err != nil {
			return err
		}
		defer func() {
			if err := ep.Close(); err != nil {
				fmt.Fprintf(w, "serve: closing the epoch of disk %s: %v\n", cfg.disk, err)
			}
		}()
		disk, name, desc = snap.Meta.DiskRoot, cfg.disk, "swarmblk disk "+cfg.disk+" on snapshot "+snap.Ref.String()
		if trace, err = snapshot.ReadTrace(ctx, get, snap); err != nil {
			return err
		}
	} else if cfg.root != "" {
		r, err := swarm.ParseHexAddress(cfg.root)
		if err != nil || len(r.Bytes()) != swarm.HashSize {
			return fmt.Errorf("disk root %q is not 64 hex digits", cfg.root)
		}
		disk, name, desc = r, r.String(), "swarmblk disk tree "+r.String()
	} else {
		snap, err := resolve(ctx, c, get, cfg.arg, w)
		if err != nil {
			return err
		}
		disk, name, desc = snap.Meta.DiskRoot, snap.Ref.String(), "swarmblk snapshot "+snap.Ref.String()
		if trace, err = snapshot.ReadTrace(ctx, get, snap); err != nil {
			return err
		}
	}
	if cfg.prefetch != "" {
		b, err := os.ReadFile(cfg.prefetch)
		if err != nil {
			return err
		}
		if trace, err = snapshot.DecodeTrace(b); err != nil {
			return fmt.Errorf("%s: %w", cfg.prefetch, err)
		}
	}
	d, err := engine.Open(ctx, get, disk, 0)
	if err != nil {
		return err
	}
	var dev nbd.Device = d
	if ep != nil {
		d.SetLocal(s)
		if dev, err = engine.NewDisk(d, ep); err != nil {
			return err
		}
	}
	l, err := listen(cfg.sock)
	if err != nil {
		return err
	}
	defer os.Remove(cfg.sock)
	var rec *engine.Recorder
	if cfg.record != "" {
		rec = d.Record()
	}
	var pf *engine.Prefetch
	if len(trace) > 0 {
		pf = d.Prefetch(ctx, trace, cfg.window)
		fmt.Fprintf(w, "serve: prefetching %d blocks of the boot trace, up to %d ahead of the guest\n", pf.Blocks(), cfg.window)
	}
	start := time.Now()
	mode := "read-only"
	if ep != nil {
		mode = fmt.Sprintf("writable, epoch %d holding %d blocks", ep.Super().Epoch, len(ep.Blocks()))
	}
	fmt.Fprintf(w, "serve: a %d MiB disk, %s, as %s\n", d.Size()>>20, mode, name)
	fmt.Fprintf(out, "nbd+unix:///%s?socket=%s\n", name, cfg.sock)
	srv := &nbd.Server{Exports: []nbd.Export{{Name: name, Description: desc, Device: dev, ReadOnly: ep == nil}}, ErrorLog: log.New(w, "serve: ", 0)}
	err = srv.Serve(ctx, l)
	fmt.Fprintf(w, "serve: stopped after %v; %d chunks fetched from Bee\n", time.Since(start).Round(time.Second), fetched.Load())
	if pf != nil {
		done, failed := pf.Fetched()
		fmt.Fprintf(w, "serve: prefetched %d of %d trace blocks, %d failed\n", done, pf.Blocks(), failed)
	}
	if rec != nil {
		runs := rec.Runs()
		if werr := os.WriteFile(cfg.record, snapshot.EncodeTrace(runs), 0o600); werr != nil && err == nil {
			err = werr
		}
		var blocks int64
		for _, r := range runs {
			blocks += r.Count
		}
		fmt.Fprintf(w, "serve: wrote a boot trace of %d blocks in %d runs to %s\n", blocks, len(runs), cfg.record)
	}
	return err
}

// listen listens on a Unix socket at path that only this user can reach.
// It replaces a socket that nobody listens on any more, but not one in use.
func listen(path string) (net.Listener, error) {
	l, err := net.Listen("unix", path)
	if errors.Is(err, syscall.EADDRINUSE) {
		if c, err := net.Dial("unix", path); err == nil {
			c.Close()
			return nil, fmt.Errorf("%s: another server listens there", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
		l, err = net.Listen("unix", path)
	}
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}

var diskName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// openDisk opens the writable disk cfg.disk: its newest write epoch,
// recovered after a crash (SPEC §11), or, for a new disk, a first epoch on
// the snapshot that cfg.arg names. If cfg.arg names a snapshot for a disk
// that exists, it must be the one the disk is on. It returns the epoch and
// that snapshot.
func openDisk(ctx context.Context, c *bee.Client, get storage.Getter, cfg serveConfig, w io.Writer) (*epoch.Epoch, snapshot.Snapshot, error) {
	if !diskName.MatchString(cfg.disk) {
		return nil, snapshot.Snapshot{}, fmt.Errorf("disk name %q: use letters, digits, dots, dashes and underscores", cfg.disk)
	}
	dir := filepath.Join(cfg.disks, cfg.disk)
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, snapshot.Snapshot{}, err
	}
	var epochs []uint64
	for _, e := range entries {
		if n, err := strconv.ParseUint(e.Name(), 10, 64); err == nil && e.IsDir() {
			epochs = append(epochs, n)
		}
	}
	var named snapshot.Snapshot
	if cfg.arg != "" {
		if named, err = resolve(ctx, c, get, cfg.arg, w); err != nil {
			return nil, snapshot.Snapshot{}, err
		}
	}
	if len(epochs) == 0 {
		if cfg.arg == "" {
			return nil, snapshot.Snapshot{}, fmt.Errorf("disk %s is new: name the snapshot or lineage it starts from", cfg.disk)
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, snapshot.Snapshot{}, err
		}
		ep, err := epoch.Create(filepath.Join(dir, "1"), epoch.Super{Epoch: 1, Parent: named.Ref, Root: named.Meta.DiskRoot, Size: named.Meta.Size})
		if err != nil {
			return nil, snapshot.Snapshot{}, err
		}
		fmt.Fprintf(w, "serve: disk %s is new, on snapshot %s\n", cfg.disk, named.Ref)
		return ep, named, nil
	}
	n := slices.Max(epochs)
	ep, err := epoch.Open(filepath.Join(dir, strconv.FormatUint(n, 10)))
	if err != nil {
		return nil, snapshot.Snapshot{}, err
	}
	parent := ep.Super().Parent
	if cfg.arg != "" && !named.Ref.Equal(parent) {
		ep.Close()
		return nil, snapshot.Snapshot{}, fmt.Errorf("disk %s is on snapshot %s, not %s", cfg.disk, parent, named.Ref)
	}
	snap, err := snapshot.Read(ctx, get, parent)
	if err != nil {
		ep.Close()
		return nil, snapshot.Snapshot{}, err
	}
	return ep, snap, nil
}
