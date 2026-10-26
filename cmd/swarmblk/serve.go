package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/engine"
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
	fs.StringVar(&cfg.disk, "disk", "", "serve the disk with this `name`, writable, from its write epochs, made on the snapshot named if new")
	fs.StringVar(&cfg.batch, "batch", os.Getenv("SWARMBLK_BATCH"), "immutable postage batch `id` that a disk's checkpoints stamp with ($SWARMBLK_BATCH if set)")
	fs.DurationVar(&cfg.every, "every", 0, "checkpoint a disk at this `interval`, if it changed")
	fs.StringVar(&cfg.qga, "qga", "", "freeze the guest's filesystems for checkpoints through the QEMU guest agent on this Unix `socket`")
	fs.StringVar(&cfg.policy, "policy", "", "a disk's hydration `policy`: lazy, hydrate or restamp (default the disk's own, else lazy)")
	cache := fs.String("cache", "4G", "bound the local chunk cache to this `size`, removing the leaves used least recently; 0 for no bound")
	fs.StringVar(&cfg.record, "record", "", "write the guest's reads to this `file` as a boot trace when serve stops")
	fs.StringVar(&cfg.prefetch, "prefetch", "", "prefetch from the boot trace in this `file` instead of the snapshot's")
	fs.Int64Var(&cfg.window, "window", 16384, "prefetch at most this many `blocks` of the trace ahead of the guest")
	nocache := fs.Bool("nocache", false, "ask Bee not to keep the chunks it retrieves (Swarm-Cache: false), so that measurements stay cold")
	fs.Parse(args)
	if fs.NArg() > 1 || cfg.root != "" && (fs.NArg() == 1 || cfg.disk != "") || cfg.root == "" && cfg.disk == "" && fs.NArg() == 0 {
		usage()
	}
	var err error
	if cfg.cache, err = parseSize(*cache); err != nil {
		log.Fatal(err)
	}
	cfg.arg = fs.Arg(0)
	cfg.disks = filepath.Join(*dir, "disks")
	cfg.keys = filepath.Join(*dir, "keys")
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
	arg, root string        // the snapshot or lineage, or else the disk root
	disk      string        // a writable disk's name
	disks     string        // the directory of writable disks
	batch     string        // what a disk's checkpoints stamp with
	keys      string        // where lineage keys live
	every     time.Duration // how often to checkpoint a disk, if at all
	qga       string        // the guest agent's socket, if any
	policy    string        // a disk's hydration policy, if given
	cache     int64         // the bound on the chunk cache, in bytes, or 0
	sock      string        // the Unix socket to listen on
	record    string        // where to write the boot trace, if anywhere
	prefetch  string        // a boot trace to prefetch from instead of the snapshot's
	window    int64         // blocks of the trace to prefetch ahead of the guest
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
// under cfg.root; or writable, the disk cfg.disk, from its write epochs
// (SPEC §11). The export is named after the snapshot's reference, the root
// or the disk, and serve prints its URI for QEMU to out:
// nbd+unix:///<name>?socket=<socket>. It prefetches from the snapshot's boot
// trace, or from cfg.prefetch, ahead of the guest (SPEC §6), and can record
// a new trace. A writable disk takes live checkpoints (SPEC §8): asked for
// on its control socket, every cfg.every, and at start if an earlier one
// stopped part way. It hydrates or re-stamps as its policy says (DESIGN
// §3), and serve bounds the chunk cache to cfg.cache bytes. It reports to
// w.
func serve(ctx context.Context, c *bee.Client, s *store.Store, cfg serveConfig, out, w io.Writer) error {
	switch {
	case cfg.disk == "" && (cfg.every > 0 || cfg.qga != ""):
		return errors.New("checkpoints are for a writable disk: serve it with -disk")
	case cfg.disk == "" && cfg.policy != "":
		return errors.New("hydration policies are for a writable disk: serve it with -disk")
	case cfg.every > 0 && cfg.batch == "":
		return errors.New("periodic checkpoints need a batch to stamp with")
	}
	w = &syncWriter{w: w}
	var fetched atomic.Int64
	fetch := retry(func(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
		ch, err := c.Chunk(ctx, addr)
		if err == nil {
			fetched.Add(1)
		}
		return ch, err
	})
	get := s.Through(fetch)
	var snap *engine.Snapshot
	var dev nbd.Device
	var ds *diskState
	var name, desc string
	var trace []tree.Range
	var err error
	switch {
	case cfg.disk != "":
		if ds, err = openDisk(ctx, c, s, get, cfg.disks, cfg.disk, cfg.arg, cfg.policy, w); err != nil {
			return err
		}
		defer func() {
			if err := ds.close(); err != nil {
				fmt.Fprintf(w, "serve: closing disk %s: %v\n", cfg.disk, err)
			}
		}()
		snap, dev = ds.disk.Snapshot(), ds.disk
		name, desc = cfg.disk, "swarmblk disk "+cfg.disk
		// A boot trace only speeds reads up, and a hydrated disk needs none:
		// without the network it can still start.
		if trace, err = snapshot.ReadTrace(ctx, get, ds.base); err != nil {
			fmt.Fprintf(w, "serve: disk %s: no boot trace: %v\n", cfg.disk, err)
			trace = nil
		}
	case cfg.root != "":
		r, err := swarm.ParseHexAddress(cfg.root)
		if err != nil || len(r.Bytes()) != swarm.HashSize {
			return fmt.Errorf("disk root %q is not 64 hex digits", cfg.root)
		}
		if snap, err = engine.Open(ctx, get, r, 0); err != nil {
			return err
		}
		dev, name, desc = snap, r.String(), "swarmblk disk tree "+r.String()
	default:
		ref, err := resolve(ctx, c, get, cfg.arg, w)
		if err != nil {
			return err
		}
		if snap, err = engine.Open(ctx, get, ref.Meta.DiskRoot, 0); err != nil {
			return err
		}
		dev, name, desc = snap, ref.Ref.String(), "swarmblk snapshot "+ref.Ref.String()
		if trace, err = snapshot.ReadTrace(ctx, get, ref); err != nil {
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
	l, err := listen(cfg.sock)
	if err != nil {
		return err
	}
	defer os.Remove(cfg.sock)
	var rec *engine.Recorder
	if cfg.record != "" {
		rec = snap.Record()
	}
	var pf *engine.Prefetch
	if len(trace) > 0 {
		pf = snap.Prefetch(ctx, trace, cfg.window)
		fmt.Fprintf(w, "serve: prefetching %d blocks of the boot trace, up to %d ahead of the guest\n", pf.Blocks(), cfg.window)
	}

	// The cache sweeps, and a writable disk's checkpoints, hydration and
	// re-stamping, run beside the export; serve waits for them before the
	// disk closes.
	var bg sync.WaitGroup
	defer bg.Wait()
	if cfg.cache > 0 {
		bg.Add(1)
		go func() {
			defer bg.Done()
			sweepCache(ctx, s, cfg.cache, w)
		}()
	}
	mode := "read-only"
	if ds != nil {
		mode = fmt.Sprintf("writable, epoch %d holding %d blocks, on snapshot %s, policy %s", ds.disk.Active().Super().Epoch, len(ds.disk.Active().Blocks()), ds.base.Ref, ds.policy)
		switch {
		case ds.policy == policyHydrate:
			bg.Add(1)
			go func() {
				defer bg.Done()
				ds.keepHydrated(ctx, storage.GetterFunc(fetch), w)
			}()
		case ds.policy == policyRestamp && cfg.batch == "":
			fmt.Fprintf(w, "serve: disk %s: re-stamping needs a batch to stamp with\n", ds.name)
		case ds.policy == policyRestamp:
			bg.Add(1)
			go func() {
				defer bg.Done()
				if err := ds.restamp(ctx, c, s, get, cfg.batch, w); err != nil && ctx.Err() == nil {
					fmt.Fprintf(w, "serve: disk %s: re-stamping: %v\n", ds.name, err)
				}
			}()
		}
		run := func(how string, w io.Writer) (swarm.Address, error) {
			snap, err := checkpointDisk(ctx, ds, c, s, get, cfg.keys, cfg.batch, cfg.qga, w)
			if err != nil {
				fmt.Fprintf(w, "serve: %s checkpoint of disk %s: %v\n", how, ds.name, err)
			}
			return snap, err
		}
		ctl, err := listen(filepath.Join(ds.dir, "control.sock"))
		if err != nil {
			return err
		}
		defer os.Remove(filepath.Join(ds.dir, "control.sock"))
		bg.Add(1)
		go func() {
			defer bg.Done()
			serveControl(ctx, ctl, run)
		}()
		if len(ds.disk.Frozen()) > 0 && cfg.batch != "" {
			bg.Add(1)
			go func() {
				defer bg.Done()
				run("the unfinished", w)
			}()
		}
		if cfg.every > 0 {
			bg.Add(1)
			go func() {
				defer bg.Done()
				t := time.NewTicker(cfg.every)
				defer t.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-t.C:
						run("a periodic", w)
					}
				}
			}()
		}
	}
	start := time.Now()
	fmt.Fprintf(w, "serve: a %d MiB disk, %s, as %s\n", snap.Size()>>20, mode, name)
	fmt.Fprintf(out, "nbd+unix:///%s?socket=%s\n", name, cfg.sock)
	srv := &nbd.Server{Exports: []nbd.Export{{Name: name, Description: desc, Device: dev, ReadOnly: ds == nil}}, ErrorLog: log.New(w, "serve: ", 0)}
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

// syncWriter lets goroutines write to one writer.
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// controlMessage is a line on a disk's control socket: a request, a line of
// a checkpoint's log, or its outcome.
type controlMessage struct {
	Request  string `json:"request,omitempty"`
	Log      string `json:"log,omitempty"`
	Snapshot string `json:"snapshot,omitempty"`
	Error    string `json:"error,omitempty"`
}

// serveControl answers checkpoint requests on the disk's control socket
// until ctx ends: each runs a checkpoint, sending its log back and then the
// snapshot or the error. It returns when the checkpoints it ran have ended.
func serveControl(ctx context.Context, l net.Listener, run func(string, io.Writer) (swarm.Address, error)) {
	stop := context.AfterFunc(ctx, func() { l.Close() })
	defer stop()
	var conns sync.WaitGroup
	defer conns.Wait()
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		conns.Add(1)
		go func() {
			defer conns.Done()
			defer conn.Close()
			stop := context.AfterFunc(ctx, func() { conn.Close() })
			defer stop()
			var req controlMessage
			if err := json.NewDecoder(conn).Decode(&req); err != nil {
				return
			}
			enc := json.NewEncoder(conn)
			if req.Request != "checkpoint" {
				enc.Encode(controlMessage{Error: fmt.Sprintf("unknown request %q", req.Request)})
				return
			}
			snap, err := run("a requested", writerFunc(func(p []byte) (int, error) {
				return len(p), enc.Encode(controlMessage{Log: string(p)})
			}))
			if err != nil {
				enc.Encode(controlMessage{Error: err.Error()})
				return
			}
			enc.Encode(controlMessage{Snapshot: snap.String()})
		}()
	}
}

// writerFunc lets a function serve as an io.Writer.
type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// sweepInterval is how often serve sweeps the chunk cache.
var sweepInterval = 5 * time.Minute

// sweepCache keeps the store within limit bytes until ctx ends, sweeping it
// at once and then every sweepInterval, and reports what each sweep
// removes to w.
func sweepCache(ctx context.Context, s *store.Store, limit int64, w io.Writer) {
	for {
		sw, err := s.Sweep(limit)
		switch {
		case err != nil:
			fmt.Fprintf(w, "serve: sweeping the chunk cache: %v\n", err)
		case sw.Removed > 0:
			fmt.Fprintf(w, "serve: chunk cache: removed %d leaves (%d MiB) used least recently; %d MiB left\n", sw.Removed, sw.Freed>>20, sw.Bytes>>20)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(sweepInterval):
		}
	}
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
