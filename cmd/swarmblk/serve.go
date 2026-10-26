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
	"sync/atomic"
	"syscall"
	"time"

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
	fs.StringVar(&cfg.record, "record", "", "write the guest's reads to this `file` as a boot trace when serve stops")
	fs.StringVar(&cfg.prefetch, "prefetch", "", "prefetch from the boot trace in this `file` instead of the snapshot's")
	fs.Int64Var(&cfg.window, "window", 16384, "prefetch at most this many `blocks` of the trace ahead of the guest")
	nocache := fs.Bool("nocache", false, "ask Bee not to keep the chunks it retrieves (Swarm-Cache: false), so that measurements stay cold")
	fs.Parse(args)
	if (fs.NArg() == 1) == (cfg.root != "") || fs.NArg() > 1 {
		usage()
	}
	cfg.arg = fs.Arg(0)
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

// serve exports a disk read-only over NBD on a Unix socket until ctx ends
// (DESIGN §8): the disk of the snapshot that cfg.arg names, or the disk tree
// under cfg.root. The export is named after the snapshot's reference, or the
// root, and serve prints its URI for QEMU to out:
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
	if cfg.root != "" {
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
	fmt.Fprintf(w, "serve: a %d MiB disk, read-only, as %s\n", d.Size()>>20, name)
	fmt.Fprintf(out, "nbd+unix:///%s?socket=%s\n", name, cfg.sock)
	srv := &nbd.Server{Exports: []nbd.Export{{Name: name, Description: desc, Device: d, ReadOnly: true}}, ErrorLog: log.New(w, "serve: ", 0)}
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
