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
	"github.com/petfold/swarmblk/internal/store"
)

func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	beeURL := fs.String("bee", envOr("SWARMBLK_BEE_URL", "http://localhost:1633"), "Bee API `url` ($SWARMBLK_BEE_URL if set)")
	dir := fs.String("home", home(), "local state `directory` ($SWARMBLK_HOME if set)")
	sock := fs.String("socket", "", "Unix `socket` to listen on (default nbd.sock in the local state directory)")
	root := fs.String("root", "", "serve the disk tree under this `root` instead of a snapshot")
	fs.Parse(args)
	if (fs.NArg() == 1) == (*root != "") || fs.NArg() > 1 {
		usage()
	}
	s, err := store.Open(filepath.Join(*dir, "chunks"))
	if err != nil {
		log.Fatal(err)
	}
	if *sock == "" {
		*sock = filepath.Join(*dir, "nbd.sock")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serve(ctx, bee.New(*beeURL, engine.Fetchers), s, fs.Arg(0), *root, *sock, os.Stdout, os.Stderr); err != nil {
		log.Fatal(err)
	}
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

// serve exports a disk read-only over NBD on a Unix socket at sock until
// ctx ends (DESIGN §8): the disk of the snapshot that arg names, or the disk
// tree under root. The export is named after the snapshot's reference, or
// the root, and serve prints its URI for QEMU to out:
// nbd+unix:///<name>?socket=<sock>. It reports to w.
func serve(ctx context.Context, c *bee.Client, s *store.Store, arg, root, sock string, out, w io.Writer) error {
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
	if root != "" {
		r, err := swarm.ParseHexAddress(root)
		if err != nil || len(r.Bytes()) != swarm.HashSize {
			return fmt.Errorf("disk root %q is not 64 hex digits", root)
		}
		disk, name, desc = r, r.String(), "swarmblk disk tree "+r.String()
	} else {
		snap, err := resolve(ctx, c, get, arg, w)
		if err != nil {
			return err
		}
		disk, name, desc = snap.Meta.DiskRoot, snap.Ref.String(), "swarmblk snapshot "+snap.Ref.String()
	}
	d, err := engine.Open(ctx, get, disk, 0)
	if err != nil {
		return err
	}
	l, err := listen(sock)
	if err != nil {
		return err
	}
	defer os.Remove(sock)
	start := time.Now()
	fmt.Fprintf(w, "serve: a %d MiB disk, read-only, as %s\n", d.Size()>>20, name)
	fmt.Fprintf(out, "nbd+unix:///%s?socket=%s\n", name, sock)
	srv := &nbd.Server{Exports: []nbd.Export{{Name: name, Description: desc, Device: d, ReadOnly: true}}, ErrorLog: log.New(w, "serve: ", 0)}
	err = srv.Serve(ctx, l)
	fmt.Fprintf(w, "serve: stopped after %v; %d chunks fetched from Bee\n", time.Since(start).Round(time.Second), fetched.Load())
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
