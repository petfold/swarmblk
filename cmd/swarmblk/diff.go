package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/store"
	"github.com/petfold/swarmblk/internal/tree"
)

func cmdDiff(args []string) {
	fs := flag.NewFlagSet("diff", flag.ExitOnError)
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
	if err := diff(context.Background(), bee.New(*beeURL, 4), s, fs.Arg(0), fs.Arg(1), os.Stdout, os.Stderr); err != nil {
		log.Fatal(err)
	}
}

// diff writes to out the 4 KiB blocks whose contents differ between the
// disks of the snapshots that a and b name, one run per line as its first
// block and the number of blocks, and reports a summary to w. The disks must
// be the same size. Only the chunks on differing paths are read, from s or
// else through Bee.
func diff(ctx context.Context, c *bee.Client, s *store.Store, a, b string, out, w io.Writer) error {
	get := s.Through(c.Chunk)
	sa, err := resolve(ctx, c, get, a, w)
	if err != nil {
		return err
	}
	sb, err := resolve(ctx, c, get, b, w)
	if err != nil {
		return err
	}
	ranges, err := tree.Diff(ctx, get, sa.Meta.DiskRoot, sb.Meta.DiskRoot)
	if err != nil {
		return err
	}
	var blocks int64
	for _, r := range ranges {
		if _, err := fmt.Fprintf(out, "%d %d\n", r.Start, r.Count); err != nil {
			return err
		}
		blocks += r.Count
	}
	fmt.Fprintf(w, "diff: %d blocks (%d KiB) in %d runs differ between snapshots %s and %s\n", blocks, blocks*swarm.ChunkSize>>10, len(ranges), sa.Ref, sb.Ref)
	return nil
}
