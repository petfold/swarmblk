// Command crashtest is swarmblk's crash-consistency test with a real guest
// (ROADMAP Phase 2). A VM runs a workload on a disk that swarmblk serve
// -disk serves writable: it creates files, overwrites a file in place and
// deletes files, fsyncs each operation and then commits it in a log. QEMU,
// the engine or both are killed at a random point, and a checker VM
// recovers the filesystem as after a power cut, checks it with the
// filesystem's own tools, and verifies every committed operation. Quick
// runs check the disk through the engine; full runs also take a live
// checkpoint while the guest writes and another after the crash, and check
// both snapshots exported through a second Bee node. Cost runs repeat the
// same workload on each filesystem and checkpoint it, to compare what the
// checkpoints upload (DESIGN §9).
//
//	crashtest image [-work dir]
//	crashtest run [-work dir] [-fs ext4,xfs,btrfs,zfs] [-quick n] [-full n] [-cost iterations] ...
//	crashtest report [-work dir]
//	crashtest guest command...
//
// image builds the guest from Ubuntu 24.04's cloud image and the ZFS
// packages, which must be in the work directory first:
//
//	curl -O https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img
//	mkdir debs && cd debs && apt-get download zfsutils-linux libnvpair3linux libuutil3linux libzfs4linux libzpool5linux
//
// run needs swarmblk in the work directory, a Bee node that stamps with an
// immutable batch, and a second node for the exports of full runs. The
// guest runs this binary, from a small disk that each boot gets with the
// command; guest is its side.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "guest":
		err = guest(os.Args[2:])
	case "image":
		err = cmdImage(os.Args[2:])
	case "run":
		err = cmdRun(os.Args[2:])
	case "report":
		err = cmdReport(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  crashtest image [-work dir]
  crashtest run [-work dir] [-fs list] [-quick n] [-full n] [-cost iterations] [-rounds n] [-first n] [-seed n] [-bucket n] [-bee url] [-bee-b url] [-batch id]
  crashtest report [-work dir]
`)
	os.Exit(2)
}

func defaultWork() string {
	h, _ := os.UserHomeDir()
	return filepath.Join(h, "swarmblk-crash")
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	work := fs.String("work", defaultWork(), "work `directory`")
	beeA := fs.String("bee", envOr("SWARMBLK_BEE_URL", "http://localhost:1633"), "Bee API `url` that stamps ($SWARMBLK_BEE_URL if set)")
	beeB := fs.String("bee-b", os.Getenv("SWARMBLK_BEE_URL_B"), "second Bee node's API `url`, for the exports of full runs ($SWARMBLK_BEE_URL_B if set)")
	batch := fs.String("batch", os.Getenv("SWARMBLK_BATCH"), "immutable postage batch `id` ($SWARMBLK_BATCH if set)")
	list := fs.String("fs", "ext4,xfs,btrfs,zfs", "`filesystems` to test")
	quick := fs.Int("quick", 0, "quick runs per filesystem: the disk checked through the engine")
	full := fs.Int("full", 0, "full runs per filesystem: live and crash snapshots checked through node B")
	costN := fs.Int("cost", 0, "`iterations` of each round of the cost workload, 0 for none")
	rounds := fs.Int("rounds", 2, "rounds of the cost workload")
	first := fs.Int("first", 1, "`number` of the first run, to go on with an earlier series")
	seed := fs.Uint64("seed", uint64(time.Now().UnixNano()), "random `seed`")
	bucket := fs.Int("bucket", 28, "stop full and cost runs once the batch's fullest bucket holds this many `chunks`")
	fs.Parse(args)
	if fs.NArg() != 0 || *batch == "" || *full > 0 && *beeB == "" {
		usage()
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	r := &runner{
		work:     *work,
		swarmblk: filepath.Join(*work, "swarmblk"),
		self:     self,
		image:    filepath.Join(*work, "guest.qcow2"),
		beeA:     *beeA,
		beeB:     *beeB,
		batch:    *batch,
		rng:      rand.New(rand.NewPCG(*seed, 0)),
		out:      os.Stdout,
		bucket:   *bucket,
	}
	if r.results, err = os.OpenFile(filepath.Join(*work, "results.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600); err != nil {
		return err
	}
	defer r.results.Close()
	if err := r.loadState(); err != nil {
		return err
	}
	r.logf("seed %d", *seed)
	if err := r.zero(); err != nil {
		return err
	}
	roomy := func() error {
		most, err := r.fullestBucket()
		if err != nil {
			return err
		}
		if most >= r.bucket {
			return fmt.Errorf("the batch's fullest bucket holds %d chunks: stopping before it fills", most)
		}
		return nil
	}
	failed := 0
	for _, f := range strings.Split(*list, ",") {
		if err := r.base(f); err != nil {
			return err
		}
		if *costN > 0 {
			if err := roomy(); err != nil {
				return err
			}
			if err := r.cost(f, *costN, *rounds); err != nil {
				return err
			}
		}
		for n := *first; n < *first+*quick; n++ {
			ok, err := r.quick(f, n)
			if err != nil {
				return fmt.Errorf("%s quick %d: %w", f, n, err)
			}
			if !ok {
				failed++
			} else {
				os.RemoveAll(filepath.Join(r.work, "homes", fmt.Sprintf("quick-%s-%d", f, n)))
			}
		}
		for n := *first; n < *first+*full; n++ {
			if err := roomy(); err != nil {
				return err
			}
			ok, err := r.full(f, n)
			if err != nil {
				return fmt.Errorf("%s full %d: %w", f, n, err)
			}
			if !ok {
				failed++
			} else {
				os.RemoveAll(filepath.Join(r.work, "homes", fmt.Sprintf("full-%s-%d", f, n)))
			}
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d runs failed", failed)
	}
	return nil
}

// cmdReport sums up the results: runs passed per filesystem and kind, and
// what the cost rounds uploaded.
func cmdReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	work := fs.String("work", defaultWork(), "work `directory`")
	fs.Parse(args)
	f, err := os.Open(filepath.Join(*work, "results.jsonl"))
	if err != nil {
		return err
	}
	defer f.Close()
	type tally struct{ runs, ok int }
	runs := map[string]*tally{}
	var costs []string
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var r struct {
			Kind, FS, Mode string
			OK             bool
			Round          int
			Iterations     int
			Checkpoint     cpStats
		}
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return err
		}
		switch r.Kind {
		case "quick", "full":
			k := r.FS + " " + r.Kind + " " + r.Mode
			if runs[k] == nil {
				runs[k] = &tally{}
			}
			runs[k].runs++
			if r.OK {
				runs[k].ok++
			}
		case "cost", "base":
			costs = append(costs, fmt.Sprintf("%-5s %-4s round %d: %d iterations, %d blocks changed, %d chunks uploaded",
				r.FS, r.Kind, r.Round, r.Iterations, r.Checkpoint.Blocks, r.Checkpoint.Chunks))
		}
	}
	for _, k := range slices.Sorted(maps.Keys(runs)) {
		fmt.Printf("%-28s %d of %d passed\n", k, runs[k].ok, runs[k].runs)
	}
	for _, c := range costs {
		fmt.Println(c)
	}
	return sc.Err()
}
