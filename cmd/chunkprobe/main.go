// Chunkprobe measures how fast a Bee node retrieves the chunks of a disk
// image, for the Phase 0 measurements (docs/PHASE0.md §4). QEMU keeps at most
// 8 requests in flight; chunkprobe shows what more parallel fetches give.
//
// It first walks the image's tree from the root, one level at a time, fetching
// every distinct intermediate chunk once and skipping zero subtrees (SPEC §3),
// and reports how long the walk took. Then, at each concurrency level, it
// fetches n random leaves that are not Z_0, a fresh set for each level while
// the image has enough distinct leaves, and prints latency percentiles and
// chunks per second. Every chunk is checked against its address.
//
// Usage:
//
//	chunkprobe -ref root [-bee url] [-n samples] [-c levels] [-nocache] [-seed n]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	"golang.org/x/sync/errgroup"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/tree"
)

func main() {
	beeURL := flag.String("bee", envOr("SWARMBLK_BEE_URL", "http://localhost:1633"), "Bee API `url` ($SWARMBLK_BEE_URL if set)")
	ref := flag.String("ref", "", "root `reference` of the disk image, as POST /bytes returned it")
	n := flag.Int("n", 2000, "`number` of random leaves to fetch at each concurrency level")
	levels := flag.String("c", "1,8,32,128", "comma-separated concurrency `levels`; the walk uses the highest")
	nocache := flag.Bool("nocache", false, "send Swarm-Cache: false, so Bee does not cache retrieved chunks")
	seed := flag.Uint64("seed", 0, "`seed` for choosing leaves; 0 picks one")
	flag.Parse()
	log.SetPrefix("chunkprobe: ")
	log.SetFlags(0)

	root, err := swarm.ParseHexAddress(*ref)
	if err != nil || len(root.Bytes()) != swarm.HashSize {
		log.Fatalf("bad -ref %q: want the 64 hex digits of a root reference", *ref)
	}
	conc, err := parseLevels(*levels)
	if err != nil {
		log.Fatal(err)
	}
	if *n < 1 {
		log.Fatal("-n must be at least 1")
	}
	if *seed == 0 {
		*seed = rand.Uint64()
	}
	c := bee.New(*beeURL, slices.Max(conc))
	c.NoCache = *nocache
	ctx := context.Background()

	fmt.Printf("Bee %s, Swarm-Cache %t, seed %d\n", c.URL, !*nocache, *seed)
	w, err := walk(ctx, c, root, slices.Max(conc))
	if err != nil {
		log.Fatal(err)
	}
	g := w.geo
	fmt.Printf("disk: %d MiB, %d leaves, %d intermediate levels, root fan-out %d\n", g.Size>>20, g.Leaves(), g.Levels, g.Fanout)
	fmt.Printf("walk: %d intermediate chunks in %.3f s at concurrency %d; %d zero subtrees not fetched\n",
		w.fetched, w.took.Seconds(), slices.Max(conc), w.zeros)
	fmt.Printf("leaves: %d distinct and not Z_0\n", len(w.leaves))
	if len(w.leaves) == 0 {
		log.Fatal("every leaf is Z_0, so there is nothing to probe")
	}
	if total := *n * len(conc); total > len(w.leaves) {
		fmt.Printf("note: %d fetches need more leaves than that, so some leaves are fetched again and caches on the way may serve them\n", total)
	}

	r := rand.New(rand.NewPCG(*seed, 0))
	r.Shuffle(len(w.leaves), func(i, j int) { w.leaves[i], w.leaves[j] = w.leaves[j], w.leaves[i] })
	fmt.Println()
	fmt.Println("| Concurrency | p50 (ms) | p90 | p99 | max | chunks/s | failed |")
	fmt.Println("|---|---|---|---|---|---|---|")
	for i, k := range conc {
		res := probe(ctx, c, pick(w.leaves, i**n, *n), k)
		fmt.Println(res.row(k))
		if res.failed > 0 {
			log.Printf("concurrency %d: %d of %d fetches failed; the first: %v", k, res.failed, *n, res.err)
		}
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parseLevels parses a comma-separated list of positive integers.
func parseLevels(s string) ([]int, error) {
	var levels []int
	for _, f := range strings.Split(s, ",") {
		k, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || k < 1 {
			return nil, fmt.Errorf("bad concurrency level %q in %q", f, s)
		}
		levels = append(levels, k)
	}
	return levels, nil
}

// fetch gets one chunk, giving up after a minute.
func fetch(ctx context.Context, c *bee.Client, addr swarm.Address) (swarm.Chunk, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return c.Chunk(ctx, addr)
}

// set holds distinct chunk addresses.
type set map[[swarm.HashSize]byte]struct{}

// list returns the addresses in s in ascending order, so that a seeded shuffle
// of the list is repeatable.
func (s set) list() []swarm.Address {
	out := make([]swarm.Address, 0, len(s))
	for k := range s {
		out = append(out, swarm.NewAddress(k[:]))
	}
	slices.SortFunc(out, swarm.Address.Compare)
	return out
}

// walked is what a walk learns about an image's tree.
type walked struct {
	geo     tree.Geometry
	fetched int             // intermediate chunks fetched, root included, each distinct address once
	zeros   int             // references to zero subtrees Z_1 … Z_(L−1), which need no fetch
	leaves  []swarm.Address // distinct leaves other than Z_0
	took    time.Duration
}

// walk fetches the root and every distinct intermediate chunk below it that is
// not a zero subtree, one level at a time with conc requests in flight, and
// collects the leaves.
func walk(ctx context.Context, c *bee.Client, root swarm.Address, conc int) (walked, error) {
	start := time.Now()
	ch, err := fetch(ctx, c, root)
	if err != nil {
		return walked{}, err
	}
	g, refs, err := tree.Root(ch)
	if err != nil {
		return walked{}, err
	}
	w := walked{geo: g, fetched: 1}
	next := set{}
	w.add(next, refs, g.Levels-1)
	for j := g.Levels - 1; j > 0; j-- {
		addrs := next.list()
		next = set{}
		var mu sync.Mutex
		eg, gctx := errgroup.WithContext(ctx)
		eg.SetLimit(conc)
		for _, addr := range addrs {
			eg.Go(func() error {
				ch, err := fetch(gctx, c, addr)
				if err != nil {
					return err
				}
				refs, err := g.Children(j, ch)
				if err != nil {
					return err
				}
				mu.Lock()
				defer mu.Unlock()
				w.add(next, refs, j-1)
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return walked{}, err
		}
		w.fetched += len(addrs)
	}
	w.leaves = next.list()
	w.took = time.Since(start)
	return w, nil
}

// add puts refs, which point to chunks at level j, into s, apart from zero
// subtrees, which it only counts.
func (w *walked) add(s set, refs []swarm.Address, j int) {
	z := tree.Zero(j)
	for _, r := range refs {
		switch {
		case !r.Equal(z):
			s[[swarm.HashSize]byte(r.Bytes())] = struct{}{}
		case j > 0:
			w.zeros++
		}
	}
}

// pick returns n leaves from offset from on, wrapping around the end.
func pick(leaves []swarm.Address, from, n int) []swarm.Address {
	out := make([]swarm.Address, n)
	for i := range out {
		out[i] = leaves[(from+i)%len(leaves)]
	}
	return out
}

// result is what fetching leaves at one concurrency level gave.
type result struct {
	lat    []time.Duration // one for each fetch that succeeded
	failed int
	err    error // the first failure
	took   time.Duration
}

// probe fetches leaves with conc requests in flight.
func probe(ctx context.Context, c *bee.Client, leaves []swarm.Address, conc int) result {
	var (
		res result
		mu  sync.Mutex
		eg  errgroup.Group
	)
	eg.SetLimit(conc)
	start := time.Now()
	for _, addr := range leaves {
		eg.Go(func() error {
			t := time.Now()
			_, err := fetch(ctx, c, addr)
			d := time.Since(t)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				res.failed++
				if res.err == nil {
					res.err = err
				}
				return nil
			}
			res.lat = append(res.lat, d)
			return nil
		})
	}
	eg.Wait()
	res.took = time.Since(start)
	return res
}

// row formats r as a row of the chunk-probe table in PHASE0.md §7, with
// nearest-rank percentiles.
func (r result) row(conc int) string {
	if len(r.lat) == 0 {
		return fmt.Sprintf("| %d | – | – | – | – | 0 | %d |", conc, r.failed)
	}
	slices.Sort(r.lat)
	pct := func(p int) string { return ms(r.lat[(len(r.lat)*p+99)/100-1]) }
	return fmt.Sprintf("| %d | %s | %s | %s | %s | %.0f | %d |", conc, pct(50), pct(90), pct(99), ms(r.lat[len(r.lat)-1]),
		float64(len(r.lat))/r.took.Seconds(), r.failed)
}

func ms(d time.Duration) string {
	return strconv.FormatFloat(float64(d.Microseconds())/1e3, 'f', 1, 64)
}
