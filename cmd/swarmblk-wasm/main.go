//go:build js && wasm

// Swarmblk-wasm is swarmblk's engine compiled to WebAssembly, for a browser,
// where it runs in a Service Worker and serves disks to an emulator in the
// page. It reads a snapshot's disk from Swarm through a Bee node, chunk by
// chunk, checks every chunk against its address, and keeps the chunks in
// the browser's Cache Storage, so that what was read once is read again
// without the network. It prefetches from the snapshot's boot trace, if it
// has one (SPEC §6), and reads ahead of sequential reads. It records the
// guest's reads as a new boot trace. It gives JavaScript four functions:
//
//	swarmblkOpen(bee, snapshot)      a promise of {size, root, trace}
//	swarmblkRead(snapshot, off, n)   a promise of n bytes, a Uint8Array
//	swarmblkTrace(snapshot)          the guest's reads so far, a boot trace in a Uint8Array
//	swarmblkStats()                  {fetched, cached, memory, reads, bytes}
//
// fetched counts the chunks that came from Bee, cached those that came from
// the browser's cache, memory those found in memory, and reads and bytes
// what the disks served.
package main

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"syscall/js"
	"time"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"golang.org/x/sync/singleflight"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/engine"
	"github.com/petfold/swarmblk/internal/snapshot"
)

const (
	window    = 16384   // blocks of the boot trace to prefetch ahead of the guest, as serve's default
	ahead     = 1 << 20 // the most bytes to read ahead of sequential reads
	aheadGets = 4       // chunks that reads ahead fetch at once, leaving the rest of the browser's connections to the guest's reads
	memChunks = 8192    // chunks kept in memory, the most recently used: 32 MiB
)

var (
	fetched, cached, memory, reads, bytes atomic.Int64

	mu    sync.Mutex
	disks = map[string]*disk{} // open disks, by snapshot reference

	ignore = js.FuncOf(func(js.Value, []js.Value) any { return nil })
)

func main() {
	g := js.Global()
	g.Set("swarmblkOpen", js.FuncOf(func(_ js.Value, args []js.Value) any {
		beeURL, ref := args[0].String(), args[1].String()
		return promise(func() (any, error) { return open(beeURL, ref) })
	}))
	g.Set("swarmblkRead", js.FuncOf(func(_ js.Value, args []js.Value) any {
		ref, off, n := args[0].String(), int64(args[1].Float()), args[2].Int()
		return promise(func() (any, error) { return read(ref, off, n) })
	}))
	g.Set("swarmblkTrace", js.FuncOf(func(_ js.Value, args []js.Value) any {
		mu.Lock()
		d, ok := disks[args[0].String()]
		mu.Unlock()
		if !ok {
			return js.Null()
		}
		return toJS(snapshot.EncodeTrace(d.rec.Runs()))
	}))
	g.Set("swarmblkStats", js.FuncOf(func(js.Value, []js.Value) any {
		return js.ValueOf(map[string]any{"fetched": fetched.Load(), "cached": cached.Load(), "memory": memory.Load(), "reads": reads.Load(), "bytes": bytes.Load()})
	}))
	select {}
}

// disk is an open snapshot's disk, and where reads ahead of the guest have
// got. Reads ahead go through their own engine, which fetches fewer chunks
// at once, and the guest's reads find what they fetched in memory.
type disk struct {
	s, ra *engine.Snapshot
	rec   *engine.Recorder // the guest's reads

	mu         sync.Mutex
	next, warm int64 // where the last read ended, and how far reads ahead reach
	streak     int   // sequential reads in a row
}

// open opens the disk of snapshot ref, through the Bee node at beeURL, and
// starts prefetching from its boot trace.
func open(beeURL, ref string) (any, error) {
	mu.Lock()
	defer mu.Unlock()
	if d, ok := disks[ref]; ok {
		return info(d.s, 0), nil
	}
	addr, err := swarm.ParseHexAddress(ref)
	if err != nil || len(addr.Bytes()) != swarm.HashSize {
		return nil, fmt.Errorf("snapshot %q is not 64 hex digits", ref)
	}
	ctx := context.Background()
	get, err := newGetter(beeURL)
	if err != nil {
		return nil, err
	}
	snap, err := snapshot.Read(ctx, get, addr)
	if err != nil {
		return nil, err
	}
	s, err := engine.Open(ctx, get, snap.Meta.DiskRoot, engine.Fetchers)
	if err != nil {
		return nil, err
	}
	ra, err := engine.Open(ctx, get, snap.Meta.DiskRoot, aheadGets)
	if err != nil {
		return nil, err
	}
	trace, err := snapshot.ReadTrace(ctx, get, snap)
	if err != nil {
		return nil, err
	}
	var blocks int64
	if len(trace) > 0 {
		blocks = s.Prefetch(ctx, trace, window).Blocks()
	}
	disks[ref] = &disk{s: s, ra: ra, rec: s.Record()}
	return info(s, blocks), nil
}

func info(s *engine.Snapshot, trace int64) js.Value {
	return js.ValueOf(map[string]any{"size": s.Size(), "root": s.Root().String(), "trace": trace})
}

// read reads n bytes at off from the open disk of snapshot ref.
func read(ref string, off int64, n int) (any, error) {
	mu.Lock()
	d, ok := disks[ref]
	mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("snapshot %s is not open", ref)
	}
	p := make([]byte, n)
	if err := d.s.ReadAt(context.Background(), p, off); err != nil {
		return nil, err
	}
	d.readAhead(off, int64(n))
	reads.Add(1)
	bytes.Add(int64(n))
	return toJS(p), nil
}

// readAhead reads past sequential reads in the background, so that the
// guest, which reads one piece at a time, finds the next pieces fetched.
// It starts after two sequential reads in a row with 64 KiB and doubles
// while they go on, up to ahead bytes.
func (d *disk) readAhead(off, n int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	end := off + n
	if off != d.next {
		d.next, d.warm, d.streak = end, end, 0
		return
	}
	d.next = end
	d.streak++
	if d.streak < 2 {
		return
	}
	size := min(int64(64<<10)<<min(d.streak-2, 4), ahead)
	from, to := max(d.warm, end), min(end+size, d.s.Size())
	if from >= to {
		return
	}
	d.warm = to
	go d.ra.ReadAt(context.Background(), make([]byte, to-from), from)
}

// getter gets chunks from memory, from the browser's cache, or else from
// Bee, checking each against its address when it comes from outside
// memory, and keeps those it fetched in both.
type getter struct {
	bee   *bee.Client
	cache js.Value // a Cache from the Cache Storage API
	group singleflight.Group

	mu    sync.Mutex
	known map[string]bool          // the chunks in the cache, by address
	mem   map[string]*list.Element // of lru, by address
	lru   *list.List               // swarm.Chunk, most recently used first
}

func newGetter(beeURL string) (*getter, error) {
	cache, err := await(js.Global().Get("caches").Call("open", "swarmblk-chunks"))
	if err != nil {
		return nil, fmt.Errorf("the browser's cache: %w", err)
	}
	g := &getter{bee: bee.New(beeURL, engine.Fetchers), cache: cache, known: map[string]bool{}, mem: map[string]*list.Element{}, lru: list.New()}
	keys, err := await(cache.Call("keys"))
	if err != nil {
		return nil, fmt.Errorf("the browser's cache: %w", err)
	}
	for i := 0; i < keys.Length(); i++ {
		u := keys.Index(i).Get("url").String()
		g.known[u[strings.LastIndexByte(u, '/')+1:]] = true
	}
	return g, nil
}

func (g *getter) Get(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	k := addr.String()
	g.mu.Lock()
	if e, ok := g.mem[k]; ok {
		g.lru.MoveToFront(e)
		g.mu.Unlock()
		memory.Add(1)
		return e.Value.(swarm.Chunk), nil
	}
	g.mu.Unlock()
	v, err, _ := g.group.Do(k, func() (any, error) { return g.load(ctx, addr) })
	if err != nil {
		return nil, err
	}
	return v.(swarm.Chunk), nil
}

// load gets a chunk from the browser's cache, or else from Bee, and keeps
// it in memory.
func (g *getter) load(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	k := addr.String()
	key := "/swarmblk/chunks/" + k
	g.mu.Lock()
	known := g.known[k]
	g.mu.Unlock()
	if known {
		if resp, err := await(g.cache.Call("match", key)); err == nil && resp.Truthy() {
			if buf, err := await(resp.Call("arrayBuffer")); err == nil {
				if ch := swarm.NewChunk(addr, fromJS(buf)); cac.Valid(ch) {
					cached.Add(1)
					g.remember(k, ch)
					return ch, nil
				}
			}
		}
	}
	var ch swarm.Chunk
	var err error
	for wait := 500 * time.Millisecond; ; wait *= 2 { // four tries, as serve's
		if ch, err = g.bee.Chunk(ctx, addr); err == nil || wait > 2*time.Second || errors.Is(err, bee.ErrInvalidChunk) {
			break
		}
		time.Sleep(wait)
	}
	if err != nil {
		return nil, err
	}
	fetched.Add(1)
	g.remember(k, ch)
	// The browser stores it in the background; a read before it has does
	// not find it, and fetches it again.
	g.cache.Call("put", key, js.Global().Get("Response").New(toJS(ch.Data()))).Call("catch", ignore)
	g.mu.Lock()
	g.known[k] = true
	g.mu.Unlock()
	return ch, nil
}

// remember keeps ch in memory, dropping the chunk used least recently if
// memory is full.
func (g *getter) remember(k string, ch swarm.Chunk) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.mem[k]; ok {
		return
	}
	g.mem[k] = g.lru.PushFront(ch)
	if g.lru.Len() > memChunks {
		last := g.lru.Back()
		g.lru.Remove(last)
		delete(g.mem, last.Value.(swarm.Chunk).Address().String())
	}
}

// promise runs fn in a goroutine and returns a JavaScript promise of its
// result.
func promise(fn func() (any, error)) js.Value {
	executor := js.FuncOf(func(_ js.Value, args []js.Value) any {
		resolve, reject := args[0], args[1]
		go func() {
			v, err := fn()
			if err != nil {
				reject.Invoke(js.Global().Get("Error").New(err.Error()))
				return
			}
			resolve.Invoke(v)
		}()
		return nil
	})
	defer executor.Release() // the Promise constructor has called it
	return js.Global().Get("Promise").New(executor)
}

// await waits for a JavaScript promise and returns its value, or its
// rejection as an error.
func await(p js.Value) (js.Value, error) {
	type result struct {
		v   js.Value
		err error
	}
	c := make(chan result, 1)
	ok := js.FuncOf(func(_ js.Value, args []js.Value) any {
		c <- result{v: args[0]}
		return nil
	})
	fail := js.FuncOf(func(_ js.Value, args []js.Value) any {
		c <- result{err: errors.New(args[0].Call("toString").String())}
		return nil
	})
	defer ok.Release()
	defer fail.Release()
	p.Call("then", ok, fail)
	r := <-c
	return r.v, r.err
}

func toJS(b []byte) js.Value {
	a := js.Global().Get("Uint8Array").New(len(b))
	js.CopyBytesToJS(a, b)
	return a
}

// fromJS copies an ArrayBuffer.
func fromJS(buf js.Value) []byte {
	a := js.Global().Get("Uint8Array").New(buf)
	b := make([]byte, a.Get("length").Int())
	js.CopyBytesToGo(b, a)
	return b
}
