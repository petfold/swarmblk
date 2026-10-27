//go:build js && wasm

// Swarmblk-wasm is swarmblk's engine compiled to WebAssembly, for a browser,
// where it runs in a Service Worker and serves disks to an emulator in the
// page (web/). It reads a snapshot's disk from Swarm through a Bee node,
// chunk by chunk, checks every chunk against its address, and keeps the
// chunks in the browser's Cache Storage, so that what was read once is read
// again without the network. It prefetches from the snapshot's boot trace,
// if it has one (SPEC §6), reads ahead of sequential reads, and records the
// guest's reads as a new boot trace.
//
// A writable disk keeps the guest's writes in memory and in an ordered log in
// Cache Storage, which a reload replays, and its checkpoints turn them into
// the next snapshot of a lineage whose key the browser keeps (SPEC §8): the
// changed chunks go to Bee, stamped from a batch, and once Bee has synced
// them, the lineage's next update names the snapshot.
//
// It gives JavaScript these functions, each returning a promise, since a
// call that waited would hold up the browser's event loop, which Go's own
// waits need:
//
//	swarmblkOpen(bee, name, snapshot, writable)  {size, snapshot, lineage, index, writes, trace}
//	swarmblkRead(name, off, n)                   n bytes, a Uint8Array
//	swarmblkWrite(name, off, bytes)              once the engine holds the write
//	swarmblkCheckpoint(name, batch)              {snapshot, lineage, index, blocks, chunks}
//	swarmblkReset(name)                          once a writable disk's local state is gone
//	swarmblkTrace(name)                          the guest's reads so far, a boot trace in a Uint8Array, or null
//	swarmblkStats()                              {fetched, cached, memory, reads, bytes, writes}
//
// A disk is named by the page, such as hda, and a snapshot is given by its
// reference or as a lineage, 0x<owner>/<topic>, for its latest update. A
// writable disk that the browser already holds goes on from its own state
// and ignores the snapshot. fetched counts the chunks that came from Bee,
// cached those from the browser's cache and memory those found in memory;
// reads and bytes are what the disks served, and writes the blocks written
// since the disks' last checkpoints.
package main

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall/js"

	"github.com/petfold/swarmblk/internal/snapshot"
)

var (
	fetched, cached, memory, reads, bytes atomic.Int64

	mu    sync.Mutex
	disks = map[string]*disk{} // open disks, by name
)

func main() {
	g := js.Global()
	g.Set("swarmblkOpen", js.FuncOf(func(_ js.Value, args []js.Value) any {
		beeURL, name, ref, writable := args[0].String(), args[1].String(), args[2].String(), args[3].Truthy()
		return promise(func() (any, error) {
			d, err := open(context.Background(), beeURL, name, ref, writable)
			if err != nil {
				return nil, err
			}
			return d.info(), nil
		})
	}))
	g.Set("swarmblkRead", js.FuncOf(func(_ js.Value, args []js.Value) any {
		name, off, n := args[0].String(), int64(args[1].Float()), args[2].Int()
		return promise(func() (any, error) {
			d, err := opened(name)
			if err != nil {
				return nil, err
			}
			p, err := d.read(context.Background(), off, n)
			if err != nil {
				return nil, err
			}
			reads.Add(1)
			bytes.Add(int64(n))
			return toJS(p), nil
		})
	}))
	g.Set("swarmblkWrite", js.FuncOf(func(_ js.Value, args []js.Value) any {
		name, off, p := args[0].String(), int64(args[1].Float()), fromJS(args[2])
		return promise(func() (any, error) {
			d, err := opened(name)
			if err != nil {
				return nil, err
			}
			return nil, d.write(context.Background(), off, p)
		})
	}))
	g.Set("swarmblkCheckpoint", js.FuncOf(func(_ js.Value, args []js.Value) any {
		name, batch := args[0].String(), args[1].String()
		return promise(func() (any, error) {
			d, err := opened(name)
			if err != nil {
				return nil, err
			}
			r, err := d.checkpoint(context.Background(), batch)
			if err != nil {
				return nil, err
			}
			return js.ValueOf(r), nil
		})
	}))
	g.Set("swarmblkReset", js.FuncOf(func(_ js.Value, args []js.Value) any {
		name := args[0].String()
		return promise(func() (any, error) { return nil, reset(name) })
	}))
	g.Set("swarmblkTrace", js.FuncOf(func(_ js.Value, args []js.Value) any {
		name := args[0].String()
		return promise(func() (any, error) {
			d, err := opened(name)
			if err != nil {
				return js.Null(), nil
			}
			return toJS(snapshot.EncodeTrace(d.rec.Runs())), nil
		})
	}))
	g.Set("swarmblkStats", js.FuncOf(func(js.Value, []js.Value) any {
		return promise(func() (any, error) {
			mu.Lock()
			all := make([]*disk, 0, len(disks))
			for _, d := range disks {
				all = append(all, d)
			}
			mu.Unlock()
			var writes int
			for _, d := range all {
				writes += d.pending()
			}
			return js.ValueOf(map[string]any{"fetched": fetched.Load(), "cached": cached.Load(), "memory": memory.Load(), "reads": reads.Load(), "bytes": bytes.Load(), "writes": writes}), nil
		})
	}))
	select {}
}
