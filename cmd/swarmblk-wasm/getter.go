//go:build js && wasm

package main

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"golang.org/x/sync/singleflight"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/engine"
)

// memChunks is how many chunks a getter keeps in memory, the most recently
// used: 32 MiB.
const memChunks = 8192

var (
	gettersMu sync.Mutex
	getters   = map[string]*getter{} // by Bee URL
)

// getter gets chunks from memory, from the browser's cache, or else from
// Bee, checking each against its address when it comes from outside
// memory, and keeps those it fetched in both.
type getter struct {
	bee   *bee.Client
	cache cache
	group singleflight.Group

	mu    sync.Mutex
	known map[string]bool          // the chunks in the cache, by address
	mem   map[string]*list.Element // of lru, by address
	lru   *list.List               // swarm.Chunk, most recently used first
}

// getterFor returns the getter for the Bee node at beeURL.
func getterFor(beeURL string) (*getter, error) {
	gettersMu.Lock()
	defer gettersMu.Unlock()
	if g, ok := getters[beeURL]; ok {
		return g, nil
	}
	c, err := openCache("swarmblk-chunks")
	if err != nil {
		return nil, fmt.Errorf("the browser's cache: %w", err)
	}
	g := &getter{bee: bee.New(beeURL, engine.Fetchers), cache: c, known: map[string]bool{}, mem: map[string]*list.Element{}, lru: list.New()}
	keys, err := c.keys()
	if err != nil {
		return nil, fmt.Errorf("the browser's cache: %w", err)
	}
	for _, k := range keys {
		g.known[k[strings.LastIndexByte(k, '/')+1:]] = true
	}
	getters[beeURL] = g
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
	g.mu.Lock()
	known := g.known[k]
	g.mu.Unlock()
	if known {
		if data, _, ok, err := g.cache.get("/swarmblk/chunks/"+k, ""); err == nil && ok {
			if ch := swarm.NewChunk(addr, data); cac.Valid(ch) {
				cached.Add(1)
				g.remember(k, ch)
				return ch, nil
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
	g.keep(ch)
	return ch, nil
}

// keep puts ch in memory, and in the browser's cache in the background; a
// read before it lands there fetches the chunk again.
func (g *getter) keep(ch swarm.Chunk) {
	k := ch.Address().String()
	g.remember(k, ch)
	g.cache.putLater("/swarmblk/chunks/"+k, ch.Data())
	g.mu.Lock()
	g.known[k] = true
	g.mu.Unlock()
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
