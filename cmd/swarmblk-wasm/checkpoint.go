//go:build js && wasm

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"golang.org/x/sync/errgroup"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/engine"
	"github.com/petfold/swarmblk/internal/lineage"
	"github.com/petfold/swarmblk/internal/snapshot"
	"github.com/petfold/swarmblk/internal/tree"
)

// uploads is how many chunks a checkpoint uploads at once.
const uploads = 6

// checkpoint turns the disk's writes so far into the next snapshot (SPEC
// §8). It cuts them between two writes, rebuilds the snapshot's tree with
// them, and uploads the chunks that differ and the new snapshot's manifest,
// the root last, stamped from batch through the disk's Bee node, which must
// be the batch's own. Once Bee has synced them all, it publishes the
// snapshot as the next update of the disk's lineage; the disk's first
// checkpoint starts a lineage with a new key, which the browser keeps.
// Then the disk moves onto the snapshot, keeping the writes made since the
// cut. The guest keeps writing throughout.
func (d *disk) checkpoint(ctx context.Context, batch string) (map[string]any, error) {
	if !d.writable {
		return nil, errReadOnly
	}
	if batch == "" {
		return nil, errors.New("checkpoints need a batch to stamp with")
	}
	d.cp.Lock()
	defer d.cp.Unlock()
	c := d.get.bee
	if err := checkBatch(ctx, c, batch); err != nil {
		return nil, err
	}

	// The cut: every write done, none half done.
	d.wmu.Lock()
	d.mu.Lock()
	cut, base, st := d.seq, d.base, d.state
	blocks := make(map[int64][]byte, len(d.writes))
	for b, w := range d.writes {
		blocks[b] = w.data
	}
	d.mu.Unlock()
	d.wmu.Unlock()
	if len(blocks) == 0 {
		return result(base.Ref, st.Lineage, 0, 0), nil
	}

	f := &fresh{m: map[string]swarm.Chunk{}, under: d.get}
	root, chunks, err := tree.Rebuild(ctx, f, base.Meta.DiskRoot, blocks)
	if err != nil {
		return nil, err
	}
	for _, ch := range chunks {
		f.Put(ctx, ch)
	}
	l, key, prev, index, err := successor(base, st)
	if err != nil {
		return nil, err
	}
	ranges, err := tree.Diff(ctx, f, base.Meta.DiskRoot, root)
	if err != nil {
		return nil, err
	}
	order, err := tree.Changes(ctx, f, base.Meta.DiskRoot, root)
	if err != nil {
		return nil, err
	}
	meta := snapshot.NewMeta(base.Meta.Size, root)
	meta.Parent = &base.Ref
	meta.Lineage = l.Meta(index)
	meta.Encryption = base.Meta.Encryption
	meta.Batches = []string{batch}
	for _, b := range base.Meta.Batches {
		if !slices.Contains(meta.Batches, b) {
			meta.Batches = append(meta.Batches, b)
		}
	}
	for _, r := range ranges {
		meta.Stats.ChangedBlocks += r.Count
	}
	meta.Stats.NewChunks = int64(len(order))
	var prefetch []byte // the parent's boot trace, still in place
	if runs, err := snapshot.ReadTrace(ctx, f, base); err != nil {
		return nil, err
	} else if runs != nil {
		prefetch = snapshot.EncodeTrace(runs)
	}
	var omu sync.Mutex // the manifest's nodes are saved concurrently
	seen := map[string]bool{}
	for _, a := range order {
		seen[a.ByteString()] = true
	}
	snap, err := snapshot.Write(ctx, putter(func(ctx context.Context, ch swarm.Chunk) error {
		omu.Lock()
		if !seen[ch.Address().ByteString()] {
			seen[ch.Address().ByteString()] = true
			order = append(order, ch.Address())
		}
		omu.Unlock()
		return f.Put(ctx, ch)
	}), f, meta, prefetch)
	if err != nil {
		return nil, err
	}
	if i := slices.IndexFunc(order, snap.Equal); i >= 0 && i < len(order)-1 { // the manifest's root last
		order = append(slices.Delete(order, i, i+1), snap)
	}
	if err := checkRoom(ctx, c, batch, order); err != nil {
		return nil, err
	}
	if err := upload(ctx, c, batch, f, order); err != nil {
		return nil, err
	}
	u := lineage.Update{Index: index, Snapshot: snap, Time: meta.Created}
	if err := lineage.Publish(ctx, c, batch, l, key, prev, u); err != nil {
		return nil, fmt.Errorf("snapshot %s is uploaded, but publishing it failed: %w", snap, err)
	}

	// The disk moves onto the snapshot, whose chunks it has at hand.
	for _, ch := range f.chunks() {
		d.get.keep(ch)
	}
	next, err := snapshot.Read(ctx, d.get, snap)
	if err != nil {
		return nil, err
	}
	s, err := engine.Open(ctx, d.get, root, engine.Fetchers)
	if err != nil {
		return nil, err
	}
	ra, err := engine.Open(ctx, d.get, root, aheadGets)
	if err != nil {
		return nil, err
	}
	raw, err := crypto.EncodeSecp256k1PrivateKey(key)
	if err != nil {
		return nil, err
	}
	d.wmu.Lock()
	d.mu.Lock()
	d.base, d.s, d.ra = next, s, ra
	for b, w := range d.writes {
		if w.seq <= cut {
			delete(d.writes, b)
		}
	}
	d.state = diskState{Snapshot: snap, Cut: cut, Lineage: l.Meta(index), Key: hex.EncodeToString(raw)}
	d.mu.Unlock()
	d.wmu.Unlock()
	if err := d.saveState(); err != nil {
		return nil, fmt.Errorf("snapshot %s is published, but the browser did not keep it: %w", snap, err)
	}
	go d.dropLog(cut)
	r := result(snap, l.Meta(index), len(blocks), len(order))
	return r, nil
}

func result(snap swarm.Address, m *snapshot.Lineage, blocks, chunks int) map[string]any {
	r := map[string]any{"snapshot": snap.String(), "lineage": "", "index": 0, "blocks": blocks, "chunks": chunks}
	if m != nil {
		if l, i, err := lineage.FromMeta(*m); err == nil {
			r["lineage"], r["index"] = l.String(), i
		}
	}
	return r
}

// successor returns where the snapshot after base goes: the next update of
// the lineage that the disk's checkpoints write, if base is its latest, or
// else update 0 of a new lineage, under a new key.
func successor(base snapshot.Snapshot, st diskState) (lineage.Lineage, *ecdsa.PrivateKey, *lineage.Update, uint64, error) {
	if st.Lineage != nil && st.Key != "" && base.Meta.Lineage != nil && *base.Meta.Lineage == *st.Lineage {
		l, i, err := lineage.FromMeta(*st.Lineage)
		if err != nil {
			return lineage.Lineage{}, nil, nil, 0, err
		}
		raw, err := hex.DecodeString(st.Key)
		if err != nil {
			return lineage.Lineage{}, nil, nil, 0, errors.New("the disk's lineage key is damaged")
		}
		key, err := crypto.DecodeSecp256k1PrivateKey(raw)
		if err != nil {
			return lineage.Lineage{}, nil, nil, 0, errors.New("the disk's lineage key is damaged")
		}
		return l, key, &lineage.Update{Index: i, Snapshot: base.Ref, Time: base.Meta.Created}, i + 1, nil
	}
	key, err := crypto.GenerateSecp256k1Key()
	if err != nil {
		return lineage.Lineage{}, nil, nil, 0, err
	}
	owner, err := crypto.NewDefaultSigner(key).EthereumAddress()
	if err != nil {
		return lineage.Lineage{}, nil, nil, 0, err
	}
	var id [16]byte
	rand.Read(id[:])
	return lineage.New(owner, id), key, nil, 0, nil
}

// fresh holds a checkpoint's new chunks, and gets the rest from under.
type fresh struct {
	mu    sync.Mutex
	m     map[string]swarm.Chunk
	under storage.Getter
}

func (f *fresh) Get(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	f.mu.Lock()
	ch, ok := f.m[addr.ByteString()]
	f.mu.Unlock()
	if ok {
		return ch, nil
	}
	return f.under.Get(ctx, addr)
}

func (f *fresh) Put(_ context.Context, ch swarm.Chunk) error {
	f.mu.Lock()
	f.m[ch.Address().ByteString()] = ch
	f.mu.Unlock()
	return nil
}

func (f *fresh) chunks() []swarm.Chunk {
	f.mu.Lock()
	defer f.mu.Unlock()
	chs := make([]swarm.Chunk, 0, len(f.m))
	for _, ch := range f.m {
		chs = append(chs, ch)
	}
	return chs
}

// putter lets a function serve as a storage.Putter.
type putter func(context.Context, swarm.Chunk) error

func (p putter) Put(ctx context.Context, ch swarm.Chunk) error { return p(ctx, ch) }

// checkBatch refuses a batch that Bee cannot stamp with, or a mutable one
// (SPEC §9, P1).
func checkBatch(ctx context.Context, c *bee.Client, batch string) error {
	b, err := c.Batch(ctx, batch)
	if err != nil {
		return err
	}
	switch {
	case !b.Exists || !b.Usable:
		return fmt.Errorf("batch %s is not usable", batch)
	case !b.Immutable:
		return fmt.Errorf("batch %s is mutable; swarmblk stamps only with immutable batches (SPEC §9, P1)", batch)
	}
	return nil
}

// checkRoom refuses an upload that could overflow a bucket of the batch
// (SPEC §9, P4), counting every chunk as new.
func checkRoom(ctx context.Context, c *bee.Client, batch string, addrs []swarm.Address) error {
	bk, err := c.Buckets(ctx, batch)
	if err != nil {
		return err
	}
	need := map[uint32]uint32{}
	for _, a := range addrs {
		need[binary.BigEndian.Uint32(a.Bytes())>>(32-bk.BucketDepth)]++
	}
	for i, n := range need {
		if n+bk.Fill[i] > bk.Capacity {
			return fmt.Errorf("not enough room in the batch: bucket %d would need %d slots of %d (SPEC §9, P4)", i, n+bk.Fill[i], bk.Capacity)
		}
	}
	return nil
}

// upload sends the chunks at addrs from get to Bee, stamped from batch, a
// few at a time and the last, the manifest's root, after all the others,
// and waits until Bee has synced every one (SPEC §8, steps 3 to 6).
func upload(ctx context.Context, c *bee.Client, batch string, get storage.Getter, addrs []swarm.Address) error {
	tag, err := c.CreateTag(ctx)
	if err != nil {
		return err
	}
	put := func(ctx context.Context, a swarm.Address) error {
		ch, err := get.Get(ctx, a)
		if err == nil {
			err = c.PutChunk(ctx, batch, tag.UID, ch)
		}
		return err
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(uploads)
	for _, a := range addrs[:len(addrs)-1] {
		g.Go(func() error { return put(gctx, a) })
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if err := put(ctx, addrs[len(addrs)-1]); err != nil {
		return err
	}
	_, err = c.WaitSynced(ctx, tag.UID, 2*time.Second)
	return err
}
