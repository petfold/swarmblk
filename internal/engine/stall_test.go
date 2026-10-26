package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// TestStall reads a block whose leaf cannot be fetched at first: without a
// Stall the read fails at once; with one that says go on, it stalls until
// the leaf comes, and the Stall hears of each failure and then the end; a
// Stall that gives up ends the read with its error.
func TestStall(t *testing.T) {
	img, root, s := testDisk(t)
	ctx := context.Background()
	const b = 1030 // random data
	var mu sync.Mutex
	failing := 0 // fetches of the block's leaf still to fail
	var leaf swarm.Address
	get := storage.GetterFunc(func(ctx context.Context, a swarm.Address) (swarm.Chunk, error) {
		mu.Lock()
		defer mu.Unlock()
		if a.Equal(leaf) && failing > 0 {
			failing--
			return nil, errors.New("no peer has it")
		}
		return s.Get(ctx, a)
	})
	snap, err := Open(ctx, get, root, 8)
	if err != nil {
		t.Fatal(err)
	}
	if leaf, _, err = snap.leaf(ctx, b); err != nil {
		t.Fatal(err)
	}
	want := img[b*swarm.ChunkSize : (b+1)*swarm.ChunkSize]
	got := make([]byte, swarm.ChunkSize)

	failing = 1
	if err := snap.ReadAt(ctx, got, b*swarm.ChunkSize); err == nil {
		t.Fatal("a read without a Stall did not fail")
	}

	var calls []int // failures, or 0 for the end
	snap.SetStall(func(ctx context.Context, blk int64, err error, failures int) error {
		if blk != b || (err == nil) != (len(calls) == 3) {
			t.Errorf("stall for block %d, error %v, after %v", blk, err, calls)
		}
		if err == nil {
			failures = 0
		}
		calls = append(calls, failures)
		return nil
	})
	failing = 3
	if err := snap.ReadAt(ctx, got, b*swarm.ChunkSize); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("a stalled read: %v", err)
	}
	if !slices.Equal(calls, []int{1, 2, 3, 0}) {
		t.Errorf("the Stall was called with %v", calls)
	}

	giveUp := errors.New("given up")
	snap.SetStall(func(context.Context, int64, error, int) error { return giveUp })
	failing = 1
	if err := snap.ReadAt(ctx, got, b*swarm.ChunkSize); !errors.Is(err, giveUp) {
		t.Errorf("a read whose Stall gave up: %v", err)
	}
}
