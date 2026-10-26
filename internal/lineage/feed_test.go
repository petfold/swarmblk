package lineage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/beetest"
)

const testBatch = "c7af5ddacf81eaa13304ea78b497c8d719b2f5c0bf53b8ab01654cb4c65edecb"

func TestPublish(t *testing.T) {
	ctx := context.Background()
	b := beetest.New(testBatch)
	c := b.Client(t)
	l, key := testLineage(t)
	u0, u1, u2 := testUpdate(0), testUpdate(1), testUpdate(2)
	posts := func(want int) {
		t.Helper()
		if n := len(b.Received()); n != want {
			t.Errorf("%d updates posted, want %d", n, want)
		}
	}
	hide := func(i uint64, hidden bool) {
		t.Helper()
		addr, err := l.Address(i)
		if err != nil {
			t.Fatal(err)
		}
		b.Hide(addr, hidden)
	}
	head := func(want Update) {
		t.Helper()
		if got, err := Head(ctx, c, l); err != nil || !same(got, want) {
			t.Errorf("Head = %+v, %v; want %+v", got, err, want)
		}
	}

	if _, err := Head(ctx, c, l); !errors.Is(err, bee.ErrNoUpdate) {
		t.Errorf("Head of a new lineage: %v, want ErrNoUpdate", err)
	}
	if err := Publish(ctx, c, testBatch, l, key, &u0, u1); !errors.Is(err, ErrNotHead) {
		t.Errorf("update 1 to a lineage without updates: %v, want ErrNotHead", err)
	}
	if err := Publish(ctx, c, testBatch, l, key, nil, u0); err != nil {
		t.Fatal(err)
	}
	head(u0)
	// A retry after an update arrived does nothing.
	if err := Publish(ctx, c, testBatch, l, key, nil, u0); err != nil {
		t.Errorf("publishing update 0 again: %v", err)
	}
	posts(1)
	if err := Publish(ctx, c, testBatch, l, key, &u0, u1); err != nil {
		t.Fatal(err)
	}
	head(u1)
	posts(2)

	other := testUpdate(9)
	other.Index = 1
	for name, prev := range map[string]*Update{"no update": nil, "update 0": &u0} {
		next := other
		if prev == nil {
			next.Index = 0
		}
		if err := Publish(ctx, c, testBatch, l, key, prev, next); !errors.Is(err, ErrNotHead) {
			t.Errorf("after %s, while the head is update 1: %v, want ErrNotHead", name, err)
		}
	}
	posts(2)

	// Bee cannot find update 1 yet: the writer must wait.
	hide(1, true)
	if err := Publish(ctx, c, testBatch, l, key, &u1, u2); !errors.Is(err, ErrNotHead) || !strings.Contains(err.Error(), "finds update 0") {
		t.Errorf("with update 1 out of sight: %v, want ErrNotHead naming update 0", err)
	}
	hide(1, false)
	if err := Publish(ctx, c, testBatch, l, key, &u1, u2); err != nil {
		t.Fatal(err)
	}
	head(u2)
	posts(3)

	_, otherKey := testLineage(t)
	u3 := testUpdate(3)
	if err := Publish(ctx, c, testBatch, l, otherKey, &u2, u3); err == nil {
		t.Error("Publish signed with another lineage's key")
	}
	if err := Publish(ctx, c, testBatch, l, key, &u1, u3); err == nil || errors.Is(err, ErrNotHead) {
		t.Errorf("update 3 after update 1: %v, want a refusal before any request", err)
	}
	posts(3)
}
