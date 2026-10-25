package lineage

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/petfold/swarmblk/internal/bee"
)

// ErrNotHead means that a lineage's head is not the update a writer expects,
// so it must not publish (SPEC §7). Either Bee cannot find the latest update
// yet, which a retry fixes, or someone else wrote to the lineage.
var ErrNotHead = errors.New("not the lineage's head")

// Head returns the latest update of l that Bee finds, or bee.ErrNoUpdate.
// GET /feeds gives its index, which can lag behind the feed, and the update
// itself comes from GET /chunks, checked against l.
func Head(ctx context.Context, c *bee.Client, l Lineage) (Update, error) {
	i, err := c.FeedIndex(ctx, l.Owner, l.Topic[:])
	if err != nil {
		return Update{}, err
	}
	return Get(ctx, c, l, i)
}

// Get fetches update i of l and checks it.
func Get(ctx context.Context, c *bee.Client, l Lineage, i uint64) (Update, error) {
	addr, err := l.Address(i)
	if err != nil {
		return Update{}, err
	}
	ch, err := c.SOC(ctx, addr)
	if err != nil {
		return Update{}, fmt.Errorf("lineage %s: update %d: %w", l, i, err)
	}
	return l.Open(i, ch)
}

// Publish adds update u to l, signed with key and stamped from batch. prev is
// the head the writer expects, or nil for a lineage without updates, and u
// must follow it. Publish reads the head first and refuses with ErrNotHead
// unless it is prev (SPEC §7). If the head already names u's snapshot at u's
// index, an earlier attempt succeeded, and Publish does nothing. Bee answers
// once a storer in the update's neighbourhood holds it.
func Publish(ctx context.Context, c *bee.Client, batch string, l Lineage, key *ecdsa.PrivateKey, prev *Update, u Update) error {
	if prev == nil && u.Index != 0 || prev != nil && u.Index != prev.Index+1 {
		return fmt.Errorf("lineage %s: update %d cannot follow %s", l, u.Index, describe(prev))
	}
	ch, err := l.Sign(key, u)
	if err != nil {
		return err
	}
	head, err := Head(ctx, c, l)
	switch {
	case errors.Is(err, bee.ErrNoUpdate):
		if prev != nil {
			return fmt.Errorf("lineage %s: %w: Bee finds no update, want %s", l, ErrNotHead, describe(prev))
		}
	case err != nil:
		return err
	case head.Index == u.Index && head.Snapshot.Equal(u.Snapshot):
		return nil // an earlier attempt published u
	case prev == nil || head.Index != prev.Index || !head.Snapshot.Equal(prev.Snapshot):
		return fmt.Errorf("lineage %s: %w: Bee finds %s, want %s", l, ErrNotHead, describe(&head), describe(prev))
	}
	return c.PutSOC(ctx, batch, ch)
}

func describe(u *Update) string {
	if u == nil {
		return "no update"
	}
	return fmt.Sprintf("update %d (snapshot %s)", u.Index, u.Snapshot)
}
