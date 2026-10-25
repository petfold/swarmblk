package bee

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// Tag is an upload tag: chunks counted as they are split, found already
// present (seen), stored, sent and synced. Only synced means that a chunk
// reached its neighbourhood (SPEC §8).
type Tag struct {
	UID       uint64        `json:"uid"`
	Split     uint64        `json:"split"`
	Seen      uint64        `json:"seen"`
	Stored    uint64        `json:"stored"`
	Sent      uint64        `json:"sent"`
	Synced    uint64        `json:"synced"`
	Address   swarm.Address `json:"address"`
	StartedAt time.Time     `json:"startedAt"`
}

// CreateTag creates an upload tag with POST /tags.
func (c *Client) CreateTag(ctx context.Context) (Tag, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/tags", nil)
	if err != nil {
		return Tag{}, err
	}
	resp, err := c.do(req, http.StatusCreated)
	if err != nil {
		return Tag{}, err
	}
	defer resp.Body.Close()
	var t Tag
	err = json.NewDecoder(resp.Body).Decode(&t)
	return t, err
}

// Tag returns the counters of the tag uid with GET /tags/{uid}.
func (c *Client) Tag(ctx context.Context, uid uint64) (Tag, error) {
	var t Tag
	err := c.getJSON(ctx, fmt.Sprintf("/tags/%d", uid), &t)
	return t, err
}

// Done reports whether every chunk counted under the tag has been synced to
// its neighbourhood: those found already present (seen) are not pushed
// again, so the rest must all be synced. This is the arithmetic of SPEC §8,
// step 6, pinned for Bee 2.8.3 by the integration tests.
func (t Tag) Done() bool { return t.Split > 0 && t.Synced >= t.Split-t.Seen }

// WaitSynced polls the tag uid every interval until it is Done, or until ctx
// ends. Call it once nothing more is uploaded under the tag.
func (c *Client) WaitSynced(ctx context.Context, uid uint64, every time.Duration) (Tag, error) {
	for {
		t, err := c.Tag(ctx, uid)
		if err != nil || t.Done() {
			return t, err
		}
		select {
		case <-ctx.Done():
			return t, fmt.Errorf("tag %d: %d of %d chunks synced: %w", uid, t.Synced, t.Split-t.Seen, ctx.Err())
		case <-time.After(every):
		}
	}
}

// Batch is a postage batch as GET /stamps/{id} reports it.
type Batch struct {
	ID          string `json:"batchID"`
	Label       string `json:"label"`
	Depth       uint8  `json:"depth"`
	BucketDepth uint8  `json:"bucketDepth"`
	Immutable   bool   `json:"immutableFlag"`
	Usable      bool   `json:"usable"`
	Exists      bool   `json:"exists"`
	Utilization uint32 `json:"utilization"` // chunks in the fullest bucket
	TTL         int64  `json:"batchTTL"`    // seconds, at the current price
}

// Batch returns the batch with the hex ID id.
func (c *Client) Batch(ctx context.Context, id string) (Batch, error) {
	var b Batch
	err := c.getJSON(ctx, "/stamps/"+id, &b)
	return b, err
}

// Buckets is the fill of every bucket of a batch.
type Buckets struct {
	Depth       uint8
	BucketDepth uint8
	Capacity    uint32   // chunks a bucket holds: 2^(Depth−BucketDepth)
	Fill        []uint32 // chunks in each bucket, by bucket number
}

// Buckets returns the fill of every bucket of the batch id, with GET
// /stamps/{id}/buckets.
func (c *Client) Buckets(ctx context.Context, id string) (Buckets, error) {
	var r struct {
		Depth            uint8  `json:"depth"`
		BucketDepth      uint8  `json:"bucketDepth"`
		BucketUpperBound uint32 `json:"bucketUpperBound"`
		Buckets          []struct {
			BucketID   uint32 `json:"bucketID"`
			Collisions uint32 `json:"collisions"`
		} `json:"buckets"`
	}
	if err := c.getJSON(ctx, "/stamps/"+id+"/buckets", &r); err != nil {
		return Buckets{}, err
	}
	b := Buckets{Depth: r.Depth, BucketDepth: r.BucketDepth, Capacity: r.BucketUpperBound, Fill: make([]uint32, 1<<r.BucketDepth)}
	for _, x := range r.Buckets {
		if int(x.BucketID) >= len(b.Fill) {
			return Buckets{}, fmt.Errorf("GET /stamps/%s/buckets: bucket %d of %d", id, x.BucketID, len(b.Fill))
		}
		b.Fill[x.BucketID] = x.Collisions
	}
	return b, nil
}
