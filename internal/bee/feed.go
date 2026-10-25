package bee

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/soc"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// ErrNoUpdate means that Bee finds no update of a feed.
var ErrNoUpdate = errors.New("feed has no update")

// SOC fetches the single-owner chunk at addr with GET /chunks/{address} and
// checks its signature and address with Bee's pkg/soc.
func (c *Client) SOC(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	return c.chunk(ctx, addr, swarm.SocMaxChunkSize, soc.Valid)
}

// PutSOC uploads the single-owner chunk ch with POST /soc/{owner}/{id},
// stamped from batch, a hex batch ID. Bee pushes it at once and answers when
// a storer in the chunk's neighbourhood has sent a receipt, so no tag is
// needed to know that it arrived.
func (c *Client) PutSOC(ctx context.Context, batch string, ch swarm.Chunk) error {
	s, err := soc.FromChunkValidate(ch)
	if err != nil {
		return fmt.Errorf("POST /soc: %w", err)
	}
	url := fmt.Sprintf("%s/soc/%x/%x?sig=%x", c.URL, s.OwnerAddress(), s.ID(), s.Signature())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(s.WrappedChunk().Data()))
	if err != nil {
		return err
	}
	req.Header.Set("Swarm-Postage-Batch-Id", batch)
	req.Header.Set("Swarm-Deferred-Upload", "false")
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.do(req, http.StatusCreated)
	if err != nil {
		return err
	}
	return stored(resp, "POST /soc", ch.Address())
}

// FeedIndex returns the index of the latest update of the sequence feed
// (owner, topic) that Bee finds with GET /feeds/{owner}/{topic}, or
// ErrNoUpdate. Bee's lookup gives up on an update it cannot retrieve within
// a second, so the index can lag behind the feed.
func (c *Client) FeedIndex(ctx context.Context, owner common.Address, topic []byte) (uint64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/feeds/%x/%x", c.URL, owner[:], topic), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Swarm-Only-Root-Chunk", "true") // the index is all we want
	resp, err := c.do(req, http.StatusOK)
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, swarm.ChunkWithSpanSize))
	resp.Body.Close()
	h := resp.Header.Get("Swarm-Feed-Index")
	idx, err := hex.DecodeString(h)
	if err != nil || len(idx) != 8 {
		return 0, fmt.Errorf("GET /feeds: Swarm-Feed-Index %q, want 16 hex digits", h)
	}
	return binary.BigEndian.Uint64(idx), nil
}
