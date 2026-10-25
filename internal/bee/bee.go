// Package bee is a minimal client for the Bee HTTP API (SPEC §13).
package bee

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// ErrInvalidChunk means that a node returned data that does not hash to the
// address asked for.
var ErrInvalidChunk = errors.New("chunk data does not match its address")

// Client sends requests to one Bee node.
type Client struct {
	URL  string       // API base, such as http://localhost:1633
	HTTP *http.Client // nil means http.DefaultClient

	// NoCache sends Swarm-Cache: false with downloads, so that Bee keeps no
	// copy of the chunks it retrieves.
	NoCache bool
}

// New returns a client for the Bee API at url that keeps up to conns
// connections open, one for each request in flight.
func New(url string, conns int) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil               // connect to Bee directly, whatever HTTP_PROXY says
	tr.DisableCompression = true // chunks do not compress
	tr.MaxIdleConnsPerHost = conns
	return &Client{URL: strings.TrimSuffix(url, "/"), HTTP: &http.Client{Transport: tr}}
}

// Chunk fetches the chunk at addr with GET /chunks/{address} and checks it
// against addr with Bee's pkg/cac. The BMT hash pads a payload with zeros, so
// a chunk that passes may lack trailing zeros that its span covers: read a
// leaf as span bytes, padded with zeros.
func (c *Client) Chunk(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL+"/chunks/"+addr.String(), nil)
	if err != nil {
		return nil, err
	}
	if c.NoCache {
		req.Header.Set("Swarm-Cache", "false")
	}
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	// One byte more than a chunk holds, so that a longer body fails the check.
	data, err := io.ReadAll(io.LimitReader(resp.Body, swarm.ChunkWithSpanSize+1))
	if err != nil {
		return nil, fmt.Errorf("GET /chunks/%s: %w", addr, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET /chunks/%s: %s: %s", addr, resp.Status, bytes.TrimSpace(data))
	}
	ch := swarm.NewChunk(addr, data)
	if !cac.Valid(ch) {
		return nil, fmt.Errorf("GET /chunks/%s: %w", addr, ErrInvalidChunk)
	}
	return ch, nil
}
