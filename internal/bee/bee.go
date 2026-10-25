// Package bee is a minimal client for the Bee HTTP API (SPEC §13).
package bee

import (
	"bytes"
	"context"
	"encoding/json"
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

// ErrOverissued means that a bucket of the batch is full. An immutable batch
// then takes no more chunks in that bucket, so new chunks need a new or
// diluted batch (SPEC §9, P4).
var ErrOverissued = errors.New("batch is overissued")

// Error is an error response from Bee.
type Error struct {
	Op      string // the request, such as "POST /chunks"
	Status  int    // the HTTP status
	Message string // Bee's message
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %d %s: %s", e.Op, e.Status, http.StatusText(e.Status), e.Message)
}

// Is reports a 402 from a chunk upload as ErrOverissued.
func (e *Error) Is(target error) bool {
	return target == ErrOverissued && e.Status == http.StatusPaymentRequired
}

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

// do sends a request and returns the response if its status is want. Any
// other status becomes an *Error carrying Bee's message.
func (c *Client) do(req *http.Request, want int) (*http.Response, error) {
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == want {
		return resp, nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	msg := strings.TrimSpace(string(body))
	var e struct{ Message string }
	if json.Unmarshal(body, &e) == nil && e.Message != "" {
		msg = e.Message
	}
	return nil, &Error{Op: req.Method + " " + req.URL.Path, Status: resp.StatusCode, Message: msg}
}

// getJSON decodes the response to GET path into out.
func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req, http.StatusOK)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
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
	resp, err := c.do(req, http.StatusOK)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	// One byte more than a chunk holds, so that a longer body fails the check.
	data, err := io.ReadAll(io.LimitReader(resp.Body, swarm.ChunkWithSpanSize+1))
	if err != nil {
		return nil, fmt.Errorf("GET /chunks/%s: %w", addr, err)
	}
	ch := swarm.NewChunk(addr, data)
	if !cac.Valid(ch) {
		return nil, fmt.Errorf("GET /chunks/%s: %w", addr, ErrInvalidChunk)
	}
	return ch, nil
}

// PutChunk uploads one chunk with POST /chunks, stamped from batch, a hex
// batch ID. With a tag (non-zero), Bee stores the chunk and pushes it later,
// counting it under the tag; without one, it pushes the chunk before
// answering.
func (c *Client) PutChunk(ctx context.Context, batch string, tag uint64, ch swarm.Chunk) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/chunks", bytes.NewReader(ch.Data()))
	if err != nil {
		return err
	}
	req.Header.Set("Swarm-Postage-Batch-Id", batch)
	req.Header.Set("Content-Type", "application/octet-stream")
	if tag != 0 {
		req.Header.Set("Swarm-Tag", fmt.Sprint(tag))
	}
	resp, err := c.do(req, http.StatusCreated)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var r struct{ Reference swarm.Address }
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return fmt.Errorf("POST /chunks: %w", err)
	}
	if !r.Reference.Equal(ch.Address()) {
		return fmt.Errorf("POST /chunks: Bee stored %s, want %s", r.Reference, ch.Address())
	}
	return nil
}
