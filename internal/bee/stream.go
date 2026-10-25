package bee

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/gorilla/websocket"
)

// Stream uploads chunks over one WebSocket connection to /chunks/stream
// (SPEC §13). Bee stores the chunks in order and acknowledges each with an
// empty message once it is stored locally, which does not mean synced. A
// Stream keeps a window of chunks in flight. It is for one goroutine.
type Stream struct {
	conn    *websocket.Conn
	slots   chan struct{} // one token for each chunk in flight
	done    chan struct{} // closed when the reader stops
	acked   atomic.Int64
	closing atomic.Bool

	mu  sync.Mutex
	err error // why the reader stopped, if not because of Close
}

// Stream opens an upload stream stamped from batch, a hex batch ID, with up
// to window chunks in flight. With a tag (non-zero), Bee pushes the chunks
// later and counts them under the tag; without one, it pushes each chunk
// before acknowledging it.
func (c *Client) Stream(ctx context.Context, batch string, tag uint64, window int) (*Stream, error) {
	h := http.Header{}
	h.Set("Swarm-Postage-Batch-Id", batch)
	if tag != 0 {
		h.Set("Swarm-Tag", fmt.Sprint(tag))
	}
	url := "ws" + strings.TrimPrefix(c.URL, "http") + "/chunks/stream" // http → ws, https → wss
	d := websocket.Dialer{HandshakeTimeout: time.Minute}               // no Proxy: connect to Bee directly
	conn, resp, err := d.DialContext(ctx, url, h)
	if err != nil {
		if resp != nil {
			defer resp.Body.Close()
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return nil, &Error{Op: "GET /chunks/stream", Status: resp.StatusCode, Message: strings.TrimSpace(string(body))}
		}
		return nil, err
	}
	s := &Stream{conn: conn, slots: make(chan struct{}, max(window, 1)), done: make(chan struct{})}
	go s.read()
	return s, nil
}

// read counts acknowledgements until the connection ends.
func (s *Stream) read() {
	defer close(s.done)
	for {
		mt, msg, err := s.conn.ReadMessage()
		if err != nil {
			s.fail(err)
			return
		}
		if mt != websocket.BinaryMessage || len(msg) != 0 {
			s.fail(fmt.Errorf("unexpected message from Bee: type %d, %d bytes", mt, len(msg)))
			return
		}
		select {
		case <-s.slots:
			s.acked.Add(1)
		default:
			s.fail(errors.New("Bee acknowledged a chunk that was not sent"))
			return
		}
	}
}

// fail records why the stream ended. A close frame from Bee carries its
// reason, such as "batch is overissued".
func (s *Stream) fail(err error) {
	var ce *websocket.CloseError
	switch {
	case s.closing.Load(): // every chunk was acknowledged; how the connection ends no longer matters
		return
	case errors.As(err, &ce) && ce.Text == ErrOverissued.Error():
		err = fmt.Errorf("chunk stream: %w", ErrOverissued)
	case errors.As(err, &ce):
		err = fmt.Errorf("chunk stream closed by Bee: %s", ce.Text)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// Err returns why the stream ended early, or nil.
func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Acked returns how many chunks Bee has acknowledged. They are acknowledged
// in the order they were sent.
func (s *Stream) Acked() int64 { return s.acked.Load() }

// Put sends a chunk, first waiting for room in the window.
func (s *Stream) Put(ch swarm.Chunk) error {
	select {
	case s.slots <- struct{}{}:
	case <-s.done:
		return s.ended()
	}
	if err := s.conn.WriteMessage(websocket.BinaryMessage, ch.Data()); err != nil {
		// Bee may have closed the connection with a reason the reader has yet
		// to see; that reason says more than the write error.
		select {
		case <-s.done:
		case <-time.After(time.Second):
		}
		if e := s.Err(); e != nil {
			return e
		}
		return err
	}
	return nil
}

// ended returns why the reader stopped.
func (s *Stream) ended() error {
	if err := s.Err(); err != nil {
		return err
	}
	return errors.New("chunk stream ended")
}

// Close waits until Bee has acknowledged every chunk sent, then closes the
// connection. It returns the error that ended the stream, if any.
func (s *Stream) Close() error {
	defer s.conn.Close()
	for range cap(s.slots) { // holding every slot means nothing is in flight
		select {
		case s.slots <- struct{}{}:
		case <-s.done:
			return s.ended()
		}
	}
	s.closing.Store(true)
	msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
	if err := s.conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(10*time.Second)); err != nil {
		return err
	}
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
	}
	return s.Err()
}
