package nbd

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

// Export is a device that clients find by name.
type Export struct {
	Name        string
	Description string
	Device      Device
	ReadOnly    bool
}

// flags returns the export's transmission flags.
func (e *Export) flags() uint16 {
	f := uint16(flagHasFlags | flagSendFlush | flagSendFUA)
	if e.ReadOnly {
		return f | flagReadOnly
	}
	return f | flagSendTrim | flagSendWriteZeroes
}

// Server serves exports. The first export is also the default one, which a
// client gets by asking for the empty name.
type Server struct {
	Exports []Export
	// InFlight bounds the requests that one connection handles at once;
	// 0 means 64.
	InFlight int
	// ErrorLog receives the errors that end connections; nil discards them.
	ErrorLog *log.Logger
}

func (s *Server) find(name string) *Export {
	for i := range s.Exports {
		if s.Exports[i].Name == name {
			return &s.Exports[i]
		}
	}
	if name == "" && len(s.Exports) > 0 {
		return &s.Exports[0]
	}
	return nil
}

// Serve accepts connections on l and serves each until ctx ends, then closes
// l and waits for the connections to finish.
func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	stop := context.AfterFunc(ctx, func() { l.Close() })
	defer stop()
	for {
		c, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.ServeConn(ctx, c); err != nil && s.ErrorLog != nil {
				s.ErrorLog.Printf("nbd: %v", err)
			}
		}()
	}
}

// ServeConn serves one client on c until it disconnects or ctx ends, and
// closes c.
func (s *Server) ServeConn(ctx context.Context, c net.Conn) error {
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	r := bufio.NewReaderSize(c, 64<<10)
	ex, opts, err := s.handshake(r, c)
	if err != nil || ex == nil {
		return err
	}
	return s.transmit(ctx, r, c, ex, opts)
}

// session holds what the client chose during the handshake.
type session struct {
	noZeroes   bool
	structured bool
	meta       string // the export for which base:allocation was chosen
	allocation bool
}

// optionReply sends a reply to option opt.
func optionReply(w io.Writer, opt, typ uint32, data ...[]byte) error {
	n := 0
	for _, d := range data {
		n += len(d)
	}
	b := make([]byte, 0, 20+n)
	b = binary.BigEndian.AppendUint64(b, repMagic)
	b = binary.BigEndian.AppendUint32(b, opt)
	b = binary.BigEndian.AppendUint32(b, typ)
	b = binary.BigEndian.AppendUint32(b, uint32(n))
	for _, d := range data {
		b = append(b, d...)
	}
	_, err := w.Write(b)
	return err
}

// parser reads the fields of an option's data and remembers whether they
// were all there.
type parser struct {
	b  []byte
	ok bool
}

func (p *parser) take(n int) []byte {
	if !p.ok || n < 0 || n > len(p.b) {
		p.ok = false
		return nil
	}
	v := p.b[:n]
	p.b = p.b[n:]
	return v
}

func (p *parser) u16() uint16 {
	if b := p.take(2); b != nil {
		return binary.BigEndian.Uint16(b)
	}
	return 0
}

func (p *parser) u32() uint32 {
	if b := p.take(4); b != nil {
		return binary.BigEndian.Uint32(b)
	}
	return 0
}

func (p *parser) str() string { return string(p.take(int(p.u32()))) }

// done reports whether all fields were there and nothing is left.
func (p *parser) done() bool { return p.ok && len(p.b) == 0 }

// handshake negotiates with a client until it chooses an export, which it
// returns, or aborts, when it returns no export and no error.
func (s *Server) handshake(r io.Reader, w io.Writer) (*Export, *session, error) {
	b := binary.BigEndian.AppendUint64(nil, nbdMagic)
	b = binary.BigEndian.AppendUint64(b, optMagic)
	b = binary.BigEndian.AppendUint16(b, flagFixedNewstyle|flagNoZeroes)
	if _, err := w.Write(b); err != nil {
		return nil, nil, err
	}
	var hdr [16]byte
	if _, err := io.ReadFull(r, hdr[:4]); err != nil {
		return nil, nil, err
	}
	flags := binary.BigEndian.Uint32(hdr[:4])
	if flags&flagFixedNewstyle == 0 || flags&^(flagFixedNewstyle|flagNoZeroes) != 0 {
		return nil, nil, fmt.Errorf("client flags %#x", flags)
	}
	sess := &session{noZeroes: flags&flagNoZeroes != 0}

	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, nil, err
		}
		if m := binary.BigEndian.Uint64(hdr[:8]); m != optMagic {
			return nil, nil, fmt.Errorf("option magic %#x", m)
		}
		opt, n := binary.BigEndian.Uint32(hdr[8:12]), binary.BigEndian.Uint32(hdr[12:16])
		if n > 64<<10 {
			return nil, nil, fmt.Errorf("option %d of %d bytes", opt, n)
		}
		data := make([]byte, n)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, nil, err
		}
		p := &parser{b: data, ok: true}
		var err error
		switch opt {
		case optExportName:
			ex := s.find(string(data))
			if ex == nil {
				return nil, nil, fmt.Errorf("no export %q", data)
			}
			b := binary.BigEndian.AppendUint64(nil, uint64(ex.Device.Size()))
			b = binary.BigEndian.AppendUint16(b, ex.flags())
			if !sess.noZeroes {
				b = append(b, make([]byte, 124)...)
			}
			if sess.meta != ex.Name {
				sess.allocation = false
			}
			_, err = w.Write(b)
			return ex, sess, err
		case optAbort:
			optionReply(w, opt, repAck)
			return nil, nil, nil
		case optList:
			if n != 0 {
				err = optionReply(w, opt, repErrInvalid)
				break
			}
			for _, e := range s.Exports { // each name, then its description
				if err = optionReply(w, opt, repServer, binary.BigEndian.AppendUint32(nil, uint32(len(e.Name))), []byte(e.Name), []byte(e.Description)); err != nil {
					break
				}
			}
			if err == nil {
				err = optionReply(w, opt, repAck)
			}
		case optInfo, optGo:
			name := p.str()
			infos := make([]uint16, p.u16())
			for i := range infos {
				infos[i] = p.u16()
			}
			ex := s.find(name)
			switch {
			case !p.done():
				err = optionReply(w, opt, repErrInvalid)
			case ex == nil:
				err = optionReply(w, opt, repErrUnknown)
			default:
				err = s.info(w, opt, ex, infos)
				if err == nil && opt == optGo {
					if sess.meta != ex.Name {
						sess.allocation = false
					}
					return ex, sess, nil
				}
			}
		case optStructuredReply:
			if n != 0 {
				err = optionReply(w, opt, repErrInvalid)
				break
			}
			sess.structured = true
			err = optionReply(w, opt, repAck)
		case optListMetaContext, optSetMetaContext:
			err = s.metaContext(w, opt, p, sess)
		default: // including optStartTLS
			err = optionReply(w, opt, repErrUnsup)
		}
		if err != nil {
			return nil, nil, err
		}
	}
}

// info answers NBD_OPT_INFO or NBD_OPT_GO for ex: its size and flags, and
// whatever else of its name, description and block sizes the client asked.
func (s *Server) info(w io.Writer, opt uint32, ex *Export, infos []uint16) error {
	b := binary.BigEndian.AppendUint16(nil, infoExport)
	b = binary.BigEndian.AppendUint64(b, uint64(ex.Device.Size()))
	b = binary.BigEndian.AppendUint16(b, ex.flags())
	if err := optionReply(w, opt, repInfo, b); err != nil {
		return err
	}
	for _, typ := range infos {
		b := binary.BigEndian.AppendUint16(nil, typ)
		switch typ {
		case infoName:
			b = append(b, ex.Name...)
		case infoDescription:
			b = append(b, ex.Description...)
		case infoBlockSize:
			b = binary.BigEndian.AppendUint32(b, MinBlock)
			b = binary.BigEndian.AppendUint32(b, Preferred)
			b = binary.BigEndian.AppendUint32(b, MaxPayload)
		default:
			continue
		}
		if err := optionReply(w, opt, repInfo, b); err != nil {
			return err
		}
	}
	return optionReply(w, opt, repAck)
}

// metaContext answers NBD_OPT_LIST_META_CONTEXT and NBD_OPT_SET_META_CONTEXT.
// base:allocation is the only context; listing with no queries, or with the
// query "base:", finds it.
func (s *Server) metaContext(w io.Writer, opt uint32, p *parser, sess *session) error {
	name := p.str()
	queries := make([]string, p.u32())
	for i := range queries {
		queries[i] = p.str()
	}
	switch {
	case !p.done() || opt == optSetMetaContext && !sess.structured:
		return optionReply(w, opt, repErrInvalid)
	case s.find(name) == nil:
		return optionReply(w, opt, repErrUnknown)
	}
	found := opt == optListMetaContext && len(queries) == 0
	for _, q := range queries {
		found = found || q == baseAllocation || opt == optListMetaContext && q == "base:"
	}
	if opt == optSetMetaContext {
		sess.meta, sess.allocation = name, found
	}
	if found {
		if err := optionReply(w, opt, repMetaContext, binary.BigEndian.AppendUint32(nil, metaAllocation), []byte(baseAllocation)); err != nil {
			return err
		}
	}
	return optionReply(w, opt, repAck)
}

// request is one command from the client.
type request struct {
	flags, typ uint16
	cookie     uint64
	off        int64
	length     uint32
	data       []byte // a write's payload
}

// transmit serves ex's commands until the client disconnects. Requests run
// concurrently, up to InFlight at a time; replies go out one at a time.
func (s *Server) transmit(ctx context.Context, r io.Reader, w io.Writer, ex *Export, sess *session) error {
	inflight := s.InFlight
	if inflight <= 0 {
		inflight = 64
	}
	t := &transmission{ex: ex, sess: sess, w: w}
	sem := make(chan struct{}, inflight)
	var wg sync.WaitGroup
	defer wg.Wait()
	var hdr [28]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if m := binary.BigEndian.Uint32(hdr[:4]); m != requestMagic {
			return fmt.Errorf("request magic %#x", m)
		}
		req := request{
			flags:  binary.BigEndian.Uint16(hdr[4:6]),
			typ:    binary.BigEndian.Uint16(hdr[6:8]),
			cookie: binary.BigEndian.Uint64(hdr[8:16]),
			off:    int64(binary.BigEndian.Uint64(hdr[16:24])),
			length: binary.BigEndian.Uint32(hdr[24:28]),
		}
		switch req.typ {
		case cmdDisc:
			return nil
		case cmdWrite:
			if req.length > MaxPayload {
				return fmt.Errorf("write of %d bytes", req.length)
			}
			req.data = make([]byte, req.length)
			if _, err := io.ReadFull(r, req.data); err != nil {
				return err
			}
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			t.do(ctx, req)
		}()
	}
}

// transmission serves one connection's commands.
type transmission struct {
	ex   *Export
	sess *session
	mu   sync.Mutex // guards w
	w    io.Writer
	err  error // the first failed write; later replies are dropped
}

// send writes one reply, made of the given buffers.
func (t *transmission) send(bufs ...[]byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err == nil {
		nb := net.Buffers(bufs)
		_, t.err = nb.WriteTo(t.w)
	}
}

// simple sends a simple reply: an error, or success with data.
func (t *transmission) simple(cookie uint64, e Errno, data []byte) {
	b := binary.BigEndian.AppendUint32(nil, simpleReplyMagic)
	b = binary.BigEndian.AppendUint32(b, uint32(e))
	b = binary.BigEndian.AppendUint64(b, cookie)
	t.send(b, data)
}

// chunk sends the last chunk of a structured reply, with its payload.
func (t *transmission) chunk(cookie uint64, typ uint16, payload ...[]byte) {
	n := 0
	for _, p := range payload {
		n += len(p)
	}
	b := binary.BigEndian.AppendUint32(nil, structuredReplyMagic)
	b = binary.BigEndian.AppendUint16(b, replyFlagDone)
	b = binary.BigEndian.AppendUint16(b, typ)
	b = binary.BigEndian.AppendUint64(b, cookie)
	b = binary.BigEndian.AppendUint32(b, uint32(n))
	t.send(append([][]byte{b}, payload...)...)
}

// fail answers req with an error: as an error chunk to a read or a block
// status query when structured replies are on, and as a simple reply
// otherwise.
func (t *transmission) fail(req request, e Errno) {
	if t.sess.structured && (req.typ == cmdRead || req.typ == cmdBlockStatus) {
		b := binary.BigEndian.AppendUint32(nil, uint32(e))
		t.chunk(req.cookie, replyTypeError, binary.BigEndian.AppendUint16(b, 0))
		return
	}
	t.simple(req.cookie, e, nil)
}

func (t *transmission) do(ctx context.Context, req request) {
	dev := t.ex.Device
	end := req.off + int64(req.length)
	writes := req.typ == cmdWrite || req.typ == cmdTrim || req.typ == cmdWriteZeroes
	switch {
	case req.typ == cmdFlush:
		if err := dev.Flush(ctx); err != nil {
			t.fail(req, errno(err))
			return
		}
		t.simple(req.cookie, 0, nil)
		return
	case writes && t.ex.ReadOnly:
		t.fail(req, EPERM)
		return
	case req.off < 0 || req.length == 0 || end > dev.Size():
		t.fail(req, EINVAL)
		return
	case req.typ == cmdRead && req.length > MaxPayload:
		t.fail(req, EOVERFLOW)
		return
	}

	fua := req.flags&cmdFlagFUA != 0
	var err error
	switch req.typ {
	case cmdRead:
		data := make([]byte, req.length)
		if err = dev.ReadAt(ctx, data, req.off); err == nil {
			if t.sess.structured {
				t.chunk(req.cookie, replyTypeOffsetData, binary.BigEndian.AppendUint64(nil, uint64(req.off)), data)
			} else {
				t.simple(req.cookie, 0, data)
			}
			return
		}
	case cmdWrite:
		err = dev.WriteAt(ctx, req.data, req.off, fua)
	case cmdTrim:
		err = dev.Trim(ctx, req.off, int64(req.length), fua)
	case cmdWriteZeroes:
		err = dev.WriteZeroes(ctx, req.off, int64(req.length), fua, req.flags&cmdFlagNoHole != 0)
	case cmdBlockStatus:
		if !t.sess.structured || !t.sess.allocation {
			err = EINVAL
			break
		}
		var payload []byte
		if payload, err = t.status(ctx, req); err == nil {
			t.chunk(req.cookie, replyTypeBlockStatus, payload)
			return
		}
	default:
		err = EINVAL
	}
	if err != nil {
		t.fail(req, errno(err))
		return
	}
	t.simple(req.cookie, 0, nil)
}

// status builds a block status reply's payload for base:allocation: the
// context ID and one descriptor per extent, or only the first with
// NBD_CMD_FLAG_REQ_ONE, cut to the request's length.
func (t *transmission) status(ctx context.Context, req request) ([]byte, error) {
	extents, err := t.ex.Device.Extents(ctx, req.off, int64(req.length))
	if err != nil {
		return nil, err
	}
	b := binary.BigEndian.AppendUint32(nil, metaAllocation)
	left := int64(req.length)
	for _, e := range extents {
		if e.Length <= 0 || left == 0 {
			continue
		}
		n := min(e.Length, left)
		left -= n
		var flags uint32
		if e.Hole {
			flags |= stateHole
		}
		if e.Zero {
			flags |= stateZero
		}
		b = binary.BigEndian.AppendUint32(b, uint32(n))
		b = binary.BigEndian.AppendUint32(b, flags)
		if req.flags&cmdFlagReqOne != 0 {
			break
		}
	}
	if len(b) == 4 {
		return nil, fmt.Errorf("no extents at %d", req.off)
	}
	return b, nil
}
