package nbd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// client speaks the protocol by hand, to test what QEMU never sends.
type client struct {
	t *testing.T
	c net.Conn
}

func dial(t *testing.T, sock string, flags uint32) *client {
	t.Helper()
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(10 * time.Second))
	cl := &client{t, c}
	greeting := cl.read(18)
	if binary.BigEndian.Uint64(greeting) != nbdMagic || binary.BigEndian.Uint64(greeting[8:]) != optMagic ||
		binary.BigEndian.Uint16(greeting[16:]) != flagFixedNewstyle|flagNoZeroes {
		t.Fatalf("greeting %x", greeting)
	}
	cl.write(binary.BigEndian.AppendUint32(nil, flags))
	return cl
}

func (cl *client) read(n int) []byte {
	cl.t.Helper()
	b := make([]byte, n)
	if _, err := io.ReadFull(cl.c, b); err != nil {
		cl.t.Fatalf("reading %d bytes: %v", n, err)
	}
	return b
}

func (cl *client) write(b []byte) {
	cl.t.Helper()
	if _, err := cl.c.Write(b); err != nil {
		cl.t.Fatal(err)
	}
}

// closed reports whether the server has closed the connection.
func (cl *client) closed() bool {
	_, err := cl.c.Read(make([]byte, 1))
	return errors.Is(err, io.EOF)
}

func (cl *client) option(opt uint32, data []byte) {
	cl.t.Helper()
	b := binary.BigEndian.AppendUint64(nil, optMagic)
	b = binary.BigEndian.AppendUint32(b, opt)
	b = binary.BigEndian.AppendUint32(b, uint32(len(data)))
	cl.write(append(b, data...))
}

// replies reads option replies up to an ACK or an error and returns their
// types and data.
func (cl *client) replies(opt uint32) (types []uint32, data [][]byte) {
	cl.t.Helper()
	for {
		h := cl.read(20)
		if binary.BigEndian.Uint64(h) != repMagic || binary.BigEndian.Uint32(h[8:]) != opt {
			cl.t.Fatalf("option reply %x to option %d", h, opt)
		}
		typ := binary.BigEndian.Uint32(h[12:])
		types = append(types, typ)
		data = append(data, cl.read(int(binary.BigEndian.Uint32(h[16:]))))
		if typ == repAck || typ&(1<<31) != 0 {
			return types, data
		}
	}
}

// last returns the final reply type to option opt with data.
func (cl *client) last(opt uint32, data []byte) uint32 {
	cl.t.Helper()
	cl.option(opt, data)
	types, _ := cl.replies(opt)
	return types[len(types)-1]
}

func nameData(name string, extra ...byte) []byte {
	return append(append(binary.BigEndian.AppendUint32(nil, uint32(len(name))), name...), extra...)
}

// metaData is the data of a meta context option for the queries.
func metaData(name string, queries ...string) []byte {
	b := binary.BigEndian.AppendUint32(nameData(name), uint32(len(queries)))
	for _, q := range queries {
		b = append(binary.BigEndian.AppendUint32(b, uint32(len(q))), q...)
	}
	return b
}

// goTo chooses an export with NBD_OPT_GO.
func (cl *client) goTo(name string) {
	cl.t.Helper()
	if typ := cl.last(optGo, nameData(name, 0, 0)); typ != repAck {
		cl.t.Fatalf("NBD_OPT_GO %q: reply %#x", name, typ)
	}
}

func (cl *client) request(typ, flags uint16, cookie uint64, off int64, length uint32, data []byte) {
	cl.t.Helper()
	b := binary.BigEndian.AppendUint32(nil, requestMagic)
	b = binary.BigEndian.AppendUint16(b, flags)
	b = binary.BigEndian.AppendUint16(b, typ)
	b = binary.BigEndian.AppendUint64(b, cookie)
	b = binary.BigEndian.AppendUint64(b, uint64(off))
	b = binary.BigEndian.AppendUint32(b, length)
	cl.write(append(b, data...))
}

// simple reads a simple reply and returns its cookie and error.
func (cl *client) simple() (uint64, Errno) {
	cl.t.Helper()
	h := cl.read(16)
	if binary.BigEndian.Uint32(h) != simpleReplyMagic {
		cl.t.Fatalf("simple reply %x", h)
	}
	return binary.BigEndian.Uint64(h[8:]), Errno(binary.BigEndian.Uint32(h[4:]))
}

// chunk reads a structured reply chunk and returns its type and payload.
func (cl *client) chunk() (uint16, []byte) {
	cl.t.Helper()
	h := cl.read(20)
	if binary.BigEndian.Uint32(h) != structuredReplyMagic || binary.BigEndian.Uint16(h[4:]) != replyFlagDone {
		cl.t.Fatalf("structured reply %x", h)
	}
	return binary.BigEndian.Uint16(h[6:]), cl.read(int(binary.BigEndian.Uint32(h[16:])))
}

func TestHandshake(t *testing.T) {
	disk := newMemDevice(64 << 20)
	sock := serve(t, Export{Name: "a", Device: disk}, Export{Name: "b", Device: newMemDevice(64 << 20)})

	if cl := dial(t, sock, 0); !cl.closed() {
		t.Error("a client that is not fixed newstyle was served")
	}
	cl := dial(t, sock, flagFixedNewstyle|flagNoZeroes)
	for name, c := range map[string]struct {
		opt  uint32
		data []byte
		want uint32
	}{
		"unknown option":         {99, nil, repErrUnsup},
		"TLS":                    {optStartTLS, nil, repErrUnsup},
		"list with data":         {optList, []byte{1}, repErrInvalid},
		"info, no export":        {optInfo, nameData("c", 0, 0), repErrUnknown},
		"info, short":            {optInfo, nameData("a", 0, 1), repErrInvalid},
		"set before structured":  {optSetMetaContext, metaData("a", baseAllocation), repErrInvalid},
		"list contexts, unknown": {optListMetaContext, metaData("c"), repErrUnknown},
		"info":                   {optInfo, nameData("a", 0, 1, 0, infoBlockSize), repAck},
	} {
		if got := cl.last(c.opt, c.data); got != c.want {
			t.Errorf("%s: reply %#x, want %#x", name, got, c.want)
		}
	}
	for query, want := range map[string]int{"base:": 1, baseAllocation: 1, "qemu:dirty-bitmap:x": 0} {
		cl.option(optListMetaContext, metaData("a", query))
		if types, _ := cl.replies(optListMetaContext); len(types)-1 != want {
			t.Errorf("listing contexts for %q: replies %v, want %d contexts", query, types, want)
		}
	}
	cl.option(optAbort, nil)
	if types, _ := cl.replies(optAbort); types[0] != repAck || !cl.closed() {
		t.Errorf("abort: replies %v; the server must then close", types)
	}

	// NBD_OPT_EXPORT_NAME, with and without the 124 bytes of padding.
	for _, flags := range []uint32{flagFixedNewstyle, flagFixedNewstyle | flagNoZeroes} {
		cl := dial(t, sock, flags)
		cl.option(optExportName, []byte("a"))
		want := 10
		if flags&flagNoZeroes == 0 {
			want += 124
		}
		r := cl.read(want)
		if binary.BigEndian.Uint64(r) != uint64(disk.size) || binary.BigEndian.Uint16(r[8:]) != (&Export{}).flags() {
			t.Errorf("export name reply %x", r[:10])
		}
		cl.request(cmdDisc, 0, 1, 0, 0, nil)
		if !cl.closed() {
			t.Error("the server stayed connected after NBD_CMD_DISC")
		}
	}
	cl = dial(t, sock, flagFixedNewstyle)
	cl.option(optExportName, []byte("c"))
	if !cl.closed() {
		t.Error("an unknown export name was served")
	}
	cl = dial(t, sock, flagFixedNewstyle)
	cl.write(bytes.Repeat([]byte{1}, 16))
	if !cl.closed() {
		t.Error("bad option magic was taken")
	}
}

func TestTransmission(t *testing.T) {
	disk := newMemDevice(64 << 20)
	disk.set(4096, 4096, bytes.Repeat([]byte{7}, 4096), false)
	sock := serve(t, Export{Name: "a", Device: disk}, Export{Name: "ro", Device: newMemDevice(64 << 20), ReadOnly: true})

	// Without structured replies, reads get simple replies and block status
	// is refused.
	cl := dial(t, sock, flagFixedNewstyle|flagNoZeroes)
	cl.goTo("a")
	cl.request(cmdRead, 0, 1, 4096, 4096, nil)
	if cookie, e := cl.simple(); cookie != 1 || e != 0 || !bytes.Equal(cl.read(4096), bytes.Repeat([]byte{7}, 4096)) {
		t.Errorf("simple read: cookie %d, error %v", cookie, e)
	}
	cl.request(cmdBlockStatus, 0, 2, 0, 4096, nil)
	if _, e := cl.simple(); e != EINVAL {
		t.Errorf("block status without structured replies: %v", e)
	}

	// With structured replies and base:allocation for another export, block
	// status is still refused.
	cl = dial(t, sock, flagFixedNewstyle|flagNoZeroes)
	if typ := cl.last(optStructuredReply, nil); typ != repAck {
		t.Fatalf("structured replies: %#x", typ)
	}
	cl.last(optSetMetaContext, metaData("ro", baseAllocation))
	cl.goTo("a")
	cl.request(cmdBlockStatus, 0, 3, 0, 4096, nil)
	if typ, p := cl.chunk(); typ != replyTypeError || binary.BigEndian.Uint32(p) != uint32(EINVAL) {
		t.Errorf("block status with a context for another export: chunk %d %x", typ, p)
	}

	cl = dial(t, sock, flagFixedNewstyle|flagNoZeroes)
	cl.last(optStructuredReply, nil)
	cl.option(optSetMetaContext, metaData("a", "other:context", baseAllocation))
	if types, data := cl.replies(optSetMetaContext); len(types) != 2 || binary.BigEndian.Uint32(data[0]) != metaAllocation || string(data[0][4:]) != baseAllocation {
		t.Fatalf("set meta context: %v %q", types, data)
	}
	cl.goTo("a")
	cl.request(cmdBlockStatus, cmdFlagReqOne, 4, 0, 3*4096, nil)
	if typ, p := cl.chunk(); typ != replyTypeBlockStatus || !bytes.Equal(p, []byte{0, 0, 0, 1, 0, 0, 0x10, 0, 0, 0, 0, stateHole | stateZero}) {
		t.Errorf("block status, one extent: chunk %d %x", typ, p)
	}
	cl.request(cmdBlockStatus, 0, 5, 0, 3*4096, nil)
	if _, p := cl.chunk(); len(p) != 4+3*8 || p[11] != stateHole|stateZero || p[19] != 0 || p[27] != stateHole|stateZero {
		t.Errorf("block status: %x", p)
	}
	for name, c := range map[string]struct {
		off    int64
		length uint32
		want   Errno
	}{
		"past the end":   {64<<20 - 4096, 8192, EINVAL},
		"empty":          {0, 0, EINVAL},
		"too long":       {0, MaxPayload + 4096, EOVERFLOW},
		"negative start": {-4096, 4096, EINVAL},
	} {
		cl.request(cmdRead, 0, 6, c.off, c.length, nil)
		if typ, p := cl.chunk(); typ != replyTypeError || Errno(binary.BigEndian.Uint32(p)) != c.want {
			t.Errorf("read %s: chunk %d %x, want %v", name, typ, p, c.want)
		}
	}

	// Pipelined requests are all answered, each by its cookie.
	for i := range uint64(100) {
		cl.request(cmdWrite, 0, 1000+i, int64(i)*4096, 4096, bytes.Repeat([]byte{byte(i)}, 4096))
	}
	seen := map[uint64]bool{}
	for range 100 {
		cookie, e := cl.simple()
		if e != 0 || cookie < 1000 || cookie >= 1100 || seen[cookie] {
			t.Fatalf("pipelined write: cookie %d, error %v", cookie, e)
		}
		seen[cookie] = true
	}
	if got := disk.bytes()[99*4096]; got != 99 {
		t.Errorf("block 99 holds %d after the pipelined writes", got)
	}

	ro := dial(t, sock, flagFixedNewstyle|flagNoZeroes)
	ro.goTo("ro")
	for _, typ := range []uint16{cmdWrite, cmdTrim, cmdWriteZeroes} {
		var data []byte
		if typ == cmdWrite {
			data = make([]byte, 4096)
		}
		ro.request(typ, 0, 7, 0, 4096, data)
		if _, e := ro.simple(); e != EPERM {
			t.Errorf("command %d on a read-only export: %v, want EPERM", typ, e)
		}
	}
	ro.request(99, 0, 8, 0, 4096, nil)
	if _, e := ro.simple(); e != EINVAL {
		t.Errorf("unknown command: %v", e)
	}
	ro.write(bytes.Repeat([]byte{1}, 28))
	if !ro.closed() {
		t.Error("bad request magic was taken")
	}
}
