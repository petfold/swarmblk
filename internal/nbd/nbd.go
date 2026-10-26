// Package nbd serves block devices to NBD clients such as QEMU and the Linux
// kernel (DESIGN §8). It implements the fixed newstyle handshake and the
// part of the protocol the engine needs: structured replies, block status
// in the base:allocation context, flush, FUA, trim and write-zeroes. It
// offers no TLS, so it is meant for local sockets.
//
// The protocol is described at
// https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md.
package nbd

import (
	"context"
	"errors"
	"syscall"
)

// Device is what an export serves. The server checks every offset and length
// against the device's size, and never calls a read-only export's writing
// methods. Methods may be called concurrently.
type Device interface {
	// Size returns the device's size in bytes.
	Size() int64
	// ReadAt fills p with the bytes from offset off.
	ReadAt(ctx context.Context, p []byte, off int64) error
	// WriteAt writes p at offset off. With fua, it returns once p is
	// durable.
	WriteAt(ctx context.Context, p []byte, off int64, fua bool) error
	// Flush returns once every write that has returned is durable.
	Flush(ctx context.Context) error
	// Trim discards length bytes from off. With fua, it returns once that
	// is durable.
	Trim(ctx context.Context, off, length int64, fua bool) error
	// WriteZeroes makes length bytes from off read as zeros: as a hole
	// unless noHole is set. With fua, it returns once that is durable.
	WriteZeroes(ctx context.Context, off, length int64, fua, noHole bool) error
	// Extents describes the bytes from off on as runs, in order. They may
	// cover less than length, but not nothing, and may not cover more.
	Extents(ctx context.Context, off, length int64) ([]Extent, error)
}

// Extent is a run of a device's bytes in the base:allocation context.
type Extent struct {
	Length int64
	Hole   bool // not allocated
	Zero   bool // reads as zeros
}

// Errno is an error that NBD clients know. A device returns one to choose
// what its client sees; any other error reaches the client as EIO.
type Errno uint32

// The errors that the protocol defines, with Linux's numbers.
const (
	EPERM     Errno = 1
	EIO       Errno = 5
	ENOMEM    Errno = 12
	EINVAL    Errno = 22
	ENOSPC    Errno = 28
	EOVERFLOW Errno = 75
	ENOTSUP   Errno = 95
	ESHUTDOWN Errno = 108
)

func (e Errno) Error() string { return "nbd: " + syscall.Errno(e).Error() }

// errno returns the error that a client sees for err.
func errno(err error) Errno {
	var e Errno
	if errors.As(err, &e) {
		return e
	}
	return EIO
}

// Block sizes that exports advertise (SPEC §4): 512-byte logical and
// 4096-byte physical blocks, and at most MaxPayload bytes in one request.
const (
	MinBlock   = 512
	Preferred  = 4096
	MaxPayload = 32 << 20
)

// Protocol constants.
const (
	nbdMagic             = 0x4e42444d41474943 // "NBDMAGIC"
	optMagic             = 0x49484156454f5054 // "IHAVEOPT"
	repMagic             = 0x0003e889045565a9
	requestMagic         = 0x25609513
	simpleReplyMagic     = 0x67446698
	structuredReplyMagic = 0x668e33ef

	flagFixedNewstyle = 1 << 0 // handshake flags, and the client's
	flagNoZeroes      = 1 << 1

	optExportName      = 1
	optAbort           = 2
	optList            = 3
	optStartTLS        = 5
	optInfo            = 6
	optGo              = 7
	optStructuredReply = 8
	optListMetaContext = 9
	optSetMetaContext  = 10

	repAck         = 1
	repServer      = 2
	repInfo        = 3
	repMetaContext = 4
	repErrUnsup    = 1<<31 | 1
	repErrInvalid  = 1<<31 | 3
	repErrUnknown  = 1<<31 | 6

	infoExport      = 0
	infoName        = 1
	infoDescription = 2
	infoBlockSize   = 3

	flagHasFlags        = 1 << 0 // transmission flags
	flagReadOnly        = 1 << 1
	flagSendFlush       = 1 << 2
	flagSendFUA         = 1 << 3
	flagSendTrim        = 1 << 5
	flagSendWriteZeroes = 1 << 6

	cmdRead        = 0
	cmdWrite       = 1
	cmdDisc        = 2
	cmdFlush       = 3
	cmdTrim        = 4
	cmdWriteZeroes = 6
	cmdBlockStatus = 7

	cmdFlagFUA    = 1 << 0
	cmdFlagNoHole = 1 << 1
	cmdFlagReqOne = 1 << 3

	replyFlagDone        = 1 << 0
	replyTypeNone        = 0
	replyTypeOffsetData  = 1
	replyTypeBlockStatus = 5
	replyTypeError       = 1<<15 | 1

	stateHole = 1 << 0 // base:allocation
	stateZero = 1 << 1

	// metaAllocation is base:allocation's context ID, the only context.
	metaAllocation = 1
	baseAllocation = "base:allocation"
)
