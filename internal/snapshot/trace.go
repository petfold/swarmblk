package snapshot

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/ethersphere/bee/v2/pkg/file/joiner"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/storage"

	"github.com/petfold/swarmblk/internal/tree"
)

// A boot trace (prefetch.bin, SPEC §6) lists the blocks that a boot read,
// as runs of consecutive 4 KiB blocks in first-touch order.
const (
	traceMagic   = "SBPF"
	traceVersion = 1
	traceHeader  = 8
	traceRecord  = 12
	maxTrace     = 64 << 20 // a 1 TiB disk read whole would need 3 GiB; boots read far less
)

// EncodeTrace returns prefetch.bin for the runs, which must be in
// first-touch order without repeating a block.
func EncodeTrace(runs []tree.Range) []byte {
	b := make([]byte, 0, traceHeader+traceRecord*len(runs))
	b = append(b, traceMagic...)
	b = append(b, traceVersion, 0, 0, 0)
	for _, r := range runs {
		b = binary.LittleEndian.AppendUint64(b, uint64(r.Start))
		b = binary.LittleEndian.AppendUint32(b, uint32(r.Count))
	}
	return b
}

// DecodeTrace parses prefetch.bin. It checks the format, not the blocks
// against a disk: the engine skips those outside the disk or seen before.
func DecodeTrace(b []byte) ([]tree.Range, error) {
	switch {
	case len(b) < traceHeader || string(b[:4]) != traceMagic:
		return nil, errors.New("prefetch.bin: not a boot trace")
	case b[4] != traceVersion || !bytes.Equal(b[5:8], []byte{0, 0, 0}):
		return nil, fmt.Errorf("prefetch.bin: version %d, want %d", b[4], traceVersion)
	case (len(b)-traceHeader)%traceRecord != 0:
		return nil, fmt.Errorf("prefetch.bin: %d bytes of records, not whole records", len(b)-traceHeader)
	}
	runs := make([]tree.Range, 0, (len(b)-traceHeader)/traceRecord)
	for p := b[traceHeader:]; len(p) > 0; p = p[traceRecord:] {
		start, count := binary.LittleEndian.Uint64(p), binary.LittleEndian.Uint32(p[8:])
		if count == 0 || start > 1<<40 {
			return nil, fmt.Errorf("prefetch.bin: a run of %d blocks from block %d", count, start)
		}
		runs = append(runs, tree.Range{Start: int64(start), Count: int64(count)})
	}
	return runs, nil
}

// ReadTrace returns the boot trace of s, or nil if it has none.
func ReadTrace(ctx context.Context, get storage.Getter, s Snapshot) ([]tree.Range, error) {
	if s.Prefetch.IsZero() {
		return nil, nil
	}
	j, size, err := joiner.New(ctx, get, discard{}, s.Prefetch, redundancy.NONE)
	if err != nil {
		return nil, fmt.Errorf("snapshot %s: %s: %w", s.Ref, PrefetchPath, err)
	}
	if size > maxTrace {
		return nil, fmt.Errorf("snapshot %s: %s has %d bytes", s.Ref, PrefetchPath, size)
	}
	b, err := io.ReadAll(j)
	if err != nil {
		return nil, fmt.Errorf("snapshot %s: %s: %w", s.Ref, PrefetchPath, err)
	}
	return DecodeTrace(b)
}
