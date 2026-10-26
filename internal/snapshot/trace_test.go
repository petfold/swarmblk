package snapshot

import (
	"context"
	"reflect"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"

	"github.com/petfold/swarmblk/internal/tree"
)

func TestTrace(t *testing.T) {
	runs := []tree.Range{{Start: 100, Count: 16}, {Start: 0, Count: 1}, {Start: 1 << 30, Count: 1 << 20}}
	b := EncodeTrace(runs)
	if string(b[:8]) != "SBPF\x01\x00\x00\x00" || len(b) != 8+3*12 {
		t.Fatalf("prefetch.bin starts %q and has %d bytes", b[:8], len(b))
	}
	if got, err := DecodeTrace(b); err != nil || !reflect.DeepEqual(got, runs) {
		t.Errorf("DecodeTrace = %v, %v; want %v", got, err, runs)
	}
	if got, err := DecodeTrace(EncodeTrace(nil)); err != nil || len(got) != 0 {
		t.Errorf("an empty trace: %v, %v", got, err)
	}
	for name, bad := range map[string][]byte{
		"short":        b[:7],
		"magic":        append([]byte("SBPX"), b[4:]...),
		"version":      append([]byte("SBPF\x02\x00\x00\x00"), b[8:]...),
		"reserved":     append([]byte("SBPF\x01\x00\x01\x00"), b[8:]...),
		"part record":  b[:len(b)-1],
		"empty run":    EncodeTrace([]tree.Range{{Start: 5, Count: 0}}),
		"beyond 4 PiB": EncodeTrace([]tree.Range{{Start: 1<<40 + 1, Count: 1}}),
	} {
		if _, err := DecodeTrace(bad); err == nil {
			t.Errorf("%s: DecodeTrace accepted %x", name, bad)
		}
	}

	ctx := context.Background()
	s := inmemchunkstore.New()
	ref, err := Write(ctx, s, s, testMeta(), b)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := Read(ctx, s, ref)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ReadTrace(ctx, s, snap); err != nil || !reflect.DeepEqual(got, runs) {
		t.Errorf("ReadTrace = %v, %v", got, err)
	}
	ref, _ = Write(ctx, s, s, testMeta(), nil)
	snap, _ = Read(ctx, s, ref)
	if got, err := ReadTrace(ctx, s, snap); err != nil || got != nil {
		t.Errorf("a snapshot without a trace: %v, %v", got, err)
	}
}
