package lineage

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/feeds"
	"github.com/ethersphere/bee/v2/pkg/feeds/sequence"
	"github.com/ethersphere/bee/v2/pkg/soc"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// testLineage returns a lineage with a new key, kept in a temporary directory.
func testLineage(t *testing.T) (Lineage, *ecdsa.PrivateKey) {
	t.Helper()
	l, key, err := Create(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return l, key
}

// testUpdate returns update i, naming the snapshot 0xii…ii.
func testUpdate(i uint64) Update {
	return Update{
		Index:    i,
		Snapshot: swarm.NewAddress(bytes.Repeat([]byte{byte(i + 1)}, swarm.HashSize)),
		Time:     time.Date(2026, 10, 26, 1, 2, int(i), 0, time.UTC),
	}
}

// same reports whether a and b are the same update.
func same(a, b Update) bool {
	return a.Index == b.Index && a.Snapshot.Equal(b.Snapshot) && a.Time.Equal(b.Time)
}

func sign(t *testing.T, l Lineage, key *ecdsa.PrivateKey, u Update) swarm.Chunk {
	t.Helper()
	ch, err := l.Sign(key, u)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func TestTopic(t *testing.T) {
	owner := common.HexToAddress("0x" + strings.Repeat("12", 20))
	id := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	want, err := crypto.LegacyKeccak256(append([]byte("swarmblk/lineage/"), id[:]...))
	if err != nil {
		t.Fatal(err)
	}
	if l := New(owner, id); l.Owner != owner || !bytes.Equal(l.Topic[:], want) {
		t.Errorf("New gave topic %x, want %x", l.Topic, want)
	}
	if a, b := New(owner, id), New(owner, [16]byte{1}); a.Topic == b.Topic {
		t.Error("two lineage IDs gave one topic")
	}
}

// TestSignMatchesBee signs updates with Bee's own sequence feed updater and
// requires the same chunks, byte for byte: same index encoding, ID, wrapped
// payload and signature.
func TestSignMatchesBee(t *testing.T) {
	l, key := testLineage(t)
	var bees []swarm.Chunk
	up, err := sequence.NewUpdater(storage.PutterFunc(func(_ context.Context, ch swarm.Chunk) error {
		bees = append(bees, ch)
		return nil
	}), crypto.NewDefaultSigner(key), l.Topic[:])
	if err != nil {
		t.Fatal(err)
	}
	for i := range uint64(3) {
		u := testUpdate(i)
		payload := binary.BigEndian.AppendUint64(nil, uint64(u.Time.Unix())) // SPEC §7: seconds, then the reference
		if err := up.Update(context.Background(), u.Time.Unix(), append(payload, u.Snapshot.Bytes()...)); err != nil {
			t.Fatal(err)
		}
		ch := sign(t, l, key, u)
		if !ch.Address().Equal(bees[i].Address()) || !bytes.Equal(ch.Data(), bees[i].Data()) {
			t.Fatalf("update %d: %s %x, Bee's updater made %s %x", i, ch.Address(), ch.Data(), bees[i].Address(), bees[i].Data())
		}
		if addr, err := l.Address(i); err != nil || !addr.Equal(ch.Address()) {
			t.Errorf("Address(%d) = %s, %v; the update is at %s", i, addr, err, ch.Address())
		}
	}
}

// TestBeeReadsUpdates stores updates and finds the head with Bee's sequence
// finders, then resolves it as Bee's GET /feeds and /bzz do for a legacy
// payload: to the chunk at the snapshot's reference.
func TestBeeReadsUpdates(t *testing.T) {
	ctx := context.Background()
	l, key := testLineage(t)
	s := inmemchunkstore.New()
	manifest, err := cac.New([]byte("a snapshot manifest's root chunk"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	var last Update
	for i := range uint64(5) {
		last = testUpdate(i)
		last.Snapshot = manifest.Address()
		if err := s.Put(ctx, sign(t, l, key, last)); err != nil {
			t.Fatal(err)
		}
	}
	feed := feeds.New(l.Topic[:], l.Owner)
	for name, finder := range map[string]feeds.Lookup{"finder": sequence.NewFinder(s, feed), "async finder": sequence.NewAsyncFinder(s, feed)} {
		ch, cur, _, err := finder.At(ctx, time.Now().Unix(), 0)
		if err != nil || ch == nil {
			t.Fatalf("%s: %v, %v", name, ch, err)
		}
		if cur.String() != "4" {
			t.Errorf("%s: head at index %s, want 4", name, cur)
		}
		if u, err := l.Open(4, ch); err != nil || !same(u, last) {
			t.Errorf("%s: head %+v, %v; want %+v", name, u, err, last)
		}
		wc, err := feeds.GetWrappedChunk(ctx, s, ch, true)
		if err != nil || !wc.Address().Equal(manifest.Address()) {
			t.Errorf("%s: Bee resolves the head to %v, %v; want the manifest %s", name, wc, err, manifest.Address())
		}
	}
}

func TestOpenRejects(t *testing.T) {
	l, key := testLineage(t)
	u := testUpdate(0)
	good := sign(t, l, key, u)
	if got, err := l.Open(0, good); err != nil || !same(got, u) {
		t.Fatalf("Open = %+v, %v; want %+v", got, err, u)
	}

	other, otherKey := testLineage(t)
	other.Topic = l.Topic // another owner writing to the same topic
	flipped := slices.Clone(good.Data())
	flipped[len(flipped)-1] ^= 1
	id, err := feeds.Id(l.Topic[:], index(0))
	if err != nil {
		t.Fatal(err)
	}
	foreign := func(data []byte) swarm.Chunk {
		wc, err := cac.NewWithDataSpan(data)
		if err != nil {
			t.Fatal(err)
		}
		ch, err := soc.New(id, wc).Sign(crypto.NewDefaultSigner(key))
		if err != nil {
			t.Fatal(err)
		}
		return ch
	}
	payload := good.Data()[len(good.Data())-PayloadSize:]
	plain, err := cac.New(payload)
	if err != nil {
		t.Fatal(err)
	}
	short := binary.LittleEndian.AppendUint64(nil, 10)
	long := binary.LittleEndian.AppendUint64(nil, swarm.ChunkSize) // the payload, under a span that claims a full chunk
	for name, c := range map[string]struct {
		index uint64
		ch    swarm.Chunk
	}{
		"another index":     {1, good},
		"another owner":     {0, sign(t, other, otherKey, u)},
		"tampered":          {0, swarm.NewChunk(good.Address(), flipped)},
		"a short payload":   {0, foreign(append(short, "swarmblk:)"...))},
		"a span over it":    {0, foreign(append(long, payload...))},
		"content-addressed": {0, swarm.NewChunk(good.Address(), plain.Data())},
		"empty":             {0, swarm.NewChunk(good.Address(), nil)},
	} {
		if got, err := l.Open(c.index, c.ch); err == nil {
			t.Errorf("%s: Open accepted it as %+v", name, got)
		}
	}
	if _, err := other.Sign(key, u); err == nil {
		t.Error("Sign used a key that is not the lineage's")
	}
	if _, err := l.Sign(key, Update{Time: u.Time}); err == nil {
		t.Error("Sign accepted an update without a snapshot")
	}
}

func TestStringAndMeta(t *testing.T) {
	l, _ := testLineage(t)
	if got, err := Parse(l.String()); err != nil || got != l {
		t.Errorf("Parse(%q) = %v, %v", l.String(), got, err)
	}
	m := l.Meta(7)
	if !strings.HasPrefix(m.Owner, "0x") || len(m.Owner) != 42 || len(m.Topic) != 64 || m.Owner != strings.ToLower(m.Owner) || m.Index != 7 {
		t.Errorf("Meta(7) = %+v", m)
	}
	if got, i, err := FromMeta(*m); err != nil || got != l || i != 7 {
		t.Errorf("FromMeta = %v, %d, %v", got, i, err)
	}
	owner, topic, _ := strings.Cut(l.String(), "/")
	for _, bad := range []string{owner, owner[2:] + "/" + topic, owner[:41] + "/" + topic, owner + "/" + topic[1:], owner + "/" + topic[:62] + "zz", owner + "/" + topic + "/"} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("Parse accepted %q", bad)
		}
	}
}

func TestKeys(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "keys")
	key, owner, err := NewKey(dir)
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
		t.Errorf("key directory: %v, %v; want mode 0700", fi.Mode(), err)
	}
	path := keyPath(dir, owner)
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 || filepath.Base(path) != strings.ToLower(owner.Hex()[2:])+".key" {
		t.Errorf("key file %s: %v, %v; want mode 0600", path, fi.Mode(), err)
	}
	got, err := LoadKey(dir, owner)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(key) {
		t.Error("LoadKey read back another key")
	}

	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKey(dir, owner); err == nil || !strings.Contains(err.Error(), "chmod 600") {
		t.Errorf("a key file of mode 0640: %v", err)
	}
	other := common.HexToAddress("0x" + strings.Repeat("34", 20))
	if err := os.Rename(path, keyPath(dir, other)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(keyPath(dir, other), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKey(dir, other); err == nil || !strings.Contains(err.Error(), "holds the key of") {
		t.Errorf("another owner's key: %v", err)
	}
	if _, err := LoadKey(dir, owner); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a missing key: %v", err)
	}
	if err := os.WriteFile(keyPath(dir, owner), []byte("sEcReT not hex\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKey(dir, owner); err == nil || strings.Contains(err.Error(), "sEcReT") || strings.Contains(err.Error(), "U+") {
		t.Errorf("a file of junk: %v; the error must not quote the file", err)
	}

	a, _ := testLineage(t)
	b, _ := testLineage(t)
	if a.Owner == b.Owner || a.Topic == b.Topic {
		t.Error("two new lineages share an owner or a topic")
	}
}
