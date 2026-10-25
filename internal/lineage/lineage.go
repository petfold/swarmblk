// Package lineage reads and writes lineage feeds (SPEC §7). A lineage is a
// Bee sequence feed owned by the lineage key, which exactly one host holds;
// update n names the manifest of the lineage's snapshot n.
package lineage

import (
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/feeds"
	"github.com/ethersphere/bee/v2/pkg/soc"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"golang.org/x/crypto/sha3"

	"github.com/petfold/swarmblk/internal/snapshot"
)

// Lineage identifies a lineage feed.
type Lineage struct {
	Owner common.Address // the lineage key's Ethereum address
	Topic [32]byte
}

// New returns owner's lineage with the given lineage ID, whose topic is
// keccak256(utf8("swarmblk/lineage/") ‖ id).
func New(owner common.Address, id [16]byte) Lineage {
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte("swarmblk/lineage/"))
	h.Write(id[:])
	l := Lineage{Owner: owner}
	h.Sum(l.Topic[:0])
	return l
}

// String returns l as 0x<owner>/<topic>, in lowercase hex.
func (l Lineage) String() string {
	return fmt.Sprintf("0x%x/%x", l.Owner[:], l.Topic[:])
}

// Parse reads a lineage written by String.
func Parse(s string) (Lineage, error) {
	owner, topic, ok := strings.Cut(s, "/")
	if !ok {
		return Lineage{}, fmt.Errorf("lineage %q: want 0x<owner>/<topic>", s)
	}
	return parse(owner, topic)
}

// Meta returns the place of update index in l as meta.json records it
// (SPEC §5).
func (l Lineage) Meta(index uint64) *snapshot.Lineage {
	return &snapshot.Lineage{Owner: fmt.Sprintf("0x%x", l.Owner[:]), Topic: hex.EncodeToString(l.Topic[:]), Index: index}
}

// FromMeta returns the lineage and index that a meta.json records.
func FromMeta(m snapshot.Lineage) (Lineage, uint64, error) {
	l, err := parse(m.Owner, m.Topic)
	return l, m.Index, err
}

func parse(owner, topic string) (Lineage, error) {
	var l Lineage
	o, err := hex.DecodeString(strings.TrimPrefix(owner, "0x"))
	if err != nil || len(o) != len(l.Owner) || !strings.HasPrefix(owner, "0x") {
		return Lineage{}, fmt.Errorf("lineage owner %q: want 0x and 40 hex digits", owner)
	}
	t, err := hex.DecodeString(topic)
	if err != nil || len(t) != len(l.Topic) {
		return Lineage{}, fmt.Errorf("lineage topic %q: want 64 hex digits", topic)
	}
	copy(l.Owner[:], o)
	copy(l.Topic[:], t)
	return l, nil
}

// Update is an update of a lineage feed.
type Update struct {
	Index    uint64
	Snapshot swarm.Address // the manifest of the lineage's snapshot Index
	Time     time.Time     // the snapshot's creation time, in whole seconds
}

// PayloadSize is the size of an update's payload: its time as an 8-byte
// big-endian count of Unix seconds, then the snapshot's manifest reference.
// This is Bee's legacy feed payload, which GET /feeds and /bzz resolve to
// the manifest (SPEC §7).
const PayloadSize = 8 + swarm.HashSize

// index is a sequence feed index, which Bee's pkg/feeds/sequence encodes as
// 8 bytes, big-endian, but does not export.
type index uint64

func (i index) MarshalBinary() ([]byte, error) {
	return binary.BigEndian.AppendUint64(nil, uint64(i)), nil
}

func (i index) Next(int64, uint64) feeds.Index { return i + 1 }

func (i index) String() string { return strconv.FormatUint(uint64(i), 10) }

// Address returns the address of the single-owner chunk that holds update i
// of l.
func (l Lineage) Address(i uint64) (swarm.Address, error) {
	return feeds.New(l.Topic[:], l.Owner).Update(index(i)).Address()
}

// Sign returns u as a single-owner chunk of l, signed with key, which must
// be l's.
func (l Lineage) Sign(key *ecdsa.PrivateKey, u Update) (swarm.Chunk, error) {
	signer := crypto.NewDefaultSigner(key)
	owner, err := signer.EthereumAddress()
	if err != nil {
		return nil, err
	}
	switch {
	case owner != l.Owner:
		return nil, fmt.Errorf("lineage %s: the key belongs to 0x%x", l, owner[:])
	case len(u.Snapshot.Bytes()) != swarm.HashSize:
		return nil, fmt.Errorf("lineage %s: update %d names no snapshot", l, u.Index)
	case u.Time.Unix() < 0:
		return nil, fmt.Errorf("lineage %s: update %d has time %v", l, u.Index, u.Time)
	}
	payload := binary.BigEndian.AppendUint64(make([]byte, 0, PayloadSize), uint64(u.Time.Unix()))
	payload = append(payload, u.Snapshot.Bytes()...)
	var ch swarm.Chunk
	p, err := feeds.NewPutter(storage.PutterFunc(func(_ context.Context, c swarm.Chunk) error {
		ch = c
		return nil
	}), signer, l.Topic[:])
	if err != nil {
		return nil, err
	}
	if err := p.Put(context.Background(), index(u.Index), payload); err != nil {
		return nil, err
	}
	return ch, nil
}

// Open checks that ch is update i of l, signed by l's owner, and returns it.
func (l Lineage) Open(i uint64, ch swarm.Chunk) (Update, error) {
	addr, err := l.Address(i)
	if err != nil {
		return Update{}, err
	}
	if !ch.Address().Equal(addr) {
		return Update{}, fmt.Errorf("lineage %s: update %d is at %s, not %s", l, i, addr, ch.Address())
	}
	// The address hashes the update's ID with the owner that the signature
	// recovers, so a valid chunk there was signed by l's owner for index i.
	s, err := soc.FromChunkValidate(ch)
	if err != nil {
		return Update{}, fmt.Errorf("lineage %s: update %d: %w", l, i, err)
	}
	data := s.WrappedChunk().Data()
	if len(data) != swarm.SpanSize+PayloadSize || binary.LittleEndian.Uint64(data) != PayloadSize {
		return Update{}, fmt.Errorf("lineage %s: update %d: %w", l, i, errPayload)
	}
	p := data[swarm.SpanSize:]
	return Update{
		Index:    i,
		Snapshot: swarm.NewAddress(slices.Clone(p[8:])),
		Time:     time.Unix(int64(binary.BigEndian.Uint64(p)), 0).UTC(),
	}, nil
}

var errPayload = errors.New("the payload is not a time and a manifest reference")
