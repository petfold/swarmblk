package card

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/snapshot"
)

// TestBlob encrypts blobs of several sizes into a store and reads them
// back: no chunk may hold the plain bytes, and a reference without its key
// must not read.
func TestBlob(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{0, 100, swarm.ChunkSize, 540672} {
		s := inmemchunkstore.New()
		data := bytes.Repeat([]byte("swarmblk uefi variables "), n/24+1)[:n]
		ref, err := Put(ctx, s, data)
		if err != nil {
			t.Fatal(err)
		}
		if len(ref.Bytes()) != 64 {
			t.Fatalf("%d bytes: a reference of %d bytes", n, len(ref.Bytes()))
		}
		got, err := Get(ctx, s, ref, int64(n))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("%d bytes: read back %d bytes: %v", n, len(got), err)
		}
		if n > 0 {
			if _, err := Get(ctx, s, ref, int64(n-1)); err == nil {
				t.Errorf("%d bytes: a blob over the limit was read", n)
			}
		}
		s.Iterate(ctx, func(ch swarm.Chunk) (bool, error) {
			if n >= 24 && bytes.Contains(ch.Data(), []byte("swarmblk uefi")) {
				t.Errorf("%d bytes: chunk %s holds plain text", n, ch.Address())
			}
			return false, nil
		})
		if _, err := Get(ctx, s, swarm.NewAddress(ref.Bytes()[:32]), int64(n)); err == nil {
			t.Errorf("%d bytes: a reference without its key was read", n)
		}
	}
}

func testCard() Card {
	ref := func(n int) *swarm.Address {
		b := make([]byte, n)
		rand.Read(b)
		a := swarm.NewAddress(b)
		return &a
	}
	return Card{
		Format:   Format,
		Name:     "web-01",
		Created:  time.Date(2026, 10, 26, 20, 0, 0, 0, time.UTC),
		Lineage:  &snapshot.Lineage{Owner: "0x" + strings.Repeat("ab", 20), Topic: strings.Repeat("cd", 32), Index: 3},
		Snapshot: *ref(32),
		Batches:  []string{strings.Repeat("ef", 32)},
		VM:       Blobs{Definition: *ref(64), NVRAM: ref(64)},
	}
}

// TestCard writes a card, reads it back, and checks what Check refuses.
func TestCard(t *testing.T) {
	c := testCard()
	path := filepath.Join(t.TempDir(), "web-01.card")
	if err := c.Write(path); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("the card file: %v, mode %v", err, fi.Mode())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"vtpm": null`)) || !bytes.Contains(b, []byte(`"format": "swarmblk-card/1"`)) {
		t.Errorf("the card:\n%s", b)
	}
	got, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, c) {
		t.Errorf("read back\n%+v\nwrote\n%+v", got, c)
	}

	for name, change := range map[string]func(*Card){
		"format":        func(c *Card) { c.Format = "swarmblk-card/2" },
		"name":          func(c *Card) { c.Name = "" },
		"snapshot":      func(c *Card) { c.Snapshot = swarm.ZeroAddress },
		"lineage":       func(c *Card) { c.Lineage.Owner = "ab" },
		"definition":    func(c *Card) { c.VM.Definition = c.Snapshot },
		"no definition": func(c *Card) { c.VM.Definition = swarm.ZeroAddress },
		"nvram":         func(c *Card) { c.VM.NVRAM = &c.Snapshot },
	} {
		bad := testCard()
		change(&bad)
		if err := bad.Check(); err == nil {
			t.Errorf("Check took a card with a bad %s", name)
		}
	}
}

// TestParseVM fills in defaults and refuses what format 1 does not allow.
func TestParseVM(t *testing.T) {
	v, err := ParseVM([]byte(`{"memory": 512, "nic": {"model": "virtio-net-pci", "mac": "52:54:00:12:34:56"}, "future": 1}`))
	if err != nil {
		t.Fatal(err)
	}
	want := VM{Format: VMFormat, Arch: "x86_64", Machine: "q35", CPUs: 1, Memory: 512, Firmware: "bios", Disk: "virtio", NIC: &NIC{Model: "virtio-net-pci", MAC: "52:54:00:12:34:56"}}
	if !reflect.DeepEqual(v, want) {
		t.Errorf("got %+v, want %+v", v, want)
	}
	for _, bad := range []string{
		`{"format": "swarmblk-vm/2"}`,
		`{"arch": "x86-64; rm"}`,
		`{"firmware": "coreboot"}`,
		`{"secure_boot": true}`,
		`{"disk": "nvme"}`,
		`{"nic": {"mac": "52:54:00:12:34:56"}}`,
		`{"nic": {"model": "e1000", "mac": "52:54:00:12:34"}}`,
		`{"memory": -1}`,
		`[]`,
	} {
		if _, err := ParseVM([]byte(bad)); err == nil {
			t.Errorf("ParseVM took %s", bad)
		}
	}
}

// TestPackDir packs a TPM state directory and unpacks it elsewhere.
func TestPackDir(t *testing.T) {
	dir := t.TempDir()
	files := map[string][]byte{"tpm2-00.permall": bytes.Repeat([]byte{7}, 9000), "tpm2-00.volatilestate": {1, 2, 3}}
	for name, b := range files {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(dir, ".lock"), nil, 0o600)
	os.Mkdir(filepath.Join(dir, "sub"), 0o700)
	b, err := PackDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "tpm")
	if err := UnpackDir(b, out); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(out)
	if len(entries) != len(files) {
		t.Errorf("unpacked %d entries, want %d", len(entries), len(files))
	}
	for name, want := range files {
		if got, err := os.ReadFile(filepath.Join(out, name)); err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
