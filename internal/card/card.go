// Package card writes and reads disk cards (SPEC §12): JSON documents that
// restore a machine from the bare network. A card names the disk's lineage
// and snapshot, and the machine's definition, UEFI variables and TPM state,
// which it keeps on Swarm under Bee's chunk encryption. Their references
// carry the keys, so a card is secret.
package card

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/ethersphere/bee/v2/pkg/encryption"
	"github.com/ethersphere/bee/v2/pkg/file/joiner"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/lineage"
	"github.com/petfold/swarmblk/internal/snapshot"
)

// Formats this package writes and reads.
const (
	Format   = "swarmblk-card/1"
	VMFormat = "swarmblk-vm/1"
)

// Card is a disk card, format 1.
type Card struct {
	Format   string            `json:"format"`
	Name     string            `json:"name"`     // the machine, and its disk's name
	Created  time.Time         `json:"created"`  // UTC
	Lineage  *snapshot.Lineage `json:"lineage"`  // the lineage the disk follows, and Snapshot's update in it; nil to stay on Snapshot
	Snapshot swarm.Address     `json:"snapshot"` // the disk's snapshot when the card was made
	Batches  []string          `json:"batches"`  // every batch known to stamp chunks the card reaches
	VM       Blobs             `json:"vm"`
}

// Blobs are a machine's encrypted blobs on Swarm. Each reference is 64
// bytes, the root chunk's address and then its key.
type Blobs struct {
	Definition swarm.Address  `json:"definition"` // the VM definition, JSON
	NVRAM      *swarm.Address `json:"nvram"`      // the UEFI variables, or nil
	VTPM       *swarm.Address `json:"vtpm"`       // the TPM's state, a tar archive of swtpm's files, or nil
}

// Check reports what in c format 1 does not allow.
func (c Card) Check() error {
	switch {
	case c.Format != Format:
		return fmt.Errorf("format %q, want %q", c.Format, Format)
	case c.Name == "":
		return errors.New("no name")
	case len(c.Snapshot.Bytes()) != swarm.HashSize:
		return errors.New("no snapshot")
	}
	if c.Lineage != nil {
		if _, _, err := lineage.FromMeta(*c.Lineage); err != nil {
			return err
		}
	}
	for _, b := range []struct {
		name string
		ref  *swarm.Address
	}{{"definition", &c.VM.Definition}, {"nvram", c.VM.NVRAM}, {"vtpm", c.VM.VTPM}} {
		if b.ref != nil && len(b.ref.Bytes()) != encryption.ReferenceSize {
			return fmt.Errorf("vm %s: want an encrypted reference, 128 hex digits", b.name)
		}
	}
	return nil
}

// Parse reads a card and checks it. Unknown fields are ignored.
func Parse(b []byte) (Card, error) {
	var c Card
	if err := json.Unmarshal(b, &c); err != nil {
		return Card{}, fmt.Errorf("card: %w", err)
	}
	if err := c.Check(); err != nil {
		return Card{}, fmt.Errorf("card: %w", err)
	}
	return c, nil
}

// Write writes c to path, private to its user, replacing what was there
// atomically.
func (c Card) Write(path string) error {
	if err := c.Check(); err != nil {
		return fmt.Errorf("card: %w", err)
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".card-*")
	if err != nil {
		return err
	}
	_, err = f.Write(append(b, '\n'))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		os.Remove(f.Name())
	}
	return err
}

// VM is a VM definition, format 1: what boot needs to run the machine with
// QEMU.
type VM struct {
	Format     string   `json:"format"`
	Arch       string   `json:"arch"`        // QEMU's target, which runs as qemu-system-<arch>
	Machine    string   `json:"machine"`     // QEMU's machine type
	CPU        string   `json:"cpu"`         // QEMU's CPU model; empty for host with KVM, max without
	CPUs       int      `json:"cpus"`        // virtual CPUs
	Memory     int      `json:"memory"`      // MiB
	Firmware   string   `json:"firmware"`    // bios or uefi
	SecureBoot bool     `json:"secure_boot"` // UEFI with Secure Boot
	TPM        string   `json:"tpm"`         // QEMU's TPM device, such as tpm-crb, or empty for none
	Disk       string   `json:"disk"`        // the disk's bus: virtio, scsi or sata
	NIC        *NIC     `json:"nic"`         // nil for none
	Agent      bool     `json:"agent"`       // a channel for the QEMU guest agent
	Args       []string `json:"args"`        // more arguments for QEMU
}

// NIC is a VM's network card.
type NIC struct {
	Model string `json:"model"` // QEMU's NIC model, such as virtio-net-pci or e1000
	MAC   string `json:"mac"`   // empty for QEMU's choice
}

var (
	archName = regexp.MustCompile(`^[a-z0-9_]+$`)
	macAddr  = regexp.MustCompile(`^[0-9a-fA-F]{2}(:[0-9a-fA-F]{2}){5}$`)
)

// ParseVM reads a VM definition, fills in the defaults of what it leaves
// out, and checks it. Unknown fields are ignored. The defaults are an
// x86_64 q35 machine with one CPU, 1024 MiB and BIOS firmware, and a
// virtio disk.
func ParseVM(b []byte) (VM, error) {
	var v VM
	if err := json.Unmarshal(b, &v); err != nil {
		return VM{}, fmt.Errorf("vm definition: %w", err)
	}
	def := func(s *string, d string) {
		if *s == "" {
			*s = d
		}
	}
	def(&v.Format, VMFormat)
	def(&v.Arch, "x86_64")
	def(&v.Machine, "q35")
	def(&v.Firmware, "bios")
	def(&v.Disk, "virtio")
	if v.CPUs == 0 {
		v.CPUs = 1
	}
	if v.Memory == 0 {
		v.Memory = 1024
	}
	if err := v.check(); err != nil {
		return VM{}, fmt.Errorf("vm definition: %w", err)
	}
	return v, nil
}

func (v VM) check() error {
	switch {
	case v.Format != VMFormat:
		return fmt.Errorf("format %q, want %q", v.Format, VMFormat)
	case !archName.MatchString(v.Arch):
		return fmt.Errorf("arch %q", v.Arch)
	case v.CPUs < 1 || v.Memory < 16:
		return fmt.Errorf("%d CPUs and %d MiB", v.CPUs, v.Memory)
	case v.Firmware != "bios" && v.Firmware != "uefi":
		return fmt.Errorf("firmware %q: use bios or uefi", v.Firmware)
	case v.SecureBoot && v.Firmware != "uefi":
		return errors.New("Secure Boot needs UEFI firmware")
	case v.Disk != "virtio" && v.Disk != "scsi" && v.Disk != "sata":
		return fmt.Errorf("disk bus %q: use virtio, scsi or sata", v.Disk)
	case v.NIC != nil && v.NIC.Model == "":
		return errors.New("a NIC without a model")
	case v.NIC != nil && v.NIC.MAC != "" && !macAddr.MatchString(v.NIC.MAC):
		return fmt.Errorf("MAC address %q", v.NIC.MAC)
	}
	return nil
}

// Put encrypts data with Bee's chunk encryption, through Bee's pipeline at
// redundancy level 0, and writes its chunks through put, the root last. It
// returns the blob's reference, which holds the root's key.
func Put(ctx context.Context, put storage.Putter, data []byte) (swarm.Address, error) {
	return builder.FeedPipeline(ctx, builder.NewPipelineBuilder(ctx, put, true, redundancy.NONE), bytes.NewReader(data))
}

// Get reads and decrypts the blob at ref, which must have at most limit
// bytes. Chunks come from get by their addresses, and the reference's keys
// decrypt them.
func Get(ctx context.Context, get storage.Getter, ref swarm.Address, limit int64) ([]byte, error) {
	if len(ref.Bytes()) != encryption.ReferenceSize {
		return nil, fmt.Errorf("blob %s: not an encrypted reference", ref)
	}
	j, size, err := joiner.New(ctx, get, discard{}, ref, redundancy.NONE)
	if err != nil {
		return nil, err
	}
	if size > limit {
		return nil, fmt.Errorf("a blob of %d bytes, more than %d", size, limit)
	}
	return io.ReadAll(j)
}

// discard is a putter for reading, which writes nothing.
type discard struct{}

func (discard) Put(context.Context, swarm.Chunk) error { return nil }

// PackDir returns the regular files directly in dir as a tar archive: a
// TPM's state, which swtpm keeps in a few files there. It leaves out
// swtpm's lock file.
func PackDir(dir string) ([]byte, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		if !e.Type().IsRegular() || e.Name() == ".lock" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		if err := tw.WriteHeader(&tar.Header{Name: e.Name(), Mode: 0o600, Size: int64(len(b)), Typeflag: tar.TypeReg, Format: tar.FormatPAX}); err != nil {
			return nil, err
		}
		if _, err := tw.Write(b); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnpackDir writes the files of a PackDir archive into dir, private to
// their user. It takes plain file names only.
func UnpackDir(b []byte, dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tr := tar.NewReader(bytes.NewReader(b))
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if h.Typeflag != tar.TypeReg || h.Name == "" || h.Name == "." || h.Name == ".." || strings.ContainsAny(h.Name, `/\`) {
			return fmt.Errorf("TPM state: unexpected entry %q", h.Name)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, h.Name), data, 0o600); err != nil {
			return err
		}
	}
}
