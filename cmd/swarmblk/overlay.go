package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// An overlay is a qcow2 file on top of a snapshot's disk, which QEMU reads
// from Bee over HTTP: its backing file names the snapshot as
// …/bzz/<snapshot>/disk.raw, in a plain URL or in a json: file name that
// also sets QEMU's HTTP options. The guest's writes stay in the overlay.
type overlay struct {
	Path     string
	Size     int64         // the disk's size
	Backing  string        // the backing file name, as the image holds it
	Snapshot swarm.Address // the snapshot that it names
}

var backingSnapshot = regexp.MustCompile(`/bzz/([0-9a-f]{64})/disk\.raw`)

// qemuImg runs qemu-img and returns its output, or an error with what it
// wrote to stderr.
func qemuImg(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "qemu-img", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, fmt.Errorf("qemu-img %s: %s", args[0], msg)
		}
		return nil, fmt.Errorf("qemu-img %s: %w", args[0], err)
	}
	return out, nil
}

// openOverlay reads the image at path with qemu-img info and checks that it
// is an overlay whose blocks swarmblk can read from the file itself.
func openOverlay(ctx context.Context, path string) (overlay, error) {
	out, err := qemuImg(ctx, "info", "--output=json", path)
	if err != nil {
		return overlay{}, err
	}
	var info struct {
		Format     string `json:"format"`
		Size       int64  `json:"virtual-size"`
		Backing    string `json:"backing-filename"`
		BackingFmt string `json:"backing-filename-format"`
		Encrypted  bool   `json:"encrypted"`
		Specific   struct {
			Data struct {
				DataFile string `json:"data-file"`
			} `json:"data"`
		} `json:"format-specific"`
	}
	if err := json.Unmarshal(out, &info); err != nil {
		return overlay{}, fmt.Errorf("qemu-img info %s: %w", path, err)
	}
	switch {
	case info.Format != "qcow2":
		return overlay{}, fmt.Errorf("%s is %s, not a qcow2 overlay", path, info.Format)
	case info.Encrypted || info.Specific.Data.DataFile != "":
		return overlay{}, fmt.Errorf("%s is encrypted or keeps its data in another file; swarmblk reads plain qcow2 overlays", path)
	case info.Backing == "" || info.BackingFmt != "raw":
		return overlay{}, fmt.Errorf("%s has no raw backing file; checkpoint an overlay on a snapshot's disk.raw", path)
	}
	m := backingSnapshot.FindAllStringSubmatch(info.Backing, -1)
	if len(m) != 1 {
		return overlay{}, fmt.Errorf("%s: backing file %q does not name one snapshot as …/bzz/<snapshot>/disk.raw", path, info.Backing)
	}
	return overlay{Path: path, Size: info.Size, Backing: info.Backing, Snapshot: swarm.MustParseHexAddress(m[0][1])}, nil
}

// extent is one entry of qemu-img map's JSON output.
type extent struct {
	Start      int64  `json:"start"`
	Length     int64  `json:"length"`
	Present    bool   `json:"present"`
	Zero       bool   `json:"zero"`
	Data       bool   `json:"data"`
	Compressed bool   `json:"compressed"`
	Offset     *int64 `json:"offset"`
}

// blocks passes the 4 KiB blocks that the overlay itself holds to fn, in
// order and at most window at a time: each with its contents, or nil for a
// zero block. It maps the overlay with qemu-img map, its backing file left
// out, and reads each block from the file at the offset that the map gives,
// so it never reads the snapshot underneath. qemu-img takes the image's
// lock, so a running VM's overlay is refused. It returns the number of
// blocks passed.
func (o overlay) blocks(ctx context.Context, window int, fn func(map[int64][]byte) error) (int64, error) {
	abs, err := filepath.Abs(o.Path)
	if err != nil {
		return 0, err
	}
	spec, err := json.Marshal(map[string]any{"driver": "qcow2", "backing": nil, "file": map[string]any{"driver": "file", "filename": abs}})
	if err != nil {
		return 0, err
	}
	out, err := qemuImg(ctx, "map", "--output=json", "json:"+string(spec))
	if err != nil {
		return 0, err
	}
	var extents []extent
	if err := json.Unmarshal(out, &extents); err != nil {
		return 0, fmt.Errorf("qemu-img map %s: %w", o.Path, err)
	}
	f, err := os.Open(o.Path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var n int64
	batch := map[int64][]byte{}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n += int64(len(batch))
		err := fn(batch)
		batch = map[int64][]byte{}
		return err
	}
	for _, e := range extents {
		if !e.Present {
			continue
		}
		if e.Start%swarm.ChunkSize != 0 || e.Length%swarm.ChunkSize != 0 {
			return n, fmt.Errorf("%s holds bytes %d to %d, which are not whole 4 KiB blocks", o.Path, e.Start, e.Start+e.Length)
		}
		if !e.Zero && (!e.Data || e.Compressed || e.Offset == nil) {
			return n, fmt.Errorf("%s holds bytes %d to %d compressed or out of the file", o.Path, e.Start, e.Start+e.Length)
		}
		for off := int64(0); off < e.Length; off += swarm.ChunkSize {
			var data []byte
			if !e.Zero {
				data = make([]byte, swarm.ChunkSize)
				if _, err := f.ReadAt(data, *e.Offset+off); err != nil {
					return n, fmt.Errorf("%s: %w", o.Path, err)
				}
			}
			batch[(e.Start+off)/swarm.ChunkSize] = data
			if len(batch) == window {
				if err := flush(); err != nil {
					return n, err
				}
			}
		}
	}
	return n, flush()
}

// rebase makes the overlay's backing file name snap instead, keeping the
// name's form and options. qemu-img rebase -u rewrites only the name, which
// is safe when snap's disk equals the old one wherever the overlay holds no
// block, as a checkpoint of the overlay's own blocks makes it.
func (o overlay) rebase(ctx context.Context, snap swarm.Address) error {
	backing := backingSnapshot.ReplaceAllLiteralString(o.Backing, "/bzz/"+snap.String()+"/disk.raw")
	if backing == o.Backing {
		return errors.New("rebase onto the same snapshot")
	}
	_, err := qemuImg(ctx, "rebase", "-u", "-f", "qcow2", "-F", "raw", "-b", backing, o.Path)
	return err
}
