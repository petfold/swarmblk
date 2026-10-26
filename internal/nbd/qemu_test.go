package nbd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const page = 4096

// memDevice keeps a device in memory as 4 KiB pages. A missing page is a
// hole, which reads as zeros.
type memDevice struct {
	size int64

	mu                            sync.Mutex
	pages                         map[int64][]byte
	writes, fua, flushes, discard int
}

func newMemDevice(size int64) *memDevice {
	return &memDevice{size: size, pages: map[int64][]byte{}}
}

func (d *memDevice) Size() int64 { return d.size }

func (d *memDevice) ReadAt(_ context.Context, p []byte, off int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := int64(0); i < int64(len(p)); {
		n, in := (off+i)/page, (off+i)%page
		step := min(page-in, int64(len(p))-i)
		if pg, ok := d.pages[n]; ok {
			copy(p[i:i+step], pg[in:in+step])
		} else {
			clear(p[i : i+step])
		}
		i += step
	}
	return nil
}

// set writes data, or zeros for nil, over length bytes from off. Whole
// pages become holes when hole is set; other pages are allocated.
func (d *memDevice) set(off, length int64, data []byte, hole bool) {
	for i := int64(0); i < length; {
		n, in := (off+i)/page, (off+i)%page
		step := min(page-in, length-i)
		if hole && in == 0 && step == page {
			delete(d.pages, n)
		} else {
			pg, ok := d.pages[n]
			if !ok {
				pg = make([]byte, page)
				d.pages[n] = pg
			}
			if data != nil {
				copy(pg[in:in+step], data[i:i+step])
			} else {
				clear(pg[in : in+step])
			}
		}
		i += step
	}
}

func (d *memDevice) WriteAt(_ context.Context, p []byte, off int64, fua bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.set(off, int64(len(p)), p, false)
	d.writes++
	if fua {
		d.fua++
	}
	return nil
}

func (d *memDevice) Flush(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.flushes++
	return nil
}

func (d *memDevice) Trim(_ context.Context, off, length int64, fua bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.set(off, length, nil, true)
	d.discard++
	return nil
}

func (d *memDevice) WriteZeroes(_ context.Context, off, length int64, fua, noHole bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.set(off, length, nil, !noHole)
	d.writes++
	return nil
}

// state returns how the page at n reads: a hole, allocated zeros, or data.
func (d *memDevice) state(n int64) (hole, zero bool) {
	pg, ok := d.pages[n]
	if !ok {
		return true, true
	}
	return false, bytes.Equal(pg, make([]byte, page))
}

func (d *memDevice) Extents(_ context.Context, off, length int64) ([]Extent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []Extent
	for i := int64(0); i < length; {
		n, in := (off+i)/page, (off+i)%page
		step := min(page-in, length-i)
		hole, zero := d.state(n)
		if k := len(out); k > 0 && out[k-1].Hole == hole && out[k-1].Zero == zero {
			out[k-1].Length += step
		} else {
			out = append(out, Extent{Length: step, Hole: hole, Zero: zero})
		}
		i += step
	}
	return out, nil
}

// bytes returns the device's contents.
func (d *memDevice) bytes() []byte {
	b := make([]byte, d.size)
	d.ReadAt(context.Background(), b, 0)
	return b
}

// needQEMU skips a test without QEMU's tools, except in CI, which installs
// them.
func needQEMU(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"qemu-img", "qemu-io", "qemu-nbd"} {
		if _, err := exec.LookPath(tool); err != nil {
			if os.Getenv("CI") != "" {
				t.Fatalf("%s is missing", tool)
			}
			t.Skipf("%s is missing; these tests use QEMU's NBD client", tool)
		}
	}
}

// serve serves the exports on a Unix socket until the test ends and returns
// the socket's path.
func serve(t *testing.T, exports ...Export) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nbd.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- (&Server{Exports: exports}).Serve(ctx, l) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return path
}

func uri(sock, name string) string { return "nbd+unix:///" + name + "?socket=" + sock }

// qemu runs a QEMU tool and returns its output; it fails the test if the
// tool fails, unless fail is set.
func qemu(t *testing.T, fail bool, name string, args ...string) string {
	t.Helper()
	out, err := exec.Command(name, args...).CombinedOutput()
	if (err != nil) != fail {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func TestQEMU(t *testing.T) {
	needQEMU(t)
	disk := newMemDevice(64 << 20)
	disk.set(8<<20, 1<<20, bytes.Repeat([]byte{0x5a}, 1<<20), false)
	sock := serve(t, Export{Name: "disk", Description: "a test disk", Device: disk})
	u := uri(sock, "disk")

	var info struct {
		Size int64 `json:"virtual-size"`
	}
	if err := json.Unmarshal([]byte(qemu(t, false, "qemu-img", "info", "--output=json", u)), &info); err != nil || info.Size != disk.size {
		t.Fatalf("qemu-img info: size %d, %v", info.Size, err)
	}

	out := qemu(t, false, "qemu-io", "-f", "raw",
		"-c", "write -P 0xab 0 64k", "-c", "read -P 0xab 0 64k",
		"-c", "write -z 64k 64k", "-c", "write -P 0xcd 128k 64k", "-c", "discard 128k 64k",
		"-c", "write -f -P 0xef 1M 4k", "-c", "flush", "-c", "read -P 0xef 1M 4k",
		"-c", "read -P 0x5a 8M 1M", "-c", "write -P 0x11 3072 2048", u)
	if strings.Contains(out, "fail") || strings.Contains(out, "rror") {
		t.Fatalf("qemu-io:\n%s", out)
	}
	disk.mu.Lock()
	fua, flushes, discards := disk.fua, disk.flushes, disk.discard
	disk.mu.Unlock()
	if fua < 1 || flushes < 1 || discards < 1 {
		t.Errorf("the device saw %d FUA writes, %d flushes and %d trims; want at least one each", fua, flushes, discards)
	}

	want := make([]byte, disk.size)
	copy(want, bytes.Repeat([]byte{0xab}, 64<<10))
	copy(want[3072:], bytes.Repeat([]byte{0x11}, 2048))
	copy(want[1<<20:], bytes.Repeat([]byte{0xef}, 4096))
	copy(want[8<<20:], bytes.Repeat([]byte{0x5a}, 1<<20))
	if !bytes.Equal(disk.bytes(), want) {
		t.Fatal("the device holds other bytes than qemu-io wrote")
	}
	raw := filepath.Join(t.TempDir(), "disk.raw")
	qemu(t, false, "qemu-img", "convert", "-f", "raw", "-O", "raw", u, raw)
	if got, err := os.ReadFile(raw); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("qemu-img convert read other bytes: %v", err)
	}

	// Block status: qemu-img map reports data, allocated zeros and holes.
	var extents []struct {
		Start, Length int64
		Data, Zero    bool
	}
	if err := json.Unmarshal([]byte(qemu(t, false, "qemu-img", "map", "-f", "raw", "--output=json", u)), &extents); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range extents {
		got = append(got, fmt.Sprintf("%d+%d data=%t zero=%t", e.Start, e.Length, e.Data, e.Zero))
	}
	wantMap := []string{
		"0+65536 data=true zero=false",
		"65536+65536 data=true zero=true", // written zeros stay allocated
		"131072+917504 data=false zero=true",
		"1048576+4096 data=true zero=false",
		"1052672+7335936 data=false zero=true",
		"8388608+1048576 data=true zero=false",
		"9437184+57671680 data=false zero=true",
	}
	if strings.Join(got, "\n") != strings.Join(wantMap, "\n") {
		t.Errorf("qemu-img map:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(wantMap, "\n"))
	}
}

func TestQEMUReadOnlyAndList(t *testing.T) {
	needQEMU(t)
	ro := newMemDevice(64 << 20)
	ro.set(0, 4096, bytes.Repeat([]byte{0x42}, 4096), false)
	sock := serve(t, Export{Name: "disk", Device: newMemDevice(128 << 20)}, Export{Name: "ro", Description: "read-only", Device: ro, ReadOnly: true})

	if out := qemu(t, false, "qemu-io", "-r", "-f", "raw", "-c", "read -P 0x42 0 4k", uri(sock, "ro")); strings.Contains(out, "fail") {
		t.Errorf("reading the read-only export:\n%s", out)
	}
	qemu(t, true, "qemu-io", "-f", "raw", "-c", "write 0 4k", uri(sock, "ro"))
	if ro.writes != 0 {
		t.Errorf("the read-only device took %d writes", ro.writes)
	}

	list := qemu(t, false, "qemu-nbd", "--list", "-k", sock)
	for _, s := range []string{"export: 'disk'", "export: 'ro'", "description: read-only", "size:  134217728", "size:  67108864",
		"flags: 0x6d ( flush fua trim zeroes )", "flags: 0xf ( readonly flush fua )", "min block: 512", "opt block: 4096", "max block: 33554432", "base:allocation"} {
		if !strings.Contains(list, s) {
			t.Errorf("qemu-nbd --list lacks %q:\n%s", s, list)
		}
	}
	// The default export is the first.
	if out := qemu(t, false, "qemu-img", "info", "--output=json", "nbd+unix://?socket="+sock); !strings.Contains(out, `"virtual-size": 134217728`) {
		t.Errorf("the default export:\n%s", out)
	}
	qemu(t, true, "qemu-img", "info", uri(sock, "nonexistent"))
}
