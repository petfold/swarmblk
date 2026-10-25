//go:build integration

package main

// Live tests against a running Bee node: $SWARMBLK_BEE_URL (default
// http://localhost:1633), uploading with the immutable batch in
// $SWARMBLK_BATCH, which must belong to that node. Run them with
//
//	go test -tags integration ./tools/beeproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
)

// upload stores data with POST /bytes at redundancy level 0, as PHASE0.md §3
// does, and returns its reference.
func upload(t *testing.T, bee string, data []byte) string {
	t.Helper()
	batch := os.Getenv("SWARMBLK_BATCH")
	if batch == "" {
		t.Fatal("SWARMBLK_BATCH is not set")
	}
	req, err := http.NewRequest(http.MethodPost, bee+"/bytes", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Swarm-Postage-Batch-Id", batch)
	req.Header.Set("Swarm-Redundancy-Level", "0")
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var r struct{ Reference string }
	if resp.StatusCode != http.StatusCreated || json.Unmarshal(body, &r) != nil || len(r.Reference) != 64 {
		t.Fatalf("upload: %s: %s", resp.Status, body)
	}
	return r.Reference
}

// TestLiveRangedReads reads an upload through the proxy with the flags of each
// boot run in PHASE0.md §5.
func TestLiveRangedReads(t *testing.T) {
	bee := envOr("SWARMBLK_BEE_URL", "http://localhost:1633")
	data := make([]byte, 4<<20+1000)       // the last chunk is partial
	rand.NewChaCha8([32]byte{}).Read(data) // the same bytes every run
	ref := upload(t, bee, data)
	ranges := [][2]int{
		{0, 4095},                         // the first chunk
		{4000, 4200},                      // across a chunk boundary
		{524000, 525000},                  // across two 512 KiB subtrees
		{len(data) - 1000, len(data) - 1}, // the partial last chunk
	}
	for _, run := range []struct {
		name      string
		lookahead int
		nocache   bool
	}{
		{"cold-la", -1, true},
		{"cold-nola", 0, true},
		{"warm", -1, false},
	} {
		t.Run(run.name, func(t *testing.T) {
			proxy, rows := startProxy(t, bee, run.lookahead, run.nocache)
			path := "/bytes/" + ref
			resp, err := http.Head(proxy + path)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK || resp.ContentLength != int64(len(data)) || resp.Header.Get("Accept-Ranges") != "bytes" {
				t.Errorf("HEAD: status %d, length %d, Accept-Ranges %q", resp.StatusCode, resp.ContentLength, resp.Header.Get("Accept-Ranges"))
			}
			want := [][]string{{"HEAD", path, "", "200", "0"}}

			for _, r := range ranges {
				rng := fmt.Sprintf("bytes=%d-%d", r[0], r[1])
				req, err := http.NewRequest(http.MethodGet, proxy+path, nil)
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Range", rng)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, data[r[0]:r[1]+1]) {
					t.Errorf("GET %s: status %d, %d bytes, want 206 and the uploaded bytes", rng, resp.StatusCode, len(body))
				}
				want = append(want, []string{"GET", path, rng, "206", strconv.Itoa(r[1] - r[0] + 1)})
			}

			got := rows()
			if len(got) != len(want) {
				t.Fatalf("rows = %q, want %d", got, len(want))
			}
			for i, row := range got {
				if !slices.Equal(row[1:6], want[i]) {
					t.Errorf("row %d = %q, want %q", i, row[1:6], want[i])
				}
			}
		})
	}
}

// TestLiveQEMUReads reads a disk image through the proxy with QEMU's HTTP block
// driver, using an overlay made as PHASE0.md §5 makes it.
func TestLiveQEMUReads(t *testing.T) {
	for _, tool := range []string{"qemu-img", "qemu-io"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip(tool, "not found")
		}
	}
	bee := envOr("SWARMBLK_BEE_URL", "http://localhost:1633")
	img := make([]byte, 16<<20) // zeros apart from three regions
	rand.NewChaCha8([32]byte{1}).Read(img[1<<20 : 2<<20])
	copy(img[8<<20:], bytes.Repeat([]byte{0xab}, 64<<10))
	copy(img[len(img)-4096:], bytes.Repeat([]byte{0xcd}, 4096))
	ref := upload(t, bee, img)
	proxy, rows := startProxy(t, bee, 0, true)

	dir := t.TempDir()
	raw := filepath.Join(dir, "disk.raw")
	if err := os.WriteFile(raw, img, 0o644); err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(dir, "overlay.qcow2")
	backing := fmt.Sprintf(`json:{"file.driver":"http","file.url":"%s/bytes/%s","file.readahead":"64k","file.timeout":60}`, proxy, ref)
	mustRun(t, "qemu-img", "create", "-q", "-f", "qcow2", "-F", "raw", "-b", backing, overlay)
	mustRun(t, "qemu-io", "-r", "-f", "qcow2", "-c", "read -P 0xab 8M 64k", "-c", "read -P 0xcd 16380k 4k", "-c", "read -P 0 4M 1M", overlay)
	mustRun(t, "qemu-img", "compare", "-f", "qcow2", "-F", "raw", overlay, raw)

	gets := 0
	for _, row := range rows() {
		switch {
		case row[1] == "HEAD" && row[4] == "200":
		case row[1] == "GET" && row[4] == "206" && row[3] != "":
			gets++
		default:
			t.Errorf("row %q, want a HEAD with 200 or a ranged GET with 206", row)
		}
	}
	if gets == 0 {
		t.Error("no ranged GETs went through the proxy")
	}
}

func mustRun(t *testing.T, name string, args ...string) {
	t.Helper()
	if out, err := exec.Command(name, args...).CombinedOutput(); err != nil {
		t.Fatalf("%s: %v\n%s", name, err, out)
	}
}
