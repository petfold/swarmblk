package main

import (
	"bytes"
	"encoding/csv"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeBee serves data at every path the way Bee serves /bytes, through
// http.ServeContent, and records the headers of each request it gets.
type fakeBee struct {
	*httptest.Server
	mu   sync.Mutex
	seen []http.Header
}

func newFakeBee(t *testing.T, data []byte) *fakeBee {
	b := &fakeBee{}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		b.seen = append(b.seen, r.Header.Clone())
		b.mu.Unlock()
		w.Header().Set("Content-Type", "application/octet-stream") // as Bee sets it for /bytes
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
	}))
	t.Cleanup(b.Close)
	return b
}

func (b *fakeBee) headers() []http.Header {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.seen)
}

// startProxy runs the proxy in front of bee. It returns the proxy's URL and a
// function that stops the proxy and returns the logged rows after the header.
func startProxy(t *testing.T, bee string, lookahead int, nocache bool) (string, func() [][]string) {
	t.Helper()
	u, err := url.Parse(bee)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	l := newLog(&buf, true)
	srv := httptest.NewServer(newProxy(u, lookahead, nocache, l))
	t.Cleanup(srv.Close)
	return srv.URL, func() [][]string {
		srv.Close() // waits for every handler, and so for every row
		l.mu.Lock()
		defer l.mu.Unlock()
		recs, err := csv.NewReader(&buf).ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(recs[0], ","); got != "start_ns,method,path,range,status,bytes,duration_ms" {
			t.Fatalf("header = %q", got)
		}
		return recs[1:]
	}
}

func TestForwardsHeadAndRangedGet(t *testing.T) {
	data := make([]byte, 64<<10)
	for i := range data {
		data[i] = byte(i * 7)
	}
	bee := newFakeBee(t, data)
	proxy, rows := startProxy(t, bee.URL, 0, true)
	before := time.Now().UnixNano()

	// QEMU's probe: a HEAD that must report the size and byte ranges.
	resp, err := http.Head(proxy + "/bytes/abc")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength != int64(len(data)) || resp.Header.Get("Accept-Ranges") != "bytes" {
		t.Errorf("HEAD: status %d, length %d, Accept-Ranges %q", resp.StatusCode, resp.ContentLength, resp.Header.Get("Accept-Ranges"))
	}

	req, err := http.NewRequest(http.MethodGet, proxy+"/bytes/abc", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=4096-8191")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, data[4096:8192]) {
		t.Errorf("GET: status %d, %d bytes, want 206 and blocks 4096-8191", resp.StatusCode, len(body))
	}

	seen := bee.headers()
	if len(seen) != 2 {
		t.Fatalf("Bee got %d requests, want 2", len(seen))
	}
	for _, h := range seen {
		if h.Get("Swarm-Lookahead-Buffer-Size") != "0" || h.Get("Swarm-Cache") != "false" {
			t.Errorf("Bee got Swarm-Lookahead-Buffer-Size %q and Swarm-Cache %q, want 0 and false",
				h.Get("Swarm-Lookahead-Buffer-Size"), h.Get("Swarm-Cache"))
		}
	}

	got := rows()
	want := [][]string{
		{"HEAD", "/bytes/abc", "", "200", "0"},
		{"GET", "/bytes/abc", "bytes=4096-8191", "206", "4096"},
	}
	if len(got) != len(want) {
		t.Fatalf("rows = %q, want %d", got, len(want))
	}
	for i, row := range got {
		if !slices.Equal(row[1:6], want[i]) {
			t.Errorf("row %d = %q, want %q", i, row[1:6], want[i])
		}
		if ns, err := strconv.ParseInt(row[0], 10, 64); err != nil || ns < before || ns > time.Now().UnixNano() {
			t.Errorf("row %d: start_ns %q is not a time during the test", i, row[0])
		}
		if ms, err := strconv.ParseFloat(row[6], 64); err != nil || ms < 0 {
			t.Errorf("row %d: duration_ms %q", i, row[6])
		}
	}
}

func TestSendsNoSwarmHeadersByDefault(t *testing.T) {
	bee := newFakeBee(t, []byte("hello"))
	proxy, _ := startProxy(t, bee.URL, -1, false)
	resp, err := http.Get(proxy + "/bytes/abc")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	for _, h := range bee.headers() {
		for _, k := range []string{"Swarm-Lookahead-Buffer-Size", "Swarm-Cache"} {
			if v, ok := h[k]; ok {
				t.Errorf("Bee got %s: %q", k, v)
			}
		}
	}
}

func TestRefusesOtherMethods(t *testing.T) {
	bee := newFakeBee(t, nil)
	proxy, rows := startProxy(t, bee.URL, -1, false)
	resp, err := http.Post(proxy+"/stamps/1000/22", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "GET, HEAD" {
		t.Errorf("POST: status %d, Allow %q", resp.StatusCode, resp.Header.Get("Allow"))
	}
	if n := len(bee.headers()); n != 0 {
		t.Errorf("Bee got %d requests, want none", n)
	}
	if r := rows(); len(r) != 1 || r[0][1] != "POST" || r[0][4] != "405" {
		t.Errorf("rows = %q, want one POST row with status 405", r)
	}
}

func TestLogsAbortedRequest(t *testing.T) {
	bee := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.Write(make([]byte, 10))
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler) // drop the connection after 10 of 100 bytes
	}))
	t.Cleanup(bee.Close)
	proxy, rows := startProxy(t, bee.URL, -1, false)
	if resp, err := http.Get(proxy + "/bytes/abc"); err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if r := rows(); len(r) != 1 || r[0][4] != "200" || r[0][5] != "10" {
		t.Errorf("rows = %q, want one row with status 200 and 10 bytes", r)
	}
}

func TestOpenLogAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.csv")
	for range 2 {
		l, err := openLog(path)
		if err != nil {
			t.Fatal(err)
		}
		l.row("1", "GET", "/bytes/abc", "", "200", "5", "0.100")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "start_ns,method,path,range,status,bytes,duration_ms\n" +
		"1,GET,/bytes/abc,,200,5,0.100\n" +
		"1,GET,/bytes/abc,,200,5,0.100\n"
	if string(got) != want {
		t.Errorf("log file:\n%s\nwant:\n%s", got, want)
	}
}
