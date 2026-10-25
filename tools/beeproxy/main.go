// Beeproxy is a logging reverse proxy in front of a Bee node, for the Phase 0
// boot measurements (docs/PHASE0.md §4).
//
// It forwards GET and HEAD unchanged, apart from the optional
// Swarm-Lookahead-Buffer-Size and Swarm-Cache headers, and refuses every other
// method, so nothing can upload or buy stamps through it. It writes one CSV row
// per request as the request finishes: start time (Unix ns), method, path,
// Range header, status, body bytes sent and duration (ms). The log is both the
// latency record and the boot access trace; sort by start time for the access
// order.
//
// Usage:
//
//	beeproxy [-listen address] [-bee url] [-lookahead bytes] [-nocache] [-log file]
package main

import (
	"encoding/csv"
	"flag"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:1636", "listen `address`; Bee itself uses 1633 (API) and 1634 (p2p)")
	bee := flag.String("bee", envOr("SWARMBLK_BEE_URL", "http://localhost:1633"), "upstream Bee API `url` ($SWARMBLK_BEE_URL if set)")
	lookahead := flag.Int("lookahead", -1, "send Swarm-Lookahead-Buffer-Size: `bytes`; 0 turns Bee's lookahead off, -1 leaves the header out")
	nocache := flag.Bool("nocache", false, "send Swarm-Cache: false, so Bee does not cache retrieved chunks")
	logPath := flag.String("log", "", "append CSV rows to `file` (default stdout)")
	flag.Parse()
	log.SetPrefix("beeproxy: ")

	target, err := url.Parse(*bee)
	if err != nil || target.Scheme == "" || target.Host == "" {
		log.Fatalf("bad -bee URL %q", *bee)
	}
	rows, err := openLog(*logPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s, forwarding to %s", *listen, target)
	log.Fatal(http.ListenAndServe(*listen, newProxy(target, *lookahead, *nocache, rows)))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// newProxy forwards GET and HEAD requests to bee, adding the Swarm headers the
// flags ask for, and logs every request to l.
func newProxy(bee *url.URL, lookahead int, nocache bool, l *csvLog) http.Handler {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil               // connect to Bee directly, whatever HTTP_PROXY says
	tr.DisableCompression = true // never ask Bee for gzip; bodies pass through as sent
	tr.MaxIdleConnsPerHost = 64  // keep a connection for each of QEMU's parallel requests
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(bee)
			if lookahead >= 0 {
				pr.Out.Header.Set("Swarm-Lookahead-Buffer-Size", strconv.Itoa(lookahead))
			}
			if nocache {
				pr.Out.Header.Set("Swarm-Cache", "false")
			}
		},
		Transport:     tr,
		FlushInterval: -1, // pass each read of the body on at once
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &recorder{ResponseWriter: w}
		finished := false
		// Deferred, so that a request aborted mid-body, which panics with
		// http.ErrAbortHandler, is logged too.
		defer func() {
			d := time.Since(start)
			l.row(strconv.FormatInt(start.UnixNano(), 10), r.Method, r.URL.Path, r.Header.Get("Range"),
				strconv.Itoa(rec.code()), strconv.FormatInt(rec.sent, 10),
				strconv.FormatFloat(float64(d.Microseconds())/1e3, 'f', 3, 64))
			if !finished {
				log.Printf("%s %s (Range %q) aborted after %d bytes and %v", r.Method, r.URL.Path, r.Header.Get("Range"), rec.sent, d)
			}
		}()
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			rp.ServeHTTP(rec, r)
		} else {
			rec.Header().Set("Allow", "GET, HEAD")
			http.Error(rec, "beeproxy forwards only GET and HEAD", http.StatusMethodNotAllowed)
		}
		finished = true
	})
}

// recorder keeps the response status and counts the body bytes sent.
type recorder struct {
	http.ResponseWriter
	status int
	sent   int64
}

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 && code >= 200 { // skip 1xx interim responses
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(p)
	r.sent += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the connection's Flush.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (r *recorder) code() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

// csvLog writes rows from concurrent requests. It flushes every row, so the
// file can be followed during a run and keeps its rows if the proxy is killed.
type csvLog struct {
	mu sync.Mutex
	w  *csv.Writer
}

func newLog(w io.Writer, header bool) *csvLog {
	l := &csvLog{w: csv.NewWriter(w)}
	if header {
		l.row("start_ns", "method", "path", "range", "status", "bytes", "duration_ms")
	}
	return l
}

// openLog appends to the file at path and writes the header only if the file
// is new or empty. An empty path means stdout.
func openLog(path string) (*csvLog, error) {
	if path == "" {
		return newLog(os.Stdout, true), nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return newLog(f, fi.Size() == 0), nil
}

func (l *csvLog) row(fields ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.w.Write(fields)
	l.w.Flush()
	if err := l.w.Error(); err != nil {
		log.Printf("writing log: %v", err)
	}
}
