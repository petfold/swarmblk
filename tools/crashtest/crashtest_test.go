package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// newDir makes the workload's first files, with a few slots.
func newDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := initDir(dir, 8); err != nil {
		t.Fatal(err)
	}
	return dir
}

func clean(t *testing.T, dir string) result {
	t.Helper()
	r := verify(dir)
	if len(r.problems) > 0 {
		t.Fatalf("verify: %v", r.problems)
	}
	return r
}

func TestWorkload(t *testing.T) {
	dirs := []string{newDir(t), newDir(t)}
	for _, dir := range dirs {
		var done []int
		w := workload{seed: 7, n: 300, done: func(i int) { done = append(done, i) }}
		if err := w.run(dir); err != nil {
			t.Fatal(err)
		}
		if r := clean(t, dir); r.committed != 300 || len(done) != 300 || done[299] != 300 || r.slots != 8 || r.files == 0 {
			t.Fatalf("after 300 iterations: %v, %d reported", r, len(done))
		}
	}
	a, _ := os.ReadFile(filepath.Join(dirs[0], "log"))
	b, _ := os.ReadFile(filepath.Join(dirs[1], "log"))
	if !bytes.Equal(a, b) {
		t.Error("the same seed gave two logs")
	}
	for _, kind := range []string{"C ", "b ", "B ", "d ", "D "} {
		if !strings.Contains(string(a), "\n"+kind) {
			t.Errorf("no %q records in 300 iterations", kind)
		}
	}
	w := workload{seed: 7, n: 50}
	if err := w.run(dirs[0]); err != nil {
		t.Fatal(err)
	}
	if r := clean(t, dirs[0]); r.committed != 350 {
		t.Errorf("continued for 50 iterations: %v", r)
	}
}

// TestCrashPoints stops the workload before each of its writes in turn,
// once inside a log record, as a crash would: whatever is left must
// verify, with every reported iteration committed.
func TestCrashPoints(t *testing.T) {
	base := newDir(t)
	for cut := 1; cut <= 120; cut++ {
		dir := t.TempDir()
		if err := os.CopyFS(dir, os.DirFS(base)); err != nil {
			t.Fatal(err)
		}
		steps, reported := 0, 0
		w := workload{seed: 11, n: 40,
			done: func(i int) { reported = i },
			stop: func() bool { steps++; return steps == cut },
		}
		if err := w.run(dir); err != errStopped && !strings.Contains(fmt.Sprint(err), errStopped.Error()) {
			t.Fatalf("cut %d: the workload ended with %v", cut, err)
		}
		r := verify(dir)
		if len(r.problems) > 0 || r.committed < reported || r.committed > reported+1 {
			t.Fatalf("cut %d after iteration %d: %v %v", cut, reported, r, r.problems)
		}
	}
}

// TestVerifyFinds damages a finished workload's directory: each kind of
// damage must be found.
func TestVerifyFinds(t *testing.T) {
	base := newDir(t)
	if err := (&workload{seed: 3, n: 200}).run(base); err != nil {
		t.Fatal(err)
	}
	r := clean(t, base)
	log, _ := os.ReadFile(filepath.Join(base, "log"))
	e, _ := replay(log)
	var kept, gone string // a file that must be there, and one deleted
	for _, line := range strings.Split(string(log), "\n") {
		if f := strings.Fields(line); len(f) == 3 && f[0] == "D" {
			gone = f[2]
		}
	}
	for name := range e.files {
		kept = name
	}
	if kept == "" || gone == "" {
		t.Fatal("the workload kept or deleted no file")
	}
	lines := strings.SplitAfter(string(log), "\n")
	for name, damage := range map[string]func(dir string){
		"a changed file": func(dir string) {
			b, _ := os.ReadFile(filepath.Join(dir, kept))
			b[len(b)/2] ^= 1
			os.WriteFile(filepath.Join(dir, kept), b, 0o644)
		},
		"a short file": func(dir string) { os.Truncate(filepath.Join(dir, kept), 4096) },
		"a lost file":  func(dir string) { os.Remove(filepath.Join(dir, kept)) },
		"a deleted file back": func(dir string) {
			os.WriteFile(filepath.Join(dir, gone), []byte("back"), 0o644)
		},
		"a changed slot": func(dir string) {
			f, _ := os.OpenFile(filepath.Join(dir, "big"), os.O_WRONLY, 0)
			f.WriteAt([]byte("x"), 3*slotSize+100)
			f.Close()
		},
		"a lost log record": func(dir string) {
			os.WriteFile(filepath.Join(dir, "log"), []byte(strings.Join(slices.Delete(slices.Clone(lines), 100, 101), "")), 0o644)
		},
		"an intent with more after it": func(dir string) {
			at := slices.IndexFunc(lines, func(l string) bool { return strings.HasPrefix(l, "B ") })
			os.WriteFile(filepath.Join(dir, "log"), []byte(strings.Join(slices.Delete(slices.Clone(lines), at, at+1), "")), 0o644)
		},
		"a file from the future": func(dir string) {
			os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d", r.committed+2)), nil, 0o644)
		},
	} {
		dir := t.TempDir()
		os.CopyFS(dir, os.DirFS(base))
		damage(dir)
		if got := verify(dir); len(got.problems) == 0 {
			t.Errorf("%s: not found", name)
		}
	}

	// The next iteration's file may be there half written.
	dir := t.TempDir()
	os.CopyFS(dir, os.DirFS(base))
	os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d", r.committed+1)), []byte("half"), 0o644)
	clean(t, dir)
}

func TestParse(t *testing.T) {
	report := `disk d: froze epoch 1 with 3668 blocks
disk d: 3668 blocks on snapshot 99ec170616bb46aeedac0c9501b75a5bc3984fc39009627fc1acb9e8b80f535f, rebuilt in 4.862s
snapshot: 68cc648741df9669335b91efbc62f60930eec07430f63a5dab1e2d3200efe935, 3653 blocks changed, 3668 chunks to upload
upload: 3668 chunks stored under tag 731771 in 23s; waiting for sync
sync: 3668 of 3668 synced after 1m8s
`
	if st := parseCheckpoint(report); st.Blocks != 3653 || st.Chunks != 3668 || st.Upload != 23 || st.Sync != 68 {
		t.Errorf("parsed %+v", st)
	}
	console := "[  OK  ] Started sbx.service\r\nSBX CHECK live fsck=ok committed=41 files=12 slots=256 bad=1\r\n" +
		"[   12.5] EXT4-fs (vdb): recovery complete\r\nSBX BAD live f7: holds 1234…, the log says 5678…\r\nSBX BAD other x\r\n"
	c, err := parseCheck(consoleLines(console), "live")
	if err != nil || c.OK || c.Fsck != "ok" || c.Committed != 41 || c.Files != 12 || c.Slots != 256 || len(c.Bad) != 1 {
		t.Errorf("parsed %+v, %v", c, err)
	}
	if _, err := parseCheck(consoleLines("SBX ERROR mount failed\r\n"), "live"); err == nil {
		t.Error("an error from the checker was taken for a report")
	}
}
