package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// diskSize is the size of the disk under test: XFS needs 300 MiB.
const diskSize = 1 << 30

// runner drives the test on the host. Each run serves its disk from its own
// state directory, whose chunk store links in the base's chunks; with no
// lineage key there, every run's checkpoints start a lineage of their own.
type runner struct {
	work     string // the work directory
	swarmblk string // the swarmblk binary
	self     string // this binary, which the guest runs
	image    string // the guest image
	beeA     string // the Bee node that stamps
	beeB     string // the node that exports go through
	batch    string
	rng      *rand.Rand
	out      io.Writer // progress
	results  *os.File  // a JSON line per run
	bucket   int       // stop uploading once the fullest bucket holds this many chunks

	state struct {
		Zero  string            `json:"zero"`  // the snapshot of the zero disk
		Bases map[string]string `json:"bases"` // each filesystem's snapshot after mkfs
	}
}

func (r *runner) logf(format string, a ...any) {
	fmt.Fprintf(r.out, "%s "+format+"\n", append([]any{time.Now().Format("15:04:05")}, a...)...)
}

func (r *runner) loadState() error {
	b, err := os.ReadFile(filepath.Join(r.work, "state.json"))
	if os.IsNotExist(err) {
		r.state.Bases = map[string]string{}
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &r.state)
}

func (r *runner) saveState() error {
	b, err := json.MarshalIndent(r.state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(r.work, "state.json"), append(b, '\n'), 0o600)
}

func (r *runner) record(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = r.results.Write(append(b, '\n'))
	return err
}

// dir makes a fresh directory for run id, for its consoles and logs.
func (r *runner) dir(id string) (string, error) {
	d := filepath.Join(r.work, "runs", id)
	if err := os.RemoveAll(d); err != nil {
		return "", err
	}
	return d, os.MkdirAll(d, 0o700)
}

// home makes a fresh state directory for id, with the chunk store of the
// home named from linked in, if any.
func (r *runner) home(id, from string) (string, error) {
	h := filepath.Join(r.work, "homes", id)
	if err := os.RemoveAll(h); err != nil {
		return "", err
	}
	if err := os.MkdirAll(h, 0o700); err != nil {
		return "", err
	}
	if from == "" {
		return h, nil
	}
	return h, run("cp", "-al", filepath.Join(r.work, "homes", from, "chunks"), filepath.Join(h, "chunks"))
}

// swarmblkCmd runs swarmblk with the state directory home.
func (r *runner) swarmblkCmd(home string, args ...string) *exec.Cmd {
	cmd := exec.Command(r.swarmblk, args...)
	cmd.Env = append(os.Environ(), "SWARMBLK_HOME="+home)
	return cmd
}

// zero imports the zero disk that every filesystem is made on.
func (r *runner) zero() error {
	if r.state.Zero != "" {
		return nil
	}
	path := filepath.Join(r.work, "zero.raw")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		return err
	}
	if err := os.Truncate(path, diskSize); err != nil {
		return err
	}
	home, err := r.home("zero", "")
	if err != nil {
		return err
	}
	out, err := r.swarmblkCmd(home, "import", "-bee", r.beeA, "-batch", r.batch, path).Output()
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}
	r.state.Zero = strings.TrimSpace(string(out))
	r.logf("zero disk: snapshot %s", r.state.Zero)
	return r.saveState()
}

// served is a running swarmblk serve -disk.
type served struct {
	cmd  *exec.Cmd
	uri  string
	done chan struct{}
}

// serve serves the disk name of home writable, on snapshot arg if new.
func (r *runner) serve(home, name, arg, sock, log string) (*served, error) {
	args := []string{"serve", "-bee", r.beeA, "-batch", r.batch, "-socket", sock, "-disk", name}
	if arg != "" {
		args = append(args, arg)
	}
	cmd := r.swarmblkCmd(home, args...)
	lf, err := os.OpenFile(log, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	cmd.Stderr = lf
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	s := &served{cmd: cmd, done: make(chan struct{})}
	uri := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(stdout).ReadString('\n')
		uri <- strings.TrimSpace(line)
		io.Copy(io.Discard, stdout)
	}()
	go func() {
		cmd.Wait()
		lf.Close()
		close(s.done)
	}()
	select {
	case s.uri = <-uri:
		if s.uri != "" {
			return s, nil
		}
	case <-time.After(5 * time.Minute):
		cmd.Process.Kill()
	}
	<-s.done
	b, _ := os.ReadFile(log)
	return nil, fmt.Errorf("serve did not start: %s", strings.TrimSpace(string(b)))
}

// stop stops serve as an interrupt does.
func (s *served) stop() {
	s.cmd.Process.Signal(os.Interrupt)
	select {
	case <-s.done:
	case <-time.After(5 * time.Minute):
		s.kill()
	}
}

// kill kills serve, as a crash of the host's engine does.
func (s *served) kill() {
	s.cmd.Process.Kill()
	<-s.done
}

// drive is the QEMU -drive option for an NBD URI or an image file.
func drive(file string, snapshot bool) string {
	d := "file=" + file + ",if=virtio,format=raw,discard=unmap"
	if snapshot {
		d += ",snapshot=on"
	}
	return d
}

// vm is a running guest.
type vm struct {
	cmd     *exec.Cmd
	console string
	done    chan struct{}
}

// boot starts a guest on an overlay of the image, with disk as its second
// disk, and a third disk, labelled SBXRUN, that brings this program and
// guestCmd for it to run.
func (r *runner) boot(dir, name, disk, guestCmd string) (*vm, error) {
	overlay := filepath.Join(dir, name+".qcow2")
	if err := run("qemu-img", "create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", r.image, overlay); err != nil {
		return nil, err
	}
	cmdFile := filepath.Join(dir, name+".cmd")
	if err := os.WriteFile(cmdFile, []byte(guestCmd+"\n"), 0o600); err != nil {
		return nil, err
	}
	iso := filepath.Join(dir, name+".iso")
	if err := run("genisoimage", "-quiet", "-o", iso, "-V", "SBXRUN", "-J", "-R", "-graft-points",
		"crashtest="+r.self, "cmd="+cmdFile); err != nil {
		return nil, err
	}
	console := filepath.Join(dir, name+".console")
	cf, err := os.Create(console)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command("qemu-system-x86_64", "-enable-kvm", "-cpu", "host", "-m", "2048", "-smp", "2",
		"-nographic", "-nic", "none",
		"-drive", "file="+overlay+",if=virtio,format=qcow2",
		"-drive", disk,
		"-drive", "file="+iso+",if=virtio,format=raw,readonly=on")
	cmd.Stdout, cmd.Stderr = cf, cf
	if err := cmd.Start(); err != nil {
		cf.Close()
		return nil, err
	}
	v := &vm{cmd: cmd, console: console, done: make(chan struct{})}
	go func() {
		cmd.Wait()
		cf.Close()
		os.Remove(overlay)
		os.Remove(iso)
		close(v.done)
	}()
	return v, nil
}

// wait waits for the guest to power off, killing it after timeout.
func (v *vm) wait(timeout time.Duration) error {
	select {
	case <-v.done:
		return nil
	case <-time.After(timeout):
		v.kill()
		return fmt.Errorf("the guest ran past %v", timeout)
	}
}

// kill kills QEMU, which to the guest is a power cut.
func (v *vm) kill() {
	v.cmd.Process.Kill()
	<-v.done
}

var sbxLine = regexp.MustCompile(`SBX ([A-Z]+) ?([^\r\n]*)`)

// lines returns the guest program's console lines: kind and the rest.
func (v *vm) lines() [][2]string {
	b, _ := os.ReadFile(v.console)
	return consoleLines(string(b))
}

func consoleLines(console string) [][2]string {
	var out [][2]string
	for _, m := range sbxLine.FindAllStringSubmatch(console, -1) {
		out = append(out, [2]string{m[1], strings.TrimSpace(m[2])})
	}
	return out
}

// lastDone returns the last iteration the guest reported committed.
func (v *vm) lastDone() int {
	last := 0
	for _, l := range v.lines() {
		if l[0] == "DONE" {
			if i, err := strconv.Atoi(l[1]); err == nil && i > last {
				last = i
			}
		}
	}
	return last
}

// waitFor waits until the guest reports a line of kind, or fails.
func (v *vm) waitFor(kind string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		for _, l := range v.lines() {
			switch l[0] {
			case kind:
				return nil
			case "ERROR":
				return fmt.Errorf("guest: %s", l[1])
			}
		}
		select {
		case <-v.done:
			return fmt.Errorf("the guest stopped before %s", kind)
		case <-time.After(200 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no %s from the guest in %v", kind, timeout)
		}
	}
}

// cpStats is what a checkpoint did.
type cpStats struct {
	Ref    string  `json:"ref"`
	Blocks int     `json:"blocks"`   // blocks changed
	Chunks int     `json:"chunks"`   // chunks uploaded
	Upload float64 `json:"upload_s"` // seconds until stored
	Sync   float64 `json:"sync_s"`   // seconds until synced
}

var (
	cpSnapshot = regexp.MustCompile(`snapshot: [0-9a-f]+, (\d+) blocks changed, (\d+) chunks to upload`)
	cpUpload   = regexp.MustCompile(`upload: \d+ chunks stored under tag \d+ in (\S+);`)
	cpSync     = regexp.MustCompile(`sync: \d+ of \d+ synced after (\S+)`)
)

// checkpoint checkpoints the disk name of home: through its serve, or
// itself if none runs it.
func (r *runner) checkpoint(home, name, log string) (cpStats, error) {
	cmd := r.swarmblkCmd(home, "checkpoint", "-bee", r.beeA, "-batch", r.batch, "-disk", name)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	os.WriteFile(log, []byte(stderr.String()), 0o600)
	if err != nil {
		return cpStats{}, fmt.Errorf("checkpoint: %v: %s", err, lastLines(stderr.String(), 3))
	}
	st := parseCheckpoint(stderr.String())
	st.Ref = strings.TrimSpace(string(out))
	return st, nil
}

// parseCheckpoint reads what a checkpoint's report says it did, summed
// over the snapshots it made.
func parseCheckpoint(report string) cpStats {
	var st cpStats
	for _, m := range cpSnapshot.FindAllStringSubmatch(report, -1) {
		b, _ := strconv.Atoi(m[1])
		c, _ := strconv.Atoi(m[2])
		st.Blocks += b
		st.Chunks += c
	}
	for _, m := range cpUpload.FindAllStringSubmatch(report, -1) {
		d, _ := time.ParseDuration(m[1])
		st.Upload += d.Seconds()
	}
	for _, m := range cpSync.FindAllStringSubmatch(report, -1) {
		d, _ := time.ParseDuration(m[1])
		st.Sync += d.Seconds()
	}
	return st
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.Join(lines[max(0, len(lines)-n):], "; ")
}

// export exports snapshot ref to out through node B, from an empty store,
// so that every chunk comes from the network.
func (r *runner) export(ref, out string) error {
	home, err := os.MkdirTemp(filepath.Join(r.work, "homes"), "export-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(home)
	b, err := r.swarmblkCmd(home, "export", "-bee", r.beeB, ref, out).CombinedOutput()
	if err != nil {
		return fmt.Errorf("export %s: %v: %s", ref, err, lastLines(string(b), 3))
	}
	return nil
}

// checkResult is what the checker reported for one disk.
type checkResult struct {
	OK        bool     `json:"ok"` // fsck passed and nothing was bad
	Fsck      string   `json:"fsck"`
	Committed int      `json:"committed"`
	Files     int      `json:"files"`
	Slots     int      `json:"slots"`
	Bad       []string `json:"bad,omitempty"`
	FsckOut   []string `json:"fsck_out,omitempty"`
}

var checkLine = regexp.MustCompile(`^(\S+) fsck=(\S+) committed=(\d+) files=(\d+) slots=(\d+) bad=(\d+)$`)

// check boots the checker on disk and returns its report.
func (r *runner) check(dir, fs, label, disk string) (checkResult, error) {
	v, err := r.boot(dir, "check-"+label, disk, "check "+fs+" "+label)
	if err != nil {
		return checkResult{}, err
	}
	if err := v.wait(15 * time.Minute); err != nil {
		return checkResult{}, err
	}
	return parseCheck(v.lines(), label)
}

// parseCheck reads the checker's report on the disk labelled label.
func parseCheck(lines [][2]string, label string) (checkResult, error) {
	var c checkResult
	seen := false
	for _, l := range lines {
		rest, ok := strings.CutPrefix(l[1], label+" ")
		switch {
		case l[0] == "ERROR":
			return c, fmt.Errorf("checker: %s", l[1])
		case !ok:
		case l[0] == "CHECK":
			m := checkLine.FindStringSubmatch(l[1])
			if m == nil {
				return c, fmt.Errorf("checker: %q", l[1])
			}
			seen = true
			c.Fsck = m[2]
			c.Committed, _ = strconv.Atoi(m[3])
			c.Files, _ = strconv.Atoi(m[4])
			c.Slots, _ = strconv.Atoi(m[5])
		case l[0] == "BAD":
			c.Bad = append(c.Bad, rest)
		case l[0] == "FSCK":
			c.FsckOut = append(c.FsckOut, rest)
		}
	}
	if !seen {
		return c, errors.New("the checker reported nothing")
	}
	c.OK = c.Fsck == "ok" && len(c.Bad) == 0
	return c, nil
}

// base makes filesystem fs on the zero disk and checkpoints it.
func (r *runner) base(fs string) error {
	if r.state.Bases[fs] != "" {
		return nil
	}
	id := "base-" + fs
	dir, err := r.dir(id)
	if err != nil {
		return err
	}
	home, err := r.home(id, "zero")
	if err != nil {
		return err
	}
	s, err := r.serve(home, "d", r.state.Zero, filepath.Join(dir, "nbd.sock"), filepath.Join(dir, "serve.log"))
	if err != nil {
		return err
	}
	v, err := r.boot(dir, "mkfs", drive(s.uri, false), "mkfs "+fs)
	if err == nil {
		if err = v.wait(15 * time.Minute); err == nil {
			err = v.waitFor("MKFS", 0)
		}
	}
	s.stop()
	if err != nil {
		return fmt.Errorf("%s: mkfs: %w", fs, err)
	}
	st, err := r.checkpoint(home, "d", filepath.Join(dir, "checkpoint.log"))
	if err != nil {
		return err
	}
	r.state.Bases[fs] = st.Ref
	r.logf("%s: base snapshot %s, %d blocks, %d chunks", fs, st.Ref, st.Blocks, st.Chunks)
	if err := r.record(map[string]any{"kind": "base", "fs": fs, "checkpoint": st}); err != nil {
		return err
	}
	return r.saveState()
}

// quick crashes the guest while it writes, by killing QEMU or the engine
// under it, and checks the disk straight through the engine: no
// checkpoint, nothing uploaded.
func (r *runner) quick(fs string, n int) (bool, error) {
	id := fmt.Sprintf("quick-%s-%d", fs, n)
	dir, err := r.dir(id)
	if err != nil {
		return false, err
	}
	home, err := r.home(id, "base-"+fs)
	if err != nil {
		return false, err
	}
	mode := []string{"qemu", "serve"}[n%2]
	sock := filepath.Join(dir, "nbd.sock")
	s, err := r.serve(home, "d", r.state.Bases[fs], sock, filepath.Join(dir, "serve.log"))
	if err != nil {
		return false, err
	}
	defer func() { s.stop() }()
	seed, interval := r.rng.Uint64()>>1, r.rng.IntN(20)
	v, err := r.boot(dir, "work", drive(s.uri, false), fmt.Sprintf("work %s %d %d", fs, seed, interval))
	if err != nil {
		return false, err
	}
	if err := v.waitFor("START", 5*time.Minute); err != nil {
		v.kill()
		return false, err
	}
	after := time.Duration(1000+r.rng.IntN(14000)) * time.Millisecond
	time.Sleep(after)
	done := v.lastDone()
	if mode == "qemu" {
		v.kill()
	} else {
		s.kill()
		time.Sleep(time.Second)
		v.kill()
		if s, err = r.serve(home, "d", "", sock, filepath.Join(dir, "serve.log")); err != nil {
			return false, err
		}
	}
	c, err := r.check(dir, fs, "crash", drive(s.uri, true))
	if err != nil {
		return false, err
	}
	ok := c.OK && c.Committed >= done
	r.logf("%s quick %d: killed %s after %v at iteration %d; recovered %d, %s", fs, n, mode, after.Round(time.Millisecond), done, c.Committed, verdict(ok, c))
	return ok, r.record(map[string]any{"kind": "quick", "fs": fs, "n": n, "mode": mode, "seed": seed,
		"interval_ms": interval, "after_s": after.Seconds(), "done": done, "check": c, "ok": ok})
}

func verdict(ok bool, c checkResult) string {
	if ok {
		return "ok"
	}
	return fmt.Sprintf("FAILED (fsck %s, %d bad: %v)", c.Fsck, len(c.Bad), c.Bad)
}

// full takes a live checkpoint while the guest writes, then crashes it by
// killing QEMU, the engine or both, and checkpoints the disk again. Both
// snapshots are exported through node B and checked there.
func (r *runner) full(fs string, n int) (bool, error) {
	id := fmt.Sprintf("full-%s-%d", fs, n)
	dir, err := r.dir(id)
	if err != nil {
		return false, err
	}
	home, err := r.home(id, "base-"+fs)
	if err != nil {
		return false, err
	}
	mode := []string{"qemu", "qemu+serve", "serve"}[n%3]
	s, err := r.serve(home, "d", r.state.Bases[fs], filepath.Join(dir, "nbd.sock"), filepath.Join(dir, "serve.log"))
	if err != nil {
		return false, err
	}
	alive := true
	defer func() {
		if alive {
			s.stop()
		}
	}()
	seed := r.rng.Uint64() >> 1
	v, err := r.boot(dir, "work", drive(s.uri, false), fmt.Sprintf("work %s %d 100", fs, seed))
	if err != nil {
		return false, err
	}
	if err := v.waitFor("START", 5*time.Minute); err != nil {
		v.kill()
		return false, err
	}
	time.Sleep(time.Duration(2000+r.rng.IntN(8000)) * time.Millisecond)
	doneLive := v.lastDone()
	live, err := r.checkpoint(home, "d", filepath.Join(dir, "live.log"))
	if err != nil {
		v.kill()
		return false, err
	}
	time.Sleep(time.Duration(1000+r.rng.IntN(7000)) * time.Millisecond)
	doneKill := v.lastDone()
	switch mode {
	case "qemu":
		v.kill()
	case "qemu+serve":
		v.kill()
		s.kill()
		alive = false
	case "serve":
		s.kill()
		alive = false
		time.Sleep(time.Second)
		v.kill()
	}
	crash, err := r.checkpoint(home, "d", filepath.Join(dir, "crash.log"))
	if err != nil {
		return false, err
	}
	if alive {
		s.stop()
		alive = false
	}
	res := map[string]any{"kind": "full", "fs": fs, "n": n, "mode": mode, "seed": seed}
	ok := true
	for _, c := range []struct {
		label string
		cp    cpStats
		done  int
	}{{"live", live, doneLive}, {"crash", crash, doneKill}} {
		img := filepath.Join(dir, c.label+".raw")
		if err := r.export(c.cp.Ref, img); err != nil {
			return false, err
		}
		cr, err := r.check(dir, fs, c.label, drive(img, true))
		os.Remove(img)
		if err != nil {
			return false, err
		}
		good := cr.OK && cr.Committed >= c.done
		ok = ok && good
		res[c.label] = map[string]any{"checkpoint": c.cp, "done": c.done, "check": cr, "ok": good}
		r.logf("%s full %d (%s): %s snapshot %s, %d chunks; iteration %d reported, %d recovered, %s", fs, n, mode, c.label, c.cp.Ref, c.cp.Chunks, c.done, cr.Committed, verdict(good, cr))
	}
	res["ok"] = ok
	return ok, r.record(res)
}

// cost runs the same workload, iterations at a time, on filesystem fs and
// checkpoints after each round with the guest stopped: what the
// checkpoints upload is the filesystem's cost (DESIGN §9).
func (r *runner) cost(fs string, iterations, rounds int) error {
	id := "cost-" + fs
	dir, err := r.dir(id)
	if err != nil {
		return err
	}
	home, err := r.home(id, "base-"+fs)
	if err != nil {
		return err
	}
	s, err := r.serve(home, "d", r.state.Bases[fs], filepath.Join(dir, "nbd.sock"), filepath.Join(dir, "serve.log"))
	if err != nil {
		return err
	}
	defer s.stop()
	for round := 1; round <= rounds; round++ {
		v, err := r.boot(dir, fmt.Sprintf("work%d", round), drive(s.uri, false), fmt.Sprintf("work %s %d 0 %d", fs, costSeed, iterations))
		if err != nil {
			return err
		}
		if err := v.wait(time.Hour); err != nil {
			return err
		}
		if err := v.waitFor("WORK", 0); err != nil {
			return err
		}
		st, err := r.checkpoint(home, "d", filepath.Join(dir, fmt.Sprintf("checkpoint%d.log", round)))
		if err != nil {
			return err
		}
		r.logf("%s cost round %d: %d iterations, %d blocks changed, %d chunks uploaded", fs, round, iterations, st.Blocks, st.Chunks)
		if err := r.record(map[string]any{"kind": "cost", "fs": fs, "round": round, "iterations": iterations, "checkpoint": st}); err != nil {
			return err
		}
	}
	return nil
}

// costSeed makes every filesystem's cost workload the same.
const costSeed = 20261026

// fullestBucket returns how many chunks the batch's fullest bucket holds.
func (r *runner) fullestBucket() (int, error) {
	resp, err := http.Get(r.beeA + "/stamps/" + r.batch + "/buckets")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var b struct {
		Buckets []struct{ Collisions int }
	}
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		return 0, err
	}
	most := 0
	for _, x := range b.Buckets {
		most = max(most, x.Collisions)
	}
	return most, nil
}
