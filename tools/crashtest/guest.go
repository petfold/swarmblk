package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Inside the guest, the disk under test is the second virtio disk.
const (
	dev  = "/dev/vdb"
	mnt  = "/mnt/t" // where the workload mounts it
	pool = "tank"   // the ZFS pool on it
)

// guest runs a command in the test VM, from the disk the host boots it with:
//
//	mkfs FS                      make FS on the disk and the workload's first files
//	work FS SEED INTERVAL [N]    run the workload, forever or for N iterations
//	check FS LABEL               recover FS after a crash, check it, and verify the files
//
// It reports on the serial console in lines that start with "SBX ", and
// powers the VM off when it is done, except while a workload runs forever:
// the host kills QEMU then.
func guest(args []string) error {
	// The console, not ttyS0 itself, which the serial getty hangs up when it
	// starts, so that writes to it fail.
	con, err := os.OpenFile("/dev/console", os.O_WRONLY|syscall.O_NOCTTY, 0)
	if err != nil {
		return err
	}
	say := func(format string, a ...any) { fmt.Fprintf(con, "SBX "+format+"\n", a...) }
	exec.Command("dmesg", "-n", "1").Run() // keep kernel messages off the console
	if len(args) < 2 || !slices.Contains([]string{"ext4", "xfs", "btrfs", "zfs"}, args[1]) {
		err = fmt.Errorf("bad command %q", args)
	} else {
		fs := args[1]
		switch {
		case args[0] == "mkfs" && len(args) == 2:
			if err = mkfs(fs); err == nil {
				say("MKFS OK")
			}
		case args[0] == "work" && (len(args) == 4 || len(args) == 5):
			err = work(fs, args[2:], say)
		case args[0] == "check" && len(args) == 3:
			err = check(fs, args[2], say)
		default:
			err = fmt.Errorf("bad command %q", args)
		}
	}
	if err != nil {
		say("ERROR %v", err)
		if len(args) > 0 && args[0] == "work" {
			select {} // the host kills QEMU
		}
	}
	return exec.Command("systemctl", "poweroff").Run()
}

// mkfs makes the filesystem fs on the disk with its defaults, aligned to
// 4 KiB, and the workload's first files on it.
func mkfs(fs string) error {
	var err error
	switch fs {
	case "ext4":
		err = run("mkfs.ext4", "-q", "-F", "-E", "lazy_itable_init=0,lazy_journal_init=0", dev)
	case "xfs":
		err = run("mkfs.xfs", "-q", "-f", dev)
	case "btrfs":
		err = run("mkfs.btrfs", "-q", "-f", dev)
	case "zfs":
		err = run("zpool", "create", "-f", "-o", "ashift=12", "-O", "mountpoint="+mnt, pool, dev)
	}
	if err != nil {
		return err
	}
	dir := mnt // where zpool create mounted the pool
	if fs != "zfs" {
		if dir, err = mount(fs, false); err != nil {
			return err
		}
	}
	if err := initDir(dir, 256); err != nil { // big is 16 MiB
		return err
	}
	return unmount(fs, dir)
}

// work mounts the filesystem and runs the workload: forever, or for N
// iterations, after which it unmounts it.
func work(fs string, args []string, say func(string, ...any)) error {
	seed, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return err
	}
	interval, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}
	n := 0
	if len(args) == 3 {
		if n, err = strconv.Atoi(args[2]); err != nil || n <= 0 {
			return fmt.Errorf("iterations %q", args[2])
		}
	}
	dir, err := mount(fs, false)
	if err != nil {
		return err
	}
	w := workload{seed: seed, interval: time.Duration(interval) * time.Millisecond, n: n}
	w.started = func(from int) { say("START %d", from) }
	w.done = func(i int) { say("DONE %d", i) }
	if err := w.run(dir); err != nil {
		return err
	}
	if err := unmount(fs, dir); err != nil {
		return err
	}
	say("WORK OK")
	return nil
}

// check recovers the filesystem as after a power cut, by mounting it,
// verifies the workload's files against its log, and then checks the
// filesystem with its own tools. It reports one CHECK line, a BAD line for
// each problem, and the checker's output if it failed.
func check(fs, label string, say func(string, ...any)) error {
	dir, err := mount(fs, true)
	if err != nil {
		return err
	}
	r := verify(dir)
	out, ferr := fsck(fs, dir)
	status := "ok"
	if ferr != nil {
		status = "failed"
	}
	say("CHECK %s fsck=%s %s", label, status, r)
	for _, p := range r.problems {
		say("BAD %s %s", label, p)
	}
	if ferr != nil {
		say("FSCK %s %v", label, ferr)
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			say("FSCK %s %s", label, line)
		}
	}
	return nil
}

// mount mounts the disk and returns the workload's directory. For a check
// it mounts elsewhere and imports a ZFS pool under an alternate root.
func mount(fs string, checking bool) (string, error) {
	dir := mnt
	if checking {
		dir = "/mnt/c"
	}
	if fs == "zfs" {
		// On a whole disk the pool is in a partition: let zpool find it.
		args := []string{"import", "-d", "/dev", "-f"}
		if checking {
			args = append(args, "-R", dir)
		}
		if err := run("zpool", append(args, pool)...); err != nil {
			return "", err
		}
		if checking {
			return dir + mnt, nil
		}
		return mnt, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	args := []string{dev, dir}
	if fs == "xfs" && checking {
		args = append([]string{"-o", "nouuid"}, args...)
	}
	return dir, run("mount", args...)
}

// unmount unmounts the filesystem, or exports the pool.
func unmount(fs, dir string) error {
	if fs == "zfs" {
		return run("zpool", "export", pool)
	}
	return run("umount", dir)
}

// fsck checks the filesystem: ext4, XFS and Btrfs offline after unmounting
// it, Btrfs also with a scrub while mounted, and ZFS with a scrub. It
// returns the checker's output.
func fsck(fs, dir string) (string, error) {
	var out []byte
	var err error
	switch fs {
	case "ext4":
		if err = unmount(fs, dir); err == nil {
			out, err = exec.Command("e2fsck", "-fn", dev).CombinedOutput()
		}
	case "xfs":
		if err = unmount(fs, dir); err == nil {
			out, err = exec.Command("xfs_repair", "-n", dev).CombinedOutput()
		}
	case "btrfs":
		if out, err = exec.Command("btrfs", "scrub", "start", "-B", dir).CombinedOutput(); err != nil {
			unmount(fs, dir)
			break
		}
		if err = unmount(fs, dir); err == nil {
			out, err = exec.Command("btrfs", "check", "--readonly", dev).CombinedOutput()
		}
	case "zfs":
		if out, err = exec.Command("zpool", "scrub", "-w", pool).CombinedOutput(); err == nil {
			out, err = exec.Command("zpool", "status", "-v", pool).CombinedOutput()
			if err == nil && !strings.Contains(string(out), "errors: No known data errors") {
				err = errors.New("zpool status reports errors")
			}
		}
		if eerr := unmount(fs, dir); err == nil {
			err = eerr
		}
	}
	return string(out), err
}

// run runs a command, and returns an error with its output if it fails.
func run(name string, args ...string) error {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %v: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// initDir writes the workload's first files in dir: big, with n random
// slots, and the log that records them, all fsynced.
func initDir(dir string, n int) error {
	src := rand.NewChaCha8([32]byte{'s', 'b', 'x'})
	big, err := os.OpenFile(filepath.Join(dir, "big"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer big.Close()
	var log strings.Builder
	b := make([]byte, slotSize)
	for s := range n {
		src.Read(b)
		if _, err := big.Write(b); err != nil {
			return err
		}
		fmt.Fprintf(&log, "I %d %s\n", s, sum(b))
	}
	if err := big.Sync(); err != nil {
		return err
	}
	if err := writeSynced(filepath.Join(dir, "log"), []byte(log.String())); err != nil {
		return err
	}
	return syncDir(dir)
}

func writeSynced(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// workload creates files, overwrites slots of big in place and deletes
// files, at random, each operation fsynced and then committed in the log.
type workload struct {
	seed     uint64
	interval time.Duration // between iterations
	n        int           // iterations, or 0 for ever

	started func(from int) // called with the last committed iteration
	done    func(i int)    // called once iteration i is committed
	stop    func() bool    // asked before each write, so tests can cut the run anywhere
}

// errStopped ends a run that stop cut short.
var errStopped = errors.New("stopped")

// run continues the workload in dir from where its log ends. The same seed
// and log give the same operations and data.
func (w *workload) run(dir string) error {
	log, err := os.ReadFile(filepath.Join(dir, "log"))
	if err != nil {
		return err
	}
	e, problems := replay(log)
	if problems != nil {
		return errors.New(problems[0])
	}
	if e.doubtSlot >= 0 || e.doubtFile != "" {
		return errors.New("the log ends in an intent, as a crash leaves it")
	}
	lf, err := os.OpenFile(filepath.Join(dir, "log"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	defer lf.Close()
	big, err := os.OpenFile(filepath.Join(dir, "big"), os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer big.Close()
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	var key [32]byte
	binary.LittleEndian.PutUint64(key[:], w.seed)
	binary.LittleEndian.PutUint64(key[8:], uint64(e.committed))
	data := rand.NewChaCha8(key)
	rng := rand.New(rand.NewPCG(w.seed, uint64(e.committed)))
	stopped := func() bool { return w.stop != nil && w.stop() }
	record := func(format string, a ...any) error {
		line := fmt.Sprintf(format+"\n", a...)
		if stopped() { // cut inside the record
			lf.WriteString(line[:len(line)/2])
			return errStopped
		}
		if _, err := lf.WriteString(line); err != nil {
			return err
		}
		return lf.Sync()
	}
	step := func(err error) error {
		if err == nil && stopped() {
			return errStopped
		}
		return err
	}
	files := slices.Sorted(maps.Keys(e.files))
	if w.started != nil {
		w.started(e.committed)
	}
	last := e.committed + w.n
	for i := e.committed + 1; w.n == 0 || i <= last; i++ {
		var err error
		switch r := rng.IntN(100); {
		case r < 15 && len(files) > 0: // delete
			j := rng.IntN(len(files))
			name := files[j]
			if err = record("d %d %s", i, name); err == nil {
				err = step(os.Remove(filepath.Join(dir, name)))
			}
			if err == nil {
				err = step(d.Sync())
			}
			if err == nil {
				err = record("D %d %s", i, name)
			}
			files = slices.Delete(files, j, j+1)
		case r < 50: // overwrite a slot
			s := rng.IntN(len(e.slots))
			b := make([]byte, slotSize)
			data.Read(b)
			if err = record("b %d %d %s", i, s, sum(b)); err == nil {
				_, err = big.WriteAt(b, int64(s)*slotSize)
				err = step(err)
			}
			if err == nil {
				err = step(big.Sync())
			}
			if err == nil {
				err = record("B %d %d", i, s)
			}
		default: // create
			name := fmt.Sprintf("f%d", i)
			b := make([]byte, (rng.IntN(16)+1)*4096)
			data.Read(b)
			err = step(createSynced(filepath.Join(dir, name), b, stopped))
			if err == nil {
				err = step(d.Sync())
			}
			if err == nil {
				err = record("C %d %s %d %s", i, name, len(b), sum(b))
			}
			files = append(files, name)
		}
		if err != nil {
			return fmt.Errorf("iteration %d: %w", i, err)
		}
		if w.done != nil {
			w.done(i)
		}
		time.Sleep(w.interval)
	}
	return nil
}

// createSynced writes a new file and fsyncs it. If stopped says so after
// the first half, it leaves the file half written.
func createSynced(path string, b []byte, stopped func() bool) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(b[:len(b)/2]); err != nil {
		return err
	}
	if stopped() {
		return errStopped
	}
	if _, err := f.Write(b[len(b)/2:]); err != nil {
		return err
	}
	return f.Sync()
}
