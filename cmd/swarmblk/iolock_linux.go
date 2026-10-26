package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// Capabilities that ioLock needs, as bits of /proc/self/status's CapEff.
const (
	capIPCLock     = 14
	capSysResource = 24
)

// prctl options (Linux 5.6).
const (
	prSetIOFlusher = 57
	prGetIOFlusher = 58
)

// effectiveCaps returns the process's effective capabilities.
func effectiveCaps() (uint64, error) {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "CapEff:"); ok {
			return strconv.ParseUint(strings.TrimSpace(v), 16, 64)
		}
	}
	return 0, errors.New("no CapEff in /proc/self/status")
}

// capNames names the capabilities that ioLock and cmdIOLock need.
var capNames = map[int]string{capIPCLock: "cap_ipc_lock", capSysResource: "cap_sys_resource"}

// needCaps checks that the process has the capabilities caps, by number.
func needCaps(what string, caps ...int) error {
	have, err := effectiveCaps()
	if err != nil {
		return err
	}
	var names, upper []string
	missing := false
	for _, c := range caps {
		names = append(names, capNames[c])
		upper = append(upper, strings.ToUpper(capNames[c]))
		missing = missing || have&(1<<c) == 0
	}
	if missing {
		return fmt.Errorf("%s needs %s: sudo setcap %s+ep %s", what, strings.Join(upper, " and "), strings.Join(names, ","), os.Args[0])
	}
	return nil
}

// ioFlusher reports whether the process is an I/O flusher.
func ioFlusher() bool {
	r, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, prGetIOFlusher, 0, 0, 0, 0, 0)
	return errno == 0 && r == 1
}

// setIOFlusher makes the calling thread, and the threads and processes it
// starts from now on, I/O flushers: the kernel lets their allocations make
// progress without waiting for writeback, which may need them.
func setIOFlusher() error {
	if _, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, prSetIOFlusher, 1, 0, 0, 0, 0); errno != 0 {
		return fmt.Errorf("PR_SET_IO_FLUSHER: %w", errno)
	}
	return nil
}

// ioLockEnv marks the process that ioLock started, which must be an I/O
// flusher already.
const ioLockEnv = "SWARMBLK_IOLOCK"

// ioLock readies serve for a host that mounts its disk through the
// kernel's NBD client, where the engine sits in the kernel's writeback
// path (ROADMAP Phase 2): it makes the process an I/O flusher, starting
// itself again with the same arguments so that all its threads are, and
// locks its memory so that it is never swapped out. It returns only in the
// process that runs on.
func ioLock() error {
	if err := needCaps("-iolock", capSysResource, capIPCLock); err != nil {
		return err
	}
	if !ioFlusher() {
		if os.Getenv(ioLockEnv) != "" {
			return errors.New("-iolock: the process lost PR_SET_IO_FLUSHER when it started itself again")
		}
		runtime.LockOSThread() // the flag is the thread's, and exec must keep it
		if err := setIOFlusher(); err != nil {
			return err
		}
		self, err := os.Executable()
		if err != nil {
			return err
		}
		return syscall.Exec(self, os.Args, append(os.Environ(), ioLockEnv+"=1")) // the flag survives exec
	}
	os.Unsetenv(ioLockEnv)
	if err := syscall.Mlockall(syscall.MCL_CURRENT | syscall.MCL_FUTURE); err != nil {
		return fmt.Errorf("mlockall: %w", err)
	}
	return nil
}

// cmdIOLock runs a command, such as Bee, as an I/O flusher: swarmblk
// iolock -- command args. Locked memory does not survive exec, so the
// command must lock its own, or its service manager must protect it.
func cmdIOLock(args []string) error {
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	if len(args) == 0 {
		usage()
	}
	if err := needCaps("iolock", capSysResource); err != nil {
		return err
	}
	runtime.LockOSThread() // the flag is the thread's, and exec must keep it
	if err := setIOFlusher(); err != nil {
		return err
	}
	path, err := exec.LookPath(args[0])
	if err != nil {
		return err
	}
	return syscall.Exec(path, args, os.Environ())
}
