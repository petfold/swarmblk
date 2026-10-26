package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// The workload keeps its files in one directory: big, a file of slots
// overwritten in place; f<i>, the file that iteration i created; and log,
// the record of every operation, appended and fsynced. Each iteration does
// one operation and ends with a record that commits it, after everything
// it wrote was fsynced:
//
//	I <slot> <sha>                  big's slot as mkfs wrote it
//	C <i> <name> <size> <sha>       a file, created and fsynced, with its directory
//	b <i> <slot> <sha>              about to overwrite a slot of big
//	B <i> <slot>                    the overwrite, fsynced
//	d <i> <name>                    about to delete a file
//	D <i> <name>                    the delete, with the directory fsynced
//
// The I records come first and number the slots from 0. After a crash,
// every committed operation must be there. An intent with no commit can
// only end the log, and leaves its slot or file in doubt.
const slotSize = 64 << 10

// file is what a created file holds.
type file struct {
	size int64
	sum  string
}

// expected is the state that the log promises.
type expected struct {
	committed int             // iterations committed: 1 to committed
	slots     []string        // each slot's sha256
	files     map[string]file // the files that must exist
	doubtSlot int             // a slot whose overwrite is in doubt, or -1
	doubtFile string          // a file whose delete is in doubt, or ""
}

// replay reads the log and returns what it promises. A last line without
// its newline was cut by the crash and is ignored; anything else that does
// not follow the rules is a problem, and replay stops there.
func replay(log []byte) (*expected, []string) {
	e := &expected{files: map[string]file{}, doubtSlot: -1}
	lines := strings.Split(string(log), "\n")
	lines = lines[:len(lines)-1] // the part after the last newline
	var pending []string         // an intent's fields
	for n, line := range lines {
		f := strings.Fields(line)
		bad := func(why string) (*expected, []string) {
			return e, []string{fmt.Sprintf("log line %d %q: %s", n+1, line, why)}
		}
		if len(f) > 0 && f[0] == "I" {
			if len(f) != 3 || e.committed > 0 || pending != nil {
				return bad("not an initial slot")
			}
			if f[1] != strconv.Itoa(len(e.slots)) {
				return bad("slots out of order")
			}
			e.slots = append(e.slots, f[2])
			continue
		}
		if len(f) < 3 {
			return bad("too short")
		}
		i, err := strconv.Atoi(f[1])
		if err != nil || i != e.committed+1 {
			return bad(fmt.Sprintf("iteration %s, want %d", f[1], e.committed+1))
		}
		switch {
		case pending == nil && f[0] == "C" && len(f) == 5:
			size, err := strconv.ParseInt(f[3], 10, 64)
			if err != nil || f[2] != fmt.Sprintf("f%d", i) {
				return bad("a bad create")
			}
			e.files[f[2]] = file{size, f[4]}
			e.committed++
		case pending == nil && f[0] == "b" && len(f) == 4:
			if s, err := strconv.Atoi(f[2]); err != nil || s < 0 || s >= len(e.slots) {
				return bad("no such slot")
			}
			pending = f
		case pending == nil && f[0] == "d" && len(f) == 3:
			if _, ok := e.files[f[2]]; !ok {
				return bad("deletes a file that is not there")
			}
			pending = f
		case pending != nil && pending[0] == "b" && f[0] == "B" && len(f) == 3 && f[2] == pending[2]:
			s, _ := strconv.Atoi(f[2])
			e.slots[s] = pending[3]
			pending = nil
			e.committed++
		case pending != nil && pending[0] == "d" && f[0] == "D" && len(f) == 3 && f[2] == pending[2]:
			delete(e.files, f[2])
			pending = nil
			e.committed++
		default:
			return bad("out of order")
		}
	}
	switch {
	case pending == nil:
	case pending[0] == "b":
		e.doubtSlot, _ = strconv.Atoi(pending[2])
	case pending[0] == "d":
		e.doubtFile = pending[2]
	}
	return e, nil
}

// result is what a check found on one disk.
type result struct {
	committed    int // iterations committed in the log
	files, slots int // files and slots verified
	problems     []string
}

func (r result) String() string {
	return fmt.Sprintf("committed=%d files=%d slots=%d bad=%d", r.committed, r.files, r.slots, len(r.problems))
}

// verify checks the workload's directory dir against its log: every
// committed operation must be there, and nothing that the log does not
// allow.
func verify(dir string) result {
	var r result
	log, err := os.ReadFile(filepath.Join(dir, "log"))
	if err != nil {
		r.problems = append(r.problems, err.Error())
		return r
	}
	e, problems := replay(log)
	r.committed = e.committed
	if problems != nil {
		r.problems = problems
		return r
	}
	problem := func(format string, a ...any) { r.problems = append(r.problems, fmt.Sprintf(format, a...)) }

	big, err := os.Open(filepath.Join(dir, "big"))
	if err != nil {
		problem("%v", err)
	} else {
		defer big.Close()
		buf := make([]byte, slotSize)
		for s, want := range e.slots {
			if s == e.doubtSlot {
				continue
			}
			if _, err := big.ReadAt(buf, int64(s)*slotSize); err != nil && err != io.EOF {
				problem("big: slot %d: %v", s, err)
			} else if got := sum(buf); got != want {
				problem("big: slot %d holds %.12s…, the log says %.12s…", s, got, want)
			} else {
				r.slots++
			}
		}
	}

	for _, name := range slices.Sorted(maps.Keys(e.files)) {
		want := e.files[name]
		b, err := os.ReadFile(filepath.Join(dir, name))
		switch {
		case os.IsNotExist(err) && name == e.doubtFile:
		case err != nil:
			problem("%s: %v", name, err)
		case int64(len(b)) != want.size:
			problem("%s: %d bytes, the log says %d", name, len(b), want.size)
		case sum(b) != want.sum:
			problem("%s: holds %.12s…, the log says %.12s…", name, sum(b), want.sum)
		default:
			r.files++
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		problem("%v", err)
		return r
	}
	for _, de := range entries {
		name := de.Name()
		// The file the next iteration was creating may be there in any
		// state, since nothing promised it.
		if _, ok := e.files[name]; ok || name == "log" || name == "big" || name == "lost+found" || name == fmt.Sprintf("f%d", e.committed+1) {
			continue
		}
		problem("%s: there, but the log does not allow it", name)
	}
	return r
}

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
