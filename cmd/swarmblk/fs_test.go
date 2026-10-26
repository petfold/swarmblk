package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// The filesystem test (ROADMAP, Phase 2): an ext4 image built from a
// directory of files, changed with debugfs as a filesystem would be,
// checkpointed and exported, must come back clean to e2fsck with every file
// as written. Nothing here needs root or a VM.

// needE2fs skips a test without e2fsprogs, except in CI.
func needE2fs(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"mkfs.ext4", "debugfs", "e2fsck"} {
		if _, err := exec.LookPath(tool); err != nil {
			if os.Getenv("CI") != "" {
				t.Fatalf("%s is missing", tool)
			}
			t.Skipf("%s is missing; the filesystem test needs e2fsprogs", tool)
		}
	}
}

// fsTree writes files of the sizes that ext4 treats differently (empty,
// inline, one block, one byte either side of it, many blocks) in nested
// directories, with an empty directory, and returns the tree's root.
func fsTree(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	src := rand.NewChaCha8([32]byte{17})
	for i, size := range []int{0, 1, 100, 4095, 4096, 4097, 65536, 1 << 20, 3<<20 + 123} {
		for _, sub := range []string{"a", "a/b", "c"} {
			data := make([]byte, size)
			src.Read(data)
			path := filepath.Join(dir, sub, fmt.Sprintf("f%d-%d", i, size))
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

// mkfs builds a 64 MiB ext4 image with 4 KiB blocks from dir.
func mkfs(t *testing.T, dir string) string {
	t.Helper()
	img := filepath.Join(t.TempDir(), "fs.raw")
	if err := os.WriteFile(img, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(img, 64<<20); err != nil {
		t.Fatal(err)
	}
	run(t, "mkfs.ext4", "-q", "-F", "-b", "4096", "-E", "nodiscard", "-d", dir, img)
	return img
}

// change applies the same changes to the image, with debugfs, and to the
// tree it came from: a new directory with new files, two files deleted, one
// replaced, and the empty directory removed.
func change(t *testing.T, img, tree string) {
	t.Helper()
	src := rand.NewChaCha8([32]byte{18})
	files := map[string][]byte{"added/big": make([]byte, 2<<20+7), "added/small": []byte("swarmblk\n"), "c/f5-4097": make([]byte, 5000)}
	stage := t.TempDir()
	for name, data := range files {
		if len(data) > 9 {
			src.Read(data)
		}
		if err := os.WriteFile(filepath.Join(stage, filepath.Base(name)), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	script := strings.Join([]string{
		"mkdir /added",
		"cd /added",
		"write " + filepath.Join(stage, "big") + " big",
		"write " + filepath.Join(stage, "small") + " small",
		"cd /",
		"rm /a/f7-1048576",
		"rm /a/b/f8-3145851",
		"rm /c/f5-4097",
		"cd /c",
		"write " + filepath.Join(stage, "f5-4097") + " f5-4097",
		"cd /",
		"rmdir /empty",
	}, "\n") + "\n"
	cmds := filepath.Join(t.TempDir(), "changes")
	if err := os.WriteFile(cmds, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("debugfs", "-w", "-f", cmds, img).CombinedOutput(); err != nil || bytes.Contains(out, []byte("rror")) || bytes.Contains(out, []byte("not found")) {
		t.Fatalf("debugfs: %v\n%s", err, out)
	}

	for _, name := range []string{"a/f7-1048576", "a/b/f8-3145851", "c/f5-4097"} {
		if err := os.Remove(filepath.Join(tree, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Remove(filepath.Join(tree, "empty")); err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		path := filepath.Join(tree, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// diffOverlay makes a qcow2 overlay on base whose disk is mod, holding the
// clusters where they differ, as a guest writing mod would leave it, and
// names its backing file as an overlay on snapshot snap. qemu-img rebase in
// safe mode copies exactly what differs between the two backing files.
func diffOverlay(t *testing.T, base, mod string, snap swarm.Address) string {
	t.Helper()
	ov := filepath.Join(t.TempDir(), "overlay.qcow2")
	run(t, "qemu-img", "create", "-q", "-f", "qcow2", "-F", "raw", "-b", mod, ov)
	run(t, "qemu-img", "rebase", "-q", "-f", "qcow2", "-F", "raw", "-b", base, ov)
	run(t, "qemu-img", "rebase", "-q", "-u", "-f", "qcow2", "-F", "raw", "-b", testBacking(t, snap), ov)
	return ov
}

// checkFS requires the ext4 image img to be clean to e2fsck and to hold the
// same directories and files as tree, byte for byte.
func checkFS(t *testing.T, img, tree string) {
	t.Helper()
	if out, err := exec.Command("e2fsck", "-fn", img).CombinedOutput(); err != nil {
		t.Fatalf("e2fsck -fn: %v\n%s", err, out)
	}
	dump := t.TempDir()
	if out, err := exec.Command("debugfs", "-R", "rdump / "+dump, img).CombinedOutput(); err != nil || bytes.Contains(out, []byte("rror")) {
		t.Fatalf("debugfs rdump: %v\n%s", err, out)
	}
	list := func(root string) map[string][]byte {
		m := map[string][]byte{}
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			switch {
			case rel == "." || rel == "lost+found" && d.IsDir():
				if rel != "." {
					return filepath.SkipDir
				}
			case d.IsDir():
				m[rel+"/"] = nil
			default:
				b, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				m[rel] = b
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	got, want := list(dump), list(tree)
	for name, data := range want {
		if b, ok := got[name]; !ok || !bytes.Equal(b, data) {
			t.Errorf("%s: %d bytes in the filesystem (there: %v), %d written", name, len(b), ok, len(data))
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s is in the filesystem but was not written", name)
		}
	}
}

func TestFilesystem(t *testing.T) {
	needQEMU(t)
	needE2fs(t)
	cp := newCheckpointer(t)
	tree := fsTree(t)
	base := mkfs(t, tree)
	snap0 := cp.importBytes(t, mustRead(t, base))
	checkFS(t, base, tree)

	mod := filepath.Join(t.TempDir(), "mod.raw")
	if err := os.WriteFile(mod, mustRead(t, base), 0o600); err != nil {
		t.Fatal(err)
	}
	change(t, mod, tree)
	o := &testOverlay{path: diffOverlay(t, base, mod, snap0), disk: mustRead(t, mod)}
	n := len(cp.bee.Received())
	snap1, err := cp.run(t, o, false)
	if err != nil {
		t.Fatal(err)
	}
	s1 := cp.check(t, o, snap1, snap0, 0, cp.since(n))
	t.Logf("checkpoint: %d blocks changed, %d chunks", s1.Meta.Stats.ChangedBlocks, s1.Meta.Stats.NewChunks)

	got, img, _ := cp.exportFresh(t, snap1.String())
	if !got.Equal(snap1) || !bytes.Equal(img, o.disk) {
		t.Fatal("the export differs from the changed image")
	}
	exported := filepath.Join(t.TempDir(), "exported.raw")
	if err := os.WriteFile(exported, img, 0o600); err != nil {
		t.Fatal(err)
	}
	checkFS(t, exported, tree)
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
