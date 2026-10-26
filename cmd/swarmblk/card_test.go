package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/petfold/swarmblk/internal/card"
)

// fakeQEMU stands in for QEMU: it records its arguments in args beside it,
// checks that the TPM's socket is there, writes a block of 0x5a at the
// start of the disk through its NBD URI, and exits; or, with
// $FAKE_QEMU=hang, it waits to be stopped.
const fakeQEMU = `#!/bin/sh
dir=$(dirname "$0")
printf '%s\n' "$@" > "$dir/args.new" && mv "$dir/args.new" "$dir/args"
uri= tpm=
for a; do
	case $a in
	file=nbd*) uri=${a#file=}; uri=${uri%%,if=none*} ;;
	socket,id=chrtpm,path=*) tpm=${a#socket,id=chrtpm,path=} ;;
	esac
done
if [ -n "$tpm" ] && [ ! -e "$tpm" ]; then echo "no TPM socket at $tpm" >&2; exit 1; fi
if [ "$FAKE_QEMU" = hang ]; then exec sleep 60; fi
exec qemu-io -f raw -c "write -P 0x5a 0 4k" "$uri"
`

// fakeSwtpm stands in for swtpm: it lists the TPM state it was given in
// tpmstate beside it, makes its control socket's file, and waits to be
// stopped.
const fakeSwtpm = `#!/bin/sh
for a; do
	case $a in
	type=unixio,path=*) sock=${a#type=unixio,path=} ;;
	dir=*) state=${a#dir=} ;;
	esac
done
ls "$state" > "$(dirname "$0")/tpmstate"
: > "$sock"
exec sleep 60
`

// fakeTools writes the fake QEMU and swtpm into a directory and returns it.
func fakeTools(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for name, script := range map[string]string{"qemu": fakeQEMU, "swtpm": fakeSwtpm} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// TestCardBoot makes the card of a writable disk whose machine has UEFI
// firmware, a TPM and a guest agent, checkpoints the disk again, and boots
// the card on another host: a new state directory and store, through the
// same Bee. The new disk must start on the lineage's head, not the card's
// snapshot, and the machine must get its UEFI variables and TPM state.
// A second boot runs the disk as it is, keeping the machine's files, and
// stops the machine when told to.
func TestCardBoot(t *testing.T) {
	needQEMU(t)
	ctx := context.Background()
	cp, h := diskCheckpointer(t)
	snap0, img := cp.importDisk(t)
	uri, stop := h.serve(t, "vm", snap0.String())
	run(t, "qemu-io", "-f", "raw", "-c", "write -P 0x31 1M 4k", uri)
	snap1, log, err := h.checkpoint(t, "vm")
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}

	files := t.TempDir()
	vmPath, nvramPath, tpmPath := filepath.Join(files, "vm.json"), filepath.Join(files, "OVMF_VARS.fd"), filepath.Join(files, "tpm")
	nvram, permall := randomBytes(64<<10), randomBytes(9000)
	os.WriteFile(vmPath, []byte(`{"memory": 256, "firmware": "uefi", "tpm": "tpm-crb", "agent": true, "nic": {"model": "virtio-net-pci", "mac": "52:54:00:12:34:56"}}`), 0o600)
	os.WriteFile(nvramPath, nvram, 0o600)
	os.Mkdir(tpmPath, 0o700)
	os.WriteFile(filepath.Join(tpmPath, "tpm2-00.permall"), permall, 0o600)
	var clog bytes.Buffer
	cd, err := makeCard(ctx, cp.c, cp.s, h.home, testBatch, cardConfig{disk: "vm", vm: vmPath, nvram: nvramPath, vtpm: tpmPath}, &clog)
	if err != nil {
		t.Fatalf("%v\n%s", err, clog.String())
	}
	t.Log("\n" + clog.String())
	if cd.Name != "vm" || !cd.Snapshot.Equal(snap1) || cd.Lineage == nil || cd.Lineage.Index != 0 || !slices.Contains(cd.Batches, testBatch) || cd.VM.NVRAM == nil || cd.VM.VTPM == nil {
		t.Fatalf("the card: %+v", cd)
	}
	if got, err := card.Get(ctx, cp.bee.Store, *cd.VM.NVRAM, maxBlob); err != nil || !bytes.Equal(got, nvram) {
		t.Errorf("the UEFI variables on Swarm: %v", err)
	}

	// The disk moves on after the card.
	run(t, "qemu-io", "-f", "raw", "-c", "write -P 0x32 2M 4k", uri)
	snap2, log, err := h.checkpoint(t, "vm")
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	stop()
	want := slices.Clone(img)
	set(want, 1<<20, 4096, 0x31)
	set(want, 2<<20, 4096, 0x32)
	set(want, 0, 4096, 0x5a)

	cardPath := filepath.Join(t.TempDir(), "vm.card")
	if err := cd.Write(cardPath); err != nil {
		t.Fatal(err)
	}
	tools := fakeTools(t)
	code := filepath.Join(files, "OVMF_CODE.fd")
	os.WriteFile(code, []byte("firmware"), 0o600)
	home2, s2 := t.TempDir(), newStore(t)
	var out, blog bytes.Buffer
	cfg := bootConfig{card: cardPath, home: home2, batch: testBatch, qemu: filepath.Join(tools, "qemu"), swtpm: filepath.Join(tools, "swtpm"), firmware: code, display: "none", nic: "user", failure: failureBlock, stdout: &out, stderr: &out}
	if err := boot(ctx, cp.c, s2, cfg, &blog); err != nil {
		t.Fatalf("boot: %v\n%s\n%s", err, blog.String(), out.String())
	}
	t.Log("\n" + blog.String())
	args, _ := os.ReadFile(filepath.Join(tools, "args"))
	vmdir := filepath.Join(home2, "disks", "vm", "vm")
	for _, a := range []string{
		"-name\nvm\n",
		"\n-m\n256\n",
		"if=pflash,format=raw,unit=0,readonly=on,file=" + code + "\n",
		"if=pflash,format=raw,unit=1,file=" + filepath.Join(vmdir, "nvram.fd") + "\n",
		",if=none,id=disk0,format=raw,discard=unmap,detect-zeroes=unmap\n-device\nvirtio-blk-pci,drive=disk0,bootindex=1\n",
		"-nic\nuser,model=virtio-net-pci,mac=52:54:00:12:34:56\n",
		"-device\ntpm-crb,tpmdev=tpm0\n",
		"virtserialport,chardev=qga0,name=org.qemu.guest_agent.0\n",
		"-display\nnone\n",
	} {
		if !strings.Contains(string(args), a) {
			t.Errorf("QEMU's arguments lack %q:\n%s", a, args)
		}
	}
	if got, err := os.ReadFile(filepath.Join(vmdir, "nvram.fd")); err != nil || !bytes.Equal(got, nvram) {
		t.Errorf("the machine's UEFI variables: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(vmdir, "tpm", "tpm2-00.permall")); err != nil || !bytes.Equal(got, permall) {
		t.Errorf("the machine's TPM state: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(tools, "tpmstate")); string(got) != "tpm2-00.permall\n" {
		t.Errorf("swtpm found %q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(home2, "disks", "vm", "policy")); strings.TrimSpace(string(got)) != policyHydrate {
		t.Errorf("a new disk from a card has policy %q", got)
	}
	// The machine stopped by itself, so boot checkpointed the disk. The new
	// host holds no key of the lineage, so the checkpoint started another,
	// on the head that boot took.
	snap3, err := diskBase(filepath.Join(home2, "disks", "vm"))
	if err != nil || !strings.Contains(blog.String(), "boot: checkpointing disk vm") || strings.Contains(blog.String(), "guest agent") {
		t.Fatalf("after boot, the disk is on %s: %v", snap3, err)
	}
	if m := mustMeta(t, cp, snap3); m.Parent == nil || !m.Parent.Equal(snap2) || m.Lineage == nil || m.Lineage.Index != 0 || m.Lineage.Owner == cd.Lineage.Owner {
		t.Errorf("the new host's snapshot: parent %v, lineage %+v; want parent %s in a new lineage", m.Parent, m.Lineage, snap2)
	}
	if _, got, _ := cp.exportFresh(t, snap3.String()); !bytes.Equal(got, want) {
		t.Error("the new host's disk holds other bytes than the machine wrote")
	}
	// The new host's card follows its own lineage, with the files boot left.
	var log4 bytes.Buffer
	cd2, err := makeCard(ctx, cp.c, s2, home2, testBatch, cardConfig{disk: "vm"}, &log4)
	if err != nil {
		t.Fatalf("%v\n%s", err, log4.String())
	}
	if cd2.Lineage == nil || cd2.Lineage.Owner != mustMeta(t, cp, snap3).Lineage.Owner || !cd2.Snapshot.Equal(snap3) || cd2.VM.NVRAM == nil || cd2.VM.VTPM == nil {
		t.Errorf("the new host's card: %+v", cd2)
	}
	if got, err := card.Get(ctx, cp.bee.Store, *cd2.VM.VTPM, maxBlob); err != nil || !bytes.Contains(got, permall) {
		t.Errorf("the TPM state on the new host's card: %v", err)
	}

	// The machine changes its variables, and the disk exists now.
	os.WriteFile(filepath.Join(vmdir, "nvram.fd"), []byte("changed"), 0o600)
	os.Remove(filepath.Join(tools, "args"))
	t.Setenv("FAKE_QEMU", "hang")
	bctx, cancel := context.WithCancel(ctx)
	defer cancel()
	blog.Reset()
	done := make(chan error, 1)
	go func() { done <- boot(bctx, cp.c, s2, cfg, &blog) }()
	waitFor(t, "QEMU to start", func() bool { return exists(filepath.Join(tools, "args")) })
	cancel()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "terminated") {
		t.Errorf("a stopped boot returned %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(vmdir, "nvram.fd")); string(got) != "changed" {
		t.Error("a second boot replaced the machine's UEFI variables")
	}
	for _, s := range []string{"disk vm exists, so the machine runs on it as it is", "asking the machine to shut down"} {
		if !strings.Contains(blog.String(), s) {
			t.Errorf("the second boot's log lacks %q:\n%s", s, blog.String())
		}
	}
}

// TestCardRefuses checks what makeCard refuses.
func TestCardRefuses(t *testing.T) {
	cp, h := diskCheckpointer(t)
	snap0, _ := cp.importDisk(t)
	vm := filepath.Join(t.TempDir(), "vm.json")
	os.WriteFile(vm, []byte(`{"memory": 256}`), 0o600)
	nvram := filepath.Join(t.TempDir(), "vars.fd")
	os.WriteFile(nvram, []byte("vars"), 0o600)
	for _, c := range []struct {
		cfg  cardConfig
		want string
	}{
		{cardConfig{arg: snap0.String(), vm: vm}, "name the machine"},
		{cardConfig{arg: snap0.String(), name: "a/b", vm: vm}, "machine name"},
		{cardConfig{arg: snap0.String(), name: "vm"}, "VM definition"},
		{cardConfig{arg: snap0.String(), name: "vm", vm: vm, nvram: nvram}, "UEFI firmware"},
		{cardConfig{disk: "none", vm: vm}, "no disk"},
	} {
		var log bytes.Buffer
		if _, err := makeCard(context.Background(), cp.c, cp.s, h.home, testBatch, c.cfg, &log); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%+v: %v, want an error about %q", c.cfg, err, c.want)
		}
	}
	// A card on a snapshot stays on it.
	var log bytes.Buffer
	cd, err := makeCard(context.Background(), cp.c, cp.s, h.home, testBatch, cardConfig{arg: snap0.String(), name: "vm", vm: vm}, &log)
	if err != nil || cd.Lineage != nil || !cd.Snapshot.Equal(snap0) || cd.VM.NVRAM != nil {
		t.Errorf("a card on a snapshot: %+v, %v", cd, err)
	}
}

// TestUEFIFirmware finds OVMF builds by the size of their variables.
func TestUEFIFirmware(t *testing.T) {
	dir := t.TempDir()
	defer func(d []string) { ovmfDirs = d }(ovmfDirs)
	ovmfDirs = []string{filepath.Join(dir, "none"), dir}
	for name, size := range map[string]int{"OVMF_CODE_4M.fd": 1, "OVMF_VARS_4M.fd": 100, "OVMF_CODE.fd": 1, "OVMF_VARS.fd": 50} {
		os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o600)
	}
	for _, c := range []struct {
		secure     bool
		code       string
		vars       int64
		want, tmpl string
	}{
		{false, "", 0, "OVMF_CODE_4M.fd", "OVMF_VARS_4M.fd"},
		{false, "", 50, "OVMF_CODE.fd", "OVMF_VARS.fd"},
		{false, "", 77, "", ""},
		{true, "", 0, "", ""},
		{false, "/my/code.fd", 50, "/my/code.fd", ""},
		{false, "/my/code.fd", 0, "", ""},
	} {
		code, tmpl, err := uefiFirmware(c.secure, c.code, c.vars)
		if c.want == "" {
			if err == nil {
				t.Errorf("%+v: found %s", c, code)
			}
			continue
		}
		if err != nil || filepath.Base(code) != filepath.Base(c.want) || c.tmpl != "" && filepath.Base(tmpl) != c.tmpl {
			t.Errorf("%+v: %s, %s, %v", c, code, tmpl, err)
		}
	}
}

// TestQEMUArgs checks the arguments for buses, Secure Boot and no network.
func TestQEMUArgs(t *testing.T) {
	vm, err := card.ParseVM([]byte(`{"firmware": "uefi", "secure_boot": true, "disk": "sata", "cpu": "EPYC", "args": ["-vga", "std"]}`))
	if err != nil {
		t.Fatal(err)
	}
	a := strings.Join(qemuArgs("w,1", vm, qemuPaths{disk: "nbd+unix:///w?socket=/a,b/s", code: "c", nvram: "n", qmp: "q"}, true, "serial", "user", []string{"-snapshot"}), " ")
	for _, s := range []string{"-machine q35,smm=on", "-accel kvm -cpu EPYC", "property=secure,value=on", "file=nbd+unix:///w?socket=/a,,b/s,if=none", "ide-hd,drive=disk0,bus=ide.0", "-nic none", "-nographic -vga std -snapshot"} {
		if !strings.Contains(a, s) {
			t.Errorf("QEMU's arguments lack %q: %s", s, a)
		}
	}
	vm.Disk = "scsi"
	if a := strings.Join(qemuArgs("w", vm, qemuPaths{}, false, "gtk", "user", nil), " "); !strings.Contains(a, "scsi-hd,drive=disk0,bus=scsi0.0") || !strings.Contains(a, "-accel tcg -cpu EPYC") || !strings.Contains(a, "-display gtk") {
		t.Errorf("QEMU's arguments: %s", a)
	}
}

// TestShutDown asks a fake QEMU monitor to power the machine down: the
// machine, a sleep, stops when the monitor gets system_powerdown.
func TestShutDown(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "qmp.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	got := make(chan []string, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte(`{"QMP": {"version": {}, "capabilities": []}}` + "\n"))
		var cmds []string
		sc := bufio.NewScanner(conn)
		for sc.Scan() {
			var c struct{ Execute string }
			json.Unmarshal(sc.Bytes(), &c)
			cmds = append(cmds, c.Execute)
			conn.Write([]byte(`{"event": "SOMETHING"}` + "\n" + `{"return": {}}` + "\n"))
			if c.Execute == "system_powerdown" {
				cmd.Process.Kill()
				break
			}
		}
		got <- cmds
	}()
	var log bytes.Buffer
	start := time.Now()
	shutDown(cmd, sock, exited, &log)
	if cmds := <-got; !slices.Equal(cmds, []string{"qmp_capabilities", "system_powerdown"}) || time.Since(start) > 5*time.Second {
		t.Errorf("the monitor got %v; shutDown took %v:\n%s", cmds, time.Since(start), log.String())
	}
}
