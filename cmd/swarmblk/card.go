package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/card"
	"github.com/petfold/swarmblk/internal/engine"
	"github.com/petfold/swarmblk/internal/epoch"
	"github.com/petfold/swarmblk/internal/lineage"
	"github.com/petfold/swarmblk/internal/snapshot"
	"github.com/petfold/swarmblk/internal/store"
)

// A disk card restores a machine from the bare network (SPEC §12): card
// makes one, and boot runs the machine it describes with QEMU, on a
// writable disk made from it.

// maxBlob bounds the UEFI variables and TPM state a card carries.
const maxBlob = 64 << 20

// Files that boot keeps in a disk's vm directory, which card takes as its
// defaults.
const (
	vmDir     = "vm"
	vmFile    = "vm.json"
	nvramFile = "nvram.fd"
	tpmDir    = "tpm"
)

func cmdCard(args []string) {
	fs := flag.NewFlagSet("card", flag.ExitOnError)
	beeURL := fs.String("bee", envOr("SWARMBLK_BEE_URL", "http://localhost:1633"), "Bee API `url` ($SWARMBLK_BEE_URL if set)")
	batch := fs.String("batch", os.Getenv("SWARMBLK_BATCH"), "immutable postage batch `id` to stamp the machine's files with ($SWARMBLK_BATCH if set)")
	dir := fs.String("home", home(), "local state `directory` ($SWARMBLK_HOME if set)")
	var cfg cardConfig
	fs.StringVar(&cfg.disk, "disk", "", "make the card for the writable disk with this `name`, following its lineage")
	fs.StringVar(&cfg.name, "name", "", "the machine's `name` on the card, which boot gives its disk (default the disk's name)")
	fs.StringVar(&cfg.vm, "vm", "", "the VM definition, a swarmblk-vm/1 JSON `file` (default the disk's vm/vm.json)")
	fs.StringVar(&cfg.nvram, "nvram", "", "the machine's UEFI variables `file` (default the disk's vm/nvram.fd, if any)")
	fs.StringVar(&cfg.vtpm, "vtpm", "", "the `directory` of the machine's swtpm state (default the disk's vm/tpm, if any)")
	out := fs.String("o", "", "write the card to this `file` (default <name>.card)")
	fs.Parse(args)
	if fs.NArg() > 1 || (fs.NArg() == 1) == (cfg.disk != "") || *batch == "" {
		usage()
	}
	cfg.arg = fs.Arg(0)
	s, err := store.Open(filepath.Join(*dir, "chunks"))
	if err != nil {
		log.Fatal(err)
	}
	c, err := makeCard(context.Background(), bee.New(*beeURL, 64), s, *dir, *batch, cfg, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}
	path := *out
	if path == "" {
		path = c.Name + ".card"
	}
	if err := c.Write(path); err != nil {
		log.Fatal(err)
	}
	fmt.Println(path)
}

// cardConfig says what a card is for.
type cardConfig struct {
	arg, disk       string // a lineage or snapshot, or else a writable disk
	name            string // the machine's name, if not the disk's
	vm, nvram, vtpm string // the machine's definition, UEFI variables and TPM state
}

// makeCard makes the card of a machine (SPEC §12): it uploads the machine's
// definition, UEFI variables and TPM state, encrypted, stamped with batch,
// and waits until they are synced. The card follows a lineage named in
// cfg.arg, stays on a snapshot named there, or, for the disk cfg.disk in
// the local state directory home, follows the lineage that the disk's
// checkpoints continue, if this host writes it. For such a disk, the files
// default to those boot left in its vm directory. It reports to w.
func makeCard(ctx context.Context, c *bee.Client, s *store.Store, home, batch string, cfg cardConfig, w io.Writer) (card.Card, error) {
	get := s.Through(retry(c.Chunk))
	var snap snapshot.Snapshot
	var follow *snapshot.Lineage
	var err error
	var nvramFrom, vtpmFrom string // what boot left, for a machine that has them
	name := cfg.name
	switch {
	case cfg.disk != "":
		if !diskName.MatchString(cfg.disk) {
			return card.Card{}, fmt.Errorf("disk name %q: use letters, digits, dots, dashes and underscores", cfg.disk)
		}
		dir := filepath.Join(home, "disks", cfg.disk)
		base, err := diskBase(dir)
		if err != nil {
			return card.Card{}, fmt.Errorf("disk %s: %w", cfg.disk, err)
		}
		if snap, err = snapshot.Read(ctx, get, base); err != nil {
			return card.Card{}, err
		}
		if m := snap.Meta.Lineage; m != nil {
			if l, _, err := lineage.FromMeta(*m); err == nil {
				if _, err := lineage.LoadKey(filepath.Join(home, "keys"), l.Owner); err == nil {
					follow = m
				}
			}
		}
		if follow == nil {
			fmt.Fprintf(w, "card: disk %s sits on snapshot %s, in no lineage this host writes, so the card stays on it; its next checkpoint starts a lineage, and a card made after that follows it\n", cfg.disk, snap.Ref)
		}
		if name == "" {
			name = cfg.disk
		}
		for _, f := range []struct {
			flag *string
			path string
		}{{&cfg.vm, vmFile}, {&nvramFrom, nvramFile}, {&vtpmFrom, tpmDir}} {
			if p := filepath.Join(dir, vmDir, f.path); *f.flag == "" && exists(p) {
				*f.flag = p
			}
		}
	default:
		if snap, err = resolve(ctx, c, get, cfg.arg, w); err != nil {
			return card.Card{}, err
		}
		if strings.Contains(cfg.arg, "/") {
			follow = snap.Meta.Lineage
		}
	}
	switch {
	case name == "":
		return card.Card{}, errors.New("name the machine with -name")
	case !diskName.MatchString(name):
		return card.Card{}, fmt.Errorf("machine name %q: use letters, digits, dots, dashes and underscores", name)
	case cfg.vm == "":
		return card.Card{}, errors.New("name the VM definition with -vm")
	}
	b, err := os.ReadFile(cfg.vm)
	if err != nil {
		return card.Card{}, err
	}
	vm, err := card.ParseVM(b)
	if err != nil {
		return card.Card{}, fmt.Errorf("%s: %w", cfg.vm, err)
	}
	def, err := json.MarshalIndent(vm, "", "  ")
	if err != nil {
		return card.Card{}, err
	}
	if cfg.nvram == "" && vm.Firmware == "uefi" {
		cfg.nvram = nvramFrom
	}
	if cfg.vtpm == "" && vm.TPM != "" {
		cfg.vtpm = vtpmFrom
	}
	var nvram, vtpm []byte
	switch {
	case cfg.nvram != "" && vm.Firmware != "uefi":
		return card.Card{}, errors.New("UEFI variables are for a machine with UEFI firmware")
	case cfg.nvram != "":
		if nvram, err = readBlob(cfg.nvram); err != nil {
			return card.Card{}, err
		}
	case vm.Firmware == "uefi":
		fmt.Fprintf(w, "card: no UEFI variables, so the machine starts from the firmware's defaults\n")
	}
	switch {
	case cfg.vtpm != "" && vm.TPM == "":
		return card.Card{}, errors.New("TPM state is for a machine with a TPM")
	case cfg.vtpm != "":
		if vtpm, err = card.PackDir(cfg.vtpm); err != nil {
			return card.Card{}, err
		}
		if len(vtpm) > maxBlob {
			return card.Card{}, fmt.Errorf("%s: TPM state of %d bytes, more than %d", cfg.vtpm, len(vtpm), maxBlob)
		}
	case vm.TPM != "":
		fmt.Fprintf(w, "card: no TPM state, so the machine gets a new TPM, which cannot unseal what the old one sealed\n")
	}

	// The blobs' chunks, each blob's root after the rest of it.
	mem := inmemchunkstore.New()
	var (
		mu    sync.Mutex
		order []swarm.Address
		seen  = map[string]bool{}
	)
	put := putterFunc(func(ctx context.Context, ch swarm.Chunk) error {
		mu.Lock()
		defer mu.Unlock()
		if k := ch.Address().ByteString(); !seen[k] {
			seen[k] = true
			order = append(order, ch.Address())
		}
		return mem.Put(ctx, ch)
	})
	blob := func(data []byte) (*swarm.Address, error) {
		if data == nil {
			return nil, nil
		}
		ref, err := card.Put(ctx, put, data)
		if err != nil {
			return nil, err
		}
		root := swarm.NewAddress(ref.Bytes()[:swarm.HashSize])
		if i := slices.IndexFunc(order, root.Equal); i >= 0 {
			order = append(slices.Delete(order, i, i+1), root)
		}
		return &ref, nil
	}
	var blobs card.Blobs
	for _, b := range []struct {
		data []byte
		ref  **swarm.Address
	}{{nvram, &blobs.NVRAM}, {vtpm, &blobs.VTPM}} {
		if *b.ref, err = blob(b.data); err != nil {
			return card.Card{}, err
		}
	}
	ref, err := blob(def)
	if err != nil {
		return card.Card{}, err
	}
	blobs.Definition = *ref
	if err := checkBatch(ctx, c, batch); err != nil {
		return card.Card{}, err
	}
	fmt.Fprintf(w, "card: uploading the machine's definition%s%s, %d chunks\n", map[bool]string{true: ", UEFI variables"}[nvram != nil], map[bool]string{true: ", TPM state"}[vtpm != nil], len(order))
	if err := uploadFrom(ctx, c, mem, batch, order, w); err != nil {
		return card.Card{}, err
	}
	batches := slices.Clone(snap.Meta.Batches)
	if !slices.Contains(batches, batch) {
		batches = append(batches, batch)
	}
	cd := card.Card{
		Format:   card.Format,
		Name:     name,
		Created:  time.Now().UTC().Truncate(time.Second),
		Lineage:  follow,
		Snapshot: snap.Ref,
		Batches:  batches,
		VM:       blobs,
	}
	if follow != nil {
		l, _, _ := lineage.FromMeta(*follow)
		fmt.Fprintf(w, "card: machine %s, following lineage %s from update %d, snapshot %s\n", name, l, follow.Index, snap.Ref)
	} else {
		fmt.Fprintf(w, "card: machine %s, on snapshot %s\n", name, snap.Ref)
	}
	return cd, nil
}

// readBlob reads a file for a card, of at most maxBlob bytes.
func readBlob(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err == nil && len(b) > maxBlob {
		err = fmt.Errorf("%s has %d bytes, more than %d", path, len(b), maxBlob)
	}
	return b, err
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// diskBase returns the snapshot that the disk in dir sits on: the parent of
// its newest epoch, which takes the writes. It reads only superblocks, so
// it works while a serve holds the disk. An error wraps os.ErrNotExist if
// the disk has no epochs.
func diskBase(dir string) (swarm.Address, error) {
	entries, err := os.ReadDir(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return swarm.ZeroAddress, err
	}
	var newest uint64
	var base swarm.Address
	for _, e := range entries {
		n, err := strconv.ParseUint(e.Name(), 10, 64)
		if err != nil || !e.IsDir() || n < newest {
			continue
		}
		if s, err := epoch.ReadSuper(filepath.Join(dir, e.Name())); err == nil {
			newest, base = n, s.Parent
		}
	}
	if base.IsZero() {
		return swarm.ZeroAddress, fmt.Errorf("no disk in %s: %w", dir, os.ErrNotExist)
	}
	return base, nil
}

func cmdBoot(args []string) {
	fs := flag.NewFlagSet("boot", flag.ExitOnError)
	beeURL := fs.String("bee", envOr("SWARMBLK_BEE_URL", "http://localhost:1633"), "Bee API `url` ($SWARMBLK_BEE_URL if set)")
	dir := fs.String("home", home(), "local state `directory` ($SWARMBLK_HOME if set)")
	var cfg bootConfig
	fs.StringVar(&cfg.disk, "disk", "", "run the machine on the writable disk with this `name` (default the card's name)")
	fs.StringVar(&cfg.at, "snapshot", "", "start a new disk on this `snapshot` or lineage, instead of the card's")
	fs.StringVar(&cfg.batch, "batch", os.Getenv("SWARMBLK_BATCH"), "immutable postage batch `id` that the disk's checkpoints stamp with ($SWARMBLK_BATCH if set)")
	fs.DurationVar(&cfg.every, "every", 0, "checkpoint the disk at this `interval`, if it changed")
	fs.StringVar(&cfg.policy, "policy", "", "the disk's hydration `policy`: lazy, hydrate or restamp (default the disk's own, and hydrate for a new disk)")
	cache := fs.String("cache", "4G", "bound the local chunk cache to this `size`; 0 for no bound")
	fs.StringVar(&cfg.failure, "failure", failureBlock, "when a block cannot be fetched: `block` the read and raise an alarm, or return an error")
	fs.StringVar(&cfg.alarm, "alarm", "", "run this shell `command` when an alarm is raised or cleared, with the message in $SWARMBLK_ALARM")
	fs.StringVar(&cfg.qemu, "qemu", "", "QEMU `binary` (default qemu-system-<arch>, for the machine's arch)")
	fs.StringVar(&cfg.firmware, "firmware", "", "UEFI firmware code `file` (default OVMF's, where distributions install it)")
	fs.StringVar(&cfg.swtpm, "swtpm", "swtpm", "swtpm `binary`, for a machine with a TPM")
	fs.StringVar(&cfg.display, "display", "serial", "how QEMU shows the machine: `serial`, its serial console on this terminal, or a QEMU -display value such as gtk, vnc=:1 or none")
	fs.StringVar(&cfg.nic, "nic", "user", "QEMU network `backend` for the machine's NIC, such as user or bridge,br=br0")
	fs.Parse(args)
	if fs.NArg() < 1 {
		usage()
	}
	cfg.card, cfg.extra = fs.Arg(0), fs.Args()[1:]
	if len(cfg.extra) > 0 && cfg.extra[0] == "--" {
		cfg.extra = cfg.extra[1:]
	}
	var err error
	if cfg.cache, err = parseSize(*cache); err != nil {
		log.Fatal(err)
	}
	cfg.home = *dir
	cfg.stdin, cfg.stdout, cfg.stderr = os.Stdin, os.Stdout, os.Stderr
	s, err := store.Open(filepath.Join(*dir, "chunks"))
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := boot(ctx, bee.New(*beeURL, engine.Fetchers), s, cfg, os.Stderr); err != nil {
		log.Fatal(err)
	}
}

// bootConfig says which machine boot runs, and how.
type bootConfig struct {
	card           string        // the card's file
	home           string        // the local state directory
	disk           string        // the disk's name, if not the card's
	at             string        // what a new disk starts on, if not what the card says
	batch          string        // what the disk's checkpoints stamp with
	every          time.Duration // how often to checkpoint the disk, if at all
	policy         string        // the disk's hydration policy, if given
	cache          int64         // the bound on the chunk cache, in bytes, or 0
	failure, alarm string        // serve's failure policy and alarm command
	qemu, swtpm    string        // binaries
	firmware       string        // UEFI firmware code, if not OVMF's
	display, nic   string        // QEMU's display, and the NIC's network backend
	extra          []string      // more arguments for QEMU

	stdin          io.Reader // QEMU's standard input and output
	stdout, stderr io.Writer
}

// powerdownWait is how long boot lets a machine shut down when told to
// stop, before it stops QEMU.
var powerdownWait = 2 * time.Minute

// boot runs the machine of a disk card with QEMU, until it stops. The
// machine runs on a writable disk in the local state directory, which serve
// exports: the card's name, or cfg.disk. A new disk starts on the lineage
// head that the card follows, or on the card's snapshot, hydrating unless
// cfg.policy says otherwise; a disk that exists runs as it is. The
// machine's UEFI variables and TPM state come from the card the first time
// and stay in the disk's vm directory. When the machine stops by itself,
// boot checkpoints the disk if it has a batch. On ctx's end it asks the
// machine to shut down, and stops QEMU after powerdownWait. It reports to
// w.
func boot(ctx context.Context, c *bee.Client, s *store.Store, cfg bootConfig, w io.Writer) error {
	w = &syncWriter{w: w} // serve writes to it too
	if fi, err := os.Stat(cfg.card); err == nil && fi.Mode().Perm()&0o077 != 0 {
		fmt.Fprintf(w, "boot: %s has mode %04o, but a card holds keys: keep it private (chmod 600)\n", cfg.card, fi.Mode().Perm())
	}
	b, err := os.ReadFile(cfg.card)
	if err != nil {
		return err
	}
	cd, err := card.Parse(b)
	if err != nil {
		return fmt.Errorf("%s: %w", cfg.card, err)
	}
	get := s.Through(retry(c.Chunk))
	def, err := card.Get(ctx, get, cd.VM.Definition, 1<<20)
	if err != nil {
		return fmt.Errorf("the machine's definition: %w", err)
	}
	vm, err := card.ParseVM(def)
	if err != nil {
		return err
	}
	name := cfg.disk
	if name == "" {
		name = cd.Name
	}
	if !diskName.MatchString(name) {
		return fmt.Errorf("disk name %q: use letters, digits, dots, dashes and underscores", name)
	}
	bin := cfg.qemu
	if bin == "" {
		bin = "qemu-system-" + vm.Arch
	}
	if _, err := exec.LookPath(bin); err != nil {
		return err
	}
	if vm.TPM != "" {
		if _, err := exec.LookPath(cfg.swtpm); err != nil {
			return fmt.Errorf("the machine has a TPM: %w", err)
		}
	}

	disks := filepath.Join(cfg.home, "disks")
	dir := filepath.Join(disks, name)
	arg := ""
	if _, err := diskBase(dir); errors.Is(err, os.ErrNotExist) {
		if arg = cfg.at; arg == "" {
			if arg, err = cardStart(ctx, c, get, cd, w); err != nil {
				return err
			}
		}
		if cfg.policy == "" {
			cfg.policy = policyHydrate
		}
	} else if err != nil {
		return err
	} else if cfg.at != "" {
		return fmt.Errorf("disk %s exists: -snapshot is for a new disk", name)
	} else {
		fmt.Fprintf(w, "boot: disk %s exists, so the machine runs on it as it is\n", name)
	}

	// The machine's files: what the card holds, unless an earlier boot left
	// them, since the machine changes them as it runs.
	vmdir := filepath.Join(dir, vmDir)
	if err := os.MkdirAll(vmdir, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(vmdir, vmFile), append(def, '\n'), 0o600); err != nil {
		return err
	}
	var p qemuPaths
	if vm.Firmware == "uefi" {
		p.nvram = filepath.Join(vmdir, nvramFile)
		var vars []byte
		if !exists(p.nvram) && cd.VM.NVRAM != nil {
			if vars, err = card.Get(ctx, get, *cd.VM.NVRAM, maxBlob); err != nil {
				return fmt.Errorf("the machine's UEFI variables: %w", err)
			}
		}
		size := int64(len(vars))
		if fi, err := os.Stat(p.nvram); err == nil {
			size = fi.Size()
		}
		var template string
		if p.code, template, err = uefiFirmware(vm.SecureBoot, cfg.firmware, size); err != nil {
			return err
		}
		if vars == nil && !exists(p.nvram) {
			fmt.Fprintf(w, "boot: the card holds no UEFI variables, so the machine starts from %s\n", template)
			if vars, err = os.ReadFile(template); err != nil {
				return err
			}
		}
		if vars != nil {
			if err := writeNew(p.nvram, func(tmp string) error { return os.WriteFile(tmp, vars, 0o600) }); err != nil {
				return err
			}
		}
	}
	if vm.TPM != "" {
		state := filepath.Join(vmdir, tpmDir)
		if !exists(state) {
			var packed []byte
			if cd.VM.VTPM != nil {
				if packed, err = card.Get(ctx, get, *cd.VM.VTPM, maxBlob); err != nil {
					return fmt.Errorf("the machine's TPM state: %w", err)
				}
			} else {
				fmt.Fprintf(w, "boot: the card holds no TPM state, so the machine gets a new TPM\n")
			}
			if err := writeNew(state, func(tmp string) error { return card.UnpackDir(packed, tmp) }); err != nil {
				return err
			}
		}
	}

	// serve exports the disk until QEMU is done with it.
	sctx, stopServe := context.WithCancel(context.WithoutCancel(ctx))
	defer stopServe()
	scfg := serveConfig{disk: name, arg: arg, disks: disks, batch: cfg.batch, keys: filepath.Join(cfg.home, "keys"), every: cfg.every, policy: cfg.policy, cache: cfg.cache, failure: cfg.failure, alarm: cfg.alarm, sock: filepath.Join(dir, "nbd.sock"), window: 16384}
	if vm.Agent {
		p.agent = filepath.Join(vmdir, "qga.sock")
		scfg.qga = p.agent
	}
	pr, pw := io.Pipe()
	served := make(chan error, 1)
	go func() {
		served <- serve(sctx, c, s, scfg, pw, w)
		pw.Close()
	}()
	line, err := bufio.NewReader(pr).ReadString('\n')
	if err != nil {
		return <-served
	}
	go io.Copy(io.Discard, pr)
	p.disk = strings.TrimSpace(line)
	stop := func() error {
		stopServe()
		return <-served
	}

	if vm.TPM != "" {
		p.tpm = filepath.Join(vmdir, "swtpm.sock")
		tpm, err := startTPM(cfg.swtpm, filepath.Join(vmdir, tpmDir), p.tpm, filepath.Join(vmdir, "swtpm.log"), w)
		if err != nil {
			stop()
			return err
		}
		defer tpm()
	}
	p.qmp = filepath.Join(vmdir, "qmp.sock")
	args := qemuArgs(name, vm, p, kvmUsable(vm.Arch), cfg.display, cfg.nic, cfg.extra)
	fmt.Fprintf(w, "boot: %s %s\n", bin, strings.Join(args, " "))
	cmd := exec.Command(bin, args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = cfg.stdin, cfg.stdout, cfg.stderr
	start := time.Now()
	if err := cmd.Start(); err != nil {
		stop()
		return err
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	var qerr error
	select {
	case qerr = <-exited:
	case <-ctx.Done():
		qerr = shutDown(cmd, p.qmp, exited, w)
	}
	fmt.Fprintf(w, "boot: the machine stopped after %v\n", time.Since(start).Round(time.Second))
	serr := stop()
	if serr == nil && qerr == nil && ctx.Err() == nil && cfg.batch != "" {
		// With serve gone, the checkpoint opens the disk itself, and asks no
		// guest agent to freeze a machine that has stopped.
		fmt.Fprintf(w, "boot: checkpointing disk %s\n", name)
		if _, err := checkpointNamedDisk(context.Background(), c, s, cfg.home, name, cfg.batch, w); err != nil {
			fmt.Fprintf(w, "boot: the checkpoint failed, so the writes stay in the disk's epochs until the next one: %v\n", err)
		}
	}
	if qerr != nil {
		return fmt.Errorf("%s: %w", bin, qerr)
	}
	return serr
}

// cardStart returns what a new disk from card cd starts on: the head of the
// lineage it follows, which must be no older than the card, or else the
// card's snapshot.
func cardStart(ctx context.Context, c *bee.Client, get storage.Getter, cd card.Card, w io.Writer) (string, error) {
	if cd.Lineage == nil {
		return cd.Snapshot.String(), nil
	}
	l, index, err := lineage.FromMeta(*cd.Lineage)
	if err != nil {
		return "", err
	}
	head, err := resolve(ctx, c, get, l.String(), w)
	if err != nil {
		return "", fmt.Errorf("%w; to boot the card's own snapshot instead, of %s, add -snapshot %s", err, cd.Created.Format(time.RFC3339), cd.Snapshot)
	}
	if head.Meta.Lineage.Index < index {
		return "", fmt.Errorf("lineage %s: the head reads as update %d, older than the card's update %d, so Bee's lookup lags; try again, or add -snapshot %s", l, head.Meta.Lineage.Index, index, cd.Snapshot)
	}
	return head.Ref.String(), nil
}

// writeNew makes path with write, which writes a file or directory at the
// temporary path it is given, and then renames that into place.
func writeNew(path string, write func(tmp string) error) error {
	tmp := path + ".new"
	os.RemoveAll(tmp)
	if err := write(tmp); err != nil {
		os.RemoveAll(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// ovmfDirs are where distributions install OVMF, x86_64's UEFI firmware.
var ovmfDirs = []string{"/usr/share/OVMF", "/usr/share/edk2/ovmf", "/usr/share/edk2/x64", "/usr/share/qemu"}

// ovmfBuilds pairs OVMF's code files with the templates of their variables,
// which only that build reads, without and with Secure Boot.
var ovmfBuilds = map[bool][][2]string{
	false: {{"OVMF_CODE_4M.fd", "OVMF_VARS_4M.fd"}, {"OVMF_CODE.4m.fd", "OVMF_VARS.4m.fd"}, {"OVMF_CODE.fd", "OVMF_VARS.fd"}},
	true:  {{"OVMF_CODE_4M.secboot.fd", "OVMF_VARS_4M.ms.fd"}, {"OVMF_CODE.secboot.4m.fd", "OVMF_VARS.4m.fd"}, {"OVMF_CODE.secboot.fd", "OVMF_VARS.secboot.fd"}},
}

// uefiFirmware returns the UEFI firmware code for a machine, and the
// template of its variables. The code is code if not empty, without a
// template. Otherwise it is OVMF's, from the first build found whose
// template has vars bytes, or any, if vars is 0.
func uefiFirmware(secure bool, code string, vars int64) (string, string, error) {
	if code != "" {
		if vars == 0 {
			return "", "", errors.New("the machine has no UEFI variables yet, so it needs OVMF's own template: leave out -firmware")
		}
		return code, "", nil
	}
	for _, dir := range ovmfDirs {
		for _, build := range ovmfBuilds[secure] {
			c, t := filepath.Join(dir, build[0]), filepath.Join(dir, build[1])
			fi, err := os.Stat(t)
			if err == nil && exists(c) && (vars == 0 || fi.Size() == vars) {
				return c, t, nil
			}
		}
	}
	if vars != 0 {
		return "", "", fmt.Errorf("no OVMF build here reads UEFI variables of %d bytes; name its code file with -firmware", vars)
	}
	return "", "", errors.New("no OVMF firmware here: install OVMF, or name its code file with -firmware")
}

// startTPM starts swtpm with the TPM state in dir and its control socket at
// sock, and waits for the socket. It returns a function that stops it.
func startTPM(bin, dir, sock, logFile string, w io.Writer) (func(), error) {
	os.Remove(sock)
	cmd := exec.Command(bin, "socket", "--tpm2", "--tpmstate", "dir="+dir, "--ctrl", "type=unixio,path="+sock, "--log", "file="+logFile, "--terminate")
	cmd.Stdout, cmd.Stderr = w, w
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	stop := func() {
		cmd.Process.Signal(syscall.SIGTERM)
		<-exited
	}
	for deadline := time.Now().Add(10 * time.Second); !exists(sock); {
		select {
		case err := <-exited:
			return nil, fmt.Errorf("swtpm stopped at once: %v; see %s", err, logFile)
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			stop()
			return nil, fmt.Errorf("swtpm made no socket in 10 s; see %s", logFile)
		}
	}
	return stop, nil
}

// shutDown asks the machine to shut down through QEMU's monitor and waits
// powerdownWait for QEMU to exit; then it stops QEMU, and kills it if it
// still runs 10 s later. It returns QEMU's exit.
func shutDown(cmd *exec.Cmd, qmp string, exited chan error, w io.Writer) error {
	if err := qmpPowerdown(qmp); err != nil {
		fmt.Fprintf(w, "boot: asking the machine to shut down: %v\n", err)
	} else {
		fmt.Fprintf(w, "boot: asked the machine to shut down; waiting up to %v\n", powerdownWait)
		select {
		case err := <-exited:
			return err
		case <-time.After(powerdownWait):
		}
	}
	cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-exited:
		return err
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
		return <-exited
	}
}

// qmpPowerdown sends system_powerdown, the ACPI power button, to QEMU's
// monitor on the Unix socket at path.
func qmpPowerdown(path string) error {
	conn, err := net.DialTimeout("unix", path, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	dec := json.NewDecoder(conn)
	var greeting map[string]json.RawMessage
	if err := dec.Decode(&greeting); err != nil {
		return fmt.Errorf("QEMU's monitor: %w", err)
	}
	for _, command := range []string{"qmp_capabilities", "system_powerdown"} {
		if _, err := fmt.Fprintf(conn, "{\"execute\": %q}\n", command); err != nil {
			return err
		}
		for { // events may come first
			var r struct {
				Return json.RawMessage
				Error  *struct{ Class, Desc string }
			}
			if err := dec.Decode(&r); err != nil {
				return fmt.Errorf("QEMU's monitor: %s: %w", command, err)
			}
			if r.Error != nil {
				return fmt.Errorf("QEMU's monitor: %s: %s", command, r.Error.Desc)
			}
			if r.Return != nil {
				break
			}
		}
	}
	return nil
}

// kvmUsable reports whether QEMU can run a machine of arch with KVM here.
func kvmUsable(arch string) bool {
	if map[string]string{"amd64": "x86_64", "arm64": "aarch64"}[runtime.GOARCH] != arch {
		return false
	}
	f, err := os.OpenFile("/dev/kvm", os.O_RDWR, 0)
	if err != nil {
		return false
	}
	f.Close()
	return true
}

// qemuPaths are what a machine's QEMU connects to.
type qemuPaths struct {
	disk        string // the disk's NBD URI
	code, nvram string // UEFI firmware and variables
	tpm         string // swtpm's control socket
	agent       string // the guest agent's socket
	qmp         string // QEMU's monitor socket
}

// qemuArgs returns QEMU's arguments for machine name, defined by vm: with
// KVM, if kvm, the display, the NIC on network backend nic, and extra
// arguments last.
func qemuArgs(name string, vm card.VM, p qemuPaths, kvm bool, display, nic string, extra []string) []string {
	machine := vm.Machine
	if vm.SecureBoot {
		machine += ",smm=on"
	}
	accel, cpu := "tcg", "max"
	if kvm {
		accel, cpu = "kvm", "host"
	}
	if vm.CPU != "" {
		cpu = vm.CPU
	}
	a := []string{"-name", name, "-machine", machine, "-accel", accel, "-cpu", cpu, "-smp", strconv.Itoa(vm.CPUs), "-m", strconv.Itoa(vm.Memory)}
	if vm.Firmware == "uefi" {
		a = append(a, "-drive", "if=pflash,format=raw,unit=0,readonly=on,file="+qemuEscape(p.code), "-drive", "if=pflash,format=raw,unit=1,file="+qemuEscape(p.nvram))
		if vm.SecureBoot {
			a = append(a, "-global", "driver=cfi.pflash01,property=secure,value=on")
		}
	}
	a = append(a, "-drive", "file="+qemuEscape(p.disk)+",if=none,id=disk0,format=raw,discard=unmap,detect-zeroes=unmap")
	switch vm.Disk {
	case "virtio":
		a = append(a, "-device", "virtio-blk-pci,drive=disk0,bootindex=1")
	case "scsi":
		a = append(a, "-device", "virtio-scsi-pci,id=scsi0", "-device", "scsi-hd,drive=disk0,bus=scsi0.0,bootindex=1")
	case "sata":
		a = append(a, "-device", "ide-hd,drive=disk0,bus=ide.0,bootindex=1")
	}
	if vm.NIC != nil {
		n := nic + ",model=" + vm.NIC.Model
		if vm.NIC.MAC != "" {
			n += ",mac=" + vm.NIC.MAC
		}
		a = append(a, "-nic", n)
	} else {
		a = append(a, "-nic", "none")
	}
	if p.tpm != "" {
		a = append(a, "-chardev", "socket,id=chrtpm,path="+qemuEscape(p.tpm), "-tpmdev", "emulator,id=tpm0,chardev=chrtpm", "-device", vm.TPM+",tpmdev=tpm0")
	}
	if p.agent != "" {
		a = append(a, "-chardev", "socket,id=qga0,path="+qemuEscape(p.agent)+",server=on,wait=off", "-device", "virtio-serial", "-device", "virtserialport,chardev=qga0,name=org.qemu.guest_agent.0")
	}
	a = append(a, "-qmp", "unix:"+qemuEscape(p.qmp)+",server=on,wait=off")
	if display == "serial" {
		a = append(a, "-nographic")
	} else {
		a = append(a, "-display", display)
	}
	a = append(a, vm.Args...)
	return append(a, extra...)
}

// qemuEscape escapes the commas in a value of a QEMU option list.
func qemuEscape(s string) string { return strings.ReplaceAll(s, ",", ",,") }
