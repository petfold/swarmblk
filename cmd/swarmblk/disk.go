package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"syscall"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"

	"github.com/petfold/swarmblk/internal/bee"
	"github.com/petfold/swarmblk/internal/engine"
	"github.com/petfold/swarmblk/internal/epoch"
	"github.com/petfold/swarmblk/internal/lineage"
	"github.com/petfold/swarmblk/internal/snapshot"
	"github.com/petfold/swarmblk/internal/store"
)

// A writable disk lives in a directory of numbered write epochs (SPEC §11):
// the active one, which takes the writes, and, while a checkpoint runs or
// after one stopped, a frozen one under it.

var diskName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// diskState is an open writable disk.
type diskState struct {
	name, dir string
	lock      *os.File          // the disk's lock, held while it is open
	base      snapshot.Snapshot // under the oldest epoch
	disk      *engine.Disk
	policy    string        // lazy, hydrate or restamp
	img       *engine.Image // the local copy of a hydrating disk
	moved     chan struct{} // told when a checkpoint moves the disk to its next snapshot
	stall     engine.Stall  // what the disk's reads do when a block cannot be fetched, if set

	mu sync.Mutex // one checkpoint at a time
}

// close closes the disk's epochs and its local copy, and releases it.
func (ds *diskState) close() error {
	err := ds.disk.Close()
	if ds.img != nil {
		if cerr := ds.img.Close(); err == nil {
			err = cerr
		}
	}
	ds.lock.Close()
	return err
}

// lockDisk takes the lock of the disk in dir, an exclusive flock on its
// lock file, which closing the file releases.
func lockDisk(name, dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("disk %s is in use", name)
		}
		return nil, err
	}
	return f, nil
}

// openDisk opens the disk name in the directory disks and locks it: its
// epochs, recovered after a crash, on the snapshot under the oldest one. A
// frozen epoch that a finished checkpoint replaced, a retired one and what
// an unfinished epoch.Create left are removed. A disk that does not exist
// yet gets a first epoch on the snapshot or lineage arg, and for one that
// does, arg must name the snapshot it is on, if anything. The disk takes
// policy, if not empty, as its hydration policy, and a hydrating disk gets
// its local copy. s keeps the chunks, which get reads from through Bee.
func openDisk(ctx context.Context, c *bee.Client, s *store.Store, get storage.Getter, disks, name, arg, policy string, w io.Writer) (_ *diskState, err error) {
	if !diskName.MatchString(name) {
		return nil, fmt.Errorf("disk name %q: use letters, digits, dots, dashes and underscores", name)
	}
	dir := filepath.Join(disks, name)
	if _, err := os.Stat(dir); os.IsNotExist(err) && arg == "" {
		return nil, fmt.Errorf("disk %s is new: name the snapshot or lineage it starts from", name)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	lock, err := lockDisk(name, dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			lock.Close()
		}
	}()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var numbers []uint64
	for _, e := range entries {
		if n, err := strconv.ParseUint(e.Name(), 10, 64); err == nil && e.IsDir() {
			numbers = append(numbers, n)
		}
	}
	slices.Sort(numbers)
	var named snapshot.Snapshot
	if arg != "" {
		if named, err = resolve(ctx, c, get, arg, w); err != nil {
			return nil, err
		}
	}

	var eps []*epoch.Epoch
	closeAll := func() {
		for _, ep := range eps {
			ep.Close()
		}
	}
	for _, n := range numbers {
		epdir := filepath.Join(dir, strconv.FormatUint(n, 10))
		if epoch.Abandoned(epdir) {
			os.RemoveAll(epdir)
			continue
		}
		ep, err := epoch.Open(epdir)
		if err != nil {
			closeAll()
			return nil, err
		}
		if ep.Super().State == epoch.Retired {
			ep.Close()
			os.RemoveAll(ep.Dir())
			continue
		}
		eps = append(eps, ep)
	}
	if len(eps) == 0 {
		if arg == "" {
			return nil, fmt.Errorf("disk %s is new: name the snapshot or lineage it starts from", name)
		}
		ep, err := epoch.Create(filepath.Join(dir, "1"), epoch.Super{Epoch: 1, Parent: named.Ref, Root: named.Meta.DiskRoot, Size: named.Meta.Size})
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(w, "disk %s: new, on snapshot %s\n", name, named.Ref)
		eps = append(eps, ep)
	}
	// A checkpoint that stopped after moving the epochs above its frozen
	// epoch onto the new snapshot leaves that epoch replaced.
	top := eps[len(eps)-1].Super()
	for len(eps) > 1 {
		s := eps[0].Super()
		if s.Snapshot == nil || !s.Snapshot.Equal(top.Parent) {
			break
		}
		eps[0].Close()
		os.RemoveAll(eps[0].Dir())
		eps = eps[1:]
	}
	parent := eps[0].Super().Parent
	for _, ep := range eps[1:] {
		if !ep.Super().Parent.Equal(parent) {
			closeAll()
			return nil, fmt.Errorf("disk %s: epoch %s sits on snapshot %s, epoch %s on %s", name, eps[0].Dir(), parent, ep.Dir(), ep.Super().Parent)
		}
	}
	if arg != "" && !named.Ref.Equal(parent) {
		closeAll()
		return nil, fmt.Errorf("disk %s is on snapshot %s, not %s", name, parent, named.Ref)
	}
	base, err := snapshot.Read(ctx, get, parent)
	if err != nil {
		closeAll()
		return nil, err
	}
	snap, err := engine.Open(ctx, get, base.Meta.DiskRoot, 0)
	if err != nil {
		closeAll()
		return nil, err
	}
	snap.SetLocal(s)
	if policy, err = diskPolicy(dir, name, policy, w); err != nil {
		closeAll()
		return nil, err
	}
	var img *engine.Image
	if policy == policyHydrate {
		if img, err = openImage(ctx, get, dir, base, eps, snap); err != nil {
			closeAll()
			return nil, err
		}
	}
	d, err := engine.NewDisk(snap, eps...)
	if err != nil {
		if img != nil {
			img.Close()
		}
		closeAll()
		return nil, err
	}
	return &diskState{name: name, dir: dir, lock: lock, base: base, disk: d, policy: policy, img: img, moved: make(chan struct{}, 1)}, nil
}

// checkpointDisk turns everything written to the disk so far into the next
// snapshot while the guest keeps writing (SPEC §8). It first finishes a
// frozen epoch that an earlier checkpoint left; then it cuts the writes
// atomically, freezing the active epoch under a new one, and finishes that.
// If qga names a guest agent's socket, the guest's filesystems are frozen
// around the cut, and the snapshot is consistent for applications, not
// only after a crash. It returns the disk's snapshot, which is still the
// base if nothing changed.
func checkpointDisk(ctx context.Context, ds *diskState, c *bee.Client, s *store.Store, get storage.Getter, keys, batch, qga string, w io.Writer) (swarm.Address, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if batch == "" {
		return swarm.ZeroAddress, errors.New("checkpoints need a batch to stamp with")
	}
	if err := checkBatch(ctx, c, batch); err != nil {
		return swarm.ZeroAddress, err
	}
	if frozen := ds.disk.Frozen(); len(frozen) > 0 {
		fmt.Fprintf(w, "disk %s: taking up the checkpoint of epoch %d\n", ds.name, frozen[0].Super().Epoch)
		if _, err := ds.finish(ctx, c, s, get, keys, batch, frozen[0], "", w); err != nil {
			return swarm.ZeroAddress, err
		}
	}
	active := ds.disk.Active()
	if len(active.Blocks()) == 0 {
		fmt.Fprintf(w, "disk %s: no writes since snapshot %s\n", ds.name, ds.base.Ref)
		return ds.base.Ref, nil
	}
	n := active.Super().Epoch + 1
	next, err := epoch.Create(filepath.Join(ds.dir, strconv.FormatUint(n, 10)), epoch.Super{Epoch: n, Parent: ds.base.Ref, Root: ds.base.Meta.DiskRoot, Size: ds.base.Meta.Size})
	if err != nil {
		return swarm.ZeroAddress, err
	}
	consistency := ""
	if qga != "" {
		if err := guestFreeze(ctx, qga); err != nil {
			fmt.Fprintf(w, "disk %s: the guest agent did not freeze the filesystems, so the snapshot is crash-consistent: %v\n", ds.name, err)
		} else {
			consistency = "application"
		}
	}
	ep, err := ds.disk.Freeze(next)
	if qga != "" { // even after a failed freeze, which may have frozen some
		if terr := guestThaw(ctx, qga); terr != nil {
			fmt.Fprintf(w, "disk %s: thawing the guest's filesystems: %v\n", ds.name, terr)
		}
	}
	if err != nil {
		next.Close()
		os.RemoveAll(next.Dir())
		return swarm.ZeroAddress, err
	}
	fmt.Fprintf(w, "disk %s: froze epoch %d with %d blocks\n", ds.name, ep.Super().Epoch, len(ep.Blocks()))
	return ds.finish(ctx, c, s, get, keys, batch, ep, consistency, w)
}

// finish turns the frozen epoch ep into the next snapshot: commit uploads
// its blocks as the next snapshot of the base's lineage and records the
// snapshot in ep before it is published, so that if the checkpoint stops
// after the upload, finish only publishes it the next time. Then promote
// puts the disk on the snapshot.
func (ds *diskState) finish(ctx context.Context, c *bee.Client, s *store.Store, get storage.Getter, keys, batch string, ep *epoch.Epoch, consistency string, w io.Writer) (swarm.Address, error) {
	var snap swarm.Address
	if done := ep.Super().Snapshot; done != nil {
		snap = *done
		up, err := snapshot.Read(ctx, get, snap)
		if err != nil {
			return swarm.ZeroAddress, err
		}
		if up.Meta.Lineage == nil {
			return swarm.ZeroAddress, fmt.Errorf("snapshot %s is in no lineage", snap)
		}
		l, i, err := lineage.FromMeta(*up.Meta.Lineage)
		if err != nil {
			return swarm.ZeroAddress, err
		}
		key, err := lineage.LoadKey(keys, l.Owner)
		if err != nil {
			return swarm.ZeroAddress, err
		}
		if _, err := publishedAt(ctx, c, batch, l, key, i, up, w); err != nil {
			return swarm.ZeroAddress, err
		}
	} else {
		var err error
		snap, err = commit(ctx, c, s, get, keys, batch, commitSpec{
			parent: ds.base,
			what:   "disk " + ds.name,
			blocks: func(fn func(map[int64][]byte) error) (int64, error) {
				var n int64
				err := ds.disk.Blocks(ctx, ep, window, func(blocks map[int64][]byte) error {
					n += int64(len(blocks))
					return fn(blocks)
				})
				return n, err
			},
			consistency: consistency,
			uploaded: func(snap swarm.Address) error {
				return ep.Update(func(s *epoch.Super) {
					s.State = epoch.Uploading
					s.Snapshot = &snap
				})
			},
		}, w)
		if err != nil {
			return swarm.ZeroAddress, err
		}
	}
	if err := ds.promote(ctx, get, s, ep, snap, batch); err != nil {
		return swarm.ZeroAddress, fmt.Errorf("snapshot %s is published, but disk %s still sits on %s: %w", snap, ds.name, ds.base.Ref, err)
	}
	return snap, nil
}

// checkpointNamedDisk checkpoints the writable disk name: through the
// control socket of the serve that runs it, with that serve's batch, or, if
// no serve runs it, here, with batch.
func checkpointNamedDisk(ctx context.Context, c *bee.Client, s *store.Store, home, name, batch string, w io.Writer) (swarm.Address, error) {
	if !diskName.MatchString(name) {
		return swarm.ZeroAddress, fmt.Errorf("disk name %q: use letters, digits, dots, dashes and underscores", name)
	}
	if conn, err := net.Dial("unix", filepath.Join(home, "disks", name, "control.sock")); err == nil {
		defer conn.Close()
		if err := json.NewEncoder(conn).Encode(controlMessage{Request: "checkpoint"}); err != nil {
			return swarm.ZeroAddress, err
		}
		dec := json.NewDecoder(conn)
		for {
			var m controlMessage
			if err := dec.Decode(&m); err != nil {
				return swarm.ZeroAddress, fmt.Errorf("disk %s: the serve that runs it: %w", name, err)
			}
			switch {
			case m.Error != "":
				return swarm.ZeroAddress, errors.New(m.Error)
			case m.Snapshot != "":
				return swarm.ParseHexAddress(m.Snapshot)
			default:
				fmt.Fprint(w, m.Log)
			}
		}
	}
	get := s.Through(retry(c.Chunk))
	ds, err := openDisk(ctx, c, s, get, filepath.Join(home, "disks"), name, "", "", w)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	snap, err := checkpointDisk(ctx, ds, c, s, get, filepath.Join(home, "keys"), batch, "", w)
	if cerr := ds.close(); err == nil {
		err = cerr
	}
	return snap, err
}

// promote puts the disk on snap, the published snapshot of the frozen epoch
// ep, which batch stamped (SPEC §8, step 8). A local copy moves to snap
// first, with ep's blocks. Then the epochs above ep move onto snap, and ep
// is retired, leaves the disk and is removed. A stop between the two leaves
// ep replaced, and openDisk removes it.
func (ds *diskState) promote(ctx context.Context, get storage.Getter, s *store.Store, ep *epoch.Epoch, snap swarm.Address, batch string) error {
	base := ds.base
	esnap := ds.disk.Snapshot()
	if !snap.Equal(base.Ref) {
		var err error
		if base, err = snapshot.Read(ctx, get, snap); err != nil {
			return err
		}
		if esnap, err = engine.Open(ctx, get, base.Meta.DiskRoot, 0); err != nil {
			return err
		}
		esnap.SetLocal(s)
		if ds.stall != nil {
			esnap.SetStall(ds.stall)
		}
		if ds.img != nil {
			old := ds.disk.Snapshot()
			old.SetImage(ds.img, base.Meta.DiskRoot) // still readable while the copy moves on
			err := ds.img.Advance(old.Root(), base.Meta.DiskRoot, func(put func(int64, []byte) error) error {
				return ds.disk.Blocks(ctx, ep, window, func(blocks map[int64][]byte) error {
					for b, data := range blocks {
						if err := put(b, data); err != nil {
							return err
						}
					}
					return nil
				})
			})
			if err != nil {
				return fmt.Errorf("moving the local copy: %w", err)
			}
			esnap.SetImage(ds.img, swarm.ZeroAddress)
		}
	}
	for _, above := range append(ds.disk.Frozen()[1:], ds.disk.Active()) {
		if err := above.Update(func(s *epoch.Super) {
			s.Parent = base.Ref
			s.Root = base.Meta.DiskRoot
		}); err != nil {
			return err
		}
	}
	if err := ep.SetState(epoch.Retired); err != nil {
		return err
	}
	if err := ds.disk.Promote(esnap, ep); err != nil {
		return err
	}
	if r, ok := ds.readRestamp(); ok && r.Snapshot.Equal(ds.base.Ref) {
		if !slices.Contains(r.Batches, batch) {
			r.Batches = append(r.Batches, batch)
		}
		r.Snapshot = base.Ref
		ds.writeRestamp(r) // without it, the next run re-stamps again
	}
	ds.base = base
	select {
	case ds.moved <- struct{}{}:
	default:
	}
	return os.RemoveAll(ep.Dir())
}
