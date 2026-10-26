// Swarmblk keeps virtual disks on Ethereum Swarm (docs/DESIGN.md). Its
// command line:
//
//	swarmblk import [-bee url] [-batch id] [-home dir] image.raw
//	swarmblk checkpoint [-bee url] [-batch id] [-home dir] [-branch] [-prefetch file] overlay.qcow2 | -disk name
//	swarmblk restore [-bee url] [-home dir] lineage|snapshot overlay.qcow2
//	swarmblk export [-bee url] [-home dir] lineage|snapshot image.raw
//	swarmblk diff [-bee url] [-home dir] lineage|snapshot lineage|snapshot
//	swarmblk serve [-bee url] [-home dir] [-socket path] [-cache size] [-failure policy] [-alarm command] [-iolock] [-record file] [-prefetch file] [-window blocks] [-nocache] lineage|snapshot | -root root
//	swarmblk serve [-bee url] [-home dir] [-socket path] [-cache size] [-failure policy] [-alarm command] [-iolock] [-batch id] [-every interval] [-qga socket] [-policy policy] ... -disk name [lineage|snapshot]
//	swarmblk card [-bee url] [-batch id] [-home dir] [-name name] [-vm file] [-nvram file] [-vtpm dir] [-o file] -disk name | lineage|snapshot
//	swarmblk boot [-bee url] [-home dir] [-disk name] [-snapshot lineage|snapshot] [-batch id] [-every interval] [-policy policy] [-qemu binary] [-firmware file] [-swtpm binary] [-display how] [-nic backend] ... card [-- qemu-arguments]
//	swarmblk iolock -- command [arguments]
//
// import uploads a raw disk image as a format-1 tree (SPEC §2) in a first
// snapshot. checkpoint turns a stopped VM's qcow2 overlay into the next
// snapshot after the one it sits on, and publishes it in a lineage feed
// (SPEC §7). restore makes a new overlay on a snapshot, which QEMU reads
// from Bee, and export writes a snapshot's disk to a raw image. These four
// print the snapshot's reference. diff prints the runs of 4 KiB blocks that
// differ between two disks of the same size, each as its first block and the
// number of blocks. serve exports a snapshot's disk, or the disk tree under
// a root, read-only over NBD on a Unix socket, through the engine (DESIGN
// §3), and prints the URI for QEMU. It prefetches from the snapshot's boot
// trace and can record a new one, which checkpoint -prefetch stores with the
// next snapshot (SPEC §6). With -disk it serves a named disk writable
// instead, keeping its writes in write epochs in the local state directory
// (SPEC §11), and takes live checkpoints of it (SPEC §8): every -every, or
// when checkpoint -disk asks, freezing the guest's filesystems through the
// guest agent if -qga names its socket. A disk's -policy says whether it
// keeps a whole local copy (hydrate), stamps every chunk again under the
// batch (restamp), or neither (lazy), and serve keeps the chunk cache under
// -cache (DESIGN §3). A read that Swarm cannot serve waits and raises an
// alarm, or with -failure error fails. card makes a disk card (SPEC §12),
// which names a disk's lineage and holds its machine's definition, UEFI
// variables and TPM state, and boot runs the card's machine with QEMU on a
// writable disk that it serves. iolock runs a command, such as Bee, as an
// I/O flusher, as serve -iolock runs serve, for a host whose kernel mounts
// the disk. A lineage is given as 0x<owner>/<topic> and stands for its
// latest snapshot. The defaults come from $SWARMBLK_BEE_URL,
// $SWARMBLK_BATCH and $SWARMBLK_HOME.
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("swarmblk: ")
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "import":
		cmdImport(os.Args[2:])
	case "checkpoint":
		cmdCheckpoint(os.Args[2:])
	case "restore":
		cmdRestore(os.Args[2:])
	case "export":
		cmdExport(os.Args[2:])
	case "diff":
		cmdDiff(os.Args[2:])
	case "serve":
		cmdServe(os.Args[2:])
	case "card":
		cmdCard(os.Args[2:])
	case "boot":
		cmdBoot(os.Args[2:])
	case "iolock":
		if err := cmdIOLock(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	default:
		usage()
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  swarmblk import [-bee url] [-batch id] [-home dir] image.raw
  swarmblk checkpoint [-bee url] [-batch id] [-home dir] [-branch] [-prefetch file] overlay.qcow2 | -disk name
  swarmblk restore [-bee url] [-home dir] lineage|snapshot overlay.qcow2
  swarmblk export [-bee url] [-home dir] lineage|snapshot image.raw
  swarmblk diff [-bee url] [-home dir] lineage|snapshot lineage|snapshot
  swarmblk serve [-bee url] [-home dir] [-socket path] [-cache size] [-failure policy] [-alarm command] [-iolock] [-record file] [-prefetch file] [-window blocks] [-nocache] lineage|snapshot | -root root
  swarmblk serve [-bee url] [-home dir] [-socket path] [-cache size] [-failure policy] [-alarm command] [-iolock] [-batch id] [-every interval] [-qga socket] [-policy policy] ... -disk name [lineage|snapshot]
  swarmblk card [-bee url] [-batch id] [-home dir] [-name name] [-vm file] [-nvram file] [-vtpm dir] [-o file] -disk name | lineage|snapshot
  swarmblk boot [-bee url] [-home dir] [-disk name] [-snapshot lineage|snapshot] [-batch id] [-every interval] [-policy policy] [-qemu binary] [-firmware file] [-swtpm binary] [-display how] [-nic backend] ... card [-- qemu-arguments]
  swarmblk iolock -- command [arguments]
`)
	os.Exit(2)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// home returns swarmblk's local state directory (SPEC §10).
func home() string {
	if h := os.Getenv("SWARMBLK_HOME"); h != "" {
		return h
	}
	dir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	return filepath.Join(dir, ".local", "share", "swarmblk")
}
