// Swarmblk keeps virtual disks on Ethereum Swarm (docs/DESIGN.md). This is
// the Phase 1 command line:
//
//	swarmblk import [-bee url] [-batch id] [-home dir] image.raw
//	swarmblk checkpoint [-bee url] [-batch id] [-home dir] [-branch] overlay.qcow2
//	swarmblk restore [-bee url] [-home dir] lineage|snapshot overlay.qcow2
//	swarmblk export [-bee url] [-home dir] lineage|snapshot image.raw
//	swarmblk diff [-bee url] [-home dir] lineage|snapshot lineage|snapshot
//
// import uploads a raw disk image as a format-1 tree (SPEC §2) in a first
// snapshot. checkpoint turns a stopped VM's qcow2 overlay into the next
// snapshot after the one it sits on, and publishes it in a lineage feed
// (SPEC §7). restore makes a new overlay on a snapshot, which QEMU reads
// from Bee, and export writes a snapshot's disk to a raw image. These four
// print the snapshot's reference. diff prints the runs of 4 KiB blocks that
// differ between two disks of the same size, each as its first block and the
// number of blocks. A lineage is given as 0x<owner>/<topic> and stands for
// its latest snapshot. The defaults come from $SWARMBLK_BEE_URL,
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
	default:
		usage()
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  swarmblk import [-bee url] [-batch id] [-home dir] image.raw
  swarmblk checkpoint [-bee url] [-batch id] [-home dir] [-branch] overlay.qcow2
  swarmblk restore [-bee url] [-home dir] lineage|snapshot overlay.qcow2
  swarmblk export [-bee url] [-home dir] lineage|snapshot image.raw
  swarmblk diff [-bee url] [-home dir] lineage|snapshot lineage|snapshot
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
