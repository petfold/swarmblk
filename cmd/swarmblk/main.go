// Swarmblk keeps virtual disks on Ethereum Swarm (docs/DESIGN.md). This is
// the Phase 1 command line:
//
//	swarmblk import [-bee url] [-batch id] [-home dir] image.raw
//	swarmblk checkpoint [-bee url] [-batch id] [-home dir] [-branch] overlay.qcow2
//
// import uploads a raw disk image as a format-1 tree (SPEC §2) in a first
// snapshot, and prints the snapshot's reference. checkpoint turns a stopped
// VM's qcow2 overlay into the next snapshot after the one it sits on,
// publishes it in a lineage feed (SPEC §7), and prints its reference. The
// defaults come from $SWARMBLK_BEE_URL, $SWARMBLK_BATCH and $SWARMBLK_HOME.
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
	default:
		usage()
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  swarmblk import [-bee url] [-batch id] [-home dir] image.raw
  swarmblk checkpoint [-bee url] [-batch id] [-home dir] [-branch] overlay.qcow2
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
