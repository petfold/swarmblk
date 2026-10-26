package engine

import (
	"errors"
	"os"
	"syscall"
)

// punch makes n bytes of f from off on a hole that reads as zeros, keeping
// the file's size, or writes zeros where the filesystem cannot.
func punch(f *os.File, off, n int64) error {
	const keepSize, punchHole = 0x01, 0x02 // FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE
	err := syscall.Fallocate(int(f.Fd()), keepSize|punchHole, off, n)
	if errors.Is(err, syscall.EOPNOTSUPP) {
		return writeZeros(f, off, n)
	}
	return err
}

// holes reports whether the bytes of f from pos on are a hole, and where
// that hole or run of data ends, at most at end. Where the filesystem
// cannot tell, everything is data.
func holes(f *os.File, pos, end int64) (bool, int64) {
	const seekData, seekHole = 3, 4 // SEEK_DATA, SEEK_HOLE
	data, err := f.Seek(pos, seekData)
	switch {
	case errors.Is(err, syscall.ENXIO): // nothing but a hole to the end
		return true, end
	case err != nil:
		return false, end
	case data > pos:
		return true, min(data, end)
	}
	hole, err := f.Seek(pos, seekHole)
	if err != nil {
		return false, end
	}
	return false, min(hole, end)
}
