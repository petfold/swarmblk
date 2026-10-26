//go:build !linux

package engine

import "os"

// punch writes n zero bytes to f from off on.
func punch(f *os.File, off, n int64) error { return writeZeros(f, off, n) }

// holes reports everything as data.
func holes(_ *os.File, _, end int64) (bool, int64) { return false, end }
