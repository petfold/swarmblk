//go:build unix

package epoch

import (
	"errors"
	"os"
	"syscall"
)

// errLocked means that another process holds the lock.
var errLocked = errors.New("locked")

// lockFile takes an exclusive flock on f without waiting, which closing f
// releases.
func lockFile(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return errLocked
	}
	return err
}
