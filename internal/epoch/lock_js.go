//go:build js

package epoch

import (
	"errors"
	"os"
)

// errLocked means that another process holds the lock.
var errLocked = errors.New("locked")

// lockFile does nothing in a browser, which has no flock, and whose one
// process cannot race itself.
func lockFile(*os.File) error { return nil }
