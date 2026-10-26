//go:build linux

package main

import (
	"strings"
	"testing"
)

// TestIOLock checks that -iolock and iolock refuse, with advice, in a
// process without the capabilities they need, which the tests are.
func TestIOLock(t *testing.T) {
	caps, err := effectiveCaps()
	if err != nil {
		t.Fatal(err)
	}
	if caps&(1<<capSysResource) != 0 {
		t.Skip("the tests run with CAP_SYS_RESOURCE")
	}
	for name, err := range map[string]error{"-iolock": ioLock(), "iolock": cmdIOLock([]string{"--", "true"})} {
		if err == nil || !strings.Contains(err.Error(), "setcap") {
			t.Errorf("%s without capabilities: %v", name, err)
		}
	}
	if ioFlusher() {
		t.Error("the test process is an I/O flusher")
	}
}
