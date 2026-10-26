package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// TestFailure hides a leaf of a served snapshot from Bee. Under the block
// policy a read of it waits, raising the alarm, and gets its block once
// the leaf is back, clearing the alarm. Under the error policy the read
// fails.
func TestFailure(t *testing.T) {
	needQEMU(t)
	defer func(d time.Duration) { stallWait = d }(stallWait)
	stallWait = 100 * time.Millisecond
	cp := newCheckpointer(t)
	snap0, img := cp.importDisk(t)
	b := -1
	for i := 0; i < len(img)/swarm.ChunkSize; i++ {
		if !bytes.Equal(img[i*swarm.ChunkSize:(i+1)*swarm.ChunkSize], make([]byte, swarm.ChunkSize)) {
			b = i
			break
		}
	}
	leaf, err := cac.New(img[b*swarm.ChunkSize : (b+1)*swarm.ChunkSize])
	if err != nil {
		t.Fatal(err)
	}
	cp.bee.Hide(leaf.Address(), true)
	alarms := filepath.Join(t.TempDir(), "alarms")
	alarmed := func(state string) bool {
		got, _ := os.ReadFile(alarms)
		return strings.Contains(string(got), state)
	}
	read := func(uri string) (string, error) {
		out, err := exec.Command("qemu-io", "-r", "-f", "raw", "-c", fmt.Sprintf("read %d 512", b*swarm.ChunkSize), uri).CombinedOutput()
		return string(out), err
	}

	uri, stop := cp.startServeWith(t, serveConfig{arg: snap0.String(), alarm: "echo $SWARMBLK_ALARM_STATE >> " + alarms})
	done := make(chan string, 1)
	go func() {
		out, err := read(uri)
		done <- fmt.Sprint(out, err)
	}()
	waitFor(t, "the alarm", func() bool { return alarmed("raised") })
	select {
	case out := <-done:
		t.Fatalf("the read did not wait for the hidden leaf: %s", out)
	default:
	}
	cp.bee.Hide(leaf.Address(), false)
	if out := <-done; !strings.Contains(out, "read 512/512 bytes") {
		t.Errorf("the read after the leaf came back: %s", out)
	}
	waitFor(t, "the alarm to clear", func() bool { return alarmed("cleared") })
	if err, log := stop(); err != nil || !strings.Contains(log, "ALARM: block "+fmt.Sprint(b)) || !strings.Contains(log, "reads go through again") {
		t.Errorf("serve ended with %v:\n%s", err, log)
	}

	cp.bee.Hide(leaf.Address(), true)
	uri, stop = cp.startServeWith(t, serveConfig{arg: snap0.String(), failure: failureError})
	if out, err := read(uri); err == nil && !strings.Contains(out, "failed") {
		t.Errorf("a read under the error policy did not fail: %s", out)
	}
	stop()
}

// TestStallsGone ends reads whose client is gone: one that never waited
// raises no alarm, and the last that waited clears it.
func TestStallsGone(t *testing.T) {
	defer func(d time.Duration) { stallWait = d }(stallWait)
	stallWait = time.Millisecond
	var log bytes.Buffer
	st := &stalls{what: "disk vm", w: &log}
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	failed := errors.New("no peer has it")
	if err := st.stall(gone, 7, context.Canceled, 1); err == nil || log.Len() != 0 {
		t.Fatalf("a read that never waited: %v, log %q", err, log.String())
	}
	if err := st.stall(context.Background(), 7, failed, 1); err != nil || !strings.Contains(log.String(), "ALARM: block 7") {
		t.Fatalf("a read that failed: %v, log %q", err, log.String())
	}
	if err := st.stall(gone, 7, context.Canceled, 2); err == nil || !strings.Contains(log.String(), "clients are gone") || st.waiting != 0 {
		t.Errorf("the read's client went: %v, %d waiting, log %q", err, st.waiting, log.String())
	}
}
