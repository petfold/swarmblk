package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

// What a read does when Swarm cannot deliver a block, after the fetch's own
// retries (DESIGN §3, fail visibly):
const (
	failureBlock = "block" // wait and try again, and raise an alarm
	failureError = "error" // return an I/O error to the guest
)

// stallWait is the longest wait between a blocked read's tries.
var stallWait = 30 * time.Second

// stalls holds the reads that cannot fetch their blocks, under the block
// failure policy: each waits and tries again, after 1, 2, 4 … and at most
// stallWait seconds, until it gets its block or its client goes. The guest
// sees a slow disk rather than an I/O error, which ext4, for one, answers
// by remounting read-only. stalls raises an alarm when the first read
// starts waiting, repeats it every minute, and clears it when no read
// waits any more: a line on w, and the shell command alarm, if not empty,
// with the message in $SWARMBLK_ALARM and raised or cleared in
// $SWARMBLK_ALARM_STATE.
type stalls struct {
	what  string // the disk, for the messages
	alarm string
	w     io.Writer

	mu      sync.Mutex
	waiting int       // reads waiting now
	since   time.Time // since the first of them began
	told    time.Time // when the alarm was last repeated
}

// stall is an engine.Stall.
func (st *stalls) stall(ctx context.Context, b int64, err error, failures int) error {
	if err == nil { // the read got its block
		st.done("reads go through again")
		return nil
	}
	if ctx.Err() != nil { // the client is gone: the read ends, and waited if it failed before
		if failures > 1 {
			st.done("no read waits for Swarm any more, since their clients are gone")
		}
		return ctx.Err()
	}
	st.mu.Lock()
	if failures == 1 {
		st.waiting++
		if st.waiting == 1 {
			st.since, st.told = time.Now(), time.Now()
			st.mu.Unlock()
			st.raise("raised", fmt.Sprintf("%s: ALARM: block %d cannot be fetched from Swarm: %v; the guest waits until it can", st.what, b, err))
			return st.wait(ctx, failures)
		}
	}
	if time.Since(st.told) >= time.Minute {
		st.told = time.Now()
		msg := fmt.Sprintf("%s: ALARM: %d reads have waited %v for Swarm: %v", st.what, st.waiting, time.Since(st.since).Round(time.Second), err)
		st.mu.Unlock()
		st.raise("raised", msg)
	} else {
		st.mu.Unlock()
	}
	return st.wait(ctx, failures)
}

// wait waits before a blocked read's next try. A read whose client is gone
// waits no more.
func (st *stalls) wait(ctx context.Context, failures int) error {
	select {
	case <-ctx.Done():
		st.done("no read waits for Swarm any more, since their clients are gone")
		return ctx.Err()
	case <-time.After(min(stallWait, time.Second<<min(failures-1, 5))):
		return nil
	}
}

// done counts a read that no longer waits, and clears the alarm, saying
// why, after the last.
func (st *stalls) done(why string) {
	st.mu.Lock()
	st.waiting--
	last := st.waiting == 0
	waited := time.Since(st.since).Round(time.Second)
	st.mu.Unlock()
	if last {
		st.raise("cleared", fmt.Sprintf("%s: %s, after %v", st.what, why, waited))
	}
}

// raise reports an alarm's message and runs the alarm command, in the
// background.
func (st *stalls) raise(state, msg string) {
	fmt.Fprintf(st.w, "serve: %s\n", msg)
	if st.alarm == "" {
		return
	}
	cmd := exec.Command("sh", "-c", st.alarm)
	cmd.Env = append(os.Environ(), "SWARMBLK_ALARM="+msg, "SWARMBLK_ALARM_STATE="+state)
	go func() {
		if out, err := cmd.CombinedOutput(); err != nil {
			fmt.Fprintf(st.w, "serve: the alarm command failed: %v: %s\n", err, out)
		}
	}()
}
