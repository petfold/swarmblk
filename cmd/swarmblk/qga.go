package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"time"
)

// guestAgent sends a command to the QEMU guest agent on the Unix socket at
// path and returns its answer (SPEC §8, step 1). It syncs first, as the
// agent's protocol asks: a 0xFF byte resets the agent's parser, and the
// answer to guest-sync-delimited with a random ID follows a 0xFF byte.
// Whatever comes before that answer was left by an earlier client.
func guestAgent(ctx context.Context, path, command string) (json.RawMessage, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(time.Minute)
	}
	conn.SetDeadline(deadline)
	type answer struct {
		Return json.RawMessage
		Error  *struct{ Class, Desc string }
	}
	send := func(cmd map[string]any) error {
		b, err := json.Marshal(cmd)
		if err == nil {
			_, err = conn.Write(append(b, '\n'))
		}
		return err
	}
	r := bufio.NewReader(conn)
	id := rand.Int64N(1 << 50)
	if _, err := conn.Write([]byte{0xff}); err != nil {
		return nil, err
	}
	if err := send(map[string]any{"execute": "guest-sync-delimited", "arguments": map[string]any{"id": id}}); err != nil {
		return nil, err
	}
	for {
		if _, err := r.ReadBytes(0xff); err != nil {
			return nil, fmt.Errorf("guest agent: %w", err)
		}
		line, err := r.ReadBytes('\n')
		if err != nil {
			return nil, fmt.Errorf("guest agent: %w", err)
		}
		var a answer
		var got int64
		if json.Unmarshal(line, &a) == nil && json.Unmarshal(a.Return, &got) == nil && got == id {
			break
		}
	}
	if err := send(map[string]any{"execute": command}); err != nil {
		return nil, err
	}
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("guest agent: %s: %w", command, err)
	}
	var a answer
	if err := json.Unmarshal(line, &a); err != nil {
		return nil, fmt.Errorf("guest agent: %s: %w", command, err)
	}
	if a.Error != nil {
		return nil, fmt.Errorf("guest agent: %s: %s: %s", command, a.Error.Class, a.Error.Desc)
	}
	return a.Return, nil
}

// guestFreeze freezes the guest's filesystems, which flushes them first.
func guestFreeze(ctx context.Context, path string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, err := guestAgent(ctx, path, "guest-fsfreeze-freeze")
	return err
}

// guestThaw thaws them. It must follow every freeze, even a failed one,
// which may have frozen some, and does no harm on filesystems not frozen.
func guestThaw(ctx context.Context, path string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	_, err := guestAgent(ctx, path, "guest-fsfreeze-thaw")
	return err
}
