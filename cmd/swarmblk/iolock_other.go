//go:build !linux

package main

import "errors"

var errIOLock = errors.New("-iolock and iolock need Linux")

func ioLock() error { return errIOLock }

func cmdIOLock([]string) error { return errIOLock }
