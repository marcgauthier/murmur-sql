//go:build !windows

package spool_test

import (
	"errors"
	"syscall"
)

// isCrossDevice reports whether err is an EXDEV cross-device link
// failure.
func isCrossDevice(err error) bool {
	return errors.Is(err, syscall.EXDEV)
}
