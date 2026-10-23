//go:build !windows

package spool

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockStore takes an exclusive, non-blocking flock on dir/LOCK for
// the store's lifetime. Double opens fail with ErrLocked instead of
// corrupting data.
func lockStore(dir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dir, lockFileName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("spool: lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("spool: %w", ErrLocked)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}
