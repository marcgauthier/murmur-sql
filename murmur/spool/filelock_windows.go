//go:build windows

package spool

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// lockStore takes an exclusive lock on dir/LOCK for the store's
// lifetime. Double opens fail with ErrLocked instead of corrupting
// data.
func lockStore(dir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dir, lockFileName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("spool: lock file: %w", err)
	}
	handle := windows.Handle(f.Fd())
	var olap windows.Overlapped
	err = windows.LockFileEx(handle,
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, 1, 0, &olap)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("spool: %w", ErrLocked)
	}
	return func() {
		_ = windows.UnlockFileEx(handle, 0, 1, 0, &olap)
		f.Close()
	}, nil
}
