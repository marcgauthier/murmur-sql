//go:build !windows

package spool

import (
	"fmt"
	"syscall"
)

// freeBytes reports the bytes available to the caller on dir's
// filesystem.
func freeBytes(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, fmt.Errorf("spool: statfs %s: %w", dir, err)
	}
	if st.Bavail < 0 || st.Bsize < 0 {
		return 0, nil
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}
