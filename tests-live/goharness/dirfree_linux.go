//go:build linux

package goharness

import (
	"syscall"
)

// DirFreeBytes returns the free bytes on the filesystem holding path
// (statvfs f_bavail * f_frsize), or false when the probe is
// unavailable. Disk-pressure legs size their filler files off this so a
// run never risks the host filesystem.
func DirFreeBytes(path string) (uint64, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, false
	}
	bsize := st.Frsize
	if bsize <= 0 {
		bsize = st.Bsize
	}
	if bsize <= 0 {
		return 0, false
	}
	return uint64(st.Bavail) * uint64(bsize), true
}
