//go:build windows

package spool

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// freeBytes reports the bytes available to the caller on dir's
// volume.
func freeBytes(dir string) (uint64, error) {
	p, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return 0, fmt.Errorf("spool: volume path: %w", err)
	}
	var avail uint64
	if err := windows.GetDiskFreeSpaceEx(p, &avail, nil, nil); err != nil {
		return 0, fmt.Errorf("spool: free space: %w", err)
	}
	return avail, nil
}
