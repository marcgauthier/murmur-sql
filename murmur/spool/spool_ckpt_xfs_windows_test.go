//go:build windows

package spool_test

// isCrossDevice on Windows never matches: the cross-filesystem
// test skips without /dev/shm before reaching the probe.
func isCrossDevice(err error) bool {
	return false
}
