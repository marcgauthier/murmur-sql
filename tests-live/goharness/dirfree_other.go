//go:build !linux

package goharness

// DirFreeBytes reports the probe unavailable off linux (callers degrade
// to an unprobed filler size, matching the Rust non-unix fallback).
func DirFreeBytes(path string) (uint64, bool) {
	return 0, false
}
