// Runtime-dir lifecycle, failure-artifact retention, and the RESULT line.
//
// Convention: each scenario owns tests-live/runtime/<scenario/> (or
// $MURMUR_LIVE_RUNTIME/<scenario>/). On failure the whole directory moves
// to tests-live/failures/<UTC-stamp>-<scenario>/; on pass it is removed.
// Every scenario prints exactly one RESULT: ... line.
package goharness

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// TestsLiveDir returns the tests-live/ directory, derived from this
// package's source location.
func TestsLiveDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("goharness: cannot locate source file")
	}
	// <tests-live>/goharness/<thisfile> -> <tests-live>
	return filepath.Dir(filepath.Dir(file))
}

// RuntimeDir returns the runtime dir for scenario, honoring
// MURMUR_LIVE_RUNTIME.
func RuntimeDir(scenario string) string {
	if root := os.Getenv("MURMUR_LIVE_RUNTIME"); root != "" {
		return filepath.Join(root, scenario)
	}
	return filepath.Join(TestsLiveDir(), "runtime", scenario)
}

// FailuresDir returns the failure-artifact root, honoring
// MURMUR_LIVE_FAILURES.
func FailuresDir() string {
	if root := os.Getenv("MURMUR_LIVE_FAILURES"); root != "" {
		return root
	}
	return filepath.Join(TestsLiveDir(), "failures")
}

// PrepareRuntimeDir creates a fresh empty runtime dir for scenario
// (removing any previous run).
func PrepareRuntimeDir(scenario string) (string, error) {
	dir := RuntimeDir(scenario)
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, nil
}

// Finish retains-or-cleans runtimeDir and prints the RESULT line.
// Infallible by contract.
func Finish(scenario, runtimeDir string, failed bool) {
	if failed {
		failures := FailuresDir()
		_ = os.MkdirAll(failures, 0o755)
		dest := filepath.Join(failures, UTCStamp()+"-"+scenario)
		if err := os.Rename(runtimeDir, dest); err != nil {
			fmt.Printf("RESULT: FAIL scenario=%s | artifact retention failed (%v); runtime left at %s\n", scenario, err, runtimeDir)
		} else {
			fmt.Printf("RESULT: FAIL scenario=%s | retained artifacts at %s\n", scenario, dest)
		}
	} else {
		_ = os.RemoveAll(runtimeDir)
		fmt.Printf("RESULT: PASS scenario=%s (runtime cleaned up)\n", scenario)
	}
}

// UTCStamp returns the current UTC time as YYYYMMDDTHHMMSSZ.
func UTCStamp() string {
	return time.Now().UTC().Format("20060102T150405Z")
}
