// Runtime-dir lifecycle, failure-artifact retention, and the RESULT line.
//
// Convention: each scenario owns tests-live/runtime/<scenario/> (or
// $MURMUR_LIVE_RUNTIME/<scenario>/). On failure the whole directory moves
// to tests-live/failures/<UTC-stamp>-<scenario>/; on pass it is removed.
// Every scenario prints exactly one RESULT: ... line.
package harness

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
		panic("harness: cannot locate source file")
	}
	// <tests-live>/harness/<thisfile> -> <tests-live>
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
			// Cross-device runtimes (e.g. MURMUR_LIVE_RUNTIME on
			// /tmp) cannot rename; copy and remove instead.
			if cerr := copyDir(runtimeDir, dest); cerr != nil {
				fmt.Printf("RESULT: FAIL scenario=%s | artifact retention failed (%v); runtime left at %s\n", scenario, cerr, runtimeDir)
			} else {
				_ = os.RemoveAll(runtimeDir)
				fmt.Printf("RESULT: FAIL scenario=%s | retained artifacts at %s\n", scenario, dest)
			}
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

// copyDir recursively copies src into dst (created fresh).
func copyDir(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, info.Mode()); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		s, d := filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())
		if e.IsDir() {
			if err := copyDir(s, d); err != nil {
				return err
			}
			continue
		}
		data, err := os.ReadFile(s)
		if err != nil {
			return err
		}
		if err := os.WriteFile(d, data, 0o600); err != nil {
			return err
		}
	}
	return nil
}
