package spool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// countOpenStoreFDs scans /proc/self/fd on Linux to verify that no open file
// descriptors point to files within storeDir.
func countOpenStoreFDs(storeDir string) int {
	fdDir := "/proc/self/fd"
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		// Not on Linux or /proc is unavailable; skip FD inspection.
		return 0
	}

	absStoreDir, err := filepath.Abs(storeDir)
	if err != nil {
		absStoreDir = storeDir
	}
	// Normalize symlinks if any
	if eval, err := filepath.EvalSymlinks(absStoreDir); err == nil {
		absStoreDir = eval
	}

	openCount := 0
	for _, entry := range entries {
		linkPath := filepath.Join(fdDir, entry.Name())
		target, err := os.Readlink(linkPath)
		if err != nil {
			continue
		}
		if evalTarget, err := filepath.EvalSymlinks(target); err == nil {
			target = evalTarget
		}
		if strings.HasPrefix(target, absStoreDir) {
			openCount++
		}
	}
	return openCount
}

// waitForGoroutines polls runtime.NumGoroutine until it falls back to <= baseline
// or the timeout expires.
func waitForGoroutines(baseline int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		runtime.Gosched()
		curr := runtime.NumGoroutine()
		if curr <= baseline {
			return curr
		}
		time.Sleep(10 * time.Millisecond)
	}
	return runtime.NumGoroutine()
}

// TestGoroutineAndFDLeaksCleanCycles verifies that repeated cycles of Open, Put,
// Flush, Reclaim, Key Rotation, and Close release all file descriptors and
// terminate all background goroutines (committer, flusher, reclaimer).
func TestGoroutineAndFDLeaksCleanCycles(t *testing.T) {
	// Let background runtime settle
	runtime.GC()
	baselineGoroutines := waitForGoroutines(runtime.NumGoroutine(), 200*time.Millisecond)

	for cycle := 0; cycle < 5; cycle++ {
		dir := t.TempDir()
		masterKey := make([]byte, 32)
		masterKey[0] = byte(cycle + 1)

		opts := Options{
			Path:                dir,
			MasterKey:           masterKey,
			MaxKeySize:          128,
			MaxValueSize:        512,
			TargetBlockBytes:    1024,
			MaxBlockBytes:       4096,
			MaxSegmentSize:      8192,
			CompactionThreshold: 0.8,
			ReclaimInterval:     50 * time.Millisecond,
		}

		st, err := Open(opts)
		if err != nil {
			t.Fatalf("cycle %d Open: %v", cycle, err)
		}

		// Perform operations
		for i := 0; i < 25; i++ {
			k := fmt.Sprintf("k-c%d-%d", cycle, i)
			v := fmt.Sprintf("v-c%d-%d", cycle, i)
			if err := st.Put([]byte(k), []byte(v)); err != nil {
				t.Fatalf("cycle %d Put: %v", cycle, err)
			}
		}

		if err := st.Flush(); err != nil {
			t.Fatalf("cycle %d Flush: %v", cycle, err)
		}
		_ = st.RotateKey()
		_ = st.Reclaim()

		// Verify that open store has active FDs before closing
		if runtime.GOOS == "linux" {
			openBeforeClose := countOpenStoreFDs(dir)
			if openBeforeClose == 0 {
				t.Fatalf("cycle %d expected active store FDs before Close, got 0", cycle)
			}
		}

		if err := st.Close(); err != nil {
			t.Fatalf("cycle %d Close: %v", cycle, err)
		}

		// Verify zero leaked file descriptors after Close
		leakedFDs := countOpenStoreFDs(dir)
		if leakedFDs > 0 {
			t.Fatalf("cycle %d leaked %d file descriptors in %s after Close", cycle, leakedFDs, dir)
		}
	}

	// Verify all background goroutines exited
	runtime.GC()
	finalGoroutines := waitForGoroutines(baselineGoroutines, 1*time.Second)
	if finalGoroutines > baselineGoroutines {
		t.Fatalf("goroutine leak detected: baseline=%d, final=%d", baselineGoroutines, finalGoroutines)
	}
}

// TestGoroutineAndFDLeaksOnTerminalCrash verifies that when a fatal fault trips
// terminal state, calling Close still properly terminates background goroutines
// and closes all open segment and lock file handles.
func TestGoroutineAndFDLeaksOnTerminalCrash(t *testing.T) {
	runtime.GC()
	baselineGoroutines := waitForGoroutines(runtime.NumGoroutine(), 200*time.Millisecond)

	for cycle := 0; cycle < 5; cycle++ {
		dir := t.TempDir()
		faults := &FaultHooks{}
		opts := Options{
			Path:             dir,
			MasterKey:        make([]byte, 32),
			MaxKeySize:       128,
			MaxValueSize:     512,
			TargetBlockBytes: 1024,
			MaxBlockBytes:    4096,
			MaxSegmentSize:   8192,
			Faults:           faults,
		}

		st, err := Open(opts)
		if err != nil {
			t.Fatalf("cycle %d Open: %v", cycle, err)
		}

		// Arm terminal append fault
		faults.Append = func() error {
			return fmt.Errorf("injected terminal crash")
		}

		// Trigger terminal failure via Commit
		m := Mutation{Key: []byte("k"), Value: []byte("v")}
		_ = st.Commit([]Mutation{m}, DurabilitySync)

		if st.StorageError() == nil {
			t.Fatalf("cycle %d expected terminal error, got nil", cycle)
		}

		// Close must terminate workers and release all FDs despite terminal state
		_ = st.Close()

		leakedFDs := countOpenStoreFDs(dir)
		if leakedFDs > 0 {
			t.Fatalf("cycle %d leaked %d file descriptors after terminal Close in %s", cycle, leakedFDs, dir)
		}
	}

	runtime.GC()
	finalGoroutines := waitForGoroutines(baselineGoroutines, 1*time.Second)
	if finalGoroutines > baselineGoroutines {
		t.Fatalf("terminal error goroutine leak detected: baseline=%d, final=%d", baselineGoroutines, finalGoroutines)
	}
}

// TestGoroutineLeakOnAbortedCheckpoint verifies that canceled checkpoints
// do not leak goroutines or file handles.
func TestGoroutineLeakOnAbortedCheckpoint(t *testing.T) {
	runtime.GC()
	baselineGoroutines := waitForGoroutines(runtime.NumGoroutine(), 200*time.Millisecond)

	dir := t.TempDir()
	opts := Options{
		Path:             dir,
		MasterKey:        make([]byte, 32),
		MaxKeySize:       128,
		MaxValueSize:     512,
		TargetBlockBytes: 1024,
		MaxBlockBytes:    4096,
		MaxSegmentSize:   8192,
	}

	st, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	for i := 0; i < 20; i++ {
		_ = st.Put([]byte(fmt.Sprintf("k%d", i)), []byte("val"))
	}
	_ = st.Flush()

	// Try checkpoint with already canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cpDir := filepath.Join(dir, "canceled-cp")
	_, err = st.Checkpoint(ctx, cpDir)
	if err == nil {
		t.Fatal("expected error from canceled checkpoint, got nil")
	}

	// Verify store remains healthy
	if st.StorageError() != nil {
		t.Fatalf("unexpected StorageError after canceled checkpoint: %v", st.StorageError())
	}
	if err := st.Put([]byte("after-cancel"), []byte("ok")); err != nil {
		t.Fatalf("Put after canceled checkpoint failed: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush after canceled checkpoint failed: %v", err)
	}

	_ = st.Close()

	leakedFDs := countOpenStoreFDs(dir)
	if leakedFDs > 0 {
		t.Fatalf("leaked %d file descriptors after checkpoint cancellation", leakedFDs)
	}

	runtime.GC()
	finalGoroutines := waitForGoroutines(baselineGoroutines, 1*time.Second)
	if finalGoroutines > baselineGoroutines {
		t.Fatalf("goroutine leak after checkpoint cancellation: baseline=%d, final=%d", baselineGoroutines, finalGoroutines)
	}
}
