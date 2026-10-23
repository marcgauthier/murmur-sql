package spool

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentCompactionRotationCheckpointInvariant hammers concurrent
// commits, deletes, key rotations, compactions, and checkpoints, while
// continuously verifying that manifest invariants (strictly sorted members,
// no duplicates, valid active segment) are never violated.
func TestConcurrentCompactionRotationCheckpointInvariant(t *testing.T) {
	dir := t.TempDir()
	masterKey := make([]byte, 32)
	for i := range masterKey {
		masterKey[i] = byte(i + 1)
	}

	opts := Options{
		Path:                dir,
		MasterKey:           masterKey,
		MaxKeySize:          128,
		MaxValueSize:        1024,
		TargetBlockBytes:    2048,
		MaxBlockBytes:       4096,
		MaxAtomicBatchBytes: 8192,
		MaxSegmentSize:      16384, // small segment size to force rapid rotations
		CompactionThreshold: 0.8,
		TombProofThreshold:  2,
		WriteShards:         4,
		IndexShards:         4,
	}

	st, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	stop := atomic.Bool{}
	var wg sync.WaitGroup
	errCh := make(chan error, 16)

	reportErr := func(err error) {
		if err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}

	// Worker 1: Continuous Commits
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for !stop.Load() {
			k := fmt.Sprintf("k-%04d", i%100)
			v := fmt.Sprintf("val-%04d-%d", i%100, i)
			m := Mutation{Key: []byte(k), Value: []byte(v)}
			if err := st.Commit([]Mutation{m}, DurabilityAsync); err != nil {
				if !stop.Load() {
					reportErr(fmt.Errorf("commit: %w", err))
				}
				return
			}
			i++
			if i%10 == 0 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// Worker 2: Continuous Deletes
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for !stop.Load() {
			k := fmt.Sprintf("k-%04d", (i+50)%100)
			if err := st.Delete([]byte(k)); err != nil {
				if !stop.Load() {
					reportErr(fmt.Errorf("delete: %w", err))
				}
				return
			}
			i++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Worker 3: Frequent Compaction Reclaims
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			if err := st.Reclaim(); err != nil {
				if !stop.Load() {
					reportErr(fmt.Errorf("reclaim: %w", err))
				}
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Worker 4: Data Key Rotations
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			if err := st.RotateKey(); err != nil {
				if !stop.Load() {
					reportErr(fmt.Errorf("rotate key: %w", err))
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Worker 5: Checkpoints
	wg.Add(1)
	go func() {
		defer wg.Done()
		cpIdx := 0
		for !stop.Load() && cpIdx < 5 {
			dest := filepath.Join(dir, fmt.Sprintf("ckpt-%d", cpIdx))
			cp, err := st.Checkpoint(context.Background(), dest)
			if err != nil {
				if !stop.Load() {
					reportErr(fmt.Errorf("checkpoint: %w", err))
				}
				return
			}
			// Verify checkpoint loads cleanly
			err = Load(dest, masterKey, func(records []Record) error {
				return nil
			})
			if err != nil {
				reportErr(fmt.Errorf("load checkpoint: %w", err))
				return
			}
			if err := cp.Release(); err != nil {
				reportErr(fmt.Errorf("release checkpoint: %w", err))
				return
			}
			cpIdx++
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Monitor invariants for 1.5 seconds
	start := time.Now()
	for time.Since(start) < 1500*time.Millisecond {
		st.manifestMu.Lock()
		m := *st.man
		members := append([]uint64(nil), m.members...)
		active := m.activeFileID
		st.manifestMu.Unlock()

		if !sort.SliceIsSorted(members, func(i, j int) bool { return members[i] < members[j] }) {
			t.Fatalf("invariant violation: members not sorted: %v", members)
		}
		for i := 1; i < len(members); i++ {
			if members[i] == members[i-1] {
				t.Fatalf("invariant violation: duplicate member: %v", members)
			}
		}
		found := false
		for _, id := range members {
			if id == active {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("invariant violation: active segment %d not in members %v", active, members)
		}

		select {
		case err := <-errCh:
			t.Fatalf("worker error: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}

	stop.Store(true)
	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("worker error after stop: %v", err)
	default:
	}

	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Ensure database reopens cleanly and index rebuilds without error
	reopened, err := OpenAndLoad(opts, func(records []Record) error {
		return nil
	})
	if err != nil {
		t.Fatalf("OpenAndLoad after stress test: %v", err)
	}
	defer reopened.Close()
}
