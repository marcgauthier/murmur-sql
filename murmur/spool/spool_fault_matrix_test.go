package spool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFaultMatrix systematically exercises failure points across
// different operational phases and verifies that:
// 1. Recoverable operations fail with ordinary retryable errors.
// 2. Storage publication failures enter terminal state without corrupting surviving files.
// 3. Post-fault recovery / reopen restores committed data intact.
func TestFaultMatrix(t *testing.T) {
	faultErr := fmt.Errorf("injected fault")

	testCases := []struct {
		name       string
		arm        func(f *FaultHooks)
		op         func(st *Store, dir string) error
		isTerminal bool
	}{
		{
			name: "commit with append fault",
			arm:  func(f *FaultHooks) { f.Append = func() error { return faultErr } },
			op: func(st *Store, dir string) error {
				m := Mutation{Key: []byte("k"), Value: []byte("v")}
				return st.Commit([]Mutation{m}, DurabilitySync)
			},
			isTerminal: true,
		},
		{
			name: "commit with sync fault",
			arm:  func(f *FaultHooks) { f.SegmentSync = func() error { return faultErr } },
			op: func(st *Store, dir string) error {
				m := Mutation{Key: []byte("k"), Value: []byte("v")}
				return st.Commit([]Mutation{m}, DurabilitySync)
			},
			isTerminal: true,
		},
		{
			name: "reclaim with compaction staging fault",
			arm:  func(f *FaultHooks) { f.CompactionStage = func() error { return faultErr } },
			op: func(st *Store, dir string) error {
				for i := 0; i < 200; i++ {
					if err := st.Reclaim(); err != nil {
						return err
					}
					if st.Stats().Compactions > 0 {
						return nil
					}
				}
				return fmt.Errorf("no compaction occurred")
			},
			isTerminal: false, // Compaction staging errors are retryable
		},
		{
			name: "reclaim with compaction rename fault",
			arm:  func(f *FaultHooks) { f.Rename = func() error { return faultErr } },
			op: func(st *Store, dir string) error {
				for i := 0; i < 200; i++ {
					if err := st.Reclaim(); err != nil {
						return err
					}
					if st.Stats().Compactions > 0 {
						return nil
					}
				}
				return fmt.Errorf("no compaction occurred")
			},
			isTerminal: false, // Staging rename before publish is retryable
		},
		{
			name: "checkpoint with link fault",
			arm:  func(f *FaultHooks) { f.CheckpointLink = func() error { return faultErr } },
			op: func(st *Store, dir string) error {
				_, err := st.Checkpoint(context.Background(), filepath.Join(dir, "cp-link-fail"))
				return err
			},
			isTerminal: false, // Checkpoint capture failure is ordinary
		},
		{
			name: "checkpoint with dirsync fault",
			arm:  func(f *FaultHooks) { f.DirSync = func() error { return faultErr } },
			op: func(st *Store, dir string) error {
				_, err := st.Checkpoint(context.Background(), filepath.Join(dir, "cp-dirsync-fail"))
				return err
			},
			isTerminal: false,
		},
		{
			name: "rotate master key with keyring persist fault",
			arm:  func(f *FaultHooks) { f.KeyringPersist = func() error { return faultErr } },
			op: func(st *Store, dir string) error {
				newKey := make([]byte, 32)
				newKey[0] = 0x99
				return st.RotateMasterKey(newKey, "")
			},
			isTerminal: true, // Failed security envelope publish is terminal
		},
		{
			name: "rewrite data keys with intent fault",
			arm:  func(f *FaultHooks) { f.IntentPersist = func() error { return faultErr } },
			op: func(st *Store, dir string) error {
				return st.RewriteDataKeys()
			},
			isTerminal: false, // Intent write failure before publish is ordinary
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			masterKey := make([]byte, 32)
			for i := range masterKey {
				masterKey[i] = byte(i + 1)
			}
			faults := &FaultHooks{}
			opts := Options{
				Path:                dir,
				MasterKey:           masterKey,
				MaxKeySize:          128,
				MaxValueSize:        512,
				TargetBlockBytes:    1024,
				MaxBlockBytes:       4096,
				MaxSegmentSize:      8192,
				CompactionThreshold: 0.8,
				TombProofThreshold:  2,
				Faults:              faults,
			}

			st, err := Open(opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}

			// Seed baseline data
			for i := 0; i < 10; i++ {
				k := fmt.Sprintf("base-%02d", i)
				v := fmt.Sprintf("val-%02d", i)
				if err := st.Put([]byte(k), []byte(v)); err != nil {
					t.Fatalf("seed Put: %v", err)
				}
			}
			if err := st.Flush(); err != nil {
				t.Fatalf("seed Flush: %v", err)
			}

			if strings.Contains(tc.name, "reclaim") {
				for i := 0; i < 100; i++ {
					v := make([]byte, 300)
					x := uint64(0x9E3779B97F4A7C15) ^ (uint64(i)*0xBF58476D1CE4E5B9 + 1)
					for j := range v {
						x ^= x << 13
						x ^= x >> 7
						x ^= x << 17
						v[j] = byte(x >> 56)
					}
					if err := st.Put([]byte(fmt.Sprintf("k%03d", i)), v); err != nil {
						t.Fatalf("seed Put: %v", err)
					}
				}
				if err := st.Flush(); err != nil {
					t.Fatalf("seed Flush: %v", err)
				}
				for i := 0; i < 90; i++ {
					if err := st.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("new")); err != nil {
						t.Fatalf("seed overwrite Put: %v", err)
					}
				}
				if err := st.Flush(); err != nil {
					t.Fatalf("seed Flush: %v", err)
				}
			}

			// Arm the fault
			tc.arm(faults)

			// Execute the operation
			opErr := tc.op(st, dir)
			if opErr == nil {
				t.Fatal("expected operation error under fault, got nil")
			}

			// Verify terminal vs ordinary classification
			storageErr := st.StorageError()
			if tc.isTerminal {
				if storageErr == nil {
					t.Fatalf("expected terminal storage error, got nil (opErr=%v)", opErr)
				}
			} else {
				if storageErr != nil {
					t.Fatalf("expected ordinary non-terminal error, got StorageError=%v (opErr=%v)", storageErr, opErr)
				}
			}

			// Close the store
			_ = st.Close()

			// Disarm faults and verify reopen succeeds and preserves baseline data
			opts.Faults = nil
			reopened, err := OpenAndLoad(opts, func(records []Record) error {
				return nil
			})
			if err != nil {
				t.Fatalf("reopen after fault failed: %v", err)
			}
			defer reopened.Close()

			// Check that baseline data is intact
			for i := 0; i < 10; i++ {
				k := fmt.Sprintf("base-%02d", i)
				loc, ok := reopened.idx.shardFor([]byte(k)).values[k]
				if !ok || loc.FileID == 0 {
					t.Fatalf("baseline key %s missing or damaged after fault recovery", k)
				}
			}
		})
	}
}

// TestFaultPartialAppendRecovery simulates a crash where a block was
// partially written to disk before sync, and verifies that startup
// clean-up detects the torn tail and discards it without data loss.
func TestFaultPartialAppendRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Path:      dir,
		MasterKey: make([]byte, 32),
	}

	st, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 20; i++ {
		m := Mutation{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("val")}
		if err := st.Commit([]Mutation{m}, DurabilitySync); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	st.Close()

	// Append corrupt garbage bytes to the active segment
	activeFile := filepath.Join(dir, segmentsDirName, segmentFileName(1))
	f, err := os.OpenFile(activeFile, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open active segment: %v", err)
	}
	// Write half of a block header
	if _, err := f.Write([]byte("SPLB\x01\x00\x00")); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	f.Close()

	// Reopen must succeed and recover all 20 keys
	seen := make(map[string]bool)
	reopened, err := OpenAndLoad(opts, func(records []Record) error {
		for _, r := range records {
			seen[string(r.Key)] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("OpenAndLoad failed to recover from partial append: %v", err)
	}
	defer reopened.Close()

	if len(seen) != 20 {
		t.Fatalf("recovered %d keys, want 20", len(seen))
	}
}
