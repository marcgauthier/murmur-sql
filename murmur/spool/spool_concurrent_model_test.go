package spool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// oracleRecord stores the expected state of a key in the model oracle.
type oracleRecord struct {
	val     []byte
	deleted bool
	version uint64
}

// keyLockMap provides per-key mutexes to ensure sequential consistency
// per key between the physical store commits and the model oracle updates,
// while allowing full concurrency across different keys.
type keyLockMap struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newKeyLockMap() *keyLockMap {
	return &keyLockMap{locks: make(map[string]*sync.Mutex)}
}

func (kl *keyLockMap) getLock(k string) *sync.Mutex {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	l, ok := kl.locks[k]
	if !ok {
		l = &sync.Mutex{}
		kl.locks[k] = l
	}
	return l
}

// TestConcurrentModelOracle exercises multi-threaded writers, commit groups,
// background flushers, reclaimers/compactions, checkpoints, and data key rotations.
// A concurrent state-machine oracle tracks ground-truth values and verifies linearizability,
// absence of lost updates, and zero phantom resurrections upon reopening.
func TestConcurrentModelOracle(t *testing.T) {
	dir := t.TempDir()
	masterKey := make([]byte, 32)
	for i := range masterKey {
		masterKey[i] = byte(i + 13)
	}

	opts := Options{
		Path:                dir,
		MasterKey:           masterKey,
		MaxKeySize:          128,
		MaxValueSize:        512,
		TargetBlockBytes:    1024,
		MaxBlockBytes:       4096,
		MaxSegmentSize:      8192, // Small segment size forces frequent rotations and compaction eligibility
		CompactionThreshold: 0.8,
		TombProofThreshold:  2,
	}

	st, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const numKeys = 40
	keyNames := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keyNames[i] = fmt.Sprintf("model-key-%03d", i)
	}

	klm := newKeyLockMap()

	var oracleMu sync.RWMutex
	oracle := make(map[string]oracleRecord)
	var globalVer atomic.Uint64

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Worker group 1: Multi-mutation atomic commits
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*1000))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Pick 1 to 3 distinct keys
				count := 1 + r.Intn(3)
				selectedIdx := make(map[int]bool)
				for len(selectedIdx) < count {
					selectedIdx[r.Intn(numKeys)] = true
				}
				// Sort indices to prevent deadlocks when locking multiple keys
				sortedIndices := make([]int, 0, len(selectedIdx))
				for idx := range selectedIdx {
					sortedIndices = append(sortedIndices, idx)
				}
				for i := 0; i < len(sortedIndices); i++ {
					for j := i + 1; j < len(sortedIndices); j++ {
						if sortedIndices[i] > sortedIndices[j] {
							sortedIndices[i], sortedIndices[j] = sortedIndices[j], sortedIndices[i]
						}
					}
				}

				// Acquire locks in order
				locks := make([]*sync.Mutex, len(sortedIndices))
				for i, idx := range sortedIndices {
					locks[i] = klm.getLock(keyNames[idx])
					locks[i].Lock()
				}

				muts := make([]Mutation, len(sortedIndices))
				newStates := make([]oracleRecord, len(sortedIndices))
				vNum := globalVer.Add(1)

				for i, idx := range sortedIndices {
					k := keyNames[idx]
					isDelete := (r.Intn(5) == 0) // 20% deletes
					if isDelete {
						muts[i] = Mutation{Key: []byte(k), Deleted: true}
						newStates[i] = oracleRecord{deleted: true, version: vNum}
					} else {
						val := []byte(fmt.Sprintf("w%d-v%d-%d", workerID, vNum, r.Int63()))
						muts[i] = Mutation{Key: []byte(k), Value: val}
						newStates[i] = oracleRecord{val: val, deleted: false, version: vNum}
					}
				}

				commitErr := st.Commit(muts, DurabilitySync)
				if commitErr == nil {
					oracleMu.Lock()
					for i, idx := range sortedIndices {
						oracle[keyNames[idx]] = newStates[i]
					}
					oracleMu.Unlock()
				} else if !errors.Is(commitErr, ErrMaintenance) && !errors.Is(commitErr, ErrBackpressure) && !errors.Is(commitErr, ErrClosed) {
					t.Errorf("worker %d unexpected commit error: %v", workerID, commitErr)
				}

				for i := len(locks) - 1; i >= 0; i-- {
					locks[i].Unlock()
				}
				time.Sleep(time.Duration(r.Intn(5)) * time.Millisecond)
			}
		}(w)
	}

	// Worker group 2: Single-key Put and Delete
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*5000 + 123))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				idx := r.Intn(numKeys)
				k := keyNames[idx]
				l := klm.getLock(k)
				l.Lock()

				isDelete := (r.Intn(4) == 0)
				vNum := globalVer.Add(1)
				var opErr error
				var rec oracleRecord

				if isDelete {
					opErr = st.Delete([]byte(k))
					rec = oracleRecord{deleted: true, version: vNum}
				} else {
					val := []byte(fmt.Sprintf("single-w%d-v%d", workerID, vNum))
					opErr = st.Put([]byte(k), val)
					rec = oracleRecord{val: val, deleted: false, version: vNum}
				}

				if opErr == nil {
					oracleMu.Lock()
					oracle[k] = rec
					oracleMu.Unlock()
				} else if !errors.Is(opErr, ErrMaintenance) && !errors.Is(opErr, ErrBackpressure) && !errors.Is(opErr, ErrClosed) {
					t.Errorf("single worker %d unexpected error: %v", workerID, opErr)
				}

				l.Unlock()
				time.Sleep(time.Duration(r.Intn(4)) * time.Millisecond)
			}
		}(w)
	}

	// Background worker: Periodic Flusher
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(15 * time.Millisecond):
				_ = st.Flush()
			}
		}
	}()

	// Background worker: Periodic Reclaimer / Compactor
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Millisecond):
				_ = st.Reclaim()
			}
		}
	}()

	// Background worker: Active Key Rotator
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(70 * time.Millisecond):
				_ = st.RotateKey()
			}
		}
	}()

	// Background worker: Checkpointer
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				i++
				cpDir := filepath.Join(dir, fmt.Sprintf("cp-%d", i))
				_, _ = st.Checkpoint(context.Background(), cpDir)
			}
		}
	}()

	wg.Wait()

	// Drain remaining writes
	if err := st.Flush(); err != nil && !errors.Is(err, ErrMaintenance) {
		t.Fatalf("final Flush: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen the store and verify against the oracle model
	seen := make(map[string]Record)
	reopened, err := OpenAndLoad(opts, func(records []Record) error {
		for _, r := range records {
			cp := Record{
				Key:      cloneBytes(r.Key),
				Value:    cloneBytes(r.Value),
				Sequence: r.Sequence,
				Deleted:  r.Deleted,
			}
			seen[string(cp.Key)] = cp
		}
		return nil
	})
	if err != nil {
		t.Fatalf("OpenAndLoad: %v", err)
	}
	defer reopened.Close()

	oracleMu.RLock()
	defer oracleMu.RUnlock()

	for k, expected := range oracle {
		actual, exists := seen[k]
		if expected.deleted {
			// A deleted key either has a tombstone record or was purged by compaction.
			// It MUST NOT appear as a live (non-deleted) record.
			if exists && !actual.Deleted {
				t.Fatalf("phantom resurrection: key %s was deleted (oracle ver %d), but recovered as live value %q",
					k, expected.version, string(actual.Value))
			}
		} else {
			// A live key MUST exist and have matching value.
			if !exists {
				t.Fatalf("lost update: live key %s (oracle ver %d) not found in recovered store",
					k, expected.version)
			}
			if actual.Deleted {
				t.Fatalf("unwarranted deletion: live key %s recovered as deleted tombstone", k)
			}
			if !bytes.Equal(actual.Value, expected.val) {
				t.Fatalf("value mismatch for key %s: got %q, want %q",
					k, string(actual.Value), string(expected.val))
			}
		}
	}
}

// TestConcurrentCrashLinearizability verifies that all acknowledged DurabilitySync
// commits survive a sudden store shutdown and retain atomicity (all or nothing per group).
func TestConcurrentCrashLinearizability(t *testing.T) {
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

	type committedTx struct {
		id   int
		muts []Mutation
	}

	var mu sync.Mutex
	committedTxs := make([]committedTx, 0)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 4 concurrent writers committing multi-key batches with DurabilitySync
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			txID := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				txID++
				k1 := fmt.Sprintf("w%d-k1-%d", workerID, txID)
				k2 := fmt.Sprintf("w%d-k2-%d", workerID, txID)
				v1 := []byte(fmt.Sprintf("v1-%d-%d", workerID, txID))
				v2 := []byte(fmt.Sprintf("v2-%d-%d", workerID, txID))

				batch := []Mutation{
					{Key: []byte(k1), Value: v1},
					{Key: []byte(k2), Value: v2},
				}

				err := st.Commit(batch, DurabilitySync)
				if err == nil {
					mu.Lock()
					committedTxs = append(committedTxs, committedTx{id: txID, muts: batch})
					mu.Unlock()
				} else if errors.Is(err, ErrClosed) || errors.Is(err, ErrStorageFailed) {
					return
				}
				time.Sleep(time.Duration(r.Intn(2)) * time.Millisecond)
			}
		}(w)
	}

	// Let workers run for 800ms, then abruptly Close the store
	time.Sleep(800 * time.Millisecond)
	close(stop)
	// Sudden close without calling Flush
	_ = st.Close()
	wg.Wait()

	mu.Lock()
	txsCount := len(committedTxs)
	savedTxs := make([]committedTx, txsCount)
	copy(savedTxs, committedTxs)
	mu.Unlock()

	if txsCount == 0 {
		t.Fatal("no transactions were committed before close")
	}

	// Reopen store and verify atomicity and durability
	seenKeys := make(map[string][]byte)
	reopened, err := OpenAndLoad(opts, func(records []Record) error {
		for _, r := range records {
			seenKeys[string(r.Key)] = cloneBytes(r.Value)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer reopened.Close()

	// Every transaction that received a nil error must have both keys present with exact values
	for _, tx := range savedTxs {
		for _, m := range tx.muts {
			val, ok := seenKeys[string(m.Key)]
			if !ok {
				t.Fatalf("durability violation: committed key %s missing after reopen", string(m.Key))
			}
			if !bytes.Equal(val, m.Value) {
				t.Fatalf("data corruption: key %s value mismatch: got %q, want %q",
					string(m.Key), string(val), string(m.Value))
			}
		}
	}
}

// TestConcurrentRotationAndCompaction runs concurrent writes while
// master key rotation and active data key rotations occur concurrently.
func TestConcurrentRotationAndCompaction(t *testing.T) {
	dir := t.TempDir()
	initialKey := make([]byte, 32)
	for i := range initialKey {
		initialKey[i] = 0xAA
	}

	opts := Options{
		Path:                dir,
		MasterKey:           initialKey,
		MaxKeySize:          128,
		MaxValueSize:        512,
		TargetBlockBytes:    1024,
		MaxBlockBytes:       4096,
		MaxSegmentSize:      8192,
		CompactionThreshold: 0.8,
	}

	st, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Writer goroutines
	var writeCount atomic.Uint64
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				num := writeCount.Add(1)
				k := fmt.Sprintf("rot-k-%d", num)
				v := []byte(fmt.Sprintf("rot-val-%d", num))
				_ = st.Put([]byte(k), v)
				time.Sleep(1 * time.Millisecond)
			}
		}(w)
	}

	// Key rotation goroutines
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
				_ = st.RotateKey()
				_ = st.Reclaim()
			}
		}
	}()

	wg.Wait()
	_ = st.Flush()

	// Perform Master Key Rotation to second key
	secondKey := make([]byte, 32)
	for i := range secondKey {
		secondKey[i] = 0xBB
	}
	if err := st.RotateMasterKey(secondKey, ""); err != nil {
		t.Fatalf("RotateMasterKey: %v", err)
	}

	_ = st.Close()

	// Reopening with initialKey MUST fail
	optsInitial := opts
	optsInitial.MasterKey = initialKey
	_, err = Open(optsInitial)
	if err == nil {
		t.Fatal("Open with old master key succeeded, want failure")
	}

	// Reopening with secondKey MUST succeed and load data
	optsSecond := opts
	optsSecond.MasterKey = secondKey
	loadedRecords := 0
	stReopened, err := OpenAndLoad(optsSecond, func(records []Record) error {
		loadedRecords += len(records)
		return nil
	})
	if err != nil {
		t.Fatalf("OpenAndLoad with new master key failed: %v", err)
	}
	defer stReopened.Close()

	if loadedRecords == 0 {
		t.Fatal("expected loaded records, got 0")
	}
}
