package spool_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

// TestKeyRotationPruneWriteStorm exercises the key lifecycle: seals race
// rotation (which retires keys) and pruning (which deletes
// retired, unreferenced keys). A seal that reads a data key, loses a
// race with retire+prune, then seals with orphaned material produces
// a block nobody can decrypt — so the oracle is a clean reopen and a
// full successful Load afterwards.
func TestKeyRotationPruneWriteStorm(t *testing.T) {
	secs := 3
	if v := os.Getenv("SPOOL_KEYRACE_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			secs = n
		}
	}
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.MaxRecordsPerBlock = 128
	st, err := spool.Open(o)
	if err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	var seals atomic.Int64
	// Writers: single-mutation async commits for maximum seal rate.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for !stop.Load() {
				m := spool.Mutation{
					Key:   []byte(fmt.Sprintf("k%04d", (w*100003+i)%1000)),
					Value: []byte(fmt.Sprintf("v%d", i)),
				}
				if err := st.Commit([]spool.Mutation{m}, spool.DurabilityAsync); err != nil {
					t.Errorf("commit: %v", err)
					return
				}
				seals.Add(1)
				i++
				// Keep the default suite bounded: an unrestricted async writer
				// can create close to a million records before the reopen check.
				time.Sleep(time.Millisecond)
			}
		}(w)
	}
	var rotations atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			if err := st.RotateKey(); err != nil {
				t.Errorf("rotate: %v", err)
				return
			}
			rotations.Add(1)
			time.Sleep(time.Millisecond)
		}
	}()
	var prunes atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			if _, err := st.PruneDataKeys(); err != nil {
				t.Errorf("prune: %v", err)
				return
			}
			prunes.Add(1)
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(time.Duration(secs) * time.Second)
	stop.Store(true)
	wg.Wait()
	t.Logf("seals=%d rotations=%d prunes=%d", seals.Load(), rotations.Load(), prunes.Load())
	if rotations.Load() < 10 || prunes.Load() < 10 {
		t.Fatalf("storm too small to exercise the race (rot=%d prune=%d)", rotations.Load(), prunes.Load())
	}
	if err := st.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	n := 0
	if err := spool.Load(dir, testMasterKey, func(recs []spool.Record) error {
		n += len(recs)
		return nil
	}); err != nil {
		t.Fatalf("load after storm: %v", err)
	}
	if n == 0 {
		t.Fatal("no records survived the storm")
	}
	if _, err := st2.KeyReferences(); err != nil {
		t.Fatalf("references after storm: %v", err)
	}
}

// TestPruneSeesBufferedAsyncSeals is the deterministic regression
// test for the prune-vs-buffer bug: Async seals publish (index and
// Stats observe them) while their bytes still sit in the segment
// writer's userspace buffer. A prune that scans the file without
// flushing first sees an empty segment, concludes the retired seal
// key is unreferenced, and deletes it — bricking the store once the
// buffer flushes. The precondition (published but unflushed) is
// asserted, not assumed: without the fixed scan this fails on the
// reopen every time.
func TestPruneSeesBufferedAsyncSeals(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	const n = 50
	for i := 0; i < n; i++ {
		m := spool.Mutation{
			Key:   []byte(fmt.Sprintf("bufkey%04d", i)),
			Value: []byte(fmt.Sprintf("bufval%d", i)),
		}
		if err := st.Commit([]spool.Mutation{m}, spool.DurabilityAsync); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	// Wait for publication through the in-memory index only: Sync
	// or Flush would push bytes to the file and defeat the test.
	deadline := time.Now().Add(10 * time.Second)
	for st.Stats().Keys != n {
		if time.Now().After(deadline) {
			t.Fatalf("seals not published after 10s (keys=%d)", st.Stats().Keys)
		}
		time.Sleep(time.Millisecond)
	}
	// Precondition: everything sealed is still buffered; the
	// segment file on disk is empty.
	segPath := filepath.Join(dir, "segments", "000000000001.spool")
	stt, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("stat active segment: %v", err)
	}
	if stt.Size() != 0 {
		t.Fatalf("precondition broken: %d bytes already flushed", stt.Size())
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	pruned, err := st.PruneDataKeys()
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	for _, id := range pruned {
		if id == 1 {
			t.Fatalf("prune deleted key 1 while %d buffered seals reference it", n)
		}
	}
	if err := st.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	got := 0
	if err := spool.Load(dir, testMasterKey, func(recs []spool.Record) error {
		got += len(recs)
		return nil
	}); err != nil {
		t.Fatalf("load after prune: %v", err)
	}
	if got != n {
		t.Fatalf("load after prune: got %d records, want %d", got, n)
	}
}
