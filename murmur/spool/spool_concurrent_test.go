package spool_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// TestConcurrentWriters hammers Put/Delete from many goroutines, then
// verifies every surviving key holds one of its written values.
func TestConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.Flush.MaxDelay = 5 * time.Millisecond // exercise background flushes
	o.ReclaimInterval = 50 * time.Millisecond
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const writers = 32
	const perWriter = 300
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := []byte(fmt.Sprintf("w%d/k%d", w, i%50))
				if i%9 == 8 {
					if err := st.Delete(k); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
					continue
				}
				if err := st.Put(k, []byte(fmt.Sprintf("w%d-v%d", w, i))); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	// Every surviving value must be one this writer wrote; keys are
	// writer-private so membership is exact.
	for w := 0; w < writers; w++ {
		for i := 0; i < 50; i++ {
			k := fmt.Sprintf("w%d/k%d", w, i)
			v, ok := got[k]
			if !ok {
				continue // deleted last, fine
			}
			var ww, vv int
			if _, err := fmt.Sscanf(string(v), "w%d-v%d", &ww, &vv); err != nil || ww != w {
				t.Fatalf("key %s holds foreign value %q", k, v)
			}
			if vv%50 != i || vv >= perWriter {
				t.Fatalf("key %s holds inconsistent value %q", k, v)
			}
		}
	}
	if len(got) == 0 {
		t.Fatalf("nothing survived")
	}
}

// TestBackpressure fills the pending buffer with the background
// writer disabled and requires TryPut to refuse plus blocking Put to
// wait for a Flush.
func TestBackpressure(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.MaxPendingBytes = 64 << 10
	o.Flush.MaxRecords = -1
	o.Flush.MaxBytes = -1
	// MaxDelay already -1 via testOptions: background never fires.
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	val := make([]byte, 8<<10)
	refused := false
	for i := 0; i < 1000 && !refused; i++ {
		err := st.TryPut([]byte(fmt.Sprintf("k%d", i)), val)
		if errors.Is(err, spool.ErrBackpressure) {
			refused = true
		} else if err != nil {
			t.Fatalf("TryPut: %v", err)
		}
	}
	if !refused {
		t.Fatalf("TryPut never hit backpressure")
	}
	// A blocking Put waits; a concurrent Flush releases it.
	done := make(chan error, 1)
	go func() {
		done <- st.Put([]byte("blocked"), val)
	}()
	select {
	case err := <-done:
		t.Fatalf("Put returned %v without flush (expected to block)", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked Put: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("blocked Put never released")
	}
	// Drained: TryPut works again.
	if err := st.TryPut([]byte("after"), []byte("x")); err != nil {
		t.Fatalf("TryPut after drain: %v", err)
	}
}
