package spool_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

func TestDeleteAndReload(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 50; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for i := 0; i < 50; i += 2 {
		if err := st.Delete([]byte(fmt.Sprintf("k%d", i))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != 25 {
		t.Fatalf("loaded %d keys, want 25", len(got))
	}
	for i := 1; i < 50; i += 2 {
		if string(got[fmt.Sprintf("k%d", i)]) != "v" {
			t.Fatalf("missing odd key k%d", i)
		}
	}
}

// TestTombstoneNoResurrection drives the dangerous lifecycle: a key's
// only value copy lives in an old segment, the tombstone is dropped
// by reclamation, the old segment is unlinked, and the store
// reopens. The key must stay deleted.
func TestTombstoneNoResurrection(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.MaxSegmentSize = 4096 // force many small segments
	o.TargetBlockBytes = 1024
	o.MaxBlockBytes = 2048
	o.MaxRecordsPerBlock = 100
	o.MaxKeySize = 64
	o.MaxValueSize = 1024
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// File 1: the victim key plus padding that will also die. Big
	// incompressible values force rotation so later generations
	// land in newer files.
	if err := st.Put([]byte("victim"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := st.Put([]byte(fmt.Sprintf("pad%d", i)), incompressible(1024, uint64(i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Delete everything from file 1; tombs land in newer files.
	if err := st.Delete([]byte("victim")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := st.Delete([]byte(fmt.Sprintf("pad%d", i))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Survivor data in still newer files.
	for i := 0; i < 30; i++ {
		if err := st.Put([]byte(fmt.Sprintf("live%d", i)), []byte("y")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	segDir := filepath.Join(dir, "segments")
	victimSeg := filepath.Join(segDir, "000000000001.spool")
	if _, err := os.Stat(victimSeg); err != nil {
		t.Fatalf("expected file 1 to exist: %v", err)
	}
	// Reclaim until file 1 (holding only dead records) is unlinked.
	// Reclaim is synchronous; a bounded pass count keeps this
	// deterministic without timing assertions.
	reclaimed := false
	for i := 0; i < 200 && !reclaimed; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
		if _, err := os.Stat(victimSeg); os.IsNotExist(err) {
			reclaimed = true
		}
	}
	if !reclaimed {
		t.Fatalf("file 1 never reclaimed; stats: %+v", st.Stats())
	}
	// Plenty more passes so tomb generations drain too.
	for i := 0; i < 50; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if _, ok := got["victim"]; ok {
		t.Fatalf("victim resurrected after tomb drop + file deletion")
	}
	if len(got) != 30 {
		t.Fatalf("loaded %d keys, want 30 survivors", len(got))
	}
}

// TestPutAfterDeleteWins verifies a re-put following a delete stays
// visible across a reopen (regression guard for sequence reuse: the
// re-put must outrank the older tombstone even though the tomb's
// sequence predates it on disk... they share one epoch here, but a
// restart in between exercises the epoch path).
func TestPutAfterDeleteWins(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// New epoch.
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := st2.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if string(got["k"]) != "v2" {
		t.Fatalf("k = %q, want v2", got["k"])
	}
}
