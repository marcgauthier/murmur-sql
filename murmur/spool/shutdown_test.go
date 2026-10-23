package spool

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// white-box master key for shutdown tests (the black-box helper
// lives in another package).
var shutdownKey = func() []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(i + 1)
	}
	return k
}()

func shutdownOptions(dir string) Options {
	o := DefaultOptions(dir)
	o.MasterKey = shutdownKey
	o.Flush.MaxDelay = -1
	o.ReclaimInterval = -1
	return o
}

// crashStore simulates a process crash: background loops halt, the
// lock drops, and no Close runs, so no clean marker is written. The
// segment fd is released for Windows file-sharing hygiene.
func crashStore(t *testing.T, st *Store) {
	t.Helper()
	st.closed.Store(true)
	st.queueMu.Lock()
	st.drained = true
	st.commitCond.Broadcast()
	st.queueMu.Unlock()
	close(st.stopCh)
	st.closeWG.Wait()
	if err := st.seg.close(); err != nil {
		t.Fatalf("seg close: %v", err)
	}
	st.unlock()
}

// TestCleanMarkerRecorded pins the mechanism: a clean close leaves a
// clean flag with the exact file count and byte total.
func TestCleanMarkerRecorded(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, manifestFileName))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	m, err := parseManifest(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !m.clean {
		t.Fatalf("clean flag not set after Close")
	}
	segRaw, err := os.ReadFile(filepath.Join(dir, segmentsDirName, segmentFileName(1)))
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if m.cleanFiles != 1 || m.cleanBytes != uint64(len(segRaw)) {
		t.Fatalf("marker = (%d files, %d bytes), want (1, %d)", m.cleanFiles, m.cleanBytes, len(segRaw))
	}
	// Opening clears the flag again until the next clean close.
	st2, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	st2.Close()
	raw, _ = os.ReadFile(filepath.Join(dir, manifestFileName))
	m, _ = parseManifest(raw)
	if !m.clean {
		t.Fatalf("clean flag not re-set after second Close")
	}
}

// TestUncleanOpenSkipsMarker simulates a crash (background loops
// halted, lock dropped, no Close) followed by external truncation.
// With no clean marker the next open treats the short tail as a
// torn tail instead of failing like a cleanly closed store would.
// (A missing member file always fails: membership is authoritative
// independent of the marker.)
func TestUncleanOpenSkipsMarker(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := st.Put([]byte{byte('a' + i)}, bytes.Repeat([]byte{byte(i)}, 200)); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := st.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	crashStore(t, st)

	p := filepath.Join(dir, segmentsDirName, segmentFileName(1))
	finfo, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(p, finfo.Size()-100); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	st2, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("reopen after unclean truncation: %v", err)
	}
	defer st2.Close()
	if n := st2.Stats().TruncatedTails; n != 1 {
		t.Fatalf("TruncatedTails = %d, want 1", n)
	}
	// The store still works on top of the truncated tail.
	if err := st2.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st2.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// TestUncleanTailCorruptionFailsClosed crashes, corrupts one byte
// of the trailing complete group, and requires the next open to
// fail authentication loudly. Structurally complete bytes that
// fail validation are corruption, never a torn tail (only
// structurally incomplete tails are discarded).
func TestUncleanTailCorruptionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("old"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st.Put([]byte("new"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	crashStore(t, st)

	p := filepath.Join(dir, segmentsDirName, segmentFileName(1))
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Open(shutdownOptions(dir)); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("reopen err = %v, want ErrAuthFailed", err)
	}
}

// TestWorkerBytesMax pins the worker-buffer memory report: Workers
// blocks built at once, each transiently holding plaintext,
// compressed, and sealed copies up to MaxBlockBytes.
func TestWorkerBytesMax(t *testing.T) {
	dir := t.TempDir()
	o := shutdownOptions(dir)
	o.Workers = 4
	st, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if st.workers != 4 {
		t.Fatalf("workers = %d, want 4", st.workers)
	}
	want := uint64(4 * o.MaxBlockBytes * 3)
	if got := st.Stats().WorkerBytesMax; got != want {
		t.Fatalf("WorkerBytesMax = %d, want %d", got, want)
	}
}
