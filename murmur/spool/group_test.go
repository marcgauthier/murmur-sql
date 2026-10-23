package spool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// TestGroupsNeverSplitAcrossSegments commits and flushes across
// rotations, then assembles every member file and requires each
// group to validate whole within exactly one file.
func TestGroupsNeverSplitAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	o := DefaultOptions(dir)
	o.MasterKey = shutdownKey
	o.Flush.MaxDelay = -1
	o.ReclaimInterval = -1
	o.MaxSegmentSize = 8192
	o.TargetBlockBytes = 1024
	o.MaxBlockBytes = 2048
	o.MaxRecordsPerBlock = 1000
	o.MaxKeySize = 64
	o.MaxValueSize = 1024
	st, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	put := func(k string, n int) {
		t.Helper()
		v := make([]byte, n)
		for i := range v {
			v[i] = byte(i*31 + len(k))
		}
		if err := st.Put([]byte(k), v); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	for i := 0; i < 30; i++ {
		put(fmt.Sprintf("k%02d", i), 500)
		if i%7 == 6 {
			if err := st.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
		}
	}
	muts := make([]Mutation, 10)
	for i := range muts {
		muts[i] = Mutation{Key: []byte(fmt.Sprintf("g%02d", i)), Value: make([]byte, 400)}
	}
	if err := st.Commit(muts, DurabilitySync); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	l, err := openLoader(LoadOptions{Path: dir, MasterKey: shutdownKey})
	if err != nil {
		t.Fatalf("openLoader: %v", err)
	}
	if len(l.man.members) < 2 {
		t.Fatalf("members = %v, want rotation", l.man.members)
	}
	seen := make(map[uint64]uint64) // group -> file
	for _, id := range l.man.members {
		p := filepath.Join(dir, segmentsDirName, segmentFileName(id))
		f, err := os.Open(p)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		fi, _ := f.Stat()
		groups, _, _, _, err := assembleFile(f, fi.Size(), id, false)
		f.Close()
		if err != nil {
			t.Fatalf("assemble %d: %v", id, err)
		}
		for _, g := range groups {
			if _, _, err := l.validateGroup(id, g); err != nil {
				t.Fatalf("validate %d: %v", id, err)
			}
			// Re-derive the group id from validated plaintext.
			plain, err := l.openPayload(g.data[0].hdr, g.data[0].sealed)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			gid, _, _, _, err := decodeBody(plain, g.data[0].hdr.recordCount)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if prev, ok := seen[gid]; ok {
				t.Fatalf("group %d in files %d and %d", gid, prev, id)
			}
			seen[gid] = id
		}
	}
	if len(seen) == 0 {
		t.Fatalf("no groups found")
	}
}

// TestSeqExhaustionRejectsWholeGroup pushes the sequence counter to
// the edge and requires Commit to reject without partial writes.
func TestSeqExhaustionRejectsWholeGroup(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	st.seqNext.Store(seqCounterMax - 3)
	muts := []Mutation{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
		{Key: []byte("d"), Value: []byte("4")},
	}
	if err := st.Commit(muts, DurabilitySync); !errors.Is(err, ErrSeqExhausted) {
		t.Fatalf("Commit err = %v, want ErrSeqExhausted", err)
	}
	if n := st.Stats().GroupsAccepted; n != 0 {
		t.Fatalf("GroupsAccepted = %d, want 0", n)
	}
	if n := st.pendingRecords.Load(); n != 0 {
		t.Fatalf("pending records = %d, want 0", n)
	}
	// A fitting group still commits.
	if err := st.Commit(muts[:2], DurabilitySync); err != nil {
		t.Fatalf("fitting Commit: %v", err)
	}
}

// TestTerminalFailureStickyFlow breaks the segment writer, then
// requires the sticky failure, single notification, and rejection
// of further mutating operations.
func TestTerminalFailureStickyFlow(t *testing.T) {
	dir := t.TempDir()
	var notifies atomic.Int64
	var nmu sync.Mutex
	var notified error
	o := shutdownOptions(dir)
	o.OnStorageError = func(err error) {
		notifies.Add(1)
		nmu.Lock()
		notified = err
		nmu.Unlock()
	}
	st, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.seg.close(); err != nil {
		t.Fatalf("seg close: %v", err)
	}
	muts := []Mutation{{Key: []byte("k"), Value: []byte("v")}}
	if err := st.Commit(muts, DurabilitySync); err == nil {
		t.Fatalf("Commit through broken writer succeeded")
	}
	if err := st.StorageError(); err == nil {
		t.Fatalf("StorageError nil after failure")
	}
	if n := notifies.Load(); n != 1 {
		t.Fatalf("notifications = %d, want 1", n)
	}
	nmu.Lock()
	nn := notified
	nmu.Unlock()
	if nn == nil {
		t.Fatalf("no notified cause")
	}
	for name, op := range map[string]func() error{
		"Put":     func() error { return st.Put([]byte("a"), []byte("b")) },
		"Delete":  func() error { return st.Delete([]byte("a")) },
		"Commit":  func() error { return st.Commit(muts, DurabilityAsync) },
		"Sync":    st.Sync,
		"Flush":   st.Flush,
		"Reclaim": st.Reclaim,
	} {
		if err := op(); !errors.Is(err, ErrStorageFailed) {
			t.Fatalf("%s err = %v, want ErrStorageFailed", name, err)
		}
	}
	if n := notifies.Load(); n != 1 {
		t.Fatalf("notifications = %d after rejected ops, want 1", n)
	}
	// Failed close releases resources without clearing the state.
	if err := st.Close(); err == nil {
		t.Fatalf("Close succeeded on failed store")
	}
	if err := st.StorageError(); err == nil {
		t.Fatalf("StorageError cleared by Close")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestManifestUnknownFeaturesRejected flips a required feature bit
// and requires a clean unsupported-version rejection.
func TestManifestUnknownFeaturesRejected(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, manifestFileName))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	m, err := parseManifest(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	m.reqFeatures |= 1 << 40
	if err := os.WriteFile(filepath.Join(dir, manifestFileName), m.encode(), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Open(shutdownOptions(dir)); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("open err = %v, want ErrUnsupportedVersion", err)
	}
}
