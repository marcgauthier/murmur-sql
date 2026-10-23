package spool_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

// TestCommitAtomicRoundTrip commits mixed puts, deletes and repeated
// keys, then requires the reloaded state to match group order with
// the final operation per key winning.
func TestCommitAtomicRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	muts := []spool.Mutation{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("1")},
		{Key: []byte("a"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("1")},
		{Key: []byte("c"), Deleted: true},
		{Key: []byte("b"), Value: []byte("3")},
		{Key: []byte("a"), Value: []byte("final")},
	}
	if err := st.Commit(muts, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != 2 || string(got["a"]) != "final" || string(got["b"]) != "3" {
		t.Fatalf("reloaded = %v, want a=final b=3", got)
	}
}

// TestCommitMultiBlockGroup forces a group across several data
// blocks and requires all-or-nothing recovery.
func TestCommitMultiBlockGroup(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.TargetBlockBytes = 1024
	o.MaxBlockBytes = 2048
	o.MaxRecordsPerBlock = 100
	o.MaxKeySize = 64
	o.MaxValueSize = 1024
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	muts := make([]spool.Mutation, 50)
	for i := range muts {
		muts[i] = spool.Mutation{Key: []byte(fmt.Sprintf("k%02d", i)), Value: incompressible(300, uint64(i))}
	}
	if err := st.Commit(muts, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != 50 {
		t.Fatalf("loaded %d keys, want 50", len(got))
	}
	for i := range muts {
		k := fmt.Sprintf("k%02d", i)
		if len(got[k]) != 300 {
			t.Fatalf("%s damaged", k)
		}
	}
}

// TestCommitValidation rejects malformed and oversized groups with
// nothing admitted.
func TestCommitValidation(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.MaxKeySize = 64
	o.MaxValueSize = 1024
	o.TargetBlockBytes = 1024
	o.MaxBlockBytes = 2048
	o.MaxAtomicBatchBytes = 4096
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	before := st.Stats()
	cases := []struct {
		name string
		muts []spool.Mutation
		want error
	}{
		{"valued tomb", []spool.Mutation{{Key: []byte("k"), Value: []byte("v"), Deleted: true}}, spool.ErrBadMutation},
		{"oversize key", []spool.Mutation{{Key: make([]byte, o.MaxKeySize+1)}}, spool.ErrKeyTooLarge},
		{"oversize value", []spool.Mutation{{Key: []byte("k"), Value: make([]byte, o.MaxValueSize+1)}}, spool.ErrValueTooLarge},
		{"oversize group", []spool.Mutation{
			{Key: []byte("a"), Value: make([]byte, 1024)},
			{Key: []byte("b"), Value: make([]byte, 1024)},
			{Key: []byte("c"), Value: make([]byte, 1024)},
			{Key: []byte("d"), Value: make([]byte, 1024)},
			{Key: []byte("e"), Value: make([]byte, 1024)},
		}, spool.ErrGroupTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := st.Commit(tc.muts, spool.DurabilitySync); !errors.Is(err, tc.want) {
				t.Fatalf("Commit err = %v, want %v", err, tc.want)
			}
		})
	}
	after := st.Stats()
	if after.GroupsAccepted != before.GroupsAccepted || after.PendingRecords != 0 {
		t.Fatalf("rejected groups admitted: %+v -> %+v", before, after)
	}
	// Empty groups are no-ops without barrier effects.
	if err := st.Commit(nil, spool.DurabilitySync); err != nil {
		t.Fatalf("empty Commit: %v", err)
	}
	if n := st.Stats().GroupsAccepted; n != before.GroupsAccepted {
		t.Fatalf("empty Commit admitted a group")
	}
	if err := st.Commit([]spool.Mutation{{Key: []byte("k"), Value: []byte("v")}}, spool.Durability(99)); err == nil {
		t.Fatalf("unknown durability accepted")
	}
}

// TestCommitAsyncAcceptance requires async commits to acknowledge on
// acceptance and Sync to fence them durably.
func TestCommitAsyncAcceptance(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.Durability = spool.DurabilityAsync
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 10; i++ {
		m := []spool.Mutation{{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}}
		if err := st.Commit(m, spool.DurabilityAsync); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	// Same-key async groups preserve admission order.
	if err := st.Commit([]spool.Mutation{{Key: []byte("ord"), Value: []byte("1")}}, spool.DurabilityAsync); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := st.Commit([]spool.Mutation{{Key: []byte("ord"), Value: []byte("2")}}, spool.DurabilityAsync); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := st.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != 11 || string(got["ord"]) != "2" {
		t.Fatalf("reloaded %d keys ord=%q, want 11 ord=2", len(got), got["ord"])
	}
}

// TestCommitSyncOrdering interleaves sync commits and individual
// writes; the last admitted version of a key must win.
func TestCommitSyncOrdering(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("put1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Commit([]spool.Mutation{{Key: []byte("k"), Value: []byte("commit2")}}, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("put3")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if string(got["k"]) != "put3" {
		t.Fatalf("k = %q, want put3", got["k"])
	}
}

// TestCommitGroupStats pins the group counters across mixed sync,
// async and flush-driven groups.
func TestCommitGroupStats(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Commit([]spool.Mutation{{Key: []byte("a"), Value: []byte("v")}}, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit sync: %v", err)
	}
	if err := st.Commit([]spool.Mutation{{Key: []byte("b"), Value: []byte("v")}}, spool.DurabilityAsync); err != nil {
		t.Fatalf("Commit async: %v", err)
	}
	if err := st.Put([]byte("c"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	s := st.Stats()
	st.Close()
	if s.Commits != 2 {
		t.Fatalf("Commits = %d, want 2", s.Commits)
	}
	if s.GroupsAccepted != 3 || s.GroupsWritten != 3 {
		t.Fatalf("groups accepted/written = %d/%d, want 3/3", s.GroupsAccepted, s.GroupsWritten)
	}
	if s.GroupsSynced != 2 { // sync commit + syncing fence group
		t.Fatalf("GroupsSynced = %d, want 2", s.GroupsSynced)
	}
	if s.Syncs != 1 || s.PendingGroups != 0 {
		t.Fatalf("Syncs = %d PendingGroups = %d, want 1/0", s.Syncs, s.PendingGroups)
	}
}

// TestCommitOrdersAfterBufferedPuts requires a Commit to seal
// previously accepted buffered writes first: an unflushed Put must
// never outrank a later Commit on the same key.
func TestCommitOrdersAfterBufferedPuts(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	m := spool.Mutation{Key: []byte("k"), Value: []byte("v2")}
	if err := st.Commit([]spool.Mutation{m}, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// And the reverse: a commit followed by an unflushed put orders
	// the put last through the sync fence seal.
	m2 := spool.Mutation{Key: []byte("j"), Value: []byte("w1")}
	if err := st.Commit([]spool.Mutation{m2}, spool.DurabilityAsync); err != nil {
		t.Fatalf("Commit async: %v", err)
	}
	if err := st.Put([]byte("j"), []byte("w2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if string(got["k"]) != "v2" {
		t.Fatalf("k = %q, want v2 (commit beat buffered put)", got["k"])
	}
	if string(got["j"]) != "w2" {
		t.Fatalf("j = %q, want w2 (sealed put beat async commit)", got["j"])
	}
}

// TestOversizedGroupDedicatedSegment commits a group larger than the
// segment target and requires it to land whole in a sealed,
// oversized file with later groups in a successor.
func TestOversizedGroupDedicatedSegment(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.MaxSegmentSize = 8192
	o.TargetBlockBytes = 2048
	o.MaxBlockBytes = 4096
	o.MaxRecordsPerBlock = 1000
	o.MaxKeySize = 64
	o.MaxValueSize = 2048
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	muts := make([]spool.Mutation, 20)
	for i := range muts {
		muts[i] = spool.Mutation{Key: []byte(fmt.Sprintf("big%02d", i)), Value: incompressible(1500, uint64(i))}
	}
	if err := st.Commit(muts, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := st.Commit([]spool.Mutation{{Key: []byte("after"), Value: []byte("v")}}, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ents, err := os.ReadDir(filepath.Join(dir, "segments"))
	if err != nil {
		t.Fatalf("segments: %v", err)
	}
	var oversized int
	for _, e := range ents {
		fi, err := e.Info()
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if fi.Size() > o.MaxSegmentSize {
			oversized++
		}
	}
	if oversized != 1 {
		t.Fatalf("oversized segments = %d, want 1 (of %d)", oversized, len(ents))
	}
	got := loadAll(t, dir)
	if len(got) != 21 || string(got["after"]) != "v" {
		t.Fatalf("loaded %d keys, want 21 with after=v", len(got))
	}
}

// TestRejectsPretransactionalStore writes a v1 manifest and requires
// a clean unsupported-version rejection without migration.
func TestRejectsPretransactionalStore(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "segments"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	raw := make([]byte, 64)
	copy(raw[0:4], "SPLM")
	raw[4] = 1 // format version 1
	if err := os.WriteFile(filepath.Join(dir, "manifest"), raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := spool.Open(testOptions(t, dir)); !errors.Is(err, spool.ErrUnsupportedVersion) {
		t.Fatalf("open err = %v, want ErrUnsupportedVersion", err)
	}
	if err := spool.Load(dir, testMasterKey, func([]spool.Record) error { return nil }); !errors.Is(err, spool.ErrUnsupportedVersion) {
		t.Fatalf("load err = %v, want ErrUnsupportedVersion", err)
	}
}
