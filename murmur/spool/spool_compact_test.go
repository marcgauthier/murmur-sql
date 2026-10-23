package spool_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

// incompressible returns deterministic pseudorandom bytes that
// defeat compression, so segment-fill tests get real byte counts
// instead of ratio-dependent ones. The seed must differ per key:
// identical values still compress away across records in a block.
func incompressible(n int, seed uint64) []byte {
	out := make([]byte, n)
	x := uint64(0x9E3779B97F4A7C15) ^ (seed*0xBF58476D1CE4E5B9 + 1)
	for i := range out {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		out[i] = byte(x >> 56)
	}
	return out
}

// compactTestOptions uses tiny segments so each flush rotates,
// making file-level behavior deterministic.
func compactTestOptions(t *testing.T, dir string) spool.Options {
	t.Helper()
	o := testOptions(t, dir)
	o.MaxSegmentSize = 8192
	o.TargetBlockBytes = 2048
	o.MaxBlockBytes = 4096
	o.MaxRecordsPerBlock = 1000
	o.MaxKeySize = 64
	o.MaxValueSize = 2048
	return o
}

func segmentCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "segments"))
	if err != nil {
		t.Fatalf("segments dir: %v", err)
	}
	return len(entries)
}

// TestCompactionRewritesLive builds a dead-heavy file, supersedes
// most of it, and requires compaction to rewrite the survivors and
// drop the old file without losing data.
func TestCompactionRewritesLive(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(compactTestOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// 100 keys with padding values so the first flush spans
	// several files.
	const padLen = 300
	for i := 0; i < 100; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%03d", i)), incompressible(padLen, uint64(i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	firstSegs := segmentCount(t, dir)
	if firstSegs == 0 {
		t.Fatalf("no segments after first flush")
	}
	// Supersede 90 keys (small values, newer files).
	for i := 0; i < 90; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("new")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Drive reclamation until a compaction completes.
	for i := 0; i < 200 && st.Stats().Compactions == 0; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	if st.Stats().Compactions == 0 {
		t.Fatalf("no compaction happened; stats: %+v segs=%d", st.Stats(), segmentCount(t, dir))
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != 100 {
		t.Fatalf("loaded %d keys, want 100", len(got))
	}
	for i := 0; i < 90; i++ {
		if string(got[fmt.Sprintf("k%03d", i)]) != "new" {
			t.Fatalf("k%03d lost its new value", i)
		}
	}
	for i := 90; i < 100; i++ {
		// Survivors were rewritten by compaction with fresh
		// sequences; values intact.
		if len(got[fmt.Sprintf("k%03d", i)]) != padLen {
			t.Fatalf("k%03d survivor damaged", i)
		}
	}
}

// deadHeavyStore builds a store with compaction-eligible files:
// 100 padded keys, then 90 superseded by small values.
func deadHeavyStore(t *testing.T, o spool.Options) *spool.Store {
	t.Helper()
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 100; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%03d", i)), incompressible(300, uint64(i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for i := 0; i < 90; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("new")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return st
}

// TestCompactionWriterIDCollision forces writer rotations to
// interleave compaction replacement ids: the shared allocator must
// keep successor and replacement ids disjoint, so post-compaction
// writes rotate onto fresh files instead of colliding with (or
// clobbering) a live replacement.
func TestCompactionWriterIDCollision(t *testing.T) {
	dir := t.TempDir()
	st := deadHeavyStore(t, compactTestOptions(t, dir))
	defer st.Close()
	// Compact at least one victim: the replacement consumes a
	// manifest-allocated id.
	for i := 0; i < 200 && st.Stats().Compactions == 0; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	if st.Stats().Compactions == 0 {
		t.Fatalf("no compaction; stats: %+v", st.Stats())
	}
	// Write several more oversized generations, forcing writer
	// rotations past the replacement id range. Under sealed+1
	// allocation the first rotation collides with the replacement
	// file and wedges the writer.
	for gen := 0; gen < 5; gen++ {
		for i := 0; i < 100; i++ {
			k := []byte(fmt.Sprintf("g%d/k%03d", gen, i))
			if err := st.Put(k, incompressible(300, uint64(gen*100+i))); err != nil {
				t.Fatalf("Put gen %d: %v", gen, err)
			}
		}
		if err := st.Flush(); err != nil {
			t.Fatalf("Flush gen %d: %v", gen, err)
		}
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim gen %d: %v", gen, err)
		}
	}
	got := loadAll(t, dir)
	if len(got) != 100+500 {
		t.Fatalf("loaded %d keys, want 600", len(got))
	}
	for i := 0; i < 90; i++ {
		if string(got[fmt.Sprintf("k%03d", i)]) != "new" {
			t.Fatalf("k%03d lost its value", i)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A clean reopen proves the file set is coherent: no wedged
	// writer, no clobbered replacement.
	st2, err := spool.Open(compactTestOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if got := loadAll(t, dir); len(got) != 600 {
		t.Fatalf("reopened %d keys, want 600", len(got))
	}
}

// TestCompactionPausesUnderPressure verifies a rewrite pass is
// skipped while the write buffer is over half full, then proceeds
// once the backlog drains.
func TestCompactionPausesUnderPressure(t *testing.T) {
	dir := t.TempDir()
	o := compactTestOptions(t, dir)
	// 64KiB budget: the ~38KiB setup stays admittable while the
	// ~38KiB backlog trips the half-full pressure line.
	o.MaxPendingBytes = 64 << 10
	st := deadHeavyStore(t, o)
	defer st.Close()
	// Backlog over half the budget, unflushed.
	pairs := make([]spool.KV, 100)
	for i := range pairs {
		pairs[i] = spool.KV{Key: []byte(fmt.Sprintf("p%03d", i)), Value: incompressible(300, uint64(1000+i))}
	}
	if err := st.PutBatch(pairs); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	if err := st.Reclaim(); err != nil {
		t.Fatalf("Reclaim: %v", err)
	}
	if n := st.Stats().Compactions; n != 0 {
		t.Fatalf("Compactions = %d under pressure, want 0", n)
	}
	// The fence committed the backlog, so the next pass compacts.
	for i := 0; i < 200 && st.Stats().Compactions == 0; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	if n := st.Stats().Compactions; n == 0 {
		t.Fatalf("no compaction after pressure drained")
	}
}

// TestCompactionMinFreeBytes verifies the free-space reserve gates
// rewrite passes: an unsatisfiable reserve blocks them, a trivial
// one allows them.
func TestCompactionMinFreeBytes(t *testing.T) {
	dir := t.TempDir()
	o := compactTestOptions(t, dir)
	o.CompactionMinFreeBytes = 1 << 62
	st := deadHeavyStore(t, o)
	for i := 0; i < 5; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	if n := st.Stats().Compactions; n != 0 {
		t.Fatalf("Compactions = %d under reserve, want 0", n)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	o.CompactionMinFreeBytes = 1
	st2, err := spool.Open(o)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	for i := 0; i < 200 && st2.Stats().Compactions == 0; i++ {
		if err := st2.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	if n := st2.Stats().Compactions; n == 0 {
		t.Fatalf("no compaction with trivial reserve")
	}
}

// loadAllSeq collects winners with their logical sequences.
func loadAllSeq(t *testing.T, dir string) map[string]uint64 {
	t.Helper()
	got := make(map[string]uint64)
	err := spool.Load(dir, testMasterKey, func(recs []spool.Record) error {
		for _, r := range recs {
			if r.Deleted {
				continue
			}
			got[string(r.Key)] = r.Sequence
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return got
}

// TestCompactionPreservesSequences requires replacement compaction
// to relocate records without creating newer logical mutations:
// every surviving key keeps its sequence across compactions and a
// reopen.
func TestCompactionPreservesSequences(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(compactTestOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 100; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%03d", i)), incompressible(300, uint64(i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for i := 0; i < 90; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("new")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before := loadAllSeq(t, dir)
	if len(before) != 100 {
		t.Fatalf("loaded %d keys, want 100", len(before))
	}
	for i := 0; i < 200 && st.Stats().Compactions == 0; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	if st.Stats().Compactions == 0 {
		t.Fatalf("no compaction happened")
	}
	after := loadAllSeq(t, dir)
	if len(after) != 100 {
		t.Fatalf("loaded %d keys after compaction, want 100", len(after))
	}
	for k, seq := range before {
		if after[k] != seq {
			t.Fatalf("key %s sequence %d -> %d across compaction", k, seq, after[k])
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := loadAllSeq(t, dir)
	for k, seq := range before {
		if reopened[k] != seq {
			t.Fatalf("key %s sequence %d -> %d across reopen", k, seq, reopened[k])
		}
	}
}

// TestTombProofBlocksAndConverges verifies proven tombstone
// collection: markers whose older values survive in other files are
// retained (never resurrected), and drain once those files are gone.
func TestTombProofBlocksAndConverges(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.MaxSegmentSize = 4096
	o.TargetBlockBytes = 1024
	o.MaxBlockBytes = 2048
	o.MaxRecordsPerBlock = 100
	o.MaxKeySize = 64
	o.MaxValueSize = 1024
	o.TombProofThreshold = 4
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Generation 1: keepers plus holders, big values force rotation.
	for i := 0; i < 10; i++ {
		if err := st.Put([]byte(fmt.Sprintf("keeper%02d", i)), incompressible(900, uint64(i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := st.Put([]byte(fmt.Sprintf("holder%02d", i)), incompressible(900, uint64(100+i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Generation 2: tombstone the keepers.
	for i := 0; i < 10; i++ {
		if err := st.Delete([]byte(fmt.Sprintf("keeper%02d", i))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Reclaim while holders live: drops happen only with proof, so
	// keepers must never resurrect, across reclaims and a reopen.
	for i := 0; i < 10; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != 10 {
		t.Fatalf("loaded %d keys, want 10 holders", len(got))
	}
	for i := 0; i < 10; i++ {
		if _, ok := got[fmt.Sprintf("keeper%02d", i)]; ok {
			t.Fatalf("keeper%02d resurrected", i)
		}
	}
	st, err = spool.Open(o)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	// Delete the holders: their files drain, then the proofs
	// complete and every tomb file is reclaimed.
	for i := 0; i < 10; i++ {
		if err := st.Delete([]byte(fmt.Sprintf("holder%02d", i))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for i := 0; i < 200 && segmentCount(t, dir) > 1; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	if n := segmentCount(t, dir); n != 1 {
		t.Fatalf("segments = %d, want 1 (active) after convergence", n)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := loadAll(t, dir); len(got) != 0 {
		t.Fatalf("loaded %d keys after convergence, want 0", len(got))
	}
}

// TestDeadFileRemoval verifies fully superseded segments are
// unlinked (not merely compacted) once nothing references them.
func TestDeadFileRemoval(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(compactTestOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 50; i++ {
		if err := st.Put([]byte(fmt.Sprintf("a%d", i)), incompressible(300, uint64(i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before := segmentCount(t, dir)
	if before < 2 {
		t.Fatalf("first flush spans %d segment(s), want >= 2", before)
	}
	for i := 0; i < 50; i++ {
		if err := st.Put([]byte(fmt.Sprintf("b%d", i)), []byte("new")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// Delete the whole first generation so its files go fully dead.
	for i := 0; i < 50; i++ {
		if err := st.Delete([]byte(fmt.Sprintf("a%d", i))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	removed := false
	for i := 0; i < 200 && !removed; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
		if n := segmentCount(t, dir); n < before {
			removed = true
		}
	}
	if !removed {
		t.Fatalf("no segment removed (still %d); stats: %+v", segmentCount(t, dir), st.Stats())
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != 50 {
		t.Fatalf("loaded %d keys, want 50", len(got))
	}
}
