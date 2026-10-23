package spool_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// TestRewriteDataKeys writes two generations under different keys,
// rewrites everything under the current key, and requires the old
// key to lose all block references (hence prunable) with data
// intact across a reopen.
func TestRewriteDataKeys(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("old"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	if err := st.Put([]byte("new"), []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	refs, err := st.KeyReferences()
	if err != nil {
		t.Fatalf("KeyReferences: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("refs = %+v, want 2 keys", refs)
	}
	genBefore := st.KeyInventory().ManifestGeneration
	if err := st.RewriteDataKeys(); err != nil {
		t.Fatalf("RewriteDataKeys: %v", err)
	}
	refs, err = st.KeyReferences()
	if err != nil {
		t.Fatalf("KeyReferences: %v", err)
	}
	for _, r := range refs {
		if r.KeyID == 1 && r.Blocks != 0 {
			t.Fatalf("key 1 still has %d blocks after rewrite", r.Blocks)
		}
	}
	if gen := st.KeyInventory().ManifestGeneration; gen != genBefore+1 {
		t.Fatalf("generation = %d, want %d", gen, genBefore+1)
	}
	if pruned, err := st.PruneDataKeys(); err != nil {
		t.Fatalf("PruneDataKeys: %v", err)
	} else if len(pruned) != 1 || pruned[0] != 1 {
		t.Fatalf("pruned = %v, want [1]", pruned)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if string(got["old"]) != "v1" || string(got["new"]) != "v2" {
		t.Fatalf("reloaded = %v, want old+new", got)
	}
}

// TestRewritePreservesSequences requires logical mutation sequences
// to survive a rewrite byte-identically.
func TestRewritePreservesSequences(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	st, err := spool.Open(o)
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
	if err := st.Delete([]byte("k0")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	before := loadAllSeq(t, dir)
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := st2.RewriteDataKeys(); err != nil {
		t.Fatalf("RewriteDataKeys: %v", err)
	}
	if err := st2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	after := loadAllSeq(t, dir)
	if len(before) != len(after) {
		t.Fatalf("keys %d -> %d across rewrite", len(before), len(after))
	}
	for k, seq := range before {
		if after[k] != seq {
			t.Fatalf("key %s sequence %d -> %d", k, seq, after[k])
		}
	}
}

// TestRebindContext rebinds a store to a new database context and
// requires the old context to fail, the new one to open, and all
// data to survive under the same wrapping material.
func TestRebindContext(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.ContextID = []byte("cccccccccccccccc")
	o.WrappingKeyID = "rebind-key"
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	var target [16]byte
	copy(target[:], "dddddddddddddddd")
	if err := st.RebindContext(target); err != nil {
		t.Fatalf("RebindContext: %v", err)
	}
	// New writes under the new context work on the live handle.
	if err := st.Put([]byte("after"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	hint, err := spool.ReadKeyHint(dir)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if hint.ContextID != target {
		t.Fatalf("hint context = %x, want %x", hint.ContextID, target)
	}
	if hint.WrappingKeyID != "rebind-key" {
		t.Fatalf("hint wrapping id = %q, want preserved", hint.WrappingKeyID)
	}
	// Old context fails; new context opens with all data.
	if _, err := spool.Open(o); !errors.Is(err, spool.ErrContextMismatch) {
		t.Fatalf("old context err = %v, want ErrContextMismatch", err)
	}
	o2 := testOptions(t, dir)
	o2.ContextID = target[:]
	o2.WrappingKeyID = "rebind-key"
	st2, err := spool.Open(o2)
	if err != nil {
		t.Fatalf("open rebound: %v", err)
	}
	st2.Close()
	got := loadAll(t, dir)
	if len(got) != 21 || string(got["after"]) != "v" {
		t.Fatalf("loaded %d keys, want 21 with after=v", len(got))
	}
}

// TestMaintenanceGatesWrites runs a rewrite large enough to observe
// and requires mutating operations to fail with ErrMaintenance
// while it runs, then succeed after.
func TestMaintenanceGatesWrites(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 200; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%04d", i)), incompressible(100<<10, uint64(i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- st.RewriteDataKeys() }()
	// Wait until the rewrite visibly holds the store.
	sawActive := false
	for i := 0; i < 5000; i++ {
		select {
		case err := <-done:
			t.Fatalf("rewrite finished before gate observed: %v", err)
		default:
		}
		if st.KeyInventory().MaintenancePhase != "idle" {
			sawActive = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !sawActive {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("RewriteDataKeys: %v", err)
			}
		default:
		}
		t.Fatal("rewrite never visibly active")
	}
	if err := st.Put([]byte("k"), []byte("v")); !errors.Is(err, spool.ErrMaintenance) {
		t.Fatalf("Put during maintenance = %v, want ErrMaintenance", err)
	}
	if err := st.Commit([]spool.Mutation{{Key: []byte("k")}}, spool.DurabilityAsync); !errors.Is(err, spool.ErrMaintenance) {
		t.Fatalf("Commit during maintenance = %v, want ErrMaintenance", err)
	}
	if err := st.RewriteDataKeys(); !errors.Is(err, spool.ErrMaintenance) {
		t.Fatalf("second maintenance = %v, want ErrMaintenance", err)
	}
	// Memory-state reads stay available throughout.
	_ = st.Stats()
	_ = st.KeyInventory()
	if err := <-done; err != nil {
		t.Fatalf("RewriteDataKeys: %v", err)
	}
	if err := st.Put([]byte("after"), []byte("v")); err != nil {
		t.Fatalf("Put after maintenance: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := loadAll(t, dir)
	if len(got) != 201 {
		t.Fatalf("loaded %d keys, want 201", len(got))
	}
}

// TestMaintenancePlainStores requires rewrite to no-op and rebind
// to update only the recorded context on plaintext stores.
func TestMaintenancePlainStores(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.Encryption = spool.EncryptionNone
	o.MasterKey = nil
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st.RewriteDataKeys(); err != nil {
		t.Fatalf("RewriteDataKeys on plain: %v", err)
	}
	var target [16]byte
	copy(target[:], "eeeeeeeeeeeeeeee")
	if err := st.RebindContext(target); err != nil {
		t.Fatalf("RebindContext on plain: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	hint, err := spool.ReadKeyHint(dir)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if hint.ContextID != target {
		t.Fatalf("hint context = %x, want %x", hint.ContextID, target)
	}
	o2 := testOptions(t, dir)
	o2.Encryption = spool.EncryptionNone
	o2.MasterKey = nil
	o2.ContextID = target[:]
	st2, err := spool.Open(o2)
	if err != nil {
		t.Fatalf("open rebound plain: %v", err)
	}
	st2.Close()
}
