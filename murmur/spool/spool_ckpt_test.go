package spool_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// materializeCheckpoint copies a checkpoint's stream set into a
// fresh directory, mimicking a backup restore, and returns it.
func materializeCheckpoint(t *testing.T, cp *spool.Checkpoint) string {
	t.Helper()
	dst := t.TempDir()
	for _, f := range cp.Files() {
		rel, err := filepath.Rel(cp.Dir, f)
		if err != nil || strings.HasPrefix(rel, "..") {
			t.Fatalf("checkpoint file %q escapes %q", f, cp.Dir)
		}
		out := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		in, err := os.Open(f)
		if err != nil {
			t.Fatalf("open %s: %v", f, err)
		}
		w, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			in.Close()
			t.Fatalf("create %s: %v", out, err)
		}
		if _, err := io.Copy(w, in); err != nil {
			in.Close()
			w.Close()
			t.Fatalf("copy %s: %v", f, err)
		}
		in.Close()
		w.Close()
	}
	return dst
}

// loadDir loads every current record from a materialized directory.
func loadDir(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	got := make(map[string][]byte)
	if err := spool.Load(dir, testMasterKey, func(recs []spool.Record) error {
		for _, r := range recs {
			if !r.Deleted {
				got[string(r.Key)] = append([]byte(nil), r.Value...)
			} else {
				delete(got, string(r.Key))
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Load %s: %v", dir, err)
	}
	return got
}

// mustCheckpoint captures into dir/name, failing the test on error.
func mustCheckpoint(t *testing.T, st *spool.Store, dir, name string) *spool.Checkpoint {
	t.Helper()
	cp, err := st.Checkpoint(context.Background(), filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	return cp
}

// TestCheckpointRoundTrip captures, streams (materializes), verifies
// standalone both directly and from the stream set, and releases a
// checkpoint.
func TestCheckpointRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Delete([]byte("k00")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cp := mustCheckpoint(t, st, dir, "backup")
	if cp.Generation == 0 || len(cp.Members) == 0 || len(cp.Files()) < 2 {
		t.Fatalf("checkpoint = %+v, want generation+members+files", cp)
	}
	if n := st.Stats().Checkpoints; n != 1 {
		t.Fatalf("Checkpoints = %d, want 1", n)
	}
	if list := st.ListCheckpoints(); len(list) != 1 || list[0].ID != cp.ID {
		t.Fatalf("list = %v, want [checkpoint]", list)
	}
	// The COMPLETE marker carries the magic, generation, and time.
	raw, err := os.ReadFile(filepath.Join(cp.Dir, "COMPLETE"))
	if err != nil {
		t.Fatalf("read COMPLETE: %v", err)
	}
	lines := strings.Split(string(raw), "\n")
	if len(lines) != 4 || lines[0] != "spool-checkpoint/1" || lines[3] != "" {
		t.Fatalf("COMPLETE = %q, want magic+generation+time", raw)
	}
	if lines[1] != fmt.Sprint(cp.Generation) {
		t.Fatalf("COMPLETE generation = %s, want %d", lines[1], cp.Generation)
	}
	// The destination opens directly as a live store.
	rst, err := spool.Open(testOptions(t, cp.Dir))
	if err != nil {
		t.Fatalf("open destination: %v", err)
	}
	if n := rst.Stats().Keys; n != 19 {
		rst.Close()
		t.Fatalf("destination Keys = %d, want 19", n)
	}
	rst.Close()
	// The streamed copy verifies standalone too.
	restored := materializeCheckpoint(t, cp)
	got := loadDir(t, restored)
	if len(got) != 19 || got["k01"] == nil {
		t.Fatalf("restored %d keys, want 19 with k01", len(got))
	}
	if _, ok := got["k00"]; ok {
		t.Fatal("restored deleted k00")
	}
	if err := cp.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if n := st.Stats().Checkpoints; n != 0 {
		t.Fatalf("Checkpoints = %d, want 0", n)
	}
	// Release is idempotent: repeats and unknown ids succeed, and
	// the caller-owned destination survives every release.
	if err := cp.Release(); err != nil {
		t.Fatalf("second Release = %v, want nil", err)
	}
	if err := st.ReleaseCheckpoint(cp.ID); err != nil {
		t.Fatalf("ReleaseCheckpoint = %v, want nil", err)
	}
	if err := st.ReleaseCheckpoint("missing"); err != nil {
		t.Fatalf("ReleaseCheckpoint(missing) = %v, want nil", err)
	}
	if _, err := os.Stat(filepath.Join(cp.Dir, "COMPLETE")); err != nil {
		t.Fatalf("destination removed by release: %v", err)
	}
	// The live store keeps validating after release.
	if err := st.Put([]byte("after"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if n := st2.Stats().Keys; n != 20 {
		t.Fatalf("Keys = %d, want 20", n)
	}
}

// TestCheckpointStableUnderMutations pins old-key data through
// rotation, rewrite, compaction, and pruning: the checkpoint keeps
// opening with its cut until release, after which pruning proceeds.
// The destination keeps opening standalone even after the live
// store prunes the checkpoint's keys.
func TestCheckpointStableUnderMutations(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.MaxSegmentSize = 16 << 10
	o.MaxBlockBytes = 8 << 10
	o.TargetBlockBytes = 4 << 10
	o.MaxRecordsPerBlock = 100
	o.MaxKeySize = 64
	o.MaxValueSize = 2048
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	gen1 := make([]spool.Mutation, 20)
	for i := range gen1 {
		gen1[i] = spool.Mutation{Key: []byte(fmt.Sprintf("k%02d", i)), Value: incompressible(1000, uint64(i))}
	}
	if err := st.Commit(gen1, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit gen1: %v", err)
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	gen2 := make([]spool.Mutation, 20)
	for i := range gen2 {
		gen2[i] = spool.Mutation{Key: []byte(fmt.Sprintf("k%02d", i)), Value: incompressible(1000, uint64(100+i))}
	}
	if err := st.Commit(gen2, spool.DurabilitySync); err != nil {
		t.Fatalf("Commit gen2: %v", err)
	}
	cp := mustCheckpoint(t, st, dir, "backup")
	// Mutate heavily: reclaim the dead gen1 file, rewrite under the
	// current key, rotate again, and attempt pruning.
	for i := 0; i < 10; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	if err := st.RewriteDataKeys(); err != nil {
		t.Fatalf("RewriteDataKeys: %v", err)
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	if err := st.RewriteDataKeys(); err != nil {
		t.Fatalf("RewriteDataKeys: %v", err)
	}
	if pruned, err := st.PruneDataKeys(); err != nil {
		t.Fatalf("PruneDataKeys: %v", err)
	} else if len(pruned) != 0 {
		t.Fatalf("pruned %v with live checkpoint, want none", pruned)
	}
	refs, err := st.KeyReferences()
	if err != nil {
		t.Fatalf("KeyReferences: %v", err)
	}
	pinned := false
	for _, r := range refs {
		if r.KeyID == 1 && r.Checkpoints > 0 {
			pinned = true
		}
	}
	if !pinned {
		t.Fatalf("key 1 has no checkpoint ref: %+v", refs)
	}
	// The checkpoint still opens with its cut (gen2 winners).
	got := loadDir(t, materializeCheckpoint(t, cp))
	if len(got) != 20 {
		t.Fatalf("restored %d keys, want 20", len(got))
	}
	for i := 0; i < 20; i++ {
		if string(got[fmt.Sprintf("k%02d", i)]) != string(incompressible(1000, uint64(100+i))) {
			t.Fatalf("k%02d mismatch in checkpoint", i)
		}
	}
	// Release frees the keys: both retired generations prune away.
	if err := cp.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	pruned, err := st.PruneDataKeys()
	if err != nil {
		t.Fatalf("PruneDataKeys: %v", err)
	}
	if len(pruned) != 2 || pruned[0] != 1 || pruned[1] != 2 {
		t.Fatalf("pruned = %v, want [1 2]", pruned)
	}
	// The destination is self-contained: it still opens with its
	// cut after the live store pruned those keys away.
	got = loadDir(t, cp.Dir)
	if len(got) != 20 {
		t.Fatalf("destination holds %d keys after live prune, want 20", len(got))
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestCheckpointReleaseIsolation releases the older of two
// checkpoints and requires the newer to stay intact. Both
// destinations survive release: the caller owns them.
func TestCheckpointReleaseIsolation(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cp1 := mustCheckpoint(t, st, dir, "cp1")
	if err := st.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cp2 := mustCheckpoint(t, st, dir, "cp2")
	if cp1.ID == cp2.ID {
		t.Fatal("checkpoint ids collide")
	}
	if err := cp1.Release(); err != nil {
		t.Fatalf("Release 1: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cp1.Dir, "COMPLETE")); err != nil {
		t.Fatalf("released destination removed: %v", err)
	}
	got := loadDir(t, materializeCheckpoint(t, cp2))
	if string(got["k1"]) != "v1" || string(got["k2"]) != "v2" {
		t.Fatalf("checkpoint 2 = %v, want k1+k2", got)
	}
	if err := cp2.Release(); err != nil {
		t.Fatalf("Release 2: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestCheckpointReopenForgetsRegistry closes with a live checkpoint
// and requires the reopen to list nothing (registrations are
// process-local) while the destination still opens standalone with
// its cut.
func TestCheckpointReopenForgetsRegistry(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cp := mustCheckpoint(t, st, dir, "backup")
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if list := st2.ListCheckpoints(); len(list) != 0 {
		t.Fatalf("list = %v, want empty registry", list)
	}
	got := loadDir(t, cp.Dir)
	if string(got["k"]) != "v" {
		t.Fatalf("destination = %v, want k=v", got)
	}
	// The stale handle releases against its (closed) store without
	// error: release is idempotent and registry-local.
	if err := cp.Release(); err != nil {
		t.Fatalf("stale Release: %v", err)
	}
}

// TestCheckpointEmptyStore captures an empty store and requires the
// copy to open empty.
func TestCheckpointEmptyStore(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cp := mustCheckpoint(t, st, dir, "backup")
	if got := loadDir(t, cp.Dir); len(got) != 0 {
		t.Fatalf("restored %d keys, want 0", len(got))
	}
	if err := cp.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestCheckpointSurvivesRebind captures under one context, rebinds
// the live store, and requires the checkpoint to keep opening under
// its original context and keyring.
func TestCheckpointSurvivesRebind(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.ContextID = []byte("cccccccccccccccc")
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
	cp := mustCheckpoint(t, st, dir, "backup")
	var target [16]byte
	copy(target[:], "dddddddddddddddd")
	if err := st.RebindContext(target); err != nil {
		t.Fatalf("RebindContext: %v", err)
	}
	if err := st.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The checkpoint opens under its original context with its cut.
	got := loadDir(t, materializeCheckpoint(t, cp))
	if len(got) != 1 || string(got["k"]) != "v" {
		t.Fatalf("checkpoint = %v, want k=v only", got)
	}
	hint, err := spool.ReadKeyHint(materializeCheckpoint(t, cp))
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if string(hint.ContextID[:]) != "cccccccccccccccc" {
		t.Fatalf("checkpoint context = %x, want original", hint.ContextID)
	}
	// The live store moved on and lists no checkpoints: the
	// registry does not survive reopen.
	o2 := testOptions(t, dir)
	o2.ContextID = target[:]
	st2, err := spool.Open(o2)
	if err != nil {
		t.Fatalf("reopen rebound: %v", err)
	}
	defer st2.Close()
	if list := st2.ListCheckpoints(); len(list) != 0 {
		t.Fatalf("list = %d, want empty registry", len(list))
	}
}

// TestCheckpointDestinationMustBeNew requires the destination to be
// a new directory: existing paths fail without touching existing
// data, and a nil context captures without cancellation.
func TestCheckpointDestinationMustBeNew(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	taken := filepath.Join(dir, "taken")
	if err := os.Mkdir(taken, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	sentinel := filepath.Join(taken, "sentinel")
	if err := os.WriteFile(sentinel, []byte("caller-data"), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	if _, err := st.Checkpoint(context.Background(), taken); err == nil {
		t.Fatal("capture into existing directory succeeded, want failure")
	} else if !strings.Contains(err.Error(), "new directory") {
		t.Fatalf("err = %q, want new-directory rejection", err)
	}
	if raw, err := os.ReadFile(sentinel); err != nil || string(raw) != "caller-data" {
		t.Fatalf("caller data disturbed: %q, %v", raw, err)
	}
	plain := filepath.Join(dir, "plain")
	if err := os.WriteFile(plain, []byte("x"), 0o600); err != nil {
		t.Fatalf("write plain: %v", err)
	}
	if _, err := st.Checkpoint(context.Background(), plain); err == nil {
		t.Fatal("capture onto existing file succeeded, want failure")
	}
	if _, err := st.Checkpoint(context.Background(), ""); err == nil {
		t.Fatal("capture with empty destination succeeded, want failure")
	}
	// A nil context means no cancellation.
	cp, err := st.Checkpoint(nil, filepath.Join(dir, "ok"))
	if err != nil {
		t.Fatalf("nil-context Checkpoint: %v", err)
	}
	cp.Release()
}

// TestCheckpointCancelCleanup requires a pre-canceled context to
// capture nothing and a mid-capture cancel to remove the partial
// destination and register nothing.
func TestCheckpointCancelCleanup(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := testOptions(t, dir)
	o.Faults = faults
	o.MaxSegmentSize = 4096
	o.TargetBlockBytes = 1024
	o.MaxBlockBytes = 2048
	o.MaxRecordsPerBlock = 100
	o.MaxKeySize = 64
	o.MaxValueSize = 1024
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	// Three flushed rounds force three segments, so the
	// mid-capture cancel below lands between links.
	for round := 0; round < 3; round++ {
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("r%d/k%02d", round, i)
			if err := st.Put([]byte(k), incompressible(300, uint64(round*10+i))); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if err := st.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	if n := segmentCount(t, dir); n < 3 {
		t.Fatalf("setup built %d segments, want >= 3", n)
	}
	pre, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := st.Checkpoint(pre, filepath.Join(dir, "pre")); err == nil {
		t.Fatal("pre-canceled capture succeeded, want failure")
	} else if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "pre")); !os.IsNotExist(err) {
		t.Fatalf("pre-canceled capture left %v", err)
	}
	// Mid-capture cancel driven by the link hook: cancel after two
	// links, then require the partial destination gone and the
	// registry empty.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var links atomic.Int32
	faults.CheckpointLink = func() error {
		if links.Add(1) == 2 {
			cancel()
		}
		return nil
	}
	dst := filepath.Join(dir, "mid")
	if _, err := st.Checkpoint(ctx, dst); err == nil {
		t.Fatal("canceled capture succeeded, want failure")
	} else if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if links.Load() < 2 {
		t.Fatalf("only %d links ran, want mid-capture cancel", links.Load())
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("canceled capture left %v", err)
	}
	if list := st.ListCheckpoints(); len(list) != 0 {
		t.Fatalf("list = %v, want empty registry", list)
	}
	// The store stays healthy after the canceled capture.
	faults.CheckpointLink = nil
	if err := st.Put([]byte("after"), []byte("v")); err != nil {
		t.Fatalf("Put after cancel: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush after cancel: %v", err)
	}
}

// TestCheckpointLinkFault injects a link failure and requires the
// capture to fail, remove its partial destination, register
// nothing, and succeed once disarmed.
func TestCheckpointLinkFault(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := testOptions(t, dir)
	o.Faults = faults
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	faults.CheckpointLink = func() error { return fmt.Errorf("boom-link") }
	dst := filepath.Join(dir, "backup")
	if _, err := st.Checkpoint(context.Background(), dst); err == nil {
		t.Fatal("capture during link fault succeeded, want failure")
	} else if !strings.Contains(err.Error(), "boom-link") {
		t.Fatalf("err = %q, want injection cause", err)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("failed capture left %v", err)
	}
	if list := st.ListCheckpoints(); len(list) != 0 {
		t.Fatalf("list = %v, want empty registry", list)
	}
	if st.StorageError() != nil {
		t.Fatalf("StorageError = %v, want nil (ordinary failure)", st.StorageError())
	}
	faults.CheckpointLink = nil
	cp, err := st.Checkpoint(context.Background(), dst)
	if err != nil {
		t.Fatalf("Checkpoint disarmed: %v", err)
	}
	cp.Release()
}

// TestCheckpointDirSyncFault fails checkpoint directory fsyncs and
// requires an ordinary failure with the partial destination
// removed, then a successful retry once disarmed.
func TestCheckpointDirSyncFault(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := testOptions(t, dir)
	o.Faults = faults
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	faults.DirSync = func() error { return fmt.Errorf("boom-dirsync") }
	dst := filepath.Join(dir, "backup")
	if _, err := st.Checkpoint(context.Background(), dst); err == nil {
		t.Fatal("capture during dirsync fault succeeded, want failure")
	} else if !strings.Contains(err.Error(), "boom-dirsync") {
		t.Fatalf("err = %q, want injection cause", err)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("failed capture left %v", err)
	}
	if list := st.ListCheckpoints(); len(list) != 0 {
		t.Fatalf("list = %v, want empty registry", list)
	}
	if st.StorageError() != nil {
		t.Fatalf("StorageError = %v, want nil (ordinary failure)", st.StorageError())
	}
	faults.DirSync = nil
	cp := mustCheckpoint(t, st, dir, "backup")
	if got := loadDir(t, cp.Dir); string(got["k"]) != "v" {
		t.Fatalf("retried checkpoint = %v, want k=v", got)
	}
	cp.Release()
}

// TestCheckpointCrossFilesystem attempts a real cross-filesystem
// capture when the platform offers two filesystems (/dev/shm versus
// the temp dir). It skips when linking across succeeds or the
// probe cannot run; the link-fault test above covers the rejection
// path deterministically.
func TestCheckpointCrossFilesystem(t *testing.T) {
	if _, err := os.Stat("/dev/shm"); err != nil {
		t.Skip("no /dev/shm for a cross-filesystem probe")
	}
	dir := t.TempDir()
	probe := filepath.Join(dir, "probe")
	if err := os.WriteFile(probe, []byte("x"), 0o600); err != nil {
		t.Fatalf("write probe: %v", err)
	}
	anchor := fmt.Sprintf("/dev/shm/spool-xfs-probe-%d", os.Getpid())
	os.Remove(anchor)
	if err := os.Link(probe, anchor); err == nil {
		os.Remove(anchor)
		t.Skip("temp dir and /dev/shm share a filesystem")
	} else if !os.IsExist(err) && !isCrossDevice(err) {
		t.Skipf("probe link failed without EXDEV: %v", err)
	}
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	parent := fmt.Sprintf("/dev/shm/spool-xfs-%d", os.Getpid())
	if err := os.MkdirAll(parent, 0o755); err != nil {
		t.Fatalf("mkdir parent: %v", err)
	}
	defer os.RemoveAll(parent)
	dst := filepath.Join(parent, "backup")
	if _, err := st.Checkpoint(context.Background(), dst); err == nil {
		t.Fatal("cross-filesystem capture succeeded, want rejection")
	} else if !strings.Contains(err.Error(), "link checkpoint segment") {
		t.Fatalf("err = %q, want link rejection", err)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("rejected capture left %v", err)
	}
}

// TestCheckpointManifestFaultOrdinary fails the capture rotation's
// manifest publication and requires an ordinary (non-terminal)
// failure with a successful retry once disarmed.
func TestCheckpointManifestFaultOrdinary(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := testOptions(t, dir)
	o.Faults = faults
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	faults.ManifestPersist = func() error { return fmt.Errorf("boom-manifest") }
	if _, err := st.Checkpoint(context.Background(), filepath.Join(dir, "backup")); err == nil {
		t.Fatal("capture during manifest fault succeeded, want failure")
	}
	if st.StorageError() != nil {
		t.Fatalf("StorageError = %v, want nil (ordinary failure)", st.StorageError())
	}
	if _, err := os.Stat(filepath.Join(dir, "backup")); !os.IsNotExist(err) {
		t.Fatalf("failed capture left %v", err)
	}
	faults.ManifestPersist = nil
	cp := mustCheckpoint(t, st, dir, "backup")
	if got := loadDir(t, cp.Dir); string(got["k"]) != "v" {
		t.Fatalf("retried checkpoint = %v, want k=v", got)
	}
	cp.Release()
}

// TestCheckpointExcludesConcurrentPublish runs reclaim, rotation,
// commits, and a rewrite against a deliberately slowed capture and
// requires every operation to succeed (blocking, never failing)
// with a consistent captured cut and intact live data.
func TestCheckpointExcludesConcurrentPublish(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := compactTestOptions(t, dir)
	o.Faults = faults
	st := deadHeavyStore(t, o)
	defer st.Close()
	// Stretch the capture section so every publisher below must
	// serialize against it.
	faults.CheckpointLink = func() error {
		time.Sleep(25 * time.Millisecond)
		return nil
	}
	done := make(chan error, 3)
	go func() {
		for i := 0; i < 20; i++ {
			if err := st.Reclaim(); err != nil {
				done <- fmt.Errorf("reclaim: %w", err)
				return
			}
		}
		done <- nil
	}()
	go func() {
		for i := 0; i < 5; i++ {
			if err := st.RotateKey(); err != nil {
				done <- fmt.Errorf("rotate: %w", err)
				return
			}
		}
		done <- nil
	}()
	go func() {
		for i := 0; i < 10; i++ {
			m := spool.Mutation{Key: []byte(fmt.Sprintf("c%02d", i)), Value: []byte(fmt.Sprintf("cv%02d", i))}
			if err := st.Commit([]spool.Mutation{m}, spool.DurabilitySync); err != nil {
				done <- fmt.Errorf("commit: %w", err)
				return
			}
		}
		done <- nil
	}()
	// Capture in the main goroutine while publishers hammer: every
	// side must succeed, blocking rather than failing.
	cp := mustCheckpoint(t, st, dir, "backup")
	timeout := time.After(120 * time.Second)
	for i := 0; i < 3; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("concurrent op: %v", err)
			}
		case <-timeout:
			t.Fatal("concurrent capture deadlocked")
		}
	}
	// The captured cut is consistent: every pre-capture key exact,
	// every concurrent commit either fully present or fully absent.
	faults.CheckpointLink = nil
	got := loadDir(t, filepath.Join(dir, "backup"))
	for i := 0; i < 90; i++ {
		if string(got[fmt.Sprintf("k%03d", i)]) != "new" {
			t.Fatalf("k%03d lost its pre-capture value", i)
		}
	}
	for i := 90; i < 100; i++ {
		if len(got[fmt.Sprintf("k%03d", i)]) != 300 {
			t.Fatalf("k%03d survivor damaged in checkpoint", i)
		}
	}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("c%02d", i)
		if v, ok := got[k]; ok && string(v) != fmt.Sprintf("cv%02d", i) {
			t.Fatalf("%s = %q in checkpoint, want cv%02d or absent", k, v, i)
		}
	}
	// The live store converged with everything.
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	live := loadAll(t, dir)
	if len(live) != 110 {
		t.Fatalf("live holds %d keys, want 110", len(live))
	}
	if err := cp.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

// TestCheckpointRewriteSerializes runs a rewrite against a slowed
// capture and requires both to succeed with a consistent cut: the
// two serialize in whichever order they collide, never interleaving
// renames with links.
func TestCheckpointRewriteSerializes(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := compactTestOptions(t, dir)
	o.Faults = faults
	st := deadHeavyStore(t, o)
	defer st.Close()
	faults.CheckpointLink = func() error {
		time.Sleep(25 * time.Millisecond)
		return nil
	}
	done := make(chan error, 1)
	go func() {
		if err := st.RewriteDataKeys(); err != nil {
			done <- fmt.Errorf("rewrite: %w", err)
			return
		}
		done <- nil
	}()
	// Either the capture or the rewrite wins the race: a capture
	// that finds the maintenance gate held retries until the
	// rewrite finishes. Failed attempts leave no destination.
	var cp *spool.Checkpoint
	dst := filepath.Join(dir, "backup")
	deadline := time.Now().Add(60 * time.Second)
	for {
		c, err := st.Checkpoint(context.Background(), dst)
		if err == nil {
			cp = c
			break
		}
		if !errors.Is(err, spool.ErrMaintenance) || time.Now().After(deadline) {
			t.Fatalf("Checkpoint: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rewrite: %v", err)
		}
	case <-time.After(120 * time.Second):
		t.Fatal("rewrite deadlocked against capture")
	}
	faults.CheckpointLink = nil
	got := loadDir(t, cp.Dir)
	for i := 0; i < 90; i++ {
		if string(got[fmt.Sprintf("k%03d", i)]) != "new" {
			t.Fatalf("k%03d lost its value in checkpoint", i)
		}
	}
	if err := cp.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

// TestCheckpointReleaseIdempotent pins the idempotency contract:
// repeats and unknown ids succeed, and pins drop exactly once.
func TestCheckpointReleaseIdempotent(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	cp := mustCheckpoint(t, st, dir, "backup")
	refs, err := st.KeyReferences()
	if err != nil {
		t.Fatalf("KeyReferences: %v", err)
	}
	pinned := false
	for _, r := range refs {
		if r.Checkpoints > 0 {
			pinned = true
		}
	}
	if !pinned {
		t.Fatalf("no checkpoint pins: %+v", refs)
	}
	if err := cp.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := cp.Release(); err != nil {
		t.Fatalf("second Release = %v, want nil", err)
	}
	if err := st.ReleaseCheckpoint(cp.ID); err != nil {
		t.Fatalf("ReleaseCheckpoint = %v, want nil", err)
	}
	if n := st.Stats().Checkpoints; n != 0 {
		t.Fatalf("Checkpoints = %d, want 0", n)
	}
	refs, err = st.KeyReferences()
	if err != nil {
		t.Fatalf("KeyReferences: %v", err)
	}
	for _, r := range refs {
		if r.Checkpoints > 0 {
			t.Fatalf("pin survives release: %+v", r)
		}
	}
}
