package spool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// backdateCurrentKey moves the live current key's creation time into
// the past so age rotation triggers deterministically, without
// wall-clock sleeps. persist controls whether the backdate is also
// written to the envelope (needed when a reopen must observe it).
func backdateCurrentKey(t *testing.T, st *Store, age time.Duration, persist bool) {
	t.Helper()
	st.ring.mu.Lock()
	k, ok := st.ring.keys[st.ring.current]
	if !ok {
		st.ring.mu.Unlock()
		t.Fatal("no current key")
	}
	k.CreatedAt = time.Now().Add(-age).UnixNano()
	st.ring.mu.Unlock()
	if persist {
		st.manifestMu.Lock()
		storeID := st.man.storeID
		st.manifestMu.Unlock()
		if err := st.ring.writeKeys(st.dir, storeID, shutdownKey, ""); err != nil {
			t.Fatalf("persist backdate: %v", err)
		}
	}
}

// TestAgeRotationOnAdmission backdates the current key and requires
// the next admission to rotate before sealing with it. A fresh key
// never rotates (hour-long margin, no wall-clock race).
func TestAgeRotationOnAdmission(t *testing.T) {
	dir := t.TempDir()
	o := shutdownOptions(dir)
	o.DataKeyMaxAge = time.Hour
	st, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if inv := st.KeyInventory(); inv.ActiveDataKeyID != 1 {
		t.Fatalf("active = %d, want 1 (fresh key)", inv.ActiveDataKeyID)
	}
	backdateCurrentKey(t, st, 2*time.Hour, false)
	if err := st.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	inv := st.KeyInventory()
	if inv.ActiveDataKeyID != 2 || len(inv.DataKeys) != 2 {
		t.Fatalf("inventory = %d keys active %d, want 2 keys active 2",
			len(inv.DataKeys), inv.ActiveDataKeyID)
	}
	if n := st.Stats().Rotations; n != 1 {
		t.Fatalf("Rotations = %d, want 1", n)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The rotation persisted: reopen shows both generations.
	st2, err := Open(o)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if inv := st2.KeyInventory(); len(inv.DataKeys) != 2 {
		t.Fatalf("reopened with %d keys, want 2", len(inv.DataKeys))
	}
}

// TestAgeRotationOnOpen persists a backdated key and requires the
// next open to rotate before serving writes.
func TestAgeRotationOnOpen(t *testing.T) {
	dir := t.TempDir()
	o := shutdownOptions(dir)
	o.DataKeyMaxAge = time.Hour
	st, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	backdateCurrentKey(t, st, 2*time.Hour, true)
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st2, err := Open(o)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if inv := st2.KeyInventory(); inv.ActiveDataKeyID != 2 {
		t.Fatalf("active = %d, want 2 (rotated on open)", inv.ActiveDataKeyID)
	}
	if err := st2.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st2.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// White-box maintenance tests: gate coverage across every mutating
// API and crash-resume at each intent phase.

// TestMaintGateAllAPIs flips the gate directly and requires every
// mutating entrypoint to fail with ErrMaintenance while memory
// reads keep working.
func TestMaintGateAllAPIs(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	st.maintActive.Store(true)
	defer st.maintActive.Store(false)
	if err := st.Put([]byte("k"), []byte("v")); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("Put = %v, want ErrMaintenance", err)
	}
	if err := st.TryPut([]byte("k"), []byte("v")); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("TryPut = %v, want ErrMaintenance", err)
	}
	if err := st.Delete([]byte("k")); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("Delete = %v, want ErrMaintenance", err)
	}
	if err := st.PutBatch([]KV{{Key: []byte("k"), Value: []byte("v")}}); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("PutBatch = %v, want ErrMaintenance", err)
	}
	if err := st.DeleteBatch([][]byte{[]byte("k")}); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("DeleteBatch = %v, want ErrMaintenance", err)
	}
	if err := st.Commit([]Mutation{{Key: []byte("k"), Value: []byte("v")}}, DurabilityAsync); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("Commit = %v, want ErrMaintenance", err)
	}
	if err := st.Flush(); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("Flush = %v, want ErrMaintenance", err)
	}
	if err := st.Sync(); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("Sync = %v, want ErrMaintenance", err)
	}
	if err := st.RotateKey(); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("RotateKey = %v, want ErrMaintenance", err)
	}
	if err := st.RotateMasterKey(shutdownKey, ""); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("RotateMasterKey = %v, want ErrMaintenance", err)
	}
	if err := st.RotateWrappingKey(shutdownKey, "x"); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("RotateWrappingKey = %v, want ErrMaintenance", err)
	}
	if err := st.Reclaim(); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("Reclaim = %v, want ErrMaintenance", err)
	}
	if _, err := st.PruneDataKeys(); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("PruneDataKeys = %v, want ErrMaintenance", err)
	}
	if _, err := st.KeyReferences(); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("KeyReferences = %v, want ErrMaintenance", err)
	}
	// Memory-state reads stay available.
	_ = st.Stats()
	if inv := st.KeyInventory(); inv.MaintenancePhase != "idle" {
		t.Fatalf("phase = %q, want idle", inv.MaintenancePhase)
	}
}

// whiteNoise returns deterministic incompressible bytes.
func whiteNoise(n int, seed uint64) []byte {
	out := make([]byte, n)
	x := seed*0x9E3779B97F4A7C15 + 0x6A09E667F3BCC909
	for i := range out {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		out[i] = byte(x >> 56)
	}
	return out
}

// writeIntentFile crafts an intent directly for resume tests.
func writeIntentFile(t *testing.T, dir string, m *maintIntent) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, intentFileName), encodeIntent(m), 0o600); err != nil {
		t.Fatalf("write intent: %v", err)
	}
}

// manifestGen reads the manifest generation from disk.
func manifestGen(t *testing.T, dir string) uint64 {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, manifestFileName))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	m, err := parseManifest(raw)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	return m.generation
}

// TestResumeRewriteStaging crashes with a staging-phase rewrite
// intent and requires the next open to complete the rewrite: same
// data, bumped generation, intent gone.
func TestResumeRewriteStaging(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	crashStore(t, st)
	gen := manifestGen(t, dir)
	writeIntentFile(t, dir, &maintIntent{op: maintOpRewrite, phase: maintPhaseStaging, manifestTarget: gen + 1, totalFiles: 1})
	st2, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if g := st2.man.generation; g != gen+1 {
		t.Fatalf("generation = %d, want %d", g, gen+1)
	}
	if _, err := os.Stat(filepath.Join(dir, intentFileName)); !os.IsNotExist(err) {
		t.Fatalf("intent still present: %v", err)
	}
	if err := st2.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put after resume: %v", err)
	}
	if err := st2.Flush(); err != nil {
		t.Fatalf("Flush after resume: %v", err)
	}
}

// TestResumeRewriteManifestDone covers a crash between manifest
// publication and intent removal: resume must skip straight to
// cleanup and open normally.
func TestResumeRewriteManifestDone(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	crashStore(t, st)
	gen := manifestGen(t, dir)
	// Manifest already at the target: nothing left to publish.
	writeIntentFile(t, dir, &maintIntent{op: maintOpRewrite, phase: maintPhaseManifestDone, manifestTarget: gen, totalFiles: 1})
	st2, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if g := st2.man.generation; g != gen {
		t.Fatalf("generation = %d, want %d", g, gen)
	}
	if _, err := os.Stat(filepath.Join(dir, intentFileName)); !os.IsNotExist(err) {
		t.Fatalf("intent still present: %v", err)
	}
}

// TestResumeRebindStaging crashes with a staging-phase rebind intent
// and requires one-step reopen under the target context.
func TestResumeRebindStaging(t *testing.T) {
	dir := t.TempDir()
	o := shutdownOptions(dir)
	o.ContextID = []byte("cccccccccccccccc")
	st, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	crashStore(t, st)
	gen := manifestGen(t, dir)
	var target [16]byte
	copy(target[:], "dddddddddddddddd")
	writeIntentFile(t, dir, &maintIntent{op: maintOpRebind, phase: maintPhaseStaging,
		targetContext: target, manifestTarget: gen + 1, totalFiles: 1})
	o2 := shutdownOptions(dir)
	o2.ContextID = target[:]
	// Authentication runs before resume: pinning the target context
	// while the keyring still carries the source fails closed.
	if _, err := Open(o2); !errors.Is(err, ErrContextMismatch) {
		t.Fatalf("reopen with target = %v, want ErrContextMismatch", err)
	}
	// Resume proceeds without a pinned context and adopts the target.
	o2.ContextID = nil
	st2, err := Open(o2)
	if err != nil {
		t.Fatalf("reopen for resume: %v", err)
	}
	defer st2.Close()
	if st2.ctx != target {
		t.Fatal("live context != target after resume")
	}
	if _, err := os.Stat(filepath.Join(dir, intentFileName)); !os.IsNotExist(err) {
		t.Fatalf("intent still present: %v", err)
	}
	// The old context no longer opens the store.
	st2.Close()
	if _, err := Open(o); !errors.Is(err, ErrContextMismatch) {
		t.Fatalf("old context err = %v, want ErrContextMismatch", err)
	}
}

// TestResumeRebindKeyringPublished covers the crash between keyring
// and manifest publication: files staged, envelope already carrying
// the target context, manifest still pointing at the old one. Resume
// must unlock via the intent, skip staging and keyring, and publish
// the manifest.
func TestResumeRebindKeyringPublished(t *testing.T) {
	dir := t.TempDir()
	o := shutdownOptions(dir)
	o.ContextID = []byte("cccccccccccccccc")
	st, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	var target [16]byte
	copy(target[:], "dddddddddddddddd")
	var oldCtx [16]byte
	copy(oldCtx[:], "cccccccccccccccc")
	// Faithful window: stage every file, publish the keyring, then
	// crash before the manifest.
	keyID, key, ok := st.currentDataKey()
	if !ok {
		t.Fatal("no current key")
	}
	env := st.maintEnv()
	for _, id := range st.man.members {
		if _, err := stageSegmentFile(env, id, keyID, key, oldCtx, target, [16]byte{}, false); err != nil {
			t.Fatalf("stage file %d: %v", id, err)
		}
	}
	if err := st.publishRebindKeyring(target); err != nil {
		t.Fatalf("publishRebindKeyring: %v", err)
	}
	crashStore(t, st)
	gen := manifestGen(t, dir)
	writeIntentFile(t, dir, &maintIntent{op: maintOpRebind, phase: maintPhaseSegmentsDone,
		targetContext: target, manifestTarget: gen + 1, totalFiles: 1})
	o2 := shutdownOptions(dir)
	o2.ContextID = target[:]
	st2, err := Open(o2)
	if err != nil {
		t.Fatalf("reopen with target: %v", err)
	}
	defer st2.Close()
	if st2.ctx != target {
		t.Fatal("live context != target after resume")
	}
	if g := st2.man.generation; g != gen+1 {
		t.Fatalf("generation = %d, want %d", g, gen+1)
	}
}

// TestResumeRebindStagingIdempotent stages every file, then crashes
// with the intent still in staging phase (phase persist lost).
// Resume must re-stage idempotently through the fallback reader and
// complete the rebind.
func TestResumeRebindStagingIdempotent(t *testing.T) {
	dir := t.TempDir()
	o := shutdownOptions(dir)
	o.ContextID = []byte("cccccccccccccccc")
	st, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	var target [16]byte
	copy(target[:], "dddddddddddddddd")
	var oldCtx [16]byte
	copy(oldCtx[:], "cccccccccccccccc")
	keyID, key, ok := st.currentDataKey()
	if !ok {
		t.Fatal("no current key")
	}
	env := st.maintEnv()
	for _, id := range st.man.members {
		if _, err := stageSegmentFile(env, id, keyID, key, oldCtx, target, [16]byte{}, false); err != nil {
			t.Fatalf("stage file %d: %v", id, err)
		}
	}
	crashStore(t, st)
	gen := manifestGen(t, dir)
	writeIntentFile(t, dir, &maintIntent{op: maintOpRebind, phase: maintPhaseStaging,
		targetContext: target, manifestTarget: gen + 1, totalFiles: 1})
	o2 := shutdownOptions(dir)
	o2.ContextID = target[:]
	// Authentication runs before resume: pinning the target context
	// while the keyring still carries the source fails closed.
	if _, err := Open(o2); !errors.Is(err, ErrContextMismatch) {
		t.Fatalf("reopen with target = %v, want ErrContextMismatch", err)
	}
	// Resume proceeds without a pinned context and adopts the target.
	o2.ContextID = nil
	st2, err := Open(o2)
	if err != nil {
		t.Fatalf("reopen for resume: %v", err)
	}
	defer st2.Close()
	if st2.ctx != target {
		t.Fatal("live context != target after resume")
	}
	if g := st2.man.generation; g != gen+1 {
		t.Fatalf("generation = %d, want %d", g, gen+1)
	}
}

// TestResumeRebindMixedFiles strands one already-rebound file among
// old-context files and requires resume's per-file fallback to
// complete the rebind.
func TestResumeRebindMixedFiles(t *testing.T) {
	dir := t.TempDir()
	o := shutdownOptions(dir)
	o.ContextID = []byte("cccccccccccccccc")
	o.MaxSegmentSize = 16 << 10
	o.MaxBlockBytes = 8 << 10
	o.TargetBlockBytes = 4 << 10
	o.MaxRecordsPerBlock = 100
	o.MaxKeySize = 64
	o.MaxValueSize = 2048
	st, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Two oversized groups land in separate files.
	for g := 0; g < 2; g++ {
		muts := make([]Mutation, 10)
		for i := range muts {
			muts[i] = Mutation{Key: []byte{byte('a' + g*10 + i)}, Value: whiteNoise(1500, uint64(g*10+i))}
		}
		if err := st.Commit(muts, DurabilitySync); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	gen := st.man.generation
	var target [16]byte
	copy(target[:], "dddddddddddddddd")
	// Rewrite file 1 under the target context directly, stranding a
	// mixed store (file 1 new, file 2 old), then crash.
	keyID, key, ok := st.currentDataKey()
	if !ok {
		t.Fatal("no current key")
	}
	var oldCtx [16]byte
	copy(oldCtx[:], "cccccccccccccccc")
	env := st.maintEnv()
	if _, err := stageSegmentFile(env, 1, keyID, key, oldCtx, target, [16]byte{}, false); err != nil {
		t.Fatalf("stage file 1: %v", err)
	}
	crashStore(t, st)
	writeIntentFile(t, dir, &maintIntent{op: maintOpRebind, phase: maintPhaseStaging,
		targetContext: target, manifestTarget: gen + 1, totalFiles: 2})
	o2 := shutdownOptions(dir)
	o2.ContextID = target[:]
	o2.MaxSegmentSize = 16 << 10
	o2.MaxBlockBytes = 8 << 10
	o2.TargetBlockBytes = 4 << 10
	o2.MaxRecordsPerBlock = 100
	o2.MaxKeySize = 64
	o2.MaxValueSize = 2048
	// Authentication runs before resume: pinning the target context
	// while the keyring still carries the source fails closed.
	if _, err := Open(o2); !errors.Is(err, ErrContextMismatch) {
		t.Fatalf("reopen with target = %v, want ErrContextMismatch", err)
	}
	// Resume proceeds without a pinned context and adopts the target.
	o2.ContextID = nil
	st2, err := Open(o2)
	if err != nil {
		t.Fatalf("reopen for resume: %v", err)
	}
	defer st2.Close()
	if st2.ctx != target {
		t.Fatal("live context != target after resume")
	}
	if g := st2.man.generation; g != gen+1 {
		t.Fatalf("generation = %d, want %d", g, gen+1)
	}
}

// TestRebaseBlocksUnit pins the live-index rebase: offsets move to
// the staged layout for values and tomb winners alike.
func TestRebaseBlocksUnit(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.Put([]byte("v"), []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Delete([]byte("t")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	sh := st.idx.shardFor([]byte("v"))
	sh.mu.RLock()
	vloc := sh.values["v"]
	sh.mu.RUnlock()
	sht := st.idx.shardFor([]byte("t"))
	sht.mu.RLock()
	tloc := sht.tombs["t"].loc
	sht.mu.RUnlock()
	// Both records share one block (single flush), so one map
	// entry rebases both.
	if vloc.BlockID != tloc.BlockID {
		t.Fatal("test setup shares no block")
	}
	if _, ok := st.idx.rebaseBlocks(map[uint64]int64{vloc.BlockID: vloc.BlockOffset + 1000}); !ok {
		t.Fatal("rebase reported missing block")
	}
	want := vloc
	want.BlockOffset += 1000
	if !st.idx.pointsAt("v", want) {
		t.Fatal("pointsAt misses rebased location")
	}
	if st.idx.pointsAt("v", vloc) {
		t.Fatal("pointsAt still matches stale location")
	}
	sht.mu.RLock()
	got := sht.tombs["t"].loc
	sht.mu.RUnlock()
	if got.BlockOffset != tloc.BlockOffset+1000 {
		t.Fatalf("tomb offset = %d, want %d", got.BlockOffset, tloc.BlockOffset+1000)
	}
	if _, ok := st.idx.rebaseBlocks(map[uint64]int64{}); ok {
		t.Fatal("rebase with empty map succeeded, want missing block")
	}
}

// TestRewriteShiftThenCompact rewrites until a completion length
// shift observably moves a data block, then compacts the old file:
// relocation must find every live record through the rebased index
// (stale offsets would rewrite stillborn and lose data).
func TestRewriteShiftThenCompact(t *testing.T) {
	dir := t.TempDir()
	o := shutdownOptions(dir)
	o.MaxSegmentSize = 40 << 10
	o.MaxBlockBytes = 8 << 10
	o.TargetBlockBytes = 4 << 10
	o.MaxRecordsPerBlock = 10
	o.MaxKeySize = 64
	o.MaxValueSize = 2048
	st, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Group 1 is the drift engine: 600 records over 60 blocks. Its
	// completion (60 block identities plus a fresh digest per seal)
	// changes length on nearly every rewrite, shifting group 2.
	for i := 0; i < 600; i++ {
		k := []byte{byte('g'), byte(i >> 8), byte(i)}
		if err := st.Put(k, whiteNoise(20, uint64(i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Group 2 shares file 1; its survivor sits after group 1's
	// completion and shifts with it.
	for i := 0; i < 10; i++ {
		if err := st.Put([]byte{byte('a' + i)}, whiteNoise(20, uint64(1000+i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Supersede everything but 'a': file 1 drops to 1/610 live
	// (eligible). The overwrites rotate into file 2.
	for i := 0; i < 600; i++ {
		k := []byte{byte('g'), byte(i >> 8), byte(i)}
		if err := st.Put(k, whiteNoise(20, uint64(2000+i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	for i := 1; i < 10; i++ {
		if err := st.Put([]byte{byte('a' + i)}, whiteNoise(20, uint64(3000+i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	offOf := func() int64 {
		sh := st.idx.shardFor([]byte("a"))
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		return sh.values["a"].BlockOffset
	}
	shifted := false
	for i := 0; i < 20 && !shifted; i++ {
		before := offOf()
		if err := st.RewriteDataKeys(); err != nil {
			t.Fatalf("RewriteDataKeys: %v", err)
		}
		shifted = offOf() != before
	}
	if !shifted {
		t.Fatal("no offset shift in 20 rewrites")
	}
	comps := st.Stats().Compactions
	if err := st.Reclaim(); err != nil {
		t.Fatalf("Reclaim: %v", err)
	}
	if st.Stats().Compactions == comps {
		t.Fatal("Reclaim compacted nothing")
	}
	crashStore(t, st)
	// Reload from disk and require every key at its winning value.
	got := make(map[string][]byte)
	l, err := openLoader(LoadOptions{Path: dir, MasterKey: shutdownKey})
	if err != nil {
		t.Fatalf("openLoader: %v", err)
	}
	if _, _, _, _, err := l.scanAll(l.man.members, func(recs []Record) error {
		for _, r := range recs {
			if !r.Deleted {
				got[string(r.Key)] = append([]byte(nil), r.Value...)
			}
		}
		return nil
	}, nil, nil, false, newReadCache(32<<20)); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 610 {
		t.Fatalf("reloaded %d keys, want 610", len(got))
	}
	for i := 0; i < 600; i++ {
		k := string([]byte{byte('g'), byte(i >> 8), byte(i)})
		if string(got[k]) != string(whiteNoise(20, uint64(2000+i))) {
			t.Fatalf("drift key %d mismatch after compact", i)
		}
	}
	if string(got["a"]) != string(whiteNoise(20, 1000)) {
		t.Fatal("survivor mismatch after compact")
	}
	for i := 1; i < 10; i++ {
		k := string([]byte{byte('a' + i)})
		if string(got[k]) != string(whiteNoise(20, uint64(3000+i))) {
			t.Fatalf("key %d mismatch after compact", i)
		}
	}
}

// TestCheckpointCaptureGated requires capture to fail while
// maintenance holds the store, with release staying available.
func TestCheckpointCaptureGated(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	cp, err := st.Checkpoint(context.Background(), filepath.Join(dir, "cp"))
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	st.maintActive.Store(true)
	defer st.maintActive.Store(false)
	if _, err := st.Checkpoint(context.Background(), filepath.Join(dir, "cp2")); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("Checkpoint during maintenance = %v, want ErrMaintenance", err)
	}
	if err := cp.Release(); err != nil {
		t.Fatalf("Release during maintenance: %v", err)
	}
	// Release is idempotent: the second release succeeds.
	if err := st.ReleaseCheckpoint(cp.ID); err != nil {
		t.Fatalf("double release = %v, want nil", err)
	}
}

// TestCompletionDigestMismatchAlone re-seals a completion with a
// valid authentication tag but a tampered digest and requires the
// open to fail on the digest (not auth): the digest check runs
// independently of AEAD.
func TestCompletionDigestMismatchAlone(t *testing.T) {
	dir := t.TempDir()
	o := shutdownOptions(dir)
	st, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Capture seal parameters before the crash.
	keyID, key, ok := st.currentDataKey()
	if !ok {
		t.Fatal("no current key")
	}
	comp := st.comp
	crypt := st.crypt
	storeID := st.man.storeID
	ctx := st.man.context
	crashStore(t, st)

	segPath := filepath.Join(dir, segmentsDirName, segmentFileName(1))
	orig, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	f, err := os.Open(segPath)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	stt, err := f.Stat()
	if err != nil {
		f.Close()
		t.Fatalf("stat: %v", err)
	}
	groups, _, _, _, err := assembleFile(f, stt.Size(), 1, false)
	f.Close()
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(groups))
	}
	g := groups[0]
	l, err := openLoader(LoadOptions{Path: dir, MasterKey: shutdownKey})
	if err != nil {
		t.Fatalf("openLoader: %v", err)
	}
	_, c, err := l.validateGroup(1, g)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	// Tamper the digest, re-seal validly, splice back.
	c.digest[0] ^= 0xff
	mode, err := modeForCompressionID(g.completion.hdr.compression)
	if err != nil {
		t.Fatalf("mode: %v", err)
	}
	frame, _, err := sealFrame(comp, crypt, mode, storeID, encodeCompletion(c), true, 0,
		keyID, key, g.completion.hdr.blockSeq, ctx)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	dataEnd := len(orig) - (blockHeaderLen + int(g.completion.hdr.sealedLen))
	rebuilt := append(append([]byte(nil), orig[:dataEnd]...), frame...)
	if err := os.WriteFile(segPath, rebuilt, 0o600); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	_, err = Open(shutdownOptions(dir))
	if err == nil {
		t.Fatal("open with bad digest succeeded, want failure")
	}
	if errors.Is(err, ErrAuthFailed) {
		t.Fatalf("err = %v, want digest failure (auth passed)", err)
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	// Control: restoring the bytes reopens cleanly.
	if err := os.WriteFile(segPath, orig, 0o600); err != nil {
		t.Fatalf("restore: %v", err)
	}
	st2, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("reopen restored: %v", err)
	}
	st2.Close()
}

// TestResumeFaultManifestFailsOpen arms manifest publication during
// a resume and requires the open to fail; disarmed, the same resume
// completes.
func TestResumeFaultManifestFailsOpen(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	crashStore(t, st)
	gen := manifestGen(t, dir)
	writeIntentFile(t, dir, &maintIntent{op: maintOpRewrite, phase: maintPhaseStaging, manifestTarget: gen + 1, totalFiles: 1})
	faults := &FaultHooks{}
	o2 := shutdownOptions(dir)
	o2.Faults = faults
	faults.ManifestPersist = func() error { return errors.New("boom-manifest") }
	if _, err := Open(o2); err == nil {
		t.Fatal("resume with manifest fault succeeded, want failure")
	}
	faults.ManifestPersist = nil
	st2, err := Open(o2)
	if err != nil {
		t.Fatalf("reopen disarmed: %v", err)
	}
	defer st2.Close()
	if g := st2.man.generation; g != gen+1 {
		t.Fatalf("generation = %d, want %d", g, gen+1)
	}
	if err := st2.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put after resume: %v", err)
	}
}

// TestCorruptIntentFailsOpen requires an unreadable intent to fail
// the open rather than guess at recovery.
func TestCorruptIntentFailsOpen(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(shutdownOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	crashStore(t, st)
	if err := os.WriteFile(filepath.Join(dir, intentFileName), []byte("garbage"), 0o600); err != nil {
		t.Fatalf("write intent: %v", err)
	}
	if _, err := Open(shutdownOptions(dir)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open err = %v, want ErrCorrupt", err)
	}
}
