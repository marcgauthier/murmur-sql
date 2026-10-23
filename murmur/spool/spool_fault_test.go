package spool_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

// failHooks arms every hook with a distinct error.
func failHooks() *spool.FaultHooks {
	return &spool.FaultHooks{
		Append:          func() error { return fmt.Errorf("boom-append") },
		SegmentSync:     func() error { return fmt.Errorf("boom-sync") },
		ManifestPersist: func() error { return fmt.Errorf("boom-manifest") },
		KeyringPersist:  func() error { return fmt.Errorf("boom-keyring") },
		IntentPersist:   func() error { return fmt.Errorf("boom-intent") },
		CompactionStage: func() error { return fmt.Errorf("boom-compact") },
		CheckpointLink:  func() error { return fmt.Errorf("boom-link") },
		Rename:          func() error { return fmt.Errorf("boom-rename") },
		DirSync:         func() error { return fmt.Errorf("boom-dirsync") },
		Delete:          func() error { return fmt.Errorf("boom-delete") },
	}
}

// TestFaultAppendTerminal fails segment appends and requires the
// terminal protocol: the commit fails, the cause is preserved,
// later operations fail fast, Close releases, and reopen recovers
// with pre-fault data intact.
func TestFaultAppendTerminal(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := testOptions(t, dir)
	o.Faults = faults
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("good"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	faults.Append = func() error { return fmt.Errorf("boom-append") }
	if err := st.Commit([]spool.Mutation{{Key: []byte("bad"), Value: []byte("v")}}, spool.DurabilitySync); err == nil {
		t.Fatal("commit during append fault succeeded, want failure")
	}
	if se := st.StorageError(); se == nil {
		t.Fatal("nil StorageError after terminal failure")
	} else if got := se.Error(); !strings.Contains(got, "boom-append") {
		t.Fatalf("StorageError = %q, want injection cause", got)
	}
	if err := st.Put([]byte("k"), []byte("v")); !errors.Is(err, spool.ErrStorageFailed) {
		t.Fatalf("Put = %v, want ErrStorageFailed", err)
	}
	if err := st.Sync(); !errors.Is(err, spool.ErrStorageFailed) {
		t.Fatalf("Sync = %v, want ErrStorageFailed", err)
	}
	if err := st.Close(); err == nil {
		t.Fatal("Close on terminal store succeeded, want error")
	}
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	got := loadAll(t, dir)
	if string(got["good"]) != "v" {
		t.Fatalf("good data = %v, want intact", got)
	}
	if _, ok := got["bad"]; ok {
		t.Fatal("failed commit survived")
	}
	if err := st2.Put([]byte("after"), []byte("v")); err != nil {
		t.Fatalf("Put after reopen: %v", err)
	}
}

// TestFaultSyncTerminal fails segment fsyncs and requires terminal
// handling with recovery on reopen.
func TestFaultSyncTerminal(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := testOptions(t, dir)
	o.Faults = faults
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Put([]byte("good"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	faults.SegmentSync = func() error { return fmt.Errorf("boom-sync") }
	if err := st.Commit([]spool.Mutation{{Key: []byte("k"), Value: []byte("v")}}, spool.DurabilitySync); err == nil {
		t.Fatal("sync commit during sync fault succeeded, want failure")
	}
	if se := st.StorageError(); se == nil {
		t.Fatal("nil StorageError after terminal failure")
	}
	st.Close()
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if got := loadAll(t, dir); string(got["good"]) != "v" {
		t.Fatalf("good data = %v, want intact", got)
	}
}

// TestFaultManifestTerminal fails manifest publication during
// rotation and requires terminal handling.
func TestFaultManifestTerminal(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := testOptions(t, dir)
	o.Faults = faults
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	faults.ManifestPersist = func() error { return fmt.Errorf("boom-manifest") }
	if err := st.RotateKey(); err == nil {
		t.Fatal("RotateKey during manifest fault succeeded, want failure")
	}
	if err := st.Put([]byte("k"), []byte("v")); !errors.Is(err, spool.ErrStorageFailed) {
		t.Fatalf("Put = %v, want ErrStorageFailed", err)
	}
	st.Close()
	if _, err := spool.Open(testOptions(t, dir)); err != nil {
		t.Fatalf("reopen: %v", err)
	}
}

// TestFaultKeyringAtomic fails keyring publication and requires the
// rotation to change nothing observable: same live current key, same
// envelope on disk, recovery on reopen.
func TestFaultKeyringAtomic(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := testOptions(t, dir)
	o.Faults = faults
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
	faults.KeyringPersist = func() error { return fmt.Errorf("boom-keyring") }
	if err := st.RotateKey(); err == nil {
		t.Fatal("RotateKey during keyring fault succeeded, want failure")
	}
	if inv := st.KeyInventory(); len(inv.DataKeys) != 1 || inv.ActiveDataKeyID != 1 {
		t.Fatalf("inventory = %+v, want single key 1", inv.DataKeys)
	}
	st.Close()
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if inv := st2.KeyInventory(); len(inv.DataKeys) != 1 {
		t.Fatalf("reopened with %d keys, want 1", len(inv.DataKeys))
	}
	if got := loadAll(t, dir); string(got["k"]) != "v" {
		t.Fatalf("data = %v, want intact", got)
	}
}

// TestFaultKeyringWrappingAtomic fails wrapping rotation and
// requires the id and material to stay put.
func TestFaultKeyringWrappingAtomic(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := testOptions(t, dir)
	o.WrappingKeyID = "w1"
	o.Faults = faults
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	faults.KeyringPersist = func() error { return fmt.Errorf("boom-keyring") }
	if err := st.RotateWrappingKey([]byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"), "w2"); err == nil {
		t.Fatal("RotateWrappingKey during fault succeeded, want failure")
	}
	if inv := st.KeyInventory(); inv.WrappingKeyID != "w1" {
		t.Fatalf("wrapping id = %q, want w1", inv.WrappingKeyID)
	}
	st.Close()
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	hint, err := spool.ReadKeyHint(dir)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if hint.WrappingKeyID != "w1" {
		t.Fatalf("hint wrapping id = %q, want w1", hint.WrappingKeyID)
	}
}

// TestFaultIntentOrdinary fails maintenance intent writes and
// requires an ordinary error: the store stays usable and the
// operation succeeds once disarmed.
func TestFaultIntentOrdinary(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := testOptions(t, dir)
	o.Faults = faults
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
	faults.IntentPersist = func() error { return fmt.Errorf("boom-intent") }
	if err := st.RewriteDataKeys(); err == nil {
		t.Fatal("rewrite during intent fault succeeded, want failure")
	}
	if st.StorageError() != nil {
		t.Fatalf("StorageError = %v, want nil (ordinary failure)", st.StorageError())
	}
	if err := st.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put after ordinary failure: %v", err)
	}
	faults.IntentPersist = nil
	if err := st.RewriteDataKeys(); err != nil {
		t.Fatalf("RewriteDataKeys disarmed: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// faultCompactionSetup builds a flushed dead-heavy store with an
// armed hook set ready for per-test arming.
func faultCompactionSetup(t *testing.T) (string, *spool.FaultHooks, *spool.Store) {
	t.Helper()
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := compactTestOptions(t, dir)
	o.Faults = faults
	return dir, faults, deadHeavyStore(t, o)
}

// driveReclaim runs Reclaim until it errors or a compaction
// completes, returning the first error (nil when a pass compacted).
func driveReclaim(t *testing.T, st *spool.Store) error {
	t.Helper()
	for i := 0; i < 200; i++ {
		err := st.Reclaim()
		if err != nil {
			return err
		}
		if st.Stats().Compactions > 0 {
			return nil
		}
	}
	t.Fatalf("200 reclaims with no compaction and no error")
	return nil
}

// assertNoStagingLitter fails when .cmp-*/.rew-* temps survive in
// the segments directory.
func assertNoStagingLitter(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "segments"))
	if err != nil {
		t.Fatalf("segments dir: %v", err)
	}
	for _, e := range entries {
		if len(e.Name()) > 0 && e.Name()[0] == '.' {
			t.Fatalf("staging litter survives: %s", e.Name())
		}
	}
}

// assertFaultCompactionRetryable requires the failed compaction to
// be ordinary (no terminal), the store healthy, the data intact,
// and the retry after disarm to compact successfully. Site pins the
// compaction wrap (proving where the fault landed); cause pins the
// injected error.
func assertFaultCompactionRetryable(t *testing.T, dir string, st *spool.Store, rerr error, site, cause string) {
	t.Helper()
	if rerr == nil {
		t.Fatalf("Reclaim during fault succeeded, want failure")
	}
	got := rerr.Error()
	if !strings.Contains(got, site) {
		t.Fatalf("Reclaim = %q, want site %q", got, site)
	}
	if !strings.Contains(got, cause) {
		t.Fatalf("Reclaim = %q, want injection cause %q", got, cause)
	}
	if st.StorageError() != nil {
		t.Fatalf("StorageError = %v, want nil (ordinary failure)", st.StorageError())
	}
	if err := st.Put([]byte("still-healthy"), []byte("v")); err != nil {
		t.Fatalf("Put after failed compaction: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush after failed compaction: %v", err)
	}
	assertNoStagingLitter(t, dir)
	data := loadAll(t, dir)
	if len(data) != 101 {
		t.Fatalf("loaded %d keys after failed compaction, want 101", len(data))
	}
}

// TestFaultCompactionStageRetryable fails compaction staging and
// requires an ordinary (retryable) failure: the victim survives,
// the store stays healthy, no staging litter remains, and the
// disarmed retry compacts with data intact.
func TestFaultCompactionStageRetryable(t *testing.T) {
	dir, faults, st := faultCompactionSetup(t)
	faults.CompactionStage = func() error { return fmt.Errorf("boom-compact") }
	rerr := driveReclaim(t, st)
	faults.CompactionStage = nil
	assertFaultCompactionRetryable(t, dir, st, rerr, "stage replacement", "boom-compact")
	for i := 0; i < 200 && st.Stats().Compactions == 0; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim disarmed: %v", err)
		}
	}
	if st.Stats().Compactions == 0 {
		t.Fatalf("no compaction after disarm; stats: %+v", st.Stats())
	}
	assertNoStagingLitter(t, dir)
	got := loadAll(t, dir)
	if len(got) != 101 {
		t.Fatalf("loaded %d keys after retry, want 101", len(got))
	}
	for i := 0; i < 90; i++ {
		if string(got[fmt.Sprintf("k%03d", i)]) != "new" {
			t.Fatalf("k%03d lost its new value", i)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestFaultCompactionSyncRetryable fails the replacement-file fsync
// and requires the same ordinary-failure protocol: nothing
// publishes, the store stays healthy, and the retry succeeds. The
// store is pre-flushed so the reclaim fence is I/O-silent and the
// sync fault lands exactly on compaction staging.
func TestFaultCompactionSyncRetryable(t *testing.T) {
	dir, faults, st := faultCompactionSetup(t)
	faults.SegmentSync = func() error { return fmt.Errorf("boom-sync") }
	rerr := driveReclaim(t, st)
	faults.SegmentSync = nil
	assertFaultCompactionRetryable(t, dir, st, rerr, "sync replacement", "boom-sync")
	driveCompactionToSuccess(t, st)
	assertNoStagingLitter(t, dir)
	if got := loadAll(t, dir); len(got) != 101 {
		t.Fatalf("loaded %d keys after retry, want 101", len(got))
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// driveCompactionToSuccess runs Reclaim until a compaction
// completes, failing on any error.
func driveCompactionToSuccess(t *testing.T, st *spool.Store) {
	t.Helper()
	for i := 0; i < 200 && st.Stats().Compactions == 0; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim disarmed: %v", err)
		}
	}
	if st.Stats().Compactions == 0 {
		t.Fatalf("no compaction after disarm; stats: %+v", st.Stats())
	}
}

// TestFaultRenameCompactionRetryable fails the replacement-file
// rename and requires an ordinary failure: nothing publishes, the
// staged temp is removed, the store stays healthy, and the
// disarmed retry compacts with data intact.
func TestFaultRenameCompactionRetryable(t *testing.T) {
	dir, faults, st := faultCompactionSetup(t)
	faults.Rename = func() error { return fmt.Errorf("boom-rename") }
	rerr := driveReclaim(t, st)
	faults.Rename = nil
	assertFaultCompactionRetryable(t, dir, st, rerr, "publish replacement file", "boom-rename")
	driveCompactionToSuccess(t, st)
	assertNoStagingLitter(t, dir)
	if got := loadAll(t, dir); len(got) != 101 {
		t.Fatalf("loaded %d keys after retry, want 101", len(got))
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestFaultDirSyncCompactionRetryable fails directory fsyncs during
// reclamation and requires ordinary failures with a converging
// retry: every tripped site returns a plain error, nothing goes
// terminal, and the disarmed passes compact.
func TestFaultDirSyncCompactionRetryable(t *testing.T) {
	dir, faults, st := faultCompactionSetup(t)
	faults.DirSync = func() error { return fmt.Errorf("boom-dirsync") }
	rerr := driveReclaim(t, st)
	faults.DirSync = nil
	assertFaultCompactionRetryable(t, dir, st, rerr, "sync segments dir", "boom-dirsync")
	driveCompactionToSuccess(t, st)
	assertNoStagingLitter(t, dir)
	if got := loadAll(t, dir); len(got) != 101 {
		t.Fatalf("loaded %d keys after retry, want 101", len(got))
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestFaultDeleteCompactionRetryable fails segment removals during
// reclamation and requires ordinary failures: the unpublished (or
// already-published) victim stays on disk, the store stays
// healthy, and the disarmed retry heals and compacts.
func TestFaultDeleteCompactionRetryable(t *testing.T) {
	dir, faults, st := faultCompactionSetup(t)
	faults.Delete = func() error { return fmt.Errorf("boom-delete") }
	rerr := driveReclaim(t, st)
	faults.Delete = nil
	assertFaultCompactionRetryable(t, dir, st, rerr, "remove segment", "boom-delete")
	driveCompactionToSuccess(t, st)
	assertNoStagingLitter(t, dir)
	if got := loadAll(t, dir); len(got) != 101 {
		t.Fatalf("loaded %d keys after retry, want 101", len(got))
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestFaultRenameMaintenanceResumes fails a rewrite at its first
// segment rename and requires the intent to persist for resume:
// the failed op is ordinary, and after disarming, a reopen
// completes the rewrite with data intact and no intent left.
func TestFaultRenameMaintenanceResumes(t *testing.T) {
	dir := t.TempDir()
	faults := &spool.FaultHooks{}
	o := testOptions(t, dir)
	o.Faults = faults
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	faults.Rename = func() error { return fmt.Errorf("boom-rename") }
	if err := st.RewriteDataKeys(); err == nil {
		t.Fatal("rewrite during rename fault succeeded, want failure")
	} else if !strings.Contains(err.Error(), "boom-rename") {
		t.Fatalf("RewriteDataKeys = %q, want injection cause", err)
	}
	if st.StorageError() != nil {
		t.Fatalf("StorageError = %v, want nil (ordinary failure)", st.StorageError())
	}
	if _, err := os.Stat(filepath.Join(dir, "maintenance.intent")); err != nil {
		t.Fatalf("intent missing after failed rewrite: %v", err)
	}
	faults.Rename = nil
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Reopen resumes the intent to completion.
	st2, err := spool.Open(o)
	if err != nil {
		t.Fatalf("reopen (resume): %v", err)
	}
	defer st2.Close()
	if _, err := os.Stat(filepath.Join(dir, "maintenance.intent")); !os.IsNotExist(err) {
		t.Fatalf("intent remains after resume: %v", err)
	}
	assertNoStagingLitter(t, dir)
	got := loadAll(t, dir)
	if len(got) != 20 || string(got["k07"]) != "v07" {
		t.Fatalf("resumed store = %d keys, want 20 intact", len(got))
	}
}

// TestOrphanSweepAtOpen plants every orphan shape (unlisted final
// file, compaction and maintenance staging temps, root temp) and
// requires the next open to sweep them all with data intact. With
// deletion faults armed the open fails instead, and the disarmed
// retry sweeps cleanly.
func TestOrphanSweepAtOpen(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := st.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	segDir := filepath.Join(dir, "segments")
	raw, err := os.ReadFile(filepath.Join(segDir, "000000000001.spool"))
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	planted := []string{
		filepath.Join(segDir, "000000000009.spool"), // unlisted final name
		filepath.Join(segDir, ".cmp-1-.tmp-orphan"), // crashed compaction staging
		filepath.Join(segDir, ".rew-1-.tmp-orphan"), // crashed maintenance staging
		filepath.Join(dir, ".keys.tmp-orphan"),      // crashed root temp
	}
	for _, p := range planted {
		if err := os.WriteFile(p, raw, 0o600); err != nil {
			t.Fatalf("plant %s: %v", p, err)
		}
	}
	faults := &spool.FaultHooks{}
	faults.Delete = func() error { return fmt.Errorf("boom-delete") }
	o := testOptions(t, dir)
	o.Faults = faults
	if _, err := spool.Open(o); err == nil {
		t.Fatal("open with sweep-delete fault succeeded, want failure")
	} else if !strings.Contains(err.Error(), "boom-delete") {
		t.Fatalf("open = %q, want injection cause", err)
	}
	faults.Delete = nil
	st2, err := spool.Open(o)
	if err != nil {
		t.Fatalf("reopen (sweep): %v", err)
	}
	defer st2.Close()
	for _, p := range planted {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("orphan survives: %s", p)
		}
	}
	assertNoStagingLitter(t, dir)
	got := loadAll(t, dir)
	if len(got) != 10 || string(got["k3"]) != "v3" {
		t.Fatalf("swept store = %d keys, want 10 intact", len(got))
	}
}
