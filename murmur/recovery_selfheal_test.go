package murmur

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur/recovery"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// waitBuildAny waits for the current/last build to finish and returns
// its status even when it failed. For success-or-fail use
// waitRecoveryBuild.
func waitBuildAny(t *testing.T, db *Database, timeout time.Duration) RecoveryBuildStatus {
	t.Helper()
	if db.inner.recovery == nil {
		t.Fatal("recovery scheduler not started")
	}
	deadline := time.Now().Add(timeout)
	for {
		st := db.inner.recovery.Status()
		if !st.Running && !st.Started.IsZero() {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("build timed out: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func generationDirs(t *testing.T, recoveryDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(recoveryDir)
	if err != nil {
		t.Fatalf("read recovery dir: %v", err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			if _, ok := recovery.ParseGenerationDir(e.Name()); ok {
				out = append(out, e.Name())
			}
		}
	}
	return out
}

// A stale .building-* staging dir from a crashed build is swept at
// startup; published generations and CURRENT survive.
func TestRecoveryStaleStagingCleanup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 100, Events: 100, Pairs: 20, Mutate: true})
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	recoveryDir := filepath.Join(path, "recovery")
	stale := filepath.Join(recoveryDir, ".building-deadbeef")
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatalf("mkdir stale: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stale, "junk.rrun"), []byte("partial"), 0o600); err != nil {
		t.Fatalf("write junk: %v", err)
	}
	db3, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen with stale staging: %v", err)
	}
	defer db3.Close()
	if rs := recoveryStatus(t, db3); rs.Outcome != "cache-hit" {
		t.Fatalf("outcome = %+v, want cache-hit", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), before, "stale cleanup")
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale staging dir survives: %v", err)
	}
	if _, err := os.Stat(filepath.Join(recoveryDir, "CURRENT")); err != nil {
		t.Fatalf("CURRENT missing after cleanup: %v", err)
	}
	if gens := generationDirs(t, recoveryDir); len(gens) == 0 {
		t.Fatal("published generations removed by cleanup")
	}
}

// A missing CURRENT falls back to scanning valid generations.
func TestFastRestoreMissingCurrent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 100, Events: 100, Pairs: 20, Mutate: true})
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := os.Remove(filepath.Join(path, "recovery", "CURRENT")); err != nil {
		t.Fatalf("remove CURRENT: %v", err)
	}
	db3, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen without CURRENT: %v", err)
	}
	defer db3.Close()
	rs := recoveryStatus(t, db3)
	if rs.Outcome != "cache-hit" {
		t.Fatalf("outcome = %+v, want cache-hit via generation scan", rs)
	}
	if rs.Generation != 1 {
		t.Fatalf("generation = %d, want 1", rs.Generation)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), before, "missing CURRENT")
}

// Deleting recovery/ on a closed database falls back with a reason,
// counts the fallback, and regenerates automatically.
func TestRecoveryAutoRebuildAfterDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 100, Events: 100, Pairs: 20, Mutate: true})
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(path, "recovery")); err != nil {
		t.Fatalf("remove recovery: %v", err)
	}
	fallbacks := RecoveryFallbackTotal()
	db3, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen after delete: %v", err)
	}
	rs := recoveryStatus(t, db3)
	if rs.Outcome != "legacy-fallback" {
		t.Fatalf("outcome = %+v, want legacy-fallback", rs)
	}
	if rs.Reason == "" {
		t.Fatal("fallback reason is empty")
	}
	if got := RecoveryFallbackTotal(); got != fallbacks+1 {
		t.Fatalf("fallback total = %d, want %d", got, fallbacks+1)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), before, "delete fallback")
	// The fallback open auto-triggered a rebuild without waiting.
	if st := waitRecoveryBuild(t, db3, 60*time.Second); st.Err != "" {
		t.Fatalf("auto rebuild: %s", st.Err)
	}
	if err := db3.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db4, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen after auto rebuild: %v", err)
	}
	defer db4.Close()
	if rs := recoveryStatus(t, db4); rs.Outcome != "cache-hit" {
		t.Fatalf("outcome = %+v, want cache-hit", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db4), before, "delete rebuilt")
}

// A schema migration invalidates the cache, falls back, rebuilds,
// and serves the new schema from the cache afterwards.
func TestRecoveryAutoRebuildAfterSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 50, Events: 50, Pairs: 10})
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	if err := db2.Migrate(NewMigration(`ALTER TABLE docs ADD COLUMN extra TEXT`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db3, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen after migrate: %v", err)
	}
	if rs := recoveryStatus(t, db3); rs.Outcome != "legacy-fallback" {
		t.Fatalf("outcome = %+v, want legacy-fallback", rs)
	}
	if st := waitRecoveryBuild(t, db3, 60*time.Second); st.Err != "" {
		t.Fatalf("auto rebuild: %s", st.Err)
	}
	if err := db3.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db4, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen after schema rebuild: %v", err)
	}
	defer db4.Close()
	rs := recoveryStatus(t, db4)
	if rs.Outcome != "cache-hit" {
		t.Fatalf("outcome = %+v, want cache-hit", rs)
	}
	v, err := db4.Read(func(sql *Sql) (any, error) { return sql.Query("SELECT COUNT(*) FROM docs") })
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) != 1 || rows[0][0].Int != 50 {
		t.Fatalf("docs count = %+v, want 50", rows)
	}
}

// Removing recovery/ while an open is in flight must not fail the
// open: either the cache survived (hit) or the loader fell back.
func TestRecoveryDeleteDuringOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 500, Events: 500, Pairs: 50, Mutate: true})
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	recoveryDir := filepath.Join(path, "recovery")
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = os.RemoveAll(recoveryDir)
			}
		}
	}()
	db3, err := Open(path, testKey(), recoveryTestOptions())
	close(stop)
	<-done
	if err != nil {
		t.Fatalf("open during delete: %v", err)
	}
	defer db3.Close()
	rs := recoveryStatus(t, db3)
	switch rs.Outcome {
	case "cache-hit":
	case "legacy-fallback":
		if rs.Reason == "" {
			t.Fatal("fallback without reason")
		}
	default:
		t.Fatalf("outcome = %+v", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), before, "delete during open")
}

// Removing recovery/ while a build runs fails that build without
// harming the database; the next trigger rebuilds cleanly.
func TestRecoveryDeleteDuringBuild(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 200, Events: 200, Pairs: 20, Mutate: true})
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	options := recoveryTestOptions()
	options.Recovery.AutoBuild = false
	options.Recovery.BuildInterval = 0
	db2, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	recoveryDir := filepath.Join(path, "recovery")
	db2.inner.recovery.Trigger()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = os.RemoveAll(recoveryDir)
				time.Sleep(time.Millisecond)
			}
		}
	}()
	st := waitBuildAny(t, db2, 60*time.Second)
	close(stop)
	<-done
	// Either the deleter killed the build (recorded, non-fatal) or
	// the build won the race. The database must be healthy either way.
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db2), before, "delete during build")
	t.Logf("raced build outcome: running=%v err=%q", st.Running, st.Err)
	db2.inner.recovery.Trigger()
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("rebuild after race: %s", st.Err)
	}
	if _, err := os.Stat(filepath.Join(recoveryDir, "CURRENT")); err != nil {
		t.Fatalf("CURRENT missing after rebuild: %v", err)
	}
}

// An unwritable recovery dir fails the build (recorded in status,
// never a database error); fixing permissions lets the next build
// succeed.
func TestRecoveryUnwritableDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission test needs an unprivileged user")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 50, Events: 50, Pairs: 10})
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	recoveryDir := filepath.Join(path, "recovery")
	if err := os.MkdirAll(recoveryDir, 0o700); err != nil {
		t.Fatalf("mkdir recovery: %v", err)
	}
	if err := os.Chmod(recoveryDir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	defer func() { _ = os.Chmod(recoveryDir, 0o700) }()
	options := recoveryTestOptions()
	options.Recovery.BuildInterval = 0
	db2, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("open with unwritable recovery: %v", err)
	}
	defer db2.Close()
	if rs := recoveryStatus(t, db2); rs.Outcome != "legacy-fallback" {
		t.Fatalf("outcome = %+v, want legacy-fallback", rs)
	}
	st := waitBuildAny(t, db2, 60*time.Second)
	if st.Err == "" {
		t.Fatal("build on unwritable dir succeeded, want recorded failure")
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db2), before, "unwritable dir")
	if err := os.Chmod(recoveryDir, 0o700); err != nil {
		t.Fatalf("chmod restore: %v", err)
	}
	db2.inner.recovery.Trigger()
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("rebuild after chmod: %s", st.Err)
	}
	if _, err := os.Stat(filepath.Join(recoveryDir, "CURRENT")); err != nil {
		t.Fatalf("CURRENT missing after rebuild: %v", err)
	}
}

// Successive builds prune old generations to RetainGenerations while
// CURRENT always points at a valid generation.
func TestRecoveryRetention(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 50, Events: 50, Pairs: 10})
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	options := recoveryTestOptions()
	options.Recovery.BuildInterval = 0
	db2, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	// The open auto-triggered build 1; queue two more.
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build 1: %s", st.Err)
	}
	db2.inner.recovery.Trigger()
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build 2: %s", st.Err)
	}
	db2.inner.recovery.Trigger()
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build 3: %s", st.Err)
	}
	recoveryDir := filepath.Join(path, "recovery")
	gens := generationDirs(t, recoveryDir)
	if len(gens) != 2 {
		t.Fatalf("generations = %v, want 2 retained", gens)
	}
	raw, err := os.ReadFile(filepath.Join(recoveryDir, "CURRENT"))
	if err != nil {
		t.Fatalf("read CURRENT: %v", err)
	}
	name, _, err := recovery.ParseCurrent(raw)
	if err != nil {
		t.Fatalf("parse CURRENT: %v", err)
	}
	if name != "generation-00000003" {
		t.Fatalf("CURRENT = %q, want generation-00000003", name)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db2), before, "retention")
}

// Rotating the data key keeps a readable cache while the old key is
// retained. A cache generation whose key is gone (rotated away and
// pruned by store maintenance) resolves no cipher, so discovery
// reports a cache miss and the open falls back transparently; the
// next build re-encrypts under the current key.
//
// The full prune composition is operational: spool maintenance frees
// a retired key only once no live block references it (see spool
// TestPruneDataKeys), and any post-rotation rebuild re-encrypts the
// cache under the new key. This test pins our side of the contract:
// retained keys stay readable, unknown key refs fail closed, and
// unresolvable ciphers yield a cache miss (the Missing/Corrupt
// tests prove the miss falls back and rebuilds).
func TestRecoveryKeyRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 100, Events: 100, Pairs: 20, Mutate: true})
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	if err := db2.RotateDataKey(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Old key retained: the cache stays readable.
	db3, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen after rotate: %v", err)
	}
	defer db3.Close()
	if rs := recoveryStatus(t, db3); rs.Outcome != "cache-hit" {
		t.Fatalf("outcome = %+v, want cache-hit with retained key", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), before, "rotate retained")
	// The generation's own key ref resolves; an unknown (pruned)
	// ref fails closed, never fails open.
	recoveryDir := filepath.Join(path, "recovery")
	opener := restoreOpener(NewLogicalStore(db3.inner.store), true)
	gen, err := recovery.Discover(recoveryDir, db3.DatabaseID(), opener)
	if err != nil {
		t.Fatalf("discover with retained key: %v", err)
	}
	if _, err := opener.CipherFor(gen.Manifest.KeyRef, true); err != nil {
		t.Fatalf("resolve retained ref %q: %v", gen.Manifest.KeyRef, err)
	}
	if _, err := opener.CipherFor("spool-key:4294967295", true); err == nil {
		t.Fatal("unknown key ref resolved, want failure")
	}
	if _, err := db3.inner.store.RecoveryKey(4294967295); err == nil {
		t.Fatal("RecoveryKey(unknown) succeeded, want failure")
	}
	// When no cipher resolves, discovery reports a cache miss and
	// the open would fall back (same path as missing/corrupt).
	failing := &recovery.Opener{
		Less: recoveryRowLess,
		CipherFor: func(ref string, enc bool) (*recovery.Cipher, error) {
			return nil, errors.New("no such key")
		},
	}
	if _, err := recovery.Discover(recoveryDir, db3.DatabaseID(), failing); !errors.Is(err, recovery.ErrNoCache) {
		t.Fatalf("discover with unresolvable keys = %v, want ErrNoCache", err)
	}
}

// Backups carry manifest, keys and segments only; a restored copy
// opens through legacy recovery and regenerates its own cache.
func TestBackupExcludesRecoveryCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 100, Events: 100, Pairs: 20, Mutate: true})
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	backup := filepath.Join(dir, "backup")
	if err := CopyStoreDir(path, backup); err != nil {
		t.Fatalf("copy store: %v", err)
	}
	if _, err := os.Stat(filepath.Join(backup, "recovery")); !os.IsNotExist(err) {
		t.Fatalf("backup contains recovery dir: %v", err)
	}
	restored := filepath.Join(dir, "restored")
	ropts := DefaultRestoreOptions()
	if err := RestoreBackup(backup, restored, testKey(), &ropts); err != nil {
		t.Fatalf("restore: %v", err)
	}
	db3, err := Open(restored, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	if rs := recoveryStatus(t, db3); rs.Outcome != "legacy-fallback" {
		t.Fatalf("outcome = %+v, want legacy-fallback (no cache in backup)", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), before, "restored")
	if st := waitRecoveryBuild(t, db3, 60*time.Second); st.Err != "" {
		t.Fatalf("rebuild on restored: %s", st.Err)
	}
	if err := db3.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db4, err := Open(restored, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen restored: %v", err)
	}
	defer db4.Close()
	if rs := recoveryStatus(t, db4); rs.Outcome != "cache-hit" {
		t.Fatalf("outcome = %+v, want cache-hit", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db4), before, "restored rebuilt")
}

// Replicated rows merge through the cache path: node B builds its
// cache while syncing, reopens through a hit, and serves rows from
// both nodes exactly like the legacy path.
func TestReplicationWithRecoveryCache(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live replication test in short mode")
	}
	dir := t.TempDir()
	idA, idB := compatNodeID(0xA1), compatNodeID(0xB2)
	dbID := compatNodeID(0xD4)
	configs := compatConfigs(t, [][16]byte{idA, idB})

	dbAOptions := DefaultOptions()
	dbAOptions.Mode = OpenCreateNew
	dbAOptions.DatabaseID = &dbID
	dbAOptions.Replication = configs[0]
	dbA, err := Open(dir+"/a", testKey(), dbAOptions)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	defer dbA.Close()

	dbBOptions := recoveryTestOptions()
	dbBOptions.Mode = OpenCreateNew
	dbBOptions.DatabaseID = &dbID
	dbBOptions.Replication = configs[1]
	dbBOptions.Recovery.BuildInterval = 0
	dbB, err := Open(dir+"/b", testKey(), dbBOptions)
	if err != nil {
		t.Fatalf("open B: %v", err)
	}
	for _, db := range []*Database{dbA, dbB} {
		if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if err := db.SetReplication("users", true); err != nil {
			t.Fatalf("enable replication: %v", err)
		}
	}
	execMust(t, dbA, "INSERT INTO users (id, name, score) VALUES (1, 'ada', 10)")
	if err := dbA.Flush(); err != nil {
		t.Fatalf("flush A: %v", err)
	}
	dbA.inner.replicationMu.Lock()
	handleA := dbA.inner.replication
	dbA.inner.replicationMu.Unlock()
	dbB.inner.replicationMu.Lock()
	handleB := dbB.inner.replication
	dbB.inner.replicationMu.Unlock()
	if handleA == nil || handleB == nil {
		t.Fatal("replication handles missing")
	}
	addrA, addrB := handleA.Status().Address, handleB.Status().Address
	if err := handleA.AddPeer(*addrB); err != nil {
		t.Fatalf("add peer B: %v", err)
	}
	if err := handleB.AddPeer(*addrA); err != nil {
		t.Fatalf("add peer A: %v", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for compatRowCount(t, dbB, 1) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("row 1 never arrived on B")
		}
		time.Sleep(200 * time.Millisecond)
	}
	execMust(t, dbB, "INSERT INTO users (id, name, score) VALUES (2, 'grace', 20)")
	if err := dbB.Flush(); err != nil {
		t.Fatalf("flush B: %v", err)
	}
	deadline = time.Now().Add(60 * time.Second)
	for compatRowCount(t, dbA, 2) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("row 2 never arrived on A")
		}
		time.Sleep(200 * time.Millisecond)
	}
	// B's open auto-triggered a build of the pre-migration schema;
	// rebuild after sync so the cache captures migrated + synced rows.
	if st := waitRecoveryBuild(t, dbB, 60*time.Second); st.Err != "" {
		t.Fatalf("build on B: %s", st.Err)
	}
	dbB.inner.recovery.Trigger()
	if st := waitRecoveryBuild(t, dbB, 60*time.Second); st.Err != "" {
		t.Fatalf("rebuild on B: %s", st.Err)
	}
	if err := dbB.Close(); err != nil {
		t.Fatalf("close B: %v", err)
	}
	reopenOptions := recoveryTestOptions()
	reopenOptions.DatabaseID = &dbID
	reopenOptions.Replication = configs[1]
	dbB2, err := Open(dir+"/b", testKey(), reopenOptions)
	if err != nil {
		t.Fatalf("reopen B: %v", err)
	}
	defer dbB2.Close()
	if rs := recoveryStatus(t, dbB2); rs.Outcome != "cache-hit" {
		t.Fatalf("outcome = %+v, want cache-hit", rs)
	}
	if n := compatRowCount(t, dbB2, 1); n != 1 {
		t.Fatalf("row 1 count = %d, want 1", n)
	}
	if n := compatRowCount(t, dbB2, 2); n != 1 {
		t.Fatalf("row 2 count = %d, want 1", n)
	}
}

// Local-only tables ride the cache path like replicated ones.
func TestFastRestoreLocalOnlyTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 50, Events: 50, Pairs: 20})
	if err := db.SetReplication("pairs", false); err != nil {
		t.Fatalf("set local-only: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db3, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db3.Close()
	if rs := recoveryStatus(t, db3); rs.Outcome != "cache-hit" {
		t.Fatalf("outcome = %+v, want cache-hit", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), before, "local-only")
}

// With an encrypted store, no database value appears in plaintext
// anywhere under recovery/.
func TestRecoveryCacheHasNoPlaintext(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 30, Events: 30, Pairs: 10})
	marker := "SECRET-MARKER-9f8e7d6c5b4a"
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		_, err := sql.Execute("INSERT INTO docs (id, title, score) VALUES (?, ?, ?)",
			recoveryDocID(777000), marker, 1)
		return nil, err
	}); err != nil {
		t.Fatalf("insert marker: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	recoveryDir := filepath.Join(path, "recovery")
	var files []string
	err = filepath.WalkDir(recoveryDir, func(p string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !e.IsDir() {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no cache files to scan")
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if bytes.Contains(raw, []byte(marker)) {
			t.Fatalf("plaintext marker in %s", f)
		}
	}
}

// A spool fault during checkpoint capture fails the build (recorded
// in status) without harming the database; clearing the fault lets
// the next build succeed.
func TestRecoveryBuildCheckpointFault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 50, Events: 50, Pairs: 10})
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	faults := &spool.FaultHooks{
		CheckpointLink: func() error { return errors.New("injected link fault") },
	}
	options := recoveryTestOptions()
	options.Recovery.AutoBuild = false
	options.Recovery.BuildInterval = 0
	options.FaultHooks = faults
	db2, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	db2.inner.recovery.Trigger()
	st := waitBuildAny(t, db2, 60*time.Second)
	if st.Err == "" || !strings.Contains(st.Err, "injected link fault") {
		t.Fatalf("build status = %+v, want injected fault", st)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db2), before, "checkpoint fault")
	faults.CheckpointLink = nil
	db2.inner.recovery.Trigger()
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("rebuild after fault: %s", st.Err)
	}
	if _, err := os.Stat(filepath.Join(path, "recovery", "CURRENT")); err != nil {
		t.Fatalf("CURRENT missing after rebuild: %v", err)
	}
}

// Foreground ingestion never touches the recovery cache: file bytes
// are bit-identical across commits.
func TestRecoveryNoForegroundWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 50, Events: 50, Pairs: 10})
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	options := recoveryTestOptions()
	options.Recovery.AutoBuild = false
	db2, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	db2.inner.recovery.Trigger()
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	recoveryDir := filepath.Join(path, "recovery")
	hashDir := func() map[string][32]byte {
		out := map[string][32]byte{}
		err := filepath.WalkDir(recoveryDir, func(p string, e os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if e.IsDir() {
				return nil
			}
			raw, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(recoveryDir, p)
			out[rel] = sha256.Sum256(raw)
			return nil
		})
		if err != nil {
			t.Fatalf("walk: %v", err)
		}
		return out
	}
	before := hashDir()
	rng := newFixtureRNG()
	for i := 0; i < 100; i++ {
		id := recoveryDocID(rng.Intn(50))
		if _, err := db2.Transaction(func(sql *Sql) (any, error) {
			_, err := sql.Execute("UPDATE docs SET score = ? WHERE id = ?", rng.Int63(), id)
			return nil, err
		}); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	if err := db2.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	after := hashDir()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("recovery cache changed during foreground ingestion")
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Status surfaces the restore outcome plus cache size, capture
// cursor, age, and the latest background build.
func TestRecoveryMetrics(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	spec := recoveryFixtureSpec{Docs: 100, Events: 100, Pairs: 20}
	buildRecoveryFixture(t, db, spec)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	rs := recoveryStatus(t, db2)
	if rs.Outcome != "legacy-fallback" {
		t.Fatalf("outcome = %+v, want legacy-fallback", rs)
	}
	if rs.Reason == "" {
		t.Fatal("fallback reason is empty")
	}
	st := waitRecoveryBuild(t, db2, 60*time.Second)
	if st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	live := recoveryStatus(t, db2)
	if live.LastBuild == nil {
		t.Fatal("LastBuild is nil after a build")
	}
	if live.LastBuild.Seconds <= 0 || live.LastBuild.Rows != uint64(spec.totalRows()) {
		t.Fatalf("LastBuild = %+v", live.LastBuild)
	}
	if live.LastBuild.Bytes == 0 || live.LastBuild.Generation != st.Generation {
		t.Fatalf("LastBuild = %+v, status = %+v", live.LastBuild, st)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db3, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db3.Close()
	hit := recoveryStatus(t, db3)
	if hit.Outcome != "cache-hit" {
		t.Fatalf("outcome = %+v, want cache-hit", hit)
	}
	if hit.Generation != st.Generation {
		t.Fatalf("generation = %d, want %d", hit.Generation, st.Generation)
	}
	if hit.CacheBytes == 0 {
		t.Fatal("CacheBytes is zero on a hit")
	}
	if hit.CaptureCursor == 0 {
		t.Fatal("CaptureCursor is zero on a hit")
	}
	if hit.CacheAgeSeconds < 0 {
		t.Fatalf("CacheAgeSeconds = %f", hit.CacheAgeSeconds)
	}
	if hit.ChangedRows != 0 || hit.TotalRows != uint64(spec.totalRows()) {
		t.Fatalf("hit = %+v, want 0 changed / %d total", hit, spec.totalRows())
	}
}
