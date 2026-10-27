package murmur

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
)

func recoveryDocID(i int) string {
	return fmt.Sprintf("%032x", uint64(i)*2654435761+uint64(recoveryBaselineSeed))
}

func assertSnapshotsEqual(t *testing.T, got, want map[string]string, what string) {
	t.Helper()
	for table, w := range want {
		if got[table] != w {
			t.Fatalf("%s: table %s mismatch", what, table)
		}
	}
	for table := range got {
		if _, ok := want[table]; !ok {
			t.Fatalf("%s: unexpected table %s", what, table)
		}
	}
}

func recoveryStatus(t *testing.T, db *Database) RecoveryStatus {
	t.Helper()
	st := db.Status()
	if st.Recovery == nil {
		t.Fatal("missing recovery status")
	}
	return *st.Recovery
}

// applyStaleWrites deterministically mutates a slice of the fixture:
// updates, deletes, and new inserts.
func applyStaleWrites(t *testing.T, db *Database, spec recoveryFixtureSpec, seed int64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	updates := max(spec.Docs/20, 1)
	deletes := max(spec.Docs/40, 1)
	inserts := max(spec.Docs/40, 1)
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		for i := 0; i < updates; i++ {
			id := recoveryDocID(rng.Intn(max(spec.Docs, 1)))
			if _, err := sql.Execute("UPDATE docs SET title = ?, score = ? WHERE id = ?",
				fmt.Sprintf("stale-%d-%d", seed, i), rng.Int63n(1000), id); err != nil {
				return nil, err
			}
		}
		for i := 0; i < deletes; i++ {
			id := recoveryDocID(rng.Intn(max(spec.Docs, 1)))
			if _, err := sql.Execute("DELETE FROM docs WHERE id = ?", id); err != nil {
				return nil, err
			}
		}
		for i := 0; i < inserts; i++ {
			id := recoveryDocID(1_000_000_000 + int(seed)*100000 + i)
			if _, err := sql.Execute("INSERT INTO docs (id, title, body, payload, score, rating) VALUES (?, ?, ?, ?, ?, ?)",
				id, fmt.Sprintf("new-%d", i), "fresh", []byte{byte(i)}, int64(i), float64(i)); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("stale writes: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestFastRestoreExactHit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 200, Events: 200, Pairs: 50, Mutate: true})
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Exact hit: no writes since capture.
	db3, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("fast reopen: %v", err)
	}
	defer db3.Close()
	rs := recoveryStatus(t, db3)
	if rs.Outcome != "cache-hit" || rs.ChangedRows != 0 {
		t.Fatalf("outcome = %+v, want clean cache-hit", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), before, "exact hit")
	// Forced legacy agrees.
	if err := db3.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db4 := reopenTestDB(t, path)
	defer db4.Close()
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db4), before, "forced legacy")
}

func TestFastRestoreStale(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	spec := recoveryFixtureSpec{Docs: 200, Events: 200, Pairs: 50, Mutate: true}
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, spec)
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
	applyStaleWrites(t, db2, spec, 1)
	want := snapshotRecoveryState(t, db2)
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db3, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("stale reopen: %v", err)
	}
	defer db3.Close()
	rs := recoveryStatus(t, db3)
	if rs.Outcome != "cache-hit" || rs.ChangedRows == 0 {
		t.Fatalf("outcome = %+v, want stale cache-hit", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), want, "stale hit")
	if err := db3.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db4 := reopenTestDB(t, path)
	defer db4.Close()
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db4), want, "forced legacy")
}

func TestFastRestoreFallbackCorrupt(t *testing.T) {
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
	// Corrupt a run file past its header.
	genDir := filepath.Join(path, "recovery", "generation-00000001")
	entries, err := os.ReadDir(genDir)
	if err != nil {
		t.Fatalf("read gen: %v", err)
	}
	corrupted := false
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".rrun") {
			p := filepath.Join(genDir, e.Name())
			raw, err := os.ReadFile(p)
			if err != nil {
				t.Fatalf("read run: %v", err)
			}
			raw[100] ^= 0xff
			if err := os.WriteFile(p, raw, 0o600); err != nil {
				t.Fatalf("write run: %v", err)
			}
			corrupted = true
			break
		}
	}
	if !corrupted {
		t.Fatal("no run file to corrupt")
	}
	db3, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("corrupt reopen: %v", err)
	}
	defer db3.Close()
	if rs := recoveryStatus(t, db3); rs.Outcome != "legacy-fallback" {
		t.Fatalf("outcome = %+v, want legacy-fallback", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), before, "corrupt fallback")
	// Auto-rebuild regenerates; the next open hits the cache.
	if st := waitRecoveryBuild(t, db3, 60*time.Second); st.Err != "" {
		t.Fatalf("rebuild: %s", st.Err)
	}
	if err := db3.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db4, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db4.Close()
	if rs := recoveryStatus(t, db4); rs.Outcome != "cache-hit" {
		t.Fatalf("outcome = %+v, want cache-hit", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db4), before, "rebuilt hit")
}

func TestFastRestoreFallbackSchemaChange(t *testing.T) {
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
	if err := db2.Migrate(NewMigration("CREATE TABLE extra (id INTEGER NOT NULL PRIMARY KEY, v TEXT)")); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db2.Transaction(func(sql *Sql) (any, error) {
		return sql.Execute("INSERT INTO extra (id, v) VALUES (1, 'one')")
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db2.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	want := snapshotRecoveryState(t, db2)
	extra, err := db2.Read(func(sql *Sql) (any, error) { return sql.Query("SELECT id, v FROM extra ORDER BY id") })
	if err != nil {
		t.Fatalf("read extra: %v", err)
	}
	_ = extra
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db3, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("schema-change reopen: %v", err)
	}
	defer db3.Close()
	if rs := recoveryStatus(t, db3); rs.Outcome != "legacy-fallback" {
		t.Fatalf("outcome = %+v, want legacy-fallback", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), want, "schema fallback")
	v, err := db3.Read(func(sql *Sql) (any, error) { return sql.Query("SELECT v FROM extra WHERE id = 1") })
	if err != nil {
		t.Fatalf("read extra: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) != 1 || rows[0][0].Str != "one" {
		t.Fatalf("extra = %+v", rows)
	}
}

func TestFastRestoreFallbackMissing(t *testing.T) {
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
	db3, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("missing-cache reopen: %v", err)
	}
	defer db3.Close()
	if rs := recoveryStatus(t, db3); rs.Outcome != "legacy-fallback" {
		t.Fatalf("outcome = %+v, want legacy-fallback", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), before, "missing fallback")
	if st := waitRecoveryBuild(t, db3, 60*time.Second); st.Err != "" {
		t.Fatalf("rebuild: %s", st.Err)
	}
	if _, err := os.Stat(filepath.Join(path, "recovery", "CURRENT")); err != nil {
		t.Fatalf("CURRENT not regenerated: %v", err)
	}
}

func TestFastRestoreManyUpdates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 20, Events: 20, Pairs: 5})
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
	target := recoveryDocID(3)
	for i := 0; i < 100; i++ {
		if _, err := db2.Transaction(func(sql *Sql) (any, error) {
			return sql.Execute("UPDATE docs SET score = ? WHERE id = ?", i, target)
		}); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
	if err := db2.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	want := snapshotRecoveryState(t, db2)
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
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db3), want, "100 updates")
}

func TestFastRestoreRandomizedMatrix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 150, Events: 150, Pairs: 40})
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	options := recoveryTestOptions()
	options.Recovery.AutoBuild = false // manual triggers keep rounds deterministic
	db2, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db2.inner.recovery.Trigger()
	if st := waitRecoveryBuild(t, db2, 60*time.Second); st.Err != "" {
		t.Fatalf("build: %s", st.Err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	rng := rand.New(rand.NewSource(777))
	nextID := 1_000_000
	for round := 0; round < 3; round++ {
		dbFast, err := Open(path, testKey(), options)
		if err != nil {
			t.Fatalf("round %d fast open: %v", round, err)
		}
		fastSnap := snapshotRecoveryState(t, dbFast)
		if err := dbFast.Close(); err != nil {
			t.Fatalf("round %d close: %v", round, err)
		}
		dbLegacy := reopenTestDB(t, path)
		legacySnap := snapshotRecoveryState(t, dbLegacy)
		assertSnapshotsEqual(t, fastSnap, legacySnap, fmt.Sprintf("round %d", round))
		// Mutate through the legacy handle (recovery off changes nothing
		// about the write path).
		if _, err := dbLegacy.Transaction(func(sql *Sql) (any, error) {
			for i := 0; i < 60; i++ {
				switch rng.Intn(4) {
				case 0: // update
					id := recoveryDocID(rng.Intn(150))
					if _, err := sql.Execute("UPDATE docs SET score = ? WHERE id = ?", rng.Int63(), id); err != nil {
						return nil, err
					}
				case 1: // delete (+ occasional reinsert)
					id := recoveryDocID(rng.Intn(150))
					if _, err := sql.Execute("DELETE FROM docs WHERE id = ?", id); err != nil {
						return nil, err
					}
					if rng.Intn(2) == 0 {
						if _, err := sql.Execute("INSERT INTO docs (id, title, score) VALUES (?, ?, ?)",
							id, "reinserted", rng.Int63n(100)); err != nil {
							return nil, err
						}
					}
				case 2: // insert
					nextID++
					if _, err := sql.Execute("INSERT INTO docs (id, title, score) VALUES (?, ?, ?)",
						recoveryDocID(nextID), "matrix", rng.Int63n(100)); err != nil {
						return nil, err
					}
				case 3: // events churn
					if _, err := sql.Execute("UPDATE events SET kind = ? WHERE id = ?",
						"matrix", int64(rng.Intn(150)+1)); err != nil {
						return nil, err
					}
				}
			}
			return nil, nil
		}); err != nil {
			t.Fatalf("round %d mutate: %v", round, err)
		}
		if err := dbLegacy.Flush(); err != nil {
			t.Fatalf("round %d flush: %v", round, err)
		}
		if err := dbLegacy.Close(); err != nil {
			t.Fatalf("round %d close: %v", round, err)
		}
	}
}

func TestFastRestoreProgressCounts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 80, Events: 80, Pairs: 20, Mutate: true})
	var wantRows uint64
	for _, dump := range snapshotRecoveryState(t, db) {
		wantRows += uint64(strings.Count(dump, "\n"))
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
	var terminal *OpenProgress
	options := recoveryTestOptions()
	options.OnOpenProgress = func(p OpenProgress) {
		cp := p
		terminal = &cp
	}
	db3, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("fast reopen: %v", err)
	}
	defer db3.Close()
	if terminal == nil || terminal.Phase != PhaseReady {
		t.Fatalf("terminal = %+v", terminal)
	}
	if terminal.RowsInserted != wantRows {
		t.Fatalf("rows_inserted = %d, want %d", terminal.RowsInserted, wantRows)
	}
}

func TestFastRestoreEmptyCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
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
		t.Fatalf("fast reopen: %v", err)
	}
	if rs := recoveryStatus(t, db3); rs.Outcome != "cache-hit" || rs.TotalRows != 0 {
		t.Fatalf("outcome = %+v", rs)
	}
	// Writes after an empty capture restore through the stale path.
	if err := db3.Migrate(NewMigration(recoveryBaselineDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	buildRecoveryFixtureRowsOnly(t, db3, recoveryFixtureSpec{Docs: 10, Events: 10, Pairs: 5})
	want := snapshotRecoveryState(t, db3)
	if err := db3.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db4, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		t.Fatalf("stale reopen: %v", err)
	}
	defer db4.Close()
	// Schema changed (migration), so this is a fallback, still exact.
	if rs := recoveryStatus(t, db4); rs.Outcome != "legacy-fallback" {
		t.Fatalf("outcome = %+v, want legacy-fallback", rs)
	}
	assertSnapshotsEqual(t, snapshotRecoveryState(t, db4), want, "post-migration")
}

// buildRecoveryFixtureRowsOnly loads rows into an already-migrated DB.
func buildRecoveryFixtureRowsOnly(t *testing.T, db *Database, spec recoveryFixtureSpec) {
	t.Helper()
	// Migrate is idempotent for identical DDL; the loader below
	// inserts without re-migrating.
	rng := newFixtureRNG()
	insertFixtureRows(t, db, rng, spec)
	applyRecoveryMutations(t, db, rng, spec)
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func benchmarkCacheOpen(b *testing.B, spec recoveryFixtureSpec, stale bool) {
	dir := b.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(b, path, OpenCreateNew)
	buildRecoveryFixture(b, db, spec)
	if err := db.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}
	db2, err := Open(path, testKey(), recoveryTestOptions())
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	st := waitRecoveryBuild(b, db2, 600*time.Second)
	if st.Err != "" {
		b.Fatalf("build: %s", st.Err)
	}
	if err := db2.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}
	if stale {
		dbm, err := Open(path, testKey(), DefaultOptions())
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		applyCacheStaleWrites(b, dbm, spec)
		if err := dbm.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
	// Expected state after all setup.
	dbv, err := Open(path, testKey(), DefaultOptions())
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	wantCounts, wantSpots := probeRecoveryState(b, dbv)
	if err := dbv.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}
	b.ResetTimer()
	var stages OpenStageTimings
	var wall time.Duration
	var changed uint64
	var heapAlloc, heapSys uint64
	for i := 0; i < b.N; i++ {
		start := time.Now()
		rdb, err := Open(path, testKey(), recoveryTestOptions())
		wall = time.Since(start)
		if err != nil {
			b.Fatalf("reopen: %v", err)
		}
		st := rdb.Status()
		if st.Recovery == nil || st.Recovery.Outcome != "cache-hit" {
			b.Fatalf("outcome = %+v, want cache-hit", st.Recovery)
		}
		changed = st.Recovery.ChangedRows
		if st.OpenStages != nil {
			stages = *st.OpenStages
		}
		verifyRecoveryProbe(b, rdb, wantCounts, wantSpots)
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		heapAlloc, heapSys = ms.HeapAlloc, ms.Sys
		if err := rdb.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(spec.totalRows()), "rows")
	b.ReportMetric(float64(changed), "changed")
	b.ReportMetric(float64(wall.Milliseconds()), "startup_ms")
	b.ReportMetric(float64(stages.SpoolOpen.Milliseconds()), "spool_ms")
	b.ReportMetric(float64(stages.RecoveryDiff.Milliseconds()), "diff_ms")
	b.ReportMetric(float64(stages.RowInsert.Milliseconds()), "rows_ms")
	b.ReportMetric(float64(stages.IndexBuild.Milliseconds()), "index_ms")
	b.ReportMetric(float64(stages.IntegrityCheck.Milliseconds()), "check_ms")
	b.ReportMetric(float64(stages.SearchInit.Milliseconds()), "search_ms")
	b.ReportMetric(float64(heapAlloc)/(1<<20), "heap_mb")
	b.ReportMetric(float64(heapSys)/(1<<20), "sys_mb")
}

// applyCacheStaleWrites applies ~1% updates, ~1% inserts, ~1% deletes.
func applyCacheStaleWrites(tb testing.TB, db *Database, spec recoveryFixtureSpec) {
	tb.Helper()
	pct := func(n int) int { return max(n/100, 1) }
	updates, inserts, deletes := pct(spec.Docs), pct(spec.Docs), pct(spec.Docs)
	rng := newFixtureRNG()
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		for i := 0; i < updates; i++ {
			id := recoveryDocID(rng.Intn(max(spec.Docs, 1)))
			if _, err := sql.Execute("UPDATE docs SET score = ? WHERE id = ?", rng.Int63(), id); err != nil {
				return nil, err
			}
		}
		for i := 0; i < deletes; i++ {
			id := recoveryDocID(rng.Intn(max(spec.Docs, 1)))
			if _, err := sql.Execute("DELETE FROM docs WHERE id = ?", id); err != nil {
				return nil, err
			}
		}
		for i := 0; i < inserts; i++ {
			id := recoveryDocID(2_000_000_000 + i)
			if _, err := sql.Execute("INSERT INTO docs (id, title, score) VALUES (?, ?, ?)",
				id, "stale-bench", i); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}); err != nil {
		tb.Fatalf("stale writes: %v", err)
	}
	if err := db.Flush(); err != nil {
		tb.Fatalf("flush: %v", err)
	}
}

func BenchmarkRecoveryCacheExact_100k(b *testing.B) {
	benchmarkCacheOpen(b, recoveryFixtureSpec{Docs: 60000, Events: 30000, Pairs: 10000}, false)
}

func BenchmarkRecoveryCacheStale_100k(b *testing.B) {
	benchmarkCacheOpen(b, recoveryFixtureSpec{Docs: 60000, Events: 30000, Pairs: 10000}, true)
}

func BenchmarkRecoveryCacheExact_1M(b *testing.B) {
	benchmarkCacheOpen(b, recoveryFixtureSpec{Docs: 600000, Events: 300000, Pairs: 100000}, false)
}

func BenchmarkRecoveryCacheStale_1M(b *testing.B) {
	benchmarkCacheOpen(b, recoveryFixtureSpec{Docs: 600000, Events: 300000, Pairs: 100000}, true)
}
