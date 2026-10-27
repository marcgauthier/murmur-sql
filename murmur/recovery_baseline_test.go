package murmur

import (
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
)

// recoveryBaselineSeed fixes the PR0 fixture generator so every run builds
// the identical dataset.
const recoveryBaselineSeed = 42

const recoveryBaselineDDL = `CREATE TABLE docs (
	id TEXT NOT NULL PRIMARY KEY,
	title TEXT NOT NULL,
	body TEXT,
	payload BLOB,
	score INTEGER NOT NULL DEFAULT 0,
	rating REAL
);
CREATE TABLE events (
	id INTEGER NOT NULL PRIMARY KEY,
	doc_id TEXT NOT NULL,
	ts INTEGER NOT NULL,
	kind TEXT NOT NULL
);
CREATE INDEX idx_events_doc ON events(doc_id);
CREATE TABLE pairs (
	a INTEGER NOT NULL,
	b TEXT NOT NULL,
	n INTEGER NOT NULL,
	PRIMARY KEY (a, b)
);`

// recoveryFixtureSpec sizes one deterministic dataset.
type recoveryFixtureSpec struct {
	Docs   int
	Events int
	Pairs  int
	// Mutate applies deterministic updates, deletes, and reinserts after
	// the initial load. Benchmarks use inserts-only loads (Mutate=false);
	// the mutation mix is covered at small scale and by PR4 equivalence.
	Mutate bool
}

func (s recoveryFixtureSpec) totalRows() int { return s.Docs + s.Events + s.Pairs }

var recoveryTitles = []string{"alpha", "beta", "gamma", "delta", "omega", "note", "draft", "final"}

// newFixtureRNG returns the deterministic fixture generator.
func newFixtureRNG() *rand.Rand {
	return rand.New(rand.NewSource(recoveryBaselineSeed))
}

// buildRecoveryFixture migrates the schema and loads the deterministic
// dataset described by spec, then flushes.
func buildRecoveryFixture(t testing.TB, db *Database, spec recoveryFixtureSpec) {
	t.Helper()
	if err := db.Migrate(NewMigration(recoveryBaselineDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	insertFixtureRows(t, db, newFixtureRNG(), spec)
	if spec.Mutate {
		applyRecoveryMutations(t, db, newFixtureRNG(), spec)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// insertFixtureRows loads the deterministic inserts of spec without
// migrating or mutating.
func insertFixtureRows(t testing.TB, db *Database, rng *rand.Rand, spec recoveryFixtureSpec) {
	t.Helper()
	const batch = 5000
	for start := 0; start < spec.Docs; start += batch {
		end := min(start+batch, spec.Docs)
		rows := make([][]any, 0, end-start)
		for i := start; i < end; i++ {
			rows = append(rows, recoveryDocRow(rng, i))
		}
		execManyMust(t, db, "INSERT INTO docs (id, title, body, payload, score, rating) VALUES (?, ?, ?, ?, ?, ?)", rows)
	}
	for start := 0; start < spec.Events; start += batch {
		end := min(start+batch, spec.Events)
		rows := make([][]any, 0, end-start)
		for i := start; i < end; i++ {
			rows = append(rows, recoveryEventRow(rng, i, spec.Docs))
		}
		execManyMust(t, db, "INSERT INTO events (id, doc_id, ts, kind) VALUES (?, ?, ?, ?)", rows)
	}
	for start := 0; start < spec.Pairs; start += batch {
		end := min(start+batch, spec.Pairs)
		rows := make([][]any, 0, end-start)
		for i := start; i < end; i++ {
			rows = append(rows, []any{i % 100, fmt.Sprintf("b-%d", i), int64(i)})
		}
		execManyMust(t, db, "INSERT INTO pairs (a, b, n) VALUES (?, ?, ?)", rows)
	}
}

func execManyMust(t testing.TB, db *Database, stmt string, rows [][]any) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		_, err := sql.ExecuteMany(stmt, rows...)
		return nil, err
	}); err != nil {
		t.Fatalf("exec %q (%d rows): %v", stmt, len(rows), err)
	}
}

func recoveryDocRow(rng *rand.Rand, i int) []any {
	id := fmt.Sprintf("%032x", uint64(i)*2654435761+uint64(recoveryBaselineSeed))
	title := recoveryTitles[rng.Intn(len(recoveryTitles))] + fmt.Sprintf(" #%d \u2014 h\u00e9llo \u2713", i)
	var body any
	switch i % 7 {
	case 0:
		body = nil
	case 1:
		body = ""
	default:
		body = strings.Repeat(fmt.Sprintf("body-%d ", i), 1+rng.Intn(8))
	}
	var payload any
	switch i % 5 {
	case 0:
		payload = nil
	case 1:
		payload = []byte{}
	default:
		b := make([]byte, 1+rng.Intn(1024))
		rng.Read(b)
		payload = b
	}
	var score int64
	switch i {
	case 0:
		score = math.MinInt64
	case 1:
		score = math.MaxInt64
	default:
		score = rng.Int63n(2000000) - 1000000
	}
	var rating any
	if i%11 == 0 {
		rating = nil
	} else {
		rating = rng.NormFloat64() * 1000
	}
	return []any{id, title, body, payload, score, rating}
}

func recoveryEventRow(rng *rand.Rand, i, docs int) []any {
	kinds := []string{"create", "update", "delete", "merge", "sync"}
	doc := 0
	if docs > 0 {
		doc = i % docs
	}
	docID := fmt.Sprintf("%032x", uint64(doc)*2654435761+uint64(recoveryBaselineSeed))
	return []any{int64(i + 1), docID, int64(1700000000 + i), kinds[rng.Intn(len(kinds))]}
}

// applyRecoveryMutations deterministically updates, deletes, and reinserts
// a slice of the loaded rows.
func applyRecoveryMutations(t testing.TB, db *Database, rng *rand.Rand, spec recoveryFixtureSpec) {
	t.Helper()
	docID := func(i int) string {
		return fmt.Sprintf("%032x", uint64(i)*2654435761+uint64(recoveryBaselineSeed))
	}
	stmts := make([]string, 0)
	args := make([][]any, 0)
	push := func(stmt string, arg ...any) {
		stmts = append(stmts, stmt)
		args = append(args, arg)
	}
	for i := 0; i < spec.Docs/10; i++ {
		push("UPDATE docs SET title = ?, score = ? WHERE id = ?",
			fmt.Sprintf("updated-%d", i), rng.Int63n(1000), docID(i*3%max(spec.Docs, 1)))
	}
	deleted := make([]int, 0)
	for i := 0; i < spec.Docs/20; i++ {
		victim := (i*7 + 5) % max(spec.Docs, 1)
		deleted = append(deleted, victim)
		push("DELETE FROM docs WHERE id = ?", docID(victim))
	}
	for j, victim := range deleted {
		if j%2 == 0 {
			row := recoveryDocRow(rng, victim)
			row[1] = fmt.Sprintf("reinserted-%d", victim)
			push("INSERT INTO docs (id, title, body, payload, score, rating) VALUES (?, ?, ?, ?, ?, ?)", row...)
		}
	}
	for i := 0; i < spec.Events/20; i++ {
		push("UPDATE events SET kind = ? WHERE id = ?", "merged", int64(i*11%max(spec.Events, 1)+1))
		push("DELETE FROM events WHERE id = ?", int64(i*13%max(spec.Events, 1)+1))
	}
	const batch = 2000
	for start := 0; start < len(stmts); start += batch {
		end := min(start+batch, len(stmts))
		if _, err := db.Transaction(func(sql *Sql) (any, error) {
			for k := start; k < end; k++ {
				if _, err := sql.Execute(stmts[k], args[k]...); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}); err != nil {
			t.Fatalf("mutate: %v", err)
		}
	}
}

func formatRecoveryValue(v sqlite.Value) string {
	switch v.Type {
	case sqlite.Null:
		return "NULL"
	case sqlite.Integer:
		return fmt.Sprintf("i:%d", v.Int)
	case sqlite.Float:
		return fmt.Sprintf("f:%v", v.Float)
	case sqlite.Text:
		return "t:" + v.Str
	case sqlite.Blob:
		return "b:" + hex.EncodeToString(v.Blob)
	}
	return "?"
}

func dumpRecoveryRows(tb testing.TB, db *Database, query string) string {
	tb.Helper()
	v, err := db.Read(func(sql *Sql) (any, error) { return sql.Query(query) })
	if err != nil {
		tb.Fatalf("query %q: %v", query, err)
	}
	var sb strings.Builder
	for _, row := range v.([][]sqlite.Value) {
		for j, col := range row {
			if j > 0 {
				sb.WriteByte(0x1f)
			}
			sb.WriteString(formatRecoveryValue(col))
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}

// snapshotRecoveryState dumps every fixture table in PK order. Small scale
// only; benchmarks use probeRecoveryState.
func snapshotRecoveryState(t *testing.T, db *Database) map[string]string {
	t.Helper()
	return map[string]string{
		"docs":   dumpRecoveryRows(t, db, "SELECT id, title, body, payload, score, rating FROM docs ORDER BY id"),
		"events": dumpRecoveryRows(t, db, "SELECT id, doc_id, ts, kind FROM events ORDER BY id"),
		"pairs":  dumpRecoveryRows(t, db, "SELECT a, b, n FROM pairs ORDER BY a, b"),
	}
}

// probeRecoveryState returns per-table counts plus boundary spot checks
// (first/last 25 rows by PK). Cheap enough to run on every benchmark
// iteration as a reopen-correctness gate.
func probeRecoveryState(tb testing.TB, db *Database) (map[string]int64, map[string]string) {
	tb.Helper()
	counts := map[string]int64{}
	for _, table := range []string{"docs", "events", "pairs"} {
		v, err := db.Read(func(sql *Sql) (any, error) {
			return sql.QueryRow("SELECT COUNT(*) FROM " + table)
		})
		if err != nil {
			tb.Fatalf("count %s: %v", table, err)
		}
		counts[table] = v.([]sqlite.Value)[0].Int
	}
	spots := map[string]string{
		"docs/head":   dumpRecoveryRows(tb, db, "SELECT id, title, body, payload, score, rating FROM docs ORDER BY id LIMIT 25"),
		"docs/tail":   dumpRecoveryRows(tb, db, "SELECT id, title, body, payload, score, rating FROM docs ORDER BY id DESC LIMIT 25"),
		"events/head": dumpRecoveryRows(tb, db, "SELECT id, doc_id, ts, kind FROM events ORDER BY id LIMIT 25"),
		"events/tail": dumpRecoveryRows(tb, db, "SELECT id, doc_id, ts, kind FROM events ORDER BY id DESC LIMIT 25"),
		"pairs/head":  dumpRecoveryRows(tb, db, "SELECT a, b, n FROM pairs ORDER BY a, b LIMIT 25"),
		"pairs/tail":  dumpRecoveryRows(tb, db, "SELECT a, b, n FROM pairs ORDER BY a DESC, b DESC LIMIT 25"),
	}
	return counts, spots
}

func verifyRecoveryProbe(tb testing.TB, db *Database, wantCounts map[string]int64, wantSpots map[string]string) {
	tb.Helper()
	gotCounts, gotSpots := probeRecoveryState(tb, db)
	for table, want := range wantCounts {
		if gotCounts[table] != want {
			tb.Fatalf("table %s count = %d, want %d", table, gotCounts[table], want)
		}
	}
	for name, want := range wantSpots {
		if gotSpots[name] != want {
			tb.Fatalf("spot %s mismatch after reopen", name)
		}
	}
}

func openRecoveryDB(tb testing.TB, path string, mode OpenMode) *Database {
	tb.Helper()
	options := DefaultOptions()
	options.Mode = mode
	db, err := Open(path, testKey(), options)
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	return db
}

func dirSizeBytes(tb testing.TB, path string) int64 {
	tb.Helper()
	var total int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		tb.Fatalf("walk %s: %v", path, err)
	}
	return total
}

// TestLegacyRebuildMatchesSpool is the PR0 equivalence harness: a legacy
// reopen must reproduce the exact pre-close dataset. PR4 reuses the
// fixtures to prove fast recovery matches this path.
func TestLegacyRebuildMatchesSpool(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	spec := recoveryFixtureSpec{Docs: 300, Events: 300, Pairs: 100, Mutate: true}
	buildRecoveryFixture(t, db, spec)
	before := snapshotRecoveryState(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Deleting a non-existent recovery/ directory never affects open.
	if err := os.RemoveAll(filepath.Join(path, "recovery")); err != nil {
		t.Fatalf("remove recovery: %v", err)
	}

	db2 := openRecoveryDB(t, path, OpenExisting)
	defer db2.Close()
	after := snapshotRecoveryState(t, db2)
	for table, want := range before {
		if after[table] != want {
			t.Fatalf("table %s mismatch after legacy reopen", table)
		}
	}
	st := db2.Status()
	if st.OpenStages == nil {
		t.Fatal("missing stage timings on Status")
	}
	if st.OpenStages.Total() <= 0 {
		t.Fatalf("empty stage timings: %+v", st.OpenStages)
	}

	// Progress wiring: a tracked reopen reports every rebuilt row.
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	var terminal *OpenProgress
	options := DefaultOptions()
	options.OnOpenProgress = func(p OpenProgress) {
		cp := p
		terminal = &cp
	}
	db3, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("tracked reopen: %v", err)
	}
	defer db3.Close()
	if terminal == nil || terminal.Phase != PhaseReady {
		t.Fatalf("terminal progress = %+v", terminal)
	}
	var wantRows uint64
	for _, dump := range before {
		wantRows += uint64(strings.Count(dump, "\n"))
	}
	if terminal.RowsInserted != wantRows {
		t.Fatalf("rows_inserted = %d, want %d", terminal.RowsInserted, wantRows)
	}
}

// TestOpenIgnoresMissingRecoveryDir locks in recovery-cache disposability:
// stray or absent recovery/ state cannot break a legacy open.
func TestOpenIgnoresMissingRecoveryDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(t, path, OpenCreateNew)
	buildRecoveryFixture(t, db, recoveryFixtureSpec{Docs: 10, Events: 10, Pairs: 5})
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Absent directory.
	if err := os.RemoveAll(filepath.Join(path, "recovery")); err != nil {
		t.Fatalf("remove recovery: %v", err)
	}
	db2 := openRecoveryDB(t, path, OpenExisting)
	wantCounts, wantSpots := probeRecoveryState(t, db2)
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Present-but-empty directory is equally inert.
	if err := os.MkdirAll(filepath.Join(path, "recovery"), 0o700); err != nil {
		t.Fatalf("mkdir recovery: %v", err)
	}
	db3 := openRecoveryDB(t, path, OpenExisting)
	defer db3.Close()
	verifyRecoveryProbe(t, db3, wantCounts, wantSpots)
}

// TestRecoveryOptionsValidation covers the PR0 feature gate: nil disables,
// defaults validate, and out-of-range knobs fail open validation.
func TestRecoveryOptionsValidation(t *testing.T) {
	var nilOpts *RecoveryOptions
	if nilOpts.IsEnabled() {
		t.Fatal("nil options report enabled")
	}
	if err := nilOpts.Validate(); err != nil {
		t.Fatalf("nil validate: %v", err)
	}
	def := DefaultRecoveryOptions()
	if def.IsEnabled() {
		t.Fatal("default options report enabled")
	}
	if err := def.Validate(); err != nil {
		t.Fatalf("default validate: %v", err)
	}
	opted := DefaultRecoveryOptions()
	opted.Enabled = true
	if !opted.IsEnabled() {
		t.Fatal("enabled options report disabled")
	}
	if err := opted.Validate(); err != nil {
		t.Fatalf("enabled validate: %v", err)
	}
	filled := RecoveryOptions{Enabled: true}.WithDefaults()
	if err := filled.Validate(); err != nil {
		t.Fatalf("defaults fill validate: %v", err)
	}
	if filled.TargetRunBytes <= 0 || filled.TargetBlockBytes <= 0 ||
		filled.MaxBuildMemory <= 0 || filled.RetainGenerations <= 0 {
		t.Fatalf("defaults not filled: %+v", filled)
	}
	bad := []RecoveryOptions{
		{BuildInterval: -time.Second, TargetRunBytes: 1 << 20, TargetBlockBytes: 4096, MaxBuildMemory: 1 << 20, RetainGenerations: 1},
		{TargetRunBytes: 0, TargetBlockBytes: 4096, MaxBuildMemory: 1 << 20, RetainGenerations: 1},
		{TargetRunBytes: 1 << 20, TargetBlockBytes: 0, MaxBuildMemory: 1 << 20, RetainGenerations: 1},
		{TargetRunBytes: 1 << 20, TargetBlockBytes: 4096, MaxBuildMemory: 0, RetainGenerations: 1},
		{TargetRunBytes: 1 << 20, TargetBlockBytes: 4096, MaxBuildMemory: 1 << 20, RetainGenerations: 0},
		{TargetRunBytes: 1 << 20, TargetBlockBytes: 4096, MaxBuildMemory: 1 << 20, RetainGenerations: 99},
	}
	for i, opts := range bad {
		if err := opts.Validate(); err == nil {
			t.Fatalf("case %d unexpectedly valid", i)
		}
		options := DefaultOptions()
		if err := options.Validate(); err != nil {
			t.Fatalf("default options: %v", err)
		}
		broken := bad[1]
		options.Recovery = &broken
		if err := options.Validate(); err == nil {
			t.Fatal("options with invalid recovery unexpectedly valid")
		}
	}
}

// benchmarkLegacyOpen rebuilds the fixture once, then measures legacy
// reopen wall time with per-stage timings. Every iteration verifies row
// counts and boundary spot checks against the pre-close state.
func benchmarkLegacyOpen(b *testing.B, spec recoveryFixtureSpec) {
	dir := b.TempDir()
	path := filepath.Join(dir, "db")
	db := openRecoveryDB(b, path, OpenCreateNew)
	buildRecoveryFixture(b, db, spec)
	wantCounts, wantSpots := probeRecoveryState(b, db)
	if err := db.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}
	diskBytes := dirSizeBytes(b, path)
	b.ResetTimer()
	var stages OpenStageTimings
	var wall time.Duration
	var heapAlloc, heapSys uint64
	for i := 0; i < b.N; i++ {
		start := time.Now()
		rdb, err := Open(path, testKey(), DefaultOptions())
		wall = time.Since(start)
		if err != nil {
			b.Fatalf("reopen: %v", err)
		}
		if st := rdb.Status(); st.OpenStages != nil {
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
	b.ReportMetric(float64(wall.Milliseconds()), "startup_ms")
	b.ReportMetric(float64(stages.SpoolOpen.Milliseconds()), "spool_ms")
	b.ReportMetric(float64(stages.RowInsert.Milliseconds()), "rows_ms")
	b.ReportMetric(float64(stages.IndexBuild.Milliseconds()), "index_ms")
	b.ReportMetric(float64(stages.IntegrityCheck.Milliseconds()), "check_ms")
	b.ReportMetric(float64(stages.SearchInit.Milliseconds()), "search_ms")
	b.ReportMetric(float64(heapAlloc)/(1<<20), "heap_mb")
	b.ReportMetric(float64(heapSys)/(1<<20), "sys_mb")
	b.ReportMetric(float64(diskBytes)/(1<<20), "disk_mb")
}

func BenchmarkRecoveryLegacy_10k(b *testing.B) {
	benchmarkLegacyOpen(b, recoveryFixtureSpec{Docs: 6000, Events: 3000, Pairs: 1000})
}

func BenchmarkRecoveryLegacy_100k(b *testing.B) {
	benchmarkLegacyOpen(b, recoveryFixtureSpec{Docs: 60000, Events: 30000, Pairs: 10000})
}

func BenchmarkRecoveryLegacy_1M(b *testing.B) {
	benchmarkLegacyOpen(b, recoveryFixtureSpec{Docs: 600000, Events: 300000, Pairs: 100000})
}
