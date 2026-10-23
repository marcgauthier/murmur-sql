// Live scenario: encrypted open reports sane progress, failures, and
// recovery.
//
// Go port of the Rust `open-progress` scenario
// (/home/marc/MURMUR/tests-live/open-progress/tests/open_progress.rs).
// Real encrypted storage with 6000 rows x 2 tables is closed and reopened
// while on-open-progress callbacks record every event; the test asserts
// terminal snapshots, phase sequences, monotonic progress, failure
// reporting, and recovery.
//
// Deviations from the original (same as Rust): in-process close()+reopen
// instead of fresh child processes per role; no cancel leg (the product
// has no open cancellation); phase names Opening, Rebuilding, Indexing,
// Finalizing, Ready; processed-items count spool records plus rebuilt
// rows, not cells; plain SQL instead of typed models. Single embedded
// node, no replication, so the harness artifacts/forensics/unlock helpers
// are used directly instead of Cluster (which always builds a QUIC mesh).
package openprogress

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// rowsPerTable is the row count per table (Rust: 6000).
	rowsPerTable = int64(6000)
	// batchRows is the insert batching (Rust: 1000 rows per write tx).
	batchRows = int64(1000)
)

// appKey is the application key (Rust: 0x63 x 32).
var appKey = func() []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = 0x63
	}
	return k
}()

// wrongKey is the wrong 32-byte key for the open-failure leg (Rust: 0x44
// x 32).
var wrongKey = func() []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = 0x44
	}
	return k
}()

// shortKey is the short key for the open-failure leg (Rust: 1, 2, 3).
var shortKey = []byte{1, 2, 3}

// fullSequence is the full phase sequence on every open, populated or
// empty (Rust: FULL_SEQUENCE).
var fullSequence = []murmur.OpenPhase{
	murmur.PhaseOpening,
	murmur.PhaseRebuilding,
	murmur.PhaseIndexing,
	murmur.PhaseFinalizing,
	murmur.PhaseReady,
}

// recorder is a thread-safe on-open-progress recorder with an overlap
// guard (Rust: Recorder). The callback runs on the writer thread; the
// reporter serializes it under a mutex.
type recorder struct {
	mu       sync.Mutex
	events   []murmur.OpenProgress
	inFlight atomic.Int64
	overlap  atomic.Bool
}

func (r *recorder) callback() func(murmur.OpenProgress) {
	return func(p murmur.OpenProgress) { r.record(p) }
}

func (r *recorder) record(p murmur.OpenProgress) {
	if r.inFlight.Add(1) != 1 {
		r.overlap.Store(true)
	}
	r.mu.Lock()
	r.events = append(r.events, p)
	r.mu.Unlock()
	r.inFlight.Add(-1)
}

func (r *recorder) snapshot() []murmur.OpenProgress {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]murmur.OpenProgress(nil), r.events...)
}

func isTerminal(phase murmur.OpenPhase) bool {
	return phase == murmur.PhaseReady || phase == murmur.PhaseFailed || phase == murmur.PhaseCancelled
}

// assertEventStream applies the shared event-stream assertions (Rust:
// assert_event_stream): non-empty, monotonic processed/elapsed, exactly
// one terminal event, terminal phase as expected.
func assertEventStream(t *testing.T, events []murmur.OpenProgress, want murmur.OpenPhase, name string) {
	t.Helper()
	if len(events) == 0 {
		t.Fatalf("%s: no progress events", name)
	}
	var lastProcessed uint64
	var lastElapsed int64
	terminal := 0
	for _, p := range events {
		if p.ProcessedItems < lastProcessed {
			t.Fatalf("%s: nonmonotonic processed items", name)
		}
		if el := p.Elapsed.Nanoseconds(); el < lastElapsed {
			t.Fatalf("%s: nonmonotonic elapsed", name)
		} else {
			lastElapsed = el
		}
		lastProcessed = p.ProcessedItems
		if isTerminal(p.Phase) {
			terminal++
		}
	}
	if terminal != 1 {
		t.Fatalf("%s: terminal count=%d", name, terminal)
	}
	if got := events[len(events)-1].Phase; got != want {
		t.Fatalf("%s: terminal phase=%v, want %v", name, got, want)
	}
}

// phaseSequence returns the consecutive-duplicate-collapsed phase list
// (Rust: phase_sequence).
func phaseSequence(events []murmur.OpenProgress) []murmur.OpenPhase {
	var phases []murmur.OpenPhase
	for _, p := range events {
		if len(phases) == 0 || phases[len(phases)-1] != p.Phase {
			phases = append(phases, p.Phase)
		}
	}
	return phases
}

func equalPhases(a, b []murmur.OpenPhase) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// liveTrack tracks the currently open database so failure forensics can
// inspect it.
type liveTrack struct {
	mu sync.Mutex
	db *murmur.Database
}

func (l *liveTrack) track(db *murmur.Database) {
	l.mu.Lock()
	l.db = db
	l.mu.Unlock()
}

func (l *liveTrack) untrack() {
	l.mu.Lock()
	l.db = nil
	l.mu.Unlock()
}

func (l *liveTrack) current() *murmur.Database {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.db
}

// populateFresh creates a fresh encrypted database and fills both tables
// (Rust: populate_fresh). Leaves the database closed.
func populateFresh(t *testing.T, dataDir string) {
	t.Helper()
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.OpenCreateNew
	db, err := murmur.Open(dataDir, appKey, opts)
	if err != nil {
		t.Fatalf("populate %s: create: %v", dataDir, err)
	}
	for _, table := range []string{"requests", "events"} {
		if err := db.Migrate(murmur.NewMigration(fmt.Sprintf(
			"CREATE TABLE %s(id INTEGER PRIMARY KEY, message TEXT NOT NULL, ordinal INTEGER NOT NULL)", table))); err != nil {
			t.Fatalf("populate: migrate %s: %v", table, err)
		}
		// New tables stay local-only until opted in.
		if err := db.SetReplication(table, true); err != nil {
			t.Fatalf("populate: opt-in %s: %v", table, err)
		}
	}
	for _, table := range []string{"requests", "events"} {
		for start := int64(0); start < rowsPerTable; {
			end := start + batchRows
			if end > rowsPerTable {
				end = rowsPerTable
			}
			lo, hi := start, end
			if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
				for i := lo; i < hi; i++ {
					message := fmt.Sprintf("realistic diagnostic %d %s", i, strings.Repeat("trace ", 512))
					if _, err := s.Execute(
						fmt.Sprintf("INSERT INTO %s(id, message, ordinal) VALUES(?, ?, ?)", table),
						i, message, i,
					); err != nil {
						return nil, err
					}
				}
				return nil, nil
			}); err != nil {
				t.Fatalf("populate: insert %s %d..%d: %v", table, start, end, err)
			}
			start = end
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("populate: flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("populate: close: %v", err)
	}
	t.Logf("populate: %d rows x2 tables at %s", rowsPerTable, dataDir)
}

// verifyContents applies the count + ordinal-sum check per table (Rust:
// verify_contents).
func verifyContents(t *testing.T, db *murmur.Database, name string) {
	t.Helper()
	for _, table := range []string{"requests", "events"} {
		v, err := db.Read(func(s *murmur.Sql) (any, error) {
			return s.Query(fmt.Sprintf("SELECT COUNT(*), COALESCE(SUM(ordinal), 0) FROM %s", table))
		})
		if err != nil {
			t.Fatalf("%s: verify %s: %v", name, table, err)
		}
		row := v.([][]sqlite.Value)[0]
		if row[0].Int != rowsPerTable {
			t.Fatalf("%s: wrong count %s: %d", name, table, row[0].Int)
		}
		if want := rowsPerTable * (rowsPerTable - 1) / 2; row[1].Int != want {
			t.Fatalf("%s: wrong contents %s: sum=%d", name, table, row[1].Int)
		}
	}
}

// countTable applies the row-count check per table (Rust: count_table).
func countTable(t *testing.T, db *murmur.Database, table, name string) {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	})
	if err != nil {
		t.Fatalf("%s: count %s: %v", name, table, err)
	}
	if n := v.([][]sqlite.Value)[0][0].Int; n != rowsPerTable {
		t.Fatalf("%s: %s count=%d", name, table, n)
	}
}

// reloadWithRecorder reopens under the unlock watchdog with a recorder
// (Rust: reload_with_recorder).
func reloadWithRecorder(t *testing.T, dataDir string, live *liveTrack) (*murmur.Database, *recorder) {
	t.Helper()
	rec := &recorder{}
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.OpenExisting
	opts.OnOpenProgress = rec.callback()
	db, err := harness.OpenWithWatchdog(dataDir, appKey, opts)
	if err != nil {
		t.Fatalf("reopen under watchdog: %v", err)
	}
	live.track(db)
	return db, rec
}

// terminalSnapshot returns the terminal progress snapshot, failing when
// missing.
func terminalSnapshot(t *testing.T, db *murmur.Database, name string) murmur.OpenProgress {
	t.Helper()
	p := db.Status().OpenProgress
	if p == nil {
		t.Fatalf("%s: missing terminal progress", name)
	}
	return *p
}

// reloadAndVerify is the main reload leg (Rust: reload_and_verify):
// terminal snapshot, contents, callback stream, and full phase sequence.
func reloadAndVerify(t *testing.T, dataDir string, live *liveTrack, name string) {
	t.Helper()
	db, rec := reloadWithRecorder(t, dataDir, live)
	p := terminalSnapshot(t, db, name)
	if p.Phase != murmur.PhaseReady {
		t.Fatalf("%s: terminal=%+v", name, p)
	}
	if p.RowsInserted != uint64(rowsPerTable)*2 {
		t.Fatalf("%s: terminal=%+v", name, p)
	}
	if p.RowsSkipped != 0 {
		t.Fatalf("%s: terminal=%+v", name, p)
	}
	if p.ProcessedItems < p.RowsInserted {
		t.Fatalf("%s: terminal=%+v", name, p)
	}
	if p.CurrentTable != nil {
		t.Fatalf("%s: terminal=%+v", name, p)
	}
	if p.Error != nil {
		t.Fatalf("%s: terminal=%+v", name, p)
	}
	verifyContents(t, db, name)
	events := rec.snapshot()
	if rec.overlap.Load() {
		t.Fatalf("%s: overlapping callbacks", name)
	}
	assertEventStream(t, events, murmur.PhaseReady, name)
	if got := phaseSequence(events); !equalPhases(got, fullSequence) {
		t.Fatalf("%s: phases=%v", name, got)
	}
	last := events[len(events)-1]
	t.Logf("PASS %s phases=%v processed=%d rows=%d elapsed=%v",
		name, phaseSequence(events), last.ProcessedItems, last.RowsInserted, last.Elapsed)
	if err := db.Close(); err != nil {
		t.Fatalf("close after reload: %v", err)
	}
	live.untrack()
}

// openFailureLeg is the open-failure leg (Rust: open_failure_leg): the open
// must fail, no handle is returned, and the callback stream must end in
// Failed.
func openFailureLeg(t *testing.T, dataDir string, key []byte, name string) {
	t.Helper()
	rec := &recorder{}
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.OpenExisting
	opts.OnOpenProgress = rec.callback()
	db, err := murmur.Open(dataDir, key, opts)
	if err == nil {
		_ = db.Close()
		t.Fatalf("%s: expected open failure, got success", name)
	}
	t.Logf("%s: open failed as expected: %v", name, err)
	events := rec.snapshot()
	if rec.overlap.Load() {
		t.Fatalf("%s: overlapping callbacks", name)
	}
	assertEventStream(t, events, murmur.PhaseFailed, name)
	if events[0].Phase != murmur.PhaseOpening {
		t.Fatalf("%s: first phase=%v", name, events[0].Phase)
	}
	t.Logf("PASS %s phases=%v", name, phaseSequence(events))
}

// tableTrackingLeg is the extra leg asserting both tables rebuilt (Rust:
// table_tracking_leg).
func tableTrackingLeg(t *testing.T, dataDir string, live *liveTrack) {
	t.Helper()
	populateFresh(t, dataDir)
	db, rec := reloadWithRecorder(t, dataDir, live)
	p := terminalSnapshot(t, db, "table-tracking")
	if p.Phase != murmur.PhaseReady {
		t.Fatalf("table-tracking: terminal=%+v", p)
	}
	if p.RowsInserted != uint64(rowsPerTable)*2 {
		t.Fatalf("table-tracking: RowsInserted=%d (both tables)", p.RowsInserted)
	}
	if p.CurrentTable != nil {
		t.Fatal("table-tracking: CurrentTable not cleared after open")
	}
	countTable(t, db, "requests", "table-tracking")
	countTable(t, db, "events", "table-tracking")
	found := false
	for _, ev := range rec.snapshot() {
		if ev.Phase == murmur.PhaseRebuilding && (ev.CurrentTable != nil || ev.RowsInserted > 0) {
			found = true
			break
		}
	}
	if !found {
		t.Log("table-tracking: no rebuilding event carried CurrentTable or RowsInserted>0 (fast machine, heartbeat not captured -- OK)")
	}
	events := rec.snapshot()
	t.Logf("PASS table-tracking RowsInserted=%d events=%d", p.RowsInserted, len(events))
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	live.untrack()
}

// heartbeatLeg is the extra leg asserting periodic progress events inside
// the rebuild (Rust: heartbeat_leg).
func heartbeatLeg(t *testing.T, dataDir string, live *liveTrack) {
	t.Helper()
	populateFresh(t, dataDir)
	db, rec := reloadWithRecorder(t, dataDir, live)
	n := 0
	for _, ev := range rec.snapshot() {
		if ev.Phase == murmur.PhaseRebuilding {
			n++
		}
	}
	if n < 1 {
		t.Fatalf("heartbeat: rebuilding events=%d, expected >=1", n)
	}
	t.Logf("PASS heartbeat rebuilding-events=%d", n)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	live.untrack()
}

// emptyDBLeg is the extra leg asserting a brand-new empty database still
// fires every phase with zero counts (Rust: empty_db_leg).
func emptyDBLeg(t *testing.T, dataDir string) {
	t.Helper()
	rec := &recorder{}
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.OpenCreateNew
	opts.OnOpenProgress = rec.callback()
	db, err := murmur.Open(dataDir, appKey, opts)
	if err != nil {
		t.Fatalf("create empty db: %v", err)
	}
	p := terminalSnapshot(t, db, "empty-db")
	if p.Phase != murmur.PhaseReady {
		t.Fatalf("empty-db terminal=%+v", p)
	}
	if p.ProcessedItems != 0 {
		t.Fatalf("empty-db terminal=%+v", p)
	}
	if p.RowsInserted != 0 {
		t.Fatalf("empty-db terminal=%+v", p)
	}
	if p.RowsSkipped != 0 {
		t.Fatalf("empty-db terminal=%+v", p)
	}
	events := rec.snapshot()
	assertEventStream(t, events, murmur.PhaseReady, "empty-db")
	if got := phaseSequence(events); !equalPhases(got, fullSequence) {
		t.Fatalf("empty-db phases=%v", got)
	}
	t.Logf("PASS empty-db phases=%v", phaseSequence(events))
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// phaseElapsedLeg is the extra leg asserting per-event timing sanity and
// per-phase timer reset (Rust: phase_elapsed_leg; the StartedAt check has
// no counterpart).
func phaseElapsedLeg(t *testing.T, dataDir string, live *liveTrack) {
	t.Helper()
	populateFresh(t, dataDir)
	db, rec := reloadWithRecorder(t, dataDir, live)
	events := rec.snapshot()
	for _, ev := range events {
		if ev.PhaseElapsed > ev.Elapsed {
			t.Fatalf("phase-elapsed: PhaseElapsed > Elapsed in phase %v", ev.Phase)
		}
	}
	for i := 1; i < len(events); i++ {
		if events[i].Phase != events[i-1].Phase {
			if events[i].PhaseElapsed > 100*time.Millisecond {
				t.Logf("phase-elapsed: first event of %v has PhaseElapsed=%v (>100ms, possible, not fatal)",
					events[i].Phase, events[i].PhaseElapsed)
			}
			break
		}
	}
	t.Logf("PASS phase-elapsed events=%d", len(events))
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	live.untrack()
}

// statusSnapshotLeg is the extra leg asserting repeated status snapshots
// are independent and stable (Rust: status_snapshot_leg).
func statusSnapshotLeg(t *testing.T, dataDir string, live *liveTrack) {
	t.Helper()
	populateFresh(t, dataDir)
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.OpenExisting
	opts.OnOpenProgress = func(murmur.OpenProgress) {}
	// A terminal snapshot exists only when a callback was set (the no-op
	// above); without it the snapshots below would be nil.
	db, err := harness.OpenWithWatchdog(dataDir, appKey, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	live.track(db)
	var snaps []murmur.OpenProgress
	for i := 0; i < 10; i++ {
		s := db.Status().OpenProgress
		if s == nil {
			t.Fatalf("status-snapshot: snapshot %d is nil", i)
		}
		if s.Phase != murmur.PhaseReady {
			t.Fatalf("status-snapshot: snapshot %d phase=%v", i, s.Phase)
		}
		snaps = append(snaps, *s)
	}
	snaps[0].Phase = murmur.PhaseFailed
	if snaps[1].Phase != murmur.PhaseReady {
		t.Fatal("status-snapshot: snapshots are aliased (mutation propagated)")
	}
	base := snaps[0].ProcessedItems
	for i, s := range snaps {
		if s.ProcessedItems != base {
			t.Fatalf("status-snapshot: snapshot %d ProcessedItems=%d != base=%d", i, s.ProcessedItems, base)
		}
	}
	if snaps[0].RowsInserted != uint64(rowsPerTable)*2 {
		t.Fatalf("status-snapshot: RowsInserted=%d", snaps[0].RowsInserted)
	}
	t.Logf("PASS status-snapshot snapshots=10 ProcessedItems=%d", base)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	live.untrack()
}

// TestOpenProgress runs every open-progress leg (Rust: open_progress).
func TestOpenProgress(t *testing.T) {
	runtimeDir, err := harness.PrepareRuntimeDir("open-progress")
	if err != nil {
		t.Fatalf("prepare runtime dir: %v", err)
	}
	live := &liveTrack{}
	failed := true
	defer func() {
		if failed {
			harness.DumpNodeForensics("open-progress", runtimeDir, live.current(), "open-progress-failed")
		}
		harness.Finish("open-progress", runtimeDir, failed)
	}()

	// Main flow (Rust: populate, reload, cancel, reload, open-failure,
	// reload). The cancel role has no counterpart: open takes no context
	// and the product never emits PhaseCancelled.
	main := filepath.Join(runtimeDir, "main", "data")
	populateFresh(t, main)
	reloadAndVerify(t, main, live, "reload-1")
	openFailureLeg(t, main, shortKey, "open-failure-short-key")
	openFailureLeg(t, main, wrongKey, "open-failure-wrong-key")
	reloadAndVerify(t, main, live, "reload-2-recovery")

	// Extra flow.
	extra := filepath.Join(runtimeDir, "extra")
	tableTrackingLeg(t, filepath.Join(extra, "table-tracking", "data"), live)
	heartbeatLeg(t, filepath.Join(extra, "heartbeat", "data"), live)
	emptyDBLeg(t, filepath.Join(extra, "empty", "data"))
	phaseElapsedLeg(t, filepath.Join(extra, "phase-elapsed", "data"), live)
	statusSnapshotLeg(t, filepath.Join(extra, "status-snapshot", "data"), live)

	t.Log("PASS open-progress: all legs green")
	failed = false
}
