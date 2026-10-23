// Live scenario: log GC keeps up with churn.
//
// Go port of the Rust `gc-balance` scenario
// (/home/marc/MURMUR/tests-live/gc-balance/tests/gc_balance.rs), itself a
// port of the original Go `gc-balance` scenario (TestLogGCKeepsUpWithChurn):
// three nodes mesh with aggressive log retention (1s log, 20s offline pin,
// 10 batches) while one node updates a fixed 200-key set with eight
// parallel updaters over disjoint ranges. After a 20s warmup, a 40s window
// must show GC collecting at least 90% of the origin-log batches the window
// produced, with zero GC failures; the suite ends with exact row counts and
// identical digests on all nodes (no loss). Plain SQL replaces the managed
// typed-contention HTTP API.
//
// Scope (same as Rust): version churn only; deletes are out of scope
// (delete pruning is unimplemented).
package gcbalance

import (
	"fmt"
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
	// keys is the fixed working set (Rust: 200).
	keys = 200
	// updaters is the parallel updater count over disjoint key ranges
	// (Rust: 8).
	updaters = 8
	// warmup is the steady-state warmup before the measurement window
	// (Rust: 20s sleep).
	warmup = 20 * time.Second
	// window is the measurement window on a quiet box (Rust: 40s sleep).
	window = 40 * time.Second
	// windowStretchStep is the window stretch step under load (Rust: 5s).
	windowStretchStep = 5 * time.Second
	// windowCap is the window stretch cap (Rust: 150s).
	windowCap = 150 * time.Second
	// commitFloor is the minimum commits for a meaningful balance proof
	// (Rust: 2000).
	commitFloor = 2000
	// minSweeps is the minimum full sweeps per updater for GC pressure
	// (Rust: 5).
	minSweeps = 5
	// runWaitCap is the post-stop GC-run wait cap (Rust: 180s).
	runWaitCap = 180 * time.Second
	// drainStablePolls and drainPoll bound the post-stop drain stability
	// gate (Rust: 2 stable 5s polls, 120s cap).
	drainStablePolls = 2
	drainPoll        = 5 * time.Second
	drainCap         = 120 * time.Second
	// countsTimeout and digestTimeout are the convergence deadlines
	// (Rust: 60s counts/digests, 30s value flow).
	countsTimeout = 60 * time.Second
	digestTimeout = 60 * time.Second
	valueTimeout  = 30 * time.Second
	// pollInterval is the convergence poll interval (Rust: 200ms).
	pollInterval = 200 * time.Millisecond
	// updaterCap is the hang guard for updater goroutines if the driver
	// fails before signalling stop (normal runs stop updaters long before
	// this; Rust: 600s).
	updaterCap = 600 * time.Second
	// counterSampleCap bounds one GC-counter sample attempt (the retention
	// and collect paths cross the engine command channel, which can
	// transiently refuse under churn; Rust blocks instead).
	counterSampleCap = 30 * time.Second
)

// ddl is the shared contention table (Rust: TypedRecords/TypedContention
// tables).
const ddl = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)"

// gcRow is one application row (Rust: TypedContentionRow).
type gcRow struct {
	id    string
	name  string
	phone string
	score int64
}

// keyID renders a 1-based 32-hex-char key (Rust: %032x of i+1; the all-zero
// ID is the unset sentinel, so keys start at 1).
func keyID(i int) string {
	return fmt.Sprintf("%032x", i+1)
}

// insertSeed inserts the seed row on node 1 (Rust: Name "v-0"; phone/score
// keep their zero values).
func insertSeed(t *testing.T, db *murmur.Database, id, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
			id, name, "", int64(0),
		)
		return nil, err
	}); err != nil {
		t.Fatalf("seed insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after seed insert: %v", err)
	}
}

// tryUpdateName applies one churned update: set name and publish to
// replication. Errors are returned (and counted by the caller) instead of
// failing, so one bad op cannot kill an updater goroutine (Rust:
// updateErrs).
func tryUpdateName(db *murmur.Database, id, name string) error {
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		n, err := s.Execute("UPDATE contention SET name = ? WHERE id = ?", name, id)
		if err != nil {
			return nil, err
		}
		if n != 1 {
			return nil, fmt.Errorf("update hit %d rows", n)
		}
		return nil, nil
	}); err != nil {
		return err
	}
	return db.Flush()
}

// readName reads the typed name on one node: ("", false) when the row is
// absent (Rust: TypedContentionRead).
func readName(t *testing.T, db *murmur.Database, id string) (string, bool) {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM contention WHERE id = ?", id)
	})
	if err != nil {
		t.Fatalf("read row failed: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return "", false
	}
	return rows[0][0].Str, true
}

// queryRows reads the canonical PK-ordered rows on one node (Rust: gcDigest
// input; compared directly instead of hashing). Errors are returned so
// convergence polls can treat them as not-yet-converged.
func queryRows(db *murmur.Database) ([]gcRow, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name, phone, score FROM contention ORDER BY id")
	})
	if err != nil {
		return nil, err
	}
	var out []gcRow
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, gcRow{id: row[0].Str, name: row[1].Str, phone: row[2].Str, score: row[3].Int})
	}
	return out, nil
}

// mustRows reads the canonical PK-ordered rows on one node, failing the
// test on error.
func mustRows(t *testing.T, db *murmur.Database) []gcRow {
	t.Helper()
	rows, err := queryRows(db)
	if err != nil {
		t.Fatalf("read all rows failed: %v", err)
	}
	return rows
}

// waitAllCounts waits until every node holds exactly want rows (Rust:
// waitAllCounts).
func waitAllCounts(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	last := make([]string, cluster.Len())
	converged := harness.WaitUntil(timeout, pollInterval, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			rows, err := queryRows(cluster.Node(i))
			if err != nil {
				last[i] = "read error"
				return false
			}
			last[i] = fmt.Sprintf("count=%d", len(rows))
			if len(rows) != want {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("counts did not reach %d within %v; last=%v", want, timeout, last)
	}
}

// waitDigestsAgreed polls until every node reports the same table contents
// (Rust: waitDigestsAgreed).
func waitDigestsAgreed(t *testing.T, cluster *harness.Cluster, timeout time.Duration) {
	t.Helper()
	converged := harness.WaitUntil(timeout, pollInterval, func() bool {
		want, err := queryRows(cluster.Node(0))
		if err != nil {
			return false
		}
		for i := 1; i < cluster.Len(); i++ {
			got, err := queryRows(cluster.Node(i))
			if err != nil || !equalRows(got, want) {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("digests did not agree within %v", timeout)
	}
}

// waitValueChanged polls followers until one reports a value other than seed
// for the given key, proving churned updates flow end to end (Rust:
// waitValueChanged).
func waitValueChanged(t *testing.T, cluster *harness.Cluster, id, seed string, timeout time.Duration) {
	t.Helper()
	changed := harness.WaitUntil(timeout, pollInterval, func() bool {
		for i := 1; i < cluster.Len(); i++ {
			v, err := cluster.Node(i).Read(func(s *murmur.Sql) (any, error) {
				return s.Query("SELECT name FROM contention WHERE id = ?", id)
			})
			if err != nil {
				return false
			}
			rows := v.([][]sqlite.Value)
			if len(rows) > 0 && rows[0][0].Str != seed {
				return true
			}
		}
		return false
	})
	if !changed {
		t.Fatalf("no follower observed a churned value for %s within %v (updates may not match)", id, timeout)
	}
}

// gcSnapshot holds commit/GC balance counters sampled from node 1 (Rust:
// gcSnapshot scraped from /metrics).
type gcSnapshot struct {
	// commits is local commits produced (Status.Committed; Rust:
	// spedsql_local_commits_total).
	commits uint64
	// collected is origin-log transactions collected (durable; Rust:
	// spedsql_gc_log_collected_total).
	collected uint64
	// runs is successful collection passes (Rust: spedsql_gc_runs_total).
	runs uint64
	// lastError is the latest replication-engine error, if any. Rust has
	// no GC-failures counter; collection errors land here instead (Rust:
	// spedsql_gc_failures_total).
	lastError string
	// localAcq is writer-scheduler local admissions. Collection bypasses
	// the scheduler, so local admissions prove the scheduler served the
	// churn instead (Rust: spedsql_sched_acquisitions_total maintenance
	// class).
	localAcq uint64
}

// tryGCCounters samples the balance counters once.
func tryGCCounters(cluster *harness.Cluster) (gcSnapshot, error) {
	var snap gcSnapshot
	db := cluster.Node(0)
	h := db.Replication()
	if h == nil {
		return snap, fmt.Errorf("node1 has no replication handle")
	}
	status := h.Status()
	retention, err := db.RetentionStatus()
	if err != nil {
		return snap, err
	}
	lastErr := ""
	if status.LastError != nil {
		lastErr = *status.LastError
	}
	return gcSnapshot{
		commits:   db.Status().Committed,
		collected: retention.CollectedTransactions,
		runs:      status.Metrics.RetentionRuns,
		lastError: lastErr,
		localAcq:  db.Scheduler().Snapshot().Local.Acquisitions,
	}, nil
}

// gcCounters samples the balance counters, retrying transient engine-channel
// refusals under churn until they clear or the cap elapses.
func gcCounters(t *testing.T, cluster *harness.Cluster) gcSnapshot {
	t.Helper()
	deadline := time.Now().Add(counterSampleCap)
	var lastErr error
	for {
		snap, err := tryGCCounters(cluster)
		if err == nil {
			return snap
		}
		lastErr = err
		if !time.Now().Before(deadline) {
			t.Fatalf("gc counters unavailable for %v: %v", counterSampleCap, lastErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestLogGCKeepsUpWithChurn churns a fixed 200-key set with eight parallel
// updaters and requires GC to collect >= 90% of the window's origin-log
// batches with zero failures and no loss (Rust:
// log_gc_keeps_up_with_churn).
func TestLogGCKeepsUpWithChurn(t *testing.T) {
	// Aggressive log retention (Rust: retention 1s, 1s collection cadence,
	// minimum retained 10, maximum offline 20s). The 1s collection cadence
	// keeps background passes inside the 40s window; the product default
	// (60s) would starve it.
	retention := time.Second
	retentionInterval := time.Second
	minimumRetained := uint64(10)
	maximumOffline := 20 * time.Second
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "gc-balance",
		NumNodes: 3,
		Replication: &harness.ReplicationOptions{
			Retention:         &retention,
			RetentionInterval: &retentionInterval,
			MinimumRetained:   &minimumRetained,
			MaximumOffline:    &maximumOffline,
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("gc-balance-no-balance")
		}
		cluster.Finish(failed)
	}()

	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, ddl); err != nil {
			t.Fatalf("node%d migrate contention table: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("contention", true); err != nil {
			t.Fatalf("node%d opt-in contention: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}

	for i := 0; i < keys; i++ {
		insertSeed(t, cluster.Node(0), keyID(i), "v-0")
	}
	waitAllCounts(t, cluster, keys, countsTimeout)

	var stopped atomic.Bool
	var updateErrs atomic.Uint64
	var updates atomic.Uint64
	var sweeps [updaters]atomic.Uint64
	updaterDeadline := time.Now().Add(updaterCap)

	// Eight updaters over disjoint key ranges: a single sequential updater
	// cannot sustain enough churn on a loaded box, which would make the
	// pressure gate vacuous. Parallelism keeps the window well above the
	// commit floor in every environment.
	var wg sync.WaitGroup
	for w := 0; w < updaters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			db := cluster.Node(0)
			var round uint64 = 1
			lo := w * keys / updaters
			hi := lo + keys/updaters
			for !stopped.Load() && time.Now().Before(updaterDeadline) {
				for i := lo; i < hi; i++ {
					if stopped.Load() {
						break
					}
					if err := tryUpdateName(db, keyID(i), fmt.Sprintf("v-%d-%d", w, round)); err != nil {
						t.Logf("updater %d key %d: %v", w, i, err)
						updateErrs.Add(1)
					} else {
						updates.Add(1)
					}
				}
				sweeps[w].Store(round)
				round++
			}
		}(w)
	}

	// Warm up to steady state, then measure one window: every commit in
	// the window appends an origin-log batch, so (commits - collected) is
	// the retained set's growth over the window. GC keeps up iff it
	// collects >= 90% of what the window produced; a dead GC scores 0%.
	// The window is 40s on a quiet box and stretches (to a 150s cap) under
	// parallel-suite load until the commit floor is met, so the ratio proof
	// stays meaningful at any production rate.
	time.Sleep(warmup)
	before := gcCounters(t, cluster)
	time.Sleep(window)
	after := gcCounters(t, cluster)
	elapsed := window
	for after.commits-before.commits < commitFloor && elapsed < windowCap {
		time.Sleep(windowStretchStep)
		elapsed += windowStretchStep
		after = gcCounters(t, cluster)
	}
	t.Logf("window commits=%d over the measurement period", after.commits-before.commits)

	stopped.Store(true)
	wg.Wait()

	// Drain: the window's tail stays uncollected until a GC pass runs
	// after the updaters stop, so first wait for a post-stop pass and then
	// settle on a stable collected count before sampling `after`.
	// Stability alone is not enough: on a starved ticker no pass may run
	// for a minute, and a prematurely stable sample measures ticker
	// alignment instead of balance. A dead GC exhausts the run wait and
	// still fails via the runs/ratio checks below.
	runDeadline := time.Now().Add(runWaitCap)
	windowEndRuns := after.runs
	for {
		after = gcCounters(t, cluster)
		if after.runs > windowEndRuns || !time.Now().Before(runDeadline) {
			break
		}
		time.Sleep(drainPoll)
	}
	drainDeadline := time.Now().Add(drainCap)
	stable := 0
	for stable < drainStablePolls && time.Now().Before(drainDeadline) {
		time.Sleep(drainPoll)
		next := gcCounters(t, cluster)
		if next.collected == after.collected {
			stable++
		} else {
			stable = 0
		}
		after = next
	}

	if updateErrs.Load() != 0 {
		t.Fatalf("%d update errors during churn", updateErrs.Load())
	}
	var totalSweeps uint64
	for w := 0; w < updaters; w++ {
		got := sweeps[w].Load()
		totalSweeps += got
		if got < minSweeps {
			t.Fatalf("updater %d completed only %d sweeps, want >= %d for GC pressure", w, got, minSweeps)
		}
	}
	commits := after.commits - before.commits
	collected := after.collected - before.collected
	runs := after.runs - before.runs
	localAcq := after.localAcq - before.localAcq
	t.Logf("churn: %d updater sweeps (%d test-counted updates), %d commits produced, %d log batches collected, %d gc runs, %d local scheduler admissions",
		totalSweeps, updates.Load(), commits, collected, runs, localAcq)

	// The balance assertion is vacuous without proven sustained churn (the
	// retained set scales with the production rate, so the 90% bound stays
	// meaningful at any rate above the floor).
	if commits < commitFloor {
		t.Fatalf("only %d commits in the window, want >= %d for GC pressure", commits, commitFloor)
	}
	if localAcq == 0 {
		t.Fatalf("no local scheduler admissions during %d commits; scheduler starves writers", commits)
	}
	if runs == 0 {
		t.Fatalf("no GC runs during the window; test cannot observe GC")
	}
	// The zero-failures check fails only when a GC-shaped replication
	// error (retention, collection, pruning, GC, snapshot) appears or
	// differs across the window; asserting the sticky last_error is
	// unchanged would trip on unrelated transport transients
	// (dial/handshake/queue pressure) under churn.
	if after.lastError != before.lastError &&
		(isGCError(before.lastError) || isGCError(after.lastError)) {
		t.Fatalf("replication engine GC error during the window: before=%q after=%q",
			before.lastError, after.lastError)
	}
	// Saturating: GC may collect window-external backlog, in which case
	// retained shrank and the balance holds trivially.
	var behind uint64
	if collected < commits {
		behind = commits - collected
	}
	if behind > commits/10 {
		t.Fatalf("GC fell behind: produced %d commits but collected %d (retained grew by %d, want <= %d)",
			commits, collected, behind, commits/10)
	}

	// End-to-end update flow: a follower must observe a churned value.
	waitValueChanged(t, cluster, keyID(0), "v-0", valueTimeout)

	// No data loss: exact key set with identical digests everywhere.
	// Counts never change for fixed-key updates, so convergence means
	// digest agreement: poll for it, since followers drain the final
	// backlog after the updaters stop.
	waitAllCounts(t, cluster, keys, countsTimeout)
	waitDigestsAgreed(t, cluster, digestTimeout)
	want := mustRows(t, cluster.Node(0))
	for i := 1; i < cluster.Len(); i++ {
		got := mustRows(t, cluster.Node(i))
		if !equalRows(got, want) {
			t.Fatalf("node%d digest differs after GC churn", i+1)
		}
	}
	failed = false
}

// isGCError reports whether err looks like a GC/retention failure rather
// than an unrelated transport transient.
func isGCError(err string) bool {
	e := strings.ToLower(err)
	return strings.Contains(e, "retention") ||
		strings.Contains(e, "collect") ||
		strings.Contains(e, "prune") ||
		strings.Contains(e, "gc ") ||
		strings.Contains(e, "snapshot")
}

func equalRows(a, b []gcRow) bool {
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
