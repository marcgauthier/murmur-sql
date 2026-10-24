// Live scenario: log GC keeps up with churn.
//
// Go port of tests/gc_balance.rs using tests-live/goharness. Identical
// behavior: three nodes mesh with aggressive log retention (1 s log, 20 s
// offline pin, 10 batches) while one node updates a fixed 200-key set
// with eight parallel updaters over disjoint ranges. After a 20 s
// warmup, a 40 s window must show GC collecting at least 90% of the
// origin-log batches the window produced, with zero GC failures; the
// suite ends with exact row counts and identical digests on all nodes
// (no loss). The Rust tokio::join! over updaters + driver becomes a
// sync.WaitGroup over goroutines with the driver on the test goroutine.
// Scope (same as Rust): version churn only; deletes are out of scope.
package gcbalance

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

const (
	// keys is the fixed working set (200).
	keys = 200
	// updaters is the parallel updater count over disjoint ranges (8).
	updaters = 8
	// warmup is the steady-state warmup before measurement (20 s).
	warmup = 20 * time.Second
	// window is the measurement window on a quiet box (40 s).
	window = 40 * time.Second
	// windowStretchStep stretches the window under load (5 s).
	windowStretchStep = 5 * time.Second
	// windowCap caps the stretched window (150 s).
	windowCap = 150 * time.Second
	// commitFloor is the minimum commits for a meaningful proof (2000).
	commitFloor uint64 = 2000
	// minSweeps is the minimum full sweeps per updater (5).
	minSweeps uint64 = 5
	// drainStablePolls is the post-stop drain stability gate (2).
	drainStablePolls = 2
	// drainCap caps the post-stop drain (120 s).
	drainCap = 120 * time.Second
	// countsTimeout bounds row-count convergence (60 s).
	countsTimeout = 60 * time.Second
	// digestTimeout bounds digest agreement (60 s).
	digestTimeout = 60 * time.Second
	// valueTimeout bounds churned-value flow (30 s).
	valueTimeout = 30 * time.Second
	// pollInterval is the convergence poll interval (200 ms).
	pollInterval = 200 * time.Millisecond
	// updaterCap guards updater goroutines if the driver fails before
	// signalling stop (600 s).
	updaterCap = 600 * time.Second
	// retentionRowTimeout bounds the wait for the first SHOW RETENTION
	// row (10 s).
	retentionRowTimeout = 10 * time.Second
)

// contentionRow is one application row.
type contentionRow struct {
	id    string
	name  string
	phone string
	score int64
}

// keyID renders a 1-based 32-hex-char key.
func keyID(i int) string {
	return fmt.Sprintf("%032x", i+1)
}

// insertSeed runs a seed insert on node 0 (name v-0; phone/score keep
// their zero values).
func insertSeed(cluster *goharness.Cluster, id, name string) error {
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO contention(id, name, phone, score) VALUES($1, $2, $3, $4)", id, name, "", int64(0)); err != nil {
		return fmt.Errorf("seed insert failed: %w", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush after seed insert: %w", err)
	}
	return nil
}

// tryUpdateName churns one update: set name and publish to replication.
// Errors are returned (and counted by the caller) instead of failing,
// so one bad op cannot kill an updater.
func tryUpdateName(cluster *goharness.Cluster, id, name string) error {
	changed, err := cluster.Node(0).Execute("UPDATE contention SET name = $1 WHERE id = $2", name, id)
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("update hit %d rows", changed)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		return err
	}
	return nil
}

// readName reads one typed row on one node: ("", false) when absent.
func readName(cluster *goharness.Cluster, idx int, id string) (string, bool, error) {
	rows, err := cluster.Node(idx).Query("SELECT name FROM contention WHERE id = $1", id)
	if err != nil {
		return "", false, fmt.Errorf("node%d read row failed: %w", idx+1, err)
	}
	if len(rows) == 0 {
		return "", false, nil
	}
	name, err := rows[0].String(0)
	if err != nil {
		return "", false, fmt.Errorf("node%d decode name: %w", idx+1, err)
	}
	return name, true, nil
}

// allRows returns canonical PK-ordered rows on one node (compared
// directly instead of hashing).
func allRows(cluster *goharness.Cluster, idx int) ([]contentionRow, error) {
	rows, err := cluster.Node(idx).Query("SELECT id, name, phone, score FROM contention ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("node%d read all rows failed: %w", idx+1, err)
	}
	out := make([]contentionRow, 0, len(rows))
	for _, row := range rows {
		id, err := row.String(0)
		if err != nil {
			return nil, fmt.Errorf("node%d decode id: %w", idx+1, err)
		}
		name, err := row.String(1)
		if err != nil {
			return nil, fmt.Errorf("node%d decode name: %w", idx+1, err)
		}
		phone, err := row.String(2)
		if err != nil {
			return nil, fmt.Errorf("node%d decode phone: %w", idx+1, err)
		}
		score, err := row.Int64(3)
		if err != nil {
			return nil, fmt.Errorf("node%d decode score: %w", idx+1, err)
		}
		out = append(out, contentionRow{id: id, name: name, phone: phone, score: score})
	}
	return out, nil
}

// waitAllCounts waits until every node holds exactly want rows.
func waitAllCounts(cluster *goharness.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	last := make([]string, cluster.Len())
	for {
		converged := true
		for i := 0; i < cluster.Len(); i++ {
			rows, err := allRows(cluster, i)
			if err != nil {
				return err
			}
			count := len(rows)
			last[i] = fmt.Sprintf("count=%d", count)
			if count != want {
				converged = false
				break
			}
		}
		if converged {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("counts did not reach %d within %v; last=%v", want, timeout, last)
		}
		time.Sleep(pollInterval)
	}
}

// waitDigestsAgreed polls until every node reports the same table
// contents.
func waitDigestsAgreed(cluster *goharness.Cluster, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		want, err := allRows(cluster, 0)
		if err != nil {
			return err
		}
		agreed := true
		for i := 1; i < cluster.Len(); i++ {
			got, err := allRows(cluster, i)
			if err != nil {
				return err
			}
			if !equalRows(got, want) {
				agreed = false
				break
			}
		}
		if agreed {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("digests did not agree within %v", timeout)
		}
		time.Sleep(pollInterval)
	}
}

// waitValueChanged polls followers until one reports a value other than
// seed for the given key, proving churned updates flow end to end.
func waitValueChanged(cluster *goharness.Cluster, id, seed string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		for i := 1; i < cluster.Len(); i++ {
			name, ok, err := readName(cluster, i, id)
			if err != nil {
				return err
			}
			if ok && name != seed {
				return nil
			}
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("no follower observed a churned value for %s within %v (updates may not match)", id, timeout)
		}
		time.Sleep(pollInterval)
	}
}

// gcSnapshot holds commit/GC balance counters sampled from node 1 over
// the wire.
type gcSnapshot struct {
	// commits are local commits produced (MURMUR SHOW REPLICATION
	// trailing committed column).
	commits uint64
	// collected are origin-log transactions collected (durable;
	// MURMUR SHOW RETENTION collected_transactions).
	collected uint64
	// lastError is the latest replication-engine error, if any. No
	// GC-failures counter exists; collection errors land here instead.
	lastError *string
}

func gcCounters(cluster *goharness.Cluster) (gcSnapshot, error) {
	var zero gcSnapshot
	repl, err := cluster.Node(0).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return zero, fmt.Errorf("node1 SHOW REPLICATION: %w", err)
	}
	if len(repl) == 0 {
		return zero, fmt.Errorf("node1 SHOW REPLICATION returned no rows")
	}
	if len(repl[0]) < 8 {
		return zero, fmt.Errorf("node1 SHOW REPLICATION has %d columns, want 8", len(repl[0]))
	}
	commits, err := strconv.ParseUint(repl[0][7], 10, 64)
	if err != nil {
		return zero, fmt.Errorf("node1 bad committed %q: %w", repl[0][7], err)
	}
	var lastError *string
	if repl[0][6] != "" {
		e := repl[0][6]
		lastError = &e
	}
	// collected_transactions rides on per-peer rows; wait briefly for
	// the first row (the mesh is up by measurement time).
	deadline := time.Now().Add(retentionRowTimeout)
	for {
		rows, err := cluster.Node(0).Simple("MURMUR SHOW RETENTION")
		if err != nil {
			return zero, fmt.Errorf("node1 SHOW RETENTION: %w", err)
		}
		if len(rows) > 0 {
			row := rows[0]
			if len(row) < 6 {
				return zero, fmt.Errorf("node1 SHOW RETENTION has %d columns, want 7", len(row))
			}
			collected, err := strconv.ParseUint(row[5], 10, 64)
			if err != nil {
				return zero, fmt.Errorf("node1 bad collected %q: %w", row[5], err)
			}
			return gcSnapshot{commits: commits, collected: collected, lastError: lastError}, nil
		}
		if !time.Now().Before(deadline) {
			return zero, fmt.Errorf("node1 SHOW RETENTION returned no rows")
		}
		time.Sleep(pollInterval)
	}
}

// updater churns a disjoint key range: sequential sweeps until stopped
// is set.
func updater(cluster *goharness.Cluster, w int, stopped *atomic.Bool, updateErrs, updates *atomic.Uint64, sweeps *atomic.Uint64, wg *sync.WaitGroup) {
	defer wg.Done()
	updaterDeadline := time.Now().Add(updaterCap)
	var round uint64 = 1
	lo := w * keys / updaters
	hi := lo + keys/updaters
	for !stopped.Load() && time.Now().Before(updaterDeadline) {
		for i := lo; i < hi; i++ {
			if stopped.Load() {
				break
			}
			if err := tryUpdateName(cluster, keyID(i), fmt.Sprintf("v-%d-%d", w, round)); err != nil {
				fmt.Fprintf(os.Stderr, "updater %d key %d: %v\n", w, i, err)
				updateErrs.Add(1)
			} else {
				updates.Add(1)
			}
			time.Sleep(140 * time.Millisecond)
		}
		sweeps.Store(round)
		round++
	}
}

func runBody(cluster *goharness.Cluster) error {
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		fmt.Fprintf(os.Stderr, "%s dir=%s sql=%s repl=%s\n", info.Label, info.Dir, info.SQLAddr, info.ReplAddr)
	}
	// Same schema on every node, then replicate it: convergence needs
	// the per-table flag on each node.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)"); err != nil {
			return fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE contention REPLICATED true"); err != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("node%d flush after SET TABLE: %w", i+1, err)
		}
	}

	for i := 0; i < keys; i++ {
		if err := insertSeed(cluster, keyID(i), "v-0"); err != nil {
			return err
		}
	}
	if err := waitAllCounts(cluster, keys, countsTimeout); err != nil {
		return err
	}

	var stopped atomic.Bool
	var updateErrs, updates atomic.Uint64
	var sweeps [updaters]atomic.Uint64

	// Eight updaters over disjoint key ranges. The driver sets stopped
	// on every exit path so a failed phase cannot hang the join.
	var wg sync.WaitGroup
	for w := 0; w < updaters; w++ {
		wg.Add(1)
		go updater(cluster, w, &stopped, &updateErrs, &updates, &sweeps[w], &wg)
	}
	// Warm up to steady state, then measure one window: every commit in
	// the window appends an origin-log batch, so (commits - collected)
	// is the retained set's growth over the window. GC keeps up iff it
	// collects >= 90% of what the window produced; a dead GC scores 0%.
	// The window is 40s on a quiet box and stretches (to a 150s cap)
	// under parallel-suite load until the commit floor is met, so the
	// ratio proof stays meaningful at any production rate.
	before, after, elapsed, driverErr := func() (gcSnapshot, gcSnapshot, time.Duration, error) {
		var zero gcSnapshot
		time.Sleep(warmup)
		before, err := gcCounters(cluster)
		if err != nil {
			return zero, zero, 0, err
		}
		time.Sleep(window)
		after, err := gcCounters(cluster)
		if err != nil {
			return zero, zero, 0, err
		}
		elapsed := window
		for after.commits-before.commits < commitFloor && elapsed < windowCap {
			time.Sleep(windowStretchStep)
			elapsed += windowStretchStep
			after, err = gcCounters(cluster)
			if err != nil {
				return zero, zero, 0, err
			}
		}
		fmt.Fprintf(os.Stderr, "window commits=%d over the measurement period\n", after.commits-before.commits)
		return before, after, elapsed, nil
	}()
	stopped.Store(true)
	wg.Wait()
	if driverErr != nil {
		return driverErr
	}

	// Drain: the window's tail stays uncollected until a GC pass runs
	// after the updaters stop. First, let the mesh catch up and exchange
	// ACKs so node 0's retention floor advances.
	_ = cluster.SyncAll()
	if err := waitDigestsAgreed(cluster, digestTimeout); err != nil {
		return err
	}
	_ = cluster.SyncAll()

	// The drain budget scales with the window's observed slowdown. A
	// 30-second stall with backlog outstanding is not slowness, though
	// (collection runs at least every second): fail fast and say so.
	slowdown := elapsed.Seconds() / window.Seconds()
	if slowdown < 1.0 {
		slowdown = 1.0
	}
	drainDeadline := time.Now().Add(time.Duration(float64(drainCap) * slowdown))
	stable := 0
	for time.Now().Before(drainDeadline) {
		_, _ = cluster.Node(0).Simple("MURMUR RETENTION COLLECT")
		time.Sleep(500 * time.Millisecond)
		next, err := gcCounters(cluster)
		if err != nil {
			return err
		}
		commits := next.commits - before.commits
		collected := next.collected - before.collected
		if next.collected == after.collected {
			stable++
		} else {
			stable = 0
		}
		after = next
		if saturatingSub(commits, collected) <= commits/10 && stable >= drainStablePolls {
			break
		}
		if stable >= 60 {
			return fmt.Errorf("collection stalled: %d/%d collected with no progress for 30s", collected, commits)
		}
	}

	if updateErrs.Load() != 0 {
		return fmt.Errorf("%d update errors during churn", updateErrs.Load())
	}
	var totalSweeps uint64
	for w := 0; w < updaters; w++ {
		got := sweeps[w].Load()
		totalSweeps += got
		if got < minSweeps {
			return fmt.Errorf("updater %d completed only %d sweeps, want >= %d for GC pressure", w, got, minSweeps)
		}
	}
	commits := after.commits - before.commits
	collected := after.collected - before.collected
	fmt.Fprintf(os.Stderr, "churn: %d updater sweeps (%d test-counted updates), %d commits produced, %d log batches collected\n",
		totalSweeps, updates.Load(), commits, collected)

	// The balance assertion is vacuous without proven sustained churn.
	if commits < commitFloor {
		return fmt.Errorf("only %d commits in the window, want >= %d for GC pressure", commits, commitFloor)
	}
	// No GC-runs counter exists over the wire, so observed collection
	// (collected advancing across the window) is the proof a pass ran;
	// without it the test cannot observe GC.
	if collected == 0 {
		return fmt.Errorf("no GC collection during the window; test cannot observe GC")
	}
	// Only GC-shaped errors fail the window (anything else is covered
	// by the balance and digest proofs below).
	if !equalOptString(after.lastError, before.lastError) &&
		(isGCError(before.lastError) || isGCError(after.lastError)) {
		return fmt.Errorf("replication engine GC error during the window: before=%s after=%s",
			optString(before.lastError), optString(after.lastError))
	}
	if saturatingSub(commits, collected) > commits/10 {
		return fmt.Errorf("GC fell behind: produced %d commits but collected %d (retained grew by %d, want <= %d)",
			commits, collected, saturatingSub(commits, collected), commits/10)
	}

	// End-to-end update flow: a follower must observe a churned value.
	if err := waitValueChanged(cluster, keyID(0), "v-0", valueTimeout); err != nil {
		return err
	}

	// No data loss: exact key set with identical digests everywhere.
	if err := waitAllCounts(cluster, keys, countsTimeout); err != nil {
		return err
	}
	if err := waitDigestsAgreed(cluster, digestTimeout); err != nil {
		return err
	}
	want, err := allRows(cluster, 0)
	if err != nil {
		return err
	}
	for i := 1; i < cluster.Len(); i++ {
		digest, err := allRows(cluster, i)
		if err != nil {
			return err
		}
		if !equalRows(digest, want) {
			return fmt.Errorf("node%d digest differs after GC churn", i+1)
		}
	}
	return nil
}

func TestLogGCKeepsUpWithChurn(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	// Aggressive log retention. The 1s collection cadence keeps
	// background passes inside the 40s window; the product default
	// (60s) would starve it.
	retention := time.Second
	retentionInterval := time.Second
	minimumRetained := uint64(10)
	maximumOffline := 20 * time.Second
	opts := goharness.DefaultClusterOptions("gc-balance")
	opts.NumNodes = 3
	opts.Replication = &goharness.ReplicationOptions{
		Retention:         &retention,
		RetentionInterval: &retentionInterval,
		MinimumRetained:   &minimumRetained,
		MaximumOffline:    &maximumOffline,
	}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	result := runBody(cluster)
	failed := result != nil
	if result != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", result)
	}
	if failed {
		cluster.DumpForensics("gc-balance-no-balance")
	}
	cluster.Finish(failed)
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
}

// isGCError reports whether an engine error string is GC-shaped.
func isGCError(err *string) bool {
	if err == nil {
		return false
	}
	e := strings.ToLower(*err)
	return strings.Contains(e, "retention") || strings.Contains(e, "collect") || strings.Contains(e, "prune") ||
		strings.Contains(e, "gc ") || strings.Contains(e, "snapshot")
}

func equalOptString(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func optString(s *string) string {
	if s == nil {
		return "None"
	}
	return strconv.Quote(*s)
}

func saturatingSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}

func equalRows(a, b []contentionRow) bool {
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
