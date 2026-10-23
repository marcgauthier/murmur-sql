// Live scenario: rolling restarts lose no writes, plus a back-to-back
// restart churn stress.
//
// Go port of the Rust `rolling-restart` scenario
// (/home/marc/MURMUR/tests-live/rolling-restart/tests/rolling_restart.rs).
// `TestRollingRestartLosesNoWrites` restarts each node in turn while every
// node keeps accepting writes: any failed write outside a restart window
// aborts, and the mesh must converge with identical digests at the end.
// `TestRollingRestartChurn` stresses session re-establishment with six
// back-to-back restarts and no quiesce between. Plain SQL replaces the
// typed-contention HTTP API, and in-process close/reopen replaces daemon
// stop/start.
package rollingrestart

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// tableDDL is the contention table DDL (Rust: only id+name matter here).
const tableDDL = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)"

// baselineRows is the baseline row count (Rust: 5).
const baselineRows = 5

const (
	// baselineTimeout is the baseline wait (Rust: 60 s).
	baselineTimeout = 60 * time.Second
	// writeTick is the main-test writer cadence (Rust: 25 ms, 1 writer
	// per node).
	writeTick = 25 * time.Millisecond
	// churnTick is the churn-test writer cadence (Rust: 10 ms single
	// writer over all nodes).
	churnTick = 10 * time.Millisecond
	// drain is the in-flight drain before stopping (Rust: 300 ms).
	drain = 300 * time.Millisecond
	// churnDrain is the churn-test in-flight drain (Rust: 200 ms).
	churnDrain = 200 * time.Millisecond
	// downtime is the sleep between stop and start (Rust: 1 s).
	downtime = time.Second
	// catchUp is the catch-up sleep after each rolling restart (Rust: 3 s).
	catchUp = 3 * time.Second
	// quiesceTimeout bounds quiesce / final waits (Rust: 150 s).
	quiesceTimeout = 150 * time.Second
	// quiescePoll is the quiesce poll interval (Rust: 200 ms).
	quiescePoll = 200 * time.Millisecond
	// countPoll is the count poll interval (Rust: 100 ms).
	countPoll = 100 * time.Millisecond
	// settle is the convergence settle re-sample delay (Rust: 500 ms).
	settle = 500 * time.Millisecond
)

// idName is one ordered contention row (Rust: (id, name) pairs).
type idName struct {
	id   string
	name string
}

// migrateAll applies the same schema on every node (Rust: migrate_all).
func migrateAll(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).Migrate(murmur.NewMigration(tableDDL)); err != nil {
			t.Fatalf("migrate contention table: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush schema: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("contention", true); err != nil {
			t.Fatalf("opt-in contention: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}
}

// rows returns the ordered contention rows on one node (Rust: rows).
func rows(t *testing.T, db *murmur.Database) []idName {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name FROM contention ORDER BY id")
	})
	if err != nil {
		t.Fatalf("read contention rows failed: %v", err)
	}
	var out []idName
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, idName{id: row[0].Str, name: row[1].Str})
	}
	return out
}

// tryInsert is a best-effort insert; true when acknowledged.
func tryInsert(db *murmur.Database, id, name string) bool {
	_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO contention(id, name) VALUES(?, ?)", id, name)
		return nil, err
	})
	return err == nil
}

// insertRow inserts one row, failing on error (baseline writes).
func insertRow(t *testing.T, db *murmur.Database, node int, id, name string) {
	t.Helper()
	if !tryInsert(db, id, name) {
		t.Fatalf("node%d baseline write failed", node)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after baseline failed: %v", node, err)
	}
}

// waitCounts waits until every node reports the exact row count (Rust:
// wait_counts).
func waitCounts(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	converged := harness.WaitUntil(timeout, countPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if len(rows(t, cluster.Node(i))) != want {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("row counts did not reach %d within %v", want, timeout)
	}
}

// quiesce pauses all writers, waits for exact digest agreement, then
// resumes (Rust: quiesce). Callers must hold no pause themselves.
func quiesce(t *testing.T, cluster *harness.Cluster, paused *[3]atomic.Bool, timeout time.Duration) {
	t.Helper()
	for i := range paused {
		paused[i].Store(true)
	}
	// Drain in-flight writes before asserting a frozen state.
	time.Sleep(drain)
	converged := harness.WaitUntil(timeout, quiescePoll, func() bool {
		want := rows(t, cluster.Node(0))
		for i := 1; i < cluster.Len(); i++ {
			if !slices.Equal(rows(t, cluster.Node(i)), want) {
				return false
			}
		}
		return true
	})
	if !converged {
		dumpIDDiff(t, cluster)
		t.Fatalf("digests did not agree within %v", timeout)
	}
	for i := range paused {
		paused[i].Store(false)
	}
}

// waitConvergedCounts waits until all nodes agree on the same row count,
// with a settle re-sample before declaring convergence (Rust:
// wait_converged_counts).
func waitConvergedCounts(t *testing.T, cluster *harness.Cluster, timeout time.Duration) {
	t.Helper()
	converged := harness.WaitUntil(timeout, countPoll, func() bool {
		first := len(rows(t, cluster.Node(0)))
		for i := 1; i < cluster.Len(); i++ {
			if len(rows(t, cluster.Node(i))) != first {
				return false
			}
		}
		// Settle: one more matching sample before declaring convergence.
		time.Sleep(settle)
		for i := 0; i < cluster.Len(); i++ {
			if len(rows(t, cluster.Node(i))) != first {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("row counts did not converge within %v", timeout)
	}
}

// dumpIDDiff logs the symmetric row-id difference plus the first ordered
// row difference, then dumps forensics (Rust: dump_id_diff).
func dumpIDDiff(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	ids := func(idx int) map[string]bool {
		out := make(map[string]bool)
		for _, r := range rows(t, cluster.Node(idx)) {
			out[r.id] = true
		}
		return out
	}
	a, b := ids(0), ids(1)
	var onlyA, onlyB []string
	for id := range a {
		if !b[id] {
			onlyA = append(onlyA, id)
		}
	}
	for id := range b {
		if !a[id] {
			onlyB = append(onlyB, id)
		}
	}
	t.Logf("divergence: only-node1=%d %v only-node2=%d %v",
		len(onlyA), onlyA[:min(5, len(onlyA))], len(onlyB), onlyB[:min(5, len(onlyB))])
	r0, r1 := rows(t, cluster.Node(0)), rows(t, cluster.Node(1))
	for i := 0; i < len(r0) && i < len(r1); i++ {
		if r0[i] != r1[i] {
			t.Logf("divergence: first diff at row %d:\n  node1=%+v\n  node2=%+v", i, r0[i], r1[i])
			break
		}
	}
	cluster.DumpForensics("divergence")
}

// assertDigestsEqual requires identical ordered digests on all nodes (Rust:
// rrDigest equality).
func assertDigestsEqual(t *testing.T, cluster *harness.Cluster, what string) {
	t.Helper()
	want := rows(t, cluster.Node(0))
	for idx := 1; idx < cluster.Len(); idx++ {
		if got := rows(t, cluster.Node(idx)); !slices.Equal(got, want) {
			t.Fatalf("node%d digest != node1 digest after %s", idx+1, what)
		}
	}
}

// logPlacement logs node placement (Rust: per-node dir/repl line).
func logPlacement(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
}

// handles holds the current-generation writer handles, refreshed after
// every restart.
type handles struct {
	mu  sync.Mutex
	dbs []*murmur.Database
}

func (h *handles) get(node int) *murmur.Database {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.dbs[node]
}

func (h *handles) set(node int, db *murmur.Database) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dbs[node] = db
}

func TestRollingRestartLosesNoWrites(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "rolling-restart",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("rolling-restart-no-convergence")
		}
		cluster.Finish(failed)
	}()
	logPlacement(t, cluster)
	h := &handles{dbs: []*murmur.Database{cluster.Node(0), cluster.Node(1), cluster.Node(2)}}

	migrateAll(t, cluster)
	for i := 0; i < baselineRows; i++ {
		insertRow(t, cluster.Node(0), 1, fmt.Sprintf("%032x", i), fmt.Sprintf("base-%d", i))
	}
	waitCounts(t, cluster, baselineRows, baselineTimeout)

	// Writers hammer every node continuously. A write to a live node
	// must never fail (zero-downtime restart); the writer for the node
	// being restarted pauses while it is down.
	var failedWrites atomic.Int64
	var paused [3]atomic.Bool
	var stop atomic.Bool
	var wg sync.WaitGroup
	// A failed assertion below must still release the writers so the
	// WaitGroup join cannot deadlock.
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()
	for node := 0; node < 3; node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			seq := 0
			for !stop.Load() {
				time.Sleep(writeTick)
				if stop.Load() {
					break
				}
				if paused[node].Load() {
					continue
				}
				id := fmt.Sprintf("ff%02x%028x", node, seq)
				seq++
				if !tryInsert(h.get(node), id, "flow") {
					failedWrites.Add(1)
					t.Logf("node%d write failed outside its restart", node+1)
				}
			}
		}(node)
	}

	for i := 0; i < 3; i++ {
		paused[i].Store(true)
		// Drain in-flight writes before stopping so none fail spuriously.
		time.Sleep(drain)
		cluster.StopNode(i)
		time.Sleep(downtime)
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("restart node: %v", err)
		}
		h.set(i, cluster.Node(i))
		if err := cluster.Mesh(); err != nil {
			t.Fatalf("re-mesh after restart: %v", err)
		}
		paused[i].Store(false)
		// Let the rejoined node catch up under load, then quiesce
		// all writers: exact-count agreement is unobservable while
		// inserts keep landing.
		time.Sleep(catchUp)
		quiesce(t, cluster, &paused, quiesceTimeout)
	}
	stop.Store(true)
	wg.Wait()

	if n := failedWrites.Load(); n != 0 {
		t.Fatalf("%d writes failed outside restart windows, want zero-downtime", n)
	}
	waitConvergedCounts(t, cluster, quiesceTimeout)
	assertDigestsEqual(t, cluster, "rolling restart")
	failed = false
}

func TestRollingRestartChurn(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "rolling-churn",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("rolling-churn-no-convergence")
		}
		cluster.Finish(failed)
	}()
	logPlacement(t, cluster)
	h := &handles{dbs: []*murmur.Database{cluster.Node(0), cluster.Node(1), cluster.Node(2)}}

	migrateAll(t, cluster)
	for i := 0; i < baselineRows; i++ {
		insertRow(t, cluster.Node(0), 1, fmt.Sprintf("%032x", i), fmt.Sprintf("base-%d", i))
	}
	waitCounts(t, cluster, baselineRows, baselineTimeout)

	// One writer ticks over every unpaused node; errors are ignored
	// (churn is about session re-establishment, not write success).
	var paused [3]atomic.Bool
	var stop atomic.Bool
	var wg sync.WaitGroup
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		seq := 0
		for !stop.Load() {
			time.Sleep(churnTick)
			if stop.Load() {
				break
			}
			for n := 0; n < 3; n++ {
				if paused[n].Load() {
					continue
				}
				id := fmt.Sprintf("cc%02x%028x", n, seq)
				_ = tryInsert(h.get(n), id, "flow")
			}
			seq++
		}
	}()

	// Six back-to-back restarts: node order 1,2,3,1,2,3 with only
	// the readiness wait between them (no catch-up sleep, no
	// quiesce). Regression test for the heal stall (restart N+1
	// while restart N still healing wedged one link forever).
	for i := 0; i < 6; i++ {
		node := i % 3
		paused[node].Store(true)
		time.Sleep(churnDrain)
		cluster.StopNode(node)
		if err := cluster.StartNode(node); err != nil {
			t.Fatalf("restart node: %v", err)
		}
		h.set(node, cluster.Node(node))
		if err := cluster.Mesh(); err != nil {
			t.Fatalf("re-mesh after restart: %v", err)
		}
		paused[node].Store(false)
		t.Logf("churn: restart %d (node%d) complete", i+1, node+1)
	}

	quiesce(t, cluster, &paused, quiesceTimeout)
	// quiesce resumes writers on success; stop them before the
	// final digest proof (digests under active writers never agree).
	stop.Store(true)
	wg.Wait()

	waitConvergedCounts(t, cluster, quiesceTimeout)
	want := rows(t, cluster.Node(0))
	for idx := 1; idx < 3; idx++ {
		if got := rows(t, cluster.Node(idx)); !slices.Equal(got, want) {
			dumpIDDiff(t, cluster)
			t.Fatalf("node%d digest != node1 digest after churn", idx+1)
		}
	}
	failed = false
}
