// Live scenario: two killed nodes rejoin simultaneously behind a start barrier.
//
// Go port of the Rust `rejoin-storm` scenario
// (/home/marc/MURMUR/tests-live/rejoin-storm/tests/rejoin_storm.rs:
// TestDualKillSimultaneousRejoin). Two of three meshed nodes are
// stopped at once while the survivor keeps taking tracked writes, then
// both restart simultaneously (thundering-herd rejoin behind a start
// barrier). Both must rejoin un-wedged, every outage write must land
// everywhere, counts plus ordered digests must match exactly, and each
// node's rejoin path (range repair vs snapshot) is recorded.
//
// Plain SQL replaces the typed-records HTTP API, and in-process
// close/reopen replaces SIGKILL.
package rejoinstorm

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// tableDDL is the table DDL (Rust: typed records; only names matter).
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)"

// outageEnv is the outage-row knob (Rust: same name, default 40).
const outageEnv = "MURMUR_REJOIN_STORM_OUTAGE_ROWS"

const (
	// defaultOutageRows is the default outage-row count.
	defaultOutageRows = 40
	// controlTimeout is the positive-control wait (Rust: 30 s).
	controlTimeout = 30 * time.Second
	// rejoinTimeout is the rejoin convergence deadline (Rust: 90 s).
	rejoinTimeout = 90 * time.Second
	// postTimeout is the post-rejoin convergence deadline (Rust: 60 s).
	postTimeout = 60 * time.Second
	// poll is the convergence poll interval (Rust: 100 ms).
	poll = 100 * time.Millisecond
	// readyBound is the no-wedge bound for the simultaneous restart
	// (Rust: ready < 60 s per node; the parallel join bounds every
	// node by the total).
	readyBound = 60 * time.Second
)

// outageRows reads the outage-row knob (Rust: same name and default,
// positive only).
func outageRows() int {
	if v, err := strconv.Atoi(os.Getenv(outageEnv)); err == nil && v > 0 {
		return v
	}
	return defaultOutageRows
}

// names returns the canonical ordered names on one node (Rust: TypedNames).
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM live_typed_records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read names failed: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// insertRow inserts one record (Rust: TypedInsert).
func insertRow(t *testing.T, db *murmur.Database, node int, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name) VALUES(?)", name)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after insert failed: %v", node, err)
	}
}

// peerCount returns the live session count (Rust: connected_peers).
func peerCount(cluster *harness.Cluster, node int) int {
	h := cluster.Node(node).Replication()
	if h == nil {
		return 0
	}
	return h.Status().Peers
}

// waitForPeers waits until every node reports its wanted peer count.
func waitForPeers(t *testing.T, cluster *harness.Cluster, want []int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if peerCount(cluster, i) != want[i] {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("peers did not reach %v within %v", want, timeout)
	}
}

// waitForConvergence polls exact counts plus ordered-digest equality.
func waitForConvergence(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		var first []string
		for i := 0; i < cluster.Len(); i++ {
			got := names(t, cluster.Node(i))
			if len(got) != want {
				return false
			}
			if first == nil {
				first = got
			} else if !equalStrings(first, got) {
				return false
			}
		}
		return true
	})
	if !ok {
		for i := 0; i < cluster.Len(); i++ {
			t.Logf("node%d at timeout: count=%d", i+1, len(names(t, cluster.Node(i))))
		}
		t.Fatalf("nodes did not converge on %d rows with equal digests within %v", want, timeout)
	}
}

// assertAcknowledgedPresent requires every acknowledged value present on
// every node.
func assertAcknowledgedPresent(t *testing.T, cluster *harness.Cluster, acknowledged map[string]bool) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		present := make(map[string]bool)
		for _, n := range names(t, cluster.Node(i)) {
			present[n] = true
		}
		for name := range acknowledged {
			if !present[name] {
				t.Fatalf("node%d lost acknowledged write %q", i+1, name)
			}
		}
	}
}

// logCounters returns the log-path activity counters (received, applied):
// snapshot application bypasses both, so a flat pair proves the snapshot
// path.
func logCounters(cluster *harness.Cluster, idx int) (uint64, uint64) {
	m := cluster.Node(idx).Replication().Status().Metrics
	return m.ReceivedTransactions, m.AppliedTransactions
}

func equalStrings(a, b []string) bool {
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

func TestDualKillSimultaneousRejoin(t *testing.T) {
	outage := outageRows()

	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "rejoin-storm",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("rejoin-storm-no-convergence")
		}
		cluster.Finish(failed)
	}()

	// Same schema on every node.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("migrate records table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("live_typed_records", true); err != nil {
			t.Fatalf("opt-in live_typed_records: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Positive control: honest 3-node convergence before any crash.
	for node := 0; node < 3; node++ {
		insertRow(t, cluster.Node(node), node+1, fmt.Sprintf("base-node%d", node+1))
	}
	waitForPeers(t, cluster, []int{2, 2, 2}, controlTimeout)
	waitForConvergence(t, cluster, 3, controlTimeout)
	t.Log("positive control: 3-node mesh converged")

	// Double stop at once; the outage is real only if both nodes
	// actually go dark (no open handle left to answer queries).
	cluster.StopNode(1)
	cluster.StopNode(2)
	for _, idx := range []int{1, 2} {
		if cluster.NodeInfo(idx).DB != nil {
			t.Fatalf("node%d answers queries after stop; outage not real", idx+1)
		}
	}
	t.Log("nodes 2 and 3 stopped; survivor taking outage writes")

	// Survivor keeps taking tracked writes through the whole outage.
	acked := map[string]bool{"base-node1": true, "base-node2": true, "base-node3": true}
	for i := 0; i < outage; i++ {
		name := fmt.Sprintf("outage-%04d", i)
		insertRow(t, cluster.Node(0), 1, name)
		acked[name] = true
	}
	if got := len(names(t, cluster.Node(0))); got != 3+outage {
		t.Fatalf("survivor count = %d, want %d", got, 3+outage)
	}

	// Thundering-herd rejoin: both nodes restart concurrently behind
	// the parallel-open barrier so their catch-up overlaps instead of
	// serializing.
	begin := time.Now()
	if err := cluster.StartNodesParallel([]int{1, 2}); err != nil {
		t.Fatalf("simultaneous restart: %v", err)
	}
	took := time.Since(begin)
	t.Logf("nodes 2 and 3 ready in %v after simultaneous restart", took)
	if took > readyBound {
		t.Fatalf("simultaneous restart took %v, want < %v (wedge)", took, readyBound)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}

	// Every outage write present on all three; exact counts plus
	// ordered digests identical everywhere.
	waitForConvergence(t, cluster, 3+outage, rejoinTimeout)
	assertAcknowledgedPresent(t, cluster, acked)
	t.Logf("all %d outage writes present on all 3 nodes with equal digests", outage)

	// Record (never mandate) each node's rejoin path. Metrics are
	// fresh after the restart, so a flat log pair means snapshot and
	// an advanced pair means log range repair.
	for _, idx := range []int{1, 2} {
		received, applied := logCounters(cluster, idx)
		path := "log range repair"
		if received == 0 && applied == 0 {
			path = "snapshot"
		}
		t.Logf("node%d rejoin path: %s (received=%d applied=%d)", idx+1, path, received, applied)
	}

	// Post-rejoin writes on each previously-dead node replicate everywhere.
	post := make(map[string]bool)
	for _, idx := range []int{1, 2} {
		name := fmt.Sprintf("post-rejoin-node%d", idx+1)
		insertRow(t, cluster.Node(idx), idx+1, name)
		post[name] = true
	}
	waitForConvergence(t, cluster, 3+outage+2, postTimeout)
	assertAcknowledgedPresent(t, cluster, post)
	t.Log("post-rejoin writes on both revived nodes replicated everywhere")

	failed = false
}
