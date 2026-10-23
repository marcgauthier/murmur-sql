// Live scenario: a partitioned node repairs its missed log tail from peers.
//
// Go port of the Rust `tail-repair` scenario
// (/home/marc/MURMUR/tests-live/tail-repair/tests/tail_repair.rs), itself a
// port of the original Go `tail-repair` scenario
// (TestPartitionedNodeRepairsTailFromPeerLogs). A node partitioned
// mid-write-burst misses a contiguous range of its peers' origin logs, then
// heals and must repair the missing tail from peer logs WITHOUT the snapshot
// path: row counts and ordered digests converge everywhere while the healed
// node's log-path counters prove the range arrived as transactions, not as a
// snapshot.
//
// Plain SQL replaces the managed typed-records HTTP API.
package tailrepair

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// pollInterval is the convergence poll interval (Rust: 100 ms).
	pollInterval = 100 * time.Millisecond
	// baselineTimeout is the baseline convergence deadline (Rust: 30 s).
	baselineTimeout = 30 * time.Second
	// splitPeersTimeout is the split peer-set settle deadline (Rust: 15 s).
	splitPeersTimeout = 15 * time.Second
	// healPeersTimeout is the heal peer-set settle deadline (Rust: 30 s).
	healPeersTimeout = 30 * time.Second
	// probeTimeout is the isolation-probe reach deadline (Rust: 15 s).
	probeTimeout = 15 * time.Second
	// settleMargin is the negative-check settle margin before asserting the
	// isolate is frozen (Rust: 2 s sleep).
	settleMargin = 2 * time.Second
	// burstTimeout is the burst convergence deadline (Rust: 30 s).
	burstTimeout = 30 * time.Second
	// healTimeout is the full post-heal convergence deadline (Rust: 60 s).
	healTimeout = 60 * time.Second
	// postHealTimeout is the post-heal write deadline (Rust: 30 s).
	postHealTimeout = 30 * time.Second
)

const (
	// baselineRows is the baseline row count on node 0 (Rust: 10).
	baselineRows = 10
	// probeID is the isolation-probe row id (Rust: 9000).
	probeID = 9000
	// burstID is the burst row id base (Rust: 10000+i).
	burstID = 10000
	// postHealID is the post-heal row id on the healed node (Rust: 20000).
	postHealID = 20000
)

// ddl is the shared records table (Rust: TypedRecords fixture tables).
const ddl = "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"

// markerRow is one marker row: the Rust (id, name) pair.
type markerRow struct {
	id   int64
	name string
}

// queryRows reads all marker rows on one node (Rust: TypedContentionRows).
// Errors are returned so convergence polls can treat them as
// not-yet-converged.
func queryRows(db *murmur.Database) ([]markerRow, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name FROM records")
	})
	if err != nil {
		return nil, err
	}
	var out []markerRow
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, markerRow{id: row[0].Int, name: row[1].Str})
	}
	return out, nil
}

// mustRows reads all marker rows on one node, failing the test on error.
func mustRows(t *testing.T, db *murmur.Database) []markerRow {
	t.Helper()
	rows, err := queryRows(db)
	if err != nil {
		t.Fatalf("read marker rows: %v", err)
	}
	return rows
}

// digest renders the canonical digest of a row set (Rust: digestRows, sorted
// then debug-rendered).
func digest(rows []markerRow) string {
	cp := append([]markerRow(nil), rows...)
	sort.Slice(cp, func(i, j int) bool {
		if cp[i].id != cp[j].id {
			return cp[i].id < cp[j].id
		}
		return cp[i].name < cp[j].name
	})
	return fmt.Sprintf("%v", cp)
}

// insertMarker inserts one marker row and publishes it (Rust: insertMarker).
func insertMarker(t *testing.T, cluster *harness.Cluster, node int, id int64, name string) {
	t.Helper()
	if _, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d insert %q failed: %v", node, name, err)
	}
	if err := cluster.Node(node).Flush(); err != nil {
		t.Fatalf("node%d flush after insert: %v", node, err)
	}
}

// waitRowCount waits until one node holds exactly want rows (Rust:
// waitRowCount).
func waitRowCount(t *testing.T, cluster *harness.Cluster, idx, want int, timeout time.Duration, what string) {
	t.Helper()
	ok := harness.WaitUntil(timeout, pollInterval, func() bool {
		rows, err := queryRows(cluster.Node(idx))
		return err == nil && len(rows) == want
	})
	if !ok {
		t.Fatalf("node%d typed row count = %d, want %d within %v (%s)",
			idx, len(mustRows(t, cluster.Node(idx))), want, timeout, what)
	}
}

// waitConverged waits until every node holds exactly want rows with equal
// digests (Rust: waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration, what string) {
	t.Helper()
	ok := harness.WaitUntil(timeout, pollInterval, func() bool {
		var first string
		for i := 0; i < cluster.Len(); i++ {
			rows, err := queryRows(cluster.Node(i))
			if err != nil || len(rows) != want {
				return false
			}
			d := digest(rows)
			if i == 0 {
				first = d
			} else if d != first {
				return false
			}
		}
		return true
	})
	if !ok {
		for i := 0; i < cluster.Len(); i++ {
			rows, err := queryRows(cluster.Node(i))
			if err != nil {
				t.Logf("node%d at timeout: read error %v", i, err)
				continue
			}
			t.Logf("node%d at timeout: count=%d digest=%s", i, len(rows), digest(rows))
		}
		t.Fatalf("nodes did not converge on %d rows with equal digests within %v (%s)", want, timeout, what)
	}
}

// waitConnectedPeers waits until one node reports exactly want replication
// peers (Rust: wait_connected_peers over the status peer count).
func waitConnectedPeers(t *testing.T, cluster *harness.Cluster, idx, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, pollInterval, func() bool {
		h := cluster.Node(idx).Replication()
		return h != nil && h.Status().Peers == want
	})
	if !ok {
		peers := -1
		if h := cluster.Node(idx).Replication(); h != nil {
			peers = h.Status().Peers
		}
		t.Fatalf("node%d connected peers = %d, want %d within %v", idx, peers, want, timeout)
	}
}

// logCounters reads one node's log-path activity counters
// (received_transactions, applied_transactions). Snapshot application
// bypasses both counters, so advancement across the heal proves the missed
// range arrived as log transactions. Note the metrics mirror of applied is
// laggy (synced on a slow engine tick): the top-level applied counter
// advances synchronously and is the only exact observable here.
func logCounters(t *testing.T, cluster *harness.Cluster, idx int) (uint64, uint64) {
	t.Helper()
	h := cluster.Node(idx).Replication()
	if h == nil {
		t.Fatalf("node%d has no replication handle", idx)
	}
	st := h.Status()
	return st.Metrics.ReceivedTransactions, st.AppliedTransactions
}

// envUsize reads a positive-integer env knob with fallback (Rust: env_usize).
func envUsize(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

// TestPartitionedNodeRepairsTailFromPeerLogs partitions node3 away during a
// survivor write burst, then heals and requires the missed contiguous range
// to arrive via log repair, never via snapshot (Rust:
// partitioned_node_repairs_tail_from_peer_logs).
func TestPartitionedNodeRepairsTailFromPeerLogs(t *testing.T) {
	burstRows := envUsize("MURMUR_TAIL_REPAIR_BURST_ROWS", 100)
	// Generous retention: the missed range must stay comfortably inside
	// every peer's origin log, so log repair (not snapshot resync) is the
	// only legitimate healing path. Mirrors the Rust 300 s / 10000-batch
	// overlays; maximum_offline keeps its 7-day default, which is strictly
	// more generous.
	retention := 300 * time.Second
	minimumRetained := uint64(10000)
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "tail-repair",
		NumNodes: 3,
		Replication: &harness.ReplicationOptions{
			Retention:       &retention,
			MinimumRetained: &minimumRetained,
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("tail-repair-no-convergence")
		}
		cluster.Finish(failed)
	}()

	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, ddl); err != nil {
			t.Fatalf("node%d migrate records table: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("records", true); err != nil {
			t.Fatalf("node%d opt-in records: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}

	// Baseline: 10 rows on node 0, converged everywhere.
	for i := 0; i < baselineRows; i++ {
		insertMarker(t, cluster, 0, int64(i), "base-"+strconv.Itoa(i))
	}
	waitConverged(t, cluster, baselineRows, baselineTimeout, "baseline")
	baselineDigest := digest(mustRows(t, cluster.Node(2)))

	// Log-path baseline on the soon-isolated node.
	recvBefore, appliedBefore := logCounters(t, cluster, 2)
	t.Logf("node3 pre-partition received_transactions=%d applied_transactions=%d", recvBefore, appliedBefore)

	// Partition node3 away from both peers in both directions.
	for _, link := range [][2]int{{2, 0}, {0, 2}, {2, 1}, {1, 2}} {
		if err := cluster.RemovePeer(link[0], link[1]); err != nil {
			t.Fatalf("remove peer %d->%d: %v", link[0], link[1], err)
		}
	}
	waitConnectedPeers(t, cluster, 2, 0, splitPeersTimeout)

	// Isolation probe: one row on node 0 must reach node 1 but never node3.
	insertMarker(t, cluster, 0, probeID, "probe")
	waitRowCount(t, cluster, 1, baselineRows+1, probeTimeout, "probe on survivor")
	time.Sleep(settleMargin) // negative check needs a settle margin
	isolated := mustRows(t, cluster.Node(2))
	if len(isolated) != baselineRows {
		t.Fatalf("partition leaked: node3 count = %d, want %d", len(isolated), baselineRows)
	}
	if digest(isolated) != baselineDigest {
		t.Fatalf("partition leaked: node3 digest changed while partitioned")
	}

	// Write burst on the survivors while node3 is isolated.
	for i := 0; i < burstRows; i++ {
		insertMarker(t, cluster, i%2, int64(burstID+i), "burst-"+strconv.Itoa(i))
	}
	want := baselineRows + 1 + burstRows
	waitRowCount(t, cluster, 0, want, burstTimeout, "burst on node1")
	waitRowCount(t, cluster, 1, want, burstTimeout, "burst on node2")
	// Anti-vacuity: node3 must actually be missing the contiguous range.
	isolated = mustRows(t, cluster.Node(2))
	if len(isolated) != baselineRows {
		t.Fatalf("node3 count = %d, want %d (it must miss the burst range)", len(isolated), baselineRows)
	}
	missed := want - baselineRows
	t.Logf("node3 missed %d rows while partitioned", missed)

	// Heal and require full convergence.
	for _, link := range [][2]int{{2, 0}, {0, 2}, {2, 1}, {1, 2}} {
		if err := cluster.AddPeer(link[0], link[1]); err != nil {
			t.Fatalf("add peer %d->%d: %v", link[0], link[1], err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		waitConnectedPeers(t, cluster, i, 2, healPeersTimeout)
	}
	waitConverged(t, cluster, want, healTimeout, "post-heal")

	// The healing must have used log range repair, not the snapshot path.
	// The engine only requests/serves/accepts snapshots for explicitly
	// trusted snapshot sources, and harness clusters never configure any,
	// so no snapshot could have been taken; the counter advancement below
	// proves the missed range arrived and applied as log transactions
	// (snapshot application bypasses both counters).
	recvAfter, appliedAfter := logCounters(t, cluster, 2)
	t.Logf("node3 log repair: received %d->%d applied %d->%d (missed %d rows)",
		recvBefore, recvAfter, appliedBefore, appliedAfter, missed)
	if recvAfter-recvBefore < uint64(missed) {
		t.Fatalf("node3 received_transactions advanced by %d, want at least %d (healing must pull the missed range from peer logs)",
			recvAfter-recvBefore, missed)
	}
	if appliedAfter-appliedBefore < uint64(missed) {
		t.Fatalf("node3 applied_transactions advanced by %d, want at least %d (healing must apply the missed range from peer logs)",
			appliedAfter-appliedBefore, missed)
	}

	// Post-heal write on the healed node replicates everywhere.
	insertMarker(t, cluster, 2, postHealID, "post-heal")
	waitConverged(t, cluster, want+1, postHealTimeout, "post-heal write")
	failed = false
}
