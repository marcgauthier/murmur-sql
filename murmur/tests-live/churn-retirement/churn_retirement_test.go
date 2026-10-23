// Live scenario: exclude persists, releases retention, and re-add heals.
//
// Go port of the Rust `churn-retirement` scenario
// (/home/marc/MURMUR/tests-live/churn-retirement/tests/churn_retirement.rs).
// Three nodes mesh; node3 is excluded on the survivors. Sessions to
// node3 must drop, its writes must stop arriving, exclusion must survive
// a survivor restart with no rediscovery re-mesh, explicit re-adding must
// heal replication, and the retired peer must release its GC retention
// pin. Plain SQL replaces the typed-records HTTP API, and the
// Prometheus legs map to Status()/RetentionStatus().
package churnretirement

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

const (
	// poll is the convergence poll interval (Rust: 200 ms).
	poll = 200 * time.Millisecond
	// convergeTimeout is the convergence deadline (Rust: 60 s).
	convergeTimeout = 60 * time.Second
	// peersTimeout is the session-drop / peer-count deadline (Rust: 60 s).
	peersTimeout = 60 * time.Second
	// gatingTimeout is the retention-entry appearance deadline after
	// baseline convergence (Rust: 30 s).
	gatingTimeout = 30 * time.Second
	// isolationHold is the negative-assertion hold after the isolation
	// write (Rust: 5 s).
	isolationHold = 5 * time.Second
)

// ddl is the plain-SQL stand-in for the typed-records fixture table.
const ddl = "CREATE TABLE records(name TEXT NOT NULL PRIMARY KEY)"

// baselineNames are the baseline rows (Rust: a-0 .. a-4 inserted on
// node 0).
func baselineNames() []string {
	out := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		out = append(out, fmt.Sprintf("a-%d", i))
	}
	return out
}

// names reads all row names on one node, sorted (Rust: TypedNames).
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records")
	})
	if err != nil {
		t.Fatalf("read record names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	slices.Sort(out)
	return out
}

// insert writes one row (Rust: TypedInsert).
func insert(t *testing.T, cluster *harness.Cluster, node int, name string) {
	t.Helper()
	_, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(name) VALUES(?)", name)
		return nil, err
	})
	if err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
	if err := cluster.Node(node).Flush(); err != nil {
		t.Fatalf("flush after insert: %v", err)
	}
}

// waitConverged waits until every node holds exactly want (Rust:
// waitConverged: equal counts and equal joined-name digests on all
// nodes).
func waitConverged(t *testing.T, cluster *harness.Cluster, want []string, what string) {
	t.Helper()
	ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if !slices.Equal(names(t, cluster.Node(i)), want) {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not converge on %v within %v (%s)", want, convergeTimeout, what)
	}
}

// peerCount is the live session count (Rust: spedsql_peer_count).
func peerCount(t *testing.T, cluster *harness.Cluster, node int) int {
	t.Helper()
	h := cluster.Node(node).Replication()
	if h == nil {
		t.Fatalf("node%d replication disabled", node+1)
	}
	return h.Status().Peers
}

// peerConnected is true while a live session to id exists (Rust:
// spedsql_peer_connected with the peer label).
func peerConnected(t *testing.T, cluster *harness.Cluster, node int, id [16]byte) bool {
	t.Helper()
	h := cluster.Node(node).Replication()
	if h == nil {
		t.Fatalf("node%d replication disabled", node+1)
	}
	for _, d := range h.Status().PeersDiagnostics {
		if d.NodeID == id && d.Connected {
			return true
		}
	}
	return false
}

// gatingMembers counts the peers currently pinning history retention
// (Rust: spedsql_gating_members).
func gatingMembers(t *testing.T, cluster *harness.Cluster, node int) int {
	t.Helper()
	st, err := cluster.Node(node).RetentionStatus()
	if err != nil {
		t.Fatalf("retention status: %v", err)
	}
	n := 0
	for _, p := range st.Peers {
		if !p.Retired {
			n++
		}
	}
	return n
}

// isRetired is true once id's retention record is marked retired.
func isRetired(t *testing.T, cluster *harness.Cluster, node int, id [16]byte) bool {
	t.Helper()
	st, err := cluster.Node(node).RetentionStatus()
	if err != nil {
		t.Fatalf("retention status: %v", err)
	}
	for _, p := range st.Peers {
		if p.Node == id && p.Retired {
			return true
		}
	}
	return false
}

func TestExcludePersistsReleasesRetentionAndReaddHeals(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "churn-retirement",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("churn-retirement")
		}
		cluster.Finish(failed)
	}()

	id2 := cluster.NodeID(2)
	baseline := baselineNames()

	// Same schema on every node (Rust: TypedRecords fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, ddl); err != nil {
			t.Fatalf("migrate records table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("records", true); err != nil {
			t.Fatalf("opt-in records: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Baseline: five rows from node 0 converge on all three nodes.
	for _, name := range baseline {
		insert(t, cluster, 0, name)
	}
	waitConverged(t, cluster, baseline, "baseline")

	// Both peers must be pinning retention before the exclusion leg
	// (Rust: gatingBefore baseline read).
	id1 := cluster.NodeID(1)
	gatingSeen := harness.WaitUntil(gatingTimeout, poll, func() bool {
		st, err := cluster.Node(0).RetentionStatus()
		if err != nil {
			t.Fatalf("retention status: %v", err)
		}
		for _, id := range [][16]byte{id1, id2} {
			found := false
			for _, p := range st.Peers {
				if p.Node == id && !p.Retired {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	})
	if !gatingSeen {
		t.Fatalf("node1/node3 retention pins never appeared on node1 within %v", gatingTimeout)
	}
	gatingBefore := gatingMembers(t, cluster, 0)

	// Exclude node3 on both survivors (Rust: RemovePeer, reversible leg).
	if err := cluster.RemovePeer(0, 2); err != nil {
		t.Fatalf("exclude node3 on node1: %v", err)
	}
	if err := cluster.RemovePeer(1, 2); err != nil {
		t.Fatalf("exclude node3 on node2: %v", err)
	}

	// Sessions to node3 drop on both survivors.
	dropped := harness.WaitUntil(peersTimeout, poll, func() bool {
		return !peerConnected(t, cluster, 0, id2) && !peerConnected(t, cluster, 1, id2)
	})
	if !dropped {
		t.Fatalf("sessions to node3 did not drop within %v", peersTimeout)
	}
	// Survivors each see exactly one peer (each other).
	for _, i := range []int{0, 1} {
		one := harness.WaitUntil(peersTimeout, poll, func() bool {
			return peerCount(t, cluster, i) == 1
		})
		if !one {
			t.Fatalf("node%d peers did not settle to 1 within %v after exclusion", i+1, peersTimeout)
		}
	}

	// Node3's writes no longer arrive on the survivors.
	insert(t, cluster, 2, "isolated")
	time.Sleep(isolationHold)
	for _, i := range []int{0, 1} {
		got := names(t, cluster.Node(i))
		if !slices.Equal(got, baseline) {
			t.Fatalf("node%d names after isolation hold: %v; want survivors-only %v", i+1, got, baseline)
		}
	}

	// Exclusion survives a survivor restart with no rediscovery re-mesh.
	// StartNode reopens under the unlock watchdog; the survivor must
	// be re-meshed to its fellow survivor (fresh ephemeral port), and
	// only to it.
	cluster.StopNode(0)
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart survivor node1: %v", err)
	}
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("re-mesh restarted node1: %v", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatalf("re-mesh node2 to node1: %v", err)
	}
	remeshed := harness.WaitUntil(peersTimeout, poll, func() bool {
		return peerCount(t, cluster, 0) == 1
	})
	if !remeshed {
		t.Fatalf("restarted node1 did not re-mesh its fellow survivor within %v", peersTimeout)
	}
	if peerConnected(t, cluster, 0, id2) {
		t.Fatalf("node1 re-meshed excluded node3 after restart")
	}
	if got := peerCount(t, cluster, 0); got != 1 {
		t.Fatalf("node1 peers after restart: got %d, want 1 (exclusion lost?)", got)
	}
	if got := names(t, cluster.Node(0)); !slices.Equal(got, baseline) {
		t.Fatalf("restarted node1 lost survivor rows: %v", got)
	}

	// Re-adding heals replication (removal was not destructive).
	if err := cluster.AddPeer(0, 2); err != nil {
		t.Fatalf("re-add node3 on node1: %v", err)
	}
	if err := cluster.AddPeer(1, 2); err != nil {
		t.Fatalf("re-add node3 on node2: %v", err)
	}
	healed := append(slices.Clone(baseline), "isolated")
	slices.Sort(healed)
	waitConverged(t, cluster, healed, "post re-add heal")

	// Retention release (Rust: spedsql_gating_members drop after
	// RemovePeer). The product splits reversible exclusion from
	// permanent retirement, so this leg runs after the heal: a retired
	// peer can never rejoin (session setup rejects retired peers).
	if err := cluster.Node(0).RetirePeer(id2); err != nil {
		t.Fatalf("retire node3 on node1: %v", err)
	}
	if err := cluster.Node(1).RetirePeer(id2); err != nil {
		t.Fatalf("retire node3 on node2: %v", err)
	}
	for _, i := range []int{0, 1} {
		gatingAfter := gatingMembers(t, cluster, i)
		if !(gatingAfter < gatingBefore) {
			t.Fatalf("node%d gating members = %d, want below baseline %d", i+1, gatingAfter, gatingBefore)
		}
		if !isRetired(t, cluster, i, id2) {
			t.Fatalf("node%d has no retired retention record for node3", i+1)
		}
		// Retirement also excludes: sessions to node3 drop again.
		redropped := harness.WaitUntil(peersTimeout, poll, func() bool {
			return !peerConnected(t, cluster, i, id2) && peerCount(t, cluster, i) == 1
		})
		if !redropped {
			t.Fatalf("node%d did not drop retired node3 within %v", i+1, peersTimeout)
		}
	}
	failed = false
}
