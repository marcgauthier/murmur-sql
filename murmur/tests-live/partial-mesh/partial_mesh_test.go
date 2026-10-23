// Live scenario: partial mesh topologies (chain forwarding + bootstrap
// discovery).
//
// Go port of the Rust `partial-mesh` scenario
// (/home/marc/MURMUR/tests-live/partial-mesh/tests/partial_mesh.rs). Two legs:
//   - TestChainPeersConvergeViaForwarding: three nodes peered only as a chain
//     (node1-node2, node2-node3); writes on node1 must converge on node3 via
//     forwarding, with no direct node1-node3 session and exact peer counts
//     {1, 2, 1}.
//   - TestDynamicBootstrapDiscovery: three nodes with node1 as bootstrap seed
//     discover one another with no explicit AddPeer calls, form a full mesh,
//     and converge exactly on writes from all three nodes.
//
// Plain SQL replaces the Go typed-records HTTP API of the original.
package partialmesh

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
	// pollInterval is the convergence poll cadence (Rust: 200 ms).
	pollInterval = 200 * time.Millisecond
	// chainConvergeTimeout is the chain convergence deadline (Rust: 60 s).
	chainConvergeTimeout = 60 * time.Second
	// membershipTimeout is the membership discovery deadline (Rust: 30 s).
	membershipTimeout = 30 * time.Second
	// membershipPoll is the membership poll interval (Rust: 250 ms).
	membershipPoll = 250 * time.Millisecond
	// bootstrapConvergeTimeout is the bootstrap convergence deadline
	// (Rust: 150 s under parallel load).
	bootstrapConvergeTimeout = 150 * time.Second
)

// migrateRecords installs the same schema on every node (Rust: TypedRecords
// fixture tables).
func migrateRecords(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
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
}

// insert writes one row (Rust: TypedInsert).
func insert(t *testing.T, cluster *harness.Cluster, node int, id int64, name string) {
	t.Helper()
	if _, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d insert %q failed: %v", node+1, name, err)
	}
	if err := cluster.Node(node).Flush(); err != nil {
		t.Fatalf("node%d flush after insert: %v", node+1, err)
	}
}

// names returns the canonical ordered names on one node (Rust: TypedNames).
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read record names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// waitConverged waits until every node holds exactly want (Rust:
// waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want []string, timeout time.Duration, what string) {
	t.Helper()
	ok := harness.WaitUntil(timeout, pollInterval, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			got := names(t, cluster.Node(i))
			if len(got) != len(want) {
				return false
			}
			for j := range got {
				if got[j] != want[j] {
					return false
				}
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not converge on %d records within %v (%s)", len(want), timeout, what)
	}
}

// peerCount is the live session count (Rust: spedsql_peer_count).
func peerCount(cluster *harness.Cluster, node int) int {
	h := cluster.Node(node).Replication()
	if h == nil {
		return 0
	}
	return h.Status().Peers
}

// peerConnected is the direct-session probe (Rust:
// spedsql_peer_connected{peer=...} == 1).
func peerConnected(cluster *harness.Cluster, node int, peerID [16]byte) bool {
	h := cluster.Node(node).Replication()
	if h == nil {
		return false
	}
	for _, d := range h.Status().PeersDiagnostics {
		if d.NodeID == peerID && d.Connected {
			return true
		}
	}
	return false
}

func TestChainPeersConvergeViaForwarding(t *testing.T) {
	membership := false
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        "partial-mesh-chain",
		NumNodes:    3,
		ManualPeers: true,
		// Membership discovery is off for this leg only: the engine
		// auto-dials discovered member addresses, which would self-heal
		// the chain into a full mesh. Transaction push runs over the peer
		// sessions, so forwarding is unaffected.
		Replication: &harness.ReplicationOptions{Membership: &membership},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("chain-no-convergence")
		}
		cluster.Finish(failed)
	}()
	migrateRecords(t, cluster)

	// Chain links only; nothing statically connects node1 to node3.
	for _, pair := range [][2]int{{0, 1}, {1, 0}, {1, 2}, {2, 1}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			t.Fatalf("add peer: %v", err)
		}
	}

	for i := int64(0); i < 10; i++ {
		insert(t, cluster, 0, i, fmt.Sprintf("d-%d", i))
	}
	want := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		want = append(want, fmt.Sprintf("d-%d", i))
	}
	waitConverged(t, cluster, want, chainConvergeTimeout, "chain forwarding")

	// No direct node1-node3 session exists: convergence arrived via
	// forwarding, and the peer counts are exactly the static links.
	id0 := cluster.NodeID(0)
	id2 := cluster.NodeID(2)
	if peerConnected(cluster, 0, id2) {
		t.Fatal("unexpected direct node1-node3 session (topology changed?)")
	}
	if peerConnected(cluster, 2, id0) {
		t.Fatal("unexpected direct node3-node1 session (topology changed?)")
	}
	for i, wantPeers := range []int{1, 2, 1} {
		if got := peerCount(cluster, i); got != wantPeers {
			t.Fatalf("node%d peers = %d, want %d", i+1, got, wantPeers)
		}
	}
	failed = false
}

func TestDynamicBootstrapDiscovery(t *testing.T) {
	// Node 0 acts as bootstrap seed. Nodes 1 and 2 point to node 0's repl
	// address. No static peers are configured; dynamic discovery must form
	// cluster sessions.
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:           "partial-mesh-bootstrap",
		NumNodes:       3,
		ManualPeers:    true,
		BootstrapSeeds: []int{0},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("bootstrap-no-convergence")
		}
		cluster.Finish(failed)
	}()
	migrateRecords(t, cluster)

	// Wait for dynamic discovery to find all cluster members without any
	// explicit AddPeer calls. Status.Members excludes self, so 3 nodes
	// means 2 members each; peers == 2 proves the full mesh formed.
	discovered := harness.WaitUntil(membershipTimeout, membershipPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			h := cluster.Node(i).Replication()
			if h == nil {
				return false
			}
			status := h.Status()
			if status.Members < cluster.Len()-1 || status.Peers != cluster.Len()-1 {
				return false
			}
		}
		return true
	})
	if !discovered {
		t.Fatalf("nodes did not discover %d members via gossip within %v", cluster.Len(), membershipTimeout)
	}

	// Write 10 rows on node 0 (seed).
	for i := int64(0); i < 10; i++ {
		insert(t, cluster, 0, i, fmt.Sprintf("dyn-seed-%d", i))
	}
	// Write 5 rows on node 1 (discovered peer).
	for i := int64(0); i < 5; i++ {
		insert(t, cluster, 1, 100+i, fmt.Sprintf("dyn-node1-%d", i))
	}
	// Write 5 rows on node 2 (discovered peer).
	for i := int64(0); i < 5; i++ {
		insert(t, cluster, 2, 200+i, fmt.Sprintf("dyn-node2-%d", i))
	}

	// All 20 rows must converge across all 3 nodes (150 s, not 60 s: under
	// a full parallel run the box is slow and exact convergence
	// legitimately takes over a minute; the proof — exact 20-row
	// convergence — is unchanged).
	var want []string
	for i := 0; i < 10; i++ {
		want = append(want, fmt.Sprintf("dyn-seed-%d", i))
	}
	for i := 0; i < 5; i++ {
		want = append(want, fmt.Sprintf("dyn-node1-%d", i))
	}
	for i := 0; i < 5; i++ {
		want = append(want, fmt.Sprintf("dyn-node2-%d", i))
	}
	// Canonical order must match the ORDER BY name reads.
	slices.Sort(want)
	waitConverged(t, cluster, want, bootstrapConvergeTimeout, "bootstrap discovery")
	failed = false
}
