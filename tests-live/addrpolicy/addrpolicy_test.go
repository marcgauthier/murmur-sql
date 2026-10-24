// Live scenario: replication address policy (allowed_networks CIDR)
// enforcement across processes.
//
// Go port of tests/addrpolicy.rs using tests-live/gosqlharness.
// Identical behavior: node 0 admits loopback peers (127.0.0.0/8),
// node 1's TEST-NET policy (198.51.100.0/24) excludes all actual
// loopback socket addresses, node 2 is unrestricted; the admitted pair
// replicates while the denied node keeps its write local. The Rust
// toml_edit closures become SetConfig section writes.
package addrpolicy

import (
	"os"
	"testing"
	"time"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestAddressPolicyAcrossProcesses(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "addrpolicy",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	// Stop all nodes to configure address policies cleanly without stale
	// QUIC sessions.
	for _, i := range []int{0, 1, 2} {
		if err := cluster.StopNode(i); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
	}

	// Node 0 admits loopback peers.
	if err := cluster.SetConfig(0, "replication", map[string]any{
		"allowed_networks": []string{"127.0.0.0/8"},
	}); err != nil {
		t.Fatalf("node 0 allow loopback: %v", err)
	}
	// Node 1 admits only TEST-NET peers (denies all loopback addresses).
	if err := cluster.SetConfig(1, "replication", map[string]any{
		"allowed_networks": []string{"198.51.100.0/24"},
	}); err != nil {
		t.Fatalf("node 1 allow TEST-NET: %v", err)
	}
	// Node 2 has unrestricted allowed_networks (default empty allows all).

	for _, i := range []int{0, 1, 2} {
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
	}

	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			t.Fatalf("node %d create records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush schema: %v", i, err)
		}
	}

	if _, err := cluster.Node(0).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(1), "allowed-loopback"); err != nil {
		t.Fatalf("node 0 insert: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node 0 flush: %v", err)
	}
	if _, err := cluster.Node(1).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(2), "denied-testnet"); err != nil {
		t.Fatalf("node 1 insert: %v", err)
	}
	if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node 1 flush: %v", err)
	}
	if _, err := cluster.Node(2).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(3), "unrestricted-loopback"); err != nil {
		t.Fatalf("node 2 insert: %v", err)
	}
	if _, err := cluster.Node(2).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node 2 flush: %v", err)
	}

	// Admitted pair (Node 0 and Node 2) must converge on both rows.
	expectedAdmitted := [][]string{{"allowed-loopback"}, {"unrestricted-loopback"}}
	if err := gosqlharness.AssertRows(cluster, []int{0, 2},
		"SELECT name FROM records ORDER BY name", expectedAdmitted); err != nil {
		t.Fatalf("converge admitted pair: %v", err)
	}

	// Settle window: several dial/accept retries must not move the denied
	// node's state.
	time.Sleep(1000 * time.Millisecond)

	// Node 1 must retain ONLY its local write.
	n1Rows, err := cluster.Node(1).Simple("SELECT name FROM records ORDER BY name")
	if err != nil {
		t.Fatalf("node 1 read names: %v", err)
	}
	if !equalRows(n1Rows, [][]string{{"denied-testnet"}}) {
		t.Fatalf("denied node state changed across network: want only its local row, got %v", n1Rows)
	}

	// Node 0 must retain exactly the 2 admitted rows.
	n0Rows, err := cluster.Node(0).Simple("SELECT name FROM records ORDER BY name")
	if err != nil {
		t.Fatalf("node 0 read names: %v", err)
	}
	if !equalRows(n0Rows, expectedAdmitted) {
		t.Fatalf("admitted node received unexpected extra rows: got %v", n0Rows)
	}

	// Verify peer topology via MURMUR SHOW PEERS.
	n1Peers, err := cluster.Node(1).Simple("MURMUR SHOW PEERS")
	if err != nil {
		t.Fatalf("node 1 show peers: %v", err)
	}
	for _, row := range n1Peers {
		// row[2] is connected.
		if row[2] == "true" {
			t.Fatalf("denied Node 1 unexpectedly formed a connected session with %s", row[0])
		}
	}

	for i := 0; i < cluster.Len(); i++ {
		if _, err := os.Stat(cluster.NodeInfo(i).Dir); err != nil {
			t.Fatalf("node %d directory does not exist", i)
		}
	}
	failed = false
}

func equalRows(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}
