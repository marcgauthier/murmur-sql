// Live scenario: chain peers converge via forwarding, and dynamic
// bootstrap discovery meshes an unconfigured cluster.
//
// Go port of tests/partial_mesh.rs using tests-live/goharness. Identical
// behavior: a statically-meshed chain (0<->1<->2, membership off)
// forwards 30 rows from node 0 to node 2 without a direct session, and a
// seed-bootstrapped cluster discovers all peers and converges on one row
// per node.
package partialmesh

import (
	"fmt"
	"os"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// wait is the convergence/peer-count deadline (Rust: WAIT = 120 s).
const wait = 120 * time.Second

func TestChainPeersConvergeViaForwarding(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("partial-mesh-chain")
	opts.ManualPeers = true
	membership := false
	opts.Replication = &goharness.ReplicationOptions{Membership: &membership}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if err := goharness.Fixture(cluster); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	for _, pair := range [][2]int{{0, 1}, {1, 0}, {1, 2}, {2, 1}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			t.Fatalf("add peer %d->%d: %v", pair[0], pair[1], err)
		}
	}
	if err := goharness.PeerCounts(cluster, []int{0, 1, 2}, []int{1, 2, 1}, wait); err != nil {
		t.Fatalf("peer counts: %v", err)
	}
	var expected [][]string
	for i := 0; i < 30; i++ {
		id := fmt.Sprintf("%04d", i)
		name := fmt.Sprintf("chain-%d", i)
		if _, err := cluster.Node(0).Execute(
			"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
		expected = append(expected, []string{id, name})
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush chain rows: %v", err)
	}
	if err := goharness.Converge(cluster, []int{0, 1, 2}, expected, wait); err != nil {
		t.Fatalf("converge chain rows: %v", err)
	}
	node2ID := cluster.NodeID(2)
	node2 := goharness.Hex(node2ID[:])
	peers, err := goharness.Peers(cluster.Node(0))
	if err != nil {
		t.Fatalf("node 0 peers: %v", err)
	}
	for _, p := range peers {
		if len(p) > 2 && p[0] == node2 && p[2] == "true" {
			t.Fatalf("node 0 holds a direct session to node 2, want forwarding only")
		}
	}
	failed = false
}

func TestDynamicBootstrapDiscovery(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("partial-mesh-discovery")
	opts.ManualPeers = true
	opts.BootstrapSeeds = []int{0}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if err := goharness.Fixture(cluster); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if err := goharness.PeerCounts(cluster, []int{0, 1, 2}, []int{2, 2, 2}, wait); err != nil {
		t.Fatalf("peer counts: %v", err)
	}
	var expected [][]string
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("%d", i)
		name := fmt.Sprintf("discovered-%d", i)
		if _, err := cluster.Node(i).Execute(
			"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
			t.Fatalf("node %d insert %s: %v", i, id, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush: %v", i, err)
		}
		expected = append(expected, []string{id, name})
	}
	if err := goharness.Converge(cluster, []int{0, 1, 2}, expected, wait); err != nil {
		t.Fatalf("converge discovered rows: %v", err)
	}
	failed = false
}
