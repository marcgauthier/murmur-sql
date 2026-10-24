// Live scenario: node identity allow-list enforcement.
//
// Go port of tests/allow_nodes.rs using tests-live/gosqlharness.
// Identical behavior: node 1's allow-list permits only node 0 while the
// other nodes accept any valid identity; node 2 communicates with
// node 1 strictly indirectly through node 0, yet all writes converge
// completely. The Rust toml_edit closure becomes a SetConfig section
// write and the Rust tokio::join! over the three writers becomes a
// sync.WaitGroup over three goroutines.
package allownodes

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestAllowedNodeIDsAcrossProcesses(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "allow-nodes",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	// Configure Node 2 (index 1) to allow only Node 1 (index 0).
	node0ID := cluster.NodeInfo(0).NodeID
	if err := cluster.StopNode(1); err != nil {
		t.Fatalf("stop node 1: %v", err)
	}
	if err := cluster.SetConfig(1, "replication", map[string]any{
		"allowed_peers": []string{node0ID},
	}); err != nil {
		t.Fatalf("node 1 allow node 0: %v", err)
	}
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart node 1: %v", err)
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

	// Concurrent writers on all three nodes.
	const numWrites = 15
	var wg sync.WaitGroup
	writerErrs := make([]error, cluster.Len())
	for node := 0; node < cluster.Len(); node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			for seq := 0; seq < numWrites; seq++ {
				id := int64(node+1)*10_000 + int64(seq)
				name := fmt.Sprintf("node%d-write-%02d", node, seq)
				if _, err := cluster.Node(node).Execute(
					"INSERT INTO records(id, name) VALUES($1, $2)", id, name); err != nil {
					writerErrs[node] = err
					return
				}
			}
			if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
				writerErrs[node] = err
			}
		}(node)
	}
	wg.Wait()
	for node, err := range writerErrs {
		if err != nil {
			t.Fatalf("node %d writer: %v", node, err)
		}
	}

	// Construct expected rows (15 per node = 45 total).
	var expected [][]string
	for node := 0; node < 3; node++ {
		for seq := 0; seq < numWrites; seq++ {
			expected = append(expected, []string{fmt.Sprintf("node%d-write-%02d", node, seq)})
		}
	}
	sort.Slice(expected, func(a, b int) bool { return expected[a][0] < expected[b][0] })

	if err := gosqlharness.AssertRows(cluster, []int{0, 1, 2},
		"SELECT name FROM records ORDER BY name", expected); err != nil {
		t.Fatalf("converge all rows: %v", err)
	}

	// Verify peer topology via MURMUR SHOW PEERS:
	// Node 1 (index 1) must have only Node 0, NEVER Node 2.
	node2ID := cluster.NodeInfo(2).NodeID
	n1Peers, err := cluster.Node(1).Simple("MURMUR SHOW PEERS")
	if err != nil {
		t.Fatalf("node 1 show peers: %v", err)
	}
	for _, row := range n1Peers {
		if row[0] == node2ID {
			t.Fatal("Node 1 directly connected to unauthorized Node 2!")
		}
	}

	// Node 2 (index 2) must have only Node 0, NEVER Node 1.
	node1ID := cluster.NodeInfo(1).NodeID
	n2Peers, err := cluster.Node(2).Simple("MURMUR SHOW PEERS")
	if err != nil {
		t.Fatalf("node 2 show peers: %v", err)
	}
	for _, row := range n2Peers {
		if row[0] == node1ID {
			t.Fatal("Node 2 directly connected to unauthorized Node 1!")
		}
	}
	failed = false
}
