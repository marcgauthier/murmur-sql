// Live SQL test: nodes from two distinct databases (different
// database_id) must never peer or replicate across databases, even when
// CAs are mutually trusted.
//
// Go port of tests/dbid_isolation.rs using tests-live/gosqlharness.
// Identical behavior: two 2-node clusters with static topologies
// cross-trust CA files and cross-peer every node, then each cluster must
// converge internally on its own marker while a 10 s observation window
// proves no cross-database row ever arrives. The Rust edit_config closure
// becomes SetConfig section writes.
package dbidisolation

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestCrossDatabasePeersNeverReplicate(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	clusterA, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:            "dbid-iso-a",
		NumNodes:        2,
		ReplicationYAML: "membership: false",
	})
	if err != nil {
		t.Fatalf("start cluster A: %v", err)
	}
	failedA := true
	defer func() { clusterA.Finish(failedA) }()

	clusterB, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:            "dbid-iso-b",
		NumNodes:        2,
		ReplicationYAML: "membership: false",
	})
	if err != nil {
		t.Fatalf("start cluster B: %v", err)
	}
	failedB := true
	defer func() { clusterB.Finish(failedB) }()

	// Initialize tables on both clusters.
	for _, cluster := range []*gosqlharness.Cluster{clusterA, clusterB} {
		for i := 0; i < 2; i++ {
			client := cluster.Node(i)
			if _, err := client.Simple("CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
				t.Fatalf("create records: %v", err)
			}
			if _, err := client.Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
				t.Fatalf("replicate records: %v", err)
			}
			if _, err := client.Simple("MURMUR FLUSH"); err != nil {
				t.Fatalf("flush schema: %v", err)
			}
		}
	}

	// Stop nodes on both clusters to cross-trust CAs and configure
	// cross-peering.
	for i := 0; i < 2; i++ {
		if err := clusterA.StopNode(i); err != nil {
			t.Fatalf("stop cluster A node %d: %v", i, err)
		}
		if err := clusterB.StopNode(i); err != nil {
			t.Fatalf("stop cluster B node %d: %v", i, err)
		}
	}

	caA, err := os.ReadFile(filepath.Join(clusterA.NodeInfo(0).Dir, "ca.pem"))
	if err != nil {
		t.Fatalf("read cluster A CA: %v", err)
	}
	caB, err := os.ReadFile(filepath.Join(clusterB.NodeInfo(0).Dir, "ca.pem"))
	if err != nil {
		t.Fatalf("read cluster B CA: %v", err)
	}

	// Cross-trust CAs by appending the peer CA to each node's ca.pem.
	for i := 0; i < 2; i++ {
		pathA := filepath.Join(clusterA.NodeInfo(i).Dir, "ca.pem")
		if err := os.WriteFile(pathA, []byte(fmt.Sprintf("%s\n%s", caA, caB)), 0o644); err != nil {
			t.Fatalf("cross-trust cluster A node %d: %v", i, err)
		}
		pathB := filepath.Join(clusterB.NodeInfo(i).Dir, "ca.pem")
		if err := os.WriteFile(pathB, []byte(fmt.Sprintf("%s\n%s", caB, caA)), 0o644); err != nil {
			t.Fatalf("cross-trust cluster B node %d: %v", i, err)
		}
	}

	// Cross-peer: node A[i] peers with other A nodes + all B nodes.
	// Node B[i] peers with other B nodes + all A nodes.
	replBAddrs := []string{
		clusterB.NodeInfo(0).ReplAddr.String(),
		clusterB.NodeInfo(1).ReplAddr.String(),
	}
	replAAddrs := []string{
		clusterA.NodeInfo(0).ReplAddr.String(),
		clusterA.NodeInfo(1).ReplAddr.String(),
	}

	for i := 0; i < 2; i++ {
		var bootstrapA []string
		for j := 0; j < 2; j++ {
			if j != i {
				bootstrapA = append(bootstrapA, clusterA.NodeInfo(j).ReplAddr.String())
			}
		}
		bootstrapA = append(bootstrapA, replBAddrs...)
		if err := clusterA.SetConfig(i, "replication", map[string]any{"bootstrap": bootstrapA}); err != nil {
			t.Fatalf("cross-peer cluster A node %d: %v", i, err)
		}

		var bootstrapB []string
		for j := 0; j < 2; j++ {
			if j != i {
				bootstrapB = append(bootstrapB, clusterB.NodeInfo(j).ReplAddr.String())
			}
		}
		bootstrapB = append(bootstrapB, replAAddrs...)
		if err := clusterB.SetConfig(i, "replication", map[string]any{"bootstrap": bootstrapB}); err != nil {
			t.Fatalf("cross-peer cluster B node %d: %v", i, err)
		}
	}

	// Restart all nodes.
	for i := 0; i < 2; i++ {
		if err := clusterA.StartNode(i); err != nil {
			t.Fatalf("restart cluster A node %d: %v", i, err)
		}
		if err := clusterB.StartNode(i); err != nil {
			t.Fatalf("restart cluster B node %d: %v", i, err)
		}
	}

	// Positive controls: write marker inside cluster A, write marker
	// inside cluster B.
	if _, err := clusterA.Node(0).Execute("INSERT INTO records(id, name) VALUES($1, $2)", int64(1), "cluster-a-marker"); err != nil {
		t.Fatalf("insert cluster A marker: %v", err)
	}
	if _, err := clusterA.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush cluster A marker: %v", err)
	}

	if _, err := clusterB.Node(0).Execute("INSERT INTO records(id, name) VALUES($1, $2)", int64(2), "cluster-b-marker"); err != nil {
		t.Fatalf("insert cluster B marker: %v", err)
	}
	if _, err := clusterB.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush cluster B marker: %v", err)
	}

	// Verify cluster A converges internally on marker A.
	expectedA := [][]string{{"cluster-a-marker"}}
	if err := gosqlharness.AssertRows(clusterA, []int{0, 1}, "SELECT name FROM records ORDER BY id", expectedA); err != nil {
		t.Fatalf("cluster A converge: %v", err)
	}

	// Verify cluster B converges internally on marker B.
	expectedB := [][]string{{"cluster-b-marker"}}
	if err := gosqlharness.AssertRows(clusterB, []int{0, 1}, "SELECT name FROM records ORDER BY id", expectedB); err != nil {
		t.Fatalf("cluster B converge: %v", err)
	}

	// Observation window: observe that cross-cluster replication never
	// happens.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rowsA0, err := clusterA.Node(0).Simple("SELECT name FROM records ORDER BY id")
		if err != nil {
			t.Fatalf("read cluster A node 0: %v", err)
		}
		rowsA1, err := clusterA.Node(1).Simple("SELECT name FROM records ORDER BY id")
		if err != nil {
			t.Fatalf("read cluster A node 1: %v", err)
		}
		rowsB0, err := clusterB.Node(0).Simple("SELECT name FROM records ORDER BY id")
		if err != nil {
			t.Fatalf("read cluster B node 0: %v", err)
		}
		rowsB1, err := clusterB.Node(1).Simple("SELECT name FROM records ORDER BY id")
		if err != nil {
			t.Fatalf("read cluster B node 1: %v", err)
		}

		if !reflect.DeepEqual(rowsA0, expectedA) {
			t.Fatalf("Cluster A node 0 received cross-database data! got %v", rowsA0)
		}
		if !reflect.DeepEqual(rowsA1, expectedA) {
			t.Fatalf("Cluster A node 1 received cross-database data! got %v", rowsA1)
		}
		if !reflect.DeepEqual(rowsB0, expectedB) {
			t.Fatalf("Cluster B node 0 received cross-database data! got %v", rowsB0)
		}
		if !reflect.DeepEqual(rowsB1, expectedB) {
			t.Fatalf("Cluster B node 1 received cross-database data! got %v", rowsB1)
		}

		time.Sleep(500 * time.Millisecond)
	}
	failedA = false
	failedB = false
}
