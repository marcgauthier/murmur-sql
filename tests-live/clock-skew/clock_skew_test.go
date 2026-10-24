// Live scenario: contended same-key writes across nodes must resolve
// deterministically to identical LWW winners on every node.
//
// Go port of tests/clock_skew.rs using tests-live/gosqlharness.
// Identical behavior: concurrent same-key writes on nodes 0-1 converge
// to identical winners on all 3 nodes, then node 2's later updates win
// everywhere. The Rust tokio::join! over the two writers becomes a
// sync.WaitGroup over two goroutines.
package clockskew

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

// rows is the contended key count (Rust: ROWS = 20).
const rows = 20

func TestSkewedClocksResolveIdenticalWinners(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "clock-skew",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE IF NOT EXISTS skew(id TEXT NOT NULL PRIMARY KEY, val TEXT NOT NULL)"); err != nil {
			t.Fatalf("node %d create skew: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE skew REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate skew: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush schema: %v", i, err)
		}
	}

	// Phase 1: Concurrent same-key writes on node 0 and node 1.
	var wg sync.WaitGroup
	writerErrs := make([]error, 2)
	for node := 0; node < 2; node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			for i := 0; i < rows; i++ {
				id := fmt.Sprintf("skew-%02d", i)
				val := fmt.Sprintf("val-node%d-%02d", node, i)
				if _, err := cluster.Node(node).Execute(
					"INSERT OR REPLACE INTO skew(id, val) VALUES($1, $2)", id, val); err != nil {
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

	// Wait for all nodes in the cluster to converge to identical state.
	deadline := time.Now().Add(30 * time.Second)
	for {
		r0, err := cluster.Node(0).Simple("SELECT id, val FROM skew ORDER BY id")
		if err != nil {
			t.Fatalf("node 0 read skew: %v", err)
		}
		r1, err := cluster.Node(1).Simple("SELECT id, val FROM skew ORDER BY id")
		if err != nil {
			t.Fatalf("node 1 read skew: %v", err)
		}
		r2, err := cluster.Node(2).Simple("SELECT id, val FROM skew ORDER BY id")
		if err != nil {
			t.Fatalf("node 2 read skew: %v", err)
		}
		if len(r0) == rows && equalRows(r0, r1) && equalRows(r1, r2) {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("Cluster failed to converge to identical winners on 20 rows")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Phase 2: Sequential updates simulating higher HLC clock (fast clock
	// wins). Node 2 updates all 20 rows.
	var expectedUpdated [][]string
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("skew-%02d", i)
		val := fmt.Sprintf("val-node2-winner-%02d", i)
		if _, err := cluster.Node(2).Execute(
			"UPDATE skew SET val = $1 WHERE id = $2", val, id); err != nil {
			t.Fatalf("node 2 update %s: %v", id, err)
		}
		expectedUpdated = append(expectedUpdated, []string{id, val})
	}
	if _, err := cluster.Node(2).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node 2 flush updates: %v", err)
	}

	// Verify all 3 nodes converge to node 2's later writes.
	if err := gosqlharness.AssertRows(cluster, []int{0, 1, 2},
		"SELECT id, val FROM skew ORDER BY id", expectedUpdated); err != nil {
		t.Fatalf("converge node 2 winners: %v", err)
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
