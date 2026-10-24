// Live scenario: two nodes killed simultaneously must rejoin at once and
// catch up without errors.
//
// Go port of tests/rejoin_storm.rs using tests-live/goharness. Identical
// behavior: a 3-node cluster converges on 10 base rows, nodes 1-2 are
// killed, node 0 acknowledges 30 more rows while they are down, both nodes
// restart in parallel and remesh, and all three nodes converge on all 40
// rows with full peer counts, received replication traffic, and no
// replication errors.
package rejoinstorm

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// wait is the convergence/peer-count deadline (Rust: WAIT = 120 s).
const wait = 120 * time.Second

func TestDualKillSimultaneousRejoin(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("rejoin-storm-sql")
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if err := goharness.Fixture(cluster); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	var expected [][]string
	for n := 0; n < 10; n++ {
		id := fmt.Sprintf("base-%03d", n)
		name := fmt.Sprintf("base-%d", n)
		if _, err := cluster.Node(0).Execute(
			"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
		expected = append(expected, []string{id, name})
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush base rows: %v", err)
	}
	if err := goharness.Converge(cluster, []int{0, 1, 2}, expected, wait); err != nil {
		t.Fatalf("converge base rows: %v", err)
	}
	for _, node := range []int{1, 2} {
		if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d pre-kill flush: %v", node, err)
		}
		cluster.KillNode(node)
	}
	for n := 0; n < 30; n++ {
		id := fmt.Sprintf("offline-%03d", n)
		name := fmt.Sprintf("acknowledged-%d", n)
		if _, err := cluster.Node(0).Execute(
			"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
		expected = append(expected, []string{id, name})
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush offline rows: %v", err)
	}
	if err := cluster.StartNodesParallel([]int{1, 2}); err != nil {
		t.Fatalf("restart nodes 1-2: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("remesh: %v", err)
	}
	sort.Slice(expected, func(a, b int) bool {
		if expected[a][0] != expected[b][0] {
			return expected[a][0] < expected[b][0]
		}
		return expected[a][1] < expected[b][1]
	})
	if err := goharness.Converge(cluster, []int{0, 1, 2}, expected, wait); err != nil {
		t.Fatalf("converge after rejoin: %v", err)
	}
	if err := goharness.PeerCounts(cluster, []int{0, 1, 2}, []int{2, 2, 2}, wait); err != nil {
		t.Fatalf("peer counts: %v", err)
	}
	for i := 1; i < 3; i++ {
		s, err := goharness.Status(cluster.Node(i))
		if err != nil {
			t.Fatalf("node %d status: %v", i, err)
		}
		received, ok := statusNumber(s, "replication", "metrics", "received_transactions")
		if !ok || received <= 0 {
			t.Fatalf("node %d received_transactions = %v, want > 0", i, received)
		}
		if lastErr := statusValue(s, "replication", "last_error"); lastErr != nil {
			t.Fatalf("node %d last_error = %v, want null", i, lastErr)
		}
	}
	failed = false
}

// statusValue walks a MURMUR SHOW STATUS JSON snapshot along path, reporting
// nil for a missing key or an explicit JSON null (matching serde_json
// indexing, where a missing key reads as Null).
func statusValue(s any, path ...string) any {
	cur := s
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur, ok = m[p]
		if !ok {
			return nil
		}
	}
	return cur
}

// statusNumber walks a status snapshot and returns the value as a float64.
func statusNumber(s any, path ...string) (float64, bool) {
	v := statusValue(s, path...)
	f, ok := v.(float64)
	return f, ok
}
