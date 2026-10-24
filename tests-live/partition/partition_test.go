// Live scenario: four real SQL servers retain independent writes across
// an isolated two-pair topology.
//
// Go port of tests/partition.rs using tests-live/gosqlharness. Identical
// behavior: four nodes converge on a baseline row, restart into isolated
// pairs with independent writes that must not leak across the partition,
// then re-mesh, heal, and converge on every row including a post-heal
// write. The Rust replication_toml fragment becomes ReplicationYAML and
// the set_peers calls map one-to-one.
package partition

import (
	"os"
	"testing"
	"time"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func insert(t *testing.T, cluster *gosqlharness.Cluster, node int, id int64, name string) {
	t.Helper()
	if _, err := cluster.Node(node).Execute(
		"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
		t.Fatalf("node %d insert %s: %v", node, name, err)
	}
	if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node %d flush %s: %v", node, name, err)
	}
}

func TestFourNodePartitionIsolationAndHealing(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:            "partition",
		NumNodes:        4,
		ReplicationYAML: "membership: false",
	})
	if err != nil {
		t.Fatalf("start SQL cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < 4; i++ {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			t.Fatalf("node %d create records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate records: %v", i, err)
		}
	}
	insert(t, cluster, 0, 1, "baseline")
	if err := gosqlharness.AssertRows(cluster, []int{0, 1, 2, 3},
		"SELECT name FROM records ORDER BY id", [][]string{{"baseline"}}); err != nil {
		t.Fatalf("converge baseline: %v", err)
	}
	// Restart from persisted config to sever every cross-pair connection.
	for i := 0; i < 4; i++ {
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d pre-partition flush: %v", i, err)
		}
		if err := cluster.StopNode(i); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
	}
	for _, pair := range [][2]int{{0, 1}, {1, 0}, {2, 3}, {3, 2}} {
		if err := cluster.SetPeers(pair[0], []int{pair[1]}); err != nil {
			t.Fatalf("set peers node %d: %v", pair[0], err)
		}
	}
	for i := 0; i < 4; i++ {
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
	}
	insert(t, cluster, 0, 100, "side-ab")
	insert(t, cluster, 2, 200, "side-cd")
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT name FROM records ORDER BY id",
		[][]string{{"baseline"}, {"side-ab"}}); err != nil {
		t.Fatalf("converge left pair: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{2, 3},
		"SELECT name FROM records ORDER BY id",
		[][]string{{"baseline"}, {"side-cd"}}); err != nil {
		t.Fatalf("converge right pair: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	left, err := cluster.Node(0).Simple("SELECT name FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("node 0 read partition rows: %v", err)
	}
	right, err := cluster.Node(2).Simple("SELECT name FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("node 2 read partition rows: %v", err)
	}
	if equalRows(left, right) {
		t.Fatalf("partition leaked: both sides read %v", left)
	}
	for i := 0; i < 4; i++ {
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d pre-heal flush: %v", i, err)
		}
		if err := cluster.StopNode(i); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
	}
	for i := 0; i < 4; i++ {
		var peers []int
		for j := 0; j < 4; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}
		if err := cluster.SetPeers(i, peers); err != nil {
			t.Fatalf("set peers node %d: %v", i, err)
		}
	}
	for i := 0; i < 4; i++ {
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
	}
	healed := [][]string{{"baseline"}, {"side-ab"}, {"side-cd"}}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1, 2, 3},
		"SELECT name FROM records ORDER BY id", healed); err != nil {
		t.Fatalf("converge healed: %v", err)
	}
	insert(t, cluster, 1, 300, "post-heal")
	healed = append(healed, []string{"post-heal"})
	if err := gosqlharness.AssertRows(cluster, []int{0, 1, 2, 3},
		"SELECT name FROM records ORDER BY id", healed); err != nil {
		t.Fatalf("converge post-heal: %v", err)
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
