// Live scenario: SQL projections reflect replicated updates and survive a
// real server restart.
//
// Go port of tests/views.rs using tests-live/gosqlharness. Identical
// behavior: three real murmur-server children share a replicated contacts
// table, each node inserts one row, two rows are promoted into a score>0
// projection, every node converges on the projection, and node 2 rebuilds
// it after a restart.
package views

import (
	"os"
	"testing"
	"time"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

// projection is the tracked SQL projection (Rust: PROJECTION).
const projection = "SELECT name FROM contacts WHERE score>0 ORDER BY name"

// enabled waits until every listed node reads ann and bob from the
// projection (Rust: enabled; 30 s deadline, 50 ms polls).
func enabled(t *testing.T, cluster *gosqlharness.Cluster, nodes []int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	want := [][]string{{"ann"}, {"bob"}}
	for {
		matches := true
		for _, i := range nodes {
			rows, err := cluster.Node(i).Simple(projection)
			if err != nil {
				t.Fatalf("node %d projection: %v", i, err)
			}
			matches = matches && equalRows(rows, want)
		}
		if matches {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatal("projection did not converge on ann and bob")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestSQLProjectionTracksReplicatedRowsAndRebuilds(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "views",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < 3; i++ {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE IF NOT EXISTS contacts(name TEXT PRIMARY KEY NOT NULL, score INTEGER NOT NULL)"); err != nil {
			t.Fatalf("node %d create contacts: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE contacts REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate contacts: %v", i, err)
		}
	}
	for i, name := range []string{"ann", "bob", "hidden"} {
		if _, err := cluster.Node(i).Execute(
			"INSERT INTO contacts(name,score) VALUES($1,0)", name); err != nil {
			t.Fatalf("node %d insert %s: %v", i, name, err)
		}
		if i < 2 {
			n, err := cluster.Node(i).Execute(
				"UPDATE contacts SET score=score+1 WHERE name=$1", name)
			if err != nil {
				t.Fatalf("node %d promote %s: %v", i, name, err)
			}
			if n != 1 {
				t.Fatalf("node %d promote %s affected %d rows, want 1", i, name, n)
			}
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush: %v", i, err)
		}
	}
	enabled(t, cluster, []int{0, 1, 2})
	if _, err := cluster.Node(2).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node 2 pre-restart flush: %v", err)
	}
	if err := cluster.StopNode(2); err != nil {
		t.Fatalf("stop node 2: %v", err)
	}
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("restart node 2: %v", err)
	}
	enabled(t, cluster, []int{2})
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
