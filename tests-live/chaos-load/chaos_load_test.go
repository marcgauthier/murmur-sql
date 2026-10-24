// Live scenario: continuous writes across a partition and healing.
//
// Go port of tests/chaos_load.rs using tests-live/goharness. Identical
// behavior: node 2 is partitioned from nodes 0-1, three concurrent
// writers insert 80 rows each, the test waits for divergent
// acknowledged state, heals the mesh, and converges on all 240 rows
// plus a post-heal write. The Rust spawned writer tasks become a
// sync.WaitGroup over three goroutines.
package chaosload

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// wait is the convergence deadline (Rust: WAIT = 120 s).
const wait = 120 * time.Second

func TestContinuousWritesAcrossPartitionAndHealing(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	membership := false
	opts := goharness.DefaultClusterOptions("chaos-load-sql")
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
	for _, other := range []int{0, 1} {
		if err := cluster.RemovePeer(other, 2); err != nil {
			t.Fatalf("remove peer %d->2: %v", other, err)
		}
		if err := cluster.RemovePeer(2, other); err != nil {
			t.Fatalf("remove peer 2->%d: %v", other, err)
		}
	}
	if err := goharness.PeerCounts(cluster, []int{0, 1, 2}, []int{1, 1, 0}, wait); err != nil {
		t.Fatalf("peer counts: %v", err)
	}
	var wg sync.WaitGroup
	acked := make([][][]string, 3)
	writerErrs := make([]error, 3)
	for node := 0; node < 3; node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			client := cluster.Node(node)
			var got [][]string
			for n := 0; n < 80; n++ {
				id := fmt.Sprintf("n%d-%04d", node, n)
				name := fmt.Sprintf("continuous-%s", id)
				if _, err := client.Execute(
					"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
					writerErrs[node] = err
					return
				}
				if n%10 == 0 {
					if _, err := client.Simple("MURMUR FLUSH"); err != nil {
						writerErrs[node] = err
						return
					}
				}
				got = append(got, []string{id, name})
				time.Sleep(25 * time.Millisecond)
			}
			if _, err := client.Simple("MURMUR FLUSH"); err != nil {
				writerErrs[node] = err
				return
			}
			acked[node] = got
		}(node)
	}
	// Wait for divergent acknowledged SQL state before healing, while
	// writers run.
	diverged := goharness.WaitUntil(wait, 25*time.Millisecond, func() bool {
		a, err := goharness.Rows(cluster.Node(0))
		if err != nil {
			return false
		}
		b, err := goharness.Rows(cluster.Node(2))
		if err != nil {
			return false
		}
		return len(a) >= 10 && len(b) >= 5 && !equalRows(a, b)
	})
	if !diverged {
		t.Fatal("partition failed to diverge acknowledged SQL state")
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("remesh: %v", err)
	}
	var expected [][]string
	wg.Wait()
	for node, err := range writerErrs {
		if err != nil {
			t.Fatalf("node %d writer: %v", node, err)
		}
		expected = append(expected, acked[node]...)
	}
	sortRows(expected)
	if err := goharness.Converge(cluster, []int{0, 1, 2}, expected, wait); err != nil {
		t.Fatalf("converge: %v", err)
	}
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO records(id,name) VALUES('post-heal','still-writable')"); err != nil {
		t.Fatalf("insert post-heal: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush post-heal: %v", err)
	}
	expected = append(expected, []string{"post-heal", "still-writable"})
	sortRows(expected)
	if err := goharness.Converge(cluster, []int{0, 1, 2}, expected, wait); err != nil {
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

// sortRows sorts row sets lexicographically (Rust: Vec<Vec<String>> sort).
func sortRows(rows [][]string) {
	sort.Slice(rows, func(a, b int) bool {
		for i := 0; i < len(rows[a]) && i < len(rows[b]); i++ {
			if rows[a][i] != rows[b][i] {
				return rows[a][i] < rows[b][i]
			}
		}
		return len(rows[a]) < len(rows[b])
	})
}
