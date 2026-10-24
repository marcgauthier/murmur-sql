// Live scenario: seed-only SWIM discovery forms a full mesh, and the
// mesh reheals after the seed is killed.
//
// Go port of tests/swim_discovery.rs using tests-live/goharness.
// Identical behavior: four nodes start with manual peering and a single
// seed, discover the full mesh within MURMUR_SWIM_DISCOVERY_SECONDS
// (default 60 s) with at least 3 replication members each, converge on
// 40 rows, and (restart leg) the seed is killed, survivors hold
// two-peer meshes and 12 more converged rows, the seed restarts, re-adds
// one peer, and the full four-mesh reconverges.
package swimdiscovery

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// wait is the convergence deadline (Rust: WAIT = 120 s).
const wait = 120 * time.Second

func TestSeedOnlyDiscoveryFormsFullMesh(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	discovery(t, false)
}

func TestSeedKillAndRediscovery(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	discovery(t, true)
}

func discovery(t *testing.T, restart bool) {
	t.Helper()
	opts := goharness.DefaultClusterOptions(fmt.Sprintf("swim-sql-%t", restart))
	opts.NumNodes = 4
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
	timeout := goharness.EnvSeconds("MURMUR_SWIM_DISCOVERY_SECONDS", 60)
	if err := goharness.PeerCounts(cluster, []int{0, 1, 2, 3}, []int{3, 3, 3, 3}, timeout); err != nil {
		t.Fatalf("peer counts: %v", err)
	}
	for i := 0; i < 4; i++ {
		s, err := goharness.Status(cluster.Node(i))
		if err != nil {
			t.Fatalf("node %d status: %v", i, err)
		}
		members, ok := statusNumber(s, "replication", "members")
		if !ok || members < 3 {
			t.Fatalf("node %d members = %v, want >= 3", i, members)
		}
	}
	var expected [][]string
	for node := 0; node < 4; node++ {
		for n := 0; n < 10; n++ {
			id := fmt.Sprintf("%d-%02d", node, n)
			name := fmt.Sprintf("discovery-%s", id)
			if _, err := cluster.Node(node).Execute(
				"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
				t.Fatalf("insert %s: %v", id, err)
			}
			expected = append(expected, []string{id, name})
		}
		if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush: %v", node, err)
		}
	}
	sortRows(expected)
	if err := goharness.Converge(cluster, []int{0, 1, 2, 3}, expected, wait); err != nil {
		t.Fatalf("converge: %v", err)
	}
	if restart {
		cluster.KillNode(0)
		if err := goharness.PeerCounts(cluster, []int{1, 2, 3}, []int{2, 2, 2}, timeout); err != nil {
			t.Fatalf("survivor peer counts: %v", err)
		}
		for n := 0; n < 12; n++ {
			id := fmt.Sprintf("down-%02d", n)
			name := fmt.Sprintf("survivor-%d", n)
			node := 1 + n%3
			if _, err := cluster.Node(node).Execute(
				"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
				t.Fatalf("insert %s: %v", id, err)
			}
			if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
				t.Fatalf("node %d flush: %v", node, err)
			}
			expected = append(expected, []string{id, name})
		}
		sortRows(expected)
		if err := goharness.Converge(cluster, []int{1, 2, 3}, expected, wait); err != nil {
			t.Fatalf("converge survivors: %v", err)
		}
		if err := cluster.StartNode(0); err != nil {
			t.Fatalf("restart seed: %v", err)
		}
		if err := cluster.AddPeer(0, 1); err != nil {
			t.Fatalf("re-add seed peer: %v", err)
		}
		if err := goharness.PeerCounts(cluster, []int{0, 1, 2, 3}, []int{3, 3, 3, 3}, timeout); err != nil {
			t.Fatalf("rejoin peer counts: %v", err)
		}
		if err := goharness.Converge(cluster, []int{0, 1, 2, 3}, expected, wait); err != nil {
			t.Fatalf("converge after rejoin: %v", err)
		}
	}
	failed = false
}

// sortRows sorts rows lexicographically by (id, name), matching Rust's
// Vec<Vec<String>> sort.
func sortRows(rows [][]string) {
	sort.Slice(rows, func(a, b int) bool {
		if rows[a][0] != rows[b][0] {
			return rows[a][0] < rows[b][0]
		}
		return rows[a][1] < rows[b][1]
	})
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
