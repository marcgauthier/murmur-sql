// Live scenario: three-way heal order does not affect the final digest.
//
// Go port of tests/three_way_heal.rs using tests-live/goharness.
// Identical behavior: a 4-node cluster with gossip membership disabled
// converges on 10 base rows, splits into two islands that diverge (10 new
// rows plus conflicting renames per side), heals link by link in the
// order named by MURMUR_THREE_WAY_HEAL_ORDER(_B), and both heal orders
// must produce the identical final row set with side B winning every
// conflict.
package threewayheal

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// wait is the convergence deadline (Rust: WAIT = 120 s).
const wait = 120 * time.Second

// links are the cross-island links healed one at a time (Rust: links).
var links = [][2]int{{0, 2}, {0, 3}, {1, 2}, {1, 3}}

// heal runs one full split-diverge-heal cycle and returns the final rows
// on node 0 (Rust: heal).
func heal(t *testing.T, order []int, tag string) [][]string {
	t.Helper()
	membership := false
	opts := goharness.DefaultClusterOptions("three-way-sql-" + tag)
	opts.NumNodes = 4
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
	var base [][]string
	for n := 0; n < 10; n++ {
		id := fmt.Sprintf("base-%03d", n)
		name := fmt.Sprintf("baseline-%d", n)
		if _, err := cluster.Node(0).Execute(
			"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
		base = append(base, []string{id, name})
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush base rows: %v", err)
	}
	if err := goharness.Converge(cluster, []int{0, 1, 2, 3}, base, wait); err != nil {
		t.Fatalf("converge base rows: %v", err)
	}
	for _, link := range links {
		a, b := link[0], link[1]
		if err := cluster.RemovePeer(a, b); err != nil {
			t.Fatalf("remove peer %d->%d: %v", a, b, err)
		}
		if err := cluster.RemovePeer(b, a); err != nil {
			t.Fatalf("remove peer %d->%d: %v", b, a, err)
		}
	}
	if err := goharness.PeerCounts(cluster, []int{0, 1, 2, 3}, []int{1, 1, 1, 1}, wait); err != nil {
		t.Fatalf("peer counts after split: %v", err)
	}
	var expected [][]string
	for _, side := range []struct {
		node int
		tag  string
	}{{1, "A"}, {3, "B"}} {
		for n := 0; n < 10; n++ {
			id := fmt.Sprintf("side%s-%03d", side.tag, n)
			name := fmt.Sprintf("side-%s-%d", side.tag, n)
			if _, err := cluster.Node(side.node).Execute(
				"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
				t.Fatalf("insert %s: %v", id, err)
			}
			expected = append(expected, []string{id, name})
		}
		for n := 0; n < 10; n++ {
			id := fmt.Sprintf("base-%03d", n)
			name := fmt.Sprintf("conflict-%s-%d", side.tag, n)
			if _, err := cluster.Node(side.node).Execute(
				"UPDATE records SET name=$1 WHERE id=$2", name, id); err != nil {
				t.Fatalf("conflict %s on node %d: %v", id, side.node, err)
			}
		}
		if _, err := cluster.Node(side.node).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush diverged rows: %v", side.node, err)
		}
		// Capture times are ordered; side B must be the same conflict winner in both runs.
		time.Sleep(20 * time.Millisecond)
	}
	if !goharness.WaitUntil(wait, 50*time.Millisecond, func() bool {
		a, err := goharness.Rows(cluster.Node(0))
		if err != nil {
			return false
		}
		b, err := goharness.Rows(cluster.Node(2))
		if err != nil {
			return false
		}
		return len(a) == 20 && len(b) == 20 && !equalRows(a, b)
	}) {
		t.Fatal("islands did not diverge on 20 differing rows each")
	}
	for _, index := range order {
		a, b := links[index][0], links[index][1]
		if err := cluster.AddPeer(a, b); err != nil {
			t.Fatalf("add peer %d->%d: %v", a, b, err)
		}
		if err := cluster.AddPeer(b, a); err != nil {
			t.Fatalf("add peer %d->%d: %v", b, a, err)
		}
		nodeBID := cluster.NodeID(b)
		nodeB := goharness.Hex(nodeBID[:])
		if !goharness.WaitUntil(wait, 50*time.Millisecond, func() bool {
			peers, err := goharness.Peers(cluster.Node(a))
			if err != nil {
				return false
			}
			connected := false
			for _, p := range peers {
				if len(p) > 2 && p[0] == nodeB && p[2] == "true" {
					connected = true
					break
				}
			}
			if !connected {
				return false
			}
			rows, err := goharness.Rows(cluster.Node(a))
			if err != nil {
				return false
			}
			return len(rows) == 30
		}) {
			t.Fatalf("link %d->%d did not heal to 30 rows", a, b)
		}
	}
	for n := 0; n < 10; n++ {
		expected = append(expected, []string{fmt.Sprintf("base-%03d", n), fmt.Sprintf("conflict-B-%d", n)})
	}
	sort.Slice(expected, func(a, b int) bool {
		if expected[a][0] != expected[b][0] {
			return expected[a][0] < expected[b][0]
		}
		return expected[a][1] < expected[b][1]
	})
	if err := goharness.Converge(cluster, []int{0, 1, 2, 3}, expected, wait); err != nil {
		t.Fatalf("converge after heal: %v", err)
	}
	finalRows, err := goharness.Rows(cluster.Node(0))
	if err != nil {
		t.Fatalf("read final rows: %v", err)
	}
	failed = false
	return finalRows
}

// order parses a comma-separated heal-link order env knob (Rust: order).
func order(t *testing.T, variable, fallback string) []int {
	t.Helper()
	raw := os.Getenv(variable)
	if raw == "" {
		raw = fallback
	}
	var values []int
	for _, s := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			t.Fatalf("heal link index %q: %v", s, err)
		}
		values = append(values, n)
	}
	sorted := append([]int(nil), values...)
	sort.Ints(sorted)
	if !equalInts(sorted, []int{0, 1, 2, 3}) {
		t.Fatalf("heal order %v is not a permutation of 0,1,2,3", values)
	}
	return values
}

func TestHealOrderDoesNotAffectFinalDigest(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	a := order(t, "MURMUR_THREE_WAY_HEAL_ORDER", "0,1,2,3")
	b := order(t, "MURMUR_THREE_WAY_HEAL_ORDER_B", "3,2,1,0")
	first := heal(t, a, "a")
	second := heal(t, b, "b")
	if !equalRows(first, second) {
		t.Fatalf("heal orders diverged:\n%v\n%v", first, second)
	}
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

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
