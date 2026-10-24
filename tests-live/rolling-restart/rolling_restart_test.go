// Live scenario: rolling real-process restarts lose no writes.
//
// Go port of tests/rolling_restart.rs using tests-live/goharness.
// Identical behavior: a 3-node cluster converges on 5 base rows, then
// each round restarts one node (round-robin) while the two live peers
// each acknowledge 25 rows concurrently with 25 ms pacing, the
// restarted node rejoins mid-write, and all three nodes converge on
// every acknowledged row. The Rust tokio tasks become goroutines over a
// sync.WaitGroup.
package rollingrestart

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

func TestRollingRestartLosesNoWrites(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	roll(t, 3)
}

func TestRollingRestartChurn(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	roll(t, 6)
}

func roll(t *testing.T, rounds int) {
	t.Helper()
	opts := goharness.DefaultClusterOptions(fmt.Sprintf("rolling-sql-%d", rounds))
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
	for n := 0; n < 5; n++ {
		id := fmt.Sprintf("base-%d", n)
		name := fmt.Sprintf("baseline-%d", n)
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
	for round := 0; round < rounds; round++ {
		restarting := round % 3
		// Persist acknowledged state, then write concurrently on both
		// live peers throughout this real process restart. Every
		// successful write is checked.
		for i := 0; i < 3; i++ {
			if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
				t.Fatalf("round %d node %d pre-restart flush: %v", round, i, err)
			}
		}
		cluster.StopNode(restarting)
		var wg sync.WaitGroup
		type writerOut struct {
			acked [][]string
			err   error
		}
		outs := make([]writerOut, 3)
		for node := 0; node < 3; node++ {
			if node == restarting {
				continue
			}
			wg.Add(1)
			go func(node int) {
				defer wg.Done()
				var acked [][]string
				for n := 0; n < 25; n++ {
					id := fmt.Sprintf("r%d-n%d-%03d", round, node, n)
					name := fmt.Sprintf("live-%s", id)
					if _, err := cluster.Node(node).Execute(
						"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
						outs[node].err = err
						return
					}
					acked = append(acked, []string{id, name})
					time.Sleep(25 * time.Millisecond)
				}
				if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
					outs[node].err = err
					return
				}
				outs[node].acked = acked
			}(node)
		}
		time.Sleep(100 * time.Millisecond)
		if err := cluster.StartNode(restarting); err != nil {
			t.Fatalf("round %d restart node %d: %v", round, restarting, err)
		}
		if err := cluster.Mesh(); err != nil {
			t.Fatalf("round %d remesh: %v", round, err)
		}
		wg.Wait()
		for node := 0; node < 3; node++ {
			if node == restarting {
				continue
			}
			if outs[node].err != nil {
				t.Fatalf("round %d node %d writer: %v", round, node, outs[node].err)
			}
			expected = append(expected, outs[node].acked...)
		}
		sortRows(expected)
		if err := goharness.Converge(cluster, []int{0, 1, 2}, expected, wait); err != nil {
			t.Fatalf("round %d converge: %v", round, err)
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
