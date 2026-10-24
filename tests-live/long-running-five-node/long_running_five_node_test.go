// Live scenario: five nodes keep writing through two rolling restarts
// without losing acknowledged rows.
//
// Go port of tests/long_running_five_node.rs using tests-live/goharness.
// Identical behavior: five writers insert paced rows for
// MURMUR_FIVE_NODE_DURATION_SECONDS (default 30 s); node 1 restarts at
// one third and node 3 at two thirds (acknowledged rows must survive);
// all five converge, then flush with zero pending bytes, bounded spool,
// and no storage error; one post-soak write converges everywhere. The
// Rust Arc<RwLock<Option<Client>>> slots become RWMutex-guarded client
// slots, and tokio::spawn becomes goroutines over a sync.WaitGroup.
package longrunningfivenode

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// wait is the final convergence deadline (Rust: WAIT = 120 s).
const wait = 120 * time.Second

// slot holds one writer's client across restarts (Rust:
// Arc<RwLock<Option<Client>>>).
type slot struct {
	mu     sync.RWMutex
	client *goharness.NodeClient
}

func TestLongRunningFiveNode(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("five-node-sql")
	opts.NumNodes = 5
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if err := goharness.Fixture(cluster); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	duration := goharness.EnvSeconds("MURMUR_FIVE_NODE_DURATION_SECONDS", 30)
	if duration < 3*time.Second {
		t.Fatalf("duration = %v, want >= 3s", duration)
	}
	interval := 50 * time.Millisecond
	if raw := os.Getenv("MURMUR_FIVE_NODE_INTERVAL_MS"); raw != "" {
		ms, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("MURMUR_FIVE_NODE_INTERVAL_MS = %q: %v", raw, err)
		}
		interval = time.Duration(ms) * time.Millisecond
	}
	started := time.Now()
	end := started.Add(duration)
	slots := make([]*slot, 5)
	for i := range slots {
		slots[i] = &slot{client: cluster.Node(i)}
	}
	type workerOut struct {
		acked [][]string
		err   error
	}
	outs := make([]workerOut, 5)
	var wg sync.WaitGroup
	for node := 0; node < 5; node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			var acked [][]string
			n := 0
			for time.Now().Before(end) {
				time.Sleep(interval)
				slots[node].mu.RLock()
				client := slots[node].client
				slots[node].mu.RUnlock()
				if client != nil {
					id := fmt.Sprintf("n%d-%09d", node, n)
					name := fmt.Sprintf("capacity-%s", id)
					if _, err := client.Execute(
						"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
						outs[node].err = err
						return
					}
					if _, err := client.Simple("MURMUR FLUSH"); err != nil {
						outs[node].err = err
						return
					}
					acked = append(acked, []string{id, name})
					n++
				}
			}
			if n < 10 {
				outs[node].err = fmt.Errorf("node %d made insufficient progress", node)
				return
			}
			outs[node].acked = acked
		}(node)
	}
	for _, round := range []struct {
		round uint32
		node  int
	}{{1, 1}, {2, 3}} {
		time.Sleep(time.Until(started.Add(duration * time.Duration(round.round) / 3)))
		slots[round.node].mu.Lock()
		slots[round.node].client = nil
		before, err := goharness.Rows(cluster.Node(round.node))
		if err != nil {
			slots[round.node].mu.Unlock()
			t.Fatalf("node %d pre-restart rows: %v", round.node, err)
		}
		cluster.StopNode(round.node)
		if err := cluster.StartNode(round.node); err != nil {
			slots[round.node].mu.Unlock()
			t.Fatalf("node %d restart: %v", round.node, err)
		}
		if err := cluster.Mesh(); err != nil {
			slots[round.node].mu.Unlock()
			t.Fatalf("remesh: %v", err)
		}
		recovered, err := goharness.Rows(cluster.Node(round.node))
		if err != nil {
			slots[round.node].mu.Unlock()
			t.Fatalf("node %d post-restart rows: %v", round.node, err)
		}
		for _, row := range before {
			if !containsRow(recovered, row) {
				slots[round.node].mu.Unlock()
				t.Fatal("acknowledged row lost on restart")
			}
		}
		slots[round.node].client = cluster.Node(round.node)
		slots[round.node].mu.Unlock()
	}
	var expected [][]string
	wg.Wait()
	for node, out := range outs {
		if out.err != nil {
			t.Fatalf("node %d writer: %v", node, out.err)
		}
		expected = append(expected, out.acked...)
	}
	if len(expected) < 100 {
		t.Fatalf("expected %d rows, want >= 100", len(expected))
	}
	sortRows(expected)
	if err := goharness.Converge(cluster, []int{0, 1, 2, 3, 4}, expected,
		goharness.EnvSeconds("MURMUR_FIVE_NODE_SETTLE_SECONDS", 120)); err != nil {
		t.Fatalf("converge: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush: %v", i, err)
		}
		s, err := goharness.Status(cluster.Node(i))
		if err != nil {
			t.Fatalf("node %d status: %v", i, err)
		}
		pending, ok := statusNumber(s, "pending_bytes")
		if !ok || pending != 0 {
			t.Fatalf("node %d pending_bytes = %v, want 0", i, pending)
		}
		disk, ok := statusNumber(s, "spool", "disk_bytes")
		if !ok || disk > 2<<30 {
			t.Fatalf("node %d spool.disk_bytes = %v, want <= 2GiB", i, disk)
		}
		if v := statusValue(s, "storage_error"); v != nil {
			t.Fatalf("node %d storage_error = %v, want null", i, v)
		}
	}
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO records(id,name) VALUES('post-soak','ready')"); err != nil {
		t.Fatalf("insert post-soak: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush post-soak: %v", err)
	}
	expected = append(expected, []string{"post-soak", "ready"})
	sortRows(expected)
	if err := goharness.Converge(cluster, []int{0, 1, 2, 3, 4}, expected, wait); err != nil {
		t.Fatalf("converge post-soak: %v", err)
	}
	failed = false
}

func sortRows(rows [][]string) {
	sort.Slice(rows, func(a, b int) bool {
		if rows[a][0] != rows[b][0] {
			return rows[a][0] < rows[b][0]
		}
		return rows[a][1] < rows[b][1]
	})
}

func containsRow(rows [][]string, want []string) bool {
	for _, r := range rows {
		if len(r) == len(want) {
			eq := true
			for i := range r {
				if r[i] != want[i] {
					eq = false
					break
				}
			}
			if eq {
				return true
			}
		}
	}
	return false
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
