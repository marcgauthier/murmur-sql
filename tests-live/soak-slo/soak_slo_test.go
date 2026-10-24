// Live scenario: sustained concurrent writes hold latency SLOs and
// bounded resource gauges.
//
// Go port of tests/soak_slo.rs using tests-live/goharness. Identical
// behavior: three writers (one per node) insert then overwrite a rolling
// 1000-key window for MURMUR_SLO_DURATION_SECONDS (default 5 s) at
// 10 ms pacing, all nodes converge within
// MURMUR_SLO_SETTLE_SECONDS (default 30 s), p95 ack latency stays within
// 5 s with a 20 s max, and every node reports staged transactions <=
// 5000, spool disk bytes <= 512 MiB, a null storage error, and zero
// pending bytes. The Rust tokio tasks become goroutines over a
// sync.WaitGroup.
package soakslo

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// wait is the peer-count deadline (Rust: WAIT = 120 s).
const wait = 120 * time.Second

func TestSoakSLO(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("soak-slo-sql")
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if err := goharness.Fixture(cluster); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if err := goharness.PeerCounts(cluster, []int{0, 1, 2}, []int{2, 2, 2}, wait); err != nil {
		t.Fatalf("peer counts: %v", err)
	}
	end := time.Now().Add(goharness.EnvSeconds("MURMUR_SLO_DURATION_SECONDS", 5))
	type workerOut struct {
		expected map[string]string
		lat      []time.Duration
		n        int
		err      error
	}
	outs := make([]workerOut, 3)
	var wg sync.WaitGroup
	for node := 0; node < 3; node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			out := &outs[node]
			out.expected = map[string]string{}
			n := 0
			for time.Now().Before(end) {
				id := fmt.Sprintf("n%d-%04d", node, n%1000)
				name := fmt.Sprintf("revision-%d", n)
				start := time.Now()
				var err error
				if n < 1000 {
					_, err = cluster.Node(node).Execute(
						"INSERT INTO records(id,name) VALUES($1,$2)", id, name)
				} else {
					_, err = cluster.Node(node).Execute(
						"UPDATE records SET name=$1 WHERE id=$2", name, id)
				}
				if err != nil {
					out.err = err
					return
				}
				out.lat = append(out.lat, time.Since(start))
				out.expected[id] = name
				n++
				time.Sleep(10 * time.Millisecond)
			}
			if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
				out.err = err
				return
			}
			out.n = n
		}(node)
	}
	wg.Wait()
	var expected [][]string
	var latency []time.Duration
	operations := 0
	for node := 0; node < 3; node++ {
		if outs[node].err != nil {
			t.Fatalf("node %d writer: %v", node, outs[node].err)
		}
		for id, name := range outs[node].expected {
			expected = append(expected, []string{id, name})
		}
		latency = append(latency, outs[node].lat...)
		operations += outs[node].n
	}
	if operations < 60 {
		t.Fatalf("operations = %d, want >= 60", operations)
	}
	sort.Slice(expected, func(a, b int) bool {
		if expected[a][0] != expected[b][0] {
			return expected[a][0] < expected[b][0]
		}
		return expected[a][1] < expected[b][1]
	})
	if err := goharness.Converge(cluster, []int{0, 1, 2}, expected,
		goharness.EnvSeconds("MURMUR_SLO_SETTLE_SECONDS", 30)); err != nil {
		t.Fatalf("converge: %v", err)
	}
	sort.Slice(latency, func(a, b int) bool { return latency[a] < latency[b] })
	if p95 := latency[len(latency)*95/100]; p95 > 5*time.Second {
		t.Fatalf("p95 latency = %v, want <= 5s", p95)
	}
	if max := latency[len(latency)-1]; max > 20*time.Second {
		t.Fatalf("max latency = %v, want <= 20s", max)
	}
	for i := 0; i < 3; i++ {
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush: %v", i, err)
		}
		s, err := goharness.Status(cluster.Node(i))
		if err != nil {
			t.Fatalf("node %d status: %v", i, err)
		}
		staged, ok := statusNumber(s, "replication", "staged_transactions")
		if !ok || staged > 5000 {
			t.Fatalf("node %d staged_transactions = %v, want <= 5000", i, staged)
		}
		disk, ok := statusNumber(s, "spool", "disk_bytes")
		if !ok || disk > 512<<20 {
			t.Fatalf("node %d spool disk_bytes = %v, want <= 536870912", i, disk)
		}
		if storageErr := statusValue(s, "storage_error"); storageErr != nil {
			t.Fatalf("node %d storage_error = %v, want null", i, storageErr)
		}
		pending, ok := statusNumber(s, "pending_bytes")
		if !ok || pending != 0 {
			t.Fatalf("node %d pending_bytes = %v, want 0", i, pending)
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
