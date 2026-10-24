// Live scenario: local writers keep their share of scheduler
// acquisitions while a remote node replicates under load.
//
// Go port of tests/loadshare.rs using tests-live/goharness. Identical
// behavior: four writers on node 0 insert 60 rows each concurrently while
// node 1 inserts 20 rows with 25 ms pacing; both nodes converge on all
// 260 rows, the slowest local write stays within 10 s, and the node 0
// scheduler snapshot shows local plus remote acquisitions with bounded
// debt. The Rust tokio::spawn workers become goroutines over a
// sync.WaitGroup.
package loadshare

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

func TestWriterSharesUnderLoad(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("loadshare-sql")
	opts.NumNodes = 2
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if err := goharness.Fixture(cluster); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	type workerOut struct {
		acked   [][]string
		latency []time.Duration
		err     error
	}
	outs := make([]workerOut, 4)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		client, err := cluster.Connect(0)
		if err != nil {
			t.Fatalf("worker %d connect: %v", worker, err)
		}
		wg.Add(1)
		go func(worker int, client *goharness.NodeClient) {
			defer wg.Done()
			defer client.Close()
			var acked [][]string
			var latency []time.Duration
			for n := 0; n < 60; n++ {
				id := fmt.Sprintf("local-%d-%04d", worker, n)
				name := fmt.Sprintf("value-%s", id)
				start := time.Now()
				if _, err := client.Execute(
					"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
					outs[worker].err = err
					return
				}
				latency = append(latency, time.Since(start))
				acked = append(acked, []string{id, name})
			}
			if _, err := client.Simple("MURMUR FLUSH"); err != nil {
				outs[worker].err = err
				return
			}
			outs[worker].acked = acked
			outs[worker].latency = latency
		}(worker, client)
	}
	var expected [][]string
	for n := 0; n < 20; n++ {
		id := fmt.Sprintf("remote-%04d", n)
		name := fmt.Sprintf("value-%s", id)
		if _, err := cluster.Node(1).Execute(
			"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
		if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("flush %s: %v", id, err)
		}
		expected = append(expected, []string{id, name})
		time.Sleep(25 * time.Millisecond)
	}
	var latencies []time.Duration
	wg.Wait()
	for worker, out := range outs {
		if out.err != nil {
			t.Fatalf("worker %d: %v", worker, out.err)
		}
		expected = append(expected, out.acked...)
		latencies = append(latencies, out.latency...)
	}
	sort.Slice(expected, func(a, b int) bool {
		if expected[a][0] != expected[b][0] {
			return expected[a][0] < expected[b][0]
		}
		return expected[a][1] < expected[b][1]
	})
	if err := goharness.Converge(cluster, []int{0, 1}, expected, wait); err != nil {
		t.Fatalf("converge: %v", err)
	}
	if len(latencies) != 240 {
		t.Fatalf("latencies = %d, want 240", len(latencies))
	}
	sort.Slice(latencies, func(a, b int) bool { return latencies[a] < latencies[b] })
	if latencies[239] > 10*time.Second {
		t.Fatalf("slowest write = %v, want <= 10s", latencies[239])
	}
	s, err := goharness.Status(cluster.Node(0))
	if err != nil {
		t.Fatalf("node 0 status: %v", err)
	}
	local, ok := statusNumber(s, "scheduler", "local", "acquisitions")
	if !ok || local < 240 {
		t.Fatalf("scheduler.local.acquisitions = %v, want >= 240", local)
	}
	remote, ok := statusNumber(s, "scheduler", "remote", "acquisitions")
	if !ok || remote <= 0 {
		t.Fatalf("scheduler.remote.acquisitions = %v, want > 0", remote)
	}
	debt, ok := statusNumber(s, "scheduler", "debt_nanos")
	if !ok || debt > 1_000_000_000 {
		t.Fatalf("scheduler.debt_nanos = %v, want <= 1e9", debt)
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
