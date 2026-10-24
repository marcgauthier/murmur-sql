// Live scenario: local writes keep priority over replication apply work.
//
// Go port of tests/write_priority.rs using tests-live/goharness.
// Identical behavior: a 2-node cluster runs 128 concurrent SQL workers
// (64 per node) issuing atomic 25-row insert/update statements for 12 s
// while the main goroutine polls row counts, then the test requires at
// least 1 s of writer service time with remote apply work present but at
// most 25% of the total, and converges on every worker's final rows.
// The Rust tokio tasks become goroutines joined by a sync.WaitGroup.
package writepriority

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

func TestLocalWritePriority(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("write-priority-sql")
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
	var before [2]uint64
	for i := 0; i < 2; i++ {
		s, err := goharness.Status(cluster.Node(i))
		if err != nil {
			t.Fatalf("node %d status: %v", i, err)
		}
		for index, class := range []string{"local", "remote"} {
			n, ok := statusNumber(s, "scheduler", class, "service_nanos")
			if !ok {
				t.Fatalf("node %d scheduler.%s.service_nanos missing", i, class)
			}
			before[index] += uint64(n)
		}
	}
	type worker struct {
		node   int
		id     int
		client *goharness.NodeClient
	}
	var workers []worker
	for node := 0; node < 2; node++ {
		for id := 0; id < 64; id++ {
			client, err := cluster.Connect(node)
			if err != nil {
				t.Fatalf("node %d worker %d connect: %v", node, id, err)
			}
			workers = append(workers, worker{node: node, id: id, client: client})
		}
	}
	deadline := time.Now().Add(goharness.EnvSeconds("MURMUR_LIVE_WRITE_PRIORITY_SECONDS", 12))
	results := make([][][]string, len(workers))
	errs := make([]error, len(workers))
	var wg sync.WaitGroup
	for k, w := range workers {
		wg.Add(1)
		go func(k int, w worker) {
			defer wg.Done()
			// Atomic 25-row statements make engine contention measurable
			// over the wire. Reuse owned rows so state stays bounded.
			ids := make([]string, 25)
			for n := range ids {
				ids[n] = fmt.Sprintf("n%d-w%03d-%03d", w.node, w.id, n)
			}
			revision := 0
			finalName := ""
			for time.Now().Before(deadline) {
				finalName = fmt.Sprintf("value-n%d-w%d-revision-%d", w.node, w.id, revision)
				var affected int64
				var err error
				if revision == 0 {
					values := make([]string, 0, len(ids))
					for _, id := range ids {
						values = append(values, fmt.Sprintf("('%s','%s')", id, finalName))
					}
					affected, err = w.client.Execute(
						"INSERT INTO records(id,name) VALUES " + strings.Join(values, ","))
				} else {
					affected, err = w.client.Execute(
						"UPDATE records SET name=$1 WHERE id BETWEEN $2 AND $3",
						finalName, ids[0], ids[24])
				}
				if err != nil {
					errs[k] = err
					return
				}
				if affected != 25 {
					errs[k] = fmt.Errorf("affected %d rows, want 25", affected)
					return
				}
				revision++
				if revision%10 == 0 {
					if _, err := w.client.Simple("MURMUR FLUSH"); err != nil {
						errs[k] = err
						return
					}
				}
			}
			if _, err := w.client.Simple("MURMUR FLUSH"); err != nil {
				errs[k] = err
				return
			}
			if revision == 0 {
				results[k] = [][]string{}
				return
			}
			rows := make([][]string, 0, len(ids))
			for _, id := range ids {
				rows = append(rows, []string{id, finalName})
			}
			results[k] = rows
		}(k, w)
	}
	reads := 0
	for time.Now().Before(deadline) {
		for i := 0; i < 2; i++ {
			result, err := cluster.Node(i).Query("SELECT COUNT(*) FROM records")
			if err != nil {
				t.Fatalf("node %d count: %v", i, err)
			}
			if len(result) != 1 {
				t.Fatalf("node %d count returned %d rows, want 1", i, len(result))
			}
			reads++
		}
		time.Sleep(20 * time.Millisecond)
	}
	wg.Wait()
	var expected [][]string
	for k, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", k, err)
		}
		expected = append(expected, results[k]...)
	}
	if len(expected) < 100 || reads < 10 {
		t.Fatal("insufficient SQL progress")
	}
	var after [2]uint64
	for i := 0; i < 2; i++ {
		s, err := goharness.Status(cluster.Node(i))
		if err != nil {
			t.Fatalf("node %d status: %v", i, err)
		}
		for index, class := range []string{"local", "remote"} {
			n, ok := statusNumber(s, "scheduler", class, "service_nanos")
			if !ok {
				t.Fatalf("node %d scheduler.%s.service_nanos missing", i, class)
			}
			after[index] += uint64(n)
		}
	}
	local := after[0] - before[0]
	remote := after[1] - before[1]
	total := local + remote
	// Policy outcome is measured in service time, not contested-grant
	// counts: replication applies each burst under a few long-held
	// tickets, so its total work stays below its share, it wins every
	// contested grant while behind, and local dual grants stay near
	// zero even under real overlap. Total shares are the honest signal.
	max1 := total
	if max1 < 1 {
		max1 = 1
	}
	fmt.Fprintf(os.Stderr,
		"dual SQL service local=%dns remote=%dns total=%dns share=%.3f\n",
		local, remote, total, float64(remote)/float64(max1))
	if total < 1_000_000_000 {
		t.Fatalf("need >=1s writer service to measure fairness, got %dns", total)
	}
	if remote == 0 {
		t.Fatal("replication performed no apply work; contention was never exercised")
	}
	if float64(remote)/float64(total) > 0.25 {
		t.Fatalf("remote service exceeded 25%% under dual contention: local=%dns remote=%dns",
			local, remote)
	}
	sort.Slice(expected, func(a, b int) bool {
		if expected[a][0] != expected[b][0] {
			return expected[a][0] < expected[b][0]
		}
		return expected[a][1] < expected[b][1]
	})
	if err := goharness.Converge(cluster, []int{0, 1}, expected,
		goharness.EnvSeconds("MURMUR_LIVE_WRITE_PRIORITY_SYNC_TIMEOUT_SECONDS", 120)); err != nil {
		t.Fatalf("converge: %v", err)
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
