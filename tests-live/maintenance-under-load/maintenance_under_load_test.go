// Live scenario: retention keeps running and collecting while real SQL
// clients sustain inserts and updates.
//
// Go port of tests/maintenance_under_load.rs using tests-live/goharness.
// Identical behavior: three nodes with 10 ms retention converge on the
// seed rows; three writers sustain inserts (then updates past the cap)
// until every node reports a retention run that pruned past the
// baseline; writers join, all rows converge, and a post-maintenance
// write converges everywhere. The Rust tokio::spawn workers become
// goroutines over a sync.WaitGroup with an atomic stop flag.
package maintenanceunderload

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// wait is the convergence deadline (Rust: WAIT = 120 s).
const wait = 120 * time.Second

func TestMaintenanceUnderSustainedLoad(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	retention := 10 * time.Millisecond
	interval := 100 * time.Millisecond
	var minimum uint64 = 1
	opts := goharness.DefaultClusterOptions("maintenance-sql")
	opts.NumNodes = 3
	opts.Replication = &goharness.ReplicationOptions{
		Retention:         &retention,
		RetentionInterval: &interval,
		MinimumRetained:   &minimum,
	}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < 3; i++ {
		if _, err := cluster.Node(i).Simple("CREATE TABLE IF NOT EXISTS contention(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL,phone TEXT NOT NULL,score INTEGER NOT NULL)"); err != nil {
			t.Fatalf("node %d create contention: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE contention REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate contention: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush schema: %v", i, err)
		}
	}
	seed := 120
	if raw := os.Getenv("MURMUR_MAINT_LOAD_SEED"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("MURMUR_MAINT_LOAD_SEED = %q: %v", raw, err)
		}
		seed = n
	}
	cap_ := 4000
	if raw := os.Getenv("MURMUR_MAINT_LOAD_MAX_INSERTS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("MURMUR_MAINT_LOAD_MAX_INSERTS = %q: %v", raw, err)
		}
		cap_ = n
	}
	if cap_ <= 0 {
		t.Fatalf("cap = %d, want > 0", cap_)
	}
	var expected [][]string
	for n := 0; n < seed; n++ {
		id := fmt.Sprintf("seed-%08d", n)
		if _, err := cluster.Node(0).Execute(
			"INSERT INTO contention(id,name,phone,score) VALUES($1,'seed','unchanged',0)", id); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
		expected = append(expected, []string{id, "seed", "unchanged", "0"})
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush seed rows: %v", err)
	}
	if !goharness.WaitUntil(wait, 50*time.Millisecond, func() bool {
		return rowsMatch(t, cluster, expected)
	}) {
		t.Fatal("seed rows did not converge")
	}
	type baseline struct{ runs, pruned float64 }
	before := make([]baseline, 3)
	for i := 0; i < 3; i++ {
		snapshot, err := goharness.Status(cluster.Node(i))
		if err != nil {
			t.Fatalf("node %d status: %v", i, err)
		}
		runs, ok := statusNumber(snapshot, "replication", "metrics", "retention_runs")
		if !ok {
			t.Fatalf("node %d has no retention_runs", i)
		}
		pruned, ok := statusNumber(snapshot, "replication", "metrics", "retention_pruned")
		if !ok {
			t.Fatalf("node %d has no retention_pruned", i)
		}
		before[i] = baseline{runs, pruned}
	}
	var stop atomic.Bool
	type finalRows struct {
		rows map[string]string
		err  error
	}
	finals := make([]finalRows, 3)
	var wg sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			client := cluster.Node(worker)
			rows := map[string]string{}
			n := 0
			deadline := time.Now().Add(900 * time.Second)
			for !stop.Load() && time.Now().Before(deadline) {
				id := fmt.Sprintf("worker-%d-%08d", worker, n%cap_)
				name := fmt.Sprintf("revision-%d", n)
				var err error
				if n < cap_ {
					_, err = client.Execute("INSERT INTO contention(id,name,phone,score) VALUES($1,$2,'unchanged',0)", id, name)
				} else {
					_, err = client.Execute("UPDATE contention SET name=$1 WHERE id=$2", name, id)
				}
				if err != nil {
					finals[worker].err = err
					return
				}
				rows[id] = name
				n++
				if n%10 == 0 {
					if _, err := client.Simple("MURMUR FLUSH"); err != nil {
						finals[worker].err = err
						return
					}
				}
			}
			if _, err := client.Simple("MURMUR FLUSH"); err != nil {
				finals[worker].err = err
				return
			}
			finals[worker].rows = rows
		}(worker)
	}
	timeout := parseDurationEnv("MURMUR_MAINT_LOAD_GC_TIMEOUT", "150s", 150)
	maintained := goharness.WaitUntil(timeout, 200*time.Millisecond, func() bool {
		for i := 0; i < 3; i++ {
			s, err := goharness.Status(cluster.Node(i))
			if err != nil {
				return false
			}
			runs, ok := statusNumber(s, "replication", "metrics", "retention_runs")
			if !ok || runs <= before[i].runs {
				return false
			}
			pruned, ok := statusNumber(s, "replication", "metrics", "retention_pruned")
			if !ok || pruned <= before[i].pruned {
				return false
			}
		}
		return true
	})
	stop.Store(true)
	wg.Wait()
	for worker, f := range finals {
		if f.err != nil {
			t.Fatalf("worker %d: %v", worker, f.err)
		}
		for id, name := range f.rows {
			expected = append(expected, []string{id, name, "unchanged", "0"})
		}
	}
	if !maintained {
		t.Fatal("retention did not run and collect under sustained SQL writes")
	}
	sortRows(expected)
	if !goharness.WaitUntil(wait, 100*time.Millisecond, func() bool {
		return rowsMatch(t, cluster, expected)
	}) {
		t.Fatal("worker rows did not converge")
	}
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO contention(id,name,phone,score) VALUES('post-maintenance','ready','unchanged',0)"); err != nil {
		t.Fatalf("insert post-maintenance: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush post-maintenance: %v", err)
	}
	expected = append(expected, []string{"post-maintenance", "ready", "unchanged", "0"})
	sortRows(expected)
	if !goharness.WaitUntil(wait, 100*time.Millisecond, func() bool {
		return rowsMatch(t, cluster, expected)
	}) {
		t.Fatal("post-maintenance row did not converge")
	}
	failed = false
}

// rowsMatch reports whether all three nodes read expected contention rows.
// A query error fails the test: unlike the polling helpers the Rust
// closure unwraps every query.
func rowsMatch(t *testing.T, cluster *goharness.Cluster, expected [][]string) bool {
	t.Helper()
	for i := 0; i < 3; i++ {
		rows, err := cluster.Node(i).Simple("SELECT id,name,phone,score FROM contention ORDER BY id")
		if err != nil {
			t.Fatalf("node %d read contention: %v", i, err)
		}
		if !equalRows(rows, expected) {
			return false
		}
	}
	return true
}

// parseDurationEnv parses the Rust timeout knob form: a trailing m/h
// multiplies minutes/hours, anything else is seconds (a trailing s is
// stripped); unparseable or non-positive values fall back.
func parseDurationEnv(name, def string, fallback uint64) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		raw = def
	}
	raw = strings.TrimSpace(raw)
	multiplier := uint64(1)
	number := raw
	if n, ok := strings.CutSuffix(raw, "m"); ok {
		number, multiplier = n, 60
	} else if n, ok := strings.CutSuffix(raw, "h"); ok {
		number, multiplier = n, 3600
	} else {
		number = strings.TrimRight(raw, "s")
	}
	n, err := strconv.ParseUint(number, 10, 64)
	if err != nil || n == 0 {
		n = fallback
	}
	return time.Duration(n*multiplier) * time.Second
}

func sortRows(rows [][]string) {
	sort.Slice(rows, func(a, b int) bool {
		for k := 0; k < len(rows[a]) && k < len(rows[b]); k++ {
			if rows[a][k] != rows[b][k] {
				return rows[a][k] < rows[b][k]
			}
		}
		return len(rows[a]) < len(rows[b])
	})
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
