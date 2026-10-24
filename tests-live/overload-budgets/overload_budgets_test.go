// Live scenario: an oversize write is rejected cleanly with no partial
// row and no writer poisoning, then a concurrent blast fully converges
// with bounded tail latency.
//
// Go port of tests/overload_budgets.rs using tests-live/goharness.
// Identical behavior: two nodes run with 64 KiB commit budgets, a value
// past the budget is rejected naming the budget, a small write converges
// after retrying replayable 40001 pressure, and 8 writers x 50 inserts
// converge with p99 under 10 s while both nodes stay ready. The Rust
// tokio::join! over the eight writers becomes a sync.WaitGroup over eight
// goroutines.
package overloadbudgets

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Commit budget on every node (Go: 64 KiB transactions; the Go 4 KiB
// per-value budget has no Rust product equivalent).
const txBudget = 64 << 10

// Admission queue: comfortably above the commit budget so the oversize leg
// trips the commit budget deterministically (and validation requires
// `transaction_bytes <= queue_bytes`).
const queueBytes = 8 << 20

// Capture worker budget (`queue_bytes >= capture_bytes` per validation).
const captureBytes = 1 << 20

// Post-rejection convergence deadline (Go: 30 s).
const probeTimeout = 30 * time.Second

// Blast convergence deadline (Go: 60 s).
const blastTimeout = 60 * time.Second

// Convergence poll interval (Go: 200 ms).
const convergePoll = 200 * time.Millisecond

// Final readiness deadline per node (Go: `WaitNodeReady`).
const readyTimeout = 30 * time.Second

// Readiness poll interval.
const readyPoll = 100 * time.Millisecond

// Blast tail-latency bound (Go: p99 under 10 s).
const p99Bound = 10 * time.Second

// Blast shape (Go: 8 writers x 50 inserts).
const (
	writers   = 8
	perWriter = 50
)

// rows returns all rows on one node as text cells (Go:
// `Cluster.TypedContentionRows`).
func rows(cluster *goharness.Cluster, idx int) ([][]string, error) {
	out, err := cluster.Node(idx).Simple("SELECT id, name, phone, score FROM records")
	if err != nil {
		return nil, fmt.Errorf("node%d SELECT rows: %w", idx+1, err)
	}
	return out, nil
}

func rowsLess(a, b []string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

// digest renders the canonical digest of a row set: sorted rows (Go
// hashes ordered `ID:Name:Phone:Score` lines; same criterion, cheaper to
// express).
func digest(rows [][]string) string {
	sorted := append([][]string(nil), rows...)
	sort.Slice(sorted, func(a, b int) bool { return rowsLess(sorted[a], sorted[b]) })
	return fmt.Sprintf("%q", sorted)
}

// rowCount returns the row count on one node (cheap pre-check before full
// digest reads).
func rowCount(cluster *goharness.Cluster, idx int) (int, error) {
	rows, err := cluster.Node(idx).Simple("SELECT COUNT(*) FROM records")
	if err != nil {
		return 0, fmt.Errorf("node%d COUNT: %w", idx+1, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return 0, fmt.Errorf("node%d COUNT returned no rows", idx+1)
	}
	n, err := strconv.Atoi(rows[0][0])
	if err != nil {
		return 0, fmt.Errorf("node%d COUNT %q: %w", idx+1, rows[0][0], err)
	}
	return n, nil
}

// waitConverged waits until every node holds exactly `want` rows with
// equal digests (Go: `waitConverged`).
func waitConverged(cluster *goharness.Cluster, want int, timeout time.Duration, what string) error {
	deadline := time.Now().Add(timeout)
	for {
		counts := make([]int, 0, cluster.Len())
		for i := 0; i < cluster.Len(); i++ {
			n, err := rowCount(cluster, i)
			if err != nil {
				return err
			}
			counts = append(counts, n)
		}
		all := true
		for _, n := range counts {
			if n != want {
				all = false
				break
			}
		}
		if all {
			first := ""
			equal := true
			for idx := 0; idx < cluster.Len(); idx++ {
				got, err := rows(cluster, idx)
				if err != nil {
					return err
				}
				d := digest(got)
				if idx == 0 {
					first = d
				} else if d != first {
					equal = false
					break
				}
			}
			if equal {
				return nil
			}
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("nodes did not converge on %d rows within %v (%s)", want, timeout, what)
		}
		time.Sleep(convergePoll)
	}
}

// waitNodeReady waits until one node is responsive with its full peer set
// (Go: `Cluster.WaitNodeReady` over the admin API).
func waitNodeReady(cluster *goharness.Cluster, idx int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		repl, err := cluster.Node(idx).Simple("MURMUR SHOW REPLICATION")
		if err != nil {
			return fmt.Errorf("node%d SHOW REPLICATION: %w", idx+1, err)
		}
		peers := ""
		if len(repl) > 0 && len(repl[0]) > 2 {
			peers = repl[0][2]
		}
		// A successful read proves the writer path is responsive, not just
		// the replication endpoint.
		_, readErr := rowCount(cluster, idx)
		readable := readErr == nil
		n, parseErr := strconv.Atoi(peers)
		if parseErr == nil && n == cluster.Len()-1 && readable {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("node%d not ready within %v (peers=%s, readable=%v)",
				idx+1, timeout, peers, readable)
		}
		time.Sleep(readyPoll)
	}
}

// insert adds one contention row on one node (Go:
// `TypedContentionInsert`).
func insert(cluster *goharness.Cluster, node int, id, name string) error {
	if _, err := cluster.Node(node).Execute(
		"INSERT INTO records(id, name, phone, score) VALUES($1, $2, $3, $4)",
		id, name, "", int64(0)); err != nil {
		return fmt.Errorf("node%d insert '%s': %s", node+1, name, goharness.DBMessage(err))
	}
	return nil
}

// insertRetry inserts one contention row, retrying replayable `40001`
// pressure (post-rejection probe only; blast writers stay single-shot so
// pressure during the blast still fails loudly).
func insertRetry(cluster *goharness.Cluster, node int, id, name string) error {
	if _, err := cluster.Node(node).ExecuteRetry(
		"INSERT INTO records(id, name, phone, score) VALUES($1, $2, $3, $4)",
		[]any{id, name, "", int64(0)}, probeTimeout); err != nil {
		return fmt.Errorf("node%d insert '%s': %s", node+1, name, goharness.DBMessage(err))
	}
	return nil
}

// writerResult is one blast writer's outcome: per-insert latencies, a
// failed-write count, and the first error.
type writerResult struct {
	lat    []time.Duration
	failed uint64
	err    error
}

// writer runs one blast writer: `perWriter` sequential inserts, recording
// per-insert latency.
func writer(cluster *goharness.Cluster, w int) writerResult {
	var out writerResult
	out.lat = make([]time.Duration, 0, perWriter)
	for i := 0; i < perWriter; i++ {
		id := fmt.Sprintf("%032x", 10000+w*1000+i)
		name := fmt.Sprintf("w%d-%d", w, i)
		start := time.Now()
		err := insert(cluster, 0, id, name)
		out.lat = append(out.lat, time.Since(start))
		if err != nil {
			out.failed++
			out.err = err
			return out
		}
		// Bound the session capture: session capture can combine pending
		// transactions, so without a durability boundary the 400-row blast
		// would sign as one capture past the 64 KiB replication budget
		// (Go captures per HTTP write; see the README deviation).
		if (i+1)%10 == 0 {
			if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
				out.failed++
				out.err = fmt.Errorf("blast flush: %s", goharness.DBMessage(err))
				return out
			}
		}
	}
	return out
}

func runBody(cluster *goharness.Cluster) error {
	// Same schema on every node (Go: TypedRecords fixture tables), flagged
	// replicated on each node (row convergence needs the per-node flag).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i,
			"CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)"); err != nil {
			return fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("node%d flush after SET TABLE: %w", i+1, err)
		}
	}

	// Past the 64 KiB commit budget: clean rejection naming the budget,
	// no partial row. (Go sends a 5 KiB value against a 4 KiB per-value
	// budget; Rust has no per-value limit, so one insert carries a value
	// past the transaction budget instead.)
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush before oversize: %w", err)
	}
	big := strings.Repeat("V", txBudget+1)
	bigID := fmt.Sprintf("%032x", 2)
	_, err := cluster.Node(0).Execute(
		"INSERT INTO records(id, name, phone, score) VALUES($1, $2, $3, $4)",
		bigID, big, "", int64(0))
	if err == nil {
		return fmt.Errorf("oversize value accepted, want budget rejection")
	}
	if !strings.Contains(goharness.DBMessage(err), "budget exceeded") {
		return fmt.Errorf("rejection error %v does not name the budget", err)
	}
	for i := 0; i < cluster.Len(); i++ {
		n, err := rowCount(cluster, i)
		if err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("node%d has %d rows after rejection, want 0 (partial apply?)", i+1, n)
		}
	}

	// Writer not poisoned: a small write works and converges. The
	// rejection leaves momentary queue pressure (`40001`, documented
	// back-off-and-retry), so the probe retries it.
	if err := insertRetry(cluster, 0, fmt.Sprintf("%032x", 3), "ok"); err != nil {
		return err
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush after post-rejection write: %w", err)
	}
	if err := waitConverged(cluster, 1, probeTimeout, "post-rejection probe"); err != nil {
		return err
	}

	// Pressure blast: concurrent writers converge with bounded tail.
	results := make([]writerResult, writers)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			results[w] = writer(cluster, w)
		}(w)
	}
	wg.Wait()
	var lat []time.Duration
	lat = make([]time.Duration, 0, writers*perWriter)
	var failedWrites uint64
	var firstError error
	for _, r := range results {
		lat = append(lat, r.lat...)
		failedWrites += r.failed
		if firstError == nil {
			firstError = r.err
		}
	}
	if failedWrites != 0 {
		return fmt.Errorf("%d blast writes failed: %v", failedWrites, firstError)
	}
	if len(lat) != writers*perWriter {
		return fmt.Errorf("blast recorded %d latencies, want %d", len(lat), writers*perWriter)
	}
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	p99 := lat[int(float64(len(lat))*0.99)]
	fmt.Fprintf(os.Stderr, "blast: %d inserts p50=%v p99=%v max=%v\n",
		len(lat), lat[len(lat)/2], p99, lat[len(lat)-1])
	if p99 > p99Bound {
		return fmt.Errorf("blast p99 %v exceeds %v bound", p99, p99Bound)
	}
	if err := waitConverged(cluster, 1+writers*perWriter, blastTimeout, "blast"); err != nil {
		return err
	}
	if err := waitNodeReady(cluster, 0, readyTimeout); err != nil {
		return err
	}
	if err := waitNodeReady(cluster, 1, readyTimeout); err != nil {
		return err
	}
	return nil
}

func TestBudgetRejectionAndPressureConvergence(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("overload-budgets")
	opts.NumNodes = 2
	budget := txBudget
	queue := queueBytes
	capture := captureBytes
	opts.Replication = &goharness.ReplicationOptions{MaxTransactionBytes: &budget}
	opts.Sqlite = &goharness.SqliteOptions{
		TransactionBytes: &budget,
		QueueBytes:       &queue,
		CaptureBytes:     &capture,
	}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 2 {
		t.Fatalf("this scenario is written for 2 nodes, got %d", cluster.Len())
	}
	failed := true
	defer func() { cluster.Finish(failed) }()
	if err := runBody(cluster); err != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", err)
		cluster.DumpForensics("overload-budgets-no-convergence")
		t.Fatalf("scenario failed: %v", err)
	}
	failed = false
}
