// Live scenario: oversize writes fail closed, then a concurrent blast
// converges with bounded tail latency.
//
// Go port of the Rust `overload-budgets` scenario
// (/home/marc/MURMUR/tests-live/overload-budgets/tests/overload_budgets.rs).
// Two nodes run with small commit budgets; an oversize write must be
// rejected cleanly with no partial row on either node and no writer
// poisoning, then a concurrent blast (8 writers x 50 inserts) must fully
// converge with bounded tail latency while both nodes stay responsive.
// Plain SQL replaces the typed-contention HTTP API; the HTTP body-cap leg
// has no equivalent (no HTTP server exists) and the per-value-bytes leg is
// adapted to the transaction_bytes commit budget (no per-value product
// limit exists).
package overloadbudgets

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// txBudget is the commit budget on every node (Rust: 64 KiB).
	txBudget = 64 << 10
	// queueBytes is the admission queue: comfortably above the commit
	// budget so the oversize leg trips the commit budget
	// deterministically (validation requires
	// transaction_bytes <= queue_bytes).
	queueBytes = 8 << 20
	// captureBytes is the capture worker budget
	// (queue_bytes >= capture_bytes per validation).
	captureBytes = 1 << 20

	// probeTimeout is the post-rejection convergence deadline (Rust: 30s).
	probeTimeout = 30 * time.Second
	// blastTimeout is the blast convergence deadline (Rust: 60s).
	blastTimeout = 60 * time.Second
	// convergePoll is the convergence poll interval (Rust: 200ms).
	convergePoll = 200 * time.Millisecond
	// readyTimeout is the final readiness deadline per node (Rust: 30s).
	readyTimeout = 30 * time.Second
	// readyPoll is the readiness poll interval (Rust: 100ms).
	readyPoll = 100 * time.Millisecond
	// p99Bound is the blast tail-latency bound (Rust: p99 under 10s).
	p99Bound = 10 * time.Second
)

// ddl is the contention table (Rust: records(id, name, phone, score)).
const ddl = "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)"

// contentionRow is one replicated row (Rust: TypedContentionRow).
type contentionRow struct {
	id    string
	name  string
	phone string
	score int64
}

// queryRows reads all rows on one node (Rust: rows).
func queryRows(t *testing.T, db *murmur.Database) []contentionRow {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name, phone, score FROM records")
	})
	if err != nil {
		t.Fatalf("read rows: %v", err)
	}
	var out []contentionRow
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, contentionRow{id: row[0].Str, name: row[1].Str, phone: row[2].Str, score: row[3].Int})
	}
	return out
}

// digest renders the canonical digest of a row set: sorted debug form
// (Rust: sorted Debug dump; same criterion, cheaper to express).
func digest(rows []contentionRow) string {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].id != rows[j].id {
			return rows[i].id < rows[j].id
		}
		if rows[i].name != rows[j].name {
			return rows[i].name < rows[j].name
		}
		if rows[i].phone != rows[j].phone {
			return rows[i].phone < rows[j].phone
		}
		return rows[i].score < rows[j].score
	})
	return fmt.Sprintf("%+v", rows)
}

// rowCount counts rows on one node (cheap pre-check before full digest
// reads).
func rowCount(t *testing.T, db *murmur.Database) int {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM records")
	})
	if err != nil {
		t.Fatalf("row count: %v", err)
	}
	return int(v.([][]sqlite.Value)[0][0].Int)
}

// tryRowCount counts rows on one node, reporting false when the read fails
// so convergence polls treat it as not-yet-converged.
func tryRowCount(db *murmur.Database) (int, bool) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM records")
	})
	if err != nil {
		return 0, false
	}
	found := v.([][]sqlite.Value)
	if len(found) == 0 {
		return 0, false
	}
	return int(found[0][0].Int), true
}

// tryDigest reads the canonical digest on one node, reporting false when
// the read fails.
func tryDigest(db *murmur.Database) (string, bool) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name, phone, score FROM records")
	})
	if err != nil {
		return "", false
	}
	var out []contentionRow
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, contentionRow{id: row[0].Str, name: row[1].Str, phone: row[2].Str, score: row[3].Int})
	}
	return digest(out), true
}

// waitConverged waits until every node holds exactly want rows with equal
// digests (Rust: wait_converged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration, what string) {
	t.Helper()
	converged := harness.WaitUntil(timeout, convergePoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			n, ok := tryRowCount(cluster.Node(i))
			if !ok || n != want {
				return false
			}
		}
		first, ok := tryDigest(cluster.Node(0))
		if !ok {
			return false
		}
		for i := 1; i < cluster.Len(); i++ {
			d, ok := tryDigest(cluster.Node(i))
			if !ok || d != first {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("nodes did not converge on %d rows within %v (%s)", want, timeout, what)
	}
}

// waitNodeReady waits until one node is responsive with its full peer set
// (Rust: wait_node_ready). A successful read proves the writer path is
// responsive, not just the replication endpoint.
func waitNodeReady(t *testing.T, cluster *harness.Cluster, idx int, timeout time.Duration) {
	t.Helper()
	ready := harness.WaitUntil(timeout, readyPoll, func() bool {
		h := cluster.Node(idx).Replication()
		if h == nil {
			return false
		}
		peers := h.Status().Peers
		_, err := cluster.Node(idx).Read(func(s *murmur.Sql) (any, error) { return nil, nil })
		return peers == cluster.Len()-1 && err == nil
	})
	if !ready {
		t.Fatalf("node%d not ready within %v", idx+1, timeout)
	}
}

// insertOne inserts one contention row on one node (Rust: insert).
func insertOne(cluster *harness.Cluster, node int, id, name string) error {
	_, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO records(id, name, phone, score) VALUES(?, ?, ?, ?)",
			id, name, "", int64(0),
		)
		return nil, err
	})
	return err
}

// TestBudgetRejectionAndPressureConvergence rejects an oversize write
// cleanly, then converges a concurrent blast with bounded tail latency
// (Rust: budget_rejection_and_pressure_convergence).
func TestBudgetRejectionAndPressureConvergence(t *testing.T) {
	maxTxBytes := txBudget
	txnBytes := txBudget
	qBytes := queueBytes
	capBytes := captureBytes
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "overload-budgets",
		NumNodes: 2,
		Replication: &harness.ReplicationOptions{
			MaxTransactionBytes: &maxTxBytes,
		},
		Sqlite: &harness.SqliteOptions{
			TransactionBytes: &txnBytes,
			QueueBytes:       &qBytes,
			CaptureBytes:     &capBytes,
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("overload-budgets-no-convergence")
		}
		cluster.Finish(failed)
	}()

	// Same schema on every node.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, ddl); err != nil {
			t.Fatalf("node%d migrate records table: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("records", true); err != nil {
			t.Fatalf("node%d opt-in records: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}

	// Past the 64 KiB commit budget: clean rejection naming the budget, no
	// partial row. (Rust has no per-value limit, so one insert carries a
	// value past the transaction budget instead.)
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush before oversize: %v", err)
	}
	big := strings.Repeat("V", txBudget+1)
	_, err = cluster.Node(0).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO records(id, name, phone, score) VALUES(?, ?, ?, ?)",
			fmt.Sprintf("%032x", 2), big, "", int64(0),
		)
		return nil, err
	})
	if err == nil {
		t.Fatal("oversize value accepted, want budget rejection")
	}
	var mErr *murmur.Error
	if !errors.As(err, &mErr) || mErr.Kind != murmur.KindLimit {
		t.Fatalf("rejection error %v is not the budget limit", err)
	}
	if !strings.Contains(err.Error(), "budget exceeded") {
		t.Fatalf("rejection error %v does not name the budget", err)
	}
	for i := 0; i < cluster.Len(); i++ {
		if n := rowCount(t, cluster.Node(i)); n != 0 {
			t.Fatalf("node%d has %d rows after rejection, want 0 (partial apply?)", i+1, n)
		}
	}

	// Writer not poisoned: a small write works and converges.
	if err := insertOne(cluster, 0, fmt.Sprintf("%032x", 3), "ok"); err != nil {
		t.Fatalf("post-rejection write: %v", err)
	}
	waitConverged(t, cluster, 1, probeTimeout, "post-rejection probe")

	// Pressure blast: concurrent writers converge with bounded tail.
	const writers = 8
	const perWriter = 50
	var latMu sync.Mutex
	lat := make([]time.Duration, 0, writers*perWriter)
	var failedWrites atomic.Uint64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				id := fmt.Sprintf("%032x", 10000+w*1000+i)
				name := fmt.Sprintf("w%d-%d", w, i)
				start := time.Now()
				result := insertOne(cluster, 0, id, name)
				el := time.Since(start)
				latMu.Lock()
				lat = append(lat, el)
				latMu.Unlock()
				if result != nil {
					failedWrites.Add(1)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if n := failedWrites.Load(); n != 0 {
		t.Fatalf("%d blast writes failed", n)
	}
	if len(lat) != writers*perWriter {
		t.Fatalf("blast recorded %d latencies, want %d", len(lat), writers*perWriter)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p99 := lat[int(float64(len(lat))*0.99)]
	t.Logf("blast: %d inserts p50=%v p99=%v max=%v", len(lat), lat[len(lat)/2], p99, lat[len(lat)-1])
	if p99 > p99Bound {
		t.Fatalf("blast p99 %v exceeds %v bound", p99, p99Bound)
	}
	waitConverged(t, cluster, 1+writers*perWriter, blastTimeout, "blast")
	waitNodeReady(t, cluster, 0, readyTimeout)
	waitNodeReady(t, cluster, 1, readyTimeout)

	failed = false
}
