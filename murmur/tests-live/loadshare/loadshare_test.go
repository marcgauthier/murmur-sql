// Live scenario: the writer scheduler shares service under local saturation.
//
// Go port of the Rust `loadshare` scenario
// (/home/marc/MURMUR/tests-live/loadshare/tests/loadshare.rs:
// TestWriterSharesUnderLoad). Two nodes form an encrypted mesh; node1
// absorbs 240 local writes across four concurrent writers while node2
// replicates 20 rows. The test requires full convergence (no remote
// starvation on the saturated node), acquisitions in both scheduler
// classes, and service debt within the 1s bound, and logs the
// local-commit latency distribution.
//
// Plain SQL replaces the typed-RIME HTTP API, and the scheduler
// assertions read the in-process WriterScheduler snapshot instead of
// scraping Prometheus /metrics.
package loadshare

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

const (
	// writers is the concurrent local writers on the saturated node.
	writers = 4
	// perWriter is the inserts per writer; 4*60 = 240 local rows.
	perWriter = 60
	// remoteRows is the rows replicated from the peer while local
	// writers saturate node1.
	remoteRows = 20
	// total is the full-convergence row count on each node.
	total = writers*perWriter + remoteRows

	// convergeTimeout is the convergence deadline (Rust: 60 s).
	convergeTimeout = 60 * time.Second
	// convergePoll is the convergence poll interval (Rust: 50 ms).
	convergePoll = 50 * time.Millisecond
	// remotePacing is the pacing between peer writes (Rust: 10 ms).
	remotePacing = 10 * time.Millisecond
	// maxDebt is the service-debt bound (Rust: 1 s, the product
	// default max_debt).
	maxDebt = time.Second
	// maxLatency is the absurd-latency tripwire for the slowest local
	// commit (Rust: 30 s).
	maxLatency = 30 * time.Second
)

// typedInsert writes one row, published to replication (Rust: TypedInsert).
//
// The product reentrancy guard (Database.Call) is database-global
// rather than goroutine-local: while any job executes on the writer
// thread, a concurrent Call from another goroutine is rejected as
// "cannot reenter". The four writers are genuinely concurrent here,
// so the guard trips constantly; retrying is the only way to keep
// them contending instead of serializing them behind a client mutex
// (which would vacate the saturation leg). Retries preserve the
// scenario intent: local commits still contend with each other and
// with remote applies under the scheduler.
func typedInsert(db *murmur.Database, name string) error {
	if err := callRetrying(func() error {
		_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
			_, err := s.Execute("INSERT INTO records(name) VALUES(?)", name)
			return nil, err
		})
		return err
	}); err != nil {
		return err
	}
	return callRetrying(db.Flush)
}

// callRetrying runs op, retrying the product's coarse "cannot reenter"
// rejection until it lands or a per-op deadline passes. Any other
// error returns immediately.
func callRetrying(op func() error) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		err := op()
		if err == nil || !strings.Contains(err.Error(), "cannot reenter") {
			return err
		}
		if !time.Now().Before(deadline) {
			return err
		}
		time.Sleep(time.Millisecond)
	}
}

// rowCount returns the row count on one node (Rust: rowCount).
func rowCount(t *testing.T, db *murmur.Database) int {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM records")
	})
	if err != nil {
		t.Fatalf("row count read failed: %v", err)
	}
	return int(v.([][]sqlite.Value)[0][0].Int)
}

func TestWriterSharesUnderLoad(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "loadshare",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("loadshare-no-converge")
		}
		cluster.Finish(failed)
	}()

	// Same schema on both nodes.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, "CREATE TABLE records(name TEXT NOT NULL PRIMARY KEY)"); err != nil {
			t.Fatalf("migrate records table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("records", true); err != nil {
			t.Fatalf("opt-in records: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Four concurrent writers saturate node1 while the driver inserts
	// the peer's rows on node2; the WaitGroup join matches the Rust
	// scope exit.
	var latMu sync.Mutex
	lat := make([]time.Duration, 0, writers*perWriter)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db := cluster.Node(0)
			for i := 0; i < perWriter; i++ {
				start := time.Now()
				res := typedInsert(db, fmt.Sprintf("local-%d-%d", w, i))
				latMu.Lock()
				lat = append(lat, time.Since(start))
				latMu.Unlock()
				if res != nil {
					t.Errorf("local write failed: %v", res)
					return
				}
			}
		}()
	}
	for i := 0; i < remoteRows; i++ {
		if err := typedInsert(cluster.Node(1), fmt.Sprintf("remote-%d", i)); err != nil {
			t.Fatalf("remote write failed: %v", err)
		}
		time.Sleep(remotePacing)
	}
	wg.Wait()

	// Full convergence: the saturated node must apply all 20 remote
	// rows, and the peer must absorb all 240 local rows.
	converged := harness.WaitUntil(convergeTimeout, convergePoll, func() bool {
		return rowCount(t, cluster.Node(0)) == total && rowCount(t, cluster.Node(1)) == total
	})
	n := rowCount(t, cluster.Node(0))
	if !converged || n != total {
		t.Fatalf("converged to %d rows, remote apply starved", n)
	}

	// Both scheduler classes must acquire on the saturated node, and
	// service debt must stay within the 1s bound.
	snap := cluster.Node(0).Scheduler().Snapshot()
	t.Logf("node1 scheduler: local_acq=%d remote_acq=%d debt_nanos=%d",
		snap.Local.Acquisitions, snap.Remote.Acquisitions, snap.DebtNanos)
	if snap.Local.Acquisitions < uint64(writers*perWriter) {
		t.Fatalf("local acquisitions = %d", snap.Local.Acquisitions)
	}
	if snap.Remote.Acquisitions == 0 {
		t.Fatal("remote class never acquired under local saturation")
	}
	if debt := time.Duration(snap.DebtNanos); debt > maxDebt {
		t.Fatalf("service debt = %v, exceeds 1s bound", debt)
	}

	// Local-commit latency distribution under load.
	latMu.Lock()
	defer latMu.Unlock()
	if len(lat) != writers*perWriter {
		t.Fatalf("latency sample = %d, want %d", len(lat), writers*perWriter)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	t.Logf("local-commit latency under load: p50=%v p95=%v max=%v (n=%d)",
		lat[len(lat)/2], lat[len(lat)*95/100], lat[len(lat)-1], len(lat))
	if lat[len(lat)-1] > maxLatency {
		t.Fatalf("max local-commit latency %v is absurd", lat[len(lat)-1])
	}

	failed = false
}
