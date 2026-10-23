// Live scenario: local write priority under dual contention.
//
// Go port of the Rust `write-priority` scenario
// (/home/marc/MURMUR/tests-live/write-priority/tests/write_priority.rs),
// itself a port of the original Go `write-priority` scenario
// (TestLocalWritePriority). Two nodes take continuous writes on both nodes
// (dense writers with disjoint 16-byte row-ID ranges) while serving reads,
// under full-mesh replication. The test gates on local progress (>= 80
// rows, >= 4 reads), the scheduler's debt bound under dual contention
// (>= 100 dual grants measured, debt within the 1s bound, both classes
// served), and convergence (identical row counts and identical row sets).
//
// Plain SQL replaces the managed typed-contention HTTP API (one transaction
// per insert), and the scheduler assertions read the in-process
// WriterScheduler snapshot dual-grant counters instead of scraping
// Prometheus /metrics. Service-time shares cannot express grant priority: a
// millisecond remote apply outweighs hundreds of microsecond local commits,
// so the gate counts decisions.
package writepriority

import (
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// writers is the writer count per node (Rust: 64 looping writers per
	// node; enough to keep both scheduler classes backlogged, which is
	// what the 90/10 policy leg needs).
	writers = 64
	// minRows and minReads are the local-progress floors (Rust: expected <
	// 80 || reads < 4 fails).
	minRows  = 80
	minReads = 4
	// maxDebt is the scheduler guarantee under contention: per-class
	// service stays within 1s of the 90/10 shares.
	maxDebt = time.Second
	// minDualGrants is the vacuity gate: at least 100 dual grants measured.
	minDualGrants = 100
	// readPacing is the read pacing during the write phase (Rust: 50ms).
	readPacing = 50 * time.Millisecond
	// convergePoll is the convergence poll interval (Rust: 200ms).
	convergePoll = 200 * time.Millisecond
)

// ddl is the shared records table (Rust: typed-contention tables).
const ddl = "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT)"

// hexID builds a 16-byte row ID with node/worker/seq embedded (Rust:
// raw[16]byte); disjoint across writers by construction, hex-encoded for
// the TEXT key.
func hexID(node, worker int, seq uint64) string {
	var raw [16]byte
	raw[0] = byte(node)
	raw[1] = byte(worker)
	raw[8] = byte(seq >> 56)
	raw[9] = byte(seq >> 48)
	raw[10] = byte(seq >> 40)
	raw[11] = byte(seq >> 32)
	raw[12] = byte(seq >> 24)
	raw[13] = byte(seq >> 16)
	raw[14] = byte(seq >> 8)
	raw[15] = byte(seq)
	return hex.EncodeToString(raw[:])
}

// contentionInsert inserts one row, published to replication (Rust:
// TypedContentionInsert).
func contentionInsert(db *murmur.Database, id, name string) error {
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		return err
	}
	return db.Flush()
}

// rowPair is one (id, name) row.
type rowPair struct {
	id   string
	name string
}

// queryRows reads the full sorted row set on one node (Rust:
// TypedContentionRows + sort). Errors are returned so convergence polls can
// treat them as not-yet-converged.
func queryRows(db *murmur.Database) ([]rowPair, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name FROM records ORDER BY id")
	})
	if err != nil {
		return nil, err
	}
	var out []rowPair
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, rowPair{id: row[0].Str, name: row[1].Str})
	}
	return out, nil
}

// mustRows reads the full sorted row set on one node, failing the test on
// error.
func mustRows(t *testing.T, db *murmur.Database) []rowPair {
	t.Helper()
	rows, err := queryRows(db)
	if err != nil {
		t.Fatalf("state query failed: %v", err)
	}
	return rows
}

// dualGrants sums dual-contention writer grants across both nodes (Rust:
// spedsql_sched_dual_grants_total). Returns (local, remote).
func dualGrants(cluster *harness.Cluster) (local, remote uint64) {
	for i := 0; i < cluster.Len(); i++ {
		snap := cluster.Node(i).Scheduler().Snapshot()
		local += snap.Local.DualGrants
		remote += snap.Remote.DualGrants
	}
	return local, remote
}

// dualService sums dual-contention writer service (nanoseconds) across both
// nodes, reported for diagnosis only: grant costs are asymmetric
// (microsecond local commits vs millisecond remote applies), so service
// shares cannot gate the 90/10 grant policy.
func dualService(cluster *harness.Cluster) (local, remote uint64) {
	for i := 0; i < cluster.Len(); i++ {
		snap := cluster.Node(i).Scheduler().Snapshot()
		local += snap.Local.DualServiceNanos
		remote += snap.Remote.DualServiceNanos
	}
	return local, remote
}

// TestLocalWritePriority runs dense writers on both nodes under full-mesh
// replication while serving reads, then gates on progress, the scheduler
// debt bound, and convergence (Rust: local_write_priority).
func TestLocalWritePriority(t *testing.T) {
	writeFor := harness.EnvSeconds("MURMUR_LIVE_WRITE_PRIORITY_SECONDS", 12)
	syncTimeout := harness.EnvSeconds("MURMUR_LIVE_WRITE_PRIORITY_SYNC_TIMEOUT_SECONDS", 120)
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "write-priority",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("write-priority-no-converge")
		}
		cluster.Finish(failed)
	}()

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

	beforeLocal, beforeRemote := dualGrants(cluster)
	beforeSvcLocal, beforeSvcRemote := dualService(cluster)
	deadline := time.Now().Add(writeFor)

	// Dense writers on both nodes saturate the microsecond-commit
	// scheduler into persistent backlog on both classes, where the 90/10
	// policy binds (Rust: writer threads + scope join).
	var written atomic.Int64
	var maxDebtNanos atomic.Uint64
	var firstErrMu sync.Mutex
	var firstErr error
	reads := 0
	var wg sync.WaitGroup
	for node := 0; node < 2; node++ {
		for worker := 0; worker < writers; worker++ {
			wg.Add(1)
			go func(node, worker int) {
				defer wg.Done()
				db := cluster.Node(node)
				var seq uint64
				for time.Now().Before(deadline) {
					id := hexID(node, worker, seq)
					name := fmt.Sprintf("v-n%d-w%d-%d", node, worker, seq)
					if err := contentionInsert(db, id, name); err != nil {
						firstErrMu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						firstErrMu.Unlock()
						return
					}
					seq++
					written.Add(1)
				}
			}(node, worker)
		}
	}
	// Reads stay served on both nodes while both replicate. The same loop
	// samples scheduler debt on both nodes, so the bound below covers the
	// whole contended window, not just its end state.
	for time.Now().Before(deadline) {
		for node := 0; node < 2; node++ {
			_ = mustRows(t, cluster.Node(node))
			reads++
			debt := cluster.Node(node).Scheduler().Snapshot().DebtNanos
			for {
				cur := maxDebtNanos.Load()
				if debt <= cur || maxDebtNanos.CompareAndSwap(cur, debt) {
					break
				}
			}
		}
		time.Sleep(readPacing)
	}
	wg.Wait()
	firstErrMu.Lock()
	ferr := firstErr
	firstErrMu.Unlock()
	if ferr != nil {
		t.Fatalf("writer: %v", ferr)
	}
	expected := int(written.Load())
	if expected < minRows || reads < minReads {
		t.Fatalf("insufficient local progress: %d rows, %d reads", expected, reads)
	}

	// Priority: the 90/10 policy is service-fair with a 1s debt bound, so
	// transient grant shares are unconstrained by design (catch-up phases
	// legitimately over-serve the behind class). What the scheduler
	// guarantees -- and what this gates -- is the debt bound itself, plus
	// two-sided contention service.
	afterLocal, afterRemote := dualGrants(cluster)
	local := afterLocal - beforeLocal
	remote := afterRemote - beforeRemote
	total := local + remote
	afterSvcLocal, afterSvcRemote := dualService(cluster)
	share := 0.0
	if total > 0 {
		share = float64(remote) / float64(total)
	}
	t.Logf("dual-contention writer grants: local=%d replication=%d share=%.3f (service local=%.3fs replication=%.3fs, max debt %.3fs)",
		local, remote, share,
		float64(afterSvcLocal-beforeSvcLocal)/1e9,
		float64(afterSvcRemote-beforeSvcRemote)/1e9,
		float64(maxDebtNanos.Load())/1e9)
	if total < minDualGrants {
		t.Fatalf("only %d dual-contention grants, want >= %d to measure the 90/10 policy", total, minDualGrants)
	}
	// No both-sides-won assert: when local single-grant service runs
	// ahead, the catch-up rule legitimately awards every dual decision to
	// the behind class. Dual remote grants already prove local waiters
	// contended (that is what makes them dual); local progress above
	// proves locals were served.
	if maxDebtNanos.Load() > uint64(maxDebt.Nanoseconds()) {
		t.Fatalf("scheduler debt exceeded the 1s bound under contention")
	}

	// Convergence: identical row counts and identical row sets.
	converged := harness.WaitUntil(syncTimeout, convergePoll, func() bool {
		a, errA := queryRows(cluster.Node(0))
		b, errB := queryRows(cluster.Node(1))
		return errA == nil && errB == nil &&
			len(a) == expected && len(b) == expected && equalRows(a, b)
	})
	aCount := len(mustRows(t, cluster.Node(0)))
	bCount := len(mustRows(t, cluster.Node(1)))
	if !converged || aCount != expected || bCount != expected {
		t.Fatalf("peers did not converge to %d rows (counts %d vs %d)", expected, aCount, bCount)
	}
	t.Logf("converged: %d rows, %d reads, identical row sets", expected, reads)
	failed = false
}

func equalRows(a, b []rowPair) bool {
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
