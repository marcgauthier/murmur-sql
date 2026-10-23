// Live scenario: origin-log GC runs on every node under sustained load.
//
// Go port of the Rust `maintenance-under-load` scenario
// (/home/marc/MURMUR/tests-live/maintenance-under-load/tests/maintenance_under_load.rs:
// TestMaintenanceUnderSustainedLoad). Three nodes take sustained concurrent
// writes (zero failed writes allowed) while tight retention forces
// origin-log GC. GC passes and batch collection must advance on every node
// with writers provably still progressing, GC failures must stay flat, and
// all nodes must converge on exact counts plus equal id-ordered digests,
// followed by a post-maintenance write that replicates everywhere.
//
// Plain SQL replaces the typed-contention HTTP API.
//
// Only counter-observed maintenance is claimed (same inventory as Rust):
// origin-log GC is claimed; receipt collection, durability sync, FilesGC,
// snapshot resync, and spool compaction are NOT claimed.
package maintenanceunderload

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// envSeed is the seed rows inserted before the load phase (Rust:
// MURMUR_MAINT_LOAD_SEED, 120).
func envSeed() int {
	return envInt("MURMUR_MAINT_LOAD_SEED", 120)
}

// envMaxInserts is the per-writer insert cap before writers switch to
// updates (Rust: MURMUR_MAINT_LOAD_MAX_INSERTS, 4000).
func envMaxInserts() int {
	return envInt("MURMUR_MAINT_LOAD_MAX_INSERTS", 4000)
}

// envGCTimeout is the maintenance observation window (Rust:
// MURMUR_MAINT_LOAD_GC_TIMEOUT, 150s).
func envGCTimeout() time.Duration {
	return envDur("MURMUR_MAINT_LOAD_GC_TIMEOUT", 150*time.Second)
}

const (
	// seedTimeout is the seed-convergence deadline (Rust: 60s).
	seedTimeout = 60 * time.Second
	// convergeTimeout is the final-convergence deadline (Rust: 90s).
	convergeTimeout = 90 * time.Second
	// postTimeout is the post-maintenance write deadline (Rust: 30s).
	postTimeout = 30 * time.Second
	// maintPoll is the maintenance poll interval (Rust: 2s sleeps).
	maintPoll = 2 * time.Second
	// convergePoll is the convergence poll interval (Rust: 100ms).
	convergePoll = 100 * time.Millisecond
	// writerCap is the hang guard for writer goroutines if the driver
	// fails before signalling stop (normal runs stop writers long before
	// this).
	writerCap = 900 * time.Second
)

// row is one application row (Rust: TypedContentionRow).
type row struct {
	id    string
	name  string
	phone string
	score int64
}

// rowID renders the Go rowID format: %08x-0000-4000-8000-%012x. Same key
// space as the original suite (seed ids 0..seed, writer ranges at
// 1<<20*(w+1), post-maintenance id at seed + 1<<22).
func rowID(n int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", n, n)
}

// tryInsert inserts on one node; errors are returned (and counted by the
// caller) so one bad op ends a writer goroutine without killing the driver.
func tryInsert(db *murmur.Database, id, name string) error {
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)", id, name, "", int64(0))
		return nil, err
	}); err != nil {
		return err
	}
	return db.Flush()
}

// tryUpdateName sets name and publishes to replication.
func tryUpdateName(db *murmur.Database, id, name string) error {
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		changed, err := s.Execute("UPDATE contention SET name = ? WHERE id = ?", name, id)
		if err != nil {
			return nil, err
		}
		if changed != 1 {
			return nil, fmt.Errorf("update hit %d rows", changed)
		}
		return nil, nil
	}); err != nil {
		return err
	}
	return db.Flush()
}

// allRows returns the canonical PK-ordered rows on one node (Rust:
// TypedContentionRows + contentionDigest input; compared directly instead
// of hashing).
func allRows(t *testing.T, db *murmur.Database) []row {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name, phone, score FROM contention ORDER BY id")
	})
	if err != nil {
		t.Fatalf("read all rows failed: %v", err)
	}
	var out []row
	for _, r := range v.([][]sqlite.Value) {
		out = append(out, row{id: r[0].Str, name: r[1].Str, phone: r[2].Str, score: r[3].Int})
	}
	return out
}

// waitConverged waits until every node holds exactly want rows with
// identical contents (Rust: waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	last := make([]string, cluster.Len())
	converged := harness.WaitUntil(timeout, convergePoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			rows := allRows(t, cluster.Node(i))
			last[i] = fmt.Sprintf("count=%d", len(rows))
			if len(rows) != want {
				return false
			}
		}
		first := allRows(t, cluster.Node(0))
		for i := 1; i < cluster.Len(); i++ {
			if !slices.Equal(allRows(t, cluster.Node(i)), first) {
				last[i] = fmt.Sprintf("count=%d digest-differs", want)
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("nodes did not converge on %d rows with equal digests within %v; last=%v", want, timeout, last)
	}
}

// gcSnapshot holds per-node GC counters (Rust: scraped from /metrics).
type gcSnapshot struct {
	// runs counts successful collection passes (Rust: spedsql_gc_runs_total).
	runs uint64
	// collected counts origin-log transactions collected (Rust:
	// spedsql_gc_log_collected_total).
	collected uint64
	// lastError is the latest replication-engine error, if any. There is
	// no GC-failures counter; collection errors land here instead (Rust:
	// spedsql_gc_failures_total).
	lastError string
}

func gcCounters(t *testing.T, db *murmur.Database) gcSnapshot {
	t.Helper()
	status := db.Replication().Status()
	retention, err := db.RetentionStatus()
	if err != nil {
		t.Fatalf("retention status: %v", err)
	}
	var lastErr string
	if status.LastError != nil {
		lastErr = *status.LastError
	}
	return gcSnapshot{
		runs:      status.Metrics.RetentionRuns,
		collected: retention.CollectedTransactions,
		lastError: lastErr,
	}
}

// waitMaintenance polls until retention_runs AND collected_transactions
// have advanced past baseline on EVERY node (Rust: waitMaintenance).
// Writers must still be running while this is called.
func waitMaintenance(t *testing.T, cluster *harness.Cluster, before []gcSnapshot, timeout time.Duration) {
	t.Helper()
	observed := harness.WaitUntil(timeout, maintPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			cur := gcCounters(t, cluster.Node(i))
			if !(cur.runs > before[i].runs && cur.collected > before[i].collected) {
				return false
			}
		}
		return true
	})
	for i := 0; i < cluster.Len(); i++ {
		cur := gcCounters(t, cluster.Node(i))
		t.Logf("node%d: gc runs %d->%d collected %d->%d last_error=%q",
			i+1, before[i].runs, cur.runs, before[i].collected, cur.collected, cur.lastError)
	}
	if !observed {
		t.Fatalf("log-GC maintenance did not run+collect on every node within %v", timeout)
	}
}

func envInt(name string, fallback int) int {
	v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

// envDur parses a Go-style duration (150, 150s, 2m) with fallback.
func envDur(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	if s, err := strconv.ParseUint(raw, 10, 64); err == nil {
		if s > 0 {
			return time.Duration(s) * time.Second
		}
		return fallback
	}
	num, unit := raw[:len(raw)-1], raw[len(raw)-1:]
	n, err := strconv.ParseUint(num, 10, 64)
	if err != nil || n == 0 {
		return fallback
	}
	switch unit {
	case "s":
		return time.Duration(n) * time.Second
	case "m":
		return time.Duration(n) * 60 * time.Second
	case "h":
		return time.Duration(n) * 3600 * time.Second
	default:
		return fallback
	}
}

func TestMaintenanceUnderSustainedLoad(t *testing.T) {
	seed := envSeed()
	maxInserts := envMaxInserts()
	gcTimeout := envGCTimeout()
	// Tight retention forces origin-log GC (Rust: MinLogRetentionMs=1000,
	// MaxOfflineLogRetentionMs=20000, MinRetainedBatches=10). The 1s
	// collection cadence keeps background passes inside the observation
	// window; the product default (60s) would starve it.
	retention := time.Second
	retentionInterval := time.Second
	var minimumRetained uint64 = 10
	maximumOffline := 20 * time.Second
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "maintenance-under-load",
		NumNodes: 3,
		Replication: &harness.ReplicationOptions{
			Retention:         &retention,
			RetentionInterval: &retentionInterval,
			MinimumRetained:   &minimumRetained,
			MaximumOffline:    &maximumOffline,
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("maintenance-under-load-no-maint")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
	// Same schema on every node (Rust: TypedRecords/TypedContention tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)"); err != nil {
			t.Fatalf("migrate contention table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("contention", true); err != nil {
			t.Fatalf("opt-in contention: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Seed a base row set and converge before the load phase.
	for i := 0; i < seed; i++ {
		if err := tryInsert(cluster.Node(0), rowID(i), fmt.Sprintf("seed-%d", i)); err != nil {
			t.Fatalf("seed insert failed: %v", err)
		}
	}
	waitConverged(t, cluster, seed, seedTimeout)

	before := make([]gcSnapshot, 0, cluster.Len())
	for i := 0; i < cluster.Len(); i++ {
		before = append(before, gcCounters(t, cluster.Node(i)))
	}

	// Sustained concurrent writers on disjoint ranges: inserts until the
	// per-writer cap, then updates cycling their own rows so load never
	// stops while maintenance is awaited. Zero failed writes allowed.
	var stop atomic.Bool
	okCounts := make([]atomic.Int64, cluster.Len())
	errCounts := make([]atomic.Int64, cluster.Len())
	writerDeadline := time.Now().Add(writerCap)
	var wg sync.WaitGroup
	for w := 0; w < cluster.Len(); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db := cluster.Node(w)
			base := 1 << 20 * (w + 1)
			n := 0
			for {
				if stop.Load() || !time.Now().Before(writerDeadline) {
					return
				}
				var res error
				if n < maxInserts {
					res = tryInsert(db, rowID(base+n), fmt.Sprintf("w%d-%d", w, n))
				} else {
					res = tryUpdateName(db, rowID(base+(n%maxInserts)), fmt.Sprintf("w%d-u%d", w, n))
				}
				if res != nil {
					t.Logf("writer %d op %d: %v", w, n, res)
					errCounts[w].Add(1)
					return
				}
				okCounts[w].Add(1)
				n++
			}
		}()
	}

	// Maintenance must run WHILE writers are active: wait until GC passes
	// and batch collection advance on EVERY node, with writers still
	// making progress throughout.
	waitMaintenance(t, cluster, before, gcTimeout)
	progressMid := make([]int64, cluster.Len())
	for w := 0; w < cluster.Len(); w++ {
		progressMid[w] = okCounts[w].Load()
	}
	stop.Store(true)
	wg.Wait()
	// Proven overlap: every writer had already progressed when GC was
	// observed.
	for w := 0; w < cluster.Len(); w++ {
		t.Logf("writer %d: %d ok when GC observed", w, progressMid[w])
		if progressMid[w] <= 0 {
			t.Fatalf("writer %d made no progress before GC was observed; load/maintenance did not overlap", w)
		}
	}

	// Zero failed writes allowed (counts are final: the writers joined).
	for w := 0; w < cluster.Len(); w++ {
		t.Logf("writer %d: %d ok, %d failed", w, okCounts[w].Load(), errCounts[w].Load())
		if failedWrites := errCounts[w].Load(); failedWrites != 0 {
			t.Fatalf("writer %d had %d failed writes under maintenance; want zero", w, failedWrites)
		}
	}

	// No GC pass may have failed anywhere. Only GC-shaped errors fail
	// the test: the sticky last_error also records unrelated transport
	// transients (duplicate dials, handshake timeouts) under load, the
	// same filter gc-balance applies.
	for i := 0; i < cluster.Len(); i++ {
		after := gcCounters(t, cluster.Node(i))
		if after.lastError != before[i].lastError &&
			(isGCError(before[i].lastError) || isGCError(after.lastError)) {
			t.Fatalf("node%d replication GC error during load: before=%q after=%q", i+1, before[i].lastError, after.lastError)
		}
	}

	// Full convergence: exact counts plus equal id-ordered digests.
	wantCount := seed
	for w := 0; w < cluster.Len(); w++ {
		wantCount += min(int(okCounts[w].Load()), maxInserts)
	}
	waitConverged(t, cluster, wantCount, convergeTimeout)

	// A post-maintenance write replicates everywhere.
	if err := tryInsert(cluster.Node(2), rowID(seed+(1<<22)), "post-maint"); err != nil {
		t.Fatalf("post-maintenance insert: %v", err)
	}
	waitConverged(t, cluster, wantCount+1, postTimeout)
	t.Log("maintenance-under-load proven: GC ran on every node under zero-error load, full convergence")
	failed = false
}

// isGCError reports whether err looks like a GC/retention failure rather
// than an unrelated transport transient (same filter as gc-balance).
func isGCError(err string) bool {
	e := strings.ToLower(err)
	return strings.Contains(e, "retention") ||
		strings.Contains(e, "collect") ||
		strings.Contains(e, "prune") ||
		strings.Contains(e, "gc ") ||
		strings.Contains(e, "snapshot")
}
