// Live scenario: log growth stays bounded under sustained load.
//
// Go port of the Rust `log-boundedness` scenario
// (/home/marc/MURMUR/tests-live/log-boundedness/tests/log_boundedness.rs:
// TestLogBoundednessUnderSustainedLoad). Two nodes absorb sustained
// alternating writes for MURMUR_LOG_BOUNDEDNESS_SECONDS (default 75 s)
// while log-GC runs; per-node log growth must stay under its ceiling,
// no engine error may appear, on-disk spool growth must stay under
// 8 KiB/row without second-half acceleration, and the mesh must
// converge on exactly the acked rows with identical contents.
//
// There is no daemon text log, so the log-growth ceiling maps onto
// retained origin-log batches (committed - collected_transactions):
// every sustained insert appends one origin-log batch, so a dead GC
// grows the retained set ~1 batch/row while a working GC holds it
// ~flat. Plain SQL replaces the typed-contention HTTP API.
package logboundedness

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

const (
	// minAcked is the minimum acked rows for the boundedness proof
	// (Rust: acked < 100 fails).
	minAcked = 100
	// retainedFloorBatches is the retained-batch ceiling floor per
	// node. It absorbs the minimum-retained batches plus in-flight
	// unacknowledged batches at the sample points; the produced/4
	// slope below is far under the dead-GC slope of ~1 retained batch
	// per produced row, so a stalled GC fails.
	retainedFloorBatches = uint64(512)
	// diskBytesPerRow is the on-disk growth cap per acked row (Rust:
	// 8 KiB/row).
	diskBytesPerRow = uint64(8192)
	// diskAccelSlack is the second-half acceleration slack (Rust:
	// 2 * first_half + 1 MiB).
	diskAccelSlack = uint64(1024 * 1024)
	// convergeTimeout is the convergence deadline after the load
	// window (Rust: 90 s).
	convergeTimeout = 90 * time.Second
	// pollInterval is the convergence poll interval (Rust: 200 ms).
	pollInterval = 200 * time.Millisecond
)

// row is one application row (Rust: TypedContentionRow).
type row struct {
	id    string
	name  string
	phone string
	score int64
}

// keyID renders a zero-based 32-hex-char key (Rust: %032x).
func keyID(seq int) string {
	return fmt.Sprintf("%032x", seq)
}

// payload renders a ~200-byte value (Rust: "payload-<seq>-" + 25x
// "abcdefgh").
func payload(seq int) string {
	return fmt.Sprintf("payload-%d-%s", seq, strings.Repeat("abcdefgh", 25))
}

// insertRow writes one sustained insert, published to replication.
func insertRow(t *testing.T, db *murmur.Database, id, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
			id, name, "", int64(0))
		return nil, err
	}); err != nil {
		t.Fatalf("load insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after load insert: %v", err)
	}
}

// allRows returns the canonical PK-ordered rows on one node (compared
// directly instead of hashing).
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

// dirSize returns the total bytes of regular files under path (errors
// are skipped like the Rust walk).
func dirSize(path string) uint64 {
	var total uint64
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.Mode().IsRegular() {
			total += uint64(info.Size())
		}
		return nil
	})
	return total
}

// logSnapshot holds the origin-log balance counters sampled from one
// node.
type logSnapshot struct {
	// commits is the local commits produced (Status.Committed).
	commits uint64
	// collected is the origin-log transactions collected (durable).
	collected uint64
	// lastError is the latest replication-engine error, if any. There
	// is no text log to grep for panic/fatal, so the no-errors check
	// asserts this is unchanged across the load window instead.
	lastError *string
}

func logCounters(t *testing.T, cluster *harness.Cluster, idx int) logSnapshot {
	t.Helper()
	db := cluster.Node(idx)
	rs, err := db.RetentionStatus()
	if err != nil {
		t.Fatalf("retention status: %v", err)
	}
	return logSnapshot{
		commits:   db.Status().Committed,
		collected: rs.CollectedTransactions,
		lastError: db.Replication().Status().LastError,
	}
}

// retained returns the retained origin-log batches: every local commit
// appends one batch and collection removes them, so this is the
// in-process analog of the daemon's log-file size.
func retained(snap logSnapshot) uint64 {
	return snap.commits - min(snap.commits, snap.collected)
}

// waitConverged polls until every node holds exactly the acked rows with
// identical contents.
func waitConverged(t *testing.T, cluster *harness.Cluster, want []row, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, pollInterval, func() bool {
		if !equalRows(allRows(t, cluster.Node(0)), want) {
			return false
		}
		for i := 1; i < cluster.Len(); i++ {
			if !equalRows(allRows(t, cluster.Node(i)), want) {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not converge on %d rows within %v", len(want), timeout)
	}
}

func equalRows(a, b []row) bool {
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

// benignChurn reports whether a latched error is connection-lifecycle
// churn rather than an engine failure. All three values have
// understood mechanisms and none indicates collection, apply, or
// verification trouble (a wedged peer would still fail convergence
// below):
//   - "duplicate connection": mutual-dial tie-break loss, recorded on
//     the dialing side when the close lands during the Welcome read
//     (dialInner errors record unconditionally).
//   - "handshake timeout": 5 s Welcome wait, tripped by slow
//     handshakes on loaded machines.
//   - "stale session": the 5 s bootstrap tick pruning a session
//     silent past the stale horizon on loaded machines.
func benignChurn(s string) bool {
	return strings.Contains(s, "duplicate connection") ||
		strings.Contains(s, "handshake timeout") ||
		strings.Contains(s, "stale session")
}

// errorRecorder samples both nodes' last_error latches for the whole
// load window. A single end-sample can miss a transient engine error
// that later churn overwrites, so the no-errors leg fails on ANY
// observed non-benign value instead.
type errorRecorder struct {
	cluster *harness.Cluster
	stop    chan struct{}
	done    chan map[int]map[string]bool
}

func startErrorRecorder(cluster *harness.Cluster) *errorRecorder {
	r := &errorRecorder{
		cluster: cluster,
		stop:    make(chan struct{}),
		done:    make(chan map[int]map[string]bool, 1),
	}
	go func() {
		seen := map[int]map[string]bool{0: {}, 1: {}}
		for {
			for i := 0; i < cluster.Len(); i++ {
				if lastErr := cluster.Node(i).Replication().Status().LastError; lastErr != nil {
					seen[i][*lastErr] = true
				}
			}
			select {
			case <-r.stop:
				r.done <- seen
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	return r
}

func (r *errorRecorder) finish() map[int]map[string]bool {
	close(r.stop)
	return <-r.done
}

// settleMesh waits for the mesh to quiesce before the window opens, so
// setup churn (mesh-time dial races) lands in the baseline instead of
// the window. Best-effort: after peers agree it waits up to settleCap
// for stableCap of latch silence, then proceeds regardless.
func settleMesh(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	if ok := harness.WaitUntil(30*time.Second, 100*time.Millisecond, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if cluster.Node(i).Replication().Status().Peers != 1 {
				return false
			}
		}
		return true
	}); !ok {
		t.Fatal("mesh never settled to one peer per node before the load window")
	}
	const stableCap = 3 * time.Second
	const settleCap = 5 * time.Second
	start := time.Now()
	stableSince := start
	last := make([]string, cluster.Len())
	for time.Since(start) < settleCap {
		changed := false
		for i := 0; i < cluster.Len(); i++ {
			cur := ""
			if e := cluster.Node(i).Replication().Status().LastError; e != nil {
				cur = *e
			}
			if cur != last[i] {
				last[i] = cur
				changed = true
			}
		}
		if changed {
			stableSince = time.Now()
		} else if time.Since(stableSince) >= stableCap {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Log("mesh latch still churning after settle cap; proceeding (recorder masks churn)")
}

func TestLogBoundednessUnderSustainedLoad(t *testing.T) {
	// Log-GC must run during the load window. The product defaults (1h
	// retention, 60s cadence) would collect nothing inside a 75s
	// window, so the port uses aggressive overlays (Rust: same).
	retention := time.Second
	retentionInterval := time.Second
	minimumRetained := uint64(10)
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "log-boundedness",
		NumNodes: 2,
		Replication: &harness.ReplicationOptions{
			Retention:         &retention,
			RetentionInterval: &retentionInterval,
			MinimumRetained:   &minimumRetained,
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("log-boundedness-unbounded")
		}
		cluster.Finish(failed)
	}()

	// Same schema on every node.
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

	duration := harness.EnvSeconds("MURMUR_LOG_BOUNDEDNESS_SECONDS", 75)
	settleMesh(t, cluster)
	startLog := make([]logSnapshot, cluster.Len())
	for i := 0; i < cluster.Len(); i++ {
		startLog[i] = logCounters(t, cluster, i)
	}
	startDisk := make([]uint64, cluster.Len())
	for i := 0; i < cluster.Len(); i++ {
		startDisk[i] = dirSize(cluster.NodeInfo(i).DataDir)
	}
	midLog := make([]uint64, cluster.Len())
	for i := 0; i < cluster.Len(); i++ {
		midLog[i] = retained(startLog[i])
	}
	midDisk := append([]uint64(nil), startDisk...)

	// Fixed-window sustained write load, alternating nodes.
	rec := startErrorRecorder(cluster)
	deadline := time.Now().Add(duration)
	acked := 0
	seq := 0
	sampled := false
	for time.Now().Before(deadline) {
		node := seq % cluster.Len()
		insertRow(t, cluster.Node(node), keyID(seq), payload(seq))
		acked++
		seq++
		if !sampled && time.Until(deadline) < duration/2 {
			for i := 0; i < cluster.Len(); i++ {
				midLog[i] = retained(logCounters(t, cluster, i))
				midDisk[i] = dirSize(cluster.NodeInfo(i).DataDir)
			}
			sampled = true
		}
	}
	// A trivially short window may never cross the halfway sample; pin
	// the midpoint to the end so the acceleration check degrades to a
	// pass instead of comparing against the start twice.
	if !sampled {
		for i := 0; i < cluster.Len(); i++ {
			midLog[i] = retained(logCounters(t, cluster, i))
			midDisk[i] = dirSize(cluster.NodeInfo(i).DataDir)
		}
	}

	t.Logf("load complete: %d acked rows in %v", acked, duration)
	if acked < minAcked {
		t.Fatalf("only %d rows acked, load too small to judge boundedness", acked)
	}

	endLog := make([]logSnapshot, cluster.Len())
	for i := 0; i < cluster.Len(); i++ {
		endLog[i] = logCounters(t, cluster, i)
	}
	endDisk := make([]uint64, cluster.Len())
	for i := 0; i < cluster.Len(); i++ {
		endDisk[i] = dirSize(cluster.NodeInfo(i).DataDir)
	}
	for i := 0; i < cluster.Len(); i++ {
		t.Logf("node%d retained batches start=%d mid=%d end=%d (growth=%d)",
			i+1, retained(startLog[i]), midLog[i], retained(endLog[i]),
			retained(endLog[i])-min(retained(endLog[i]), retained(startLog[i])))
		t.Logf("node%d data-dir bytes start=%d mid=%d end=%d (growth=%d)",
			i+1, startDisk[i], midDisk[i], endDisk[i],
			endDisk[i]-min(endDisk[i], startDisk[i]))
	}

	// Primary assertion: per-node retained-batch growth ceiling. The
	// floor absorbs the minimum-retained batches and in-flight
	// unacknowledged batches; the produced/4 slope is far below the
	// dead-GC slope of ~1 retained batch per produced row, so a
	// stalled GC fails.
	for i := 0; i < cluster.Len(); i++ {
		produced := endLog[i].commits - min(endLog[i].commits, startLog[i].commits)
		collected := endLog[i].collected - min(endLog[i].collected, startLog[i].collected)
		growth := produced - min(produced, collected)
		ceiling := retainedFloorBatches + produced/4
		if growth > ceiling {
			t.Fatalf("node%d retained-batch growth %d over %d produced rows exceeds ceiling %d (unbounded log retention?)",
				i+1, growth, produced, ceiling)
		}
	}

	// No engine errors may be recorded during the load window (Rust:
	// no panic/fatal in the daemon log). Any observed value that is
	// neither the baseline nor understood connection churn fails.
	seen := rec.finish()
	for i := 0; i < cluster.Len(); i++ {
		if endLog[i].lastError != nil {
			seen[i][*endLog[i].lastError] = true
		}
		startStr := strPtr(startLog[i].lastError)
		for v := range seen[i] {
			if v != startStr && !benignChurn(v) {
				t.Fatalf("node%d replication engine error during the load window: baseline=%v observed=%q",
					i+1, startStr, v)
			}
		}
	}

	// Disk usage must stay proportional to data (Rust: 8 KiB/row cap,
	// and second-half growth must not accelerate past 2x the first
	// half plus 1 MiB slack).
	for i := 0; i < cluster.Len(); i++ {
		growth := endDisk[i] - min(endDisk[i], startDisk[i])
		cap := uint64(acked) * diskBytesPerRow
		if growth > cap {
			t.Fatalf("node%d data-dir growth %d bytes over %d rows exceeds %d (8KiB/row cap)",
				i+1, growth, acked, cap)
		}
		firstHalf := midDisk[i] - min(midDisk[i], startDisk[i])
		secondHalf := endDisk[i] - min(endDisk[i], midDisk[i])
		if secondHalf > 2*firstHalf+diskAccelSlack {
			t.Fatalf("node%d data-dir growth accelerates: first-half=%d second-half=%d",
				i+1, firstHalf, secondHalf)
		}
	}

	// Positive control: honest behavior works — the mesh converges on
	// exactly the acked rows with identical contents.
	want := make([]row, 0, acked)
	for s := 0; s < acked; s++ {
		want = append(want, row{id: keyID(s), name: payload(s)})
	}
	waitConverged(t, cluster, want, convergeTimeout)
	t.Logf("PASS: log growth bounded, disk proportional, mesh converged on %d rows", acked)

	failed = false
}

func strPtr(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
