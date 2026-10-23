// Live scenario: graceful shutdown mid-write and mid-snapshot.
//
// Go port of the Rust `graceful-shutdown` scenario
// (/home/marc/MURMUR/tests-live/graceful-shutdown/tests/graceful_shutdown.rs),
// itself a port of the original Go `graceful-shutdown` scenario
// (TestGracefulShutdownMidWriteAndMidSnapshot). Phase A stops node 1
// mid-write under load; phase B restarts node 3 stale (log ranges GC'd,
// forcing snapshot-resync) and stops it again mid-transfer. Both restarts
// must be clean and fast, lose no acknowledged rows, and the mesh must
// reconverge with identical contents; the snapshot path is proven, not
// assumed. Plain SQL replaces the managed typed-contention API, and
// in-process close/reopen replaces SIGTERM.
package gracefulshutdown

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// tableDDL is the contention table DDL (Rust: typed contention rows;
// `phone` carries the fat snapshot-widening payload, otherwise empty).
const tableDDL = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL DEFAULT '')"

// deadlineEnv is the member-deadline knob (Rust:
// MURMUR_GRACEFUL_MEMBER_DEADLINE_SECONDS, default 25).
const deadlineEnv = "MURMUR_GRACEFUL_MEMBER_DEADLINE_SECONDS"
const defaultDeadlineSecs = 25

const (
	// baselineRows is the baseline row count (Rust: 5).
	baselineRows = 5
	// minimumRetained is the retained log tail per origin (replication
	// minimum_retained); the rejoin proof allows at most this many log
	// transactions per survivor origin.
	minimumRetained = 10
	// preRoll is the phase-A load pre-roll (Rust: 4 s).
	preRoll = 4 * time.Second
	// writeTick is the load-writer cadence (Rust: 25 ms ticker, 1 writer
	// per node).
	writeTick = 25 * time.Millisecond
	// inFlightTimeout and inFlightPoll bound the in-flight gate (Rust: 5 s
	// deadline, 1 ms poll).
	inFlightTimeout = 5 * time.Second
	inFlightPoll    = time.Millisecond
	// restartBound is the fast-restart bound (Rust: < 60 s).
	restartBound = 60 * time.Second
	// bulkRows is the survivor bulk row count while node 3 is stale
	// (Rust: 100).
	bulkRows = 100
	// fatRows and fatBytes size the fat snapshot-widening rows (Rust: 8 x
	// ~256 KB).
	fatRows  = 8
	fatBytes = 256 * 1024
	// fatFiller is the snapshot-filler repeat unit.
	fatFiller = "SPeD-SQL-snapshot-filler-"
	// gcTimeout and gcPoll bound the GC-pass wait (Rust: 120 s, 500 ms).
	gcTimeout = 120 * time.Second
	gcPoll    = 500 * time.Millisecond
	// catchTimeout and catchPoll bound the snapshot catch window (Rust: 60
	// s, 5 ms).
	catchTimeout = 60 * time.Second
	catchPoll    = 5 * time.Millisecond
	// baselineTimeout is the baseline convergence deadline (Rust: 30 s).
	baselineTimeout = 30 * time.Second
	// phaseATimeout is the phase-A convergence deadline (Rust: 90 s).
	phaseATimeout = 90 * time.Second
	// countTimeout is the row-count deadline (Rust: 30 s).
	countTimeout = 30 * time.Second
	// finalTimeout is the final convergence deadline (Rust: 120 s).
	finalTimeout = 120 * time.Second
	// postTimeout is the post-recovery convergence deadline (Rust: 60 s).
	postTimeout = 60 * time.Second
	// poll is the convergence/row-count poll interval (Rust: 200 ms).
	poll = 200 * time.Millisecond
	// counterSampleCap bounds one retention-counter sample attempt (the
	// retention path crosses the engine command channel, which can
	// transiently refuse under load; Rust blocks instead).
	counterSampleCap = 30 * time.Second
)

// rowID renders a row id (Rust: gsRowID).
func rowID(n uint64) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", n>>32, n&0xffffffffffff)
}

// queryNames reads the ordered names on one node (Rust: sorted-name digest
// input). Errors are returned so convergence polls can treat them as
// not-yet-converged.
func queryNames(db *murmur.Database) ([]string, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM contention ORDER BY name")
	})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out, nil
}

// mustNames reads the ordered names on one node, failing the test on error.
func mustNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	names, err := queryNames(db)
	if err != nil {
		t.Fatalf("read names failed: %v", err)
	}
	return names
}

// queryCount reads the row count on one node. Errors are returned so polls
// can treat them as not-yet-converged.
func queryCount(db *murmur.Database) (int, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM contention")
	})
	if err != nil {
		return 0, err
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return 0, nil
	}
	return int(rows[0][0].Int), nil
}

// mustCount reads the row count on one node, failing the test on error.
func mustCount(t *testing.T, db *murmur.Database) int {
	t.Helper()
	n, err := queryCount(db)
	if err != nil {
		t.Fatalf("count rows failed: %v", err)
	}
	return n
}

// tryInsert is a best-effort insert; it reports true when acknowledged.
func tryInsert(db *murmur.Database, id, name, phone string) bool {
	_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO contention(id, name, phone) VALUES(?, ?, ?)",
			id, name, phone,
		)
		return nil, err
	})
	return err == nil
}

// insertRow inserts one row, failing the test on error.
func insertRow(t *testing.T, db *murmur.Database, node int, id, name, phone string) {
	t.Helper()
	if !tryInsert(db, id, name, phone) {
		t.Fatalf("node%d insert failed", node)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after insert failed: %v", node, err)
	}
}

// waitConverged polls exact counts plus equal ordered contents (Rust:
// waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	converged := harness.WaitUntil(timeout, poll, func() bool {
		var first []string
		for i := 0; i < cluster.Len(); i++ {
			got, err := queryNames(cluster.Node(i))
			if err != nil || len(got) != want {
				return false
			}
			if first == nil {
				first = got
			} else if !equalStrings(got, first) {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("nodes did not converge on %d rows with equal contents within %v", want, timeout)
	}
}

// waitRowCount waits until one node holds exactly want rows (Rust:
// waitRowCount).
func waitRowCount(t *testing.T, cluster *harness.Cluster, idx, want int, timeout time.Duration) {
	t.Helper()
	converged := harness.WaitUntil(timeout, poll, func() bool {
		n, err := queryCount(cluster.Node(idx))
		return err == nil && n == want
	})
	if !converged {
		t.Fatalf("node%d row count = %d, want %d within %v",
			idx+1, mustCount(t, cluster.Node(idx)), want, timeout)
	}
}

// assertAcknowledgedPresent requires every acknowledged value present on
// every node (Rust: assertAcknowledgedPresent).
func assertAcknowledgedPresent(t *testing.T, cluster *harness.Cluster, acknowledged map[string]bool) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		present := make(map[string]bool)
		for _, name := range mustNames(t, cluster.Node(i)) {
			present[name] = true
		}
		for value := range acknowledged {
			if !present[value] {
				t.Fatalf("node%d lost acknowledged write %q after restart", i+1, value)
			}
		}
	}
}

// assertCleanStop proves a clean stop (Rust: daemon log markers, which have
// no in-process equivalent): the close path completed and released the
// handle. StopNode already logs close errors; the no-loss assertions carry
// the real proof.
func assertCleanStop(t *testing.T, cluster *harness.Cluster, idx int, what string) {
	t.Helper()
	if cluster.NodeInfo(idx).DB != nil {
		t.Fatalf("%s: node handle still open after stop", what)
	}
}

// logCounters reads the log-path activity counters (received, applied):
// snapshot application bypasses both, so a flat pair proves the snapshot
// path (Rust: established snapshot-resync port pattern).
func logCounters(t *testing.T, cluster *harness.Cluster, idx int) (uint64, uint64) {
	t.Helper()
	h := cluster.Node(idx).Replication()
	if h == nil {
		t.Fatalf("node%d has no replication handle", idx+1)
	}
	m := h.Status().Metrics
	return m.ReceivedTransactions, m.AppliedTransactions
}

// collectedTotal reads the durable collected counter on one node (Rust:
// spedsql_gc_log_collected_total), retrying transient engine-channel
// refusals.
func collectedTotal(t *testing.T, cluster *harness.Cluster, idx int) uint64 {
	t.Helper()
	deadline := time.Now().Add(counterSampleCap)
	var lastErr error
	for {
		st, err := cluster.Node(idx).RetentionStatus()
		if err == nil {
			return st.CollectedTransactions
		}
		lastErr = err
		if !time.Now().Before(deadline) {
			t.Fatalf("node%d retention status unavailable for %v: %v", idx+1, counterSampleCap, lastErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// gcRuns reads the GC run counter on one node (Rust:
// spedsql_gc_runs_total).
func gcRuns(t *testing.T, cluster *harness.Cluster, idx int) uint64 {
	t.Helper()
	h := cluster.Node(idx).Replication()
	if h == nil {
		t.Fatalf("node%d has no replication handle", idx+1)
	}
	return h.Status().Metrics.RetentionRuns
}

// waitGCPass runs on-demand retention passes until one completes past the
// baseline (Rust: waitGCPass on the background ticker; here each poll
// triggers one directly instead of waiting for the tick). Transient collect
// refusals keep polling rather than failing: under contention the channel
// can briefly refuse.
func waitGCPass(t *testing.T, cluster *harness.Cluster, idx int, baseline uint64, timeout time.Duration) {
	t.Helper()
	converged := harness.WaitUntil(timeout, gcPoll, func() bool {
		if _, err := cluster.Node(idx).CollectRetention(); err != nil {
			return false
		}
		h := cluster.Node(idx).Replication()
		return h != nil && h.Status().Metrics.RetentionRuns > baseline
	})
	if !converged {
		t.Fatalf("no GC pass completed within %v (baseline %d)", timeout, baseline)
	}
}

// TestGracefulShutdownMidWriteAndMidSnapshot stops node 1 mid-write under
// load, then restarts node 3 stale (forcing snapshot-resync) and stops it
// again mid-transfer; both restarts must be clean and fast with no loss
// (Rust: graceful_shutdown_mid_write_and_mid_snapshot).
func TestGracefulShutdownMidWriteAndMidSnapshot(t *testing.T) {
	// Fixed node ids so the stale node can name its snapshot donors: the
	// harness trusts nobody by default, and phase B forces the snapshot
	// path.
	var idA, idB, idC [16]byte
	for i := range idA {
		idA[i] = 0x61
		idB[i] = 0x62
		idC[i] = 0x63
	}
	retention := time.Second
	minRetained := uint64(minimumRetained)
	maxOffline := 20 * time.Second
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "graceful-shutdown",
		NumNodes: 3,
		NodeIDs:  [][16]byte{idA, idB, idC},
		Replication: &harness.ReplicationOptions{
			Retention:       &retention,
			MinimumRetained: &minRetained,
			MaximumOffline:  &maxOffline,
		},
		TrustedSnapshotSources: map[int]map[[16]byte]bool{
			2: {idA: true, idB: true},
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("graceful-shutdown-no-convergence")
		}
		cluster.Finish(failed)
	}()

	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
	// Current-generation writer handles, refreshed after every restart.
	var handlesMu sync.Mutex
	handles := []*murmur.Database{cluster.Node(0), cluster.Node(1), cluster.Node(2)}
	getHandle := func(node int) *murmur.Database {
		handlesMu.Lock()
		defer handlesMu.Unlock()
		return handles[node]
	}
	setHandle := func(node int, db *murmur.Database) {
		handlesMu.Lock()
		defer handlesMu.Unlock()
		handles[node] = db
	}

	// Same schema on every node (Rust: typed contention fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("node%d migrate contention table: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("contention", true); err != nil {
			t.Fatalf("node%d opt-in contention: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}

	// Baseline: 5 rows converge before any shutdown.
	for i := 0; i < baselineRows; i++ {
		insertRow(t, cluster.Node(0), 1, rowID(uint64(i)), fmt.Sprintf("base-%d", i), "")
	}
	waitConverged(t, cluster, baselineRows, baselineTimeout)

	// Phase A: all three nodes under write load; stop node 1 mid-write.
	var ackMu sync.Mutex
	acknowledged := make(map[string]bool)
	for i := 0; i < baselineRows; i++ {
		acknowledged[fmt.Sprintf("base-%d", i)] = true
	}
	ack := func(value string) {
		ackMu.Lock()
		defer ackMu.Unlock()
		acknowledged[value] = true
	}
	ackCopy := func() map[string]bool {
		ackMu.Lock()
		defer ackMu.Unlock()
		out := make(map[string]bool, len(acknowledged))
		for k, v := range acknowledged {
			out[k] = v
		}
		return out
	}
	var stop atomic.Bool
	var active [3]atomic.Int64
	var seqs [3]atomic.Int64
	var wg sync.WaitGroup
	for node := 0; node < 3; node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			for !stop.Load() {
				time.Sleep(writeTick)
				if stop.Load() {
					break
				}
				seq := seqs[node].Add(1)
				value := fmt.Sprintf("load-n%d-%08d", node+1, seq)
				id := rowID(uint64(node+1)*1000000000 + uint64(seq))
				active[node].Add(1)
				db := getHandle(node)
				ok := tryInsert(db, id, value, "")
				active[node].Add(-1)
				if ok {
					ack(value)
				}
			}
		}(node)
	}
	// A failed phase assertion below must still release the writers so the
	// WaitGroup join cannot deadlock (t.Fatalf runs deferred calls).
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()
	time.Sleep(preRoll)

	// Prove a write was actually in flight when the stop landed, so a
	// vacuous pass (idle node, clean exit, nothing at stake) is
	// detectable.
	gate := harness.WaitUntil(inFlightTimeout, inFlightPoll, func() bool {
		return active[0].Load() > 0
	})
	if !gate {
		t.Fatalf("node1 had no write in flight before stop")
	}
	t.Logf("stopping node1 with %d writes in flight", active[0].Load())
	stopStart := time.Now()
	cluster.StopNode(0)
	t.Logf("node1 graceful stop took %v", time.Since(stopStart))
	assertCleanStop(t, cluster, 0, "node1 mid-write stop")

	restartStart := time.Now()
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node1: %v", err)
	}
	setHandle(0, cluster.Node(0))
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	restartElapsed := time.Since(restartStart)
	t.Logf("node1 restart took %v", restartElapsed)
	if restartElapsed > restartBound {
		t.Fatalf("node1 restart took %v, want fast restart under %v", restartElapsed, restartBound)
	}

	// Positive control: honest load converges across the restart.
	stop.Store(true)
	wg.Wait()
	total := len(ackCopy())
	waitConverged(t, cluster, total, phaseATimeout)
	assertAcknowledgedPresent(t, cluster, ackCopy())

	// Phase B: node 3 goes stale under aggressive retention while the
	// survivors write a fat snapshot payload, then a stop lands
	// mid-transfer on its restart.
	staleCount := mustCount(t, cluster.Node(2))
	cluster.StopNode(2)
	for i := 0; i < bulkRows; i++ {
		name := fmt.Sprintf("fresh-%d", i)
		insertRow(t, cluster.Node(0), 1, rowID(uint64(5000+i)), name, "")
		ack(name)
	}
	// Fat values widen the transfer window so the stop can land inside it
	// deterministically.
	big := strings.Repeat(fatFiller, fatBytes/len(fatFiller))
	for i := 0; i < fatRows; i++ {
		name := fmt.Sprintf("fat-%d", i)
		insertRow(t, cluster.Node(1), 2, rowID(uint64(6000+i)), name, big)
		ack(name)
	}
	want := len(ackCopy())
	waitRowCount(t, cluster, 0, want, countTimeout)
	waitRowCount(t, cluster, 1, want, countTimeout)

	// Member-deadline expiry plus a GC pass on EACH survivor (not a fixed
	// sleep for the pass: under contention the tick slips, so a fixed wait
	// can elapse without any pass collecting node 3's ranges, and the
	// rejoin silently takes the log path; and one survivor's pass is not
	// enough, since node 3 would pull the other's intact log). First sleep
	// out node 3's 20 s member deadline so its ranges stop gating GC, then
	// run passes until one completes after expiry and collects log
	// batches.
	gcRunsBefore := [2]uint64{gcRuns(t, cluster, 0), gcRuns(t, cluster, 1)}
	collectedBefore := [2]uint64{collectedTotal(t, cluster, 0), collectedTotal(t, cluster, 1)}
	deadlineWait := time.Duration(defaultDeadlineSecs) * time.Second
	if v, err := strconv.ParseUint(os.Getenv(deadlineEnv), 10, 64); err == nil && v > 0 {
		deadlineWait = time.Duration(v) * time.Second
	}
	t.Logf("waiting %v for member-deadline expiry", deadlineWait)
	time.Sleep(deadlineWait)
	for _, survivor := range []int{0, 1} {
		waitGCPass(t, cluster, survivor, gcRunsBefore[survivor], gcTimeout)
		collectedAfter := collectedTotal(t, cluster, survivor)
		if collectedAfter <= collectedBefore[survivor] {
			t.Fatalf("node%d post-expiry GC pass ran but collected no log batches (%d -> %d); node3's ranges are still pinned and the snapshot path is not forced",
				survivor+1, collectedBefore[survivor], collectedAfter)
		}
		t.Logf("node%d GC collected log batches after expiry (%d -> %d)",
			survivor+1, collectedBefore[survivor], collectedAfter)
	}
	t.Log("both survivors collected after expiry; snapshot path forced")

	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("restart stale node3: %v", err)
	}
	setHandle(2, cluster.Node(2))
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh stale node3: %v", err)
	}

	// Catch the transfer in flight: a row count strictly between the stale
	// count and the target while no transfer has completed yet. Poll fast;
	// the fat payload keeps the window open.
	detail := "no transfer activity observed"
	caught := harness.WaitUntil(catchTimeout, catchPoll, func() bool {
		count, err := queryCount(cluster.Node(2))
		if err != nil {
			return false
		}
		detail = fmt.Sprintf("stale=%d count=%d want=%d", staleCount, count, want)
		if count >= want {
			return true
		}
		return staleCount < count && count < want
	})
	t.Logf("stopping node3 during rejoin (caught=%v %s)", caught, detail)
	finalCount, err := queryCount(cluster.Node(2))
	if err != nil {
		t.Fatalf("node3 count read failed: %v", err)
	}
	if !caught || finalCount >= want {
		t.Fatalf("transfer completed before the stop could land mid-apply (%s); widen the payload", detail)
	}
	cluster.StopNode(2)
	assertCleanStop(t, cluster, 2, "node3 mid-transfer stop")

	restartStart = time.Now()
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("restart node3 after interrupt: %v", err)
	}
	setHandle(2, cluster.Node(2))
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after interrupt: %v", err)
	}
	restartElapsed = time.Since(restartStart)
	t.Logf("node3 post-interrupt restart took %v", restartElapsed)
	if restartElapsed > restartBound {
		t.Fatalf("node3 restart took %v, want fast restart under %v", restartElapsed, restartBound)
	}

	waitConverged(t, cluster, want, finalTimeout)
	// The snapshot path is proven via the receiver's counters on the fresh
	// post-restart handle, not merely by convergence: log catch-up would
	// hide a snapshot-path regression. The retained tail
	// (minimum_retained per origin) legitimately arrives via logs, so the
	// proof bounds the log path instead of requiring zero: at most the
	// retained tail, and far below the gap (the rest can only have arrived
	// via snapshot, the only other row path).
	received, applied := logCounters(t, cluster, 2)
	gap := want - staleCount
	if received > 2*minimumRetained {
		t.Fatalf("node3 rejoin pulled %d log transactions, above the retained tail (2 x %d); want snapshot for the bulk", received, minimumRetained)
	}
	if received >= uint64(gap) {
		t.Fatalf("node3 rejoin arrived over the log path (received=%d applied=%d gap=%d), want snapshot", received, applied, gap)
	}
	assertAcknowledgedPresent(t, cluster, ackCopy())

	// Post-recovery honest write still replicates everywhere.
	insertRow(t, cluster.Node(0), 1, rowID(999999), "post-recovery", "")
	ack("post-recovery")
	want = len(ackCopy())
	waitConverged(t, cluster, want, postTimeout)
	assertAcknowledgedPresent(t, cluster, ackCopy())
	t.Logf("graceful shutdown proven: 2 restarts, %d rows intact, contents converge", want)
	failed = false
}

func equalStrings(a, b []string) bool {
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
