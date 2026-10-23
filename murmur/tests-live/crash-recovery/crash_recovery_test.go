// Live scenario: two nodes restart at different times while background
// writers hammer all nodes; every acknowledged write must survive.
//
// Go port of the Rust `crash-recovery` scenario
// (/home/marc/MURMUR/tests-live/crash-recovery/tests/crash_recovery.rs).
// Two nodes are abruptly restarted at different times while background
// writers (2 workers x 3 nodes, 50 ms tick) hammer all nodes; every
// acknowledged write must survive on every node with equal digests, plus a
// post-recovery write. Plain SQL replaces the typed-records HTTP API, and
// in-process close/reopen replaces SIGKILL.
package crashrecovery

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// tableDDL is the table DDL (Rust: typed records; only names matter
// here).
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)"

// writeEnv is the write-window knob (Rust: MURMUR_CRASH_WRITE_SECONDS,
// default 12).
const writeEnv = "MURMUR_CRASH_WRITE_SECONDS"
const defaultWriteSecs = 12

// settleEnv is the settle knob (Rust: MURMUR_CRASH_SETTLE_SECONDS,
// fallbacks 60 then 20).
const settleEnv = "MURMUR_CRASH_SETTLE_SECONDS"

const (
	// writeTick is the background-writer cadence (Rust: 50 ms ticker, 2
	// workers per node).
	writeTick = 50 * time.Millisecond
	// workersPerNode is the writer count per node (Rust: 2).
	workersPerNode = 2
	// refusedBackoff is the refused-request backoff (Rust: 5 ms sleep
	// after a failed insert).
	refusedBackoff = 5 * time.Millisecond
	// peersTimeout is the initial peer wait (Rust: 20 s waitForPeers).
	peersTimeout = 20 * time.Second
	// rejoinTimeout is the post-restart peer wait (Rust: 90 s).
	rejoinTimeout = 90 * time.Second
	// peersPoll is the peer poll interval (Rust: 50 ms).
	peersPoll = 50 * time.Millisecond
	// ackPoll is the acknowledged-set poll interval (Rust: 100 ms).
	ackPoll = 100 * time.Millisecond
	// inFlightTimeout and inFlightPoll bound the in-flight gate (Rust: 5
	// s deadline, 1 ms poll).
	inFlightTimeout = 5 * time.Second
	inFlightPoll    = time.Millisecond
)

// names is the canonical ordered names on one node (Rust:
// Cluster.TypedNames).
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM live_typed_records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read names failed: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// tryInsert is a best-effort insert; true when acknowledged (Rust: writers
// keep only err == nil values; kill/restart windows may refuse requests).
func tryInsert(db *murmur.Database, name string) bool {
	_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name) VALUES(?)", name)
		return nil, err
	})
	return err == nil
}

// peerCount is the live session count (Rust: connected_peers).
func peerCount(t *testing.T, cluster *harness.Cluster, node int) int {
	t.Helper()
	h := cluster.Node(node).Replication()
	if h == nil {
		t.Fatalf("node%d replication disabled", node+1)
	}
	return h.Status().Peers
}

// waitForPeers waits until every node reports its wanted peer count (Rust:
// waitForPeers).
func waitForPeers(t *testing.T, cluster *harness.Cluster, want []int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, peersPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if peerCount(t, cluster, i) != want[i] {
				return false
			}
		}
		return true
	})
	if !ok {
		var states []string
		for i := 0; i < cluster.Len(); i++ {
			states = append(states, fmt.Sprintf("node%d=%d(want %d)", i+1, peerCount(t, cluster, i), want[i]))
		}
		t.Fatalf("peers did not reach %v within %v: %v", want, timeout, states)
	}
}

// allNodesHaveAcknowledged reports whether every node holds every
// acknowledged value.
func allNodesHaveAcknowledged(t *testing.T, cluster *harness.Cluster, acknowledged map[string]bool) bool {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		present := make(map[string]bool)
		for _, n := range names(t, cluster.Node(i)) {
			present[n] = true
		}
		for v := range acknowledged {
			if !present[v] {
				return false
			}
		}
	}
	return true
}

// assertAcknowledgedPresent requires every acknowledged value present on
// every node (Rust: assertAcknowledgedPresent).
func assertAcknowledgedPresent(t *testing.T, cluster *harness.Cluster, acknowledged map[string]bool) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		present := make(map[string]bool)
		for _, n := range names(t, cluster.Node(i)) {
			present[n] = true
		}
		for value := range acknowledged {
			if !present[value] {
				t.Fatalf("node%d lost acknowledged write %q after restart", i+1, value)
			}
		}
	}
}

// convergenceSnapshot is the equal-counts plus equal-ordered-contents
// snapshot on all nodes (Rust: assertConverged / waitForConvergence;
// ordered-vec equality replaces the SHA-256 digest with identical
// strength).
func convergenceSnapshot(t *testing.T, cluster *harness.Cluster) [][]string {
	t.Helper()
	out := make([][]string, 0, cluster.Len())
	for i := 0; i < cluster.Len(); i++ {
		out = append(out, names(t, cluster.Node(i)))
	}
	return out
}

func assertConverged(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	snap := convergenceSnapshot(t, cluster)
	for i, got := range snap {
		if i == 0 {
			continue
		}
		if !slices.Equal(got, snap[0]) {
			t.Fatalf("node%d contents != node1 contents", i+1)
		}
	}
}

func waitForConvergence(t *testing.T, cluster *harness.Cluster, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, ackPoll, func() bool {
		snap := convergenceSnapshot(t, cluster)
		for _, got := range snap[1:] {
			if !slices.Equal(got, snap[0]) {
				return false
			}
		}
		return true
	})
	if !ok {
		for i, got := range convergenceSnapshot(t, cluster) {
			t.Logf("node%d after recovery rows=%d", i+1, len(got))
		}
		t.Fatalf("three nodes did not converge after recovery within %v", timeout)
	}
}

// killDuringWrites requires a write in flight on node before the restart
// lands (Rust: killDuringWrites refuses a vacuous kill).
func killDuringWrites(t *testing.T, active *[3]atomic.Int64, node int) {
	t.Helper()
	gate := harness.WaitUntil(inFlightTimeout, inFlightPoll, func() bool {
		return active[node].Load() > 0
	})
	if !gate {
		t.Fatalf("node%d had no typed write in flight before restart", node+1)
	}
	t.Logf("restarting node%d with %d typed writes in flight", node+1, active[node].Load())
}

func TestAbruptProcessDeathRecoversCommittedRows(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "crash-recovery",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("crash-recovery-no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
	// Current-generation writer handles, refreshed after every restart
	// (Rust writers re-resolve the node endpoint after each restart; a
	// pre-restart handle only ever sees the old generation).
	var handleMu sync.Mutex
	handles := []*murmur.Database{cluster.Node(0), cluster.Node(1), cluster.Node(2)}
	getHandle := func(node int) *murmur.Database {
		handleMu.Lock()
		defer handleMu.Unlock()
		return handles[node]
	}
	setHandle := func(node int, db *murmur.Database) {
		handleMu.Lock()
		defer handleMu.Unlock()
		handles[node] = db
	}

	// Same schema on every node (Rust: typed fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).Migrate(murmur.NewMigration(tableDDL)); err != nil {
			t.Fatalf("migrate records table: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush schema: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("live_typed_records", true); err != nil {
			t.Fatalf("opt-in live_typed_records: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Baseline: one acknowledged write per node.
	var ackMu sync.Mutex
	acknowledged := make(map[string]bool)
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
	for node := 0; node < cluster.Len(); node++ {
		value := fmt.Sprintf("baseline-node%d", node+1)
		if !tryInsert(cluster.Node(node), value) {
			t.Fatalf("node%d baseline write failed", node+1)
		}
		if err := cluster.Node(node).Flush(); err != nil {
			t.Fatalf("flush baseline: %v", err)
		}
		ack(value)
	}
	waitForPeers(t, cluster, []int{2, 2, 2}, peersTimeout)
	if ok := harness.WaitUntil(peersTimeout, ackPoll, func() bool {
		return allNodesHaveAcknowledged(t, cluster, ackCopy())
	}); !ok {
		t.Fatalf("baseline writes did not converge within %v", peersTimeout)
	}

	// Background writers hammer all nodes until the end (2 workers per
	// node, 50 ms tick). In-flight counters gate each restart.
	var stop atomic.Bool
	var wg sync.WaitGroup
	// A failed assertion below must still release the writers so the
	// WaitGroup join cannot deadlock (t.Fatalf runs deferred calls).
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()
	var active [3]atomic.Int64
	var seqs [3]atomic.Int64
	for node := 0; node < cluster.Len(); node++ {
		for worker := 0; worker < workersPerNode; worker++ {
			wg.Add(1)
			go func(node, worker int) {
				defer wg.Done()
				for !stop.Load() {
					time.Sleep(writeTick)
					if stop.Load() {
						break
					}
					seq := seqs[node].Add(1)
					value := fmt.Sprintf("node%d-worker%d-write-%08d", node+1, worker, seq)
					active[node].Add(1)
					db := getHandle(node)
					ok := tryInsert(db, value)
					active[node].Add(-1)
					if ok {
						ack(value)
					} else {
						// Kill/restart and short startup windows can
						// refuse requests; the next unique insert
						// continues the workload.
						time.Sleep(refusedBackoff)
					}
				}
			}(node, worker)
		}
	}
	writeBurst := harness.EnvSeconds(writeEnv, defaultWriteSecs)

	// First crash window: burst, kill node 2 mid-write, burst, restart,
	// rejoin under continuing load.
	time.Sleep(writeBurst)
	killDuringWrites(t, &active, 1)
	cluster.StopNode(1)
	time.Sleep(writeBurst)
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart node2: %v", err)
	}
	setHandle(1, cluster.Node(1))
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	waitForPeers(t, cluster, []int{2, 2, 2}, rejoinTimeout)
	t.Log("node2 rejoined under continuing application load")

	// Second crash window on node 3.
	time.Sleep(writeBurst)
	killDuringWrites(t, &active, 2)
	cluster.StopNode(2)
	time.Sleep(writeBurst)
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("restart node3: %v", err)
	}
	setHandle(2, cluster.Node(2))
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	waitForPeers(t, cluster, []int{2, 2, 2}, rejoinTimeout)

	stop.Store(true)
	wg.Wait()

	waitForConvergence(t, cluster, harness.EnvSeconds(settleEnv, 60))
	guard := ackCopy()
	assertAcknowledgedPresent(t, cluster, guard)
	assertConverged(t, cluster)
	t.Logf("all acknowledged application writes survived two restarts (%d acknowledged rows)", len(guard))

	// A post-recovery write replicates everywhere.
	postCrashValue := "post-recovery-write"
	if !tryInsert(cluster.Node(0), postCrashValue) {
		t.Fatalf("post-recovery typed write failed")
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush post-recovery write: %v", err)
	}
	ack(postCrashValue)
	waitForConvergence(t, cluster, harness.EnvSeconds(settleEnv, 20))
	guard = ackCopy()
	assertAcknowledgedPresent(t, cluster, guard)
	assertConverged(t, cluster)
	failed = false
}
