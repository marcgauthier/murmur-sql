// Live scenario: five nodes soak writes across restarts within a 2 GiB
// persistent capacity budget.
//
// Go port of the Rust `long-running-five-node` scenario
// (/home/marc/MURMUR/tests-live/long-running-five-node/tests/long_running_five_node.rs).
// Five nodes run a continuous per-node write soak across a full mesh while
// node2 and node4 restart in place mid-run; persisted rows must survive each
// reopen, all five nodes must converge to the same row set, every node must
// be fully materialized within a 2 GiB spool budget, and one final
// application write must converge. Plain SQL replaces the typed-records
// HTTP API.
package longrunningfivenode

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// numNodes is the node count (Rust: 5).
	numNodes = 5
	// minRowsPerNode is the minimum acknowledged rows per node (Rust: 10).
	minRowsPerNode = uint64(10)
	// minRowsTotal is the minimum acknowledged rows mesh-wide (Rust: 100).
	minRowsTotal = uint64(100)
	// spoolBudgetBytes is the persistent capacity budget per node (Rust:
	// 2 GiB).
	spoolBudgetBytes = uint64(2 << 30)
	// peerTimeout is the mesh wait bound (Rust: 45s).
	peerTimeout = 45 * time.Second
	// persistTimeout is the post-restart persistence poll bound (Rust: 30s).
	persistTimeout = 30 * time.Second
	// quiesceTimeout is the materialization quiescence bound (Rust-only
	// settle; the original asserts instantly).
	quiesceTimeout = 30 * time.Second
	// topoPoll is the peer/persistence poll interval (Rust: 100ms).
	topoPoll = 100 * time.Millisecond
	// convergePoll is the convergence poll interval (Rust: 200ms).
	convergePoll = 200 * time.Millisecond
	// postWritePoll is the post-write convergence poll interval (Rust:
	// 100ms).
	postWritePoll = 100 * time.Millisecond
)

// restartNodes are the zero-based restart targets (Rust: nodes 1 and 3,
// i.e. node2 and node4).
var restartNodes = [2]int{1, 3}

// ddl is the records table (Rust: records(id, name)).
const ddl = "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)"

// envMillis reads a positive-integer milliseconds env knob with fallback
// (Rust: env_millis).
func envMillis(name string, fallbackMs uint64) time.Duration {
	if v, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil && v > 0 {
		return time.Duration(v) * time.Millisecond
	}
	return time.Duration(fallbackMs) * time.Millisecond
}

// sleepUntil sleeps until deadline, returning immediately when it already
// passed (Rust: sleep_until).
func sleepUntil(deadline time.Time) {
	if d := time.Until(deadline); d > 0 {
		time.Sleep(d)
	}
}

// tryRowCount reads the row count on one node, reporting false when the
// read fails.
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

// tryCanonicalRows reads the canonical ordered names on one node (Rust:
// tryCanonicalRows), reporting false when the read fails.
func tryCanonicalRows(db *murmur.Database) ([]string, bool) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records ORDER BY name")
	})
	if err != nil {
		return nil, false
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out, true
}

// digestHex renders a deterministic log-only FNV-1a digest over sorted
// names (Rust: digest_hex; comparisons use byte-identical vectors, which
// imply identical digests).
func digestHex(rows []string) string {
	h := uint64(0xcbf29ce484222325)
	for _, row := range rows {
		for i := 0; i < len(row); i++ {
			h ^= uint64(row[i])
			h *= 0x100000001b3
		}
		h ^= uint64('\n')
		h *= 0x100000001b3
	}
	return fmt.Sprintf("%016x", h)
}

// peerConnected probes one directed pair for a live session (Rust:
// peer_connected).
func peerConnected(cluster *harness.Cluster, node int, peerID [16]byte) bool {
	h := cluster.Node(node).Replication()
	if h == nil {
		return false
	}
	for _, d := range h.Status().PeersDiagnostics {
		if d.NodeID == peerID && d.Connected {
			return true
		}
	}
	return false
}

// peerCount reads the live-session count on one node (for failure logs).
func peerCount(cluster *harness.Cluster, node int) int {
	h := cluster.Node(node).Replication()
	if h == nil {
		return -1
	}
	return h.Status().Peers
}

// waitForPeers waits until every node holds a live session to the other
// four (Rust: wait_for_peers).
func waitForPeers(t *testing.T, cluster *harness.Cluster, what string) {
	t.Helper()
	peersOK := harness.WaitUntil(peerTimeout, topoPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			for j := 0; j < cluster.Len(); j++ {
				if i != j && !peerConnected(cluster, i, cluster.NodeID(j)) {
					return false
				}
			}
		}
		return true
	})
	if !peersOK {
		t.Fatalf("five-node mesh did not reach 4 connected peers per node within %v (%s)", peerTimeout, what)
	}
}

// waitForRowCount waits until every node holds exactly count rows with
// identical contents (Rust: wait_for_row_count); returns the shared digest.
func waitForRowCount(t *testing.T, cluster *harness.Cluster, count int, timeout time.Duration) string {
	t.Helper()
	digest := ""
	converged := harness.WaitUntil(timeout, postWritePoll, func() bool {
		first, ok := tryCanonicalRows(cluster.Node(0))
		if !ok || len(first) != count {
			return false
		}
		for i := 1; i < numNodes; i++ {
			other, ok := tryCanonicalRows(cluster.Node(i))
			if !ok || len(other) != len(first) {
				return false
			}
			for j := range first {
				if other[j] != first[j] {
					return false
				}
			}
		}
		digest = digestHex(first)
		return true
	})
	if !converged {
		t.Fatalf("post-soak write did not converge to %d rows", count)
	}
	return digest
}

// TestLongRunningFiveNode soaks per-node writes across two in-place
// restarts, then requires identical convergence within budget (Rust:
// long_running_five_node).
func TestLongRunningFiveNode(t *testing.T) {
	duration := harness.EnvSeconds("MURMUR_FIVE_NODE_DURATION_SECONDS", 30)
	interval := envMillis("MURMUR_FIVE_NODE_INTERVAL_MS", 50)
	settle := harness.EnvSeconds("MURMUR_FIVE_NODE_SETTLE_SECONDS", 120)
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "long-running-five-node",
		NumNodes: numNodes,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("five-node-soak-no-converge")
		}
		cluster.Finish(failed)
	}()

	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("Node %s running in directory %s (repl=%s)", info.Label, info.Dir, info.ReplAddr)
	}

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
	waitForPeers(t, cluster, "initial mesh")

	start := time.Now()
	firstRestartAt := start.Add(duration / 3)
	secondRestartAt := start.Add(2 * duration / 3)
	stopAt := start.Add(duration)

	// Writer goroutines share a handle table with the main goroutine: the
	// entry for a restarted node is swapped for the reopened database
	// (Rust: writers address daemons by stable address across restarts).
	// The RWMutex additionally fences writer ops against close/reopen so
	// no op runs on a half-restarted handle. A committed transaction
	// counts as acknowledged (Rust: writes); failed transactions and
	// failed flushes count as transient errors (Rust: failures), which the
	// restart windows are expected to produce on the two restarted nodes.
	var handlesMu sync.RWMutex
	handles := make([]*murmur.Database, numNodes)
	for i := 0; i < numNodes; i++ {
		handles[i] = cluster.Node(i)
	}
	var writes [numNodes]atomic.Uint64
	var failures [numNodes]atomic.Uint64
	var stop atomic.Bool

	var wg sync.WaitGroup
	for node := 0; node < numNodes; node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			for {
				if stop.Load() {
					return
				}
				time.Sleep(interval)
				if stop.Load() || !time.Now().Before(stopAt) {
					return
				}
				// Only acknowledged writes advance the sequence, so ids
				// stay dense per node (Rust: seq := writes[node]).
				seq := writes[node].Load()
				id := fmt.Sprintf("n%d-%09d", node+1, seq)
				name := fmt.Sprintf("node-%d-capacity-%09d", node+1, seq)
				handlesMu.RLock()
				db := handles[node]
				_, txErr := db.Transaction(func(s *murmur.Sql) (any, error) {
					_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
					return nil, err
				})
				var flushErr error
				if txErr == nil {
					flushErr = db.Flush()
				}
				handlesMu.RUnlock()
				if txErr != nil {
					failures[node].Add(1)
					continue
				}
				writes[node].Add(1)
				if flushErr != nil {
					failures[node].Add(1)
				}
			}
		}(node)
	}

	restartAndCheckPersistence := func(node int) {
		label := fmt.Sprintf("node%d", node+1)
		started := time.Now()
		handlesMu.RLock()
		before, ok := tryRowCount(handles[node])
		handlesMu.RUnlock()
		if !ok {
			t.Fatalf("%s pre-restart row count unreadable", label)
		}
		handlesMu.Lock()
		cluster.StopNode(node)
		if err := cluster.StartNode(node); err != nil {
			handlesMu.Unlock()
			t.Fatalf("%s restart failed: %v", label, err)
		}
		// The reopened node rebinds a fresh ephemeral port: re-mesh so
		// every ordered pair dials the current addresses.
		if err := cluster.Mesh(); err != nil {
			handlesMu.Unlock()
			t.Fatalf("re-mesh after restart: %v", err)
		}
		handles[node] = cluster.Node(node)
		handlesMu.Unlock()
		waitForPeers(t, cluster, label+" rejoin")
		recovered := harness.WaitUntil(persistTimeout, topoPoll, func() bool {
			got, ok := tryRowCount(cluster.Node(node))
			return ok && got >= before
		})
		got, _ := tryRowCount(cluster.Node(node))
		if !recovered {
			t.Fatalf("%s lost durable rows after restart: got=%d before=%d", label, got, before)
		}
		t.Logf("%s restarted and rejoined in %v with %d rows (had %d before restart)", label, time.Since(started), got, before)
	}

	sleepUntil(firstRestartAt)
	restartAndCheckPersistence(restartNodes[0])
	sleepUntil(secondRestartAt)
	restartAndCheckPersistence(restartNodes[1])
	sleepUntil(stopAt)
	stop.Store(true)
	wg.Wait()

	totalAcked := 0
	for i := 0; i < numNodes; i++ {
		acked := writes[i].Load()
		totalAcked += int(acked)
		label := cluster.NodeInfo(i).Label
		t.Logf("%s acknowledged=%d transient_write_errors=%d", label, acked, failures[i].Load())
		if acked < minRowsPerNode {
			t.Fatalf("%s committed only %d rows during %v", label, acked, duration)
		}
	}
	if uint64(totalAcked) < minRowsTotal {
		t.Fatalf("only %d application writes committed across five nodes", totalAcked)
	}

	// Every node must reach the same row set with at least the acknowledged
	// rows (Rust: equal counts, equal digests, count >= acked).
	converged := harness.WaitUntil(settle, convergePoll, func() bool {
		first, ok := tryCanonicalRows(cluster.Node(0))
		if !ok || len(first) < totalAcked {
			return false
		}
		for i := 1; i < numNodes; i++ {
			other, ok := tryCanonicalRows(cluster.Node(i))
			if !ok || len(other) != len(first) {
				return false
			}
			for j := range first {
				if other[j] != first[j] {
					return false
				}
			}
		}
		return true
	})
	if !converged {
		for i := 0; i < numNodes; i++ {
			n, _ := tryRowCount(cluster.Node(i))
			t.Logf("%s count=%d peers=%d", cluster.NodeInfo(i).Label, n, peerCount(cluster, i))
		}
		t.Fatalf("five-node mesh did not converge to at least %d acknowledged rows within %v", totalAcked, settle)
	}
	finalRows, ok := tryCanonicalRows(cluster.Node(0))
	if !ok {
		t.Fatal("final canonical rows unreadable")
	}
	finalDigest := digestHex(finalRows)
	finalCount := len(finalRows)

	// Fully-materialized state within the persistent capacity budget.
	for i := 0; i < numNodes; i++ {
		label := cluster.NodeInfo(i).Label
		db := cluster.Node(i)
		if err := db.Flush(); err != nil {
			t.Fatalf("%s flush before materialization check: %v", label, err)
		}
		quiesced := harness.WaitUntil(quiesceTimeout, convergePoll, func() bool {
			s := db.Status()
			return s.StorageError == nil && s.PendingBytes == 0 && s.Committed == s.Durable
		})
		if !quiesced {
			t.Fatalf("%s state has not been fully materialized", label)
		}
		if diskBytes := db.SpoolMetrics().DiskBytes; diskBytes > spoolBudgetBytes {
			t.Fatalf("%s exceeded 2 GiB persistent capacity limit (%d bytes)", label, diskBytes)
		}
	}

	// One final application write must converge everywhere.
	if _, err := cluster.Node(numNodes - 1).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", "post-soak-write", "post-soak-write")
		return nil, err
	}); err != nil {
		t.Fatalf("post-soak application write: %v", err)
	}
	if err := cluster.Node(numNodes - 1).Flush(); err != nil {
		t.Fatalf("flush post-soak write: %v", err)
	}
	postDigest := waitForRowCount(t, cluster, finalCount+1, settle)
	t.Logf("five encrypted nodes held %v of load, survived two orderly data-directory restarts, and converged %d rows (digest=%s; before-post-write=%s)",
		duration, finalCount+1, postDigest, finalDigest)

	failed = false
}
