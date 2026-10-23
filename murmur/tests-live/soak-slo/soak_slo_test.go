// Live scenario: three encrypted nodes sustain writes within SLOs.
//
// Go port of the Rust `soak-slo` scenario
// (/home/marc/MURMUR/tests-live/soak-slo/tests/soak_slo.rs). Three
// encrypted nodes sustain independent managed-record writes for
// MURMUR_SLO_DURATION_SECONDS (each node owns a bounded 1,000-row range:
// inserts until the range fills, then updates), then converge to an
// identical table digest within MURMUR_SLO_SETTLE_SECONDS. Write p95 must
// stay under 5 s, maximum write latency under 20 s, replication queues
// under 5,000 staged transactions, and spool disk usage under 512 MiB per
// node. Plain SQL replaces the typed-contention HTTP API.
package soakslo

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

const (
	// numNodes is the node count (Rust: 3).
	numNodes = 3
	// rowsPerNode is the bounded owned range per node (Rust: 1000).
	rowsPerNode = uint64(1000)
	// minOperations is the minimum committed writes mesh-wide (Rust: 60).
	minOperations = uint64(60)
	// queueSLO is the replication queue SLO per node (Rust: 5000 staged).
	queueSLO = 5000
	// spoolBudgetBytes is the spool disk budget per node (Rust: 512 MiB).
	spoolBudgetBytes = uint64(512 << 20)
	// p95SLO is the write p95 SLO (Rust: 5s).
	p95SLO = 5 * time.Second
	// maxSLO is the maximum write latency SLO (Rust: 20s).
	maxSLO = 20 * time.Second
	// peerTimeout is the mesh wait bound (Rust: 30s).
	peerTimeout = 30 * time.Second
	// quiesceTimeout is the materialization quiescence bound (Rust-only
	// settle; the original asserts instantly).
	quiesceTimeout = 30 * time.Second
	// poll is the convergence poll interval (Rust: 100ms).
	poll = 100 * time.Millisecond
)

// ddl is the contention table (Rust: contention(id, name, phone)).
const ddl = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL)"

// tryRowCount reads the row count on one node, reporting false when the
// read fails.
func tryRowCount(db *murmur.Database) (int, bool) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM contention")
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

// tryCanonicalRows reads the canonical encoded rows on one node (Rust:
// canonical_rows: `"id":"name":"phone"` sorted), reporting false when the
// read fails.
func tryCanonicalRows(db *murmur.Database) ([]string, bool) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name, phone FROM contention ORDER BY id")
	})
	if err != nil {
		return nil, false
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, fmt.Sprintf("%q:%q:%q", row[0].Str, row[1].Str, row[2].Str))
	}
	return out, true
}

// digestHex renders a deterministic log-only FNV-1a digest over canonical
// rows (Rust: digest_hex; comparisons use byte-identical vectors, which
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

// waitForPeers waits until every node holds a live session to the other
// two (Rust: wait_for_peers).
func waitForPeers(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	peersOK := harness.WaitUntil(peerTimeout, poll, func() bool {
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
		t.Fatalf("three-node mesh did not reach 2 connected peers per node within %v", peerTimeout)
	}
}

// TestSoakSLO sustains per-node writes for the soak duration, then requires
// identical convergence within every SLO gate (Rust: soak_slo).
func TestSoakSLO(t *testing.T) {
	duration := harness.EnvSeconds("MURMUR_SLO_DURATION_SECONDS", 5)
	settle := harness.EnvSeconds("MURMUR_SLO_SETTLE_SECONDS", 30)
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "soak-slo",
		NumNodes: numNodes,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("soak-slo-no-converge")
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
	waitForPeers(t, cluster)

	// One writer goroutine per node: bounded owned range, inserts while the
	// sequence is below the range size, updates afterwards (Rust: worker
	// threads with the same insert/update split and row-id shape).
	stopAt := time.Now().Add(duration)
	var writes [numNodes]atomic.Uint64
	var latMu sync.Mutex
	var latencies []time.Duration
	errCh := make(chan string, numNodes)
	var wg sync.WaitGroup
	for node := 0; node < numNodes; node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			db := cluster.Node(node)
			for {
				if !time.Now().Before(stopAt) {
					return
				}
				seq := writes[node].Load()
				slot := seq % rowsPerNode
				rowID := fmt.Sprintf("%032x", int64(node+1)*1_000_000_000_000+int64(slot)+1)
				value := fmt.Sprintf("node-%d-write-%09d", node+1, seq)
				started := time.Now()
				var outcome error
				if seq < rowsPerNode {
					name := fmt.Sprintf("node-%d", node+1)
					_, outcome = db.Transaction(func(s *murmur.Sql) (any, error) {
						_, err := s.Execute(
							"INSERT INTO contention(id, name, phone) VALUES(?, ?, ?)",
							rowID, name, value,
						)
						return nil, err
					})
				} else {
					var changed any
					changed, outcome = db.Transaction(func(s *murmur.Sql) (any, error) {
						return s.Execute("UPDATE contention SET phone = ? WHERE id = ?", value, rowID)
					})
					if outcome == nil && changed.(int64) != 1 {
						outcome = fmt.Errorf("update touched %d rows, want 1", changed.(int64))
					}
				}
				if outcome == nil {
					outcome = db.Flush()
				}
				latMu.Lock()
				latencies = append(latencies, time.Since(started))
				latMu.Unlock()
				if outcome != nil {
					errCh <- fmt.Sprintf("node%d write: %v", node+1, outcome)
					return
				}
				writes[node].Add(1)
			}
		}(node)
	}
	wg.Wait()
	close(errCh)
	var workerErrors []string
	for e := range errCh {
		workerErrors = append(workerErrors, e)
	}
	if len(workerErrors) != 0 {
		t.Fatalf("writer failures: %v", workerErrors)
	}

	expected := 0
	var operations uint64
	for i := 0; i < numNodes; i++ {
		w := writes[i].Load()
		operations += w
		if w < rowsPerNode {
			expected += int(w)
		} else {
			expected += int(rowsPerNode)
		}
	}
	if operations < minOperations {
		t.Fatalf("only %d application writes committed across %v", operations, duration)
	}

	// Every node must hold exactly the owned rows with identical contents.
	digest := ""
	converged := harness.WaitUntil(settle, poll, func() bool {
		first, ok := tryCanonicalRows(cluster.Node(0))
		if !ok || len(first) != expected {
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
		for i := 0; i < numNodes; i++ {
			n, ok := tryRowCount(cluster.Node(i))
			t.Logf("%s count=%d ok=%v", cluster.NodeInfo(i).Label, n, ok)
		}
		t.Fatalf("three application processes did not converge to %d rows within %v", expected, settle)
	}

	// Per-node SLO gates (Rust: ready state, materialized generation,
	// queue depth, spool disk usage).
	for i := 0; i < numNodes; i++ {
		label := cluster.NodeInfo(i).Label
		db := cluster.Node(i)
		h := db.Replication()
		if h == nil || !h.Status().Running {
			t.Fatalf("%s replication is not running", label)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("%s flush before materialization check: %v", label, err)
		}
		quiesced := harness.WaitUntil(quiesceTimeout, poll, func() bool {
			s := db.Status()
			return s.StorageError == nil && s.PendingBytes == 0 && s.Committed == s.Durable
		})
		if !quiesced {
			t.Fatalf("%s state has not been fully materialized", label)
		}
		if staged := h.Status().StagedTransactions; staged > queueSLO {
			t.Fatalf("%s exceeded queue SLO: staged=%d", label, staged)
		}
		if diskBytes := db.SpoolMetrics().DiskBytes; diskBytes > spoolBudgetBytes {
			t.Fatalf("%s Spool size %d exceeds 512 MiB SLO", label, diskBytes)
		}
	}

	if len(latencies) == 0 {
		t.Fatalf("no write latencies recorded across %v", duration)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	// Same nearest-rank p95 position as the original.
	p95 := latencies[(95*len(latencies)+99)/100-1]
	maxLatency := latencies[len(latencies)-1]
	t.Logf("multi-process encrypted write soak duration=%v rows=%d operations=%d digest=%s writes_per_node=%d/%d/%d write_p95=%v max=%v",
		duration, expected, operations, digest,
		writes[0].Load(), writes[1].Load(), writes[2].Load(), p95, maxLatency)
	if p95 > p95SLO {
		t.Fatalf("write p95 %v exceeded 5s SLO", p95)
	}
	if maxLatency > maxSLO {
		t.Fatalf("maximum write latency %v exceeded 20s SLO", maxLatency)
	}

	failed = false
}
