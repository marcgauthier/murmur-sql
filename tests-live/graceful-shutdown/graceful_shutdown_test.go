// Live scenario: graceful shutdown mid-write under load and mid-snapshot
// during rejoin.
//
// Go port of tests/graceful_shutdown.rs using tests-live/gosqlharness.
// Identical behavior: phase A stops node 1 mid-write under concurrent
// client load with SIGTERM; phase B restarts node 3 after retention log
// GC (forcing snapshot resync) and stops it again mid-transfer. Both
// restarts must be clean and fast, lose no acknowledged writes, and the
// cluster must reconverge. The Rust toml_edit closure becomes SetConfig
// section writes, tokio tasks become goroutines, and the libc SIGTERM
// becomes a runtime.GOOS-guarded process signal.
package gracefulshutdown

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

const tableDDL = "CREATE TABLE IF NOT EXISTS contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL DEFAULT '')"

func rowID(node int, seq uint64) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", node, seq)
}

// ackedSet is the concurrent acknowledged-write registry (Rust:
// Arc<tokio::sync::Mutex<HashSet<String>>>).
type ackedSet struct {
	mu sync.Mutex
	m  map[string]bool
}

func (s *ackedSet) insert(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[name] = true
}

func (s *ackedSet) snapshot() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]bool, len(s.m))
	for k, v := range s.m {
		out[k] = v
	}
	return out
}

func waitConverged(t *testing.T, cluster *gosqlharness.Cluster, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	query := "SELECT name FROM contention ORDER BY name"
	for {
		r0, e0 := cluster.Node(0).Simple(query)
		r1, e1 := cluster.Node(1).Simple(query)
		r2, e2 := cluster.Node(2).Simple(query)
		if e0 == nil && e1 == nil && e2 == nil && len(r0) > 0 && equalRows(r0, r1) && equalRows(r1, r2) {
			out := make([]string, len(r0))
			for i, row := range r0 {
				out[i] = row[0]
			}
			return out
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("cluster failed to converge within %v", timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitPeerGone waits until none of watchers lists goneID in
// MURMUR SHOW PEERS. SIGKILLed nodes are noticed via QUIC idle timeout
// (30s), and a lingering session keeps the dead peer's unacked range
// protected past maximum_offline by design, so pruning assertions must
// await teardown, not just record staleness.
func waitPeerGone(t *testing.T, cluster *gosqlharness.Cluster, watchers []int, goneID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		present := false
		for _, i := range watchers {
			if rows, err := cluster.Node(i).Simple("MURMUR SHOW PEERS"); err == nil {
				for _, row := range rows {
					if len(row) > 0 && row[0] == goneID {
						present = true
					}
				}
			}
		}
		if !present {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("peer %s session never dropped", goneID)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func assertAcknowledgedPresent(t *testing.T, present []string, acknowledged map[string]bool) {
	t.Helper()
	set := make(map[string]bool, len(present))
	for _, p := range present {
		set[p] = true
	}
	for ack := range acknowledged {
		if !set[ack] {
			t.Fatalf("acknowledged write %q missing from converged cluster state", ack)
		}
	}
}

func TestGracefulShutdownMidWriteAndMidSnapshot(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "graceful-shutdown",
		NumNodes: 3,
		ReplicationYAML: `
retention: 1s
minimum_retained: 10
maximum_offline: 5s
`,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	// Configure trusted_snapshot_sources so rejoining nodes accept snapshots from peers.
	for i := 0; i < cluster.Len(); i++ {
		var sources []string
		for j := 0; j < cluster.Len(); j++ {
			if j != i {
				sources = append(sources, cluster.NodeInfo(j).NodeID)
			}
		}
		if err := cluster.StopNode(i); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
		if err := cluster.SetConfig(i, "replication", map[string]any{"trusted_snapshot_sources": sources}); err != nil {
			t.Fatalf("edit config node %d: %v", i, err)
		}
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
	}

	// Initialize replicated table on all 3 nodes.
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple(tableDDL); err != nil {
			t.Fatalf("node %d migrate: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE contention REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush schema: %v", i, err)
		}
	}

	// Baseline: 5 rows converge across all 3 nodes before any shutdown.
	acknowledged := map[string]bool{}
	var expectedBaseline [][]string
	for i := 0; i < 5; i++ {
		id := rowID(0, uint64(i))
		name := fmt.Sprintf("base-%d", i)
		if _, err := cluster.Node(0).Execute(
			"INSERT INTO contention(id, name, phone) VALUES($1, $2, $3)", id, name, ""); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
		acknowledged[name] = true
		expectedBaseline = append(expectedBaseline, []string{name})
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	sort.Slice(expectedBaseline, func(a, b int) bool { return expectedBaseline[a][0] < expectedBaseline[b][0] })
	if err := gosqlharness.AssertRows(cluster, []int{0, 1, 2},
		"SELECT name FROM contention ORDER BY name", expectedBaseline); err != nil {
		t.Fatalf("baseline converge: %v", err)
	}

	// Phase A: concurrent client writes on all nodes; stop node 0 under
	// load while other nodes write.
	var stop atomic.Bool
	var paused [3]atomic.Bool
	var inFlight atomic.Int64
	acked := &ackedSet{m: map[string]bool{}}
	for k, v := range acknowledged {
		acked.m[k] = v
	}

	var wg sync.WaitGroup
	for nodeIdx := 0; nodeIdx < 3; nodeIdx++ {
		client, err := cluster.Connect(nodeIdx)
		if err != nil {
			t.Fatalf("connect node %d writer: %v", nodeIdx, err)
		}
		defer client.Close()
		wg.Add(1)
		go func(nodeIdx int, client *goharness.Client) {
			defer wg.Done()
			var seq uint64
			for !stop.Load() {
				if paused[nodeIdx].Load() {
					time.Sleep(20 * time.Millisecond)
					continue
				}
				seq++
				id := rowID(nodeIdx+1, seq)
				name := fmt.Sprintf("load-n%d-%06d", nodeIdx, seq)
				inFlight.Add(1)
				_, resErr := client.Execute(
					"INSERT INTO contention(id, name, phone) VALUES($1, $2, $3)", id, name, "")
				inFlight.Add(-1)
				if resErr == nil {
					acked.insert(name)
				}
				time.Sleep(15 * time.Millisecond)
			}
		}(nodeIdx, client)
	}

	// Let writes get in flight.
	time.Sleep(300 * time.Millisecond)

	// Pause node 0 writer and drain pending writes.
	paused[0].Store(true)
	time.Sleep(100 * time.Millisecond)
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush node 0: %v", err)
	}

	// Send SIGTERM to node 0 for graceful shutdown (Rust: #[cfg(unix)]).
	if runtime.GOOS != "windows" {
		if pid, ok := cluster.PID(0); ok {
			if proc, err := os.FindProcess(pid); err == nil {
				_ = proc.Signal(syscall.SIGTERM)
			}
		}
	}

	// Allow graceful flush and exit before reaping.
	time.Sleep(200 * time.Millisecond)
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node 0: %v", err)
	}

	// Nodes 1 and 2 continue writing while node 0 is offline.
	time.Sleep(200 * time.Millisecond)

	// Restart node 0 cleanly.
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node 0: %v", err)
	}

	// Re-connect node 0 writer.
	client0New, err := cluster.Connect(0)
	if err != nil {
		t.Fatalf("reconnect node 0 writer: %v", err)
	}
	defer client0New.Close()
	wg.Add(1)
	go func() {
		defer wg.Done()
		var seq uint64 = 500
		for !stop.Load() {
			seq++
			id := rowID(1, seq)
			name := fmt.Sprintf("load-n0-postrestart-%06d", seq)
			if _, resErr := client0New.Execute(
				"INSERT INTO contention(id, name, phone) VALUES($1, $2, $3)", id, name, ""); resErr == nil {
				acked.insert(name)
			}
			time.Sleep(15 * time.Millisecond)
		}
	}()

	// Let concurrent writes proceed across all nodes after restart.
	time.Sleep(300 * time.Millisecond)

	// Stop all phase A writers.
	stop.Store(true)
	wg.Wait()

	// Flush all nodes.
	for i := 0; i < 3; i++ {
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("flush node %d: %v", i, err)
		}
	}

	// Verify all 3 nodes converge with identical contents, and every
	// acknowledged write is present.
	ackedSnapshot := acked.snapshot()
	convergedA := waitConverged(t, cluster, 60*time.Second)
	assertAcknowledgedPresent(t, convergedA, ackedSnapshot)

	// Phase B: node 2 goes stale under aggressive retention while
	// survivors write bulk + fat payload.
	if err := cluster.StopNode(2); err != nil {
		t.Fatalf("stop node 2: %v", err)
	}
	phaseBAcked := map[string]bool{}
	for k, v := range ackedSnapshot {
		phaseBAcked[k] = v
	}
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("fresh-%03d", i)
		id := rowID(1, 5000+uint64(i))
		if _, err := cluster.Node(0).Execute(
			"INSERT INTO contention(id, name, phone) VALUES($1, $2, $3)", id, name, ""); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
		phaseBAcked[name] = true
	}

	// 8 fat rows (~64 KB each) to widen the transfer window.
	fatChunk := strings.Repeat("FAT-FILLER-DATA-STRING-PADDING-1234567890\n", 1500)
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("fat-%02d", i)
		id := rowID(2, 6000+uint64(i))
		if _, err := cluster.Node(1).Execute(
			"INSERT INTO contention(id, name, phone) VALUES($1, $2, $3)", id, name, fatChunk); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
		phaseBAcked[name] = true
	}

	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush node 0: %v", err)
	}
	if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush node 1: %v", err)
	}

	var expectedSurvivors [][]string
	for n := range phaseBAcked {
		expectedSurvivors = append(expectedSurvivors, []string{n})
	}
	sort.Slice(expectedSurvivors, func(a, b int) bool { return expectedSurvivors[a][0] < expectedSurvivors[b][0] })
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT name FROM contention ORDER BY name", expectedSurvivors); err != nil {
		t.Fatalf("survivors converge: %v", err)
	}

	// Node 2's session must drop before its unacknowledged logs unpin.
	node2ID := cluster.NodeInfo(2).NodeID
	waitPeerGone(t, cluster, []int{0, 1}, node2ID, 60*time.Second)

	// Run retention collection passes on both survivors to prune log history.
	c0, err := cluster.Node(0).Simple("MURMUR RETENTION COLLECT")
	if err != nil {
		t.Fatalf("collect node 0: %v", err)
	}
	c1, err := cluster.Node(1).Simple("MURMUR RETENTION COLLECT")
	if err != nil {
		t.Fatalf("collect node 1: %v", err)
	}
	collected0, _ := strconv.ParseInt(c0[0][0], 10, 64)
	collected1, _ := strconv.ParseInt(c1[0][0], 10, 64)
	if collected0+collected1 <= 0 {
		t.Fatal("retention collection pass collected 0 batches; expected log pruning")
	}

	// Restart stale node 2: it must take the snapshot path because its log position was pruned.
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("restart stale node 2: %v", err)
	}

	// Interrupt node 2 mid-transfer: brief delay then stop.
	time.Sleep(25 * time.Millisecond)
	if err := cluster.StopNode(2); err != nil {
		t.Fatalf("interrupt node 2: %v", err)
	}

	// Restart node 2 again after interrupt.
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("restart node 2 after interrupt: %v", err)
	}

	// All nodes must reconverge on the full dataset with no missing rows.
	convergedB := waitConverged(t, cluster, 60*time.Second)
	assertAcknowledgedPresent(t, convergedB, phaseBAcked)

	// Post-recovery write on node 0 replicates to all nodes.
	postName := "post-recovery-write"
	postID := rowID(1, 999999)
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO contention(id, name, phone) VALUES($1, $2, $3)", postID, postName, ""); err != nil {
		t.Fatalf("post-recovery insert: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush post-recovery: %v", err)
	}

	expectedFinal := append(append([][]string(nil), expectedSurvivors...), []string{postName})
	sort.Slice(expectedFinal, func(a, b int) bool { return expectedFinal[a][0] < expectedFinal[b][0] })
	if err := gosqlharness.AssertRows(cluster, []int{0, 1, 2},
		"SELECT name FROM contention ORDER BY name", expectedFinal); err != nil {
		t.Fatalf("final converge: %v", err)
	}
	failed = false
}

func equalRows(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}
