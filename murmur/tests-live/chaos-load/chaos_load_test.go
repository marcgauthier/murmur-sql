// Live scenario: continuous writes across a partition and healing.
//
// Go port of the Rust `chaos-load` scenario
// (/home/marc/MURMUR/tests-live/chaos-load/tests/chaos_load.rs). Three nodes
// write concurrently while node3 is cut off from the node1/node2 pair; the
// test proves divergent row counts and ordered state digests, then re-admits
// peers while writes continue, waits for full count/digest convergence, and
// commits a post-heal write. Plain SQL replaces the Go typed-records HTTP API
// of the original.
package chaosload

import (
	"fmt"
	"slices"
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
	// pollInterval is the convergence poll cadence (Rust: 50 ms, 100 ms
	// for full-cluster counts).
	pollInterval = 50 * time.Millisecond
	// writeInterval is the per-writer insert cadence (Rust: 25 ms ticker).
	writeInterval = 25 * time.Millisecond
	// baselineTimeout is the baseline convergence deadline (Rust: 20 s).
	baselineTimeout = 20 * time.Second
	// splitPeersTimeout / healPeersTimeout are the split/heal peer-mesh
	// deadlines (Rust: 20 s / 30 s).
	splitPeersTimeout = 20 * time.Second
	healPeersTimeout  = 30 * time.Second
	// splitCountTimeout is the per-node count deadline during the split
	// (Rust: 15 s).
	splitCountTimeout = 15 * time.Second
	// healCountTimeout is the full convergence deadline after healing
	// (Rust: 60 s).
	healCountTimeout = 60 * time.Second
	// postHealTimeout is the post-heal write deadline (Rust: 20 s).
	postHealTimeout = 20 * time.Second
	// drainTimeout is the measurement-barrier drain deadline (Rust: 5 s).
	drainTimeout = 5 * time.Second
)

// idStride keeps the three concurrent writers collision-free: ids are
// node*idStride + seq (Rust: server-generated typed IDs).
const idStride = int64(10_000_000)

// baselineID and postHealID are marker ids outside every writer range.
const (
	baselineID = int64(0)
	postHealID = int64(99_000_001)
)

// names returns all record names on one node (Rust: TypedNames).
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records")
	})
	if err != nil {
		t.Fatalf("read record names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// namesDigest is the canonical digest of a name set (Rust: sorted then
// joined).
func namesDigest(nodeNames []string) string {
	sorted := slices.Clone(nodeNames)
	slices.Sort(sorted)
	return strings.Join(sorted, "\n")
}

// insert writes one uniquely-named row (Rust: TypedInsert).
func insert(cluster *harness.Cluster, node int, id int64, name string) error {
	if _, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		return fmt.Errorf("node%d insert %q failed: %w", node+1, name, err)
	}
	if err := cluster.Node(node).Flush(); err != nil {
		return fmt.Errorf("node%d flush after %q failed: %w", node+1, name, err)
	}
	return nil
}

// connectedPeers counts live QUIC sessions on one node (Rust:
// /v1/status connected_peers).
func connectedPeers(cluster *harness.Cluster, node int) int {
	h := cluster.Node(node).Replication()
	if h == nil {
		return 0
	}
	n := 0
	for _, d := range h.Status().PeersDiagnostics {
		if d.Connected {
			n++
		}
	}
	return n
}

// waitAllCount waits until every node holds exactly expected rows
// (Rust: waitAllCount).
func waitAllCount(t *testing.T, cluster *harness.Cluster, expected int, timeout time.Duration, what string) {
	t.Helper()
	ok := harness.WaitUntil(timeout, 100*time.Millisecond, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if len(names(t, cluster.Node(i))) != expected {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("cluster failed to converge to %d rows within %v (%s)", expected, timeout, what)
	}
}

// waitNodeCount waits until one node holds exactly expected rows
// (Rust: waitNodeCount).
func waitNodeCount(t *testing.T, cluster *harness.Cluster, node, expected int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, pollInterval, func() bool {
		return len(names(t, cluster.Node(node))) == expected
	})
	if !ok {
		t.Fatalf("node%d rows=%d within %v, want %d", node+1, len(names(t, cluster.Node(node))), timeout, expected)
	}
}

// waitForPeers waits until every node reports its expected live-peer count
// (Rust: waitForPeers).
func waitForPeers(t *testing.T, cluster *harness.Cluster, want []int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, pollInterval, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if connectedPeers(cluster, i) != want[i] {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("peer mesh failed to reach %v within %v", want, timeout)
	}
}

// pauseAndDrain freezes writers and waits for in-flight inserts to finish
// (Rust: pauseAndDrain).
func pauseAndDrain(t *testing.T, paused *atomic.Bool, active *atomic.Int64) {
	t.Helper()
	paused.Store(true)
	ok := harness.WaitUntil(drainTimeout, time.Millisecond, func() bool {
		return active.Load() == 0
	})
	if !ok {
		t.Fatalf("%d application writes still active at measurement barrier", active.Load())
	}
}

// assertConverged asserts identical row counts and digests on all nodes
// (Rust: assertConverged).
func assertConverged(t *testing.T, cluster *harness.Cluster, wantRows int) {
	t.Helper()
	digest := ""
	for i := 0; i < cluster.Len(); i++ {
		nodeNames := names(t, cluster.Node(i))
		if len(nodeNames) != wantRows {
			t.Fatalf("node%d rows=%d, want %d", i+1, len(nodeNames), wantRows)
		}
		got := namesDigest(nodeNames)
		if digest == "" {
			digest = got
		} else if got != digest {
			t.Fatalf("node%d digest differs from node1", i+1)
		}
	}
}

func TestContinuousWritesAcrossPartitionAndHealing(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{Name: "chaos-load", NumNodes: 3})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	// Writers must stop before forensics/finish even when the test fails
	// (t.Fatalf runs deferred calls via Goexit).
	var stop atomic.Bool
	var wg sync.WaitGroup
	failed := true
	defer func() {
		stop.Store(true)
		wg.Wait()
		if failed {
			cluster.DumpForensics("chaos-load-no-convergence")
		}
		cluster.Finish(failed)
	}()

	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
	// Same schema on every node (Rust: TypedRecords fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
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
	if err := insert(cluster, 0, baselineID, "baseline"); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}
	waitAllCount(t, cluster, 1, baselineTimeout, "baseline")

	// Split {node1,node2} from {node3} by removing every cross-partition
	// relationship in both directions. Keep the surviving pair connected.
	for _, pair := range [][2]int{{0, 2}, {2, 0}, {1, 2}, {2, 1}} {
		if err := cluster.RemovePeer(pair[0], pair[1]); err != nil {
			t.Fatalf("remove peer node%d -> node%d: %v", pair[0]+1, pair[1]+1, err)
		}
	}
	waitForPeers(t, cluster, []int{1, 1, 0}, splitPeersTimeout)

	var paused atomic.Bool
	var active atomic.Int64
	var writes [3]atomic.Int64
	// Rust records write failures via MarkFailed and keeps going; the Go
	// writers collect them here and the test fails after they join.
	var writeMu sync.Mutex
	var writeErrors []string

	for node := 0; node < cluster.Len(); node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			var seq int64
			for !stop.Load() {
				time.Sleep(writeInterval)
				if stop.Load() {
					return
				}
				active.Add(1)
				if paused.Load() {
					active.Add(-1)
					continue
				}
				seq++
				name := fmt.Sprintf("node%d-write-%06d", node+1, seq)
				if err := insert(cluster, node, int64(node)*idStride+seq, name); err != nil {
					writeMu.Lock()
					writeErrors = append(writeErrors, err.Error())
					writeMu.Unlock()
				} else {
					writes[node].Add(1)
				}
				active.Add(-1)
			}
		}(node)
	}

	partitionWindow := harness.EnvSeconds("MURMUR_CHAOS_PARTITION_SECONDS", 12)
	healWindow := harness.EnvSeconds("MURMUR_CHAOS_HEAL_SECONDS", 12)
	time.Sleep(partitionWindow)
	pauseAndDrain(t, &paused, &active)
	left := 1 + int(writes[0].Load()+writes[1].Load())
	right := 1 + int(writes[2].Load())
	if left < 2 || right < 1 {
		t.Fatalf("insufficient partition writes: left=%d right=%d", left, right)
	}
	waitNodeCount(t, cluster, 0, left, splitCountTimeout)
	waitNodeCount(t, cluster, 1, left, splitCountTimeout)
	waitNodeCount(t, cluster, 2, right, splitCountTimeout)
	leftDigest := namesDigest(names(t, cluster.Node(0)))
	rightDigest := namesDigest(names(t, cluster.Node(2)))
	if rightDigest == leftDigest {
		t.Fatal("partitioned groups unexpectedly have identical state digests")
	}
	if got := connectedPeers(cluster, 2); got != 0 {
		t.Fatalf("isolated node3 has %d connected peers, want 0", got)
	}
	t.Logf("partition held for %v with divergent states: node1/node2=%d rows, node3=%d rows", partitionWindow, left, right)

	// Re-admit the cut edges while all application writers run again.
	paused.Store(false)
	for _, pair := range [][2]int{{0, 2}, {2, 0}, {1, 2}, {2, 1}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			t.Fatalf("add peer node%d -> node%d: %v", pair[0]+1, pair[1]+1, err)
		}
	}
	waitForPeers(t, cluster, []int{2, 2, 2}, healPeersTimeout)
	time.Sleep(healWindow)
	pauseAndDrain(t, &paused, &active)
	stop.Store(true)
	wg.Wait()

	writeMu.Lock()
	errs := slices.Clone(writeErrors)
	writeMu.Unlock()
	if len(errs) != 0 {
		t.Fatalf("application write errors: %v", errs)
	}

	total := 1 + int(writes[0].Load()+writes[1].Load()+writes[2].Load())
	waitAllCount(t, cluster, total, healCountTimeout, "post-heal")
	assertConverged(t, cluster, total)
	t.Logf("healed with %d writes; all nodes converge", total-1)

	// A new application write after reconciliation must still traverse
	// the healed mesh.
	if err := insert(cluster, 1, postHealID, "post-heal"); err != nil {
		t.Fatalf("post-heal write: %v", err)
	}
	waitAllCount(t, cluster, total+1, postHealTimeout, "post-heal write")
	assertConverged(t, cluster, total+1)
	failed = false
}
