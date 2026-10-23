// Live scenario: a flapping partition loses no writes.
//
// Go port of the Rust `flap-partition` scenario
// (/home/marc/MURMUR/tests-live/flap-partition/tests/flap_partition.rs),
// itself a port of the original Go `flap-partition` scenario
// (TestFlappingPartitionLosesNoWrites). Three fully meshed nodes take
// continuous writes from all nodes while node3's links flap through
// split/heal cycles. The first split carries an isolation proof (a marker
// written on node1 must reach node2 but never node3); no write to a live
// node may fail, and the final heal must converge every written row with
// equal contents.
//
// Plain SQL replaces the managed typed-records HTTP API.
package flappartition

import (
	"fmt"
	"os"
	"sort"
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
	// pollInterval is the convergence poll interval (Rust: 200 ms).
	pollInterval = 200 * time.Millisecond
	// writeInterval is the per-writer insert cadence (Rust: 50 ms ticker).
	writeInterval = 50 * time.Millisecond
	// isolationPoll is the isolation-probe poll interval (Rust: 50 ms).
	isolationPoll = 50 * time.Millisecond
	// convergeTimeout is the baseline / final convergence deadline
	// (Rust: 150 s each).
	convergeTimeout = 150 * time.Second
	// isolationTimeout is the isolation marker propagation deadline
	// (Rust: 10 s).
	isolationTimeout = 10 * time.Second
	// leakWindow is the post-proof leak watch window (Rust: 1 s).
	leakWindow = time.Second
	// progressLogInterval is the progress-log cadence inside convergence
	// waits (Rust: 10 s).
	progressLogInterval = 10 * time.Second
	// baselineRows is the baseline row count on node 0 (Rust: 6).
	baselineRows = 6
)

const (
	// idStride is the row-id stride per writer node: ids are
	// node*idStride + seq, so the three concurrent writers never collide
	// (Rust: server-generated typed IDs).
	idStride = 10000000
	// baselineIDBase is the baseline id base, above every writer range.
	baselineIDBase = 90000001
	// isolationMarkerID is the isolation marker id, outside baseline and
	// writer ranges.
	isolationMarkerID = 90001001
	// isolationMarker is the isolation marker name (Rust:
	// `isolation-marker`).
	isolationMarker = "isolation-marker"
)

// ddl is the shared records table (Rust: TypedRecords fixture tables).
const ddl = "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"

// queryNames reads all record names on one node (Rust: TypedNames). Errors
// are returned so polls can treat them as not-yet-converged.
func queryNames(db *murmur.Database) ([]string, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records")
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

// mustNames reads all record names on one node, failing the test on error.
func mustNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	names, err := queryNames(db)
	if err != nil {
		t.Fatalf("read record names: %v", err)
	}
	return names
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

// hasName reports whether one node currently holds name (Rust: hasName).
func hasName(t *testing.T, cluster *harness.Cluster, idx int, name string) bool {
	t.Helper()
	for _, got := range mustNames(t, cluster.Node(idx)) {
		if got == name {
			return true
		}
	}
	return false
}

// setPartition isolates (true) or rejoins (false) node index 2 (Rust:
// setPartition).
func setPartition(t *testing.T, cluster *harness.Cluster, isolate bool) {
	t.Helper()
	for _, peer := range []int{0, 1} {
		if isolate {
			if err := cluster.RemovePeer(2, peer); err != nil {
				t.Fatalf("partition link 2<->%d (isolate=true): %v", peer+1, err)
			}
			if err := cluster.RemovePeer(peer, 2); err != nil {
				t.Fatalf("partition link %d<->2 (isolate=true): %v", peer+1, err)
			}
		} else {
			if err := cluster.AddPeer(2, peer); err != nil {
				t.Fatalf("partition link 2<->%d (heal): %v", peer+1, err)
			}
			if err := cluster.AddPeer(peer, 2); err != nil {
				t.Fatalf("partition link %d<->2 (heal): %v", peer+1, err)
			}
		}
	}
}

// proveIsolated writes a marker row on node 0 while node 2 is split off and
// proves the split is real: node 1 must observe it, node 2 must never
// observe it during the whole window (Rust: proveIsolated). The marker
// counts in written so the final want stays exact.
func proveIsolated(t *testing.T, cluster *harness.Cluster, written *atomic.Int64) {
	t.Helper()
	if err := insert(cluster, 0, isolationMarkerID, isolationMarker); err != nil {
		t.Fatalf("isolation marker write: %v", err)
	}
	written.Add(1)
	// Node 1 must see the marker: proves cross-node traffic flows on the
	// intact side while we watch node 2 for leaks.
	deadline := time.Now().Add(isolationTimeout)
	for {
		if hasName(t, cluster, 1, isolationMarker) {
			break
		}
		if hasName(t, cluster, 2, isolationMarker) {
			t.Fatalf("isolation violated: node3 observed the marker while split (remove_peer ineffective?)")
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("node2 never observed the isolation marker within %v", isolationTimeout)
		}
		time.Sleep(isolationPoll)
	}
	// Node 1 has it; give a leak one more second to show on node 2.
	time.Sleep(leakWindow)
	if hasName(t, cluster, 2, isolationMarker) {
		t.Fatalf("isolation violated: node3 observed the marker while split (remove_peer ineffective?)")
	}
	t.Log("isolation proven: marker reached node2, never node3")
}

// waitConverged waits until every node holds exactly want rows with equal
// ordered contents (Rust: waitConverged). Each node's names are sorted
// before the ordered comparison (SQL SELECT without ORDER BY does not
// guarantee order).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	for {
		ok := true
		var first []string
		for i := 0; i < cluster.Len(); i++ {
			nodeNames, err := queryNames(cluster.Node(i))
			if err != nil || len(nodeNames) != want {
				ok = false
				break
			}
			sort.Strings(nodeNames)
			if i == 0 {
				first = nodeNames
			} else if !equalStrings(nodeNames, first) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if !time.Now().Before(deadline) {
			break
		}
		if time.Since(lastLog) > progressLogInterval {
			lastLog = time.Now()
			var counts []int
			for i := 0; i < cluster.Len(); i++ {
				names, err := queryNames(cluster.Node(i))
				if err != nil {
					counts = append(counts, -1)
				} else {
					counts = append(counts, len(names))
				}
			}
			t.Logf("converge progress: counts=%v want=%d", counts, want)
		}
		time.Sleep(pollInterval)
	}
	for i := 0; i < cluster.Len(); i++ {
		t.Logf("node%d at timeout: count=%d", i+1, len(mustNames(t, cluster.Node(i))))
	}
	t.Fatalf("nodes did not converge on %d rows with equal contents within %v", want, timeout)
}

// envInt reads a positive-integer env knob with fallback (Rust: env_int).
func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

// TestFlappingPartitionLosesNoWrites flaps node3's partition through
// split/heal cycles under continuous writes on every node: no write to a
// live node may fail, and the final heal converges everything (Rust:
// flapping_partition_loses_no_writes).
func TestFlappingPartitionLosesNoWrites(t *testing.T) {
	cycles := envInt("MURMUR_FLAP_CYCLES", 5)
	split := harness.EnvSeconds("MURMUR_FLAP_SPLIT_SECONDS", 3)
	healGap := harness.EnvSeconds("MURMUR_FLAP_HEAL_SECONDS", 2)

	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "flap-partition",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("flap-partition-no-convergence")
		}
		cluster.Finish(failed)
	}()

	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
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

	// Baseline converges before flapping (honest-path control).
	for i := 0; i < baselineRows; i++ {
		if err := insert(cluster, 0, int64(baselineIDBase+i), "base-"+strconv.Itoa(i)); err != nil {
			t.Fatalf("baseline write: %v", err)
		}
	}
	waitConverged(t, cluster, baselineRows, convergeTimeout)

	// Continuous writers on every node; node3 stays live (running, only
	// unreachable), so its local commits must succeed too.
	var written, failedWrites atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup
	for node := 0; node < cluster.Len(); node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			var seq int64
			for !stop.Load() {
				time.Sleep(writeInterval)
				if stop.Load() {
					break
				}
				seq++
				name := "n" + strconv.Itoa(node) + "-s" + strconv.FormatInt(seq, 10)
				if err := insert(cluster, node, int64(node)*idStride+seq, name); err != nil {
					failedWrites.Add(1)
				} else {
					written.Add(1)
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

	// Flap node3's partition: split from both peers, hold, heal, settle.
	for c := 0; c < cycles; c++ {
		setPartition(t, cluster, true)
		t.Logf("cycle %d/%d: node3 isolated for %v", c+1, cycles, split)
		if c == 0 {
			// Isolation proof (F1): while split, a marker written on
			// node0 must reach node1 but never node2. Without this, a
			// no-op remove_peer would pass vacuously. The marker is
			// counted in written so the final want stays exact.
			proveIsolated(t, cluster, &written)
		}
		time.Sleep(split)
		setPartition(t, cluster, false)
		t.Logf("cycle %d/%d: healed, settling %v", c+1, cycles, healGap)
		time.Sleep(healGap)
	}
	stop.Store(true)
	wg.Wait()

	// Final heal (idempotent) then full convergence on everything written.
	setPartition(t, cluster, false)
	want := baselineRows + int(written.Load())
	t.Logf("flapping done: %d cycles, %d rows written, %d failed", cycles, want, failedWrites.Load())
	if failedWrites.Load() != 0 {
		t.Fatalf("partition failed %d writes to live nodes, want 0", failedWrites.Load())
	}
	waitConverged(t, cluster, want, convergeTimeout)
	t.Logf("post-flap convergence: %d rows, identical digests", want)
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
