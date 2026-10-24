// Live scenario: a flapping partition loses no writes.
//
// Go port of tests/flap_partition.rs using tests-live/goharness.
// Identical behavior: three fully meshed murmur-server processes take
// continuous writes on every node while node 3's links flap through
// split/heal cycles. The first split carries an isolation proof (a
// marker written on node 1 must reach node 2 but never node 3); no write
// to a live node may fail, and the final heal must converge every
// written row with equal contents. The Rust tokio::join! over writers +
// driver becomes a sync.WaitGroup over goroutines with the driver on the
// test goroutine.
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

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

const (
	// pollInterval is the convergence poll interval (Rust: 200 ms).
	pollInterval = 200 * time.Millisecond
	// writeInterval is the per-writer insert cadence (Rust: 50 ms).
	writeInterval = 50 * time.Millisecond
	// isolationPoll is the isolation-probe poll interval (Rust: 50 ms).
	isolationPoll = 50 * time.Millisecond
	// convergeTimeout bounds baseline / final convergence (Rust: 150 s).
	convergeTimeout = 150 * time.Second
	// isolationTimeout bounds marker propagation (Rust: 10 s).
	isolationTimeout = 10 * time.Second
	// leakWindow is the post-proof leak watch (Rust: 1 s).
	leakWindow = 1 * time.Second
	// progressLogInterval is the progress-log cadence (Rust: 10 s).
	progressLogInterval = 10 * time.Second
	// baselineRows is written on node 0 before flapping (Rust: 6).
	baselineRows = 6
	// idStride keeps the three concurrent writers collision-free.
	idStride int64 = 10_000_000
	// baselineIDBase sits above every writer range.
	baselineIDBase int64 = 90_000_001
	// isolationMarkerID is outside baseline and writer ranges.
	isolationMarkerID int64 = 90_001_001
	// isolationMarker is the split-proof row name.
	isolationMarker = "isolation-marker"
)

// names returns all record names on one node.
func names(cluster *goharness.Cluster, idx int) ([]string, error) {
	rows, err := cluster.Node(idx).Query("SELECT name FROM records")
	if err != nil {
		return nil, fmt.Errorf("node%d SELECT names: %w", idx+1, err)
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		name, err := row.String(0)
		if err != nil {
			return nil, fmt.Errorf("node%d decode name: %w", idx+1, err)
		}
		out = append(out, name)
	}
	return out, nil
}

// insert writes one uniquely-named row plus a durability barrier.
func insert(cluster *goharness.Cluster, node int, id int64, name string) error {
	if _, err := cluster.Node(node).Execute("INSERT INTO records(id, name) VALUES($1, $2)", id, name); err != nil {
		return fmt.Errorf("node%d insert '%s': %w", node+1, name, err)
	}
	if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after '%s': %w", node+1, name, err)
	}
	return nil
}

// hasName reports whether one node currently holds name.
func hasName(cluster *goharness.Cluster, idx int, name string) (bool, error) {
	got, err := names(cluster, idx)
	if err != nil {
		return false, err
	}
	for _, n := range got {
		if n == name {
			return true, nil
		}
	}
	return false, nil
}

// setPartition isolates (true) or rejoins (false) node index 2.
func setPartition(cluster *goharness.Cluster, isolate bool) error {
	for _, peer := range []int{0, 1} {
		if isolate {
			if err := cluster.RemovePeer(2, peer); err != nil {
				return fmt.Errorf("partition link 2<->%d (isolate=true): %w", peer+1, err)
			}
			if err := cluster.RemovePeer(peer, 2); err != nil {
				return fmt.Errorf("partition link %d<->2 (isolate=true): %w", peer+1, err)
			}
		} else {
			if err := cluster.AddPeer(2, peer); err != nil {
				return fmt.Errorf("partition link 2<->%d (heal): %w", peer+1, err)
			}
			if err := cluster.AddPeer(peer, 2); err != nil {
				return fmt.Errorf("partition link %d<->2 (heal): %w", peer+1, err)
			}
		}
	}
	return nil
}

// proveIsolated writes a marker row on node 0 while node 2 is split off
// and proves the split is real: node 1 must observe it, node 2 must
// never observe it during the whole window. The marker counts in
// written so the final want stays exact.
func proveIsolated(cluster *goharness.Cluster, written *atomic.Int64) error {
	if err := insert(cluster, 0, isolationMarkerID, isolationMarker); err != nil {
		return fmt.Errorf("isolation marker write: %w", err)
	}
	written.Add(1)
	// Node 1 must see the marker: proves cross-node traffic flows on the
	// intact side while we watch node 2 for leaks.
	deadline := time.Now().Add(isolationTimeout)
	for {
		seen, err := hasName(cluster, 1, isolationMarker)
		if err != nil {
			return err
		}
		if seen {
			break
		}
		leaked, err := hasName(cluster, 2, isolationMarker)
		if err != nil {
			return err
		}
		if leaked {
			return fmt.Errorf("isolation violated: node3 observed the marker while split (remove_peer ineffective?)")
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("node2 never observed the isolation marker within %v", isolationTimeout)
		}
		time.Sleep(isolationPoll)
	}
	// Node 1 has it; give a leak one more second to show on node 2.
	time.Sleep(leakWindow)
	leaked, err := hasName(cluster, 2, isolationMarker)
	if err != nil {
		return err
	}
	if leaked {
		return fmt.Errorf("isolation violated: node3 observed the marker while split (remove_peer ineffective?)")
	}
	fmt.Fprintln(os.Stderr, "isolation proven: marker reached node2, never node3")
	return nil
}

// waitConverged waits until every node holds exactly want rows with
// equal ordered contents.
func waitConverged(cluster *goharness.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	for {
		ok := true
		var first []string
		for i := 0; i < cluster.Len(); i++ {
			nodeNames, err := names(cluster, i)
			if err != nil {
				return err
			}
			if len(nodeNames) != want {
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
			return nil
		}
		if !time.Now().Before(deadline) {
			break
		}
		if time.Since(lastLog) > progressLogInterval {
			lastLog = time.Now()
			counts := make([]int, 0, cluster.Len())
			for i := 0; i < cluster.Len(); i++ {
				got, err := names(cluster, i)
				if err != nil {
					return err
				}
				counts = append(counts, len(got))
			}
			fmt.Fprintf(os.Stderr, "converge progress: counts=%v want=%d\n", counts, want)
		}
		time.Sleep(pollInterval)
	}
	for i := 0; i < cluster.Len(); i++ {
		got, err := names(cluster, i)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "node%d at timeout: count=%d\n", i+1, len(got))
	}
	return fmt.Errorf("nodes did not converge on %d rows with equal contents within %v", want, timeout)
}

// envInt is a positive-integer env knob with fallback.
func envInt(name string, fallback uint64) uint64 {
	if v, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil && v > 0 {
		return v
	}
	return fallback
}

// writer inserts on node every writeInterval until stop is set.
func writer(cluster *goharness.Cluster, node int, written, failed *atomic.Int64, stop *atomic.Bool, wg *sync.WaitGroup) {
	defer wg.Done()
	var seq int64
	for !stop.Load() {
		time.Sleep(writeInterval)
		if stop.Load() {
			break
		}
		seq++
		name := fmt.Sprintf("n%d-s%d", node, seq)
		if err := insert(cluster, node, int64(node)*idStride+seq, name); err != nil {
			fmt.Fprintf(os.Stderr, "writer node%d failed: %v\n", node+1, err)
			failed.Add(1)
		} else {
			written.Add(1)
		}
	}
}

func runBody(cluster *goharness.Cluster, cycles int, split, healGap time.Duration) error {
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		fmt.Fprintf(os.Stderr, "%s dir=%s sql=%s repl=%s\n", info.Label, info.Dir, info.SQLAddr, info.ReplAddr)
	}
	// Same schema on every node, then replicate it: row convergence
	// needs the per-table flag on each node.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			return fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("node%d flush after SET TABLE: %w", i+1, err)
		}
	}

	// Baseline converges before flapping (honest-path control).
	for i := int64(0); i < baselineRows; i++ {
		if err := insert(cluster, 0, baselineIDBase+i, fmt.Sprintf("base-%d", i)); err != nil {
			return fmt.Errorf("baseline write: %w", err)
		}
	}
	if err := waitConverged(cluster, baselineRows, convergeTimeout); err != nil {
		return err
	}

	// Continuous writers on every node; node3 stays live (running, only
	// unreachable), so its local commits must succeed too.
	var written, failed atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go writer(cluster, w, &written, &failed, &stop, &wg)
	}
	// The driver sets stop on every exit path so a failed phase cannot
	// hang the join.
	driverResult := func() error {
		for c := 0; c < cycles; c++ {
			if err := setPartition(cluster, true); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "cycle %d/%d: node3 isolated for %v\n", c+1, cycles, split)
			if c == 0 {
				// Isolation proof (F1): while split, a marker written on
				// node0 must reach node1 but never node2. Without this,
				// a no-op remove_peer would pass vacuously. The marker
				// is counted in written so the final want stays exact.
				if err := proveIsolated(cluster, &written); err != nil {
					return err
				}
			}
			time.Sleep(split)
			if err := setPartition(cluster, false); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "cycle %d/%d: healed, settling %v\n", c+1, cycles, healGap)
			time.Sleep(healGap)
		}
		return nil
	}()
	stop.Store(true)
	wg.Wait()
	if driverResult != nil {
		return driverResult
	}

	// Final heal (idempotent) then full convergence on everything written.
	if err := setPartition(cluster, false); err != nil {
		return err
	}
	if err := cluster.SyncAll(); err != nil {
		return fmt.Errorf("final sync: %w", err)
	}
	want := baselineRows + int(written.Load())
	fmt.Fprintf(os.Stderr, "flapping done: %d cycles, %d rows written, %d failed\n", cycles, want, failed.Load())
	if failed.Load() != 0 {
		return fmt.Errorf("partition failed %d writes to live nodes, want 0", failed.Load())
	}
	if err := waitConverged(cluster, want, convergeTimeout); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "post-flap convergence: %d rows, identical digests\n", want)
	return nil
}

func TestFlappingPartitionLosesNoWrites(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cycles := int(envInt("MURMUR_FLAP_CYCLES", 5))
	split := goharness.EnvSeconds("MURMUR_FLAP_SPLIT_SECONDS", 3)
	healGap := goharness.EnvSeconds("MURMUR_FLAP_HEAL_SECONDS", 2)

	cluster, err := goharness.NewCluster(goharness.DefaultClusterOptions("flap-partition"))
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 3 {
		cluster.Finish(true)
		t.Fatalf("this scenario is written for 3 nodes, got %d", cluster.Len())
	}
	result := runBody(cluster, cycles, split, healGap)
	failed := result != nil
	if result != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", result)
	}
	if failed {
		cluster.DumpForensics("flap-partition-no-convergence")
	}
	cluster.Finish(failed)
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
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
