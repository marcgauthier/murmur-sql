// Live scenario: crash-looped node converges without loss.
//
// Go port of the Rust `crash-loop` scenario
// (/home/marc/MURMUR/tests-live/crash-loop/tests/crash_loop.rs),
// itself a port of the original Go `crash-loop` scenario
// (TestCrashLoopNodeConvergesWithoutLoss). One of three meshed nodes is
// restarted N times under continuous writes from every node, with short
// uptimes between restarts. Every write acknowledged before each restart
// must survive; the looped node must converge to identical contents; it
// must reach ready within a bound after the final restart (no wedge); and
// a post-loop write on it must replicate everywhere.
//
// Plain SQL replaces the typed-records HTTP API, and in-process
// close/reopen replaces SIGKILL: killing one in-process node would kill
// the test driver.
package crashloop

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

// tableDDL is the records table (Rust: typed records; only names matter).
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)"

// victim is the looped node (Rust: `VICTIM = 2`, node3).
const victim = 2

const (
	// roundsEnv sizes the restart loop (Rust: default 5).
	roundsEnv     = "MURMUR_CRASH_LOOP_ROUNDS"
	defaultRounds = 5
	// uptimeEnv sizes the uptime between restarts (Rust: default 1000ms).
	uptimeEnv       = "MURMUR_CRASH_LOOP_UPTIME_MS"
	defaultUptimeMs = 1000
	// writeTick is the background-writer cadence (Rust: 25ms, 1 per node).
	writeTick = 25 * time.Millisecond
	// burstRows is the final-round burst on a survivor (Rust: 30).
	burstRows = 30
	// finalKillWindow bounds the final restart so the burst is still
	// unflushed, mid-replication by construction (Rust: 500ms).
	finalKillWindow = 500 * time.Millisecond
	// inFlightTimeout gates non-final rounds on a provable in-flight
	// write (Rust: 10s `waitInflight`).
	inFlightTimeout = 10 * time.Second
	inFlightPoll    = time.Millisecond
	// readyBound is the no-wedge bound for the final restart (Rust: 60s).
	readyBound = 60 * time.Second
	// controlTimeout bounds the positive-control waits (Rust: 30s).
	controlTimeout = 30 * time.Second
	// loopTimeout bounds the post-loop waits (Rust: 90s).
	loopTimeout = 90 * time.Second
	// postTimeout bounds post-loop write convergence (Rust: 60s).
	postTimeout = 60 * time.Second
	// poll is the convergence poll interval (Rust: 100ms).
	poll = 100 * time.Millisecond
)

// envInt reads a positive-integer env knob with fallback (Rust: `envInt`).
func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

// queryNames reads canonical ordered names on one node (Rust: `names`).
func queryNames(db *murmur.Database) ([]string, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM live_typed_records ORDER BY name")
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

// mustNames reads canonical ordered names, failing the test on error.
func mustNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	names, err := queryNames(db)
	if err != nil {
		t.Fatalf("read names failed: %v", err)
	}
	return names
}

// tryInsert is a best-effort insert; true when acknowledged (Rust:
// writers keep only acknowledged names; the victim's downtime refuses
// requests).
func tryInsert(db *murmur.Database, name string) bool {
	_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name) VALUES(?)", name)
		return nil, err
	})
	return err == nil
}

// peerCount reads the live session count (Rust: `connected_peers`).
func peerCount(t *testing.T, cluster *harness.Cluster, node int) int {
	t.Helper()
	h := cluster.Node(node).Replication()
	if h == nil {
		t.Fatalf("node%d has no replication", node+1)
	}
	return h.Status().Peers
}

// waitForPeers waits until every node reports its wanted peer count.
func waitForPeers(t *testing.T, cluster *harness.Cluster, want []int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			h := cluster.Node(i).Replication()
			if h == nil || h.Status().Peers != want[i] {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("peers did not reach %v within %v", want, timeout)
	}
}

// waitForConvergence polls exact counts plus ordered-name equality.
func waitForConvergence(t *testing.T, cluster *harness.Cluster, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		first, err := queryNames(cluster.Node(0))
		if err != nil {
			return false
		}
		for i := 1; i < cluster.Len(); i++ {
			got, err := queryNames(cluster.Node(i))
			if err != nil || len(got) != len(first) {
				return false
			}
			for j := range got {
				if got[j] != first[j] {
					return false
				}
			}
		}
		return true
	})
	if !ok {
		for i := 0; i < cluster.Len(); i++ {
			t.Logf("node%d at timeout: count=%d", i+1, len(mustNames(t, cluster.Node(i))))
		}
		t.Fatalf("nodes did not converge (counts + name digests) within %v", timeout)
	}
}

// assertAcknowledgedPresent requires every acknowledged value present on
// every node.
func assertAcknowledgedPresent(t *testing.T, cluster *harness.Cluster, acknowledged map[string]bool) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		present := make(map[string]bool)
		for _, n := range mustNames(t, cluster.Node(i)) {
			present[n] = true
		}
		for name := range acknowledged {
			if !present[name] {
				t.Fatalf("node%d lost acknowledged write %q", i+1, name)
			}
		}
	}
}

// TestCrashLoopNodeConvergesWithoutLoss restarts node3 N times under
// continuous writes and requires zero acknowledged-write loss plus a
// bounded final restart (Rust: crash_loop_node_converges_without_loss).
func TestCrashLoopNodeConvergesWithoutLoss(t *testing.T) {
	rounds := envInt(roundsEnv, defaultRounds)
	uptime := time.Duration(envInt(uptimeEnv, defaultUptimeMs)) * time.Millisecond

	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "crash-loop",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("crash-loop-no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}

	// Same schema on every node (Rust: typed fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("node%d migrate records table: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("live_typed_records", true); err != nil {
			t.Fatalf("node%d opt-in live_typed_records: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}

	// Positive control: honest 3-node convergence before any crash.
	for node := 0; node < 3; node++ {
		name := fmt.Sprintf("base-node%d", node+1)
		if !tryInsert(cluster.Node(node), name) {
			t.Fatalf("baseline write node%d failed", node+1)
		}
		if err := cluster.Node(node).Flush(); err != nil {
			t.Fatalf("node%d flush baseline: %v", node+1, err)
		}
	}
	waitForPeers(t, cluster, []int{2, 2, 2}, controlTimeout)
	waitForConvergence(t, cluster, controlTimeout)
	t.Logf("positive control: 3-node mesh converged")

	// Current-generation writer handles, refreshed after every restart
	// (writers re-resolve the node endpoint after each restart).
	var handlesMu sync.Mutex
	handles := make([]*murmur.Database, cluster.Len())
	for n := 0; n < cluster.Len(); n++ {
		handles[n] = cluster.Node(n)
	}
	var ackedMu sync.Mutex
	acknowledged := map[string]bool{
		"base-node1": true,
		"base-node2": true,
		"base-node3": true,
	}
	var stop atomic.Bool
	// A Fatalf anywhere below must not wedge the writer join on workers
	// waiting for the stop flag.
	defer stop.Store(true)
	var inFlight [3]atomic.Int64
	var seqs [3]atomic.Int64
	var wg sync.WaitGroup
	for node := 0; node < cluster.Len(); node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			for !stop.Load() {
				time.Sleep(writeTick)
				if stop.Load() {
					return
				}
				seq := seqs[node].Add(1)
				name := fmt.Sprintf("n%d-%08d", node+1, seq)
				inFlight[node].Add(1)
				handlesMu.Lock()
				db := handles[node]
				handlesMu.Unlock()
				ok := tryInsert(db, name)
				inFlight[node].Add(-1)
				if ok {
					ackedMu.Lock()
					acknowledged[name] = true
					ackedMu.Unlock()
				}
			}
		}(node)
	}

	var readyDurations []time.Duration
	for round := 1; round <= rounds; round++ {
		time.Sleep(uptime)
		if round == rounds {
			// Burst on a survivor, then restart at once: prefer a
			// moment with a request in flight (mid-write), but
			// bound the wait to 500 ms so the burst is still
			// unflushed (mid-replication by construction).
			for i := 0; i < burstRows; i++ {
				name := fmt.Sprintf("burst-%d", i)
				if !tryInsert(cluster.Node(0), name) {
					t.Fatalf("round %d burst failed", round)
				}
				ackedMu.Lock()
				acknowledged[name] = true
				ackedMu.Unlock()
			}
			if err := cluster.Node(0).Flush(); err != nil {
				t.Fatalf("round %d flush burst: %v", round, err)
			}
			end := time.Now().Add(finalKillWindow)
			for inFlight[victim].Load() == 0 && time.Now().Before(end) {
				time.Sleep(time.Millisecond)
			}
		} else {
			// Every non-final restart requires a request in
			// flight: mid-write proven.
			gate := harness.WaitUntil(inFlightTimeout, inFlightPoll, func() bool {
				return inFlight[victim].Load() > 0
			})
			if !gate {
				t.Fatalf("node%d had no typed write request in flight before restart", victim+1)
			}
		}
		t.Logf("round %d/%d: restarting node3 with %d request(s) in flight",
			round, rounds, inFlight[victim].Load())
		cluster.StopNode(victim)
		start := time.Now()
		if err := cluster.StartNode(victim); err != nil {
			t.Fatalf("restart victim: %v", err)
		}
		handlesMu.Lock()
		handles[victim] = cluster.Node(victim)
		handlesMu.Unlock()
		if err := cluster.Mesh(); err != nil {
			t.Fatalf("re-mesh after restart: %v", err)
		}
		ready := time.Since(start)
		readyDurations = append(readyDurations, ready)
		t.Logf("round %d/%d: node3 ready in %v", round, rounds, ready)
	}
	stop.Store(true)
	wg.Wait()

	// No wedge: the final restart reaches ready within the bound, the
	// mesh reforms, and every acked write is present everywhere.
	last := readyDurations[len(readyDurations)-1]
	if last > readyBound {
		t.Fatalf("final restart took %v to reach ready, want < %v (wedge)", last, readyBound)
	}
	waitForPeers(t, cluster, []int{2, 2, 2}, loopTimeout)
	t.Logf("mesh reformed after the crash loop")
	ackedMu.Lock()
	total := len(acknowledged)
	ackedMu.Unlock()
	waitForConvergence(t, cluster, loopTimeout)
	ackedMu.Lock()
	snapshot := make(map[string]bool, len(acknowledged))
	for n := range acknowledged {
		snapshot[n] = true
	}
	ackedMu.Unlock()
	assertAcknowledgedPresent(t, cluster, snapshot)
	t.Logf("all %d acknowledged writes survived %d restarts", total, rounds)

	// A post-loop write on the looped node replicates everywhere.
	const postLoop = "post-loop-from-victim"
	if !tryInsert(cluster.Node(victim), postLoop) {
		t.Fatalf("post-loop write failed")
	}
	if err := cluster.Node(victim).Flush(); err != nil {
		t.Fatalf("flush post-loop write: %v", err)
	}
	waitForConvergence(t, cluster, postTimeout)
	assertAcknowledgedPresent(t, cluster, map[string]bool{postLoop: true})
	t.Logf("post-loop write on the looped node replicated everywhere")
	failed = false
}
