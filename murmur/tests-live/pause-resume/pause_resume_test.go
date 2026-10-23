// Live scenario: a frozen node resumes with zero data loss.
//
// Go port of the Rust `pause-resume` scenario
// (/home/marc/MURMUR/tests-live/pause-resume/tests/pause_resume.rs).
// Three nodes mesh while all take writes; node 2 is frozen for 15-30 s
// while the survivors keep writing, then resumes with zero data loss
// and converging digests. SIGSTOP of one in-process node would stop the
// test driver too, so the freeze is a scoped iptables packet blackhole
// on the frozen node's replication port: from the mesh's view the node
// goes dark exactly like a stopped process (no packets in or out),
// while locally it simply takes no writes — which a frozen node cannot
// do either.
//
// Env-gated: SKIPs when iptables rules cannot be installed (no
// privilege); teardown never leaks a rule.
package pauseresume

import (
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
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

// frozen is the frozen node (Rust: node2).
const frozen = 1

const (
	// stopEnv is the freeze-window knob (Rust:
	// MURMUR_PAUSE_RESUME_STOP_SECONDS, default 20, 15 s floor).
	stopEnv = "MURMUR_PAUSE_RESUME_STOP_SECONDS"
	// defaultStopSecs is the default freeze window.
	defaultStopSecs = 20
	// minStopSecs is the freeze-window floor.
	minStopSecs = 15
	// baselineRows is the baseline row count (Rust: 5).
	baselineRows = 5
	// writeTick is the writer cadence (Rust: 25 ms ticker on every node).
	writeTick = 25 * time.Millisecond
	// drain is the in-flight drain before freezing (Rust: 300 ms).
	drain = 300 * time.Millisecond
	// preRoll is the pre-roll before the freeze (Rust: 2 s).
	preRoll = 2 * time.Second
	// meshTimeout bounds the mesh waits (Rust: 60 s peers + membership).
	meshTimeout = 60 * time.Second
	// baselineTimeout bounds baseline convergence (Rust: 30 s).
	baselineTimeout = 30 * time.Second
	// sampleTick is the freeze-detection sample cadence (Rust: 1 s).
	sampleTick = time.Second
	// convergeTimeout is the post-resume convergence bound (Rust: 180 s).
	convergeTimeout = 180 * time.Second
	// readmitTimeout bounds re-admission waits (Rust: 90 s sessions +
	// membership).
	readmitTimeout = 90 * time.Second
	// postTimeout bounds post-resume convergence (Rust: 60 s).
	postTimeout = 60 * time.Second
	// poll is the convergence poll interval (Rust: 200 ms).
	poll = 200 * time.Millisecond
	// memberPoll is the membership/session poll interval (Rust: 250 ms).
	memberPoll = 250 * time.Millisecond
)

// names is the canonical ordered names on one node (Rust: TypedNames).
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

// tryInsert is a best-effort insert; true when acknowledged.
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

// memberCount is the discovered-member count, excluding self.
func memberCount(t *testing.T, cluster *harness.Cluster, node int) int {
	t.Helper()
	h := cluster.Node(node).Replication()
	if h == nil {
		t.Fatalf("node%d replication disabled", node+1)
	}
	return h.Status().Members
}

// probeFailures counts failed membership probes on one node (Rust:
// spedsql_swim_probe_failures_total).
func probeFailures(t *testing.T, cluster *harness.Cluster, node int) uint64 {
	t.Helper()
	h := cluster.Node(node).Replication()
	if h == nil {
		t.Fatalf("node%d replication disabled", node+1)
	}
	return h.Status().Metrics.ProbesFailed
}

// needsSent counts gap-pull requests sent by one node (Rust:
// spedsql_repl_needs_sent_total; proves the catch-up path ran).
func needsSent(t *testing.T, cluster *harness.Cluster, node int) uint64 {
	t.Helper()
	h := cluster.Node(node).Replication()
	if h == nil {
		t.Fatalf("node%d replication disabled", node+1)
	}
	return h.Status().Metrics.ChunkNeedSent
}

// waitConnectedPeers waits until every node reports want live sessions
// (Rust: waitConnectedPeers).
func waitConnectedPeers(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, memberPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if peerCount(t, cluster, i) != want {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not reach %d connected peers within %v", want, timeout)
	}
}

// waitMembership waits until every node sees want discovered members
// (Rust: waitMembership).
func waitMembership(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, memberPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if memberCount(t, cluster, i) != want {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not reach membership %d (excl. self) within %v", want, timeout)
	}
}

// waitConverged polls exact counts plus ordered-name equality (Rust:
// waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		var first []string
		for i := 0; i < cluster.Len(); i++ {
			got := names(t, cluster.Node(i))
			if len(got) != want {
				return false
			}
			if i == 0 {
				first = got
			} else if !slices.Equal(first, got) {
				return false
			}
		}
		return true
	})
	if !ok {
		for i := 0; i < cluster.Len(); i++ {
			t.Logf("node%d at timeout: count=%d", i+1, len(names(t, cluster.Node(i))))
		}
		t.Fatalf("nodes did not converge on %d records with equal contents within %v", want, timeout)
	}
}

// assertAcknowledgedPresent requires every acknowledged value present
// on every node (Rust: assertAcknowledgedPresent).
func assertAcknowledgedPresent(t *testing.T, cluster *harness.Cluster, acknowledged map[string]bool) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		present := make(map[string]bool)
		for _, n := range names(t, cluster.Node(i)) {
			present[n] = true
		}
		for value := range acknowledged {
			if !present[value] {
				t.Fatalf("node%d lost acknowledged write %q after resume", i+1, value)
			}
		}
	}
}

// freezePackets installs the packet blackhole: DROP every UDP packet
// whose source OR destination port is the frozen node's replication
// port. Rules insert at the head of OUTPUT (an early
// ACCEPT-established rule would otherwise shadow an append), and match
// only the unique ephemeral port, so no other traffic is touched. It
// returns the applied rule specs for exact-match deletion. A
// first-rule failure means no privilege (caller SKIPs); a later
// failure is a real error (privilege proven).
func freezePackets(t *testing.T, port uint16) ([][]string, bool) {
	t.Helper()
	portStr := strconv.Itoa(int(port))
	specs := [][]string{
		{"OUTPUT", "-p", "udp", "--dport", portStr, "-j", "DROP"},
		{"OUTPUT", "-p", "udp", "--sport", portStr, "-j", "DROP"},
	}
	var applied [][]string
	for _, spec := range specs {
		args := append([]string{"-I", "1"}, spec...)
		if err := exec.Command("iptables", args...).Run(); err != nil {
			if len(applied) == 0 {
				return nil, false
			}
			// Privilege proven by the first rule: clean up, then fail loud.
			for i := len(applied) - 1; i >= 0; i-- {
				_ = exec.Command("iptables", append([]string{"-D"}, applied[i]...)...).Run()
			}
			t.Fatalf("iptables: first rule applied but second failed; rules removed")
		}
		applied = append(applied, spec)
	}
	return applied, true
}

// thawPackets deletes blackhole rules in reverse order; true when all
// deletions succeeded (teardown must never leak a rule).
func thawPackets(t *testing.T, applied [][]string) bool {
	t.Helper()
	ok := true
	for i := len(applied) - 1; i >= 0; i-- {
		if err := exec.Command("iptables", append([]string{"-D"}, applied[i]...)...).Run(); err != nil {
			t.Logf("teardown: failed to delete iptables rule %v", applied[i])
			ok = false
		}
	}
	return ok
}

// assertNoEviction proves the frozen member was not permanently
// evicted: every node holds a live session to both peers and the member
// view is full again (Rust: assertNoEviction, via peer diagnostics +
// member counts).
func assertNoEviction(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		h := cluster.Node(i).Replication()
		if h == nil {
			t.Fatalf("node%d replication disabled", i+1)
		}
		diags := h.Status().PeersDiagnostics
		if len(diags) != 2 {
			t.Fatalf("node%d sees %d peers, want 2 (a peer was evicted)", i+1, len(diags))
		}
		for _, p := range diags {
			if !p.Connected {
				t.Fatalf("node%d peer %v not connected after resume (evicted?)", i+1, p.NodeID)
			}
		}
		if got := memberCount(t, cluster, i); got != 2 {
			t.Fatalf("node%d members = %d, want 2 (frozen member never re-admitted)", i+1, got)
		}
	}
}

func TestPauseResumeLosesNoWrites(t *testing.T) {
	stopSecs := defaultStopSecs
	if v, err := strconv.Atoi(os.Getenv(stopEnv)); err == nil && v > 0 {
		stopSecs = v
	}
	if stopSecs < minStopSecs {
		t.Logf("stop window %ds below the 15s floor; using 15s", stopSecs)
		stopSecs = minStopSecs
	}

	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:           "pause-resume",
		NumNodes:       3,
		BootstrapSeeds: []int{0},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("pause-resume-no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
	frozenPort := cluster.NodeInfo(frozen).ReplAddr.Port()
	// Set once the blackhole is up; teardown always removes it.
	var blackhole [][]string
	thawOK := true
	defer func() {
		if len(blackhole) > 0 {
			thawOK = thawPackets(t, blackhole)
		}
		if !thawOK {
			t.Errorf("teardown leaked iptables rules")
		}
	}()

	// Same schema on every node (Rust: typed fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("migrate records table: %v", err)
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

	// Positive control baseline: full mesh (sessions + membership).
	waitConnectedPeers(t, cluster, 2, meshTimeout)
	waitMembership(t, cluster, 2, meshTimeout)
	for i := 0; i < baselineRows; i++ {
		name := fmt.Sprintf("base-%d", i)
		if !tryInsert(cluster.Node(0), name) {
			t.Fatalf("baseline write failed")
		}
		if err := cluster.Node(0).Flush(); err != nil {
			t.Fatalf("flush baseline: %v", err)
		}
	}
	waitConverged(t, cluster, baselineRows, baselineTimeout)

	var ackMu sync.Mutex
	acknowledged := map[string]bool{
		"base-0": true, "base-1": true, "base-2": true, "base-3": true, "base-4": true,
	}
	ackedLen := func() int {
		ackMu.Lock()
		defer ackMu.Unlock()
		return len(acknowledged)
	}
	needsBefore := needsSent(t, cluster, frozen)

	// Writers hammer every node continuously; node 2's writer pauses
	// while it is frozen (a frozen node accepts nothing by definition).
	var paused [3]atomic.Bool
	var stop atomic.Bool
	var seqs [3]atomic.Int64
	// Writer handles cloned up front: the frozen node is never
	// stopped, so no handle refresh is needed (unlike restart legs).
	dbs := []*murmur.Database{cluster.Node(0), cluster.Node(1), cluster.Node(2)}
	var wg sync.WaitGroup
	for node := 0; node < 3; node++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				time.Sleep(writeTick)
				if stop.Load() {
					return
				}
				if paused[node].Load() {
					continue
				}
				seq := seqs[node].Add(1)
				value := fmt.Sprintf("flow-n%d-%08d", node+1, seq)
				if tryInsert(dbs[node], value) {
					ackMu.Lock()
					acknowledged[value] = true
					ackMu.Unlock()
				}
			}
		}()
	}
	joinWriters := func() {
		stop.Store(true)
		wg.Wait()
	}
	// A Fatal in the driver body must never wedge the join on workers
	// waiting for the stop flag: t.Fatalf runs deferred calls.
	joined := false
	defer func() {
		if !joined {
			joinWriters()
		}
	}()

	time.Sleep(preRoll)

	// Freeze node 2 mid-replication: no packets in or out.
	paused[frozen].Store(true)
	time.Sleep(drain) // drain in-flight requests first
	ackedAtStop := ackedLen()
	t.Logf("freezing node2 packets with %d rows acknowledged so far", ackedAtStop)
	applied, ok := freezePackets(t, frozenPort)
	if !ok {
		joinWriters()
		joined = true
		failed = false
		t.Skipf("SKIP: iptables rule install failed (need privilege?)")
	}
	blackhole = applied

	// While frozen: survivors keep writing; sample the alive view
	// from the outside. Healthy reads 2 (excludes self); detection
	// of the frozen member may dip survivors to 1.
	probesBefore := [2]uint64{probeFailures(t, cluster, 0), probeFailures(t, cluster, 2)}
	minAlive := 2
	sampleEnd := time.Now().Add(time.Duration(stopSecs) * time.Second)
	for time.Now().Before(sampleEnd) {
		for _, idx := range []int{0, 2} {
			if m := memberCount(t, cluster, idx); m < minAlive {
				minAlive = m
			}
		}
		time.Sleep(sampleTick)
	}
	ackedDuringPause := ackedLen() - ackedAtStop
	probesDuring := (probeFailures(t, cluster, 0) - probesBefore[0]) + (probeFailures(t, cluster, 2) - probesBefore[1])
	t.Logf("during %ds freeze: minAlivePeers=%d probeFailures=%d ackedDuringPause=%d", stopSecs, minAlive, probesDuring, ackedDuringPause)
	// Freeze detection proof: survivors must notice the frozen
	// member, either as an alive-view dip or as failed probes.
	// Without this, broken probing would pass silently.
	if !(minAlive < 2 || probesDuring > 0) {
		t.Fatalf("freeze undetected: survivors kept alive=%d with zero probe failures during %ds (probing ineffective?)", minAlive, stopSecs)
	}
	if ackedDuringPause == 0 {
		t.Fatalf("no writes acknowledged while node2 was frozen; pause proved nothing")
	}

	// Resume and prove streams come back.
	thawOK = thawPackets(t, blackhole)
	blackhole = nil
	if !thawOK {
		t.Fatalf("teardown leaked iptables rules")
	}
	// Health: the node answers reads immediately (it never
	// stopped; only its packets were blackholed).
	resumeStart := time.Now()
	_ = names(t, cluster.Node(frozen))
	t.Logf("node2 healthy %v after resume", time.Since(resumeStart))
	// Sessions re-establish over the reopened path (explicit
	// re-mesh: the blackhole killed the QUIC sessions and there
	// is no auto-redial).
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after resume: %v", err)
	}
	paused[frozen].Store(false)

	joinWriters()
	joined = true

	total := ackedLen()
	// 180 s bound (post-resume range-repair normally lands fast,
	// but cold page cache once needed past 120 s).
	waitConverged(t, cluster, total, convergeTimeout)
	ackMu.Lock()
	assertAcknowledgedPresent(t, cluster, acknowledged)
	ackMu.Unlock()

	// Sessions resumed on every link; membership re-admitted the member.
	waitConnectedPeers(t, cluster, 2, readmitTimeout)
	waitMembership(t, cluster, 2, readmitTimeout)
	assertNoEviction(t, cluster)

	// Record the rejoin path: with default retention the log still
	// holds everything, so catch-up must arrive as gap-pull needs
	// (a snapshot here would mean ranges vanished early).
	needsAfter := needsSent(t, cluster, frozen)
	if !(needsAfter > needsBefore) {
		t.Fatalf("node2 rejoined with no gap-pull (needs %d->%d); catch-up path unproven", needsBefore, needsAfter)
	}
	t.Logf("node2 rejoin path: range-repair (needs %d->%d)", needsBefore, needsAfter)

	// Post-resume honest write still replicates everywhere.
	if !tryInsert(cluster.Node(frozen), "post-resume") {
		t.Fatalf("post-resume write failed")
	}
	if err := cluster.Node(frozen).Flush(); err != nil {
		t.Fatalf("flush post-resume: %v", err)
	}
	ackMu.Lock()
	acknowledged["post-resume"] = true
	ackMu.Unlock()
	total = ackedLen()
	waitConverged(t, cluster, total, postTimeout)
	ackMu.Lock()
	assertAcknowledgedPresent(t, cluster, acknowledged)
	ackMu.Unlock()
	t.Logf("pause/resume proven: %ds freeze, %d rows intact, digests converge", stopSecs, total)
	failed = false
}
