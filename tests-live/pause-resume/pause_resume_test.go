// Live scenario: node 2 is frozen with SIGSTOP while the survivors keep
// writing, then resumes with zero data loss and converging digests.
//
// Go port of tests/pause_resume.rs using tests-live/goharness. Identical
// behavior: three seed-meshed nodes take continuous writes on every node,
// node 2 freezes for 15-30 s while survivors keep acknowledging writes,
// the freeze must be detected (member-view dip, stale peer, or dropped
// session), the frozen member must not be evicted, catch-up must arrive
// over the log path, and a post-resume write replicates everywhere. The
// Rust tokio::join! over the three writers and the driver becomes a
// sync.WaitGroup over four goroutines with atomic flags.
//
// Env-gated like the Rust platform skip: SKIPs on Windows, where process
// suspend needs signals that do not exist.
package pauseresume

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Table DDL (Go: typed records; only names matter here).
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)"

// Frozen node (Go: node2).
const frozen = 1

// Freeze-window knob (Go: `MURMUR_PAUSE_RESUME_STOP_SECONDS`, default 20,
// 15 s floor).
const (
	stopEnv        = "MURMUR_PAUSE_RESUME_STOP_SECONDS"
	defaultStopSec = 20
	minStopSec     = 15
)

// Baseline rows (Go: 5).
const baselineRows = 5

// Writer cadence (Go: 25 ms ticker on every node).
const writeTick = 25 * time.Millisecond

// In-flight drain before freezing (Go: 300 ms).
const drain = 300 * time.Millisecond

// Pre-roll before the freeze (Go: 2 s).
const preRoll = 2 * time.Second

// Mesh waits (Go: 60 s peers + membership, 30 s baseline converge).
const (
	meshTimeout     = 60 * time.Second
	baselineTimeout = 30 * time.Second
)

// Freeze-detection sample cadence (Go: 1 s).
const sampleTick = time.Second

// Post-resume convergence bound (Go: 180 s).
const convergeTimeout = 180 * time.Second

// Re-admission waits (Go: 90 s sessions + membership).
const readmitTimeout = 90 * time.Second

// Post-resume convergence (Go: 60 s).
const postTimeout = 60 * time.Second

// Convergence poll interval (Go: 200 ms).
const poll = 200 * time.Millisecond

// Membership/session poll interval (Go: 250 ms).
const memberPoll = 250 * time.Millisecond

// A peer unseen this long counts as stale (freeze-detection proof).
const staleMillis = 5000

// names returns the canonical ordered names on one node (Go:
// `Cluster.TypedNames`).
func names(cluster *goharness.Cluster, idx int) ([]string, error) {
	rows, err := cluster.Node(idx).Query("SELECT name FROM live_typed_records ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("node%d read names: %w", idx+1, err)
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.Get(0))
	}
	return out, nil
}

// tryInsert is a best-effort insert; it reports true when acknowledged.
func tryInsert(cluster *goharness.Cluster, idx int, name string) bool {
	_, err := cluster.Node(idx).Execute("INSERT INTO live_typed_records(name) VALUES($1)", name)
	return err == nil
}

// replicationSummary reads one `MURMUR SHOW REPLICATION` row:
// (peers, members, staged, applied).
func replicationSummary(cluster *goharness.Cluster, idx int) (peers, members, staged, applied uint64, err error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("node%d SHOW REPLICATION: %w", idx+1, err)
	}
	if len(rows) == 0 {
		return 0, 0, 0, 0, fmt.Errorf("node%d SHOW REPLICATION returned no rows", idx+1)
	}
	row := rows[0]
	if len(row) < 6 {
		return 0, 0, 0, 0, fmt.Errorf("node%d SHOW REPLICATION has %d columns, want >= 6", idx+1, len(row))
	}
	num := func(i int, what string) (uint64, error) {
		var n uint64
		n, err := strconv.ParseUint(row[i], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("node%d %s %q: %w", idx+1, what, row[i], err)
		}
		return n, nil
	}
	if peers, err = num(2, "peers"); err != nil {
		return 0, 0, 0, 0, err
	}
	if members, err = num(3, "members"); err != nil {
		return 0, 0, 0, 0, err
	}
	if staged, err = num(4, "staged"); err != nil {
		return 0, 0, 0, 0, err
	}
	if applied, err = num(5, "applied"); err != nil {
		return 0, 0, 0, 0, err
	}
	return peers, members, staged, applied, nil
}

// peerCount returns the live session count (Go: `connected_peers`).
func peerCount(cluster *goharness.Cluster, node int) (uint64, error) {
	peers, _, _, _, err := replicationSummary(cluster, node)
	return peers, err
}

// memberCount returns the discovered-member count, excluding self (Go
// `membership_count` minus one).
func memberCount(cluster *goharness.Cluster, node int) (uint64, error) {
	_, members, _, _, err := replicationSummary(cluster, node)
	return members, err
}

// applied returns cumulative log-applied (incoming) transactions on one
// node. Local commits do not move it, and snapshot application bypasses
// it, so a post-resume delta proves the log-path catch-up (the wire has
// no gap-pull `Need` counter). (`staged_transactions` is a live gauge
// that drains to zero on convergence, so it cannot prove anything
// after the fact.)
func applied(cluster *goharness.Cluster, node int) (uint64, error) {
	_, _, _, applied, err := replicationSummary(cluster, node)
	return applied, err
}

// frozenView reports how one survivor currently sees the frozen peer:
// (row_present, connected, last_seen_millis_ago or MaxUint64 when
// absent/unparseable).
func frozenView(cluster *goharness.Cluster, idx int, frozenHex string) (present, connected bool, lastSeen uint64, err error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW PEERS")
	if err != nil {
		return false, false, 0, fmt.Errorf("node%d SHOW PEERS: %w", idx+1, err)
	}
	for _, row := range rows {
		if len(row) > 0 && row[0] == frozenHex {
			connected := len(row) > 2 && row[2] == "true"
			lastSeen := uint64(math.MaxUint64)
			if len(row) > 3 {
				if n, err := strconv.ParseUint(row[3], 10, 64); err == nil {
					lastSeen = n
				}
			}
			return true, connected, lastSeen, nil
		}
	}
	return false, false, math.MaxUint64, nil
}

// waitConnectedPeers waits until every node reports `want` live sessions
// (Go: `waitConnectedPeers`).
func waitConnectedPeers(cluster *goharness.Cluster, want uint64, timeout time.Duration) error {
	converged := goharness.WaitUntil(timeout, memberPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			n, err := peerCount(cluster, i)
			if err != nil || n != want {
				return false
			}
		}
		return true
	})
	if !converged {
		return fmt.Errorf("nodes did not reach %d connected peers within %v", want, timeout)
	}
	return nil
}

// waitMembership waits until every node sees `want` discovered members
// (Go: `waitMembership`).
func waitMembership(cluster *goharness.Cluster, want uint64, timeout time.Duration) error {
	converged := goharness.WaitUntil(timeout, memberPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			n, err := memberCount(cluster, i)
			if err != nil || n != want {
				return false
			}
		}
		return true
	})
	if !converged {
		return fmt.Errorf("nodes did not reach membership %d (excl. self) within %v", want, timeout)
	}
	return nil
}

// waitConverged polls exact counts plus ordered-name equality (Go:
// `waitConverged`).
func waitConverged(cluster *goharness.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var first []string
		hasFirst := false
		ok := true
		for i := 0; i < cluster.Len(); i++ {
			got, err := names(cluster, i)
			if err != nil || len(got) != want {
				ok = false
				break
			}
			if !hasFirst {
				first, hasFirst = got, true
			} else if !equalStrings(first, got) {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		if !time.Now().Before(deadline) {
			for i := 0; i < cluster.Len(); i++ {
				n := -1
				if got, err := names(cluster, i); err == nil {
					n = len(got)
				}
				fmt.Fprintf(os.Stderr, "node%d at timeout: count=%d\n", i+1, n)
			}
			return fmt.Errorf("nodes did not converge on %d records with equal contents within %v",
				want, timeout)
		}
		time.Sleep(poll)
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

// acknowledgedSet is the mutex-guarded set of acknowledged writes.
type acknowledgedSet struct {
	mu sync.Mutex
	m  map[string]bool
}

func (s *acknowledgedSet) add(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[value] = true
}

func (s *acknowledgedSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}

func (s *acknowledgedSet) snapshot() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]bool, len(s.m))
	for k, v := range s.m {
		out[k] = v
	}
	return out
}

// assertAcknowledgedPresent requires every acknowledged value present on
// every node (Go: `assertAcknowledgedPresent`).
func assertAcknowledgedPresent(cluster *goharness.Cluster, acknowledged map[string]bool) error {
	for i := 0; i < cluster.Len(); i++ {
		got, err := names(cluster, i)
		if err != nil {
			return err
		}
		present := make(map[string]bool, len(got))
		for _, v := range got {
			present[v] = true
		}
		for value := range acknowledged {
			if !present[value] {
				return fmt.Errorf("node%d lost acknowledged write %q after resume", i+1, value)
			}
		}
	}
	return nil
}

// assertNoEviction proves the frozen member was not permanently evicted:
// every node holds a live session to both peers and the member view is
// full again (Go: `assertNoEviction`, via peer diagnostics + member
// counts).
func assertNoEviction(cluster *goharness.Cluster) error {
	for i := 0; i < cluster.Len(); i++ {
		rows, err := cluster.Node(i).Simple("MURMUR SHOW PEERS")
		if err != nil {
			return fmt.Errorf("node%d SHOW PEERS: %w", i+1, err)
		}
		if len(rows) != 2 {
			return fmt.Errorf("node%d sees %d peers, want 2 (a peer was evicted)", i+1, len(rows))
		}
		for _, row := range rows {
			if len(row) > 2 && row[2] != "true" {
				id := ""
				if len(row) > 0 {
					id = row[0]
				}
				return fmt.Errorf("node%d peer %q not connected after resume (evicted?)", i+1, id)
			}
		}
		members, err := memberCount(cluster, i)
		if err != nil {
			return err
		}
		if members != 2 {
			return fmt.Errorf("node%d members = %d, want 2 (frozen member never re-admitted)", i+1, members)
		}
	}
	return nil
}

// writer runs one continuous writer. The frozen node's writer pauses
// while it is suspended (a frozen node accepts nothing by definition).
func writer(cluster *goharness.Cluster, node int, acknowledged *acknowledgedSet, paused, stop *atomic.Bool, seq *atomic.Int64) {
	for {
		time.Sleep(writeTick)
		if stop.Load() {
			return
		}
		if paused.Load() {
			continue
		}
		n := seq.Add(1)
		value := fmt.Sprintf("flow-n%d-%08d", node+1, n)
		if tryInsert(cluster, node, value) {
			acknowledged.add(value)
		}
	}
}

func runBody(cluster *goharness.Cluster, stopSecs uint64) error {
	// Same schema on every node (Go: typed fixture tables), flagged
	// replicated on each node (row convergence needs the per-node flag).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			return fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE live_typed_records REPLICATED true"); err != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("node%d flush after SET TABLE: %w", i+1, err)
		}
	}

	// Positive control baseline: full mesh (sessions + membership).
	if err := waitConnectedPeers(cluster, 2, meshTimeout); err != nil {
		return err
	}
	if err := waitMembership(cluster, 2, meshTimeout); err != nil {
		return err
	}
	for i := 0; i < baselineRows; i++ {
		if !tryInsert(cluster, 0, fmt.Sprintf("base-%d", i)) {
			return fmt.Errorf("baseline write failed")
		}
		if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("flush baseline: %w", err)
		}
	}
	if err := waitConverged(cluster, baselineRows, baselineTimeout); err != nil {
		return err
	}

	acknowledged := &acknowledgedSet{m: map[string]bool{
		"base-0": true, "base-1": true, "base-2": true, "base-3": true, "base-4": true,
	}}
	appliedBefore, err := applied(cluster, frozen)
	if err != nil {
		return err
	}
	frozenID := cluster.NodeID(frozen)
	frozenHex := goharness.Hex(frozenID[:])

	var paused [3]atomic.Bool
	var stop atomic.Bool
	var seqs [3]atomic.Int64

	// The driver sets `stop` on every exit path so a failed phase cannot
	// hang the join.
	driver := func() error {
		result := func() error {
			time.Sleep(preRoll)

			// Freeze node 2 mid-replication: the process stops cold.
			paused[frozen].Store(true)
			time.Sleep(drain) // drain in-flight requests first
			ackedAtStop := acknowledged.len()
			fmt.Fprintf(os.Stderr, "freezing node2 with %d rows acknowledged so far\n", ackedAtStop)
			if err := cluster.SuspendNode(frozen); err != nil {
				return fmt.Errorf("suspend node2: %w", err)
			}

			// While frozen: survivors keep writing; sample the alive view
			// from the outside. Healthy reads 2 (excludes self); detection
			// of the frozen member may dip survivors to 1, stale the peer,
			// or drop its session.
			minAlive := uint64(2)
			sawStale := false
			sawDisconnected := false
			sampleEnd := time.Now().Add(time.Duration(stopSecs) * time.Second)
			for time.Now().Before(sampleEnd) {
				for _, idx := range []int{0, 2} {
					if m, err := memberCount(cluster, idx); err == nil && m < minAlive {
						minAlive = m
					}
					if present, connected, lastSeen, err := frozenView(cluster, idx, frozenHex); err == nil {
						if lastSeen > staleMillis {
							sawStale = true
						}
						if !present || !connected {
							sawDisconnected = true
						}
					}
				}
				time.Sleep(sampleTick)
			}
			ackedDuringPause := acknowledged.len() - ackedAtStop
			fmt.Fprintf(os.Stderr,
				"during %ds freeze: minAlivePeers=%d stale=%v disconnected=%v ackedDuringPause=%d\n",
				stopSecs, minAlive, sawStale, sawDisconnected, ackedDuringPause)
			// Freeze detection proof: survivors must notice the frozen
			// member. Without this, broken probing would pass silently.
			// (The wire has no probe-failure counter; staleness and
			// session state take its place.)
			if !(minAlive < 2 || sawStale || sawDisconnected) {
				return fmt.Errorf("freeze undetected: survivors kept alive=%d with a fresh connected peer view during %ds (probing ineffective?)",
					minAlive, stopSecs)
			}
			if ackedDuringPause == 0 {
				return fmt.Errorf("no writes acknowledged while node2 was frozen; pause proved nothing")
			}

			// Resume and prove streams come back.
			if err := cluster.ResumeNode(frozen); err != nil {
				return fmt.Errorf("resume node2: %w", err)
			}
			// Health: the node answers reads immediately.
			resumeStart := time.Now()
			if _, err := names(cluster, frozen); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "node2 healthy %v after resume\n", time.Since(resumeStart))
			// Sessions re-establish over the reopened path (explicit
			// re-mesh: the freeze killed the QUIC sessions and there is
			// no auto-redial).
			if err := cluster.Mesh(); err != nil {
				return fmt.Errorf("re-mesh after resume: %w", err)
			}
			if err := cluster.SyncAll(); err != nil {
				return fmt.Errorf("sync after resume: %w", err)
			}
			paused[frozen].Store(false)
			return nil
		}()
		stop.Store(true)
		return result
	}
	var wg sync.WaitGroup
	var driverErr error
	wg.Add(4)
	for node := 0; node < 3; node++ {
		go func(node int) {
			defer wg.Done()
			writer(cluster, node, acknowledged, &paused[node], &stop, &seqs[node])
		}(node)
	}
	go func() {
		defer wg.Done()
		driverErr = driver()
	}()
	wg.Wait()
	if driverErr != nil {
		return driverErr
	}

	total := acknowledged.len()
	// 180 s bound (Go: post-resume range-repair normally lands fast,
	// but cold page cache once needed past 120 s).
	if err := waitConverged(cluster, total, convergeTimeout); err != nil {
		return err
	}
	if err := assertAcknowledgedPresent(cluster, acknowledged.snapshot()); err != nil {
		return err
	}

	// Sessions resumed on every link; membership re-admitted the member.
	if err := waitConnectedPeers(cluster, 2, readmitTimeout); err != nil {
		return err
	}
	if err := waitMembership(cluster, 2, readmitTimeout); err != nil {
		return err
	}
	if err := assertNoEviction(cluster); err != nil {
		return err
	}

	// Record the rejoin path: with default retention the log still holds
	// everything, so catch-up must arrive over the log path (applied
	// advances; a snapshot here would mean ranges vanished early, and
	// snapshot application bypasses the applied counter while local
	// commits never move it).
	appliedAfter, err := applied(cluster, frozen)
	if err != nil {
		return err
	}
	if appliedAfter <= appliedBefore {
		return fmt.Errorf("node2 rejoined with no log-path apply (applied %d->%d); catch-up path unproven",
			appliedBefore, appliedAfter)
	}
	fmt.Fprintf(os.Stderr, "node2 rejoin path: log apply (applied %d->%d)\n", appliedBefore, appliedAfter)

	// Post-resume honest write still replicates everywhere.
	if !tryInsert(cluster, frozen, "post-resume") {
		return fmt.Errorf("post-resume write failed")
	}
	if _, err := cluster.Node(frozen).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("flush post-resume: %w", err)
	}
	acknowledged.add("post-resume")
	total = acknowledged.len()
	if err := waitConverged(cluster, total, postTimeout); err != nil {
		return err
	}
	if err := assertAcknowledgedPresent(cluster, acknowledged.snapshot()); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "pause/resume proven: %ds freeze, %d rows intact, digests converge\n", stopSecs, total)
	return nil
}

func TestPauseResumeLosesNoWrites(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	if runtime.GOOS == "windows" {
		t.Skip("pause-resume needs unix signals (SIGSTOP/SIGCONT) to freeze a node")
	}
	stopSecs := uint64(goharness.EnvSeconds(stopEnv, defaultStopSec) / time.Second)
	if stopSecs < minStopSec {
		fmt.Fprintf(os.Stderr, "stop window %ds below the 15s floor; using 15s\n", stopSecs)
		stopSecs = minStopSec
	}

	opts := goharness.DefaultClusterOptions("pause-resume")
	opts.BootstrapSeeds = []int{0}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 3 {
		t.Fatalf("this scenario is written for 3 nodes, got %d", cluster.Len())
	}
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		fmt.Fprintf(os.Stderr, "%s dir=%s sql=%s repl=%s\n",
			info.Label, info.Dir, info.SQLAddr, info.ReplAddr)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()
	if err := runBody(cluster, stopSecs); err != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", err)
		cluster.DumpForensics("pause-resume-no-convergence")
		t.Fatalf("scenario failed: %v", err)
	}
	failed = false
}
