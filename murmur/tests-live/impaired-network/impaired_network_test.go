// Live scenario: a mesh keeps converging while tc/netem impairs only the
// replication ports.
//
// Go port of the Rust `impaired-network` scenario
// (/home/marc/MURMUR/tests-live/impaired-network/tests/impaired_network.rs).
// A 3-node mesh keeps converging while tc/netem impairs ONLY the
// replication ports: (a) +150 ms latency, (b) 3% packet loss, (c) a 1 Mbit
// cap during a stale-node snapshot resync under sustained writes.
// Membership must stay stable (no false evictions) and per-row visibility
// latencies are recorded with relaxed p95 bounds. Plain SQL replaces the
// typed-contention HTTP API.
//
// Env-gated like Rust: SKIPs without tc/netem + CAP_NET_ADMIN on lo;
// MURMUR_IMPAIRED_NETWORK_FORCE=1 runs the same write/converge workload
// with no impairment (logic verification only).
package impairednetwork

import (
	"crypto/rand"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// tableDDL is the contention table DDL (Rust: typed contention rows).
const tableDDL = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)"

// forceEnv is the force knob (Rust: MURMUR_IMPAIRED_NETWORK_FORCE=1).
const forceEnv = "MURMUR_IMPAIRED_NETWORK_FORCE"

// rowsEnv is the row-count knob (Rust: MURMUR_IMPAIRED_NETWORK_ROWS,
// default 8).
const rowsEnv = "MURMUR_IMPAIRED_NETWORK_ROWS"

const defaultRows = 8

// baselineRows is the baseline row count (Rust: 10).
const baselineRows = 10

const (
	// baselineTimeout is the baseline/capped convergence deadline (Rust:
	// 150 s, 180 s capped).
	baselineTimeout = 150 * time.Second
	cappedTimeout   = 180 * time.Second
	// latRowTimeout and lossRowTimeout are the per-row visibility
	// deadlines (Rust: 30 s latency, 45 s loss).
	latRowTimeout  = 30 * time.Second
	lossRowTimeout = 45 * time.Second
	// latP95Bound and lossP95Bound are the p95 visibility bounds (Rust:
	// 15 s latency, 30 s loss).
	latP95Bound  = 15 * time.Second
	lossP95Bound = 30 * time.Second
	// membershipTimeout is the membership wait (Rust: 60 s) and
	// membershipPoll the poll (Rust: 250 ms).
	membershipTimeout = 60 * time.Second
	membershipPoll    = 250 * time.Millisecond
	// visibilityPoll is the row-visibility poll interval (Rust: 50 ms).
	visibilityPoll = 50 * time.Millisecond
	// poll is the convergence poll interval (Rust: 200 ms).
	poll = 200 * time.Millisecond
	// countTimeout is the survivor-count deadline (Rust: 60 s).
	countTimeout = 60 * time.Second
	// expirySleep is the GC-pass wait (Rust: 60 s after a 12 s expiry
	// sleep).
	expirySleep = 12 * time.Second
	gcTimeout   = 60 * time.Second
)

// contentionRow is one ordered contention row.
type contentionRow struct {
	id   string
	name string
}

// rows returns the ordered contention rows on one node (Rust:
// TypedContentionRows).
func rows(t *testing.T, db *murmur.Database) []contentionRow {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name FROM contention ORDER BY id")
	})
	if err != nil {
		t.Fatalf("read contention rows failed: %v", err)
	}
	var out []contentionRow
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, contentionRow{id: row[0].Str, name: row[1].Str})
	}
	return out
}

// insertRow inserts one row (Rust: TypedContentionInsert).
func insertRow(t *testing.T, db *murmur.Database, node int, id, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO contention(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after insert failed: %v", node, err)
	}
}

// countWhere counts rows named name on one node (Rust: countWhere).
func countWhere(t *testing.T, db *murmur.Database, name string) int {
	t.Helper()
	n := 0
	for _, r := range rows(t, db) {
		if r.name == name {
			n++
		}
	}
	return n
}

// memberCount is the discovered-member count, excluding self (Rust:
// membership_count minus one; established swim-discovery port pattern).
func memberCount(cluster *harness.Cluster, node int) int {
	return cluster.Node(node).Replication().Status().Members
}

// waitMembership waits until every node sees want discovered members
// (Rust: waitMembership).
func waitMembership(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	converged := harness.WaitUntil(timeout, membershipPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if memberCount(cluster, i) != want {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("nodes did not reach membership %d (excl. self) within %v", want, timeout)
	}
}

// assertStableMembership requires a full healthy membership view on every
// node: all members present (Rust also checks alive == 2, which has no
// equivalent; any eviction still drops the member view and fails here).
func assertStableMembership(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		if got := memberCount(cluster, i); got != 2 {
			t.Fatalf("node%d members = %d during impairment, want 2 (false eviction?)", i+1, got)
		}
	}
}

// waitRowVisible waits until name is visible exactly once on every node
// (Rust: waitRowVisible).
func waitRowVisible(t *testing.T, cluster *harness.Cluster, name string, timeout time.Duration) {
	t.Helper()
	converged := harness.WaitUntil(timeout, visibilityPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if countWhere(t, cluster.Node(i), name) != 1 {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("row %q not visible on all nodes within %v", name, timeout)
	}
}

// waitConverged polls exact counts plus ordered-digest equality, logging
// counts every 10 s while waiting (Rust: waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	lastLog := time.Now()
	converged := harness.WaitUntil(timeout, poll, func() bool {
		var first []contentionRow
		ok := true
		for i := 0; i < cluster.Len(); i++ {
			got := rows(t, cluster.Node(i))
			if len(got) != want {
				ok = false
				break
			}
			if i == 0 {
				first = got
			} else if !slices.Equal(got, first) {
				ok = false
				break
			}
		}
		if !ok && time.Since(lastLog) > 10*time.Second {
			lastLog = time.Now()
			var counts []int
			for i := 0; i < cluster.Len(); i++ {
				counts = append(counts, len(rows(t, cluster.Node(i))))
			}
			t.Logf("converge progress: counts=%v want=%d", counts, want)
		}
		return ok
	})
	if !converged {
		t.Fatalf("nodes did not converge on %d rows with equal digests within %v", want, timeout)
	}
}

// waitRowCount waits until one node holds exactly want rows (Rust:
// waitRowCount).
func waitRowCount(t *testing.T, cluster *harness.Cluster, idx, want int, timeout time.Duration) {
	t.Helper()
	converged := harness.WaitUntil(timeout, poll, func() bool {
		return len(rows(t, cluster.Node(idx))) == want
	})
	if !converged {
		t.Fatalf("node%d count = %d, want %d within %v", idx+1, len(rows(t, cluster.Node(idx))), want, timeout)
	}
}

// percentile is the nearest-rank percentile (Rust: k = ceil(len*p/100)).
func percentile(d []time.Duration, p int) time.Duration {
	if len(d) == 0 {
		return 0
	}
	slices.Sort(d)
	k := (len(d)*p + 99) / 100
	if k < 1 {
		k = 1
	}
	return d[k-1]
}

// tcCapable probes tc/netem usability on lo (Rust: tcCapable). It returns
// true when impairment can be applied, else false with a reason.
func tcCapable() (bool, string) {
	if err := exec.Command("tc", "--version").Run(); err != nil {
		return false, "tc binary not found"
	}
	for range 2 {
		out, err := exec.Command("tc", "qdisc", "show", "dev", "lo").Output()
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				return false, fmt.Sprintf("tc qdisc show failed: %s", strings.TrimSpace(string(ee.Stderr)))
			}
			return false, fmt.Sprintf("tc qdisc show failed: %v", err)
		}
		show := string(out)
		if strings.Contains(show, "77:") {
			// Our own leftover from a crashed run: remove and re-probe.
			if err := exec.Command("tc", "qdisc", "del", "dev", "lo", "root").Run(); err != nil {
				return false, "cannot clear stale qdisc 77:"
			}
			continue
		}
		if strings.Contains(show, "qdisc") && !strings.Contains(show, "noqueue") {
			return false, fmt.Sprintf("lo already has a root qdisc (%s); refusing to disturb shared box",
				strings.TrimSpace(show))
		}
		// Privilege probe: install and immediately remove an empty prio.
		if err := exec.Command("tc", "qdisc", "add", "dev", "lo", "root", "handle", "77:", "prio").Run(); err != nil {
			return false, "tc qdisc add failed (need CAP_NET_ADMIN)"
		}
		if err := exec.Command("tc", "qdisc", "del", "dev", "lo", "root").Run(); err != nil {
			return false, "tc probe cleanup failed"
		}
		return true, ""
	}
	return false, "cannot clear stale qdisc 77:"
}

// clearImpairment removes qdisc 77: when present (Rust: clearImpairment;
// always runs).
func clearImpairment() {
	out, err := exec.Command("tc", "qdisc", "show", "dev", "lo").Output()
	if err != nil || !strings.Contains(string(out), "77:") {
		return
	}
	_ = exec.Command("tc", "qdisc", "del", "dev", "lo", "root").Run()
}

// tcRun runs one tc command; on failure it clears the impairment and fails
// (Rust: the run closure inside applyImpairment).
func tcRun(t *testing.T, what string, args ...string) {
	t.Helper()
	if out, err := exec.Command("tc", args...).CombinedOutput(); err != nil {
		clearImpairment()
		t.Fatalf("tc %s failed (%s)", what, strings.TrimSpace(string(out)))
	}
}

// applyImpairment attaches the impairment to lo as root qdisc 77: (prio)
// with the netem/tbf child on band 77:3; u32 filters steer only packets
// whose source OR destination port is a cluster replication port into the
// impaired band (Rust: applyImpairment).
func applyImpairment(t *testing.T, ports []uint16, child string, args ...string) {
	t.Helper()
	tcRun(t, "root", "qdisc", "replace", "dev", "lo", "root", "handle", "77:", "prio", "bands", "3")
	childArgs := []string{"qdisc", "replace", "dev", "lo", "parent", "77:3", "handle", "773:", child}
	childArgs = append(childArgs, args...)
	tcRun(t, "child", childArgs...)
	for _, port := range ports {
		p := strconv.Itoa(int(port))
		// Both directions: every replication packet touches a repl port
		// as either source or destination.
		tcRun(t, "filter-dport", "filter", "add", "dev", "lo", "protocol", "ip", "parent", "77:0", "prio",
			"77", "u32", "match", "ip", "dport", p, "0xffff", "flowid", "77:3")
		tcRun(t, "filter-sport", "filter", "add", "dev", "lo", "protocol", "ip", "parent", "77:0", "prio",
			"77", "u32", "match", "ip", "sport", p, "0xffff", "flowid", "77:3")
	}
}

// gcRuns is the GC run counter on one node (Rust:
// spedsql_gc_runs_total).
func gcRuns(cluster *harness.Cluster, idx int) uint64 {
	return cluster.Node(idx).Replication().Status().Metrics.RetentionRuns
}

// waitGCPass runs on-demand retention passes until one completes past the
// baseline (Rust: waitGCPass on the background ticker; the pass is exposed
// directly — established graceful-shutdown port pattern).
func waitGCPass(t *testing.T, cluster *harness.Cluster, idx int, timeout time.Duration) {
	t.Helper()
	base := gcRuns(cluster, idx)
	converged := harness.WaitUntil(timeout, time.Second, func() bool {
		if _, err := cluster.Node(idx).CollectRetention(); err != nil {
			t.Fatalf("retention collection pass: %v", err)
		}
		return gcRuns(cluster, idx) > base
	})
	if !converged {
		t.Fatalf("no GC pass observed within %v", timeout)
	}
}

// logCounters are the log-path activity counters (received, applied):
// snapshot application bypasses both, so a flat pair proves the snapshot
// path (established snapshot-resync port pattern; Rust reads
// spedsql_repl_snapshots_received_total, which has no equivalent).
func logCounters(cluster *harness.Cluster, idx int) (uint64, uint64) {
	m := cluster.Node(idx).Replication().Status().Metrics
	return m.ReceivedTransactions, m.AppliedTransactions
}

// timedWrites inserts rows round-robin and records per-row full-mesh
// visibility latency; every row also samples stable membership (Rust:
// timedWrites).
func timedWrites(t *testing.T, cluster *harness.Cluster, prefix string, base, n int, perRow time.Duration) []time.Duration {
	t.Helper()
	lat := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("%s-%d", prefix, i)
		id := fmt.Sprintf("%032x", (base+1)*100000+i)
		start := time.Now()
		insertRow(t, cluster.Node(i%cluster.Len()), i%cluster.Len()+1, id, name)
		waitRowVisible(t, cluster, name, perRow)
		lat = append(lat, time.Since(start))
		assertStableMembership(t, cluster)
	}
	return lat
}

func TestImpairedNetworkConverges(t *testing.T) {
	force := os.Getenv(forceEnv) == "1"
	tcOK, skipReason := tcCapable()
	if !force && !tcOK {
		t.Skipf("impaired-network needs tc/netem + CAP_NET_ADMIN on lo: %s (set %s=1 to run the workload unimpaired)",
			skipReason, forceEnv)
	}
	if force && !tcOK {
		t.Logf("FORCE mode: tc unusable (%s); running workload WITHOUT impairment", skipReason)
	}
	nrows := defaultRows
	if v, err := strconv.Atoi(os.Getenv(rowsEnv)); err == nil && v > 0 {
		nrows = v
	}

	// Fixed node ids so phase (c) can pin snapshot trust at construction:
	// the engine only requests snapshots from explicitly trusted sources,
	// and without trust the stale rejoin would arrive over the log path
	// and the snapshot-path proof below could never hold. (Deviation from
	// the Rust harness default; documented in the README.)
	nodeIDs := make([][16]byte, 3)
	for i := range nodeIDs {
		if _, err := rand.Read(nodeIDs[i][:]); err != nil {
			t.Fatalf("node id entropy: %v", err)
		}
	}
	all := make(map[[16]byte]bool, len(nodeIDs))
	for _, id := range nodeIDs {
		all[id] = true
	}
	trusted := make(map[int]map[[16]byte]bool, len(nodeIDs))
	for i := range nodeIDs {
		perNode := make(map[[16]byte]bool, len(all))
		for id := range all {
			perNode[id] = true
		}
		trusted[i] = perNode
	}
	retention := time.Second
	minimumRetained := uint64(10)
	maximumOffline := 10 * time.Second
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "impaired-network",
		NumNodes: 3,
		// Aggressive retention so phase (c) can force a snapshot resync
		// after a short offline window; harmless to phases (a)/(b).
		Replication: &harness.ReplicationOptions{
			Retention:       &retention,
			MinimumRetained: &minimumRetained,
			MaximumOffline:  &maximumOffline,
		},
		BootstrapSeeds:         []int{0},
		NodeIDs:                nodeIDs,
		TrustedSnapshotSources: trusted,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		// The suite always removes qdisc 77:, on success and on failure.
		if !force {
			clearImpairment()
		}
		if failed {
			cluster.DumpForensics("impaired-network-no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}
	replPorts := make([]uint16, 0, cluster.Len())
	for i := 0; i < cluster.Len(); i++ {
		replPorts = append(replPorts, cluster.NodeInfo(i).ReplAddr.Port())
	}

	// Same schema on every node (Rust: typed contention fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("migrate contention table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("contention", true); err != nil {
			t.Fatalf("opt-in contention: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}
	waitMembership(t, cluster, 2, membershipTimeout)

	// Baseline converges unimpaired (honest-path control).
	base := baselineRows
	for i := 0; i < base; i++ {
		insertRow(t, cluster.Node(0), 1, fmt.Sprintf("%032x", 1000+i), fmt.Sprintf("base-%d", i))
	}
	waitConverged(t, cluster, base, baselineTimeout)

	// Phase (a): +150 ms latency on replication traffic.
	if force {
		t.Logf("FORCE mode: NO 150ms latency applied; green here proves the workload only, not impairment tolerance")
	} else {
		applyImpairment(t, replPorts, "netem", "delay", "150ms")
	}
	lat := timedWrites(t, cluster, "lat", base, nrows, latRowTimeout)
	p95 := percentile(lat, 95)
	t.Logf("latency phase: %d rows p95 visibility=%v (bound 15s)", len(lat), p95)
	if p95 > latP95Bound {
		t.Fatalf("p95 visibility %v exceeds 15s bound", p95)
	}
	if !force {
		clearImpairment()
	}
	base += nrows

	// Phase (b): 3% packet loss on replication traffic.
	if force {
		t.Logf("FORCE mode: NO 3%% loss applied; green here proves the workload only, not impairment tolerance")
	} else {
		applyImpairment(t, replPorts, "netem", "loss", "3%")
	}
	lat = timedWrites(t, cluster, "loss", base, nrows, lossRowTimeout)
	p95 = percentile(lat, 95)
	t.Logf("loss phase: %d rows p95 visibility=%v (bound 30s)", len(lat), p95)
	if p95 > lossP95Bound {
		t.Fatalf("p95 visibility %v exceeds 30s bound", p95)
	}
	if !force {
		clearImpairment()
	}
	base += nrows

	// Phase (c): 1 Mbit cap during a stale-node snapshot resync under
	// sustained writes.
	if force {
		t.Logf("FORCE mode: NO bandwidth cap applied; green here proves the workload only, not impairment tolerance")
	}
	for i := 0; i < 5; i++ {
		insertRow(t, cluster.Node(2), 3, fmt.Sprintf("%032x", 5000+i), fmt.Sprintf("stale-%d", i))
	}
	waitConverged(t, cluster, base+5, baselineTimeout)
	cluster.StopNode(2)

	// Survivors write far past the short retention while node 3 is
	// offline (sustained writes continue below).
	for i := 0; i < 60; i++ {
		insertRow(t, cluster.Node(0), 1, fmt.Sprintf("%032x", 6000+i), fmt.Sprintf("fresh-%d", i))
	}
	waitRowCount(t, cluster, 0, base+65, countTimeout)
	waitRowCount(t, cluster, 1, base+65, countTimeout)

	// Node 3's 10 s retention pin must lapse, then one GC pass must
	// collect its ranges so the rejoin needs a snapshot. Poll the GC
	// counter instead of a fixed sleep.
	time.Sleep(expirySleep)
	waitGCPass(t, cluster, 0, gcTimeout)

	// Cap replication throughput, then restart node 3 into it and
	// keep writing while the snapshot transfers.
	if !force {
		applyImpairment(t, replPorts, "tbf", "rate", "1mbit", "burst", "32kbit", "latency", "400ms")
	}
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("restart stale node3: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh stale node3: %v", err)
	}
	for i := 0; i < 20; i++ {
		insertRow(t, cluster.Node(1), 2, fmt.Sprintf("%032x", 8000+i), fmt.Sprintf("sustain-%d", i))
	}
	waitConverged(t, cluster, base+85, cappedTimeout)
	received, applied := logCounters(cluster, 2)
	if received != 0 || applied != 0 {
		t.Fatalf("node3 rejoin arrived over the log path (received=%d applied=%d), want snapshot "+
			"(log catch-up would hide a snapshot-path regression)", received, applied)
	}
	waitMembership(t, cluster, 2, membershipTimeout)
	t.Logf("capped resync converged with snapshot path proven")
	if !force {
		clearImpairment()
	}
	failed = false
}
