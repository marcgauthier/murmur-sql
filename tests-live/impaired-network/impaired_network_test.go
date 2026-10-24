// Live scenario: a 3-node mesh keeps converging while tc/netem impairs
// ONLY the replication ports.
//
// Go port of tests/impaired_network.rs using tests-live/goharness.
// Identical behavior: (a) +150 ms latency, (b) 3% packet loss, (c) a
// 1 Mbit cap during a stale-node snapshot resync under sustained writes.
// Membership must stay stable (no false evictions) and per-row
// visibility latencies are recorded with relaxed p95 bounds. Plain SQL
// replaces the Go typed-contention HTTP API of the original Go source
// (see the scenario README.md for deviations).
//
// Env-gated like Rust: SKIPs without tc/netem + CAP_NET_ADMIN on lo;
// `MURMUR_IMPAIRED_NETWORK_FORCE=1` runs the same write/converge
// workload with no impairment (logic verification only).
package impairednetwork

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// tableDDL is the contention table DDL (Go: typed contention rows).
const tableDDL = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)"

// forceEnv is the force knob (Go: `MURMUR_IMPAIRED_NETWORK_FORCE=1`).
const forceEnv = "MURMUR_IMPAIRED_NETWORK_FORCE"

// rowsEnv is the row-count knob (Go: `MURMUR_IMPAIRED_NETWORK_ROWS`,
// default 8).
const rowsEnv = "MURMUR_IMPAIRED_NETWORK_ROWS"

const defaultRows = 8

// baselineRows is the baseline row count (Go: 10).
const baselineRows = 10

const (
	// baselineTimeout and cappedTimeout are the baseline/capped
	// convergence deadlines (Go: 150 s, 180 s capped).
	baselineTimeout = 150 * time.Second
	cappedTimeout   = 180 * time.Second
	// latRowTimeout and lossRowTimeout are the per-row visibility
	// deadlines (Go: 30 s latency, 45 s loss).
	latRowTimeout  = 30 * time.Second
	lossRowTimeout = 45 * time.Second
	// latP95Bound and lossP95Bound are the p95 visibility bounds (Go:
	// 15 s latency, 30 s loss).
	latP95Bound  = 15 * time.Second
	lossP95Bound = 30 * time.Second
	// membershipTimeout and membershipPoll are the membership waits
	// (Go: 60 s) and poll (Go: 250 ms).
	membershipTimeout = 60 * time.Second
	membershipPoll    = 250 * time.Millisecond
	// visibilityPoll is the row-visibility poll interval (Go: 50 ms).
	visibilityPoll = 50 * time.Millisecond
	// poll is the convergence poll interval (Go: 200 ms).
	poll = 200 * time.Millisecond
	// countTimeout is the survivor-count deadline (Go: 60 s).
	countTimeout = 60 * time.Second
	// expirySleep and gcTimeout are the GC-pass wait (Go: 60 s after a
	// 12 s expiry sleep).
	expirySleep = 12 * time.Second
	gcTimeout   = 60 * time.Second
)

type idName struct {
	id   string
	name string
}

// rows reads the ordered contention rows on one node (Go:
// `TypedContentionRows`).
func rows(cluster *goharness.Cluster, idx int) ([]idName, error) {
	dbRows, err := cluster.Node(idx).Query("SELECT id, name FROM contention ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("node%d read contention rows: %w", idx+1, err)
	}
	var out []idName
	for _, r := range dbRows {
		id, err := r.String(0)
		if err != nil {
			return nil, fmt.Errorf("node%d decode id: %w", idx+1, err)
		}
		name, err := r.String(1)
		if err != nil {
			return nil, fmt.Errorf("node%d decode name: %w", idx+1, err)
		}
		out = append(out, idName{id, name})
	}
	return out, nil
}

// insertRow inserts one row (Go: `TypedContentionInsert`).
func insertRow(cluster *goharness.Cluster, idx int, id, name string) error {
	if _, err := cluster.Node(idx).Execute(
		"INSERT INTO contention(id, name) VALUES($1, $2)", id, name); err != nil {
		return fmt.Errorf("node%d insert failed: %w", idx+1, err)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after insert failed: %w", idx+1, err)
	}
	return nil
}

// countWhere counts rows named `name` on one node (Go: `countWhere`).
func countWhere(cluster *goharness.Cluster, idx int, name string) (int, error) {
	got, err := rows(cluster, idx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, r := range got {
		if r.name == name {
			n++
		}
	}
	return n, nil
}

// memberCount is the discovered-member count, excluding self (Go
// `membership_count` minus one).
func memberCount(cluster *goharness.Cluster, node int) (uint64, error) {
	got, err := cluster.Node(node).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return 0, fmt.Errorf("node%d SHOW REPLICATION: %w", node+1, err)
	}
	if len(got) == 0 || len(got[0]) < 4 {
		return 0, fmt.Errorf("node%d SHOW REPLICATION returned no members", node+1)
	}
	cell := got[0][3]
	n, err := strconv.ParseUint(cell, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("node%d members %q: %w", node+1, cell, err)
	}
	return n, nil
}

// waitMembership waits until every node sees `want` discovered members
// (Go: `waitMembership`).
func waitMembership(cluster *goharness.Cluster, want uint64, timeout time.Duration) error {
	converged := goharness.WaitUntil(timeout, membershipPoll, func() bool {
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

// assertStableMembership requires a full healthy membership view on every
// node: all members present (Go also checks alive == 2, which has no Rust
// equivalent; any eviction still drops the member view and fails here).
func assertStableMembership(cluster *goharness.Cluster) error {
	for i := 0; i < cluster.Len(); i++ {
		got, err := memberCount(cluster, i)
		if err != nil {
			return err
		}
		if got != 2 {
			return fmt.Errorf("node%d members = %d during impairment, want 2 (false eviction?)",
				i+1, got)
		}
	}
	return nil
}

// waitRowVisible waits until `name` is visible exactly once on every node
// (Go: `waitRowVisible`).
func waitRowVisible(cluster *goharness.Cluster, name string, timeout time.Duration) error {
	converged := goharness.WaitUntil(timeout, visibilityPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			n, err := countWhere(cluster, i, name)
			if err != nil || n != 1 {
				return false
			}
		}
		return true
	})
	if !converged {
		return fmt.Errorf("row %q not visible on all nodes within %v", name, timeout)
	}
	return nil
}

// waitConverged polls exact counts plus ordered-digest equality, logging
// counts every 10 s while waiting (Go: `waitConverged`).
func waitConverged(cluster *goharness.Cluster, want int, timeout time.Duration) error {
	lastLog := time.Now()
	deadline := time.Now().Add(timeout)
	for {
		var first []idName
		var haveFirst bool
		ok := true
		for i := 0; i < cluster.Len(); i++ {
			got, err := rows(cluster, i)
			if err != nil || len(got) != want {
				ok = false
				break
			}
			if !haveFirst {
				first, haveFirst = got, true
			} else if !equalIDNames(first, got) {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		if time.Since(lastLog) > 10*time.Second {
			lastLog = time.Now()
			var counts []uint
			for i := 0; i < cluster.Len(); i++ {
				got, err := rows(cluster, i)
				if err != nil {
					counts = append(counts, ^uint(0))
				} else {
					counts = append(counts, uint(len(got)))
				}
			}
			fmt.Fprintf(os.Stderr, "converge progress: counts=%v want=%d\n", counts, want)
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("nodes did not converge on %d rows with equal digests within %v",
				want, timeout)
		}
		time.Sleep(poll)
	}
}

// waitRowCount waits until one node holds exactly `want` rows (Go:
// `waitRowCount`).
func waitRowCount(cluster *goharness.Cluster, idx, want int, timeout time.Duration) error {
	converged := goharness.WaitUntil(timeout, poll, func() bool {
		got, err := rows(cluster, idx)
		return err == nil && len(got) == want
	})
	if !converged {
		got, err := rows(cluster, idx)
		count := len(got)
		if err != nil {
			count = int(^uint(0) >> 1)
		}
		return fmt.Errorf("node%d count = %d, want %d within %v", idx+1, count, want, timeout)
	}
	return nil
}

// percentile is the nearest-rank percentile (Go: `percentile`;
// k = ceil(len*p/100)).
func percentile(d []time.Duration, p int) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(a, b int) bool { return d[a] < d[b] })
	k := (len(d)*p + 99) / 100
	if k < 1 {
		k = 1
	}
	return d[k-1]
}

// tcCapable probes tc/netem usability on lo (Go: `tcCapable`). It returns
// (true, "") when impairment can be applied, else (false, reason).
func tcCapable() (bool, string) {
	if err := exec.Command("tc", "--version").Run(); err != nil {
		return false, "tc binary not found"
	}
	for range 2 {
		out, err := exec.Command("tc", "qdisc", "show", "dev", "lo").Output()
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				return false, fmt.Sprintf("tc qdisc show failed: %s",
					strings.TrimSpace(string(ee.Stderr)))
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
			return false, fmt.Sprintf(
				"lo already has a root qdisc (%s); refusing to disturb shared box",
				strings.TrimSpace(show))
		}
		// Privilege probe: install and immediately remove an empty prio.
		if err := exec.Command("tc", "qdisc", "add", "dev", "lo", "root",
			"handle", "77:", "prio").Run(); err != nil {
			return false, "tc qdisc add failed (need CAP_NET_ADMIN)"
		}
		if err := exec.Command("tc", "qdisc", "del", "dev", "lo", "root").Run(); err != nil {
			return false, "tc probe cleanup failed"
		}
		return true, ""
	}
	return false, "cannot clear stale qdisc 77:"
}

// clearImpairment removes qdisc 77: when present (Go: `clearImpairment`;
// always runs).
func clearImpairment() {
	out, err := exec.Command("tc", "qdisc", "show", "dev", "lo").Output()
	if err != nil || !strings.Contains(string(out), "77:") {
		return
	}
	_ = exec.Command("tc", "qdisc", "del", "dev", "lo", "root").Run()
}

// tcRun runs one tc command; on failure it clears the impairment and
// reports an error (Go: the `run` closure inside `applyImpairment`;
// the Rust port panics, this port returns the failure to runBody).
func tcRun(what string, args ...string) error {
	out, err := exec.Command("tc", args...).CombinedOutput()
	if err != nil {
		clearImpairment()
		return fmt.Errorf("tc %s failed (%s)", what, strings.TrimSpace(string(out)))
	}
	return nil
}

// applyImpairment attaches the impairment to lo as root qdisc 77: (prio)
// with the netem/tbf child on band 77:3; u32 filters steer only packets
// whose source OR destination port is a cluster replication port into
// the impaired band (Go: `applyImpairment`).
func applyImpairment(ports []uint16, child string, args ...string) error {
	if err := tcRun("root", "qdisc", "replace", "dev", "lo", "root",
		"handle", "77:", "prio", "bands", "3"); err != nil {
		return err
	}
	childArgs := []string{"qdisc", "replace", "dev", "lo", "parent", "77:3",
		"handle", "773:", child}
	childArgs = append(childArgs, args...)
	if err := tcRun("child", childArgs...); err != nil {
		return err
	}
	for _, port := range ports {
		p := strconv.Itoa(int(port))
		// Both directions: every replication packet touches a repl port
		// as either source or destination.
		if err := tcRun("filter-dport", "filter", "add", "dev", "lo", "protocol", "ip",
			"parent", "77:0", "prio", "77", "u32", "match", "ip", "dport", p, "0xffff",
			"flowid", "77:3"); err != nil {
			return err
		}
		if err := tcRun("filter-sport", "filter", "add", "dev", "lo", "protocol", "ip",
			"parent", "77:0", "prio", "77", "u32", "match", "ip", "sport", p, "0xffff",
			"flowid", "77:3"); err != nil {
			return err
		}
	}
	return nil
}

// retentionMarker is the retention-collection marker on one node: max
// `last_collection_millis` across `MURMUR SHOW RETENTION` rows
// (node-global; repeated per row).
func retentionMarker(cluster *goharness.Cluster, idx int) (uint64, error) {
	got, err := cluster.Node(idx).Simple("MURMUR SHOW RETENTION")
	if err != nil {
		return 0, fmt.Errorf("node%d SHOW RETENTION: %w", idx+1, err)
	}
	var best uint64
	seen := false
	for _, r := range got {
		if len(r) > 6 {
			if v, err := strconv.ParseUint(r[6], 10, 64); err == nil {
				seen = true
				if v > best {
					best = v
				}
			}
		}
	}
	if !seen {
		return 0, fmt.Errorf("node%d SHOW RETENTION has no collection marker", idx+1)
	}
	return best, nil
}

// waitGCPass runs on-demand retention passes until one completes past the
// baseline (Go: `waitGCPass` on the background ticker; the wire exposes
// the pass directly via `MURMUR RETENTION COLLECT`).
func waitGCPass(cluster *goharness.Cluster, idx int, timeout time.Duration) error {
	base, err := retentionMarker(cluster, idx)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for {
		if _, err := cluster.Node(idx).Simple("MURMUR RETENTION COLLECT"); err != nil {
			return fmt.Errorf("node%d RETENTION COLLECT: %w", idx+1, err)
		}
		marker, err := retentionMarker(cluster, idx)
		if err != nil {
			return err
		}
		if marker > base {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("no GC pass observed within %v", timeout)
		}
		time.Sleep(time.Second)
	}
}

// logCounters reads the log-path activity counters `(staged, applied)`:
// snapshot application bypasses both, so a flat pair proves the snapshot
// path (Go reads `spedsql_repl_snapshots_received_total`, which has no
// Rust equivalent).
func logCounters(cluster *goharness.Cluster, idx int) (uint64, uint64, error) {
	got, err := cluster.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return 0, 0, fmt.Errorf("node%d SHOW REPLICATION: %w", idx+1, err)
	}
	if len(got) == 0 {
		return 0, 0, fmt.Errorf("node%d SHOW REPLICATION returned no rows", idx+1)
	}
	if len(got[0]) < 6 {
		return 0, 0, fmt.Errorf("node%d SHOW REPLICATION has %d columns, want >= 6",
			idx+1, len(got[0]))
	}
	staged, err := strconv.ParseUint(got[0][4], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("node%d staged %q: %w", idx+1, got[0][4], err)
	}
	applied, err := strconv.ParseUint(got[0][5], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("node%d applied %q: %w", idx+1, got[0][5], err)
	}
	return staged, applied, nil
}

// timedWrites inserts rows round-robin and records per-row full-mesh
// visibility latency; every row also samples stable membership (Go:
// `timedWrites`).
func timedWrites(cluster *goharness.Cluster, prefix string, base, n int, perRow time.Duration) ([]time.Duration, error) {
	lat := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("%s-%d", prefix, i)
		id := fmt.Sprintf("%032x", (base+1)*100000+i)
		start := time.Now()
		if err := insertRow(cluster, i%cluster.Len(), id, name); err != nil {
			return nil, err
		}
		if err := waitRowVisible(cluster, name, perRow); err != nil {
			return nil, err
		}
		lat = append(lat, time.Since(start))
		if err := assertStableMembership(cluster); err != nil {
			return nil, err
		}
	}
	return lat, nil
}

func replPorts(cluster *goharness.Cluster) []uint16 {
	var ports []uint16
	for i := 0; i < cluster.Len(); i++ {
		ports = append(ports, cluster.NodeInfo(i).ReplAddr.Port())
	}
	return ports
}

func equalIDNames(a, b []idName) bool {
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

func runBody(cluster *goharness.Cluster, force bool, rowsN int) error {
	// Same schema on every node (Go: typed contention fixture tables),
	// flagged replicated on each node (row convergence needs the per-node
	// flag).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			return fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE contention REPLICATED true"); err != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("node%d flush after SET TABLE: %w", i+1, err)
		}
	}
	if err := waitMembership(cluster, 2, membershipTimeout); err != nil {
		return err
	}

	// Baseline converges unimpaired (honest-path control).
	base := baselineRows
	for i := 0; i < base; i++ {
		if err := insertRow(cluster, 0, fmt.Sprintf("%032x", 1000+i), fmt.Sprintf("base-%d", i)); err != nil {
			return err
		}
	}
	if err := waitConverged(cluster, base, baselineTimeout); err != nil {
		return err
	}

	// Phase (a): +150 ms latency on replication traffic.
	if force {
		fmt.Fprintln(os.Stderr, "FORCE mode: NO 150ms latency applied; green here proves the workload only, not impairment tolerance")
	} else if err := applyImpairment(replPorts(cluster), "netem", "delay", "150ms"); err != nil {
		return err
	}
	lat, err := timedWrites(cluster, "lat", base, rowsN, latRowTimeout)
	if err != nil {
		return err
	}
	p95 := percentile(lat, 95)
	fmt.Fprintf(os.Stderr, "latency phase: %d rows p95 visibility=%v (bound 15s)\n", len(lat), p95)
	if p95 > latP95Bound {
		return fmt.Errorf("p95 visibility %v exceeds 15s bound", p95)
	}
	if !force {
		clearImpairment()
	}
	base += rowsN

	// Phase (b): 3% packet loss on replication traffic.
	if force {
		fmt.Fprintln(os.Stderr, "FORCE mode: NO 3% loss applied; green here proves the workload only, not impairment tolerance")
	} else if err := applyImpairment(replPorts(cluster), "netem", "loss", "3%"); err != nil {
		return err
	}
	lat, err = timedWrites(cluster, "loss", base, rowsN, lossRowTimeout)
	if err != nil {
		return err
	}
	p95 = percentile(lat, 95)
	fmt.Fprintf(os.Stderr, "loss phase: %d rows p95 visibility=%v (bound 30s)\n", len(lat), p95)
	if p95 > lossP95Bound {
		return fmt.Errorf("p95 visibility %v exceeds 30s bound", p95)
	}
	if !force {
		clearImpairment()
	}
	base += rowsN

	// Phase (c): 1 Mbit cap during a stale-node snapshot resync under
	// sustained writes.
	if force {
		fmt.Fprintln(os.Stderr, "FORCE mode: NO bandwidth cap applied; green here proves the workload only, not impairment tolerance")
	}
	for i := 0; i < 5; i++ {
		if err := insertRow(cluster, 2, fmt.Sprintf("%032x", 5000+i), fmt.Sprintf("stale-%d", i)); err != nil {
			return err
		}
	}
	if err := waitConverged(cluster, base+5, baselineTimeout); err != nil {
		return err
	}
	cluster.StopNode(2)

	// Survivors write far past the short retention while node 3 is
	// offline (sustained writes continue below).
	for i := 0; i < 60; i++ {
		if err := insertRow(cluster, 0, fmt.Sprintf("%032x", 6000+i), fmt.Sprintf("fresh-%d", i)); err != nil {
			return err
		}
	}
	if err := waitRowCount(cluster, 0, base+65, countTimeout); err != nil {
		return err
	}
	if err := waitRowCount(cluster, 1, base+65, countTimeout); err != nil {
		return err
	}

	// Node 3's 10 s retention pin must lapse, then one GC pass must
	// collect its ranges so the rejoin needs a snapshot. Poll the GC
	// marker instead of a fixed sleep.
	time.Sleep(expirySleep)
	if err := waitGCPass(cluster, 0, gcTimeout); err != nil {
		return err
	}

	// Cap replication throughput, then restart node 3 into it and
	// keep writing while the snapshot transfers. The restart rebinds a
	// fresh replication port, so the filters are re-steered after the
	// restart (replace semantics make the re-apply cheap).
	if !force {
		if err := applyImpairment(replPorts(cluster), "tbf", "rate", "1mbit", "burst", "32kbit", "latency", "400ms"); err != nil {
			return err
		}
	}
	if err := cluster.StartNode(2); err != nil {
		return fmt.Errorf("restart stale node3: %w", err)
	}
	if !force {
		if err := applyImpairment(replPorts(cluster), "tbf", "rate", "1mbit", "burst", "32kbit", "latency", "400ms"); err != nil {
			return err
		}
	}
	if err := cluster.Mesh(); err != nil {
		return fmt.Errorf("re-mesh stale node3: %w", err)
	}
	for i := 0; i < 20; i++ {
		if err := insertRow(cluster, 1, fmt.Sprintf("%032x", 8000+i), fmt.Sprintf("sustain-%d", i)); err != nil {
			return err
		}
	}
	if err := waitConverged(cluster, base+85, cappedTimeout); err != nil {
		return err
	}
	staged, applied, err := logCounters(cluster, 2)
	if err != nil {
		return err
	}
	if staged != 0 || applied != 0 {
		return fmt.Errorf(
			"node3 rejoin arrived over the log path (received=%d applied=%d), want snapshot (log catch-up would hide a snapshot-path regression)",
			staged, applied)
	}
	if err := waitMembership(cluster, 2, membershipTimeout); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "capped resync converged with snapshot path proven")
	if !force {
		clearImpairment()
	}
	return nil
}

func TestImpairedNetworkConverges(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	force := os.Getenv(forceEnv) == "1"
	tcOK, skipReason := tcCapable()
	if !force && !tcOK {
		t.Skipf("impaired-network needs tc/netem + CAP_NET_ADMIN on lo: %s (set %s=1 to run the workload unimpaired)",
			skipReason, forceEnv)
	}
	if force && !tcOK {
		fmt.Fprintf(os.Stderr, "FORCE mode: tc unusable (%s); running workload WITHOUT impairment\n", skipReason)
	}
	rowsN := defaultRows
	if raw := os.Getenv(rowsEnv); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			rowsN = n
		}
	}

	retention := time.Second
	var minimum uint64 = 10
	maximumOffline := 10 * time.Second
	opts := goharness.DefaultClusterOptions("impaired-network")
	opts.NumNodes = 3
	// Aggressive retention so phase (c) can force a snapshot resync
	// after a short offline window; harmless to phases (a)/(b).
	opts.Replication = &goharness.ReplicationOptions{
		Retention:       &retention,
		MinimumRetained: &minimum,
		MaximumOffline:  &maximumOffline,
	}
	opts.BootstrapSeeds = []int{0}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		fmt.Fprintf(os.Stderr, "%s dir=%s sql=%s repl=%s\n",
			info.Label, info.Dir, info.SQLAddr, info.ReplAddr)
	}
	if err := runBody(cluster, force, rowsN); err != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", err)
		// The suite always removes qdisc 77:, on success and on failure.
		if !force {
			clearImpairment()
		}
		cluster.DumpForensics("impaired-network-no-convergence")
		t.Fatalf("scenario failed; artifacts retained under tests-live/failures/: %v", err)
	}
	// The suite always removes qdisc 77:, on success and on failure.
	if !force {
		clearImpairment()
	}
	failed = false
}
