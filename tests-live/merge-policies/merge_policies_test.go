// Live scenario: disconnected PN_COUNTER / OR_SET / MAX / MIN policy
// ops forward across restarts with observed-remove semantics.
//
// Go port of tests/merge_policies.rs using tests-live/goharness.
// Identical behavior: three disconnected murmur-server processes write
// counter/set/extrema ops on one shared row; nodes 0 and 1 connect and
// converge the counter; node 0 stops and node 1 forwards the causal
// operations to node 2; node 2 restarts and recovers; finally an
// observed set remove propagates. The unobserved remove (node 1
// removing "shared" before seeing node 0's add) must not erase the add,
// while the later observed remove must.
//
// Policy ops run as `MURMUR POLICY ...` commands over the wire; set
// projections decode from the stored blob (murmur.DecodeValues: the Go
// server materializes the OR_SET projection as an encoded value vec,
// where the Rust server stores a postcard value vec).
package mergepolicies

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur"
	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// table is the typed table (Go: managed typed record with
// counter/set/extrema fields).
const table = "live_merge_policies"

// baseDDL is the base DDL. `counter`/`tags`/`peak`/`floor` are policy
// columns. Set projections start as `x'00'` (the empty encoding), the
// `tests/` convention for fresh set cells.
const baseDDL = "CREATE TABLE live_merge_policies(name TEXT NOT NULL PRIMARY KEY, counter INTEGER NOT NULL, tags BLOB NOT NULL, peak INTEGER NOT NULL, floor REAL NOT NULL)"

// row is the shared row (Go: `merge-policy-row`, one row id seeded on
// every node).
const row = "merge-policy-row"

// wait is the convergence deadline (Go: 30 s waits).
const wait = 30 * time.Second

// poll is the convergence poll interval (Go: 50 ms polls).
const poll = 50 * time.Millisecond

// mergePolicies maps (table, column) -> merge policy for the typed table
// (Go: PN_COUNTER, OR_SET, numeric MAX/MIN field kinds).
func mergePolicies() map[goharness.MergeColumn]goharness.MergePolicy {
	return map[goharness.MergeColumn]goharness.MergePolicy{
		{Table: table, Column: "counter"}: goharness.MergePolicyCounter,
		{Table: table, Column: "tags"}:    goharness.MergePolicySet,
		{Table: table, Column: "peak"}:    goharness.MergePolicyMax,
		{Table: table, Column: "floor"}:   goharness.MergePolicyMin,
	}
}

// migrateAll migrates every node and flags the table replicated on each
// node (row and policy-op convergence needs the per-node flag). Flagging
// happens before any row exists, so no populated-table rebuild is
// involved.
func migrateAll(cluster *goharness.Cluster) error {
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, baseDDL); err != nil {
			return fmt.Errorf("node%d migrate failed: %w", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple(
			fmt.Sprintf("MURMUR SET TABLE %s REPLICATED true", table)); err != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("node%d flush after SET TABLE: %w", i+1, err)
		}
	}
	return nil
}

// insertRow is a typed insert on one node (Go: `Cluster.TypedInsertWithID`).
func insertRow(cluster *goharness.Cluster, idx int, name string) error {
	if _, err := cluster.Node(idx).Simple(fmt.Sprintf(
		"INSERT INTO %s(name, counter, tags, peak, floor) VALUES('%s', 0, x'00', 0, 0.0)",
		table, name)); err != nil {
		return fmt.Errorf("node%d typed insert failed: %w", idx+1, err)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after typed insert: %w", idx+1, err)
	}
	return nil
}

// selectOne reads the single text cell of a single-row SELECT.
func selectOne(cluster *goharness.Cluster, idx int, sql string) (string, error) {
	rows, err := cluster.Node(idx).Simple(sql)
	if err != nil {
		return "", fmt.Errorf("node%d %s: %w", idx+1, sql, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return "", fmt.Errorf("node%d %s: no rows", idx+1, sql)
	}
	return rows[0][0], nil
}

// counterValue is the PN_COUNTER projection (Go:
// `Cluster.TypedCounterValue`).
func counterValue(cluster *goharness.Cluster, idx int, name string) int64 {
	cell, err := selectOne(cluster, idx,
		fmt.Sprintf("SELECT counter FROM %s WHERE name = '%s'", table, name))
	if err != nil {
		return math.MinInt64
	}
	v, err := strconv.ParseInt(cell, 10, 64)
	if err != nil {
		return math.MinInt64
	}
	return v
}

// setValues is the OR_SET projection, sorted (Go:
// `Cluster.TypedSetValues`). The stored blob renders as `\x<hex>` over the
// simple protocol; hex-decode it and decode the value vec.
func setValues(cluster *goharness.Cluster, idx int, name string) []string {
	cell, err := selectOne(cluster, idx,
		fmt.Sprintf("SELECT tags FROM %s WHERE name = '%s'", table, name))
	if err != nil {
		return []string{"<read-error>"}
	}
	hex := strings.TrimPrefix(cell, `\x`)
	if len(hex)%2 != 0 {
		return []string{}
	}
	raw := make([]byte, len(hex)/2)
	for i := 0; i < len(raw); i++ {
		b, err := strconv.ParseUint(hex[2*i:2*i+2], 16, 8)
		if err != nil {
			return []string{}
		}
		raw[i] = byte(b)
	}
	values, err := murmur.DecodeValues(raw)
	if err != nil {
		values = nil
	}
	var out []string
	for _, v := range values {
		if v.Tag == murmur.ValueText {
			out = append(out, v.S)
		}
	}
	sort.Strings(out)
	return out
}

// extrema is the numeric extrema projection (Go:
// `Cluster.TypedExtremaValues`).
func extrema(cluster *goharness.Cluster, idx int, name string) (int64, float64) {
	rows, err := cluster.Node(idx).Simple(
		fmt.Sprintf("SELECT peak, floor FROM %s WHERE name = '%s'", table, name))
	if err != nil {
		return math.MinInt64, math.NaN()
	}
	if len(rows) == 0 || len(rows[0]) < 2 {
		return math.MinInt64, math.NaN()
	}
	peak, err := strconv.ParseInt(rows[0][0], 10, 64)
	if err != nil {
		peak = math.MinInt64
	}
	floor, err := strconv.ParseFloat(rows[0][1], 64)
	if err != nil {
		floor = math.NaN()
	}
	return peak, floor
}

// policy runs one `MURMUR POLICY ...` op plus a durability barrier.
func policy(cluster *goharness.Cluster, idx int, op string) error {
	if _, err := cluster.Node(idx).Simple(op); err != nil {
		return fmt.Errorf("node%d %s: %w", idx+1, op, err)
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after %s: %w", idx+1, op, err)
	}
	return nil
}

// counterAdd is a PN_COUNTER op (Go: `Cluster.TypedCounterAdd`).
func counterAdd(cluster *goharness.Cluster, idx int, name string, delta int64) error {
	return policy(cluster, idx, fmt.Sprintf(
		"MURMUR POLICY COUNTER_ADD %s counter '%s' %d", table, name, delta))
}

// setAdd is an OR_SET add (Go: `Cluster.TypedSetAdd`).
func setAdd(cluster *goharness.Cluster, idx int, name, value string) error {
	return policy(cluster, idx, fmt.Sprintf(
		"MURMUR POLICY SET_ADD %s tags '%s' '%s'", table, name, value))
}

// setRemove is an OR_SET remove (Go: `Cluster.TypedSetRemove`).
func setRemove(cluster *goharness.Cluster, idx int, name, value string) error {
	return policy(cluster, idx, fmt.Sprintf(
		"MURMUR POLICY SET_REMOVE %s tags '%s' '%s'", table, name, value))
}

// extremaUpdate is a numeric MIN/MAX op (Go:
// `Cluster.TypedExtremaUpdate`).
func extremaUpdate(cluster *goharness.Cluster, idx int, name string, peak int64, floor float64) error {
	if err := policy(cluster, idx, fmt.Sprintf(
		"MURMUR POLICY MAX %s peak '%s' %d", table, name, peak)); err != nil {
		return err
	}
	return policy(cluster, idx, fmt.Sprintf(
		"MURMUR POLICY MIN %s floor '%s' %v", table, name, floor))
}

func waitCounter(cluster *goharness.Cluster, node int, name string, want int64) error {
	converged := goharness.WaitUntil(wait, poll, func() bool {
		return counterValue(cluster, node, name) == want
	})
	if !converged {
		return fmt.Errorf("node%d counter for %q = %d, want %d",
			node+1, name, counterValue(cluster, node, name), want)
	}
	return nil
}

// waitPolicyProjection is the combined set + extrema projection wait (Go:
// `waitPolicyProjection`).
func waitPolicyProjection(cluster *goharness.Cluster, node int, name string, wantTags []string, wantPeak int64, wantFloor float64) error {
	converged := goharness.WaitUntil(wait, poll, func() bool {
		if !equalStrings(setValues(cluster, node, name), wantTags) {
			return false
		}
		peak, floor := extrema(cluster, node, name)
		return peak == wantPeak && floor == wantFloor
	})
	if !converged {
		peak, floor := extrema(cluster, node, name)
		return fmt.Errorf(
			"node%d projection tags=%v extrema=(%d,%v); want tags=%v extrema=(%d,%v)",
			node+1, setValues(cluster, node, name), peak, floor, wantTags, wantPeak, wantFloor)
	}
	return nil
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

func runBody(cluster *goharness.Cluster) error {
	if err := migrateAll(cluster); err != nil {
		return err
	}

	// Every node seeds the same logical row while disconnected (Go:
	// TypedInsertWithID with one shared row id on all nodes).
	for i := 0; i < cluster.Len(); i++ {
		if err := insertRow(cluster, i, row); err != nil {
			return err
		}
	}

	// Each node makes independent typed CRDT updates while disconnected.
	if err := counterAdd(cluster, 0, row, 10); err != nil {
		return err
	}
	if err := setAdd(cluster, 0, row, "shared"); err != nil {
		return err
	}
	if err := setAdd(cluster, 0, row, "red"); err != nil {
		return err
	}
	if err := extremaUpdate(cluster, 0, row, 10, 30.0); err != nil {
		return err
	}

	if err := counterAdd(cluster, 1, row, 7); err != nil {
		return err
	}
	// This remove has not observed node 0's add and must not erase it.
	if err := setRemove(cluster, 1, row, "shared"); err != nil {
		return err
	}
	if err := setAdd(cluster, 1, row, "blue"); err != nil {
		return err
	}
	if err := extremaUpdate(cluster, 1, row, 20, 15.0); err != nil {
		return err
	}

	if err := counterAdd(cluster, 2, row, -2); err != nil {
		return err
	}
	if err := setAdd(cluster, 2, row, "green"); err != nil {
		return err
	}
	if err := extremaUpdate(cluster, 2, row, 5, -7.0); err != nil {
		return err
	}

	// Nodes 0 and 1 connect (node 2 stays isolated); their counters sum.
	if err := cluster.AddPeer(0, 1); err != nil {
		return fmt.Errorf("connect 0 -> 1: %w", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		return fmt.Errorf("connect 1 -> 0: %w", err)
	}
	if err := cluster.SyncAll(); err != nil {
		return fmt.Errorf("sync after 0<->1 connect: %w", err)
	}
	if err := waitCounter(cluster, 0, row, 17); err != nil {
		return err
	}
	if err := waitCounter(cluster, 1, row, 17); err != nil {
		return err
	}

	// The original writer stops; node 1 forwards its causal operations to
	// node 2.
	cluster.StopNode(0)
	if err := cluster.AddPeer(1, 2); err != nil {
		return fmt.Errorf("connect 1 -> 2: %w", err)
	}
	if err := cluster.AddPeer(2, 1); err != nil {
		return fmt.Errorf("connect 2 -> 1: %w", err)
	}
	if err := cluster.SyncAll(); err != nil {
		return fmt.Errorf("sync after 1<->2 connect: %w", err)
	}
	if err := waitCounter(cluster, 1, row, 15); err != nil {
		return err
	}
	if err := waitCounter(cluster, 2, row, 15); err != nil {
		return err
	}
	if err := waitPolicyProjection(cluster, 1, row,
		[]string{"blue", "green", "red", "shared"}, 20, -7.0); err != nil {
		return err
	}
	if err := waitPolicyProjection(cluster, 2, row,
		[]string{"blue", "green", "red", "shared"}, 20, -7.0); err != nil {
		return err
	}

	// Node 2 restarts and recovers the merged projections from durable
	// state. The restart rebinds fresh ports, so the 1 <-> 2 edges are
	// re-added explicitly (peers still dial the old address otherwise).
	cluster.StopNode(2)
	if err := cluster.StartNode(2); err != nil {
		return fmt.Errorf("restart node 2: %w", err)
	}
	if err := cluster.AddPeer(1, 2); err != nil {
		return fmt.Errorf("reconnect 1 -> 2: %w", err)
	}
	if err := cluster.AddPeer(2, 1); err != nil {
		return fmt.Errorf("reconnect 2 -> 1: %w", err)
	}
	if err := cluster.SyncAll(); err != nil {
		return fmt.Errorf("sync after node 2 restart: %w", err)
	}
	if err := waitCounter(cluster, 2, row, 15); err != nil {
		return err
	}
	if err := waitPolicyProjection(cluster, 2, row,
		[]string{"blue", "green", "red", "shared"}, 20, -7.0); err != nil {
		return err
	}

	// A later observed remove must propagate after restart.
	if err := setRemove(cluster, 1, row, "shared"); err != nil {
		return err
	}
	if err := waitPolicyProjection(cluster, 2, row,
		[]string{"blue", "green", "red"}, 20, -7.0); err != nil {
		return err
	}
	return nil
}

func TestDisconnectedPoliciesForwardAndRestart(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("merge-policies")
	opts.NumNodes = 3
	opts.ManualPeers = true
	opts.Replication = &goharness.ReplicationOptions{MergePolicies: mergePolicies()}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()
	if cluster.Len() != 3 {
		t.Fatal("this scenario is written for 3 nodes")
	}
	if err := runBody(cluster); err != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", err)
		cluster.DumpForensics("merge-policies-no-convergence")
		t.Fatalf("scenario failed; artifacts retained under tests-live/failures/: %v", err)
	}
	failed = false
}
