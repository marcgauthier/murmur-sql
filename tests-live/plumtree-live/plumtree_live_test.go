// Live scenario: a three-node all-Plumtree mesh converges end to end,
// while a gossip-mode node meshed with Plumtree peers stays isolated in
// both directions.
//
// Go port of tests/plumtree_live.rs using tests-live/goharness. Identical
// behavior: `plumtree_mesh_converges` requires 10 rows to converge on all
// three Plumtree nodes; `mixed_mode_refuses_gossip_peer` requires the
// Plumtree pair to converge while the gossip node keeps only its own row,
// handshake incompatibility refusals are recorded, and the gossip node
// holds zero sessions. The Rust wait_until_async polls become
// goharness.WaitUntil.
package plumtreelive

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Rows inserted by the origin node (Go: 10 per test).
const rows = 10

// All-Plumtree convergence deadline (Go: 60 s).
const convergeTimeout = 60 * time.Second

// Mixed-mode pair-convergence deadline (Go: 30 s).
const mixedTimeout = 30 * time.Second

// Convergence poll interval (Go: 200 ms).
const pollInterval = 200 * time.Millisecond

// Leak window after mixed-mode convergence (Go: 5 s).
const leakWindow = 5 * time.Second

// names returns the canonical ordered names on one node (Go:
// `Cluster.TypedNames`).
func names(cluster *goharness.Cluster, idx int) ([]string, error) {
	rows, err := cluster.Node(idx).Query("SELECT name FROM records ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("node%d SELECT names: %w", idx+1, err)
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.Get(0))
	}
	return out, nil
}

// migrateAll creates the same `records` table on every node (Go:
// TypedRecords fixture tables), flagged replicated on each node (row
// convergence needs the per-node flag).
func migrateAll(cluster *goharness.Cluster) error {
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
	return nil
}

func insertNames(cluster *goharness.Cluster, idx int, prefix string, count int) error {
	for row := 0; row < count; row++ {
		name := fmt.Sprintf("%s-%d", prefix, row)
		if _, err := cluster.Node(idx).Execute(
			"INSERT INTO records(id, name) VALUES($1, $2)", int64(row), name); err != nil {
			return fmt.Errorf("node%d insert '%s': %w", idx+1, name, err)
		}
	}
	if _, err := cluster.Node(idx).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after inserts: %w", idx+1, err)
	}
	return nil
}

// replicationSummary reads one `MURMUR SHOW REPLICATION` row:
// (peers, last_error).
func replicationSummary(cluster *goharness.Cluster, idx int) (uint64, string, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return 0, "", fmt.Errorf("node%d SHOW REPLICATION: %w", idx+1, err)
	}
	if len(rows) == 0 {
		return 0, "", fmt.Errorf("node%d SHOW REPLICATION returned no rows", idx+1)
	}
	row := rows[0]
	if len(row) < 7 {
		return 0, "", fmt.Errorf("node%d SHOW REPLICATION has %d columns, want >= 7", idx+1, len(row))
	}
	peers, err := strconv.ParseUint(row[2], 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("node%d peers %q: %w", idx+1, row[2], err)
	}
	return peers, row[6], nil
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

func runConverges(cluster *goharness.Cluster) error {
	if err := migrateAll(cluster); err != nil {
		return err
	}
	if err := insertNames(cluster, 0, "plum", rows); err != nil {
		return err
	}

	want := make([]string, 0, rows)
	for row := 0; row < rows; row++ {
		want = append(want, fmt.Sprintf("plum-%d", row))
	}
	converged := goharness.WaitUntil(convergeTimeout, pollInterval, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			got, err := names(cluster, i)
			if err != nil || !equalStrings(got, want) {
				return false
			}
		}
		return true
	})
	if !converged {
		return fmt.Errorf("three Plumtree nodes did not converge on %d rows within %v", rows, convergeTimeout)
	}
	fmt.Fprintf(os.Stderr, "Three Plumtree nodes converged on %d rows\n", rows)
	return nil
}

func runMixed(cluster *goharness.Cluster) error {
	if err := migrateAll(cluster); err != nil {
		return err
	}
	if err := insertNames(cluster, 0, "mixed-plum", rows); err != nil {
		return err
	}
	if err := insertNames(cluster, 2, "gossip-only", 1); err != nil {
		return err
	}

	// The Plumtree pair converges; the gossip node stays isolated.
	pairConverged := goharness.WaitUntil(mixedTimeout, pollInterval, func() bool {
		n0, err0 := names(cluster, 0)
		n1, err1 := names(cluster, 1)
		return err0 == nil && err1 == nil && len(n0) == rows && len(n1) == rows
	})
	if !pairConverged {
		return fmt.Errorf("Plumtree pair did not converge on %d rows within %v", rows, mixedTimeout)
	}
	time.Sleep(leakWindow) // let any leak arrive
	for i, want := range []int{rows, rows, 1} {
		got, err := names(cluster, i)
		if err != nil {
			return err
		}
		if len(got) != want {
			return fmt.Errorf("node%d count = %d, want %d (mode isolation broken?)", i+1, len(got), want)
		}
	}

	// Refusal evidence (Go: `spedsql_repl_handshake_capability_refusals_total >= 1`).
	// The engine fails closed with `incompatible hello/welcome`, latched in the
	// `last_error` column of `MURMUR SHOW REPLICATION` on both dial and accept sides.
	refusals := 0
	for i := 0; i < cluster.Len(); i++ {
		peers, lastError, err := replicationSummary(cluster, i)
		if err != nil {
			return err
		}
		if strings.Contains(lastError, "incompatible") {
			refusals++
		}
		fmt.Fprintf(os.Stderr, "node%d: peers=%d last_error=%q\n", i+1, peers, lastError)
	}
	if refusals < 1 {
		return fmt.Errorf("no handshake incompatibility refusals recorded (negotiation silent?)")
	}
	fmt.Fprintf(os.Stderr, "mode refusals recorded on %d node(s)\n", refusals)

	// Session-level isolation: the gossip node never holds a session,
	// while the converged Plumtree pair holds exactly each other.
	gossipPeers, _, err := replicationSummary(cluster, 2)
	if err != nil {
		return err
	}
	if gossipPeers != 0 {
		return fmt.Errorf("gossip node holds %d sessions, want 0 (mode isolation broken?)", gossipPeers)
	}
	for i := 0; i < 2; i++ {
		peers, _, err := replicationSummary(cluster, i)
		if err != nil {
			return err
		}
		if peers != 1 {
			return fmt.Errorf("plumtree node%d holds %d sessions, want 1 (peer of the gossip node?)", i, peers)
		}
	}
	return nil
}

func TestPlumtreeMeshConverges(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("plumtree-live")
	plumtree := replication.DisseminationPlumtree
	opts.Replication = &goharness.ReplicationOptions{Dissemination: &plumtree}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 3 {
		t.Fatalf("this scenario is written for 3 nodes, got %d", cluster.Len())
	}
	failed := true
	defer func() { cluster.Finish(failed) }()
	if err := runConverges(cluster); err != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", err)
		cluster.DumpForensics("no-convergence")
		t.Fatalf("scenario failed: %v", err)
	}
	failed = false
}

func TestMixedModeRefusesGossipPeer(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("plumtree-mixed")
	plumtree := replication.DisseminationPlumtree
	opts.Replication = &goharness.ReplicationOptions{Dissemination: &plumtree}
	opts.DisseminationByNode = map[int]replication.Dissemination{2: replication.DisseminationGossip}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 3 {
		t.Fatalf("this scenario is written for 3 nodes, got %d", cluster.Len())
	}
	failed := true
	defer func() { cluster.Finish(failed) }()
	if err := runMixed(cluster); err != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", err)
		cluster.DumpForensics("mode-isolation")
		t.Fatalf("scenario failed: %v", err)
	}
	failed = false
}
