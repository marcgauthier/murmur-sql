// Live scenario: heal order must not affect the converged state.
//
// Go port of the Rust `three-way-heal` scenario
// (/home/marc/MURMUR/tests-live/three-way-heal/tests/three_way_heal.rs).
// Four fully meshed nodes split into isolated pairs, diverge with
// deterministic writes (disjoint inserts plus conflicting updates to the
// same base rows), then heal link-by-link in a caller-supplied order. Two
// different heal orders must reach the identical final digest, including
// identical LWW conflict winners. Plain SQL replaces the typed-records
// HTTP API.
package threewayheal

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

const (
	// pollInterval is the convergence poll interval (Rust: 200 ms).
	pollInterval = 200 * time.Millisecond
	// convergeTimeout is the baseline / per-side / final convergence
	// deadline (Rust: 150 s each).
	convergeTimeout = 150 * time.Second
	// linkFlowTimeout is the cross-side traffic deadline after each
	// healed link (Rust: 30 s).
	linkFlowTimeout = 30 * time.Second
	// linkFlowPoll is the cross-side traffic poll interval (Rust: 50 ms).
	linkFlowPoll = 50 * time.Millisecond
	// progressLogInterval is the progress-log cadence inside convergence
	// waits (Rust: 10 s).
	progressLogInterval = 10 * time.Second

	// baseID is the baseline row id base (Rust: 1000+i, 10 rows).
	baseID = 1000
	// sideAID is the side A disjoint-insert id base, written on node 1
	// (Rust: 2000+i).
	sideAID = 2000
	// sideBID is the side B disjoint-insert id base, written on node 3
	// (Rust: 3000+i).
	sideBID = 3000
	// batchRows is the rows per write batch (Rust: 10).
	batchRows = 10
)

// crossLinks spans the (0,1)|(2,3) split; a heal order is a permutation
// of indexes into this table (Rust: crossLinks).
var crossLinks = [][2]int{{0, 2}, {0, 3}, {1, 2}, {1, 3}}

// row is one (id, name) record.
type row struct {
	id   int64
	name string
}

// rows reads all (id, name) rows on one node, ordered by id (Rust:
// TypedContentionRows).
func rows(t *testing.T, db *murmur.Database) []row {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name FROM records ORDER BY id")
	})
	if err != nil {
		t.Fatalf("read contention rows: %v", err)
	}
	var out []row
	for _, r := range v.([][]sqlite.Value) {
		out = append(out, row{id: r[0].Int, name: r[1].Str})
	}
	return out
}

// digestRows is a deterministic digest over the canonical row encoding:
// FNV-1a 64, hex-rendered. Std-only stand-in for sha256 hex; what the
// test asserts is digest equality across heal orders, which this
// preserves.
func digestRows(nodeRows []row) string {
	var hash uint64 = 0xcbf29ce484222325
	for _, r := range nodeRows {
		for _, b := range []byte(fmt.Sprintf("%d\x00%s\x00", r.id, r.name)) {
			hash ^= uint64(b)
			hash *= 0x100000001b3
		}
	}
	return fmt.Sprintf("%016x", hash)
}

// insert writes one row (Rust: insertRow).
func insert(t *testing.T, cluster *harness.Cluster, node int, id int64, name string) {
	t.Helper()
	_, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	})
	if err != nil {
		t.Fatalf("node%d insert id %d failed: %v", node+1, id, err)
	}
	if err := cluster.Node(node).Flush(); err != nil {
		t.Fatalf("node%d flush after insert id %d failed: %v", node+1, id, err)
	}
}

// updateName applies a conflicting update to one base row (Rust:
// TypedContentionUpdate).
func updateName(t *testing.T, cluster *harness.Cluster, node int, id int64, name string) {
	t.Helper()
	_, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		changed, err := s.Execute("UPDATE records SET name = ? WHERE id = ?", name, id)
		if err != nil {
			return nil, err
		}
		if changed != 1 {
			t.Fatalf("conflict update of id %d hit %d rows", id, changed)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("node%d conflict update id %d failed: %v", node+1, id, err)
	}
	if err := cluster.Node(node).Flush(); err != nil {
		t.Fatalf("node%d flush after conflict update id %d failed: %v", node+1, id, err)
	}
}

// hasID reports whether one node currently holds a row id (Rust: hasRow).
func hasID(t *testing.T, cluster *harness.Cluster, idx int, id int64) bool {
	t.Helper()
	for _, r := range rows(t, cluster.Node(idx)) {
		if r.id == id {
			return true
		}
	}
	return false
}

// waitConvergedOn waits until the indexed nodes hold exactly want rows
// with equal digests (Rust: waitConvergedOn).
func waitConvergedOn(t *testing.T, cluster *harness.Cluster, idxs []int, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	for {
		ok := true
		first := ""
		for k, idx := range idxs {
			nodeRows := rows(t, cluster.Node(idx))
			if len(nodeRows) != want {
				ok = false
				break
			}
			digest := digestRows(nodeRows)
			if k == 0 {
				first = digest
			} else if digest != first {
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
			counts := make([]int, 0, cluster.Len())
			for i := 0; i < cluster.Len(); i++ {
				counts = append(counts, len(rows(t, cluster.Node(i))))
			}
			t.Logf("converge progress: counts=%v want=%d", counts, want)
		}
		time.Sleep(pollInterval)
	}
	for _, idx := range idxs {
		nodeRows := rows(t, cluster.Node(idx))
		t.Logf("node%d at timeout: count=%d digest=%s", idx+1, len(nodeRows), digestRows(nodeRows))
	}
	t.Fatalf("nodes %v did not converge on %d rows with equal digests within %v", idxs, want, timeout)
}

// waitConverged waits until all four nodes hold exactly want rows with
// equal digests (Rust: waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	waitConvergedOn(t, cluster, []int{0, 1, 2, 3}, want, timeout)
}

// waitLinkFlowed blocks until a side-originated row is visible across the
// newly healed link in either direction: a sideA row (id 2000, written
// on node 1) on the sideB endpoint, or a sideB row (id 3000, written on
// node 3) on the sideA endpoint (Rust: waitLinkFlowed).
func waitLinkFlowed(t *testing.T, cluster *harness.Cluster, order []int, link [2]int) {
	t.Helper()
	deadline := time.Now().Add(linkFlowTimeout)
	for time.Now().Before(deadline) {
		if hasID(t, cluster, link[1], sideAID) || hasID(t, cluster, link[0], sideBID) {
			return
		}
		time.Sleep(linkFlowPoll)
	}
	t.Fatalf("order %v: no cross-side traffic on healed link %v within %v", order, link, linkFlowTimeout)
}

// parseOrder reads a comma-separated permutation of link indexes; invalid
// values fail fast rather than silently healing another order (Rust:
// parseOrder).
func parseOrder(t *testing.T, env, fallback string) []int {
	t.Helper()
	raw := os.Getenv(env)
	if raw == "" {
		raw = fallback
	}
	parts := strings.Split(raw, ",")
	if len(parts) != len(crossLinks) {
		t.Fatalf("%s=%q: want %d comma-separated link indexes", env, raw, len(crossLinks))
	}
	seen := make([]bool, len(crossLinks))
	order := make([]int, 0, len(parts))
	for _, part := range parts {
		parsed, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || parsed < 0 || parsed >= len(crossLinks) || seen[parsed] {
			t.Fatalf("%s=%q: want a permutation of 0..%d", env, raw, len(crossLinks)-1)
		}
		seen[parsed] = true
		order = append(order, parsed)
	}
	return order
}

// runHealOrder runs one full split/diverge/heal cycle in the given link
// order and returns the final digest (Rust: runHealOrder).
func runHealOrder(t *testing.T, order []int, tag string) string {
	t.Helper()
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "three-way-heal-" + tag,
		NumNodes: 4,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("three-way-heal-no-convergence")
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

	// Baseline converges before the split (honest-path control).
	// IDs/names are fixed so both heal orders must reach the same digest.
	for i := 0; i < batchRows; i++ {
		insert(t, cluster, 0, baseID+int64(i), fmt.Sprintf("base-%d", i))
	}
	waitConverged(t, cluster, batchRows, convergeTimeout)

	// Split into isolated pairs (0,1) and (2,3).
	for _, link := range crossLinks {
		if err := cluster.RemovePeer(link[0], link[1]); err != nil {
			t.Fatalf("order %v split %v: %v", order, link, err)
		}
		if err := cluster.RemovePeer(link[1], link[0]); err != nil {
			t.Fatalf("order %v split %v: %v", order, link, err)
		}
	}

	// Deterministic divergent writes per side: disjoint inserts plus
	// CONFLICTING updates to the same base rows. Union-merge alone
	// cannot prove heal-order independence where it matters; LWW must
	// pick the same winner per cell regardless of arrival order. Side B
	// writes strictly after side A (sequential loops), so the
	// relative HLC order (and hence the winner) is deterministic
	// across runs.
	for i := 0; i < batchRows; i++ {
		insert(t, cluster, 1, sideAID+int64(i), fmt.Sprintf("sideA-%d", i))
		insert(t, cluster, 3, sideBID+int64(i), fmt.Sprintf("sideB-%d", i))
	}
	for i := 0; i < batchRows; i++ {
		updateName(t, cluster, 1, baseID+int64(i), fmt.Sprintf("conflict-A-%d", i))
	}
	for i := 0; i < batchRows; i++ {
		updateName(t, cluster, 3, baseID+int64(i), fmt.Sprintf("conflict-B-%d", i))
	}

	// Each pair converges internally; equal counts but divergent
	// digests prove the split held (otherwise this test would be vacuous).
	waitConvergedOn(t, cluster, []int{0, 1}, 20, convergeTimeout)
	waitConvergedOn(t, cluster, []int{2, 3}, 20, convergeTimeout)
	digestA := digestRows(rows(t, cluster.Node(0)))
	digestB := digestRows(rows(t, cluster.Node(2)))
	if digestA == digestB {
		t.Fatalf("order %v: sides unexpectedly agree during split (%s)", order, digestA)
	}

	// Heal link-by-link in the prescribed order. After each heal we
	// wait for real cross-side traffic on the new link (not a blind
	// sleep that could elapse before anything flows, making the
	// orders effectively identical).
	for step, linkIdx := range order {
		link := crossLinks[linkIdx]
		if err := cluster.AddPeer(link[0], link[1]); err != nil {
			t.Fatalf("order %v heal %v: %v", order, link, err)
		}
		if err := cluster.AddPeer(link[1], link[0]); err != nil {
			t.Fatalf("order %v heal %v: %v", order, link, err)
		}
		t.Logf("order %v: healed link %d/%d (%d<->%d)", order, step+1, len(order), link[0], link[1])
		waitLinkFlowed(t, cluster, order, link)
	}

	waitConverged(t, cluster, 30, convergeTimeout)
	nodeRows := rows(t, cluster.Node(0))
	// Log the LWW winner on a conflicted row: the digest comparison
	// across orders is the assertion, this names the value for forensics.
	finalDigest := digestRows(nodeRows)
	for _, r := range nodeRows {
		if r.id == baseID {
			t.Logf("order %v: conflict winner row %d = %s", order, baseID, r.name)
			break
		}
	}
	failed = false
	return finalDigest
}

func TestHealOrderDoesNotAffectFinalDigest(t *testing.T) {
	orderA := parseOrder(t, "MURMUR_THREE_WAY_HEAL_ORDER", "0,1,2,3")
	orderB := parseOrder(t, "MURMUR_THREE_WAY_HEAL_ORDER_B", "3,2,1,0")

	digestA := runHealOrder(t, orderA, "a")
	digestB := runHealOrder(t, orderB, "b")
	t.Logf("final digest order %v: %s", orderA, digestA)
	t.Logf("final digest order %v: %s", orderB, digestB)
	if digestA != digestB {
		t.Fatalf("heal-order divergence: order %v -> %s, order %v -> %s", orderA, digestA, orderB, digestB)
	}
}
