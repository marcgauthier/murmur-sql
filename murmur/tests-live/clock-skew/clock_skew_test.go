// Live scenario: skewed clocks resolve identical LWW winners.
//
// Go port of the Rust `clock-skew` scenario
// (/home/marc/MURMUR/tests-live/clock-skew/tests/clock_skew.rs), itself a
// test-side equivalent of the original Go libfaketime scenario: per-node
// clocks are impossible in-process (one clock per process), so this port
// proves the same property deterministically — real exported replica states
// with synthetically skewed version stamps (±5 min) merge to identical
// winners regardless of merge order. Four unlinked nodes (manual peers, no
// links) keep merge_state as the only sync path.
package clockskew

import (
	"fmt"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// tableDDL is the contention table (Rust: skew(id, val)).
const tableDDL = "CREATE TABLE skew(id TEXT NOT NULL PRIMARY KEY, val TEXT NOT NULL)"

// rows is the contended row count (Rust: 20).
const rows = 20

// skewMillis is the synthetic clock skew applied to exported versions
// (Rust: ±5 min, both signs).
const skewMillis = int64(5 * 60 * 1000)

// shiftVersion shifts one HLC stamp by a signed skew (saturating;
// node/logical kept).
func shiftVersion(v murmur.Version, delta int64) murmur.Version {
	if delta >= 0 {
		v.Millis += uint64(delta)
		if v.Millis < uint64(delta) {
			v.Millis = ^uint64(0)
		}
	} else if sub := uint64(-delta); sub > v.Millis {
		v.Millis = 0
	} else {
		v.Millis -= sub
	}
	return v
}

// shiftState clones an exported state and shifts every version stamp in it:
// row generations, cell versions, tombstones, and the state clock. The
// result simulates "this replica's clock was off by delta" with
// byte-identical content (Rust: shift_state).
func shiftState(s *murmur.ReplicaState, delta int64) *murmur.ReplicaState {
	out := *s
	out.Rows = make([]murmur.RowState, len(s.Rows))
	for i, row := range s.Rows {
		out.Rows[i] = row
		out.Rows[i].Key = append([]murmur.Value(nil), row.Key...)
		out.Rows[i].Generation = shiftVersion(row.Generation, delta)
		out.Rows[i].Cells = make([]murmur.Cell, len(row.Cells))
		for j, cell := range row.Cells {
			out.Rows[i].Cells[j] = cell
			out.Rows[i].Cells[j].Version = shiftVersion(cell.Version, delta)
		}
	}
	out.Tombstones = make([]murmur.Tombstone, len(s.Tombstones))
	for i, t := range s.Tombstones {
		out.Tombstones[i] = t
		out.Tombstones[i].Key = append([]byte(nil), t.Key...)
		out.Tombstones[i].Generation = shiftVersion(t.Generation, delta)
		out.Tombstones[i].Version = shiftVersion(t.Version, delta)
	}
	out.Clock = shiftVersion(s.Clock, delta)
	return &out
}

// insertRow inserts one row (Rust: insert_row).
func insertRow(t *testing.T, db *murmur.Database, node int, id, val string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO skew(id, val) VALUES(?, ?)", id, val)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after insert failed: %v", node, err)
	}
}

// readVal point-reads one row by id, returning ("", false) when missing
// (Rust: read_val).
func readVal(t *testing.T, db *murmur.Database, id string) (string, bool) {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT val FROM skew WHERE id = ?", id)
	})
	if err != nil {
		return "", false
	}
	found := v.([][]sqlite.Value)
	if len(found) == 0 {
		return "", false
	}
	return found[0][0].Str, true
}

// assertRows requires every contended row to read want(i) on idx (Rust:
// assert_rows).
func assertRows(t *testing.T, cluster *harness.Cluster, idx int, want func(i int) string, what string) {
	t.Helper()
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("skew-%02d", i)
		got, ok := readVal(t, cluster.Node(idx), id)
		if !ok || got != want(i) {
			t.Fatalf("node%d row %s after %s: got %q, want %q", idx+1, id, what, got, want(i))
		}
	}
}

// TestSkewedClocksResolveIdenticalWinners merges synthetically skewed
// exports and requires identical LWW winners regardless of merge order
// (Rust: skewed_clocks_resolve_identical_winners).
func TestSkewedClocksResolveIdenticalWinners(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        "clock-skew",
		NumNodes:    4,
		ManualPeers: true,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("clock-skew-no-convergence")
		}
		cluster.Finish(failed)
	}()

	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}

	// Same schema on every node.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).Migrate(murmur.NewMigration(tableDDL)); err != nil {
			t.Fatalf("node%d migrate skew table: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush schema: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("skew", true); err != nil {
			t.Fatalf("node%d opt-in skew: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}

	// Concurrent same-key writes from two nodes at (near-)identical wall
	// times; exports are pristine inputs for every leg below.
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("skew-%02d", i)
		insertRow(t, cluster.Node(0), 1, id, fmt.Sprintf("from-a-%02d", i))
		insertRow(t, cluster.Node(1), 2, id, fmt.Sprintf("from-b-%02d", i))
	}
	a0, err := cluster.Node(0).ExportState()
	if err != nil {
		t.Fatalf("export A: %v", err)
	}
	b0, err := cluster.Node(1).ExportState()
	if err != nil {
		t.Fatalf("export B: %v", err)
	}
	if len(a0.Rows) != rows {
		t.Fatalf("export A has %d rows, want %d", len(a0.Rows), rows)
	}
	if len(b0.Rows) != rows {
		t.Fatalf("export B has %d rows, want %d", len(b0.Rows), rows)
	}

	// Fast clock wins: A's state with +5 min skew merged into B beats B's
	// local present-time writes on every row.
	if err := cluster.Node(1).MergeState(shiftState(a0, skewMillis)); err != nil {
		t.Fatalf("merge skewed-fast A into B: %v", err)
	}
	assertRows(t, cluster, 1, func(i int) string { return fmt.Sprintf("from-a-%02d", i) }, "fast-clock merge")

	// Slow clock loses: B's state with -5 min skew merged into A loses to
	// A's local present-time writes on every row.
	if err := cluster.Node(0).MergeState(shiftState(b0, -skewMillis)); err != nil {
		t.Fatalf("merge skewed-slow B into A: %v", err)
	}
	assertRows(t, cluster, 0, func(i int) string { return fmt.Sprintf("from-a-%02d", i) }, "slow-clock merge")

	// Merge order is irrelevant: C merges [present, skewed-fast], D merges
	// [skewed-fast, present]; both must hold identical winners (the
	// skewed-fast values) on every row.
	if err := cluster.Node(2).MergeState(shiftState(b0, 0)); err != nil {
		t.Fatalf("merge present B into C: %v", err)
	}
	if err := cluster.Node(2).MergeState(shiftState(a0, skewMillis)); err != nil {
		t.Fatalf("merge skewed-fast A into C: %v", err)
	}
	if err := cluster.Node(3).MergeState(shiftState(a0, skewMillis)); err != nil {
		t.Fatalf("merge skewed-fast A into D: %v", err)
	}
	if err := cluster.Node(3).MergeState(shiftState(b0, 0)); err != nil {
		t.Fatalf("merge present B into D: %v", err)
	}
	assertRows(t, cluster, 2, func(i int) string { return fmt.Sprintf("from-a-%02d", i) }, "present-then-fast order")
	assertRows(t, cluster, 3, func(i int) string { return fmt.Sprintf("from-a-%02d", i) }, "fast-then-present order")
	t.Log("PASS: ±5min skew resolves to identical winners regardless of merge order")

	failed = false
}
