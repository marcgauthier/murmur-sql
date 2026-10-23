// Live scenario: three nodes start from one replicated row, apply
// concurrent disjoint-column updates (all must survive), then apply 40
// competing updates per node to one shared cell; the shared-cell winner
// must be identical everywhere and full rows must match.
//
// Go port of the Rust `crdt-contention` scenario
// (/home/marc/MURMUR/tests-live/crdt-contention/tests/crdt_contention.rs).
// Plain SQL replaces the typed-records HTTP API.
package crdtcontention

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// rowID is the contended row id (Rust: ROW_ID).
	rowID = "03141592-6000-4000-8000-000000000000"
	// updatesPerNode is the competing updates per node on the shared
	// cell (Rust: UPDATES_PER_NODE = 40).
	updatesPerNode = 40
	// peersTimeout is the peer-mesh deadline (Rust: 20 s).
	peersTimeout = 20 * time.Second
	// stateTimeout bounds the baseline / disjoint-state waits (Rust: 20 s).
	stateTimeout = 20 * time.Second
	// convergenceTimeout bounds shared-cell convergence (Rust: 30 s).
	convergenceTimeout = 30 * time.Second
	// pollInterval is the convergence poll interval (Rust: 50 ms).
	pollInterval = 50 * time.Millisecond
)

// contentionRow is one application row (Rust: Row).
type contentionRow struct {
	name  string
	phone string
	score int64
}

// readRow reads the contended row on one node (Rust: read_row).
func readRow(t *testing.T, db *murmur.Database) (contentionRow, error) {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name, phone, score FROM contention WHERE id = ?", rowID)
	})
	if err != nil {
		return contentionRow{}, fmt.Errorf("read row %s failed: %w", rowID, err)
	}
	got := v.([][]sqlite.Value)
	if len(got) == 0 {
		return contentionRow{}, fmt.Errorf("row %s missing", rowID)
	}
	return contentionRow{name: got[0][0].Str, phone: got[0][1].Str, score: got[0][2].Int}, nil
}

// insertBaseline inserts the baseline row on one node (Rust: insert_baseline).
func insertBaseline(t *testing.T, db *murmur.Database, row contentionRow) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
			rowID, row.name, row.phone, row.score)
		return nil, err
	}); err != nil {
		t.Fatalf("baseline insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after baseline insert: %v", err)
	}
}

// updateText applies one TEXT cell update (Rust: update_text). It returns
// an error instead of failing: workers run off the test goroutine, where
// FailNow is forbidden.
func updateText(db *murmur.Database, node int, column, value string) error {
	statement := fmt.Sprintf("UPDATE contention SET %s = ? WHERE id = ?", column)
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		changed, err := s.Execute(statement, value, rowID)
		if err != nil {
			return nil, err
		}
		if changed != 1 {
			return nil, fmt.Errorf("node%d %s update hit %d rows", node, column, changed)
		}
		return nil, nil
	}); err != nil {
		return fmt.Errorf("node%d %s update failed: %w", node, column, err)
	}
	if err := db.Flush(); err != nil {
		return fmt.Errorf("node%d flush after %s update failed: %w", node, column, err)
	}
	return nil
}

// updateScore applies one INTEGER cell update (Rust: update_score). It
// returns an error instead of failing (see updateText).
func updateScore(db *murmur.Database, node int, value int64) error {
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		changed, err := s.Execute("UPDATE contention SET score = ? WHERE id = ?", value, rowID)
		if err != nil {
			return nil, err
		}
		if changed != 1 {
			return nil, fmt.Errorf("node%d score update hit %d rows", node, changed)
		}
		return nil, nil
	}); err != nil {
		return fmt.Errorf("node%d score update failed: %w", node, err)
	}
	if err := db.Flush(); err != nil {
		return fmt.Errorf("node%d flush after score update failed: %w", node, err)
	}
	return nil
}

// connectedPeers counts live QUIC connections on one node (Rust:
// connected_peers, via peers diagnostics).
func connectedPeers(cluster *harness.Cluster, node int) int {
	h := cluster.Node(node).Replication()
	if h == nil {
		return 0
	}
	n := 0
	for _, d := range h.Status().PeersDiagnostics {
		if d.Connected {
			n++
		}
	}
	return n
}

// waitForPeers waits until every node reports want live peers (Rust:
// wait_for_peers).
func waitForPeers(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	converged := harness.WaitUntil(timeout, pollInterval, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if connectedPeers(cluster, i) != want {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("peer mesh failed to reach %d peers/node within %v", want, timeout)
	}
}

// waitForState waits until every node holds exactly want (Rust: wait_for_state).
func waitForState(t *testing.T, cluster *harness.Cluster, want contentionRow, timeout time.Duration) {
	t.Helper()
	last := make([]string, cluster.Len())
	converged := harness.WaitUntil(timeout, pollInterval, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			got, err := readRow(t, cluster.Node(i))
			if err != nil || got != want {
				last[i] = fmt.Sprintf("%v %v", got, err)
				return false
			}
			last[i] = fmt.Sprintf("%+v", got)
		}
		return true
	})
	if !converged {
		t.Fatalf("timed out waiting for %+v on every node; last=%v", want, last)
	}
}

// waitForCellConvergence waits until every node agrees on the shared-cell
// value while the other cells hold their disjoint values; it returns the
// winner (Rust: wait_for_cell_convergence).
func waitForCellConvergence(t *testing.T, cluster *harness.Cluster, wantPhone string, wantScore int64, timeout time.Duration) string {
	t.Helper()
	last := make([]string, cluster.Len())
	winner := ""
	converged := harness.WaitUntil(timeout, pollInterval, func() bool {
		var current *string
		for i := 0; i < cluster.Len(); i++ {
			row, err := readRow(t, cluster.Node(i))
			if err != nil || row.phone != wantPhone || row.score != wantScore {
				last[i] = fmt.Sprintf("%v %v", row, err)
				return false
			}
			last[i] = fmt.Sprintf("name=%q phone=%q score=%d", row.name, row.phone, row.score)
			if current == nil {
				current = &row.name
			} else if *current != row.name {
				return false
			}
		}
		if current != nil {
			winner = *current
		}
		return true
	})
	if !converged {
		t.Fatalf("same-cell contention did not converge across %d nodes; last=%v", cluster.Len(), last)
	}
	return winner
}

func TestMultiProcessDisjointAndSharedCellContention(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "crdt-contention",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("contention-no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}

	waitForPeers(t, cluster, 2, peersTimeout)

	// Same schema on every node (Rust: TypedRecords/TypedContention tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).Migrate(murmur.NewMigration(
			"CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)")); err != nil {
			t.Fatalf("migrate contention table: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush schema: %v", err)
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

	// Baseline row from node 1, replicated everywhere.
	baseline := contentionRow{name: "baseline", phone: "baseline", score: 0}
	insertBaseline(t, cluster.Node(0), baseline)
	waitForState(t, cluster, baseline, stateTimeout)

	// Concurrent edits to separate cells must all survive regardless of
	// arrival order at other nodes. Worker errors are collected and
	// failed on the test goroutine (Rust: panics caught by catch_unwind).
	runConcurrent := func(fn func(nodeIdx int, db *murmur.Database) error) {
		start := make(chan struct{})
		errs := make(chan error, cluster.Len())
		var wg sync.WaitGroup
		for nodeIdx := 0; nodeIdx < cluster.Len(); nodeIdx++ {
			wg.Add(1)
			go func(nodeIdx int) {
				defer wg.Done()
				<-start
				errs <- fn(nodeIdx, cluster.Node(nodeIdx))
			}(nodeIdx)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent worker failed: %v", err)
			}
		}
	}
	runConcurrent(func(nodeIdx int, db *murmur.Database) error {
		label := nodeIdx + 1
		switch nodeIdx {
		case 0:
			return updateText(db, label, "name", "node1-name")
		case 1:
			return updateText(db, label, "phone", "node2-phone")
		default:
			return updateScore(db, label, 42)
		}
	})
	disjoint := contentionRow{name: "node1-name", phone: "node2-phone", score: 42}
	waitForState(t, cluster, disjoint, stateTimeout)

	// All three nodes now contend on the same name cell. The winning
	// value is intentionally not preselected; the LWW version must
	// resolve to the same application-visible result on every node.
	runConcurrent(func(nodeIdx int, db *murmur.Database) error {
		label := nodeIdx + 1
		for seq := 0; seq < updatesPerNode; seq++ {
			if err := updateText(db, label, "name", fmt.Sprintf("node%d-cell-%03d", label, seq)); err != nil {
				return err
			}
		}
		return nil
	})

	winner := waitForCellConvergence(t, cluster, "node2-phone", 42, convergenceTimeout)
	if winner == "" {
		t.Fatal("same-cell contention resolved to an empty name")
	}
	if !(strings.HasPrefix(winner, "node1-cell-") ||
		strings.HasPrefix(winner, "node2-cell-") ||
		strings.HasPrefix(winner, "node3-cell-")) {
		t.Fatalf("same-cell winner %q did not come from the concurrent write set", winner)
	}
	first, err := readRow(t, cluster.Node(0))
	if err != nil {
		t.Fatalf("read node1 final row: %v", err)
	}
	for i := 1; i < cluster.Len(); i++ {
		other, err := readRow(t, cluster.Node(i))
		if err != nil {
			t.Fatalf("read node final row: %v", err)
		}
		if other != first {
			t.Fatalf("logical row differs: node1=%+v node%d=%+v", first, i+1, other)
		}
	}
	t.Logf("three node cell conflict converged to %q after %d updates per node", winner, updatesPerNode)
	failed = false
}
