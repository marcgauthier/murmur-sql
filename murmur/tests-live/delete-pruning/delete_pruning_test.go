// Live scenario: three nodes share 300 rows, then concurrently delete,
// update, and resurrect disjoint thirds under aggressive log retention; a
// log-GC pass is forced mid-flight and all nodes must converge on exact
// counts plus equal row contents.
//
// Go port of the Rust `delete-pruning` scenario
// (/home/marc/MURMUR/tests-live/delete-pruning/tests/delete_pruning.rs).
// Plain SQL replaces the typed-contention HTTP API.
//
// Scope (same as Rust): delete/resurrect convergence plus log-GC
// collection of the delete batches; stored tombstone markers stay in state
// by design and are out of scope.
package deletepruning

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// keysEnv is the row-count knob (Rust: MURMUR_DELETE_PRUNING_KEYS,
	// default 300, multiple of 3).
	keysEnv = "MURMUR_DELETE_PRUNING_KEYS"
	// defaultKeys is the default shared row count.
	defaultKeys = 300
	// convergedTimeout is the convergence deadline (Rust: 60 s).
	convergedTimeout = 60 * time.Second
	// postPruneTimeout is the post-prune write deadline (Rust: 30 s).
	postPruneTimeout = 30 * time.Second
	// pollInterval is the convergence poll interval (Rust: 100 ms).
	pollInterval = 100 * time.Millisecond
	// gcTimeout is the GC-pass wait deadline (Rust: 150 s).
	gcTimeout = 150 * time.Second
	// gcPoll is the GC-pass poll interval (Rust: 2 s).
	gcPoll = 2 * time.Second
)

// rowID renders a row id (Rust: row_id).
func rowID(n int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", n, n)
}

// insertRow is a typed insert on one node (Rust: insert_row).
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

// updateName is a typed single-field update (Rust: update_name).
func updateName(db *murmur.Database, node int, id, name string) error {
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		changed, err := s.Execute("UPDATE contention SET name = ? WHERE id = ?", name, id)
		if err != nil {
			return nil, err
		}
		if changed != 1 {
			return nil, fmt.Errorf("node%d update hit %d rows", node, changed)
		}
		return nil, nil
	}); err != nil {
		return fmt.Errorf("node%d update failed: %w", node, err)
	}
	if err := db.Flush(); err != nil {
		return fmt.Errorf("node%d flush after update failed: %w", node, err)
	}
	return nil
}

// deleteRow is a typed delete (Rust: delete_row).
func deleteRow(db *murmur.Database, node int, id string) error {
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		changed, err := s.Execute("DELETE FROM contention WHERE id = ?", id)
		if err != nil {
			return nil, err
		}
		if changed != 1 {
			return nil, fmt.Errorf("node%d delete hit %d rows", node, changed)
		}
		return nil, nil
	}); err != nil {
		return fmt.Errorf("node%d delete failed: %w", node, err)
	}
	if err := db.Flush(); err != nil {
		return fmt.Errorf("node%d flush after delete failed: %w", node, err)
	}
	return nil
}

// readName is a typed read on one node: ok is false when the row is absent
// (Rust: read_name returning None).
func readName(t *testing.T, db *murmur.Database, id string) (name string, ok bool) {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM contention WHERE id = ?", id)
	})
	if err != nil {
		t.Fatalf("read row failed: %v", err)
	}
	got := v.([][]sqlite.Value)
	if len(got) == 0 {
		return "", false
	}
	return got[0][0].Str, true
}

// idName is one canonical ordered row (Rust: (id, name) pairs).
type idName struct {
	id   string
	name string
}

// allRows returns the canonical ordered rows on one node (Rust: all_rows).
func allRows(t *testing.T, db *murmur.Database) []idName {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name FROM contention ORDER BY id")
	})
	if err != nil {
		t.Fatalf("read all rows failed: %v", err)
	}
	var out []idName
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, idName{id: row[0].Str, name: row[1].Str})
	}
	return out
}

// waitConverged waits until every node holds exactly want rows with
// identical contents (Rust: wait_converged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	last := make([]string, cluster.Len())
	converged := harness.WaitUntil(timeout, pollInterval, func() bool {
		var first []idName
		for i := 0; i < cluster.Len(); i++ {
			got := allRows(t, cluster.Node(i))
			last[i] = fmt.Sprintf("count=%d", len(got))
			if len(got) != want {
				return false
			}
			if first == nil {
				first = got
			} else if !slices.Equal(got, first) {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("nodes did not converge on %d rows with equal contents within %v; last=%v",
			want, timeout, last)
	}
}

// waitGCCollected runs retention-collection passes on node 0 until the
// durable collected counter advances past before (Rust: wait_gc_collected;
// each poll triggers a pass instead of waiting for a background ticker).
func waitGCCollected(t *testing.T, cluster *harness.Cluster, before uint64) {
	t.Helper()
	collected := before
	converged := harness.WaitUntil(gcTimeout, gcPoll, func() bool {
		pass, err := cluster.Node(0).CollectRetention()
		if err != nil {
			t.Fatalf("retention collection pass: %v", err)
		}
		status, err := cluster.Node(0).RetentionStatus()
		if err != nil {
			t.Fatalf("retention status: %v", err)
		}
		collected = status.CollectedTransactions
		t.Logf("retention pass collected %d records; total %d (before %d)", pass, collected, before)
		return collected > before
	})
	if !converged {
		t.Fatalf("no log batches collected (before=%d after=%d); GC passes did not prune delete batches",
			before, collected)
	}
}

func TestConcurrentDeletesResurrectionsAndUpdatesConverge(t *testing.T) {
	keys := defaultKeys
	if v, err := strconv.Atoi(os.Getenv(keysEnv)); err == nil && v > 0 {
		keys = v
	}
	if keys%3 != 0 {
		t.Fatalf("%s=%d must be a multiple of 3", keysEnv, keys)
	}
	third := keys / 3

	// Aggressive log retention (Rust: retention 1 s, minimum retained 10,
	// maximum offline 20 s).
	retention := time.Second
	minimumRetained := uint64(10)
	maximumOffline := 20 * time.Second
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "delete-pruning",
		NumNodes: 3,
		Replication: &harness.ReplicationOptions{
			Retention:       &retention,
			MinimumRetained: &minimumRetained,
			MaximumOffline:  &maximumOffline,
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("delete-pruning-no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}

	// Same schema on every node (Rust: TypedRecords/TypedContention tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).Migrate(murmur.NewMigration(
			"CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")); err != nil {
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

	// Seed the shared row set on node 1 and converge.
	for i := 0; i < keys; i++ {
		insertRow(t, cluster.Node(0), 1, rowID(i), fmt.Sprintf("seed-%d", i))
	}
	waitConverged(t, cluster, keys, convergedTimeout)

	var gcRunsBefore uint64
	if h := cluster.Node(0).Replication(); h != nil {
		gcRunsBefore = h.Status().Metrics.RetentionRuns
	}
	baseline, err := cluster.Node(0).RetentionStatus()
	if err != nil {
		t.Fatalf("retention status baseline: %v", err)
	}
	gcCollectedBefore := baseline.CollectedTransactions

	// Phase 1, concurrent on disjoint thirds: node 1 deletes the first
	// third, node 2 updates the middle third, node 3 deletes the last
	// third. Only the middle third survives. Worker errors are collected
	// and failed on the test goroutine.
	start := make(chan struct{})
	errs := make(chan error, cluster.Len())
	var wg sync.WaitGroup
	for nodeIdx := 0; nodeIdx < cluster.Len(); nodeIdx++ {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()
			<-start
			db := cluster.Node(nodeIdx)
			label := nodeIdx + 1
			switch nodeIdx {
			case 0:
				for i := 0; i < third; i++ {
					if err := deleteRow(db, label, rowID(i)); err != nil {
						errs <- err
						return
					}
				}
			case 1:
				for i := third; i < 2*third; i++ {
					if err := updateName(db, label, rowID(i), fmt.Sprintf("upd-%d", i)); err != nil {
						errs <- err
						return
					}
				}
			default:
				for i := 2 * third; i < keys; i++ {
					if err := deleteRow(db, label, rowID(i)); err != nil {
						errs <- err
						return
					}
				}
			}
			errs <- nil
		}(nodeIdx)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("phase 1 worker failed: %v", err)
		}
	}
	// Anti-vacuity: the deletes must actually remove rows everywhere.
	waitConverged(t, cluster, third, convergedTimeout)
	wantMiddle := fmt.Sprintf("upd-%d", third)
	if got, ok := readName(t, cluster.Node(0), rowID(third)); !ok || got != wantMiddle {
		t.Fatalf("middle-third value = %q (present=%v); want %q (concurrent update lost)", got, ok, wantMiddle)
	}

	// Phase 2: resurrect the first third with new values from node 2.
	for i := 0; i < third; i++ {
		insertRow(t, cluster.Node(1), 2, rowID(i), fmt.Sprintf("res-%d", i))
	}
	waitConverged(t, cluster, 2*third, convergedTimeout)

	// Phase 3: delete the middle third from node 1. Survivors: the
	// resurrected first third only.
	for i := third; i < 2*third; i++ {
		if err := deleteRow(cluster.Node(0), 1, rowID(i)); err != nil {
			t.Fatalf("phase 3 delete failed: %v", err)
		}
	}
	waitConverged(t, cluster, third, convergedTimeout)

	// Force GC passes to run over the tombstone batches and require the
	// durable collected counter to advance.
	waitGCCollected(t, cluster, gcCollectedBefore)
	var gcRunsAfter uint64
	if h := cluster.Node(0).Replication(); h != nil {
		gcRunsAfter = h.Status().Metrics.RetentionRuns
	}
	final, err := cluster.Node(0).RetentionStatus()
	if err != nil {
		t.Fatalf("retention status final: %v", err)
	}
	t.Logf("retention runs advanced %d -> %d; log records collected during test: %d",
		gcRunsBefore, gcRunsAfter, final.CollectedTransactions-gcCollectedBefore)

	// Identical ordered digests and exact row counts on all nodes.
	wantCount := third
	for i := 0; i < cluster.Len(); i++ {
		got := allRows(t, cluster.Node(i))
		if len(got) != wantCount {
			t.Fatalf("node%d count = %d, want %d", i+1, len(got), wantCount)
		}
	}
	wantDigest := allRows(t, cluster.Node(0))
	for i := 1; i < cluster.Len(); i++ {
		if got := allRows(t, cluster.Node(i)); !slices.Equal(got, wantDigest) {
			t.Fatalf("node%d digest differs", i+1)
		}
	}
	// Content proof: resurrected rows present with new values, deleted
	// rows absent, on every node.
	for i := 0; i < cluster.Len(); i++ {
		got, ok := readName(t, cluster.Node(i), rowID(0))
		if !ok || got != "res-0" {
			t.Fatalf("node%d resurrected value = %q (present=%v), want %q", i+1, got, ok, "res-0")
		}
		if absent, ok := readName(t, cluster.Node(i), rowID(third)); ok {
			t.Fatalf("node%d deleted row lookup = %q; want not found", i+1, absent)
		}
	}

	// A post-prune write replicates everywhere.
	insertRow(t, cluster.Node(2), 3, rowID(keys+1), "post-prune")
	waitConverged(t, cluster, wantCount+1, postPruneTimeout)
	failed = false
}
