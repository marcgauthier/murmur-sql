// Live scenario: four nodes form a mesh, split into two isolated pairs,
// write independently, heal, and converge on identical ordered state.
//
// Go port of the Rust `partition` scenario
// (/home/marc/MURMUR/tests-live/partition/tests/partition.rs).
// Plain SQL replaces the typed-records HTTP API.
package partition

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

const (
	// pollInterval is the convergence poll interval (Rust: 50 ms).
	pollInterval = 50 * time.Millisecond
	// baselineTimeout bounds the baseline / post-heal convergence
	// (Rust: 10 s each).
	baselineTimeout = 10 * time.Second
	// splitTimeout bounds intra-group convergence during the split
	// (Rust: 15 s).
	splitTimeout = 15 * time.Second
	// healTimeout bounds full convergence after healing (Rust: 15 s).
	healTimeout = 15 * time.Second
)

// markerRow is one marker row: Rust's MarkerRow (id, name).
type markerRow struct {
	id   int64
	name string
}

// rows returns all marker rows on one node, unordered (Rust: rows).
func rows(t *testing.T, db *murmur.Database) []markerRow {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name FROM records")
	})
	if err != nil {
		t.Fatalf("read marker rows: %v", err)
	}
	var out []markerRow
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, markerRow{id: row[0].Int, name: row[1].Str})
	}
	return out
}

// digest is the canonical digest of a row set (Rust: digest, sorted then
// rendered).
func digest(rowSet []markerRow) string {
	slices.SortFunc(rowSet, func(a, b markerRow) int {
		if a.id != b.id {
			if a.id < b.id {
				return -1
			}
			return 1
		}
		if a.name < b.name {
			return -1
		}
		if a.name > b.name {
			return 1
		}
		return 0
	})
	return fmt.Sprintf("%v", rowSet)
}

// insertMarker inserts one marker row (Rust: insert_marker).
func insertMarker(t *testing.T, cluster *harness.Cluster, node int, id int64, name string) {
	t.Helper()
	if _, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		t.Fatalf("node %d insert %q failed: %v", node, name, err)
	}
	if err := cluster.Node(node).Flush(); err != nil {
		t.Fatalf("flush after insert: %v", err)
	}
}

// waitAllCount waits until every node holds exactly expected rows (Rust:
// wait_all_count).
func waitAllCount(t *testing.T, cluster *harness.Cluster, expected int, timeout time.Duration, what string) {
	t.Helper()
	converged := harness.WaitUntil(timeout, pollInterval, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if len(rows(t, cluster.Node(i))) != expected {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("cluster failed to converge to %d rows within %v (%s)", expected, timeout, what)
	}
}

func TestFourNodePartitionIsolationAndHealing(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "partition",
		NumNodes: 4,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("partition-no-convergence")
		}
		cluster.Finish(failed)
	}()

	// Same schema on every node (Rust: TypedRecords fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).Migrate(murmur.NewMigration(
			"CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")); err != nil {
			t.Fatalf("migrate records table: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush schema: %v", err)
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

	// Initial baseline insert on node 0; all 4 nodes must receive it.
	insertMarker(t, cluster, 0, 1, "baseline")
	waitAllCount(t, cluster, 1, baselineTimeout, "baseline")

	// Partition the mesh into two isolated groups: {0, 1} and {2, 3}.
	for _, g1 := range []int{0, 1} {
		for _, g2 := range []int{2, 3} {
			if err := cluster.RemovePeer(g1, g2); err != nil {
				t.Fatalf("remove peer: %v", err)
			}
			if err := cluster.RemovePeer(g2, g1); err != nil {
				t.Fatalf("remove peer: %v", err)
			}
		}
	}
	time.Sleep(100 * time.Millisecond)

	// Independent writes on each side of the split.
	insertMarker(t, cluster, 0, 100, "side-ab-write")
	insertMarker(t, cluster, 2, 200, "side-cd-write")

	// Intra-group replication is asynchronous, so poll for internal
	// convergence. Any count above 2 means the partition leaked.
	splitOK := harness.WaitUntil(splitTimeout, pollInterval, func() bool {
		allTwo := true
		for i := 0; i < cluster.Len(); i++ {
			n := len(rows(t, cluster.Node(i)))
			if n > 2 {
				t.Fatalf("partition leaked: node %d count = %d, want at most 2", i, n)
			}
			if n != 2 {
				allTwo = false
			}
		}
		return allTwo
	})
	if !splitOK {
		t.Fatalf("groups failed to converge internally within %v (want all 2)", splitTimeout)
	}

	// Isolation by content: equal counts but divergent digests prove
	// each group converged internally without cross-group replication.
	digests := make([]string, 0, cluster.Len())
	for i := 0; i < cluster.Len(); i++ {
		digests = append(digests, digest(rows(t, cluster.Node(i))))
	}
	if !(digests[0] == digests[1] && digests[2] == digests[3]) {
		t.Fatalf("intra-group digests diverge: %v", digests)
	}
	if digests[0] == digests[2] {
		t.Fatalf("cross-group digests unexpectedly match during partition: %v", digests)
	}

	// Heal the partition: reconnect the two groups both directions.
	for _, g1 := range []int{0, 1} {
		for _, g2 := range []int{2, 3} {
			if err := cluster.AddPeer(g1, g2); err != nil {
				t.Fatalf("add peer: %v", err)
			}
			if err := cluster.AddPeer(g2, g1); err != nil {
				t.Fatalf("add peer: %v", err)
			}
		}
	}

	// Full convergence on 3 rows, then bit-identical digests on all 4.
	waitAllCount(t, cluster, 3, healTimeout, "post-heal")
	digests = digests[:0]
	for i := 0; i < cluster.Len(); i++ {
		digests = append(digests, digest(rows(t, cluster.Node(i))))
	}
	for i, d := range digests {
		if i == 0 {
			continue
		}
		if d != digests[0] {
			t.Fatalf("node %d digest %s != node 0 digest %s", i, d, digests[0])
		}
	}

	// Post-heal write replicates to all.
	insertMarker(t, cluster, 1, 300, "post-heal")
	waitAllCount(t, cluster, 4, baselineTimeout, "post-heal write")
	failed = false
}
