// Live scenario: a rolling typed schema migration is adopted by older
// peers without losing the new field.
//
// Go port of the Rust `schema-evolution` scenario
// (/home/marc/MURMUR/tests-live/schema-evolution/tests/schema_evolution.rs).
// Only node 0 registers the additive Note field; its peers adopt the
// replicated schema while an older binding updates its known Name field
// after adoption without erasing the new Note field. Late inserts, CRDT
// counters, and restart reconstruction are all verified. Plain SQL replaces
// the typed-records HTTP API.
package schemaevolution

import (
	"fmt"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// table is the base table (Rust: typed records with a Name and a PN
// counter).
const table = "live_typed_records"

// tableDDL is the base table DDL.
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, counter INTEGER NOT NULL)"

// noteDDL is the additive migration DDL, applied on node 0 only (Rust:
// MigrateTypedRecords registers the Note field; the manifest replicates to
// the peers).
const noteDDL = "ALTER TABLE live_typed_records ADD COLUMN note TEXT"

const (
	// rows is the seeded row count (Rust: ROWS = 5).
	rows = 5
	// namesTimeout is the name-convergence deadline (Rust: 30 s waitNames).
	namesTimeout = 30 * time.Second
	// namesPoll is the name poll interval (Rust: 50 ms).
	namesPoll = 50 * time.Millisecond
	// epochLocalTimeout is the local-epoch wait (Rust: 10 s waitEpoch on
	// the migrating node).
	epochLocalTimeout = 10 * time.Second
	// epochPeerTimeout is the peer-adoption wait (Rust: 30 s waitEpoch on
	// each peer).
	epochPeerTimeout = 30 * time.Second
	// fieldTimeout is the field wait deadline (Rust: 30 s
	// waitNote/waitCounter).
	fieldTimeout = 30 * time.Second
)

// rowName is the seed row name (Rust: typed-row-%d).
func rowName(i int) string {
	return fmt.Sprintf("typed-row-%d", i)
}

// names returns the canonical ordered names on one node (Rust:
// Cluster.TypedNames).
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM live_typed_records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read names failed: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// insertRow inserts one record with a zero counter (Rust:
// Cluster.TypedInsert).
func insertRow(t *testing.T, db *murmur.Database, node int, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name, counter) VALUES(?, 0)", name)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after insert failed: %v", node, err)
	}
}

// counterAdd is a counter add on one record (Rust:
// Cluster.TypedCounterAdd).
func counterAdd(t *testing.T, db *murmur.Database, node int, name string, delta int64) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		changed, err := s.Execute("UPDATE live_typed_records SET counter = counter + ? WHERE name = ?", delta, name)
		if err != nil {
			return nil, err
		}
		if changed != 1 {
			t.Fatalf("node%d counter add hit %d rows", node, changed)
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("node%d counter add failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after counter add failed: %v", node, err)
	}
}

// setNote writes the added Note field (Rust: Cluster.TypedSetNote).
func setNote(t *testing.T, db *murmur.Database, node int, name, note string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		changed, err := s.Execute("UPDATE live_typed_records SET note = ? WHERE name = ?", note, name)
		if err != nil {
			return nil, err
		}
		if changed != 1 {
			t.Fatalf("node%d set note hit %d rows", node, changed)
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("node%d set note failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after set note failed: %v", node, err)
	}
}

// renameRow renames a record from an older binding (Rust:
// Cluster.TypedRename). Ordinary transactions cannot update primary keys,
// so the rename copies the row's fields under the new name (delete +
// insert, one transaction). Callers must barrier on the fields first (a
// record-id rename would let concurrent field writes follow it; the copy
// only sees local state).
func renameRow(t *testing.T, db *murmur.Database, node int, old, new string) {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT counter, note FROM live_typed_records WHERE name = ?", old)
	})
	if err != nil {
		t.Fatalf("node%d rename read failed: %v", node, err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) != 1 {
		t.Fatalf("node%d rename read hit %d rows", node, len(rows))
	}
	counter := rows[0][0].Int
	var note any = nil
	if rows[0][1].Type != sqlite.Null {
		note = rows[0][1].Str
	}
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		deleted, err := s.Execute("DELETE FROM live_typed_records WHERE name = ?", old)
		if err != nil {
			return nil, err
		}
		if deleted != 1 {
			t.Fatalf("node%d rename deleted %d rows", node, deleted)
		}
		_, err = s.Execute("INSERT INTO live_typed_records(name, counter, note) VALUES(?, ?, ?)", new, counter, note)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d rename failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after rename failed: %v", node, err)
	}
}

// hasColumn reports column presence (Rust: schema-manifest epoch
// assertions; established typed-records port pattern).
func hasColumn(t *testing.T, db *murmur.Database, tableName, column string) bool {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("PRAGMA table_info(" + tableName + ")")
	})
	if err != nil {
		return false
	}
	for _, row := range v.([][]sqlite.Value) {
		if len(row) > 1 && row[1].Str == column {
			return true
		}
	}
	return false
}

// schemaAdoptions is the schema-adoption metric (Rust: epoch advance on the
// adopting peer).
func schemaAdoptions(cluster *harness.Cluster, idx int) uint64 {
	h := cluster.Node(idx).Replication()
	if h == nil {
		return 0
	}
	return h.Status().Metrics.SchemaAdoptions
}

// waitNames waits until every node holds exactly want (Rust: waitNames).
func waitNames(t *testing.T, cluster *harness.Cluster, want []string) {
	t.Helper()
	last := make([]string, cluster.Len())
	converged := harness.WaitUntil(namesTimeout, namesPoll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			got := names(t, cluster.Node(i))
			last[i] = fmt.Sprintf("node%d names=%v", i+1, got)
			if len(got) != len(want) {
				return false
			}
			for k := range want {
				if got[k] != want[k] {
					return false
				}
			}
		}
		return true
	})
	if !converged {
		got := make([][]string, cluster.Len())
		for i := 0; i < cluster.Len(); i++ {
			got[i] = names(t, cluster.Node(i))
		}
		t.Fatalf("typed records did not converge to %v: %v (last=%v)", want, got, last)
	}
}

// waitEpoch waits until node adopts the additive column (Rust: waitEpoch).
func waitEpoch(t *testing.T, cluster *harness.Cluster, node int, timeout time.Duration) {
	t.Helper()
	adopted := harness.WaitUntil(timeout, namesPoll, func() bool {
		return hasColumn(t, cluster.Node(node), table, "note")
	})
	if !adopted {
		t.Fatalf("node%d never adopted the additive note column within %v", node+1, timeout)
	}
}

// readNote reads the Note field of one record (nil when the row is absent
// or the field is NULL).
func readNote(t *testing.T, db *murmur.Database, name string) (string, bool) {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT note FROM live_typed_records WHERE name = ?", name)
	})
	if err != nil {
		return "", false
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 || rows[0][0].Type == sqlite.Null {
		return "", false
	}
	return rows[0][0].Str, true
}

// waitNote waits until the Note field reads want (Rust: waitNote).
func waitNote(t *testing.T, cluster *harness.Cluster, node int, name, want string) {
	t.Helper()
	matched := harness.WaitUntil(fieldTimeout, namesPoll, func() bool {
		got, ok := readNote(t, cluster.Node(node), name)
		return ok && got == want
	})
	if !matched {
		t.Fatalf("node%d Note for %q never became %q within %v", node+1, name, want, fieldTimeout)
	}
}

// readCounter reads the counter of one record.
func readCounter(t *testing.T, db *murmur.Database, name string) (int64, bool) {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT counter FROM live_typed_records WHERE name = ?", name)
	})
	if err != nil {
		return 0, false
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return 0, false
	}
	return rows[0][0].Int, true
}

// waitCounter waits until the counter reads want (Rust: waitCounter).
func waitCounter(t *testing.T, cluster *harness.Cluster, node int, name string, want int64) {
	t.Helper()
	matched := harness.WaitUntil(fieldTimeout, namesPoll, func() bool {
		got, ok := readCounter(t, cluster.Node(node), name)
		return ok && got == want
	})
	if !matched {
		t.Fatalf("node%d counter for %q never became %d within %v", node+1, name, want, fieldTimeout)
	}
}

func TestRollingTypedSchemaMigration(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "schema-evolution",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("schema-evolution-no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}

	// Same base schema on every node (Rust: typed fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("migrate records table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("live_typed_records", true); err != nil {
			t.Fatalf("opt-in live_typed_records: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Seed rows plus counters on node 1 and converge.
	for i := 0; i < rows; i++ {
		name := rowName(i)
		insertRow(t, cluster.Node(0), 1, name)
		counterAdd(t, cluster.Node(0), 1, name, int64(100+i))
	}
	waitNames(t, cluster, []string{
		"typed-row-0",
		"typed-row-1",
		"typed-row-2",
		"typed-row-3",
		"typed-row-4",
	})

	// Only node 1 registers the additive Note field. Its peers adopt
	// the replicated schema (column presence + adoption metric stand in
	// for the epoch assertions).
	if err := cluster.Node(0).Migrate(murmur.NewMigration(noteDDL)); err != nil {
		t.Fatalf("additive typed migration: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush migration: %v", err)
	}
	waitEpoch(t, cluster, 0, epochLocalTimeout)
	waitEpoch(t, cluster, 1, epochPeerTimeout)
	waitEpoch(t, cluster, 2, epochPeerTimeout)
	for _, peer := range []int{1, 2} {
		// Poll, don't sample once: the column lands in SQLite by
		// accept_schema, but the metric increments only after the
		// acceptance record commits to Spool, so a single read right
		// after waitEpoch can observe the column before the count.
		recorded := harness.WaitUntil(epochPeerTimeout, namesPoll, func() bool {
			return schemaAdoptions(cluster, peer) >= 1
		})
		if !recorded {
			t.Fatalf("peer%d adopted the column without recording a schema adoption", peer+1)
		}
	}

	// The new field is written from node 1 while an older binding on
	// node 2 updates its known Name field after adoption; this must not
	// erase the new Note field. Node 3 inserts a late row.
	setNote(t, cluster.Node(0), 1, "typed-row-0", "added-field-survives")
	// Barrier: the SQL rename copies local fields, so node 2 must hold
	// the note before renaming (a record-id rename keeps the id).
	waitNote(t, cluster, 1, "typed-row-0", "added-field-survives")
	renameRow(t, cluster.Node(1), 2, "typed-row-0", "typed-row-renamed")
	insertRow(t, cluster.Node(2), 3, "typed-row-late")
	wantNames := []string{
		"typed-row-1",
		"typed-row-2",
		"typed-row-3",
		"typed-row-4",
		"typed-row-late",
		"typed-row-renamed",
	}
	waitNames(t, cluster, wantNames)
	waitNote(t, cluster, 0, "typed-row-renamed", "added-field-survives")
	for i := 1; i < rows; i++ {
		name := rowName(i)
		for node := 0; node < cluster.Len(); node++ {
			waitCounter(t, cluster, node, name, int64(100+i))
		}
	}

	// The adopted schema and its rows survive the third peer's restart.
	cluster.StopNode(2)
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("restart node3: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	waitNames(t, cluster, wantNames)
	waitCounter(t, cluster, 2, "typed-row-4", 104)
	failed = false
}
