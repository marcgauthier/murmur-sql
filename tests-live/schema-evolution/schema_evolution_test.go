// Live scenario: additive SQL schema changes replicate; destructive
// replicated DDL is rejected.
//
// Go port of tests/schema_evolution.rs using tests-live/gosqlharness.
// Identical behavior: three nodes share a replicated records table, node
// 2 stops while node 0 adds a column plus rows, the offline peer learns
// the new schema and values on reconnect, destructive replicated DDL
// (DROP/RENAME/RENAME COLUMN/DROP COLUMN) is rejected everywhere while
// the replication flag survives, and the same DDL succeeds on a local
// table without disturbing replicated rows.
package schemaevolution

import (
	"os"
	"testing"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestAdditiveSQLSchemaMigrationPreservesRowsAndReplicationSettings(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "schema-evolution",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < 3; i++ {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE IF NOT EXISTS records(name TEXT PRIMARY KEY NOT NULL, score INTEGER NOT NULL)"); err != nil {
			t.Fatalf("node %d create records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate records: %v", i, err)
		}
	}
	if _, err := cluster.Node(0).Simple("INSERT INTO records(name,score) VALUES('seed',1)"); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1, 2},
		"SELECT name,score FROM records ORDER BY name",
		[][]string{{"seed", "1"}}); err != nil {
		t.Fatalf("converge seed: %v", err)
	}
	if _, err := cluster.Node(2).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node 2 pre-stop flush: %v", err)
	}
	if err := cluster.StopNode(2); err != nil {
		t.Fatalf("stop node 2: %v", err)
	}
	if _, err := cluster.Node(0).Simple("ALTER TABLE records ADD COLUMN note TEXT"); err != nil {
		t.Fatalf("add column: %v", err)
	}
	if _, err := cluster.Node(0).Simple(
		"UPDATE records SET note='new-field' WHERE name='seed'; INSERT INTO records(name,score,note) VALUES('new',2,'written-after-ddl')"); err != nil {
		t.Fatalf("write after DDL: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush after DDL: %v", err)
	}
	expected := [][]string{
		{"new", "2", "written-after-ddl"},
		{"seed", "1", "new-field"},
	}
	// The offline peer must learn the new schema and values on reconnect.
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("restart node 2: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1, 2},
		"SELECT name,score,note FROM records ORDER BY name", expected); err != nil {
		t.Fatalf("converge migrated rows: %v", err)
	}
	for _, sql := range []string{
		"DROP TABLE records",
		"ALTER TABLE records RENAME TO renamed",
		"ALTER TABLE records RENAME COLUMN score TO points",
		"ALTER TABLE records DROP COLUMN note",
	} {
		if _, err := cluster.Node(0).Simple(sql); err == nil {
			t.Fatalf("replicated destructive DDL accepted: %s", sql)
		}
	}
	for i := 0; i < 3; i++ {
		rows, err := cluster.Node(i).Simple("MURMUR SHOW TABLES REPLICATION")
		if err != nil {
			t.Fatalf("node %d show tables replication: %v", i, err)
		}
		found := false
		for _, f := range rows {
			if len(f) >= 2 && f[0] == "records" && f[1] == "true" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("node %d lost records replication flag: %v", i, rows)
		}
	}
	if _, err := cluster.Node(0).Simple(
		"CREATE TABLE local_only(id INTEGER PRIMARY KEY, old TEXT); ALTER TABLE local_only RENAME COLUMN old TO renamed; ALTER TABLE local_only RENAME TO renamed_local; DROP TABLE renamed_local"); err != nil {
		t.Fatalf("local DDL chain: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1, 2},
		"SELECT name,score,note FROM records ORDER BY name", expected); err != nil {
		t.Fatalf("converge after local DDL: %v", err)
	}
	failed = false
}
