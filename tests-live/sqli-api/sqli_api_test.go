// Live scenario: SQL is public now — bound values stay data and errors
// leave the connection usable.
//
// Go port of tests/sqli_api.rs using tests-live/gosqlharness. Identical
// behavior: two nodes share a replicated records table, six injection
// payloads inserted as bound parameters land byte-exact on both nodes, a
// duplicate-key insert fails with 23505, a bad-column query errors, the
// same connection still counts all six rows afterwards, and sqlite_master
// on both nodes shows only the records table.
package sqliapi

import (
	"os"
	"strconv"
	"testing"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestSQLParametersDoNotExecuteInjectedStatements(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "sqli-api",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, body TEXT NOT NULL)"); err != nil {
			t.Fatalf("node %d create records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate records: %v", i, err)
		}
	}
	attacks := []string{
		"'; DROP TABLE records; --",
		"x' OR 1=1 --",
		"'); DELETE FROM records; --",
		"SELECT * FROM sqlite_master",
		"quote' slash\\ newline\n unicode 日本語",
		"/v1/query?sql=DROP%20TABLE%20records",
	}
	var expected [][]string
	for i, body := range attacks {
		n, err := cluster.Node(0).Execute(
			"INSERT INTO records(id,body) VALUES($1,$2)", int64(i), body)
		if err != nil {
			t.Fatalf("insert attack %d: %v", i, err)
		}
		if n != 1 {
			t.Fatalf("insert attack %d affected %d rows, want 1", i, n)
		}
		expected = append(expected, []string{body})
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush attacks: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT body FROM records ORDER BY id", expected); err != nil {
		t.Fatalf("converge attacks: %v", err)
	}
	_, err = cluster.Node(0).Execute(
		"INSERT INTO records(id,body) VALUES($1,$2)", int64(0), "duplicate")
	if err == nil {
		t.Fatal("duplicate insert accepted")
	}
	if code := goharness.ErrorCode(err); code != "23505" {
		t.Fatalf("duplicate code = %q, want 23505 (%v)", code, err)
	}
	if _, err := cluster.Node(0).Simple("SELECT definitely_missing_column FROM records"); err == nil {
		t.Fatal("bad-column query accepted")
	}
	rows, err := cluster.Node(0).Simple("SELECT count(*) FROM records")
	if err != nil {
		t.Fatalf("count records: %v", err)
	}
	want := [][]string{{strconv.Itoa(len(attacks))}}
	if !equalRows(rows, want) {
		t.Fatalf("count = %v, want %v", rows, want)
	}
	for i := 0; i < 2; i++ {
		tables, err := cluster.Node(i).Simple(
			"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
		if err != nil {
			t.Fatalf("node %d list tables: %v", i, err)
		}
		if !equalRows(tables, [][]string{{"records"}}) {
			t.Fatalf("node %d tables = %v, want [[records]]", i, tables)
		}
	}
	failed = false
}

func equalRows(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}
