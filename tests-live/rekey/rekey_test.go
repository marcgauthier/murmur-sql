// Live scenario: rotate the encryption key through the SQL client,
// crash the actual server, and reopen with the new key.
//
// Go port of tests/rekey.rs using tests-live/gosqlharness. Identical
// behavior: one node takes a row, MURMUR ROTATE KEY swaps to a new key,
// the server is stopped without a graceful shutdown or extra flush, the
// node restarts with the new key, the old key is rejected with 28000, the
// pre-rotation row reads back, a post-rotation row is accepted, and both
// rows survive a second restart.
package rekey

import (
	"os"
	"testing"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestSQLKeyRotationSurvivesKillAndRejectsOldKey(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "rekey",
		NumNodes: 1,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if _, err := cluster.Node(0).Simple(
		"CREATE TABLE records(id INTEGER PRIMARY KEY, body TEXT); INSERT INTO records VALUES(1,'before-rotation')"); err != nil {
		t.Fatalf("seed records: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	newKey := "live-new-encryption-key-after-rotation"
	if _, err := cluster.Node(0).Simple("MURMUR ROTATE KEY " +
		gosqlharness.SQLLiteral(gosqlharness.Key) + " " +
		gosqlharness.SQLLiteral(newKey)); err != nil {
		t.Fatalf("rotate key: %v", err)
	}
	// No graceful shutdown or additional flush after the acknowledged rotation.
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	cluster.SetKey(0, newKey)
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart with new key: %v", err)
	}
	old := cluster.Config(0)
	old.Key = gosqlharness.Key
	if conn, err := goharness.Connect(old); err == nil {
		_ = conn.Close()
		t.Fatal("old key accepted after rotation")
	} else if code := goharness.ErrorCode(err); code != "28000" {
		t.Fatalf("old key code = %q, want 28000 (%v)", code, err)
	}
	assertRows(t, "pre-rotation read", cluster.Node(0), "SELECT body FROM records",
		[][]string{{"before-rotation"}})
	if _, err := cluster.Node(0).Simple("INSERT INTO records VALUES(2,'after-rotation')"); err != nil {
		t.Fatalf("insert after rotation: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush after rotation: %v", err)
	}
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node: %v", err)
	}
	assertRows(t, "post-restart read", cluster.Node(0),
		"SELECT body FROM records ORDER BY id",
		[][]string{{"before-rotation"}, {"after-rotation"}})
	failed = false
}

func assertRows(t *testing.T, what string, client *goharness.Client, query string, expected [][]string) {
	t.Helper()
	rows, err := client.Simple(query)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if len(rows) != len(expected) {
		t.Fatalf("%s: got %v, want %v", what, rows, expected)
	}
	for i := range rows {
		if len(rows[i]) != len(expected[i]) {
			t.Fatalf("%s: got %v, want %v", what, rows, expected)
		}
		for j := range rows[i] {
			if rows[i][j] != expected[i][j] {
				t.Fatalf("%s: got %v, want %v", what, rows, expected)
			}
		}
	}
}
