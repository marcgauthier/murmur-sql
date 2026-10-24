// Live scenario: encrypted SQL persistence rejects a wrong unlock key
// across process restarts.
//
// Go port of tests/encryption.rs using tests-live/gosqlharness.
// Identical behavior: a single node takes a confidential row, stops, the
// stopped dir is audited for plaintext, a cold restart with a wrong key
// fails with 28000, the node restarts on the right key, a wrong-key wire
// client is rejected with 28000 (without echoing the key), and the row
// survives.
package encryption

import (
	"os"
	"strings"
	"testing"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

const secret = "SQL-CONFIDENTIAL-PLAINTEXT-SECRET-246813579"

func TestEncryptedSQLRowsSurviveRestartAndRejectWrongKey(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "encryption",
		NumNodes: 1,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if _, err := cluster.Node(0).Simple("CREATE TABLE records(name TEXT PRIMARY KEY NOT NULL)"); err != nil {
		t.Fatalf("create records: %v", err)
	}
	if _, err := cluster.Node(0).Execute("INSERT INTO records(name) VALUES($1)", secret); err != nil {
		t.Fatalf("insert secret: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	expected := [][]string{{secret}}
	rows, err := cluster.Node(0).Simple("SELECT name FROM records")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !equalRows(rows, expected) {
		t.Fatalf("read back: got %v, want %v", rows, expected)
	}
	dir := cluster.NodeInfo(0).Dir
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	if err := gosqlharness.AssertNoPlaintext(dir, [][]byte{[]byte(secret)}); err != nil {
		t.Fatalf("plaintext audit: %v", err)
	}
	// The first unlock on a cold server must reject a wrong key too.
	cluster.SetKey(0, "cold-start-wrong-key")
	if err := cluster.StartNode(0); err == nil {
		t.Fatal("cold wrong key accepted")
	} else if code := goharness.ErrorCode(err); code != "28000" {
		t.Fatalf("cold wrong key code = %q, want 28000 (%v)", code, err)
	}
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	cluster.SetKey(0, gosqlharness.Key)
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node: %v", err)
	}
	wrong := cluster.Config(0)
	wrong.Key = "deliberately-wrong-key"
	conn, err := goharness.Connect(wrong)
	if err == nil {
		_ = conn.Close()
		t.Fatal("wrong SQL unlock key accepted")
	}
	if code := goharness.ErrorCode(err); code != "28000" {
		t.Fatalf("wrong key code = %q, want 28000 (%v)", code, err)
	}
	if strings.Contains(err.Error(), wrong.Key) {
		t.Fatal("wrong-key error echoes the key")
	}
	rows, err = cluster.Node(0).Simple("SELECT name FROM records")
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if !equalRows(rows, expected) {
		t.Fatalf("read after restart: got %v, want %v", rows, expected)
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
