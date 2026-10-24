// Live scenario: SQL values and unlock credentials must never reach
// durable artifacts in plaintext.
//
// Go port of tests/plaintext_audit.rs using tests-live/gosqlharness.
// Identical behavior: two nodes replicate marker rows, node 0 takes a
// backup, both nodes stop, the runtime dir is audited byte-by-byte for
// the marker and the unlock key, then both nodes restart and converge
// on the same rows.
package plaintextaudit

import (
	"fmt"
	"os"
	"testing"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestPlaintextAuditMarkersAndUnlockKeyAbsentOnDisk(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "plaintext-audit",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("start SQL cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE IF NOT EXISTS secrets(id INTEGER PRIMARY KEY, body TEXT NOT NULL)"); err != nil {
			t.Fatalf("node %d create secrets: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE secrets REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate secrets: %v", i, err)
		}
	}
	marker := fmt.Sprintf("live-sql-plaintext-audit-%s", cluster.NodeInfo(0).NodeID)
	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Execute(
			"INSERT INTO secrets(id,body) VALUES($1,$2)", int64(i), marker); err != nil {
			t.Fatalf("node %d insert marker: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush marker: %v", i, err)
		}
	}
	expected := [][]string{{marker}, {marker}}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT body FROM secrets ORDER BY id", expected); err != nil {
		t.Fatalf("converge markers: %v", err)
	}
	// Backup archives are included in the byte-level audit.
	if _, err := cluster.Node(0).Simple("MURMUR BACKUP SAVE"); err != nil {
		t.Fatalf("backup: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d pre-stop flush: %v", i, err)
		}
		if err := cluster.StopNode(i); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
	}
	if err := gosqlharness.AssertNoPlaintext(cluster.RuntimeDir(),
		[][]byte{[]byte(marker), []byte(gosqlharness.Key)}); err != nil {
		t.Fatalf("plaintext audit: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT body FROM secrets ORDER BY id", expected); err != nil {
		t.Fatalf("converge after restart: %v", err)
	}
	failed = false
}
