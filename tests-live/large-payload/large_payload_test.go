// Live scenario: extended-protocol parameters and replication across
// many Spool blocks.
//
// Go port of tests/large_payload.rs using tests-live/goharness.
// Identical behavior: a 2-node cluster takes 250 small rows plus one
// 1.5 MiB text parameter on node 0; node 1 converges on all 251 rows,
// returns the large payload byte-identical, and both nodes read the same
// full ordered table.
package largepayload

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

func TestLargePayload(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("large-payload")
	opts.NumNodes = 2
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple("CREATE TABLE IF NOT EXISTS records(id TEXT PRIMARY KEY NOT NULL, phone TEXT NOT NULL)"); err != nil {
			t.Fatalf("node %d create records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate records: %v", i, err)
		}
	}
	for i := 0; i < 250; i++ {
		id := fmt.Sprintf("bulk-%04d", i)
		value := fmt.Sprintf("phone-%d", i)
		if _, err := cluster.Node(0).Execute(
			"INSERT INTO records(id,phone) VALUES($1,$2)", id, value); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}
	const size = 1536 * 1024
	payload := strings.Repeat("murmur-large-payload-", size/21+1)
	payload = payload[:size]
	if len(payload) != size {
		t.Fatalf("payload len = %d, want %d", len(payload), size)
	}
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO records(id,phone) VALUES($1,$2)", "large-payload-row", payload); err != nil {
		t.Fatalf("insert large payload row: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		count, err := cluster.Node(1).Simple("SELECT count(*) FROM records")
		if err != nil {
			t.Fatalf("node 1 count: %v", err)
		}
		if len(count) == 1 && len(count[0]) == 1 && count[0][0] == "251" {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("large payload did not replicate; count=%v", count)
		}
		time.Sleep(50 * time.Millisecond)
	}
	rows, err := cluster.Node(1).Query("SELECT phone FROM records WHERE id=$1", "large-payload-row")
	if err != nil {
		t.Fatalf("node 1 read payload: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("node 1 payload rows = %d, want 1", len(rows))
	}
	got, err := rows[0].String(0)
	if err != nil {
		t.Fatalf("node 1 decode payload: %v", err)
	}
	if got != payload {
		t.Fatal("node 1 large payload differs")
	}
	a, err := cluster.Node(1).Simple("SELECT id,phone FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("node 1 read all: %v", err)
	}
	b, err := cluster.Node(0).Simple("SELECT id,phone FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("node 0 read all: %v", err)
	}
	if !equalRows(a, b) {
		t.Fatal("node 0 and node 1 tables differ")
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
