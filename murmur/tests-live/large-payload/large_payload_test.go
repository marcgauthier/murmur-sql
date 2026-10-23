// Live scenario: a bulk transaction with one large value replicates
// exactly.
//
// Go port of the Rust `large-payload` scenario
// (/home/marc/MURMUR/tests-live/large-payload/tests/large_payload.rs).
// 250 small rows plus one row carrying a 1.5 MiB value commit on one node
// and must replicate exactly (row count plus value length and content) to
// its mesh peer. Plain SQL replaces the typed-records HTTP API.
package largepayload

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// bulkRows is the bulk rows committed before the large value (Rust:
	// BULK_ROWS = 250).
	bulkRows = 250
	// largeValueBytes is the large value size in bytes (Rust:
	// LARGE_VALUE_BYTES = 1536 * 1024).
	largeValueBytes = 1536 * 1024
	// largeID is the large row id (Rust uses a random row id; a fixed id
	// is equivalent here because the cluster starts empty and only one
	// large row exists).
	largeID = "large-payload-row"
	// convergenceTimeout is the replication deadline (Rust: 60 s).
	convergenceTimeout = 60 * time.Second
	// pollInterval is the convergence poll interval (Rust: 50 ms).
	pollInterval = 50 * time.Millisecond
)

// rowCount is the row count on one node (Rust: len(TypedContentionRows)).
func rowCount(t *testing.T, db *murmur.Database) int {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM records")
	})
	if err != nil {
		t.Fatalf("row count: %v", err)
	}
	return int(v.([][]sqlite.Value)[0][0].Int)
}

// readPhone reads one row's phone value by id (Rust: TypedContentionRead).
func readPhone(t *testing.T, db *murmur.Database, id string) (string, bool) {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT phone FROM records WHERE id = ?", id)
	})
	if err != nil {
		t.Fatalf("read phone: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return "", false
	}
	return rows[0][0].Str, true
}

func TestLargePayload(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "large-payload",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("no-convergence")
		}
		cluster.Finish(failed)
	}()

	// Same schema on both nodes (Rust: TypedRecords fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i,
			"CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL)"); err != nil {
			t.Fatalf("migrate records table: %v", err)
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

	// Large value: repeated pattern plus 'x' padding to the exact size
	// (Rust: "murmur-large-payload-" repeated + pad).
	const pattern = "murmur-large-payload-"
	largeValue := strings.Repeat(pattern, largeValueBytes/len(pattern))
	largeValue += strings.Repeat("x", largeValueBytes-len(largeValue))
	if len(largeValue) != largeValueBytes {
		t.Fatalf("large value length = %d, want %d", len(largeValue), largeValueBytes)
	}

	// 250 bulk rows plus the large row, each in its own transaction on
	// node 0 (Rust: one TypedContentionInsert call per row).
	for i := 0; i < bulkRows; i++ {
		rowID := fmt.Sprintf("bulk-%04d", i)
		name := fmt.Sprintf("bulk-%04d", i)
		score := fmt.Sprintf("%d", i)
		if _, err := cluster.Node(0).Transaction(func(s *murmur.Sql) (any, error) {
			_, err := s.Execute("INSERT INTO records(id, name, phone) VALUES(?, ?, ?)", rowID, name, score)
			return nil, err
		}); err != nil {
			t.Fatalf("insert bulk row %d: %v", i, err)
		}
	}
	if _, err := cluster.Node(0).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name, phone) VALUES(?, ?, ?)",
			"large-payload-row", "large-payload", largeValue)
		return nil, err
	}); err != nil {
		t.Fatalf("insert large value: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush after inserts: %v", err)
	}

	// Wait for node 1 to hold all rows with the exact large value (Rust:
	// row count + length/content check in the 60 s poll loop).
	deadline := time.Now().Add(convergenceTimeout)
	for {
		count := rowCount(t, cluster.Node(1))
		if count == bulkRows+1 {
			got, ok := readPhone(t, cluster.Node(1), largeID)
			if !ok {
				t.Fatalf("node 2 holds all rows but the large row is missing")
			}
			if len(got) != largeValueBytes {
				t.Fatalf("replicated value length = %d, want %d", len(got), largeValueBytes)
			}
			if got != largeValue {
				t.Fatalf("replicated value content differs from the committed payload")
			}
			t.Logf("Node 2 holds %d rows with the exact %d-byte payload", count, largeValueBytes)
			failed = false
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("timed out waiting for %d bulk rows and the large payload on node 2 (count=%d)",
				bulkRows+1, count)
		}
		time.Sleep(pollInterval)
	}
}
