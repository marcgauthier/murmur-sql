// Live scenario: the encrypted store keeps records encrypted across restart.
//
// Go port of the Rust `encryption` scenario
// (/home/marc/MURMUR/tests-live/encryption/tests/encryption.rs:
// TestTypedEncryptedStoreKeepsRecordsEncryptedAcrossRestart). A single
// node stores one typed row carrying a secret marker, stops, and its
// whole node directory is scanned for the marker in plaintext; the node
// restarts, a wrong-key open must be rejected, and the correct key
// unlocks the row again.
//
// Plain SQL replaces the typed-records HTTP API, and the wrong-key leg
// is a direct murmur.Open asserting murmur.ErrWrongKey (there is no
// Remote Unlock endpoint).
package encryption

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// secretMarker is the secret row content. Long and unique so a substring
// hit on disk is unambiguous evidence of leaked plaintext.
var secretMarker = []byte("TYPED-CONFIDENTIAL-PLAINTEXT-SECRET-246813579")

// ddl is the typed table (Rust: Config.Tables; here plain SQL with the
// marker as the row key).
const ddl = "CREATE TABLE records(name TEXT NOT NULL PRIMARY KEY)"

// typedInsert writes one typed row on one node (Rust: TypedInsert).
func typedInsert(t *testing.T, db *murmur.Database, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(name) VALUES(?)", name)
		return nil, err
	}); err != nil {
		t.Fatalf("typed insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after typed insert: %v", err)
	}
}

// typedCount counts rows by name (Rust: TypedCount).
func typedCount(t *testing.T, db *murmur.Database, name string) int64 {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.QueryRow("SELECT count(*) FROM records WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("typed count: %v", err)
	}
	return v.([]sqlite.Value)[0].Int
}

// containsPlaintext reports whether any regular file under root contains
// the marker bytes (Rust: containsPlaintext; byte windows instead of a
// string search so binary spool files are covered too).
func containsPlaintext(t *testing.T, root string, marker []byte) bool {
	t.Helper()
	found := false
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || found {
			return nil
		}
		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		if bytes.Contains(data, marker) {
			t.Logf("plaintext marker found in %s", path)
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan node directory: %v", err)
	}
	return found
}

func TestTypedEncryptedStoreKeepsRecordsEncryptedAcrossRestart(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "encryption",
		NumNodes: 1,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("encryption")
		}
		cluster.Finish(failed)
	}()

	if err := cluster.EnsureMigrated(0, ddl); err != nil {
		t.Fatalf("migrate typed table: %v", err)
	}
	if err := cluster.Node(0).SetReplication("records", true); err != nil {
		t.Fatalf("opt-in records: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush after migrate: %v", err)
	}
	marker := string(secretMarker)
	typedInsert(t, cluster.Node(0), marker)
	if got := typedCount(t, cluster.Node(0), marker); got != 1 {
		t.Fatalf("typed secret before restart: count = %d, want 1", got)
	}

	// Stop, then confirm the marker occurs in no durable file.
	dir := cluster.NodeInfo(0).Dir
	dataDir := cluster.NodeInfo(0).DataDir
	cluster.StopNode(0)
	if containsPlaintext(t, dir, secretMarker) {
		t.Fatalf("typed plaintext secret marker found on disk in %s", dir)
	}

	// A wrong key must be rejected (Rust: Error::WrongKey from open).
	if db, err := murmur.Open(dataDir, make([]byte, 32), murmur.DefaultOptions()); err != nil {
		if !errors.Is(err, murmur.ErrWrongKey) {
			t.Fatalf("wrong-key open failed with %v, want WrongKey", err)
		}
	} else {
		_ = db.Close()
		t.Fatal("typed database unexpectedly accepted the wrong key")
	}

	// The correct key unlocks and the row reads back.
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart with correct key: %v", err)
	}
	if got := typedCount(t, cluster.Node(0), marker); got != 1 {
		t.Fatalf("typed secret after restart: count = %d, want 1", got)
	}

	failed = false
}
