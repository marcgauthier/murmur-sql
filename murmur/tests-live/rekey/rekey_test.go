// Live scenario: online storage-key rotation (rekey) with restart and
// unclean-restart recovery.
//
// Go port of the Rust `rekey` scenario
// (/home/marc/MURMUR/tests-live/rekey/tests/rekey.rs). A single node rotates
// its application (master) storage key without downtime: records written
// before and after rotation read back after reopening with the new key, and
// the retired key is rejected. The second leg covers the SIGKILL recovery
// path: rotation survives an unclean restart and the reopened store still
// requires the new key.
//
// Plain SQL replaces the Go typed-records HTTP API of the original;
// rotation is Database.RotateMasterKey, restart is in-process Close plus a
// direct murmur.Open with the new key (the cluster keeps the pre-rotation
// key), and the retired-key leg asserts murmur.ErrWrongKey. The product has
// no key IDs and no Remote Unlock endpoint, so key-ID assertions cannot be
// ported; see README.md.
package rekey

import (
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// oldKey is the pre-rotation application key (also the cluster default).
var oldKey = [32]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}

// newKey is the post-rotation application key (fixed so the old-vs-new
// distinction is deterministic).
var newKey = [32]byte{0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a}

// ddl is the typed table (record name as the row key).
const ddl = "CREATE TABLE records(name TEXT NOT NULL PRIMARY KEY)"

// typedInsert is a typed insert on one node (Rust: TypedInsert).
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

// typedCount is the row count by name (Rust: TypedCount).
func typedCount(t *testing.T, db *murmur.Database, name string) int64 {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT count(*) FROM records WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("typed count: %v", err)
	}
	return v.([][]sqlite.Value)[0][0].Int
}

// assertKeyRejected requires that key no longer opens dataDir (Rust:
// assertNeverReady after rewriting the config to the retired key: the
// daemon exits on failed unlock instead of serving health).
func assertKeyRejected(t *testing.T, dataDir string, key [32]byte) {
	t.Helper()
	db, err := murmur.Open(dataDir, key[:], murmur.DefaultOptions())
	switch {
	case err == nil:
		_ = db.Close()
		t.Fatal("store unexpectedly accepted the retired key")
	case errors.Is(err, murmur.ErrWrongKey):
	default:
		t.Fatalf("retired-key open failed with %v, want WrongKey", err)
	}
}

func TestTypedStorageKeyRekeySurvivesRestart(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{Name: "rekey-restart", NumNodes: 1, Key: oldKey})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("rekey-restart")
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
	typedInsert(t, cluster.Node(0), "typed-before-rotate")

	// Rotate without downtime. The product has no key IDs, so the new-key
	// opens / old-key rejected legs below carry that proof.
	if err := cluster.Node(0).RotateMasterKey(newKey); err != nil {
		t.Fatalf("rotate master key: %v", err)
	}
	typedInsert(t, cluster.Node(0), "typed-after-rotate")

	// Restart on the new key: stop, then reopen directly with the new key
	// (the cluster keeps the pre-rotation key, so StartNode cannot reopen
	// a rotated node).
	dataDir := cluster.NodeInfo(0).DataDir
	cluster.StopNode(0)
	db, err := murmur.Open(dataDir, newKey[:], murmur.DefaultOptions())
	if err != nil {
		t.Fatalf("reopen with new key: %v", err)
	}
	if got := typedCount(t, db, "typed-before-rotate"); got != 1 {
		_ = db.Close()
		t.Fatalf("pre-rotation row after restart = %d, want 1", got)
	}
	if got := typedCount(t, db, "typed-after-rotate"); got != 1 {
		_ = db.Close()
		t.Fatalf("post-rotation row after restart = %d, want 1", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}

	// The pre-rotation key no longer opens the store after rotation.
	assertKeyRejected(t, dataDir, oldKey)
	failed = false
}

func TestRekeySigkillRestartsWithNewKey(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{Name: "rekey-sigkill", NumNodes: 1, Key: oldKey})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("rekey-sigkill")
		}
		cluster.Finish(failed)
	}()

	// No await-unlock leg: murmur.Open takes the key directly (no Remote
	// Unlock endpoint), and key IDs do not exist, so the unlock-by-ID
	// assertions are dropped.
	if err := cluster.EnsureMigrated(0, ddl); err != nil {
		t.Fatalf("migrate typed table: %v", err)
	}
	if err := cluster.Node(0).SetReplication("records", true); err != nil {
		t.Fatalf("opt-in records: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush after migrate: %v", err)
	}
	typedInsert(t, cluster.Node(0), "pre-rotate")
	if err := cluster.Node(0).RotateMasterKey(newKey); err != nil {
		t.Fatalf("rotate master key: %v", err)
	}
	// RotateMasterKey is synchronous, so the rotation is durable at return
	// and cannot depend on a later clean shutdown.
	dataDir := cluster.NodeInfo(0).DataDir
	cluster.StopNode(0)

	// SIGKILL (not graceful stop): a fresh process opens the rotated store
	// with the new key and is killed while holding it open; nothing may be
	// lost and the new key stays required.
	readyPath := filepath.Join(cluster.NodeInfo(0).Dir, "fixture.ready")
	child, err := harness.Spawn("open",
		"--dir", dataDir,
		"--key-hex", hex.EncodeToString(newKey[:]),
		"--mode", "existing",
		"--ready", readyPath,
		"--hold-secs", "120",
	)
	if err != nil {
		t.Fatalf("spawn testnode fixture: %v", err)
	}
	ready := harness.WaitUntil(30*time.Second, 50*time.Millisecond, func() bool {
		_, err := os.Stat(readyPath)
		return err == nil
	})
	if !ready {
		_ = harness.Kill(child)
		_, _ = harness.WaitTimeout(child, 10*time.Second)
		t.Fatal("testnode fixture never opened the rotated store with the new key")
	}
	if err := harness.Kill(child); err != nil {
		t.Fatalf("SIGKILL fixture: %v", err)
	}
	done, waitErr := harness.WaitTimeout(child, 10*time.Second)
	if !done {
		t.Fatal("fixture still running after SIGKILL")
	}
	if waitErr == nil {
		t.Fatal("fixture unexpectedly exited cleanly after SIGKILL")
	}

	// Reopen with the new key: pre-rotation row present, new writes work.
	db, err := murmur.Open(dataDir, newKey[:], murmur.DefaultOptions())
	if err != nil {
		t.Fatalf("reopen with new key after SIGKILL: %v", err)
	}
	if got := typedCount(t, db, "pre-rotate"); got != 1 {
		_ = db.Close()
		t.Fatalf("pre-rotation row after SIGKILL restart = %d, want 1", got)
	}
	typedInsert(t, db, "post-restart")
	if got := typedCount(t, db, "post-restart"); got != 1 {
		_ = db.Close()
		t.Fatalf("post-restart write after SIGKILL restart = %d, want 1", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}

	// The retired key is rejected after the unclean restart too.
	assertKeyRejected(t, dataDir, oldKey)
	failed = false
}
