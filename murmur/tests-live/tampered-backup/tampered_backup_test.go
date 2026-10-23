// Live scenario: tampered backups fail closed; the pristine backup
// restores exactly.
//
// Go port of the Rust `tampered-backup` scenario
// (/home/marc/MURMUR/tests-live/tampered-backup/tests/tampered_backup.rs).
// A single node seeds 30 rows, stops, and is backed up offline through
// the library. Three tampered copies of the backup (byte flip in a
// payload file, manifest edit, payload truncation) must each fail
// Restore and VerifyBackup with no partial state, while the pristine
// backup restores exactly (row count plus a canonical digest of the
// sorted record names).
//
// Plain SQL replaces the typed-records HTTP API. The backup format is a
// directory (manifest/keys.enc/segments/), not a .tar.gz archive, so the
// gzip-magic and backup-metadata.json tampers become segment-byte and
// manifest tampers; there is no restore-intent file, so fail-closed is
// asserted as "restore target directory does not exist" (the product
// removes dst on verification failure).
package tamperedbackup

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// numRows is the seed row count (Rust: 30 typed rows).
const numRows = 30

// namesDDL is the mesh-leg DDL: plain replicated rows (Rust:
// TypedInsert names).
const namesDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)"

// typedInsert inserts one row on one node (Rust: TypedInsert).
func typedInsert(t *testing.T, db *murmur.Database, name string) {
	t.Helper()
	_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name) VALUES(?)", name)
		return nil, err
	})
	if err != nil {
		t.Fatalf("typed insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after typed insert: %v", err)
	}
}

// typedNames reads the canonical ordered names on one node (Rust:
// TypedNames).
func typedNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM live_typed_records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read typed names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// namesDigest is the canonical digest of the record names (Rust:
// namesDigest, SHA-256 over sorted \n-joined names). FNV-1a/64 over the
// same canonical byte stream: only pre/post equality matters.
func namesDigest(names []string) string {
	sorted := slices.Clone(names)
	slices.Sort(sorted)
	var hash uint64 = 0xcbf29ce484222325
	for _, name := range sorted {
		for _, b := range []byte(name + "\n") {
			hash ^= uint64(b)
			hash *= 0x100000001b3
		}
	}
	return fmt.Sprintf("%016x", hash)
}

// openOffline opens a stopped node's data dir without replication (Rust:
// openNodeDir offline library open for backup/restore legs).
func openOffline(t *testing.T, dataDir string, key [32]byte, databaseID, nodeID [16]byte) *murmur.Database {
	t.Helper()
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.OpenExisting
	opts.DatabaseID = &databaseID
	opts.NodeID = &nodeID
	db, err := murmur.Open(dataDir, key[:], opts)
	if err != nil {
		t.Fatalf("offline open of node data dir: %v", err)
	}
	return db
}

// copyDirAll recursively copies a directory for tampered-backup
// fixtures (the pristine backup must stay immutable).
func copyDirAll(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatalf("create tampered backup dir: %v", err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("read backup dir: %v", err)
	}
	for _, entry := range entries {
		from := filepath.Join(src, entry.Name())
		to := filepath.Join(dst, entry.Name())
		if entry.IsDir() {
			copyDirAll(t, from, to)
		} else {
			bytes, err := os.ReadFile(from)
			if err != nil {
				t.Fatalf("read backup file: %v", err)
			}
			if err := os.WriteFile(to, bytes, 0o600); err != nil {
				t.Fatalf("copy backup file: %v", err)
			}
		}
	}
}

// payloadFile is the payload file to tamper: the first segment file,
// falling back to keys.enc if the backup unexpectedly holds no
// segments.
func payloadFile(t *testing.T, backupDir string) string {
	t.Helper()
	segments := filepath.Join(backupDir, "segments")
	if info, err := os.Stat(segments); err == nil && info.IsDir() {
		entries, err := os.ReadDir(segments)
		if err != nil {
			t.Fatalf("read segments dir: %v", err)
		}
		var files []string
		for _, e := range entries {
			if !e.IsDir() {
				files = append(files, filepath.Join(segments, e.Name()))
			}
		}
		slices.Sort(files)
		if len(files) > 0 {
			return files[0]
		}
	}
	keys := filepath.Join(backupDir, "keys.enc")
	if info, err := os.Stat(keys); err != nil || info.IsDir() {
		t.Fatalf("backup has neither segment files nor keys.enc")
	}
	return keys
}

// assertFailsClosed requires that restore and verify of backupDir both
// fail, the restore target does not exist afterwards (the product
// removes dst on verification failure), and no .verify-tmp-* residue
// remains in the backup's parent dir.
func assertFailsClosed(t *testing.T, backupDir, target string, key [32]byte, leg string) {
	t.Helper()
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("%s: restore target must not exist before the attempt", leg)
	}
	if err := murmur.Restore(backupDir, target, key[:]); err == nil {
		t.Fatalf("%s: tampered restore succeeded, want integrity failure", leg)
	} else {
		t.Logf("%s: tampered restore rejected: %v", leg, err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("%s: failed restore left partial state at %s", leg, target)
	}
	if err := murmur.VerifyBackup(backupDir, key[:]); err == nil {
		t.Fatalf("%s: tampered verify_backup succeeded, want integrity failure", leg)
	} else {
		t.Logf("%s: tampered verify rejected: %v", leg, err)
	}
	parent := filepath.Dir(backupDir)
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("read backup parent dir: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".verify-tmp-") {
			t.Fatalf("%s: verify left temp residue behind: %s", leg, e.Name())
		}
	}
}

func TestTamperedBackup(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("key entropy: %v", err)
	}
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "tampered-backup",
		NumNodes: 1,
		Key:      key,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("tampered-backup")
		}
		cluster.Finish(failed)
	}()

	if err := cluster.EnsureMigrated(0, namesDDL); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := cluster.Node(0).SetReplication("live_typed_records", true); err != nil {
		t.Fatalf("opt-in live_typed_records: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush after migrate: %v", err)
	}
	for i := 0; i < numRows; i++ {
		typedInsert(t, cluster.Node(0), fmt.Sprintf("tampered-row-%02d", i))
	}
	names := typedNames(t, cluster.Node(0))
	if len(names) != numRows {
		t.Fatalf("seeded row count = %d, want %d", len(names), numRows)
	}
	wantDigest := namesDigest(names)

	// Offline backup of the stopped node through the library.
	cluster.StopNode(0)
	info := cluster.NodeInfo(0)
	dataDir, nodeID := info.DataDir, info.NodeID
	databaseID := cluster.DatabaseID()
	runtimeDir := cluster.RuntimeDir()
	offline := openOffline(t, dataDir, key, databaseID, nodeID)
	pristine := filepath.Join(runtimeDir, "pristine-backup")
	if err := offline.Backup(pristine); err != nil {
		t.Fatalf("offline backup: %v", err)
	}
	if err := offline.Close(); err != nil {
		t.Fatalf("close offline handle: %v", err)
	}

	// The pristine backup verifies cleanly (positive control setup).
	if err := murmur.VerifyBackup(pristine, key[:]); err != nil {
		t.Fatalf("pristine backup must verify: %v", err)
	}

	// Payload tamper (gzip-magic flip): flip the first two bytes
	// of a payload file so the stream is rejected before use.
	payloadTampered := filepath.Join(runtimeDir, "tampered-payload")
	copyDirAll(t, pristine, payloadTampered)
	target := payloadFile(t, payloadTampered)
	bytes, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read payload file: %v", err)
	}
	if len(bytes) < 2 {
		t.Fatalf("payload file too small to flip")
	}
	bytes[0] ^= 0xFF
	bytes[1] ^= 0xFF
	if err := os.WriteFile(target, bytes, 0o600); err != nil {
		t.Fatalf("write flipped payload: %v", err)
	}
	assertFailsClosed(t, payloadTampered, filepath.Join(runtimeDir, "target-payload"), key, "payload-flip")

	// Metadata tamper (corrupt backup-metadata.json): flip a
	// byte in the middle of the store manifest.
	metaTampered := filepath.Join(runtimeDir, "tampered-manifest")
	copyDirAll(t, pristine, metaTampered)
	manifest := filepath.Join(metaTampered, "manifest")
	bytes, err = os.ReadFile(manifest)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if len(bytes) == 0 {
		t.Fatalf("manifest must be non-empty")
	}
	bytes[len(bytes)/2] ^= 0xFF
	if err := os.WriteFile(manifest, bytes, 0o600); err != nil {
		t.Fatalf("write tampered manifest: %v", err)
	}
	assertFailsClosed(t, metaTampered, filepath.Join(runtimeDir, "target-manifest"), key, "manifest-edit")

	// Truncation tamper (late-payload flip surfacing mid-read):
	// cut a payload file short; the restore must still fail with no
	// partial state published.
	truncTampered := filepath.Join(runtimeDir, "tampered-truncated")
	copyDirAll(t, pristine, truncTampered)
	target = payloadFile(t, truncTampered)
	stat, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat payload: %v", err)
	}
	newLen := stat.Size() - stat.Size()/2 - 1
	if newLen < 0 {
		newLen = 0
	}
	if err := os.Truncate(target, newLen); err != nil {
		t.Fatalf("truncate payload: %v", err)
	}
	assertFailsClosed(t, truncTampered, filepath.Join(runtimeDir, "target-truncated"), key, "payload-truncate")

	// Positive control: the pristine backup restores exactly under a
	// fresh writer identity (row count plus canonical digest).
	var fresh [16]byte
	if _, err := rand.Read(fresh[:]); err != nil {
		t.Fatalf("fresh id entropy: %v", err)
	}
	if fresh == nodeID {
		t.Fatalf("fresh identity must differ")
	}
	staging := filepath.Join(runtimeDir, "staging")
	if err := murmur.RestoreWithOptions(pristine, staging, key[:], murmur.RestoreOptions{NewNodeID: &fresh}); err != nil {
		t.Fatalf("pristine restore: %v", err)
	}
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.OpenExisting
	opts.DatabaseID = &databaseID
	opts.NodeID = &fresh
	restored, err := murmur.Open(staging, key[:], opts)
	if err != nil {
		t.Fatalf("open restored data: %v", err)
	}
	restoredNames := typedNames(t, restored)
	if len(restoredNames) != numRows {
		t.Fatalf("restored row count = %d, want %d", len(restoredNames), numRows)
	}
	if got := namesDigest(restoredNames); got != wantDigest {
		t.Fatalf("restored digest must match pre-backup digest: got %s want %s", got, wantDigest)
	}
	if restored.NodeID() != fresh {
		t.Fatalf("restored identity must be fresh")
	}
	if err := restored.Close(); err != nil {
		t.Fatalf("close restored handle: %v", err)
	}
	t.Logf("PASS: tampered backups fail closed; pristine backup restores exactly")
	failed = false
}
