// Live scenario: product-owned secret paths stay locked down.
//
// Go port of the Rust `file-permissions` scenario
// (/home/marc/MURMUR/tests-live/file-permissions/tests/file_permissions.rs:
// TestFilePermissionsFreshInitAndBackupRestore). Product-owned secret
// paths (the data/keys.enc key container and the data directory) must
// grant nothing to group/other (0600 files, 0700 dirs) after fresh init
// and again after backup plus restore. Harness-minted material
// (tls/node.key) and operator-owned paths are audit-logged, never
// asserted, since no product change can alter fixture-chosen modes.
//
// Plain SQL replaces the typed-records HTTP API; stop/start replaces
// StopNode/StartNode+unlock; the backup is an online Backup on the
// surviving node; restore uses RestoreWithOptions with a fresh node id.
package filepermissions

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// tableDDL is the typed table (Rust: live_typed_records managed table).
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)"

const (
	// numRows is the seed row count (Rust: 25).
	numRows = 25
	// convergeTimeout is the convergence deadline (Rust: 30 s).
	convergeTimeout = 30 * time.Second
	// reconvergeTimeout is the post-restart deadline (Rust: 60 s).
	reconvergeTimeout = 60 * time.Second
	// poll is the convergence poll interval (Rust: 200 ms).
	poll = 200 * time.Millisecond
)

// keysFileName is the product-owned key container, relative to the node
// data dir (Rust: data/keys.enc).
const keysFileName = "keys.enc"

// typedInsert writes one typed row on one node (Rust: TypedInsert).
func typedInsert(t *testing.T, db *murmur.Database, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name) VALUES(?)", name)
		return nil, err
	}); err != nil {
		t.Fatalf("typed insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after typed insert: %v", err)
	}
}

// typedNames returns the canonical ordered names on one node (Rust:
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

// waitConverged waits until every running node holds the same want names.
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		var first string
		for i := 0; i < cluster.Len(); i++ {
			names := typedNames(t, cluster.Node(i))
			if len(names) != want {
				return false
			}
			joined := strings.Join(names, "\x00")
			if i == 0 {
				first = joined
			} else if joined != first {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not converge on %d rows within %v", want, timeout)
	}
}

// assertSecretPathsLockedDown fails when any product-owned secret path
// on node idx carries group/other permission bits. node.key is
// deliberately absent: harness-minted fixture material, audit-logged in
// logModeTable instead of asserted here.
func assertSecretPathsLockedDown(t *testing.T, cluster *harness.Cluster, idx int, phase string) {
	t.Helper()
	info := cluster.NodeInfo(idx)
	keys := filepath.Join(info.DataDir, keysFileName)
	if st, err := os.Stat(keys); err != nil || !st.Mode().IsRegular() {
		t.Fatalf("%s node%d: expected product key container at %s", phase, idx+1, keys)
	}
	for _, root := range []string{keys, info.DataDir} {
		assertTreeLockedDown(t, root, fmt.Sprintf("%s node%d %s", phase, idx+1, root))
	}
}

// assertTreeLockedDown walks root (a file or directory) and fails on the
// first path whose mode grants anything to group or other. Symlinks are
// logged and skipped (their 0777 link modes are not access grants).
func assertTreeLockedDown(t *testing.T, root, label string) {
	t.Helper()
	var violations []string
	var seen uint64
	stack := []string{root}
	for len(stack) > 0 {
		path := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		meta, err := os.Lstat(path)
		if err != nil {
			t.Fatalf("%s: stat %s: %v", label, path, err)
		}
		if meta.Mode()&os.ModeSymlink != 0 {
			t.Logf("%s: symlink skipped: %s", label, path)
			continue
		}
		if !meta.Mode().IsRegular() && !meta.IsDir() {
			violations = append(violations, fmt.Sprintf("%s (special file)", path))
			continue
		}
		seen++
		if perm := meta.Mode().Perm(); perm&0o077 != 0 {
			violations = append(violations, fmt.Sprintf("%s mode=%04o", path, perm))
		}
		if meta.IsDir() {
			entries, err := os.ReadDir(path)
			if err != nil {
				t.Fatalf("%s: read_dir %s: %v", label, path, err)
			}
			for _, entry := range entries {
				stack = append(stack, filepath.Join(path, entry.Name()))
			}
		}
	}
	if seen == 0 {
		t.Fatalf("%s: walked zero paths under %s", label, root)
	}
	if len(violations) > 0 {
		shown := violations
		suffix := ""
		if len(shown) > 10 {
			shown = shown[:10]
			suffix = fmt.Sprintf("\n  ... and %d more", len(violations)-10)
		}
		t.Fatalf("%s: %d paths grant group/other access:\n  %s%s",
			label, len(violations), strings.Join(shown, "\n  "), suffix)
	}
	t.Logf("%s: %s locked down (no group/other bits)", label, root)
}

// logModeTable records the observed modes of operator-owned paths for
// audit evidence. These modes are chosen by the test fixture, not the
// product. The harness provisions no logs dir or config file, and its
// TLS material is DER (ca.der/node.der/node.key).
func logModeTable(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		tls := filepath.Join(info.Dir, "tls")
		paths := []string{
			info.Dir,
			info.DataDir,
			tls,
			filepath.Join(tls, "ca.der"),
			filepath.Join(tls, "node.der"),
			filepath.Join(tls, "node.key"),
		}
		for _, p := range paths {
			meta, err := os.Lstat(p)
			if err != nil {
				t.Logf("node%d %s: stat error: %v", i+1, p, err)
				continue
			}
			t.Logf("node%d %s: mode=%04o dir=%v", i+1, p, meta.Mode().Perm(), meta.IsDir())
		}
	}
}

func TestFilePermissionsFreshInitAndBackupRestore(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file permissions (0600/0700) do not apply to Windows NTFS ACLs")
	}
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("key entropy: %v", err)
	}
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "file-permissions",
		NumNodes: 2,
		Key:      key,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("file-permissions")
		}
		cluster.Finish(failed)
	}()

	// Phase 1: fresh init. Every node's private key material must
	// already be locked down before any data is written.
	for i := 0; i < cluster.Len(); i++ {
		assertSecretPathsLockedDown(t, cluster, i, "fresh-init")
	}
	logModeTable(t, cluster)

	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("node%d migrate failed: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("live_typed_records", true); err != nil {
			t.Fatalf("opt-in live_typed_records: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Positive control: honest writes replicate and converge, proving
	// the permission posture does not break the database.
	for i := 0; i < numRows; i++ {
		typedInsert(t, cluster.Node(0), fmt.Sprintf("row-%d", i))
	}
	waitConverged(t, cluster, numRows, convergeTimeout)

	// Phase 2: backup plus restore through the product library, then
	// re-assert: backup artifacts, the restored tree, and the live nodes.
	cluster.StopNode(1)
	backupDir := filepath.Join(cluster.RuntimeDir(), "backup-dest")
	if err := cluster.Node(0).Backup(backupDir); err != nil {
		t.Fatalf("backup node 1: %v", err)
	}
	assertTreeLockedDown(t, backupDir, "backup-artifacts")

	restoreTarget := filepath.Join(cluster.RuntimeDir(), "restored")
	var fresh [16]byte
	if _, err := rand.Read(fresh[:]); err != nil {
		t.Fatalf("fresh node id entropy: %v", err)
	}
	if err := murmur.RestoreWithOptions(backupDir, restoreTarget, append([]byte(nil), key[:]...), murmur.RestoreOptions{
		NewNodeID: &fresh,
	}); err != nil {
		t.Fatalf("restore backup: %v", err)
	}
	assertTreeLockedDown(t, restoreTarget, "restored-tree")
	// The restored key container and data directory in particular must
	// be private (the backup is a store dir, so it IS the data dir).
	assertTreeLockedDown(t, restoreTarget, "restored-data")

	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart node 2: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	waitConverged(t, cluster, numRows, reconvergeTimeout)

	for i := 0; i < cluster.Len(); i++ {
		assertSecretPathsLockedDown(t, cluster, i, "post-backup-restore")
	}
	t.Log("PASS: private key material locked down (0600/0700) fresh and post restore")

	failed = false
}
