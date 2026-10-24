// Live scenario: product-created durable paths stay private after SQL
// initialization, backup, and restore.
//
// Go port of tests/file_permissions.rs using tests-live/gosqlharness.
// Identical behavior: a local-only single node is audited (no symlinks,
// no group/other bits) after init, after 25 rows + flush, on the backup
// save dir, on the uploaded backup copy, after DELETE + RESTORE, after a
// flush, and across a restart; restored rows read back exactly. The Rust
// #[cfg(unix)] gate becomes a runtime.GOOS skip.
package filepermissions

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

func TestSQLStorageAndBackupPermissionsRemainPrivateAfterRestore(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	if runtime.GOOS == "windows" {
		t.Skip("unix permission audit (Rust: #[cfg(unix)])")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:      "file-permissions",
		NumNodes:  1,
		LocalOnly: true,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	data := cluster.NodeInfo(0).DataDir
	if info, err := os.Stat(filepath.Join(data, "keys.enc")); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("keys.enc missing: %v", err)
	}
	privateTree(t, data)
	if _, err := cluster.Node(0).Simple("CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL)"); err != nil {
		t.Fatalf("create records: %v", err)
	}
	var expected [][]string
	for id := int64(0); id < 25; id++ {
		name := fmt.Sprintf("kept-%03d", id)
		if _, err := cluster.Node(0).Execute("INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
		expected = append(expected, []string{name})
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush rows: %v", err)
	}
	privateTree(t, data)
	backup, err := cluster.Node(0).Simple("MURMUR BACKUP SAVE")
	if err != nil {
		t.Fatalf("backup save: %v", err)
	}
	privateTree(t, filepath.Join(cluster.NodeInfo(0).Dir, "backups", backup[0][0]))
	var archive []byte
	if err := gosqlharness.Download(cluster.Node(0),
		"MURMUR BACKUP DOWNLOAD "+gosqlharness.SQLLiteral(backup[0][0]), &archive); err != nil {
		t.Fatalf("backup download: %v", err)
	}
	if err := gosqlharness.Upload(cluster.Node(0), "permissions-copy.murmur-backup", archive); err != nil {
		t.Fatalf("backup upload: %v", err)
	}
	privateTree(t, filepath.Join(cluster.NodeInfo(0).Dir, "backups", "permissions-copy.murmur-backup"))
	if _, err := cluster.Node(0).Simple("DELETE FROM records"); err != nil {
		t.Fatalf("delete rows: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR RESTORE FROM " +
		gosqlharness.SQLLiteral("permissions-copy.murmur-backup") + " KEY " +
		gosqlharness.SQLLiteral(gosqlharness.Key)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	fresh, err := cluster.Connect(0)
	if err != nil {
		t.Fatalf("fresh connect: %v", err)
	}
	rows, err := fresh.Simple("SELECT name FROM records ORDER BY id")
	if err != nil {
		_ = fresh.Close()
		t.Fatalf("read restored: %v", err)
	}
	if !equalRows(rows, expected) {
		_ = fresh.Close()
		t.Fatalf("read restored: got %v, want %v", rows, expected)
	}
	if _, err := fresh.Simple("MURMUR FLUSH"); err != nil {
		_ = fresh.Close()
		t.Fatalf("flush restored: %v", err)
	}
	_ = fresh.Close()
	privateTree(t, data)
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node: %v", err)
	}
	privateTree(t, data)
	rows, err = cluster.Node(0).Simple("SELECT name FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if !equalRows(rows, expected) {
		t.Fatalf("read after restart: got %v, want %v", rows, expected)
	}
	failed = false
}

// privateTree asserts every path under root is symlink-free and carries
// no group/other permission bits (Rust: private_tree).
func privateTree(t *testing.T, root string) {
	t.Helper()
	pending := []string{root}
	count := 0
	for len(pending) > 0 {
		path := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		meta, err := os.Lstat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if meta.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("unexpected symlink in product data tree: %s", path)
		}
		if meta.Mode().Perm()&0o077 != 0 {
			t.Fatalf("group/other access on %s (mode %o)", path, meta.Mode().Perm()&0o777)
		}
		count++
		if meta.IsDir() {
			entries, err := os.ReadDir(path)
			if err != nil {
				t.Fatalf("read dir %s: %v", path, err)
			}
			for _, e := range entries {
				pending = append(pending, filepath.Join(path, e.Name()))
			}
		}
	}
	if count == 0 {
		t.Fatal("permission audit visited no paths")
	}
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
