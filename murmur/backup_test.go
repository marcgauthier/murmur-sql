package murmur

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// makeBackup builds a migrated database with one row and returns its
// online backup directory.
func makeBackup(t *testing.T) (backup string, dir string) {
	t.Helper()
	dir = t.TempDir()
	db := openTestDB(t, dir+"/db")
	if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, "INSERT INTO users (id, name) VALUES (7, 'backup')")
	if err := db.Backup(dir + "/backup"); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir + "/backup", dir
}

func TestRestoreBackupRejectsBadInput(t *testing.T) {
	backup, dir := makeBackup(t)
	opts := DefaultRestoreOptions()
	if err := RestoreBackup(backup, dir+"/r1", []byte("short"), &opts); err == nil {
		t.Fatal("short key accepted")
	}
	if err := RestoreBackup(dir, dir+"/r2", testKey(), &opts); err == nil ||
		!strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("missing manifest = %v", err)
	}
	if err := os.Mkdir(dir+"/r3", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := RestoreBackup(backup, dir+"/r3", testKey(), &opts); err == nil {
		t.Fatal("existing destination accepted")
	}
	wrong := make([]byte, 32)
	for i := range wrong {
		wrong[i] = 0xAA
	}
	if err := RestoreBackup(backup, dir+"/r4", wrong, &opts); err == nil {
		t.Fatal("wrong key accepted")
	}
}

func TestRestoreBackupRemovesCorruptCopy(t *testing.T) {
	backup, dir := makeBackup(t)
	// Corrupt the backup's manifest and truncate one segment: the copy
	// succeeds but verification must fail and remove the destination.
	manifest := filepath.Join(backup, "manifest")
	raw, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF
	if err := os.WriteFile(manifest, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	segments, err := os.ReadDir(filepath.Join(backup, "segments"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range segments {
		if e.IsDir() {
			continue
		}
		path := filepath.Join(backup, "segments", e.Name())
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		keep := info.Size() / 2
		if keep < 16 {
			keep = 16
		}
		if info.Size() > keep {
			if err := os.Truncate(path, keep); err != nil {
				t.Fatal(err)
			}
		}
		break
	}
	opts := DefaultRestoreOptions()
	if err := RestoreBackup(backup, dir+"/restored", testKey(), &opts); err == nil {
		t.Fatal("corrupt backup restored")
	}
	if _, err := os.Stat(dir + "/restored"); !os.IsNotExist(err) {
		t.Fatal("corrupt destination kept")
	}
}

func TestCopyCheckpointFiles(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(filepath.Join(tmp, "segments"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "segments", "s1"), []byte("segment-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "COMPLETE"), []byte("done"), 0o600); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(dir, "backup")
	if err := CopyCheckpointFiles(tmp, []string{filepath.Join(tmp, "segments", "s1")}, dst); err != nil {
		t.Fatal(err)
	}
	seg, err := os.ReadFile(filepath.Join(dst, "segments", "s1"))
	if err != nil || string(seg) != "segment-bytes" {
		t.Fatalf("segment = %q, %v", seg, err)
	}
	complete, err := os.ReadFile(filepath.Join(dst, "COMPLETE"))
	if err != nil || string(complete) != "done" {
		t.Fatalf("complete = %q, %v", complete, err)
	}
	// Escaping paths are refused and the destination removed.
	evil := filepath.Join(dir, "evil")
	if err := CopyCheckpointFiles(tmp, []string{"/etc/hostname"}, evil); err == nil ||
		!strings.Contains(err.Error(), "escapes") {
		t.Fatalf("escaping path = %v", err)
	}
	if _, err := os.Stat(evil); !os.IsNotExist(err) {
		t.Fatal("evil destination kept")
	}
	// The destination must be new.
	if err := CopyCheckpointFiles(tmp, nil, dst); err == nil {
		t.Fatal("existing destination accepted")
	}
}
