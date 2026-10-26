package murmur

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Run on Windows too: syncing a read-only file handle succeeds on Unix but
// fails with Access is denied on Windows, even when the file was just created.
func TestCopyFileSyncBackupPayload(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "manifest")
	data := bytes.Repeat([]byte("encrypted backup bytes\n"), 1024)
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := copyFileSync(source, destination); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("copied backup bytes differ from source")
	}
}

func TestCopyFileSyncPropagatesFailures(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := copyFileSync(source, filepath.Join(root, "manifest")); err == nil {
		t.Fatal("missing source accepted")
	}
	if err := os.WriteFile(source, []byte("backup"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := copyFileSync(source, filepath.Join(root, "missing", "manifest")); err == nil {
		t.Fatal("unwritable destination accepted")
	}
}

func TestSyncBackupDirectory(t *testing.T) {
	if err := syncDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}
