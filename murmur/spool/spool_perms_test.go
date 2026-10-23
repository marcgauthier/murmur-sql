package spool_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// TestSecretPathsLockedDown pins the file-permissions contract on one
// store: the key registry is owner-only and product-created directories
// grant nothing to group/other. (The file-permissions live scenario
// proves the same across backup plus restore.)
func TestSecretPathsLockedDown(t *testing.T) {
	dir := t.TempDir()
	store, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Commit([]spool.Mutation{{Key: []byte("k"), Value: []byte("v")}}, spool.DurabilitySync); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	keys := filepath.Join(dir, "keys.enc")
	info, err := os.Stat(keys)
	if err != nil {
		t.Fatalf("stat keys.enc: %v", err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Fatalf("keys.enc mode %04o grants group/other", mode)
	}
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if mode := info.Mode().Perm(); mode&0o077 != 0 {
				t.Errorf("dir %s mode %04o grants group/other", path, mode)
			}
			return nil
		}
		// Secret-bearing files must be owner-only; segment data is
		// ciphertext but still must not grant group/other access.
		if mode := info.Mode().Perm(); mode&0o077 != 0 {
			t.Errorf("file %s mode %04o grants group/other", path, mode)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}
