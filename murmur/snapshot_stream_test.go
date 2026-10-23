package murmur

import (
	"os"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

func TestSnapshotSourceClose(t *testing.T) {
	// Nil checkpoint: key zeroed, no panic.
	key := testKey()
	source := &SnapshotSource{Options: spool.Options{MasterKey: key}}
	source.Close()
	for i, b := range key {
		if b != 0 {
			t.Fatalf("key byte %d not zeroed", i)
		}
	}
	// Orphaned checkpoint dir is removed.
	dir := t.TempDir()
	source = &SnapshotSource{
		Checkpoint: &spool.Checkpoint{ID: "orphan", Dir: dir},
		Options:    spool.Options{MasterKey: testKey()},
	}
	source.Close()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("checkpoint dir kept")
	}
	if source.Checkpoint != nil {
		t.Fatal("checkpoint not cleared")
	}
}

func TestSnapshotPagesEmpty(t *testing.T) {
	dir := t.TempDir()
	store, err := spool.Open(spool.Options{Path: dir, MasterKey: testKey()})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	source := &SnapshotSource{Options: spool.Options{Path: dir, MasterKey: testKey()}}
	pages, err := OpenSnapshotPages(source)
	if err != nil {
		t.Fatal(err)
	}
	defer pages.Close()
	if pages.Source() != source {
		t.Fatal("source mismatch")
	}
	page, done, err := pages.Next()
	if err != nil || !done || page != nil {
		t.Fatalf("empty next = %d bytes, done=%v, %v", len(page), done, err)
	}
	pages.Close()
}

func TestSnapshotPagesPolicyRecord(t *testing.T) {
	dir := t.TempDir()
	store, err := spool.Open(spool.Options{Path: dir, MasterKey: testKey()})
	if err != nil {
		t.Fatal(err)
	}
	err = store.Commit([]spool.Mutation{{Key: []byte("10/policy/x"), Value: []byte("v")}}, spool.DurabilitySync)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	source := &SnapshotSource{Options: spool.Options{Path: dir, MasterKey: testKey()}}
	pages, err := OpenSnapshotPages(source)
	if err != nil {
		t.Fatal(err)
	}
	defer pages.Close()
	page, done, err := pages.Next()
	if err != nil || done || len(page) == 0 {
		t.Fatalf("policy next = %d bytes, done=%v, %v", len(page), done, err)
	}
	page, done, err = pages.Next()
	if err != nil || !done || page != nil {
		t.Fatalf("second next = %d bytes, done=%v, %v", len(page), done, err)
	}
}

func TestOpenSnapshotPagesError(t *testing.T) {
	source := &SnapshotSource{Options: spool.Options{MasterKey: testKey()}}
	if _, err := OpenSnapshotPages(source); err == nil {
		t.Fatal("empty path opened")
	}
}
