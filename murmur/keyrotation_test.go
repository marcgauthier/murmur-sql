package murmur

import (
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
)

// TestRotateDataKeyKeepsDataReadable pins that a data-key rotation
// re-keys the store without losing rows, across a reopen with the same
// application key.
func TestRotateDataKeyKeepsDataReadable(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	if err := db.Migrate(NewMigration(
		`CREATE TABLE kv(k TEXT NOT NULL PRIMARY KEY, v TEXT NOT NULL)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.Transaction(func(s *Sql) (any, error) {
		_, err := s.Execute("INSERT INTO kv(k, v) VALUES(?, ?)", "a", "1")
		return nil, err
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	before := db.KeyInventory()
	if !before.Encrypted || len(before.DataKeys) == 0 {
		t.Fatalf("inventory before rotation: %+v", before)
	}
	if err := db.RotateDataKey(); err != nil {
		t.Fatalf("rotate data key: %v", err)
	}
	after := db.KeyInventory()
	if after.ActiveDataKeyID == before.ActiveDataKeyID && len(after.DataKeys) == len(before.DataKeys) {
		t.Fatalf("rotation changed nothing: before=%+v after=%+v", before, after)
	}
	readValue := func(db *Database) string {
		t.Helper()
		v, err := db.Read(func(s *Sql) (any, error) {
			return s.Query("SELECT v FROM kv WHERE k = ?", "a")
		})
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		rows := v.([][]sqlite.Value)
		if len(rows) != 1 {
			t.Fatalf("row count = %d, want 1", len(rows))
		}
		return rows[0][0].Str
	}
	if got := readValue(db); got != "1" {
		t.Fatalf("value after rotation = %q, want %q", got, "1")
	}
	path := dir + "/db"
	key := testKey()
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	options := DefaultOptions()
	options.Mode = OpenExisting
	db2, err := Open(path, key, options)
	if err != nil {
		t.Fatalf("reopen after rotation: %v", err)
	}
	defer db2.Close()
	if got := readValue(db2); got != "1" {
		t.Fatalf("value after reopen = %q, want %q", got, "1")
	}
}

// TestRotateMasterKeyRekeysStore pins that a master-key rotation swaps the
// application key: the new key opens the store and the old key fails.
func TestRotateMasterKeyRekeysStore(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	if err := db.Migrate(NewMigration(
		`CREATE TABLE kv(k TEXT NOT NULL PRIMARY KEY, v TEXT NOT NULL)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.Transaction(func(s *Sql) (any, error) {
		_, err := s.Execute("INSERT INTO kv(k, v) VALUES(?, ?)", "a", "1")
		return nil, err
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	var next [32]byte
	for i := range next {
		next[i] = byte(0xa0 + i)
	}
	if err := db.RotateMasterKey(next); err != nil {
		t.Fatalf("rotate master key: %v", err)
	}
	v, err := db.Read(func(s *Sql) (any, error) {
		return s.Query("SELECT v FROM kv WHERE k = ?", "a")
	})
	if err != nil {
		t.Fatalf("read after rotation: %v", err)
	}
	if got := v.([][]sqlite.Value)[0][0].Str; got != "1" {
		t.Fatalf("value = %q, want %q", got, "1")
	}
	path := dir + "/db"
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	options := DefaultOptions()
	options.Mode = OpenExisting
	if _, err := Open(path, testKey(), options); err == nil {
		t.Fatalf("old key opened the store after master rotation")
	}
	db2, err := Open(path, next[:], options)
	if err != nil {
		t.Fatalf("new key failed to open: %v", err)
	}
	defer db2.Close()
	v, err = db2.Read(func(s *Sql) (any, error) {
		return s.Query("SELECT v FROM kv WHERE k = ?", "a")
	})
	if err != nil {
		t.Fatalf("read with new key: %v", err)
	}
	if got := v.([][]sqlite.Value)[0][0].Str; got != "1" {
		t.Fatalf("value = %q, want %q", got, "1")
	}
}

// TestRetryStorageSucceedsWhenHealthy pins the recovery entry point on a
// healthy store. Fault-injection retry paths live in the spool tests.
func TestRetryStorageSucceedsWhenHealthy(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(`CREATE TABLE t(id INTEGER PRIMARY KEY)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := db.RetryStorage(); err != nil {
		t.Fatalf("retry storage on healthy store: %v", err)
	}
}
