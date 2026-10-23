package murmur

import (
	"bytes"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

func openLogical(t *testing.T, dir string) *LogicalStore {
	t.Helper()
	opts := spool.DefaultOptions(dir)
	opts.MasterKey = bytesRepeat(9, 32)
	opts.ReclaimInterval = 0
	s, err := spool.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return NewLogicalStore(s)
}

func TestLogicalSplitOverwriteDelete(t *testing.T) {
	dir := t.TempDir()
	store := openLogical(t, dir)
	key := []byte("04/test/value")
	data := bytesRepeat(42, 17<<20)
	if err := store.Commit([]spool.Mutation{{Key: key, Value: data}}, spool.DurabilitySync); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(key)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("get = %d bytes, %v", len(got), err)
	}
	if n := len(store.Physical().KeysWithPrefix([]byte("11/blob/"))); n <= 256 {
		t.Fatalf("blob pieces = %d", n)
	}
	magic := append([]byte(nil), blobMagic...)
	if err := store.Commit([]spool.Mutation{
		{Key: key, Value: magic},
		{Key: key, Value: []byte("second")},
	}, spool.DurabilitySync); err != nil {
		t.Fatal(err)
	}
	got, err = store.Get(key)
	if err != nil || string(got) != "second" {
		t.Fatalf("get = %q, %v", got, err)
	}
	if keys := store.Physical().KeysWithPrefix([]byte("11/blob/")); len(keys) != 0 {
		t.Fatalf("stale pieces = %d", len(keys))
	}
	if err := store.Commit([]spool.Mutation{{Key: key, Value: magic}}, spool.DurabilitySync); err != nil {
		t.Fatal(err)
	}
	if err := store.Physical().Close(); err != nil {
		t.Fatal(err)
	}
	store = openLogical(t, dir)
	got, err = store.Get(key)
	if err != nil || !bytes.Equal(got, magic) {
		t.Fatalf("reopened get = %q, %v", got, err)
	}
	if err := store.Commit([]spool.Mutation{{Key: key, Deleted: true}}, spool.DurabilitySync); err != nil {
		t.Fatal(err)
	}
	got, err = store.Get(key)
	if err != nil || got != nil {
		t.Fatalf("deleted get = %q, %v", got, err)
	}
}
