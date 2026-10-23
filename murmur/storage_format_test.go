package murmur

import (
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// TestSupportedFormatWindow pins the open-compatibility window: the
// current format plus the two previous generations.
func TestSupportedFormatWindow(t *testing.T) {
	for _, f := range []uint32{6, 7, storageFormat} {
		if !supportedFormat(f) {
			t.Fatalf("format %d rejected", f)
		}
	}
	for _, f := range []uint32{0, 1, 5, storageFormat + 1, storageFormat + 100, ^uint32(0)} {
		if supportedFormat(f) {
			t.Fatalf("format %d accepted", f)
		}
	}
}

func rawSpoolOptions(dir string) spool.Options {
	opts := spool.DefaultOptions(dir)
	opts.MasterKey = testKey()
	opts.Codecs = []compression.Codec{compression.MustZstd(3)}
	opts.ReclaimInterval = 0
	return opts
}

// rewriteIdentityFormat overwrites the 00/identity record of a stopped
// store with a new format generation.
func rewriteIdentityFormat(t *testing.T, dir string, format uint32) {
	t.Helper()
	store, err := spool.Open(rawSpoolOptions(dir))
	if err != nil {
		t.Fatalf("spool open: %v", err)
	}
	defer func() { _ = store.Close() }()
	raw, err := store.Get([]byte("00/identity"))
	if err != nil {
		t.Fatalf("spool get identity: %v", err)
	}
	if raw == nil {
		t.Fatal("no 00/identity record")
	}
	ident, err := DecodeIdentity(raw)
	if err != nil {
		t.Fatalf("decode identity: %v", err)
	}
	ident.Format = format
	if err := store.Commit([]spool.Mutation{{
		Key:   []byte("00/identity"),
		Value: EncodeIdentity(ident),
	}}, spool.DurabilitySync); err != nil {
		t.Fatalf("commit identity: %v", err)
	}
}

func openFormatFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir() + "/db"
	db := openTestDB(t, dir)
	if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir
}

// TestOpenRejectsNewerFormat proves a store written by a newer release
// fails closed with the FORMAT guard instead of opening half-decoded.
func TestOpenRejectsNewerFormat(t *testing.T) {
	dir := openFormatFixture(t)
	rewriteIdentityFormat(t, dir, storageFormat+1)
	_, err := Open(dir, testKey(), DefaultOptions())
	if err == nil {
		t.Fatal("newer format opened")
	}
	if !strings.Contains(err.Error(), "unsupported database format") {
		t.Fatalf("err = %v, want format guard", err)
	}
}

// TestOpenRejectsAncientFormat proves a store older than the compat
// window fails closed the same way.
func TestOpenRejectsAncientFormat(t *testing.T) {
	dir := openFormatFixture(t)
	rewriteIdentityFormat(t, dir, 5)
	_, err := Open(dir, testKey(), DefaultOptions())
	if err == nil {
		t.Fatal("ancient format opened")
	}
	if !strings.Contains(err.Error(), "unsupported database format") {
		t.Fatalf("err = %v, want format guard", err)
	}
}
