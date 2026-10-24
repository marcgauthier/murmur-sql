package murmur

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

func TestCredentialValidation(t *testing.T) {
	if err := (Credential{}).validate(); err == nil {
		t.Fatal("empty credential accepted")
	}
	if err := (Credential{Key: bytes.Repeat([]byte{1}, 32), Passphrase: "x"}).validate(); err == nil {
		t.Fatal("dual credential accepted")
	}
	if err := (Credential{Key: []byte{1, 2, 3}}).validate(); err == nil {
		t.Fatal("short key accepted")
	}
	if err := (Credential{Key: bytes.Repeat([]byte{1}, 32)}).validate(); err != nil {
		t.Fatalf("32-byte key rejected: %v", err)
	}
	if err := (Credential{Passphrase: "secret"}).validate(); err != nil {
		t.Fatalf("passphrase rejected: %v", err)
	}
}

func TestCredentialForSecret(t *testing.T) {
	raw := strings.Repeat("ab", 32)
	for _, kdf := range []uint8{spool.KDFDirect, spool.KDFArgon2id} {
		cred := CredentialForSecret(raw, kdf)
		if cred.Passphrase != "" || len(cred.Key) != 32 {
			t.Fatalf("raw secret kdf=%d mapped to %+v, want 32-byte key", kdf, cred)
		}
		if !bytes.Equal(cred.Key, bytes.Repeat([]byte{0xab}, 32)) {
			t.Fatalf("raw secret kdf=%d decoded to %x", kdf, cred.Key)
		}
	}
	legacy := CredentialForSecret("secret", spool.KDFDirect)
	sum := sha256.Sum256([]byte("secret"))
	if legacy.Passphrase != "" || !bytes.Equal(legacy.Key, sum[:]) {
		t.Fatalf("KDF-0 passphrase mapped to %+v, want SHA-256 master key", legacy)
	}
	migrated := CredentialForSecret("secret", spool.KDFArgon2id)
	if migrated.Passphrase != "secret" || len(migrated.Key) != 0 {
		t.Fatalf("KDF-1 passphrase mapped to %+v, want passphrase", migrated)
	}
	unknown := CredentialForSecret("secret", 9)
	if unknown.Passphrase != "" || !bytes.Equal(unknown.Key, sum[:]) {
		t.Fatalf("unknown KDF mapped to %+v, want legacy mapping", unknown)
	}
	notHex := strings.Repeat("zz", 32)
	if cred := CredentialForSecret(notHex, spool.KDFArgon2id); cred.Passphrase != notHex {
		t.Fatalf("non-hex 64-char secret mapped to %+v, want passphrase", cred)
	}
}

func writeCanary(t *testing.T, db *Database) {
	t.Helper()
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
}

func readCanary(t *testing.T, db *Database) string {
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

func keyHint(t *testing.T, path string) spool.KeyHint {
	t.Helper()
	hint, err := spool.ReadKeyHint(path)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	return hint
}

// TestPassphraseOpenRoundTrip pins Argon2id store creation and reopen:
// the envelope reports KDF-1, the passphrase opens it, and both a wrong
// passphrase and a raw master key fail closed.
func TestPassphraseOpenRoundTrip(t *testing.T) {
	path := t.TempDir() + "/db"
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	db, err := OpenWithCredential(path, PassphraseCredential("correct horse"), options)
	if err != nil {
		t.Fatalf("create with passphrase: %v", err)
	}
	writeCanary(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if hint := keyHint(t, path); hint.KDF != spool.KDFArgon2id {
		t.Fatalf("hint KDF = %d, want KDFArgon2id", hint.KDF)
	}
	options.Mode = OpenExisting
	if _, err := OpenWithCredential(path, PassphraseCredential("wrong horse"), options); !errors.Is(err, ErrWrongKey) {
		t.Fatalf("wrong passphrase returned %v, want ErrWrongKey", err)
	}
	if _, err := Open(path, bytes.Repeat([]byte{0x4b}, 32), options); !errors.Is(err, ErrWrongKey) {
		t.Fatalf("raw key against passphrase store returned %v, want ErrWrongKey", err)
	}
	db2, err := OpenWithCredential(path, PassphraseCredential("correct horse"), options)
	if err != nil {
		t.Fatalf("reopen with passphrase: %v", err)
	}
	defer db2.Close()
	if got := readCanary(t, db2); got != "1" {
		t.Fatalf("value = %q, want %q", got, "1")
	}
}

// TestLegacyPassphraseMigration pins the KDF-0 to KDF-1 migration: a
// store created under the legacy SHA-256-derived master key reopens
// through the legacy mapping, rotates to a passphrase, and afterwards
// only the passphrase opens it, with rows intact.
func TestLegacyPassphraseMigration(t *testing.T) {
	path := t.TempDir() + "/db"
	legacy := CredentialForSecret("legacy-secret", spool.KDFDirect)
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	db, err := OpenWithCredential(path, legacy, options)
	if err != nil {
		t.Fatalf("create legacy store: %v", err)
	}
	writeCanary(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if hint := keyHint(t, path); hint.KDF != spool.KDFDirect {
		t.Fatalf("hint KDF = %d, want KDFDirect", hint.KDF)
	}
	options.Mode = OpenExisting
	db2, err := OpenWithCredential(path, CredentialForSecret("legacy-secret", spool.KDFDirect), options)
	if err != nil {
		t.Fatalf("reopen legacy store: %v", err)
	}
	if err := db2.RotateCredential(PassphraseCredential("new-secret")); err != nil {
		t.Fatalf("rotate to passphrase: %v", err)
	}
	if err := db2.Flush(); err != nil {
		t.Fatalf("flush after rotation: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if hint := keyHint(t, path); hint.KDF != spool.KDFArgon2id {
		t.Fatalf("hint KDF = %d, want KDFArgon2id after migration", hint.KDF)
	}
	if _, err := OpenWithCredential(path, legacy, options); !errors.Is(err, ErrWrongKey) {
		t.Fatalf("legacy master key returned %v, want ErrWrongKey", err)
	}
	if _, err := OpenWithCredential(path, CredentialForSecret("legacy-secret", spool.KDFDirect), options); !errors.Is(err, ErrWrongKey) {
		t.Fatalf("legacy passphrase mapping returned %v, want ErrWrongKey", err)
	}
	db3, err := OpenWithCredential(path, PassphraseCredential("new-secret"), options)
	if err != nil {
		t.Fatalf("reopen with new passphrase: %v", err)
	}
	defer db3.Close()
	if got := readCanary(t, db3); got != "1" {
		t.Fatalf("value = %q, want %q", got, "1")
	}
}

// TestPassphraseBackupRoundTrip pins restore and verify through a
// passphrase credential.
func TestPassphraseBackupRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/db"
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	db, err := OpenWithCredential(path, PassphraseCredential("backup-secret"), options)
	if err != nil {
		t.Fatalf("create with passphrase: %v", err)
	}
	writeCanary(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	cred := PassphraseCredential("backup-secret")
	if err := VerifyBackupWithCredential(path, cred); err != nil {
		t.Fatalf("verify backup: %v", err)
	}
	if err := VerifyBackupWithCredential(path, PassphraseCredential("wrong")); !errors.Is(err, ErrWrongKey) {
		t.Fatalf("verify with wrong passphrase returned %v, want ErrWrongKey", err)
	}
	restored := dir + "/restored"
	if err := RestoreWithCredential(path, restored, cred); err != nil {
		t.Fatalf("restore: %v", err)
	}
	options.Mode = OpenExisting
	db2, err := OpenWithCredential(restored, cred, options)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer db2.Close()
	if got := readCanary(t, db2); got != "1" {
		t.Fatalf("value = %q, want %q", got, "1")
	}
}
