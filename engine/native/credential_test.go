package native

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/marcgauthier/murmur-sql/config"
	"github.com/marcgauthier/murmur-sql/engine"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// TestPassphraseOpenUnlockRotate pins the passphrase path through the
// native engine: creation selects Argon2id, unlock verifies the
// passphrase, rotation migrates to the new passphrase, and afterwards
// only the new passphrase opens the store.
func TestPassphraseOpenUnlockRotate(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir + "/data"
	ctx := context.Background()
	db, outcome, err := Factory{}.Open(ctx, engine.OpenRequest{Config: cfg, Passphrase: "open-sesame"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if outcome != engine.OpenedNew {
		t.Fatalf("outcome = %d, want OpenedNew", outcome)
	}
	hint, err := spool.ReadKeyHint(cfg.Storage.DataDir)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if hint.KDF != spool.KDFArgon2id {
		t.Fatalf("hint KDF = %d, want KDFArgon2id", hint.KDF)
	}
	var zero [32]byte
	if err := db.Unlock(ctx, zero, "open-sesame"); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if err := db.Unlock(ctx, zero, "wrong"); err == nil {
		t.Fatal("wrong passphrase unlocked")
	}
	var raw [32]byte
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	if err := db.Unlock(ctx, raw, ""); err == nil {
		t.Fatal("raw key unlocked a passphrase store")
	}
	if _, err := db.Command(ctx, 1, engine.Command{Operation: "ROTATE KEY", Arguments: []engine.Value{"open-sesame", "new-secret"}}); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if err := db.Unlock(ctx, zero, "new-secret"); err != nil {
		t.Fatalf("unlock after rotation: %v", err)
	}
	if err := db.Unlock(ctx, zero, "open-sesame"); err == nil {
		t.Fatal("retired passphrase unlocked after rotation")
	}
	if err := db.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, _, err := Factory{}.Open(ctx, engine.OpenRequest{Config: cfg, Passphrase: "new-secret"})
	if err != nil {
		t.Fatalf("reopen with new passphrase: %v", err)
	}
	defer db2.Close(ctx)
	if _, _, err := (Factory{}).Open(ctx, engine.OpenRequest{Config: cfg, Passphrase: "open-sesame"}); err == nil {
		t.Fatal("retired passphrase reopened the store")
	}
}

// TestLegacyPassphraseOpen pins that a KDF-0 store created under a raw
// legacy SHA-256 master key still opens through the passphrase form.
func TestLegacyPassphraseOpen(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir + "/data"
	ctx := context.Background()
	legacy := sha256.Sum256([]byte("legacy-secret"))
	db, _, err := Factory{}.Open(ctx, engine.OpenRequest{Config: cfg, Key: legacy})
	if err != nil {
		t.Fatalf("open with raw key: %v", err)
	}
	if err := db.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, outcome, err := Factory{}.Open(ctx, engine.OpenRequest{Config: cfg, Passphrase: "legacy-secret"})
	if err != nil {
		t.Fatalf("open with legacy passphrase: %v", err)
	}
	defer db2.Close(ctx)
	if outcome != engine.OpenedExisting {
		t.Fatalf("outcome = %d, want OpenedExisting", outcome)
	}
	var zero [32]byte
	if err := db2.Unlock(ctx, zero, "legacy-secret"); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestCredentialEqual(t *testing.T) {
	rawA := sha256.Sum256([]byte("a"))
	rawB := sha256.Sum256([]byte("b"))
	cases := []struct {
		name               string
		oldKey, newKey     [32]byte
		oldPass, newPass   string
		want               bool
	}{
		{"same raw", rawA, rawA, "", "", true},
		{"different raw", rawA, rawB, "", "", false},
		{"same passphrase", [32]byte{}, [32]byte{}, "a", "a", true},
		{"different passphrase", [32]byte{}, [32]byte{}, "a", "b", false},
		{"legacy no-op", [32]byte{}, rawA, "a", "", true},
		{"passphrase target always differs", rawA, [32]byte{}, "", "b", false},
		{"raw target differs", rawA, rawB, "a", "", false},
	}
	for _, c := range cases {
		if got := credentialEqual(c.oldKey, c.newKey, c.oldPass, c.newPass); got != c.want {
			t.Fatalf("%s: credentialEqual = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestSplitCredential(t *testing.T) {
	var key [32]byte
	var pass string
	var want [32]byte
	for i := range want {
		want[i] = 0xab
	}
	if err := splitCredential(append([]byte{}, want[:]...), &key, &pass); err != nil || key != want || pass != "" {
		t.Fatalf("raw split = %x %q %v", key, pass, err)
	}
	key = [32]byte{}
	if err := splitCredential("secret", &key, &pass); err != nil || key != [32]byte{} || pass != "secret" {
		t.Fatalf("passphrase split = %x %q %v", key, pass, err)
	}
	for _, v := range []engine.Value{[]byte{1, 2, 3}, "", int64(7)} {
		if err := splitCredential(v, &key, &pass); err == nil {
			t.Fatalf("split(%T) accepted", v)
		}
	}
}
