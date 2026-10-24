package native

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/config"
	"github.com/marcgauthier/murmur-sql/engine"
	"github.com/marcgauthier/murmur-sql/murmur"
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
		name             string
		oldKey, newKey   [32]byte
		oldPass, newPass string
		want             bool
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

func TestResolveSecret(t *testing.T) {
	hexed := strings.Repeat("ab", 32)
	str := func(s string) *string { return &s }
	if got, err := resolveSecret(str(hexed), nil, nil); err != nil || got[0] != 0xab {
		t.Fatalf("hex = %x %v", got, err)
	}
	dir := t.TempDir()
	rawPath := dir + "/raw.key"
	if err := os.WriteFile(rawPath, bytes.Repeat([]byte{0xcd}, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	hexPath := dir + "/hex.key"
	if err := os.WriteFile(hexPath, []byte(strings.Repeat("ef", 32)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := resolveSecret(nil, str(rawPath), nil); err != nil || got[0] != 0xcd {
		t.Fatalf("raw file = %x %v", got, err)
	}
	if got, err := resolveSecret(nil, str(hexPath), nil); err != nil || got[0] != 0xef {
		t.Fatalf("hex file = %x %v", got, err)
	}
	if _, err := resolveSecret(nil, str(dir+"/missing"), nil); err == nil {
		t.Fatal("missing file accepted")
	}
	t.Setenv("MURMUR_TEST_NATIVE_SECRET", strings.Repeat("12", 32))
	if got, err := resolveSecret(nil, nil, str("env:MURMUR_TEST_NATIVE_SECRET")); err != nil || got[0] != 0x12 {
		t.Fatalf("env uri = %x %v", got, err)
	}
	if _, err := resolveSecret(nil, nil, str("bogus:x")); err == nil {
		t.Fatal("bad uri accepted")
	}
	if _, err := resolveSecret(nil, nil, nil); err == nil {
		t.Fatal("empty sources accepted")
	}
	if _, err := resolveSecret(str(hexed), str(rawPath), nil); err == nil {
		t.Fatal("dual sources accepted")
	}
	if hexRef("") != nil {
		t.Fatal("hexRef empty accepted")
	}
	if got := hexRef(hexed); got == nil || *got != hexed {
		t.Fatalf("hexRef = %v", got)
	}
}

// TestAdminProviderOpen pins offline administration against a
// provider-resolved store key: a file URI opens the store, and a bad
// URI fails before any open is attempted.
func TestAdminProviderOpen(t *testing.T) {
	dir := t.TempDir()
	var key [32]byte
	for i := range key {
		key[i] = byte(0x40 + i)
	}
	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenCreateNew
	db, err := murmur.OpenWithCredential(dir+"/data", murmur.KeyCredential(key[:]), options)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	keyPath := dir + "/store.key"
	if err := os.WriteFile(keyPath, key[:], 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	result, err := Factory{}.Admin(ctx, engine.AdminRequest{Operation: "keys", DataDir: dir + "/data", KeyProviderURI: "file:" + keyPath})
	if err != nil {
		t.Fatalf("admin keys: %v", err)
	}
	report, ok := result.(adminReport)
	if !ok || report.values["encrypted"] != true {
		t.Fatalf("report = %#v", result)
	}
	if _, err := (Factory{}).Admin(ctx, engine.AdminRequest{Operation: "keys", DataDir: dir + "/data", KeyProviderURI: "bogus:x"}); err == nil {
		t.Fatal("bad provider uri accepted")
	}
}
