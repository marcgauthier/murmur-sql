package server

import (
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/auth"
	"github.com/marcgauthier/murmur-sql/config"
)

// TestUnlockSecretClassification pins that UNLOCK routes non-hex
// secrets to the engine passphrase field and leaves the raw key zero.
func TestUnlockSecretClassification(t *testing.T) {
	f := &fixtureEngine{}
	_, addr, _ := startFixture(t, config.DefaultConfig(), f)
	w := mustConnect(t, addr)
	if ms := w.query(t, "MURMUR UNLOCK KEY 'fixture-secret'"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.passphrase != "fixture-secret" {
		t.Fatalf("passphrase = %q, want fixture-secret", f.passphrase)
	}
	if f.key != [32]byte{} {
		t.Fatalf("key = %x, want zero", f.key)
	}
}

// TestUnlockRawKeyClassification pins that a 64-hex UNLOCK secret
// routes to the raw engine key with an empty passphrase.
func TestUnlockRawKeyClassification(t *testing.T) {
	raw := strings.Repeat("ab", 32)
	f := &fixtureEngine{}
	_, addr, _ := startFixture(t, config.DefaultConfig(), f)
	w := mustConnect(t, addr)
	if ms := w.query(t, "MURMUR UNLOCK KEY '"+raw+"'"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.passphrase != "" {
		t.Fatalf("passphrase = %q, want empty", f.passphrase)
	}
	if want := auth.ResolveKey(raw); f.key != want {
		t.Fatalf("key = %x, want %x", f.key, want)
	}
}
