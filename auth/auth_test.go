package auth

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestRustPHCCompatibility(t *testing.T) {
	if !VerifyPassword(DummyHash, []byte("murmur-dummy-unusable-login-value")) {
		t.Fatal("Rust default PHC rejected")
	}
	if VerifyPassword(DummyHash, []byte("wrong")) {
		t.Fatal("wrong password accepted")
	}
	h, e := HashPassword([]byte("correct horse"))
	if e != nil {
		t.Fatal(e)
	}
	if !VerifyPassword(h, []byte("correct horse")) || VerifyPassword(h, []byte("wrong")) {
		t.Fatal("password roundtrip")
	}
	for _, h := range []string{"", "not-phc", strings.Replace(DummyHash, "m=19456", "m=4294967295", 1), strings.Replace(DummyHash, "p=1", "p=0", 1), strings.Replace(DummyHash, "t=2", "t=0", 1)} {
		if VerifyPassword(h, []byte("wrong")) {
			t.Fatal("accepted malformed hash")
		}
	}
}
func TestKeyCompatibility(t *testing.T) {
	known := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	sum := ResolveKey("")
	if hex.EncodeToString(sum[:]) != known {
		t.Fatal("SHA-256 reference vector mismatch")
	}
	raw := strings.Repeat("ab", 32)
	sum = ResolveKey(raw)
	if hex.EncodeToString(sum[:]) != raw {
		t.Fatal("raw key was hashed")
	}
	if ResolvePassphrase(raw) == sum {
		t.Fatal("admin passphrase accidentally treated as hex")
	}
}
