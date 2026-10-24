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

func TestIsRawKey(t *testing.T) {
	raw := strings.Repeat("ab", 32)
	upper := strings.Repeat("AB", 32)
	for _, s := range []string{raw, upper, strings.Repeat("09", 32)} {
		if !IsRawKey(s) {
			t.Fatalf("IsRawKey(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"", "secret", strings.Repeat("a", 63), strings.Repeat("a", 65), strings.Repeat("zz", 32), strings.Repeat("ab", 31) + "zz", " " + raw[:63]} {
		if IsRawKey(s) {
			t.Fatalf("IsRawKey(%q) = true, want false", s)
		}
	}
}
