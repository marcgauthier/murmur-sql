// Package auth implements Rust-compatible Argon2 PHC password verification and
// SHA-256 database passphrase derivation. It contains no storage cryptography.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

const DummyHash = "$argon2id$v=19$m=19456,t=2,p=1$RjLO/Gm4xHJYqxTHnHPWAw$kDfb0vmRLziIILEYgJ7IXsy2PQ6u0dCzBn0y42PPZIE"

func HashPassword(password []byte) (string, error) {
	salt := make([]byte, 16)
	if _, e := rand.Read(salt); e != nil {
		return "", e
	}
	h := argon2.IDKey(password, salt, 2, 19456, 1, 32)
	return fmt.Sprintf("$argon2id$v=19$m=19456,t=2,p=1$%s$%s", base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(h)), nil
}
func VerifyPassword(phc string, password []byte) bool {
	parts := strings.Split(phc, "$")
	if len(parts) != 6 || (parts[1] != "argon2id" && parts[1] != "argon2i") || parts[2] != "v=19" {
		return false
	}
	vals := map[string]uint64{}
	for _, p := range strings.Split(parts[3], ",") {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			return false
		}
		if _, ok := vals[kv[0]]; ok {
			return false
		}
		n, e := strconv.ParseUint(kv[1], 10, 32)
		if e != nil {
			return false
		}
		vals[kv[0]] = n
	}
	m, t, p := vals["m"], vals["t"], vals["p"]
	if len(vals) != 3 || p == 0 || p > 32 || m < 8*p || m > 256*1024 || t == 0 || t > 16 {
		return false
	}
	salt, e := base64.RawStdEncoding.Strict().DecodeString(parts[4])
	if e != nil || len(salt) < 8 || len(salt) > 1024 {
		return false
	}
	want, e := base64.RawStdEncoding.Strict().DecodeString(parts[5])
	if e != nil || len(want) < 4 || len(want) > 1024 {
		return false
	}
	var got []byte
	if parts[1] == "argon2id" {
		got = argon2.IDKey(password, salt, uint32(t), uint32(m), uint8(p), uint32(len(want)))
	} else {
		got = argon2.Key(password, salt, uint32(t), uint32(m), uint8(p), uint32(len(want)))
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}

// IsRawKey reports whether s is a raw 32-byte key: exactly 64 hexadecimal
// characters. Every other string is a passphrase. The rule is shared by
// SQL unlock secrets and key-hex options; murmur mirrors it for credential
// selection, so keep the two in sync.
func IsRawKey(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, e := hex.DecodeString(s)
	return e == nil
}

// ResolveKey matches MURMUR UNLOCK: exactly 64 hex characters is a raw key;
// all other strings are SHA-256 passphrases.
func ResolveKey(s string) [32]byte {
	if IsRawKey(s) {
		b, _ := hex.DecodeString(s)
		var k [32]byte
		copy(k[:], b)
		return k
	}
	return sha256.Sum256([]byte(s))
}

// ResolvePassphrase always hashes, matching the admin --passphrase flag even
// when the passphrase happens to contain exactly 64 hex characters.
func ResolvePassphrase(s string) [32]byte { return sha256.Sum256([]byte(s)) }
