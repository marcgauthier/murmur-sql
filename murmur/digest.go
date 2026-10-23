package murmur

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

// SHA-256 for managed persistence, logical keys and stable schema identities.

// hash returns the SHA-256 digest of bytes.
func hash(bytes []byte) [32]byte {
	return sha256.Sum256(bytes)
}

// hex lowercases bytes into hexadecimal text.
func hexEncode(bytes []byte) string {
	var b strings.Builder
	b.Grow(len(bytes) * 2)
	for _, c := range bytes {
		fmt.Fprintf(&b, "%02x", c)
	}
	return b.String()
}

// hashHex returns the hex SHA-256 digest of bytes.
func hashHex(bytes []byte) string {
	sum := hash(bytes)
	return hexEncode(sum[:])
}

// id derives a stable identity string from a seed.
func id(seed []byte) string {
	return hashHex(seed)
}

// quote renders an identifier for SQL: double quotes with "" escapes.
func quote(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
