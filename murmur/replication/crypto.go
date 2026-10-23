package replication

import (
	"bytes"
	"crypto/ed25519"

	"golang.org/x/crypto/curve25519"
)

// PublicKey derives the Ed25519 public key of a seed.
func PublicKey(seed *[32]byte) [32]byte {
	var out [32]byte
	copy(out[:], ed25519.NewKeyFromSeed(seed[:]).Public().(ed25519.PublicKey))
	return out
}

// Sign signs bytes with an Ed25519 seed.
func Sign(seed *[32]byte, msg []byte) []byte {
	return ed25519.Sign(ed25519.NewKeyFromSeed(seed[:]), msg)
}

// Verify checks an Ed25519 signature, rejecting low-order public keys
// and signature R points like the Rust strict verifier.
func Verify(public *[32]byte, msg, signature []byte) error {
	if len(signature) != 64 || !strictPoint(public[:]) || !strictPoint(signature[:32]) {
		return authErr("invalid Ed25519 point or signature length")
	}
	if !ed25519.Verify(ed25519.PublicKey(public[:]), msg, signature) {
		return authErr("invalid Ed25519 signature")
	}
	return nil
}

// strictPoint is the encoded-point input policy: the masked y coordinate
// must be below the field prime and not one of the five low-order
// Edwards25519 y values. See RFC 8032 section 5.1.3.
func strictPoint(encoded []byte) bool {
	if len(encoded) != 32 {
		return false
	}
	var y [32]byte
	copy(y[:], encoded)
	y[31] &= 0x7f
	var prime [32]byte
	for i := range prime {
		prime[i] = 0xff
	}
	prime[0] = 0xed
	prime[31] = 0x7f
	less := false
	for i := 31; i >= 0; i-- {
		if y[i] != prime[i] {
			less = y[i] < prime[i]
			break
		}
	}
	if !less {
		return false
	}
	lowOrder := [5][32]byte{
		{},
		{1},
		{0x26, 0xe8, 0x95, 0x8f, 0xc2, 0xb2, 0x27, 0xb0, 0x45, 0xc3, 0xf4, 0x89, 0xf2, 0xef, 0x98, 0xf0, 0xd5, 0xdf, 0xac, 0x05, 0xd3, 0xc6, 0x33, 0x39, 0xb1, 0x38, 0x02, 0x88, 0x6d, 0x53, 0xfc, 0x05},
		{0xc7, 0x17, 0x6a, 0x70, 0x3d, 0x4d, 0xd8, 0x4f, 0xba, 0x3c, 0x0b, 0x76, 0x0d, 0x10, 0x67, 0x0f, 0x2a, 0x20, 0x53, 0xfa, 0x2c, 0x39, 0xcc, 0xc6, 0x4e, 0xc7, 0xfd, 0x77, 0x92, 0xac, 0x03, 0x7a},
		{0xec, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
	}
	return y != lowOrder[0] && y != lowOrder[1] && y != lowOrder[2] && y != lowOrder[3] && y != lowOrder[4]
}

// RecipientPublic derives the X25519 public key of a seed.
func RecipientPublic(seed *[32]byte) [32]byte {
	var out [32]byte
	pub, err := curve25519.X25519(seed[:], curve25519.Basepoint)
	if err != nil {
		panic("replication: X25519 basepoint: " + err.Error())
	}
	copy(out[:], pub)
	return out
}

// Agree computes the X25519 shared secret, rejecting weak peer keys.
func Agree(seed, peer *[32]byte) ([32]byte, error) {
	var out [32]byte
	secret, err := curve25519.X25519(seed[:], peer[:])
	if err != nil {
		return out, authErr("invalid X25519 peer key")
	}
	var zero [32]byte
	if len(secret) != 32 || bytes.Equal(secret, zero[:]) {
		return out, authErr("invalid X25519 peer key")
	}
	copy(out[:], secret)
	return out, nil
}
