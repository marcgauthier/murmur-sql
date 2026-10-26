package recovery

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Cipher seals and opens recovery payloads. With a 32-byte key it uses
// AES-256-GCM (unique random nonce per seal); with no key it appends a
// CRC32-Castagnoli integrity tag instead, mirroring spool's algoNone
// mode for unencrypted databases. A Cipher is safe for concurrent use.
type Cipher struct {
	aead      cipher.AEAD
	encrypted bool
}

// NewCipher builds a Cipher. An empty key selects integrity-only plain
// mode; a 32-byte key selects AES-256-GCM. Anything else fails.
func NewCipher(key []byte) (*Cipher, error) {
	if len(key) == 0 {
		return &Cipher{}, nil
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("recovery: key must be empty or 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("recovery: cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("recovery: gcm: %w", err)
	}
	return &Cipher{aead: aead, encrypted: true}, nil
}

// Encrypted reports whether the cipher seals with AES-GCM.
func (c *Cipher) Encrypted() bool { return c != nil && c.encrypted }

// Seal protects plain, authenticating aad. It returns a fresh random
// nonce plus the sealed payload (ciphertext+tag, or payload+CRC32-C in
// plain mode). Callers must never reuse a (key, nonce) pair; Seal
// always samples a new random nonce.
func (c *Cipher) Seal(plain, aad []byte) (nonce, sealed []byte, err error) {
	if c == nil || !c.encrypted {
		out := make([]byte, 0, len(plain)+CRCTagLen)
		out = append(out, plain...)
		var tag [CRCTagLen]byte
		binary.LittleEndian.PutUint32(tag[:], crc32.Checksum(plain, castagnoli))
		return make([]byte, NonceLen), append(out, tag[:]...), nil
	}
	nonce = make([]byte, NonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, fmt.Errorf("recovery: nonce: %w", err)
	}
	return nonce, c.aead.Seal(nil, nonce, plain, aad), nil
}

// Open verifies and decrypts sealed (produced by Seal) against aad.
func (c *Cipher) Open(nonce, sealed, aad []byte) ([]byte, error) {
	if c == nil || !c.encrypted {
		if len(nonce) != NonceLen {
			return nil, corruptErr("bad plain nonce length %d", len(nonce))
		}
		if len(sealed) < CRCTagLen {
			return nil, corruptErr("truncated plain payload")
		}
		body := sealed[:len(sealed)-CRCTagLen]
		want := binary.LittleEndian.Uint32(sealed[len(sealed)-CRCTagLen:])
		if got := crc32.Checksum(body, castagnoli); got != want {
			return nil, corruptErr("payload checksum mismatch")
		}
		return append([]byte(nil), body...), nil
	}
	if len(nonce) != NonceLen {
		return nil, corruptErr("bad nonce length %d", len(nonce))
	}
	plain, err := c.aead.Open(nil, nonce, sealed, aad)
	if err != nil {
		return nil, corruptErr("authentication failed: %v", err)
	}
	return plain, nil
}

// Overhead reports the bytes Seal adds beyond the plaintext.
func (c *Cipher) Overhead() int {
	if c == nil || !c.encrypted {
		return CRCTagLen
	}
	return GCMTagLen
}

// BlockAAD binds one data block to its exact database, generation,
// schema, file, and position. Tampering with any bound value breaks
// authentication.
func BlockAAD(dbID [16]byte, generation uint64, schemaDigest [32]byte, file string, block uint32) []byte {
	aad := make([]byte, 0, 64+len(file))
	aad = append(aad, "MRC1/block/"...)
	aad = append(aad, dbID[:]...)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], generation)
	aad = append(aad, tmp[:]...)
	aad = append(aad, schemaDigest[:]...)
	aad = appendBytes(aad, []byte(file))
	var b4 [4]byte
	binary.LittleEndian.PutUint32(b4[:], block)
	return append(aad, b4[:]...)
}

// ManifestAAD binds a sealed manifest to its database and format.
func ManifestAAD(dbID [16]byte) []byte {
	aad := make([]byte, 0, 32)
	aad = append(aad, "MRC1/manifest/"...)
	aad = append(aad, dbID[:]...)
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], FormatVersion)
	return append(aad, tmp[:]...)
}
