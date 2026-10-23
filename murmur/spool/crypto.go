package spool

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"golang.org/x/crypto/argon2"
)

// Wire ids for the block AEAD. Id 1 is retired (it named a
// non-standard-library cipher in pre-release stores) and fails
// closed as unknown.
const (
	algoNone   = 0
	algoAESGCM = 2
)

// KDF ids for keys.enc.
const (
	kdfDirectHKDF = 0
	kdfArgon2id   = 1
)

// Argon2id parameters for passphrase-derived keys.enc protectors.
const (
	argonTime    = 3
	argonMemory  = 64 * 1024 // KiB
	argonThreads = 4
	argonKeyLen  = 32
)

// keysEncInfo is the HKDF info prefix binding the keys.enc
// protector to this exact use, store, and database context. The
// full info is prefix || storeID || contextID.
const keysEncInfo = "spool/keys-enc/v3/"

// blockCrypt seals and opens block payloads. In algoNone mode the
// "seal" appends a CRC32-Castagnoli instead of an AEAD tag.
type blockCrypt struct {
	algo uint8
}

func blockCryptFor(e Encryption) (blockCrypt, error) {
	switch e {
	case EncryptionNone:
		return blockCrypt{algo: algoNone}, nil
	case EncryptionAES256GCM, EncryptionDefault:
		return blockCrypt{algo: algoAESGCM}, nil
	default:
		return blockCrypt{}, fmt.Errorf("spool: unknown encryption %d", int(e))
	}
}

// nonceSize reports the nonce length for the algorithm.
func (b blockCrypt) nonceSize() int {
	if b.algo == algoAESGCM {
		return 12
	}
	return 0
}

// overhead reports authentication bytes added by seal.
func (b blockCrypt) overhead() int {
	if b.algo == algoAESGCM {
		return 16
	}
	return blockCRCSize
}

// aesGCM builds the standard-library AES-256-GCM AEAD shared by
// blocks and the key container.
func aesGCM(key [32]byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func (b blockCrypt) aead(key [32]byte) (cipher.AEAD, error) {
	if b.algo == algoAESGCM {
		return aesGCM(key)
	}
	return nil, fmt.Errorf("spool: no AEAD for algorithm %d", b.algo)
}

// seal encrypts plain with key and nonce, authenticating aad. Every
// call must use a fresh nonce.
func (b blockCrypt) seal(key [32]byte, nonce, plain, aad []byte) ([]byte, error) {
	if b.algo == algoNone {
		out := make([]byte, 0, len(plain)+blockCRCSize)
		out = append(out, plain...)
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], crc32.Checksum(plain, castagnoli))
		return append(out, tmp[:]...), nil
	}
	a, err := b.aead(key)
	if err != nil {
		return nil, fmt.Errorf("spool: seal cipher: %w", err)
	}
	return a.Seal(nil, nonce, plain, aad), nil
}

// open authenticates and decrypts sealed.
func (b blockCrypt) open(key [32]byte, nonce, sealed, aad []byte) ([]byte, error) {
	if b.algo == algoNone {
		if len(sealed) < blockCRCSize {
			return nil, fmt.Errorf("spool: truncated plain payload: %w", ErrCorrupt)
		}
		body, tag := sealed[:len(sealed)-blockCRCSize], sealed[len(sealed)-blockCRCSize:]
		if got, want := crc32.Checksum(body, castagnoli), binary.LittleEndian.Uint32(tag); got != want {
			return nil, fmt.Errorf("spool: payload checksum mismatch: %w", ErrCorrupt)
		}
		return body, nil
	}
	a, err := b.aead(key)
	if err != nil {
		return nil, fmt.Errorf("spool: open cipher: %w", err)
	}
	plain, err := a.Open(nil, nonce, sealed, aad)
	if err != nil {
		return nil, fmt.Errorf("spool: block authentication failed: %w", ErrAuthFailed)
	}
	return plain, nil
}

// newNonce draws a fresh random nonce of the algorithm's size.
func (b blockCrypt) newNonce() ([]byte, error) {
	n := b.nonceSize()
	if n == 0 {
		return nil, nil
	}
	out := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, out); err != nil {
		return nil, fmt.Errorf("spool: nonce entropy: %w", err)
	}
	return out, nil
}

// deriveProtector derives the 256-bit keys.enc protector using the
// container's KDF. Exactly one of masterKey and passphrase must be
// set, matching the KDF id. Both KDFs finish through HKDF with an
// info string binding the store id and database context, so a
// protector from another store or context cannot unlock this
// container.
func deriveProtector(kdf uint8, masterKey []byte, passphrase string, salt []byte, time, memory uint32, threads uint8, storeID, contextID [16]byte) ([32]byte, error) {
	var out [32]byte
	info := make([]byte, 0, len(keysEncInfo)+32)
	info = append(info, keysEncInfo...)
	info = append(info, storeID[:]...)
	info = append(info, contextID[:]...)
	switch kdf {
	case kdfDirectHKDF:
		if len(masterKey) == 0 {
			return out, fmt.Errorf("spool: keys.enc needs a master key: %w", ErrWrongKey)
		}
		dk, err := hkdf.Key(sha256.New, masterKey, salt, string(info), 32)
		if err != nil {
			return out, fmt.Errorf("spool: hkdf: %w", err)
		}
		copy(out[:], dk)
		wipe(dk)
		return out, nil
	case kdfArgon2id:
		if passphrase == "" {
			return out, fmt.Errorf("spool: keys.enc needs a passphrase: %w", ErrWrongKey)
		}
		if threads == 0 || time == 0 || memory == 0 {
			return out, fmt.Errorf("spool: bad argon2 parameters: %w", ErrCorrupt)
		}
		stretched := argon2.IDKey([]byte(passphrase), salt, time, memory, threads, argonKeyLen)
		dk, err := hkdf.Key(sha256.New, stretched, salt, string(info), 32)
		wipe(stretched)
		if err != nil {
			return out, fmt.Errorf("spool: hkdf: %w", err)
		}
		copy(out[:], dk)
		wipe(dk)
		return out, nil
	default:
		return out, fmt.Errorf("spool: unknown keys.enc kdf %d: %w", kdf, ErrCorrupt)
	}
}

// blockAAD binds a sealed block to its store and database context:
// the 59 header bytes covered by the header CRC (with a zero
// sealed-length field, matching seal time), the 16-byte store id,
// and the 16-byte database context. Callers pass the header bytes
// with the sealed-length field already zeroed.
func blockAAD(hdr59, storeID, contextID []byte) []byte {
	aad := make([]byte, 0, 59+16+16)
	aad = append(aad, hdr59...)
	aad = append(aad, storeID...)
	return append(aad, contextID...)
}

// keysAAD binds the keys.enc envelope to its store: the full header
// plus the 16-byte store id. The database context needs no separate
// AAD slot here: it travels inside the authenticated body and binds
// the KDF info as well.
func keysAAD(hdr, storeID []byte) []byte {
	aad := make([]byte, 0, len(hdr)+16)
	aad = append(aad, hdr...)
	return append(aad, storeID...)
}

// wipe zeroes sensitive buffers best-effort. Callers retain ownership
// of their own slices; this covers derived copies.
func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
