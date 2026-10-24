package murmur

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// Credential supplies store unlock material: exactly one of Key or
// Passphrase must be set.
//
// Key is a 32-byte master key protecting the keys.enc envelope
// directly. Passphrase is stretched with Argon2id (parameters stored
// in the envelope) into the envelope protector. Passphrase strings
// are immutable: callers must drop references after use, while Key
// bytes are wiped by the paths that consume them.
type Credential struct {
	Key        []byte
	Passphrase string
}

// KeyCredential returns a master-key credential. The caller retains
// ownership of key; open copies what it keeps.
func KeyCredential(key []byte) Credential { return Credential{Key: key} }

// PassphraseCredential returns a passphrase credential.
func PassphraseCredential(passphrase string) Credential { return Credential{Passphrase: passphrase} }

func (c Credential) validate() error {
	if (len(c.Key) > 0) == (c.Passphrase != "") {
		return invalidErr("set exactly one of key and passphrase")
	}
	if len(c.Key) > 0 && len(c.Key) != 32 {
		return invalidErr("application key must be 32 bytes")
	}
	return nil
}

// isRawKey mirrors auth.IsRawKey: exactly 64 hexadecimal characters
// is a raw 32-byte key, every other string is a passphrase. Keep the
// two in sync.
func isRawKey(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// CredentialForSecret maps a user-supplied unlock secret to a store
// credential for the given envelope KDF (spool.KDFDirect or
// spool.KDFArgon2id from spool.ReadKeyHint).
//
// A 64-hex secret is always a raw master key, whatever the KDF:
// presenting one to an Argon2id store fails closed with ErrWrongKey.
// Other secrets are passphrases: Argon2id for KDF-1 envelopes,
// legacy SHA-256-derived master keys for KDF-0 envelopes created
// before passphrase-KDF support. Unknown KDF ids take the legacy
// mapping so unexpected envelopes fail in open with the store's own
// error, not here.
func CredentialForSecret(secret string, kdf uint8) Credential {
	if isRawKey(secret) {
		raw, _ := hex.DecodeString(secret)
		return Credential{Key: raw}
	}
	if kdf == spool.KDFArgon2id {
		return Credential{Passphrase: secret}
	}
	sum := sha256.Sum256([]byte(secret))
	return Credential{Key: sum[:]}
}
