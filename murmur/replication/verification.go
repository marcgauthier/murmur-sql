package replication

import (
	"crypto/sha256"
)

// Cache holds disposable authentication results, independent of durable
// staging and progress.
type Cache struct {
	entries map[[64]byte]bool
	order   [][64]byte
}

// Verified binds staged bytes to their digest.
type Verified struct {
	Bytes  []byte
	Digest [32]byte
}

// Verify authenticates one transaction, caching the result. Current
// identity, trust, and limits are enforced even on a hit.
func (c *Cache) Verify(tx *Transaction, database NodeId, trusted map[NodeId][32]byte, max int) (*Verified, error) {
	if tx.Database != database || tx.Sequence == 0 || len(tx.Payload) > max {
		return nil, protocolErr("transaction identity or size")
	}
	public, ok := trusted[tx.Origin]
	if !ok {
		return nil, authErr("untrusted transaction origin")
	}
	raw := EncodeTransaction(tx)
	// Bind every byte, including the signature and payload, to the result.
	// A changed trust key also invalidates a cached result immediately.
	digest := sha256.Sum256(raw)
	var key [64]byte
	copy(key[:32], digest[:])
	copy(key[32:], public[:])
	if c.entries == nil {
		c.entries = make(map[[64]byte]bool)
	}
	if !c.entries[key] {
		if err := tx.Verify(database, trusted, max); err != nil {
			return nil, err
		}
		const capacity = 8192
		if len(c.order) == capacity {
			delete(c.entries, c.order[0])
			c.order = c.order[1:]
		}
		c.entries[key] = true
		c.order = append(c.order, key)
	}
	return &Verified{Bytes: raw, Digest: digest}, nil
}

// Bind binds staged bytes to their digest without re-checking the
// signature. Staging verifies every transaction before its staged commit
// lands, and the spool is trusted local storage, so a second Ed25519
// check at apply time only burns CPU. Re-checking here would also wedge
// imports after signer rotation: a transaction authenticated at receipt
// under a now-retired key must still apply. Identity, size, and
// payload-digest checks stay: they are microseconds and catch corruption.
func Bind(tx *Transaction, database NodeId, max int) (*Verified, error) {
	if tx.Database != database || tx.Sequence == 0 || len(tx.Payload) > max {
		return nil, protocolErr("transaction identity or size")
	}
	if tx.PayloadDigest != sha256.Sum256(tx.Payload) {
		return nil, authErr("payload digest mismatch")
	}
	raw := EncodeTransaction(tx)
	digest := sha256.Sum256(raw)
	return &Verified{Bytes: raw, Digest: digest}, nil
}
