package spool

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// keys.enc container v3 (all integers little-endian).
//
// Header (variable, 61+H bytes):
//
//	off    size  field
//	0      4     magic "SPLK"
//	4      2     format version (3)
//	6      1     KDF id (0 = master-key HKDF, 1 = argon2id)
//	7      8     sequence (bumped per persist, informational)
//	15     32    salt
//	47     4     argon2 time (direct mode: zero)
//	51     4     argon2 memory KiB (direct mode: zero)
//	55     1     argon2 threads (direct mode: zero)
//	56     1     wrapping-key id hint length H (<= 128)
//	57     H     wrapping-key id hint (lookup aid only, not authenticated identity)
//	57+H   4     CRC32-Castagnoli over bytes [0,57+H)
//
// Followed by: nonce[12] | sealedLen u32 | sealed.
//
// The sealed body (AES-256-GCM, AAD = header || store id):
//
//	count u32
//	database context id [16]
//	wrapping-key id length u8 + id bytes (authenticated value)
//	repeated entry: id u32 | createdAt i64 (unix nanos) | status u8 | key[32]
//
// The header hint lets a key provider select wrapping material
// before decryption; it is only a hint until the envelope
// authenticates. The body values are authoritative: the context
// also binds the KDF, so an envelope from another store or context
// fails authentication.
//
// Data keys are append-only across rotation; historical keys are
// dropped only by explicit pruning once nothing references them, so
// old blocks stay readable.
//
// Versions 1-2 are rejected without migration; version 3 is
// AES-256-GCM throughout with store/context binding.

const (
	keysMagic   = "SPLK"
	keysVersion = 3
	keysNonce   = 12

	maxDataKeys = 1 << 16
	// maxWrapIDLen bounds the wrapping-key identifier. It is
	// metadata (a provider lookup name), never key material.
	maxWrapIDLen = 128
)

// KeyStatus marks a data key's role.
type KeyStatus uint8

const (
	// KeyStatusActive is the current encryption key for new blocks.
	KeyStatusActive KeyStatus = iota
	// KeyStatusRetired keys only decrypt old blocks.
	KeyStatusRetired
)

// DataKey is one envelope data key: 256 bits for AES-256-GCM.
// CreatedAt is unix nanoseconds.
type DataKey struct {
	ID        uint32
	Key       [32]byte
	CreatedAt int64
	Status    KeyStatus
}

// keyring holds the unlocked data keys plus the authenticated
// envelope identity: database context and wrapping-key id.
type keyring struct {
	mu      sync.RWMutex
	keys    map[uint32]*DataKey
	current uint32
	seq     uint64
	context [16]byte
	wrapID  string
	faults  *FaultHooks
}

func newKeyring() *keyring {
	return &keyring{keys: make(map[uint32]*DataKey)}
}

// currentKey returns the key id and material for new blocks.
func (r *keyring) currentKey() (uint32, [32]byte, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	k, ok := r.keys[r.current]
	if !ok {
		return 0, [32]byte{}, false
	}
	return k.ID, k.Key, true
}

// byID returns the material for a key id.
func (r *keyring) byID(id uint32) ([32]byte, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	k, ok := r.keys[id]
	if !ok {
		return [32]byte{}, false
	}
	return k.Key, true
}

// createdAt returns a key's creation time.
func (r *keyring) createdAt(id uint32) (time.Time, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	k, ok := r.keys[id]
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(0, k.CreatedAt), true
}

// rotate generates a successor key, retires the previous current key
// and returns the new current id. It serves fresh-store creation
// only; runtime rotation uses rotatePersisted so no group can ever
// reference an undurable key.
func (r *keyring) rotate(now time.Time) (uint32, error) {
	var raw [32]byte
	if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
		return 0, fmt.Errorf("spool: data key entropy: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var next uint32 = 1
	for id := range r.keys {
		if id >= next {
			next = id + 1
		}
	}
	if prev, ok := r.keys[r.current]; ok {
		prev.Status = KeyStatusRetired
	}
	r.keys[next] = &DataKey{ID: next, Key: raw, CreatedAt: now.UnixNano(), Status: KeyStatusActive}
	r.current = next
	return next, nil
}

// rotatePersisted generates a successor key, persists it, and only
// then installs it as current. The whole operation holds the write
// lock, so a concurrent committer either seals with the previous
// durable key or blocks until the successor is durable. Nothing
// observable changes on persist failure.
func (r *keyring) rotatePersisted(now time.Time, dir string, storeID [16]byte, masterKey []byte, passphrase string) (uint32, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var raw [32]byte
	if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
		return 0, fmt.Errorf("spool: data key entropy: %w", err)
	}
	var next uint32 = 1
	for id := range r.keys {
		if id >= next {
			next = id + 1
		}
	}
	staged := &DataKey{ID: next, Key: raw, CreatedAt: now.UnixNano(), Status: KeyStatusActive}
	if err := r.writeKeysLocked(dir, storeID, masterKey, passphrase, staged); err != nil {
		wipe(raw[:])
		return 0, err
	}
	if prev, ok := r.keys[r.current]; ok {
		prev.Status = KeyStatusRetired
	}
	r.keys[next] = staged
	r.current = next
	return next, nil
}

// keysFileName is the key container leaf name.
const keysFileName = "keys.enc"

// writeKeys persists the ring atomically: temp file, fsync, rename,
// directory fsync. The envelope binds the store id (AAD) and the
// ring's database context (sealed body and KDF). It takes the write
// lock: every envelope persist bumps the sequence.
func (r *keyring) writeKeys(dir string, storeID [16]byte, masterKey []byte, passphrase string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writeKeysLocked(dir, storeID, masterKey, passphrase, nil)
}

// writeKeysLocked implements writeKeys with an optional staged
// successor included as active. The caller must hold the write
// lock; the persist counter bumps on every call.
func (r *keyring) writeKeysLocked(dir string, storeID [16]byte, masterKey []byte, passphrase string, staged *DataKey) error {
	if err := r.faults.trip("keyring"); err != nil {
		return fmt.Errorf("spool: injected keyring fault: %w", err)
	}
	r.seq++
	keys := make([]*DataKey, 0, len(r.keys)+1)
	for _, k := range r.keys {
		keys = append(keys, k)
	}
	if staged != nil {
		if _, dup := r.keys[staged.ID]; dup {
			return fmt.Errorf("spool: staged key id %d collides: %w", staged.ID, ErrCorrupt)
		}
		keys = append(keys, staged)
	}
	seq := r.seq
	contextID := r.context
	wrapID := r.wrapID

	if len(wrapID) > maxWrapIDLen {
		return fmt.Errorf("spool: wrapping-key id of %d bytes exceeds %d", len(wrapID), maxWrapIDLen)
	}

	// Deterministic order for stable bytes.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j-1].ID > keys[j].ID; j-- {
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}

	var kdf uint8 = kdfDirectHKDF
	var aTime, aMem uint32
	var aThreads uint8
	if passphrase != "" {
		kdf = kdfArgon2id
		aTime, aMem, aThreads = argonTime, argonMemory, argonThreads
	}
	var salt [32]byte
	if _, err := io.ReadFull(rand.Reader, salt[:]); err != nil {
		return fmt.Errorf("spool: salt entropy: %w", err)
	}
	protector, err := deriveProtector(kdf, masterKey, passphrase, salt[:], aTime, aMem, aThreads, storeID, contextID)
	if err != nil {
		return err
	}
	defer wipe(protector[:])

	body := make([]byte, 0, 4+16+1+len(wrapID)+len(keys)*(4+8+1+32))
	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(keys)))
	body = append(body, tmp[:4]...)
	body = append(body, contextID[:]...)
	body = append(body, byte(len(wrapID)))
	body = append(body, wrapID...)
	for _, k := range keys {
		binary.LittleEndian.PutUint32(tmp[:4], k.ID)
		body = append(body, tmp[:4]...)
		binary.LittleEndian.PutUint64(tmp[:], uint64(k.CreatedAt))
		body = append(body, tmp[:]...)
		body = append(body, byte(k.Status))
		body = append(body, k.Key[:]...)
	}

	hdr := make([]byte, 0, 61+len(wrapID))
	hdr = append(hdr, keysMagic...)
	hdr = append(hdr, 0, 0)
	binary.LittleEndian.PutUint16(hdr[4:6], keysVersion)
	hdr = append(hdr, kdf)
	hdr = append(hdr, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint64(hdr[7:15], seq)
	hdr = append(hdr, salt[:]...)
	hdr = append(hdr, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(hdr[47:51], aTime)
	hdr = append(hdr, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(hdr[51:55], aMem)
	hdr = append(hdr, aThreads)
	hdr = append(hdr, byte(len(wrapID)))
	hdr = append(hdr, wrapID...)
	hdr = append(hdr, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(hdr[len(hdr)-4:], crc32.Checksum(hdr[:len(hdr)-4], castagnoli))

	var nonce [keysNonce]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return fmt.Errorf("spool: nonce entropy: %w", err)
	}
	aead, err := aesGCM(protector)
	if err != nil {
		return fmt.Errorf("spool: keys cipher: %w", err)
	}
	sealed := aead.Seal(nil, nonce[:], body, keysAAD(hdr, storeID[:]))
	wipe(body)

	out := make([]byte, 0, len(hdr)+keysNonce+4+len(sealed))
	out = append(out, hdr...)
	out = append(out, nonce[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(sealed)))
	out = append(out, tmp[:4]...)
	out = append(out, sealed...)
	return atomicWriteFile(dir, keysFileName, out)
}

// keysHeader is the parsed envelope header: KDF parameters plus the
// unauthenticated wrapping-key lookup hint.
type keysHeader struct {
	kdf      uint8
	seq      uint64
	salt     []byte
	aTime    uint32
	aMem     uint32
	aThreads uint8
	wrapHint string
	length   int // total header bytes including the CRC
}

// parseKeysHeader validates the envelope header structurally. It
// authenticates nothing; the hint it returns selects key material
// but never proves identity.
func parseKeysHeader(raw []byte) (*keysHeader, error) {
	if len(raw) < 61 {
		return nil, fmt.Errorf("spool: truncated keys.enc: %w", ErrCorrupt)
	}
	if string(raw[0:4]) != keysMagic {
		return nil, fmt.Errorf("spool: bad keys.enc magic: %w", ErrCorrupt)
	}
	if v := binary.LittleEndian.Uint16(raw[4:6]); v != keysVersion {
		return nil, fmt.Errorf("spool: keys.enc version %d rejected without migration: %w", v, ErrUnsupportedVersion)
	}
	h := int(raw[56])
	if h > maxWrapIDLen || len(raw) < 61+h {
		return nil, fmt.Errorf("spool: bad keys.enc hint length: %w", ErrCorrupt)
	}
	hdr := raw[:57+h]
	if got, want := crc32.Checksum(hdr, castagnoli), binary.LittleEndian.Uint32(raw[57+h:61+h]); got != want {
		return nil, fmt.Errorf("spool: keys.enc header checksum mismatch: %w", ErrCorrupt)
	}
	return &keysHeader{
		kdf:      raw[6],
		seq:      binary.LittleEndian.Uint64(raw[7:15]),
		salt:     append([]byte(nil), raw[15:47]...),
		aTime:    binary.LittleEndian.Uint32(raw[47:51]),
		aMem:     binary.LittleEndian.Uint32(raw[51:55]),
		aThreads: raw[55],
		wrapHint: string(raw[57 : 57+h]),
		length:   61 + h,
	}, nil
}

// readKeys loads and unlocks the container. storeID and contextID
// come from the store manifest; the KDF binds both, so an envelope
// from another store or context fails authentication.
func readKeys(dir string, masterKey []byte, passphrase string, storeID, contextID [16]byte) (*keyring, error) {
	raw, err := os.ReadFile(filepath.Join(dir, keysFileName))
	if err != nil {
		return nil, fmt.Errorf("spool: read keys.enc: %w", err)
	}
	h, err := parseKeysHeader(raw)
	if err != nil {
		return nil, err
	}
	if len(raw) < h.length+keysNonce+4 {
		return nil, fmt.Errorf("spool: truncated keys.enc: %w", ErrCorrupt)
	}
	protector, err := deriveProtector(h.kdf, masterKey, passphrase, h.salt, h.aTime, h.aMem, h.aThreads, storeID, contextID)
	if err != nil {
		return nil, err
	}
	defer wipe(protector[:])

	off := h.length
	nonce := raw[off : off+keysNonce]
	off += keysNonce
	sealedLen := binary.LittleEndian.Uint32(raw[off : off+4])
	off += 4
	if sealedLen > 1<<24 || uint64(len(raw[off:])) != uint64(sealedLen) {
		return nil, fmt.Errorf("spool: bad keys.enc sealed size: %w", ErrCorrupt)
	}
	aead, err := aesGCM(protector)
	if err != nil {
		return nil, fmt.Errorf("spool: keys cipher: %w", err)
	}
	body, err := aead.Open(nil, nonce, raw[off:], keysAAD(raw[:h.length], storeID[:]))
	if err != nil {
		return nil, fmt.Errorf("spool: keys.enc authentication failed: %w", ErrWrongKey)
	}
	defer wipe(body)
	if len(body) < 4+16+1 {
		return nil, fmt.Errorf("spool: truncated keys.enc body: %w", ErrCorrupt)
	}
	count := binary.LittleEndian.Uint32(body[:4])
	if count == 0 || count > maxDataKeys {
		return nil, fmt.Errorf("spool: bad keys.enc key count %d: %w", count, ErrCorrupt)
	}
	var context [16]byte
	copy(context[:], body[4:20])
	w := int(body[20])
	if w > maxWrapIDLen || len(body) < 21+w {
		return nil, fmt.Errorf("spool: bad keys.enc wrapping id: %w", ErrCorrupt)
	}
	wrapID := string(body[21 : 21+w])
	const entryLen = 4 + 8 + 1 + 32
	if uint64(len(body[21+w:])) != uint64(count)*entryLen {
		return nil, fmt.Errorf("spool: keys.enc body size mismatch: %w", ErrCorrupt)
	}
	// The KDF bound the manifest's context, so a successful open
	// authenticates this same context; anything else is a mixed
	// generation.
	if context != contextID {
		return nil, fmt.Errorf("spool: keys.enc context does not match manifest: %w", ErrCorrupt)
	}
	r := newKeyring()
	r.seq = h.seq
	r.context = context
	r.wrapID = wrapID
	pos := 21 + w
	for i := uint32(0); i < count; i++ {
		e := body[pos : pos+entryLen]
		pos += entryLen
		id := binary.LittleEndian.Uint32(e[0:4])
		created := int64(binary.LittleEndian.Uint64(e[4:12]))
		status := KeyStatus(e[12])
		if status != KeyStatusActive && status != KeyStatusRetired {
			return nil, fmt.Errorf("spool: bad key status %d: %w", status, ErrCorrupt)
		}
		if id == 0 {
			return nil, fmt.Errorf("spool: zero key id: %w", ErrCorrupt)
		}
		if _, dup := r.keys[id]; dup {
			return nil, fmt.Errorf("spool: duplicate key id %d: %w", id, ErrCorrupt)
		}
		k := &DataKey{ID: id, CreatedAt: created, Status: status}
		copy(k.Key[:], e[13:45])
		r.keys[id] = k
		if status == KeyStatusActive && id > r.current {
			r.current = id
		}
	}
	if r.current == 0 {
		return nil, fmt.Errorf("spool: no active data key: %w", ErrCorrupt)
	}
	return r, nil
}

// KeyHint carries bounded, non-secret store metadata for wrapping-key
// selection. It is read without key material: the wrapping id is a
// lookup hint only, trusted after the envelope authenticates.
type KeyHint struct {
	// Encrypted reports whether the store needs key material.
	Encrypted bool
	// StoreID is the manifest's immutable store identity.
	StoreID [16]byte
	// ContextID is the manifest's database context.
	ContextID [16]byte
	// WrappingKeyID hints the wrapping material to look up.
	WrappingKeyID string
	// KDF names the envelope KDF (0 = direct, 1 = argon2id).
	KDF uint8
	// KeyringSeq is the envelope's informational persist counter.
	KeyringSeq uint64
}

// ReadKeyHint returns a store's key-selection metadata without
// unlocking anything. Use it to pick KeyProvider material, then open
// with the selected key; the open authenticates the hint.
func ReadKeyHint(dir string) (KeyHint, error) {
	var hint KeyHint
	raw, err := os.ReadFile(filepath.Join(dir, manifestFileName))
	if err != nil {
		return hint, fmt.Errorf("spool: read manifest: %w", err)
	}
	m, err := parseManifest(raw)
	if err != nil {
		return hint, err
	}
	hint.StoreID = m.storeID
	hint.ContextID = m.context
	hint.Encrypted = m.encrypted
	if !m.encrypted {
		return hint, nil
	}
	kraw, err := os.ReadFile(filepath.Join(dir, keysFileName))
	if err != nil {
		return hint, fmt.Errorf("spool: read keys.enc: %w", err)
	}
	h, err := parseKeysHeader(kraw)
	if err != nil {
		return hint, err
	}
	hint.WrappingKeyID = h.wrapHint
	hint.KDF = h.kdf
	hint.KeyringSeq = h.seq
	return hint, nil
}

// ValidateWrappingKey authenticates a store's key envelope without opening a
// writable Spool instance or scanning its records. expectedContext and
// expectedWrappingID are optional; when supplied they must match the
// authenticated envelope identity.
func ValidateWrappingKey(dir string, masterKey, expectedContext []byte, expectedWrappingID string) error {
	if len(masterKey) != 32 {
		return fmt.Errorf("spool: master key must be 32 bytes")
	}
	if len(expectedContext) != 0 && len(expectedContext) != 16 {
		return fmt.Errorf("spool: expected context must be 16 bytes")
	}
	hint, err := ReadKeyHint(dir)
	if err != nil {
		return err
	}
	if !hint.Encrypted {
		return fmt.Errorf("spool: store is not encrypted: %w", ErrWrongKey)
	}
	if len(expectedContext) == 16 && string(expectedContext) != string(hint.ContextID[:]) {
		return fmt.Errorf("spool: expected context does not match store: %w", ErrContextMismatch)
	}
	ring, err := readKeys(dir, masterKey, "", hint.StoreID, hint.ContextID)
	if err != nil {
		return err
	}
	defer func() {
		ring.mu.Lock()
		defer ring.mu.Unlock()
		for _, key := range ring.keys {
			wipe(key.Key[:])
		}
		clear(ring.keys)
	}()
	if expectedWrappingID != "" && ring.wrapID != expectedWrappingID {
		return fmt.Errorf("spool: authenticated wrapping-key id %q does not match %q: %w", ring.wrapID, expectedWrappingID, ErrWrongKey)
	}
	return nil
}
