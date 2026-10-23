package spool

import "errors"

// Sentinels returned by the spool package. Use errors.Is for matching.
var (
	// ErrClosed is returned when operating on a closed store.
	ErrClosed = errors.New("spool: store is closed")
	// ErrBackpressure is returned by TryPut when the pending write
	// buffer is full and the call would have blocked.
	ErrBackpressure = errors.New("spool: pending write buffer full")
	// ErrKeyTooLarge is returned when a key exceeds MaxKeySize.
	ErrKeyTooLarge = errors.New("spool: key exceeds MaxKeySize")
	// ErrValueTooLarge is returned when a value exceeds MaxValueSize.
	ErrValueTooLarge = errors.New("spool: value exceeds MaxValueSize")
	// ErrRecordTooLarge is returned when a single record cannot fit
	// into MaxBlockBytes.
	ErrRecordTooLarge = errors.New("spool: record exceeds MaxBlockBytes")
	// ErrCorrupt is returned when on-disk data fails structural
	// validation (magic, version, bounds, checksums).
	ErrCorrupt = errors.New("spool: corrupt data")
	// ErrAuthFailed is returned when block or key-container
	// authentication fails. It indicates corruption or, for a freshly
	// opened store, a wrong master key / passphrase.
	ErrAuthFailed = errors.New("spool: authentication failed")
	// ErrWrongKey wraps authentication failures observed while opening
	// an existing store; the master key or passphrase is wrong, or
	// keys.enc is damaged.
	ErrWrongKey = errors.New("spool: wrong master key or damaged keys.enc")
	// ErrLocked is returned when the store directory is already open
	// in another process.
	ErrLocked = errors.New("spool: store is locked by another process")
	// ErrUnknownKeyID is returned when a block references a data key
	// ID absent from the keyring.
	ErrUnknownKeyID = errors.New("spool: block references unknown data key")
	// ErrUnsupportedVersion is returned for on-disk structures with a
	// format version newer than this package understands.
	ErrUnsupportedVersion = errors.New("spool: unsupported format version")
	// ErrSeqExhausted is returned when the per-open sequence counter
	// would overflow into the next epoch. Restart cleanly to continue;
	// reaching this requires ~2^48 writes in one process lifetime.
	ErrSeqExhausted = errors.New("spool: sequence space exhausted, restart the store")
	// ErrGroupTooLarge is returned when a Commit group exceeds
	// MaxAtomicBatchBytes or cannot fit the pending capacity. The
	// whole group is rejected; nothing is admitted.
	ErrGroupTooLarge = errors.New("spool: commit group too large")
	// ErrBadMutation is returned when a Commit mutation is malformed
	// (a deleted mutation carrying a value). The whole group is
	// rejected; nothing is admitted.
	ErrBadMutation = errors.New("spool: deleted mutation must have no value")
	// ErrStorageFailed is returned by mutating operations after a
	// terminal storage failure (uncertain append/sync or failed
	// authoritative publication). StorageError reports the cause;
	// reopen to recover.
	ErrStorageFailed = errors.New("spool: terminal storage failure")
	// ErrContextMismatch is returned when the supplied database
	// context does not match the store's persisted context.
	ErrContextMismatch = errors.New("spool: database context mismatch")
	// ErrMaintenance is returned by mutating operations while a
	// rewrite or rebind holds the store. Reads of memory state
	// (Stats, KeyInventory) stay available.
	ErrMaintenance = errors.New("spool: maintenance in progress")
)
