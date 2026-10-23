package spool

import (
	"fmt"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
)

// Durability selects how much of the write path a commit waits for.
// It applies to background flushes; the explicit Flush method always
// fsyncs before returning.
type Durability int

const (
	// DurabilityAsync commits a block once its bytes are staged in the
	// segment writer buffer. Fastest; recent writes may disappear
	// after process or OS failure.
	DurabilityAsync Durability = iota
	// DurabilityFlush commits a block once its bytes reached the OS
	// (write syscall). Survives process crashes; may lose data on OS
	// failure.
	DurabilityFlush
	// DurabilitySync fsyncs before acknowledging a commit. Slowest,
	// strongest durability.
	DurabilitySync
)

// String returns a human-readable durability name.
func (d Durability) String() string {
	switch d {
	case DurabilityAsync:
		return "async"
	case DurabilityFlush:
		return "flush"
	case DurabilitySync:
		return "sync"
	default:
		return fmt.Sprintf("unknown(%d)", int(d))
	}
}

// Compression selects the block-level codec. Compression always runs
// before encryption.
type Compression int

const (
	// CompressionNone stores blocks uncompressed.
	CompressionNone Compression = iota
	// compression ids 1 and 2 are retired (formerly zstd) and are
	// never reused.
	_
	_
	// CompressionDeflate uses the standard library's deflate at its
	// fastest level. It is the default: no external dependency.
	CompressionDeflate
)

// String returns a human-readable compression name.
func (c Compression) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionDeflate:
		return "deflate"
	default:
		if c >= Compression(compression.MinUserID) && c <= 255 {
			return fmt.Sprintf("custom(%d)", int(c))
		}
		return fmt.Sprintf("unknown(%d)", int(c))
	}
}

// Encryption selects the block AEAD. All ciphers come from the Go
// standard library (crypto/aes, crypto/cipher).
//
// The zero value selects the default cipher, so plaintext storage
// always requires explicitly passing EncryptionNone together with
// empty key material.
type Encryption int

const (
	// EncryptionDefault selects EncryptionAES256GCM.
	EncryptionDefault Encryption = iota
	// EncryptionNone stores blocks without encryption. Headers and a
	// trailing payload checksum still detect corruption, but data is
	// readable on disk. Explicit opt-out only, for tests and local
	// development.
	EncryptionNone
	// EncryptionAES256GCM uses AES-256-GCM with random 96-bit nonces
	// per block. It is the default and the only cipher: 256-bit data
	// keys, standard-library only.
	EncryptionAES256GCM
)

// String returns a human-readable encryption name.
func (e Encryption) String() string {
	switch e {
	case EncryptionDefault:
		return "default(aes-256-gcm)"
	case EncryptionNone:
		return "none"
	case EncryptionAES256GCM:
		return "aes-256-gcm"
	default:
		return fmt.Sprintf("unknown(%d)", int(e))
	}
}

// FlushPolicy controls when buffered writes become a block. Whichever
// threshold trips first seals the current buffers. Zero selects the
// default; negative disables a threshold.
type FlushPolicy struct {
	// MaxRecords seals after this many pending records.
	MaxRecords int
	// MaxBytes seals after this many pending bytes.
	MaxBytes int64
	// MaxDelay seals buffers older than this.
	MaxDelay time.Duration
}

// Options configures a spool store. Use DefaultOptions and override
// the fields you need.
type Options struct {
	// Path is the store directory holding manifest, keys.enc and
	// segments/. It is created when missing.
	Path string

	// MasterKey is a caller-owned 256-bit key used to protect keys.enc
	// (via HKDF, never directly). Exactly one of MasterKey and
	// Passphrase must be set unless Encryption is EncryptionNone.
	MasterKey []byte
	// Passphrase is stretched with Argon2id into the keys.enc
	// protector. Prefer MasterKey when the caller already manages key
	// material.
	Passphrase string
	// WrappingKeyID names the wrapping material for key-provider
	// selection. It is recorded in the authenticated key envelope;
	// when set, open verifies it against that authenticated value.
	// Empty accepts any envelope id. At most 128 bytes, and only
	// with encryption.
	WrappingKeyID string
	// ContextID is the expected 16-byte database context. It binds
	// block authentication and key derivation; a provided value must
	// match the store's persisted context before writable open. Nil
	// or empty selects the default: fresh stores persist their
	// generated store id, opens accept the persisted context.
	ContextID []byte

	// Encryption selects the block AEAD. Default
	// EncryptionAES256GCM (standard library).
	Encryption Encryption
	// Compression selects the block codec. Default CompressionDeflate.
	Compression Compression
	// CompressionSet distinguishes an explicit CompressionNone (the zero
	// value) from an omitted setting. DefaultOptions sets this automatically;
	// struct literals selecting CompressionNone must set it to true.
	CompressionSet bool
	// Codec, when non-nil, is a custom block codec (id 128-255) used
	// for new blocks; it overrides Compression. It is also registered
	// for reading.
	Codec compression.Codec
	// Codecs are additional custom codecs registered for reading
	// blocks written under other codecs. The same set must be
	// supplied on every open of a store that contains such blocks.
	Codecs []compression.Codec
	// Durability selects background-flush durability. Default
	// DurabilityAsync: maximum speed, recent writes may disappear
	// after process/OS failure. Call Flush for explicit durability.
	Durability Durability

	// Flush bounds a flush batch. Defaults: 10k records, 8MB, 100ms.
	Flush FlushPolicy

	// WriteShards is the number of put buffers; must be a power of
	// two. Default 128.
	WriteShards int
	// IndexShards is the number of index shards; must be a power of
	// two. Default 256.
	IndexShards int

	// TargetBlockBytes seals a block around this size; single records
	// larger than it get a dedicated block. Default 64KB.
	TargetBlockBytes int
	// ReadCacheBytes budgets the hot block cache: total retained
	// sealed frames, decoded bodies and record descriptors.
	// Default 32MB. Zero disables caching. Blocks are admitted on
	// a second read.
	ReadCacheBytes int
	// MaxBlockBytes is the hard cap for one block. Default 64MB.
	MaxBlockBytes int
	// MaxRecordsPerBlock caps records in one block. Default 10,000.
	MaxRecordsPerBlock int

	// MaxKeySize rejects larger keys. Default 1MB.
	MaxKeySize int
	// MaxValueSize rejects larger values. Default 16MB.
	MaxValueSize int

	// MaxAtomicBatchBytes bounds one Commit group's plaintext bodies
	// including record/group framing. A group has no wire-size
	// surprise: plaintext is computed exactly before admission and
	// the encoded frames are rechecked after building. Default
	// 256MB; must cover MaxBlockBytes.
	MaxAtomicBatchBytes int64
	// MaxSegmentSize rolls to a new segment past this size. Default
	// 256MB.
	MaxSegmentSize int64
	// MaxPendingBytes caps buffered plus in-flight write bytes; Put
	// blocks past it (TryPut returns ErrBackpressure). Default 512MB.
	// A single record larger than the cap bypasses the wait (it is
	// already bounded by MaxValueSize).
	MaxPendingBytes int64

	// Workers bounds parallel block compression/encryption. Default
	// runtime.NumCPU clamped to [2,32]. This separately bounds
	// worker-held buffers: at most Workers blocks are built at
	// once, each holding plaintext, compressed, and sealed copies
	// transiently (see Stats.WorkerBytesMax).
	Workers int

	// OnStorageError receives the terminal storage failure, once,
	// outside internal locks. The store rejects further mutating
	// operations after it fires; reopen to recover.
	OnStorageError func(error)
	// OnReplayProgress, when non-nil, receives segment replay
	// advancement during open. Calls are serialized and run outside
	// internal locks. Nil disables reporting entirely.
	OnReplayProgress func(ReplayProgress)
	// CompactionThreshold in (0,1]: a sealed segment is rewritten
	// when its live value bytes are at most this fraction of its
	// total bytes. Tombstones are excluded (see pickCompaction).
	// Default 0.20.
	CompactionThreshold float64
	// CompactionMinFreeBytes refuses rewrite passes while the
	// store's filesystem reports less free space: compaction holds
	// old and replacement data simultaneously. Zero disables the
	// check. A starting point is one MaxSegmentSize. Deletion of
	// fully dead files still runs (it frees space).
	CompactionMinFreeBytes int64
	// TombProofThreshold gates deletion-marker garbage collection:
	// a reclamation pass scans authoritative segments to prove
	// tombstones droppable only once this many live markers
	// accumulate. Zero selects the default 1024. The scan is exact
	// but store-wide, hence the gate.
	TombProofThreshold int
	// ReclaimInterval ticks background tombstone cleanup, dead-file
	// deletion and compaction. Negative disables the background
	// worker (reclamation then only runs explicitly via Reclaim).
	// Default 30s.
	ReclaimInterval time.Duration
	// DataKeyMaxAge rotates the data key when the current key is
	// older than this. The check runs on open and before new groups
	// are admitted; the successor persists before any group can
	// reference it. Zero disables age rotation.
	DataKeyMaxAge time.Duration
	// Faults injects storage failures at durability boundaries
	// for integration testing. Nil disables injection. Hooks must
	// be safe for concurrent use.
	Faults *FaultHooks
}

// FaultHooks injects storage failures at durability boundaries. Each
// hook runs before its operation; a non-nil return fails the
// operation as if the disk failed. Murmur's integration tests use
// these to prove terminal-failure and recovery behavior without
// privileged disk-full setups.
type FaultHooks struct {
	// Append runs before group frames append to a segment.
	Append func() error
	// SegmentSync runs before a segment fsync.
	SegmentSync func() error
	// ManifestPersist runs before a manifest publication.
	ManifestPersist func() error
	// KeyringPersist runs before a keys.enc publication.
	KeyringPersist func() error
	// IntentPersist runs before a maintenance intent write.
	IntentPersist func() error
	// CompactionStage runs before a compaction pass stages its
	// replacement file.
	CompactionStage func() error
	// CheckpointLink runs before each checkpoint segment hard
	// link. It simulates cross-filesystem and link failures.
	CheckpointLink func() error
	// Rename runs before a staged segment file renames over its
	// final name (compaction and maintenance publishes).
	Rename func() error
	// DirSync runs before directory fsyncs that persist segment,
	// checkpoint, and sweep structural changes.
	DirSync func() error
	// Delete runs before segment, staging-temp, and intent
	// removals.
	Delete func() error
}

// trip runs one hook by name, tolerating a nil set or hook. Names
// are append, sync, manifest, keyring, intent, compact,
// checkpoint, rename, dirsync, and delete.
func (f *FaultHooks) trip(which string) error {
	if f == nil {
		return nil
	}
	var h func() error
	switch which {
	case "append":
		h = f.Append
	case "sync":
		h = f.SegmentSync
	case "manifest":
		h = f.ManifestPersist
	case "keyring":
		h = f.KeyringPersist
	case "intent":
		h = f.IntentPersist
	case "compact":
		h = f.CompactionStage
	case "checkpoint":
		h = f.CheckpointLink
	case "rename":
		h = f.Rename
	case "dirsync":
		h = f.DirSync
	case "delete":
		h = f.Delete
	}
	if h == nil {
		return nil
	}
	return h()
}

// DefaultOptions returns Options with the recommended defaults. Path
// and key material must still be supplied by the caller.
func DefaultOptions(path string) Options {
	return Options{
		Path:                path,
		Encryption:          EncryptionAES256GCM,
		Compression:         CompressionDeflate,
		CompressionSet:      true,
		Durability:          DurabilityAsync,
		Flush:               FlushPolicy{MaxRecords: 10_000, MaxBytes: 8 << 20, MaxDelay: 100 * time.Millisecond},
		WriteShards:         128,
		IndexShards:         256,
		TargetBlockBytes:    64 << 10,
		ReadCacheBytes:      32 << 20,
		MaxBlockBytes:       64 << 20,
		MaxRecordsPerBlock:  10_000,
		MaxKeySize:          1 << 20,
		MaxValueSize:        16 << 20,
		MaxAtomicBatchBytes: 256 << 20,
		MaxSegmentSize:      256 << 20,
		MaxPendingBytes:     512 << 20,
		CompactionThreshold: 0.20,
		TombProofThreshold:  1024,
		ReclaimInterval:     30 * time.Second,
	}
}

// normalized fills zero-valued option fields with defaults and
// validates the result.
// allCodecs returns the user codecs to register (Codec first).
func (o Options) allCodecs() []compression.Codec {
	if o.Codec == nil {
		return o.Codecs
	}
	return append([]compression.Codec{o.Codec}, o.Codecs...)
}

func (o Options) normalized() (Options, error) {
	d := DefaultOptions(o.Path)
	if o.Encryption == EncryptionDefault {
		d.Encryption = EncryptionAES256GCM
	} else {
		d.Encryption = o.Encryption
	}
	if o.CompressionSet || o.Compression != CompressionNone {
		d.Compression = o.Compression
	}
	// Durability zero value is DurabilityAsync: always honor the
	// caller's value verbatim.
	d.Durability = o.Durability
	if o.Flush.MaxRecords != 0 {
		d.Flush.MaxRecords = o.Flush.MaxRecords
	}
	if o.Flush.MaxBytes != 0 {
		d.Flush.MaxBytes = o.Flush.MaxBytes
	}
	if o.Flush.MaxDelay != 0 {
		d.Flush.MaxDelay = o.Flush.MaxDelay
	}
	if o.WriteShards != 0 {
		d.WriteShards = o.WriteShards
	}
	if o.IndexShards != 0 {
		d.IndexShards = o.IndexShards
	}
	if o.TargetBlockBytes != 0 {
		d.TargetBlockBytes = o.TargetBlockBytes
	}
	// Zero disables the cache; copy verbatim without default fill.
	d.ReadCacheBytes = o.ReadCacheBytes
	if o.MaxBlockBytes != 0 {
		d.MaxBlockBytes = o.MaxBlockBytes
	}
	if o.MaxRecordsPerBlock != 0 {
		d.MaxRecordsPerBlock = o.MaxRecordsPerBlock
	}
	if o.MaxKeySize != 0 {
		d.MaxKeySize = o.MaxKeySize
	}
	if o.MaxValueSize != 0 {
		d.MaxValueSize = o.MaxValueSize
	}
	if o.MaxAtomicBatchBytes != 0 {
		d.MaxAtomicBatchBytes = o.MaxAtomicBatchBytes
	}
	if o.MaxSegmentSize != 0 {
		d.MaxSegmentSize = o.MaxSegmentSize
	}
	d.OnStorageError = o.OnStorageError
	d.OnReplayProgress = o.OnReplayProgress
	if o.MaxPendingBytes != 0 {
		d.MaxPendingBytes = o.MaxPendingBytes
	}
	if o.Workers != 0 {
		d.Workers = o.Workers
	}
	if o.CompactionThreshold != 0 {
		d.CompactionThreshold = o.CompactionThreshold
	}
	if o.CompactionMinFreeBytes != 0 {
		d.CompactionMinFreeBytes = o.CompactionMinFreeBytes
	}
	if o.TombProofThreshold != 0 {
		d.TombProofThreshold = o.TombProofThreshold
	}
	if o.ReclaimInterval != 0 {
		d.ReclaimInterval = o.ReclaimInterval
	}
	if o.DataKeyMaxAge != 0 {
		d.DataKeyMaxAge = o.DataKeyMaxAge
	}
	d.Faults = o.Faults
	d.Path = o.Path
	d.MasterKey = o.MasterKey
	d.Passphrase = o.Passphrase
	d.WrappingKeyID = o.WrappingKeyID
	d.ContextID = o.ContextID
	d.Codec = o.Codec
	d.Codecs = o.Codecs
	if o.Codec != nil {
		d.Compression = Compression(o.Codec.ID())
	}

	if err := d.validate(); err != nil {
		return Options{}, err
	}
	return d, nil
}

func isPow2(n int) bool { return n > 0 && n&(n-1) == 0 }

func (o Options) validate() error {
	if o.Path == "" {
		return fmt.Errorf("spool: Path is required")
	}
	if len(o.ContextID) != 0 && len(o.ContextID) != 16 {
		return fmt.Errorf("spool: ContextID must be 16 bytes, got %d", len(o.ContextID))
	}
	if len(o.WrappingKeyID) > maxWrapIDLen {
		return fmt.Errorf("spool: WrappingKeyID of %d bytes exceeds %d", len(o.WrappingKeyID), maxWrapIDLen)
	}
	switch o.Encryption {
	case EncryptionNone:
		if len(o.MasterKey) > 0 || o.Passphrase != "" {
			return fmt.Errorf("spool: key material must be empty with EncryptionNone")
		}
		if o.WrappingKeyID != "" {
			return fmt.Errorf("spool: WrappingKeyID requires encryption")
		}
	case EncryptionAES256GCM:
		if len(o.MasterKey) > 0 && o.Passphrase != "" {
			return fmt.Errorf("spool: set exactly one of MasterKey and Passphrase")
		}
		if len(o.MasterKey) == 0 && o.Passphrase == "" {
			return fmt.Errorf("spool: MasterKey or Passphrase is required unless EncryptionNone")
		}
		if len(o.MasterKey) > 0 && len(o.MasterKey) != 32 {
			return fmt.Errorf("spool: MasterKey must be 32 bytes, got %d", len(o.MasterKey))
		}
	default:
		return fmt.Errorf("spool: unknown Encryption %d", int(o.Encryption))
	}
	if _, err := newCompressor(o.Compression, o.allCodecs()); err != nil {
		return err
	}
	switch o.Durability {
	case DurabilityAsync, DurabilityFlush, DurabilitySync:
	default:
		return fmt.Errorf("spool: unknown Durability %d", int(o.Durability))
	}
	if !isPow2(o.WriteShards) {
		return fmt.Errorf("spool: WriteShards must be a power of two, got %d", o.WriteShards)
	}
	if !isPow2(o.IndexShards) {
		return fmt.Errorf("spool: IndexShards must be a power of two, got %d", o.IndexShards)
	}
	if o.TargetBlockBytes <= 0 {
		return fmt.Errorf("spool: TargetBlockBytes must be positive")
	}
	if o.MaxBlockBytes < o.TargetBlockBytes {
		return fmt.Errorf("spool: MaxBlockBytes must cover TargetBlockBytes")
	}
	if o.MaxRecordsPerBlock <= 0 {
		return fmt.Errorf("spool: MaxRecordsPerBlock must be positive")
	}
	if o.MaxKeySize <= 0 {
		return fmt.Errorf("spool: MaxKeySize must be positive")
	}
	if o.MaxValueSize <= 0 {
		return fmt.Errorf("spool: MaxValueSize must be positive")
	}
	// A record must fit into one block with room for framing.
	if int64(o.MaxKeySize)+int64(o.MaxValueSize)+recordOverhead > int64(o.MaxBlockBytes) {
		return fmt.Errorf("spool: MaxKeySize+MaxValueSize must fit into MaxBlockBytes")
	}
	if o.MaxAtomicBatchBytes < int64(o.MaxBlockBytes) {
		return fmt.Errorf("spool: MaxAtomicBatchBytes must cover MaxBlockBytes")
	}
	if o.MaxSegmentSize <= int64(o.MaxBlockBytes) {
		return fmt.Errorf("spool: MaxSegmentSize must exceed MaxBlockBytes")
	}
	if o.MaxPendingBytes <= 0 {
		return fmt.Errorf("spool: MaxPendingBytes must be positive")
	}
	if o.Workers < 0 {
		return fmt.Errorf("spool: Workers must be non-negative")
	}
	if o.CompactionThreshold <= 0 || o.CompactionThreshold > 1 {
		return fmt.Errorf("spool: CompactionThreshold must be in (0,1]")
	}
	if o.CompactionMinFreeBytes < 0 {
		return fmt.Errorf("spool: CompactionMinFreeBytes must be non-negative")
	}
	if o.TombProofThreshold < 0 {
		return fmt.Errorf("spool: TombProofThreshold must be non-negative")
	}
	if o.DataKeyMaxAge < 0 {
		return fmt.Errorf("spool: DataKeyMaxAge must be non-negative")
	}
	// Flush thresholds accept negatives (disable) by design.
	return nil
}
