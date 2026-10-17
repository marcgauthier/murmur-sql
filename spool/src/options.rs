//! Store options.
//!
//! Rust port of `spool/options.go`.

use std::sync::Arc;
use std::time::Duration;

use crate::compression::Codec;
use crate::error::{err, Error, ErrorKind, Result};
use crate::loader::ReplayProgress;

/// Durability selects how much of the write path a commit waits for.
///
/// It applies to background flushes; [`crate::Store::flush`] always fsyncs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// Commit once bytes are staged in the segment writer buffer. Fastest;
    /// recent writes may disappear after process or OS failure.
    #[default]
    Async,
    /// Commit once bytes reached the OS (write syscall). Survives process
    /// crashes; may lose data on OS failure.
    Flush,
    /// fsync before acknowledging. Slowest, strongest durability.
    Sync,
}

impl std::fmt::Display for Durability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Durability::Async => write!(f, "async"),
            Durability::Flush => write!(f, "flush"),
            Durability::Sync => write!(f, "sync"),
        }
    }
}

/// Compression selects the block-level codec. Compression always runs
/// before encryption. The value equals the wire codec id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Compression(pub u8);

impl Compression {
    /// Store blocks uncompressed (wire id 0).
    pub const NONE: Compression = Compression(0);
    /// Ids 1 and 2 are retired (formerly zstd) and never reused.
    pub const DEFLATE: Compression = Compression(3);

    /// Lowest id a user codec may use.
    pub const MIN_USER_ID: u8 = 128;
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            0 => write!(f, "none"),
            3 => write!(f, "deflate"),
            v if v >= Self::MIN_USER_ID => write!(f, "custom({v})"),
            v => write!(f, "unknown({v})"),
        }
    }
}

/// Encryption selects the block AEAD.
///
/// The default selects AES-256-GCM, so plaintext storage always requires
/// explicitly passing [`Encryption::None`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Encryption {
    /// Selects [`Encryption::Aes256Gcm`].
    #[default]
    Default,
    /// Store blocks without encryption. Headers and a trailing payload
    /// checksum still detect corruption, but data is readable on disk.
    /// Explicit opt-out only.
    None,
    /// AES-256-GCM with random 96-bit nonces per block. The default and
    /// only cipher.
    Aes256Gcm,
}

impl std::fmt::Display for Encryption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Encryption::Default => write!(f, "default(aes-256-gcm)"),
            Encryption::None => write!(f, "none"),
            Encryption::Aes256Gcm => write!(f, "aes-256-gcm"),
        }
    }
}

/// FlushPolicy controls when buffered writes become a block. Whichever
/// threshold trips first seals the current buffers. Zero selects the
/// default; negative disables a threshold.
#[derive(Debug, Clone)]
pub struct FlushPolicy {
    /// Seal after this many pending records.
    pub max_records: i64,
    /// Seal after this many pending bytes.
    pub max_bytes: i64,
    /// Seal buffers older than this.
    pub max_delay: Duration,
}

impl Default for FlushPolicy {
    fn default() -> Self {
        FlushPolicy {
            max_records: 10_000,
            max_bytes: 8 << 20,
            max_delay: Duration::from_millis(100),
        }
    }
}

/// Fault hook: `Ok(())` lets the operation proceed, `Err(msg)` fails it
/// as if the disk failed.
pub type FaultHook = Arc<dyn Fn() -> std::result::Result<(), String> + Send + Sync>;

/// FaultHooks inject storage failures at durability boundaries for
/// integration testing. All hooks must be safe for concurrent use.
#[derive(Clone, Default)]
pub struct FaultHooks {
    /// Before group frames append to a segment.
    pub append: Option<FaultHook>,
    /// Before a segment fsync.
    pub segment_sync: Option<FaultHook>,
    /// Before a manifest publication.
    pub manifest_persist: Option<FaultHook>,
    /// Before a keys.enc publication.
    pub keyring_persist: Option<FaultHook>,
    /// Before a maintenance intent write.
    pub intent_persist: Option<FaultHook>,
    /// Before a compaction pass stages its replacement file.
    pub compaction_stage: Option<FaultHook>,
    /// Before each checkpoint segment hard link.
    pub checkpoint_link: Option<FaultHook>,
    /// Before a staged segment file renames over its final name.
    pub rename: Option<FaultHook>,
    /// Before directory fsyncs.
    pub dir_sync: Option<FaultHook>,
    /// Before segment, staging-temp, and intent removals.
    pub delete: Option<FaultHook>,
}

impl std::fmt::Debug for FaultHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultHooks {{ .. }}")
    }
}

impl FaultHooks {
    /// Run one hook by name. Names: append, sync, manifest, keyring,
    /// intent, compact, checkpoint, rename, dirsync, delete.
    pub fn trip(&self, which: &str) -> crate::error::Result<()> {
        let h = match which {
            "append" => &self.append,
            "sync" => &self.segment_sync,
            "manifest" => &self.manifest_persist,
            "keyring" => &self.keyring_persist,
            "intent" => &self.intent_persist,
            "compact" => &self.compaction_stage,
            "checkpoint" => &self.checkpoint_link,
            "rename" => &self.rename,
            "dirsync" => &self.dir_sync,
            "delete" => &self.delete,
            _ => &None,
        };
        if let Some(h) = h {
            h().map_err(|e| err(ErrorKind::Io, format!("injected {which} fault: {e}")))?;
        }
        Ok(())
    }
}

/// Options configures a spool store. Use [`Options::with_defaults`] and
/// override the fields you need.
#[derive(Clone)]
pub struct Options {
    /// Store directory holding manifest, keys.enc and segments/.
    /// Created when missing.
    pub path: String,
    /// Caller-owned 256-bit key protecting keys.enc (via HKDF, never
    /// directly). Exactly one of master_key and passphrase must be set
    /// unless encryption is [`Encryption::None`].
    pub master_key: Vec<u8>,
    /// Passphrase stretched with Argon2id into the keys.enc protector.
    pub passphrase: String,
    /// Names the wrapping material for key-provider selection. Recorded
    /// in the authenticated key envelope; when set, open verifies it.
    /// At most 128 bytes, only with encryption.
    pub wrapping_key_id: String,
    /// Expected 16-byte database context. Binds block authentication and
    /// key derivation. Empty selects the default: fresh stores persist
    /// their generated store id, opens accept the persisted context.
    pub context_id: Vec<u8>,
    /// Block AEAD. Default AES-256-GCM.
    pub encryption: Encryption,
    /// Block codec. Default deflate. `None` means "unset" (default);
    /// use `Some(Compression::NONE)` to select plaintext explicitly.
    pub compression: Option<Compression>,
    /// Custom block codec (id 128-255) used for new blocks; overrides
    /// compression. Also registered for reading.
    pub codec: Option<Arc<dyn Codec>>,
    /// Additional custom codecs registered for reading.
    pub codecs: Vec<Arc<dyn Codec>>,
    /// Background-flush durability. Default async.
    pub durability: Durability,
    /// Flush batch bounds. Default 10k records, 8MB, 100ms.
    pub flush: FlushPolicy,
    /// Number of put buffers; power of two. Default 128.
    pub write_shards: usize,
    /// Number of index shards; power of two. Default 256.
    pub index_shards: usize,
    /// Seal a block around this size. Default 64 KiB.
    pub target_block_bytes: usize,
    /// Total retained compressed frames, decoded bodies and record descriptors.
    /// Default 32 MiB. Zero disables caching. Blocks are admitted on a second read.
    pub read_cache_bytes: usize,
    /// Hard cap for one block. Default 64MB.
    pub max_block_bytes: usize,
    /// Cap records in one block. Default 10,000.
    pub max_records_per_block: usize,
    /// Reject larger keys. Default 1MB.
    pub max_key_size: usize,
    /// Reject larger values. Default 16MB.
    pub max_value_size: usize,
    /// Bound one Commit group's plaintext bodies. Default 256MB; must
    /// cover max_block_bytes.
    pub max_atomic_batch_bytes: i64,
    /// Roll to a new segment past this size. Default 256MB.
    pub max_segment_size: i64,
    /// Cap buffered plus in-flight write bytes. Default 512MB.
    pub max_pending_bytes: i64,
    /// Bound parallel block compression/encryption. 0 selects
    /// available-parallelism clamped to [2,32].
    pub workers: usize,
    /// Receives the terminal storage failure once, outside internal
    /// locks.
    pub on_storage_error: Option<Arc<dyn Fn(Error) + Send + Sync>>,
    /// Receives segment replay advancement during open. None disables.
    pub on_replay_progress: Option<Arc<dyn Fn(ReplayProgress) + Send + Sync>>,
    /// Compaction threshold in (0,1]: a sealed segment is rewritten
    /// when its live value bytes are at most this fraction of its
    /// total bytes. Tombstones are excluded (see `pick_compaction`).
    /// Default 0.20.
    pub compaction_threshold: f64,
    /// Refuse rewrite passes while free space is below this. 0 disables.
    pub compaction_min_free_bytes: i64,
    /// Tombstone-proof scan gate. 0 selects the default 1024.
    pub tomb_proof_threshold: usize,
    /// Background reclamation interval. `None` disables the background
    /// worker (explicit `reclaim` only). Default 30s.
    pub reclaim_interval: Option<Duration>,
    /// Rotate the data key when older than this. `None` disables.
    pub data_key_max_age: Option<Duration>,
    /// Storage failure injection. None disables.
    pub faults: Option<Arc<FaultHooks>>,
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("path", &self.path)
            .field("encryption", &self.encryption)
            .field("compression", &self.compression)
            .field("durability", &self.durability)
            .finish_non_exhaustive()
    }
}

/// Default options for `path`. Key material must still be supplied.
pub fn default_options(path: impl Into<String>) -> Options {
    Options {
        path: path.into(),
        master_key: Vec::new(),
        passphrase: String::new(),
        wrapping_key_id: String::new(),
        context_id: Vec::new(),
        encryption: Encryption::Aes256Gcm,
        compression: Some(Compression::DEFLATE),
        codec: None,
        codecs: Vec::new(),
        durability: Durability::Async,
        flush: FlushPolicy::default(),
        write_shards: 128,
        index_shards: 256,
        target_block_bytes: 64 << 10,
        read_cache_bytes: 32 << 20,
        max_block_bytes: 64 << 20,
        max_records_per_block: 10_000,
        max_key_size: 1 << 20,
        max_value_size: 16 << 20,
        max_atomic_batch_bytes: 256 << 20,
        max_segment_size: 256 << 20,
        max_pending_bytes: 512 << 20,
        workers: 0,
        on_storage_error: None,
        on_replay_progress: None,
        compaction_threshold: 0.20,
        compaction_min_free_bytes: 0,
        tomb_proof_threshold: 1024,
        reclaim_interval: Some(Duration::from_secs(30)),
        data_key_max_age: None,
        faults: None,
    }
}

fn is_pow2(n: usize) -> bool {
    n > 0 && n & (n - 1) == 0
}

pub(crate) fn max_wrap_id_len() -> usize {
    128
}

impl Options {
    /// Fill unset fields with defaults and validate.
    pub(crate) fn normalized(&self) -> Result<NormalizedOptions> {
        let mut d = default_options(self.path.clone());
        d.path = self.path.clone();
        d.master_key = self.master_key.clone();
        d.passphrase = self.passphrase.clone();
        d.wrapping_key_id = self.wrapping_key_id.clone();
        d.context_id = self.context_id.clone();
        d.encryption = match self.encryption {
            Encryption::Default => Encryption::Aes256Gcm,
            e => e,
        };
        if let Some(c) = self.compression {
            d.compression = Some(c);
        }
        d.codec = self.codec.clone();
        d.codecs = self.codecs.clone();
        if let Some(c) = &d.codec {
            d.compression = Some(Compression(c.id()));
        }
        d.durability = self.durability;
        d.read_cache_bytes = self.read_cache_bytes;
        // FlushPolicy: zero selects the default; negative disables a
        // threshold (a huge MaxDelay approximates a disabled timer).
        if self.flush.max_records != 0 {
            d.flush.max_records = self.flush.max_records;
        }
        if self.flush.max_bytes != 0 {
            d.flush.max_bytes = self.flush.max_bytes;
        }
        if !self.flush.max_delay.is_zero() {
            d.flush.max_delay = self.flush.max_delay;
        }
        if self.write_shards != 0 {
            d.write_shards = self.write_shards;
        }
        if self.index_shards != 0 {
            d.index_shards = self.index_shards;
        }
        if self.target_block_bytes != 0 {
            d.target_block_bytes = self.target_block_bytes;
        }
        if self.max_block_bytes != 0 {
            d.max_block_bytes = self.max_block_bytes;
        }
        if self.max_records_per_block != 0 {
            d.max_records_per_block = self.max_records_per_block;
        }
        if self.max_key_size != 0 {
            d.max_key_size = self.max_key_size;
        }
        if self.max_value_size != 0 {
            d.max_value_size = self.max_value_size;
        }
        if self.max_atomic_batch_bytes != 0 {
            d.max_atomic_batch_bytes = self.max_atomic_batch_bytes;
        }
        if self.max_segment_size != 0 {
            d.max_segment_size = self.max_segment_size;
        }
        if self.max_pending_bytes != 0 {
            d.max_pending_bytes = self.max_pending_bytes;
        }
        if self.workers != 0 {
            d.workers = self.workers;
        }
        d.on_storage_error = self.on_storage_error.clone();
        d.on_replay_progress = self.on_replay_progress.clone();
        if self.compaction_threshold != 0.0 {
            d.compaction_threshold = self.compaction_threshold;
        }
        if self.compaction_min_free_bytes != 0 {
            d.compaction_min_free_bytes = self.compaction_min_free_bytes;
        }
        if self.tomb_proof_threshold != 0 {
            d.tomb_proof_threshold = self.tomb_proof_threshold;
        }
        // reclaim_interval: None disables; the zero-idiom differs from Go.
        // An explicit None here disables the worker (documented).
        d.reclaim_interval = self.reclaim_interval;
        d.data_key_max_age = self.data_key_max_age;
        d.faults = self.faults.clone();
        d.validate()?;
        Ok(NormalizedOptions(d))
    }

    /// User codecs to register (codec first).
    pub(crate) fn all_codecs(&self) -> Vec<Arc<dyn Codec>> {
        let mut out = Vec::new();
        if let Some(c) = &self.codec {
            out.push(c.clone());
        }
        out.extend(self.codecs.iter().cloned());
        out
    }

    fn validate(&self) -> Result<()> {
        if self.path.is_empty() {
            return Err(err(ErrorKind::Invalid, "Path is required"));
        }
        if !self.context_id.is_empty() && self.context_id.len() != 16 {
            return Err(err(
                ErrorKind::Invalid,
                format!("ContextID must be 16 bytes, got {}", self.context_id.len()),
            ));
        }
        if self.wrapping_key_id.len() > max_wrap_id_len() {
            return Err(err(
                ErrorKind::Invalid,
                format!(
                    "WrappingKeyID of {} bytes exceeds {}",
                    self.wrapping_key_id.len(),
                    max_wrap_id_len()
                ),
            ));
        }
        match self.encryption {
            Encryption::None => {
                if !self.master_key.is_empty() || !self.passphrase.is_empty() {
                    return Err(err(
                        ErrorKind::Invalid,
                        "key material must be empty with EncryptionNone",
                    ));
                }
                if !self.wrapping_key_id.is_empty() {
                    return Err(err(ErrorKind::Invalid, "WrappingKeyID requires encryption"));
                }
            }
            Encryption::Aes256Gcm => {
                if !self.master_key.is_empty() && !self.passphrase.is_empty() {
                    return Err(err(
                        ErrorKind::Invalid,
                        "set exactly one of MasterKey and Passphrase",
                    ));
                }
                if self.master_key.is_empty() && self.passphrase.is_empty() {
                    return Err(err(
                        ErrorKind::Invalid,
                        "MasterKey or Passphrase is required unless EncryptionNone",
                    ));
                }
                if !self.master_key.is_empty() && self.master_key.len() != 32 {
                    return Err(err(
                        ErrorKind::Invalid,
                        format!("MasterKey must be 32 bytes, got {}", self.master_key.len()),
                    ));
                }
            }
            Encryption::Default => {
                return Err(err(
                    ErrorKind::Invalid,
                    "unknown Encryption default (internal)",
                ));
            }
        }
        crate::compression::new_registry(self.all_codecs()).map(|_| ())?;
        if !is_pow2(self.write_shards) {
            return Err(err(
                ErrorKind::Invalid,
                format!(
                    "WriteShards must be a power of two, got {}",
                    self.write_shards
                ),
            ));
        }
        if !is_pow2(self.index_shards) {
            return Err(err(
                ErrorKind::Invalid,
                format!(
                    "IndexShards must be a power of two, got {}",
                    self.index_shards
                ),
            ));
        }
        if self.target_block_bytes == 0 {
            return Err(err(ErrorKind::Invalid, "TargetBlockBytes must be positive"));
        }
        if self.max_block_bytes < self.target_block_bytes {
            return Err(err(
                ErrorKind::Invalid,
                "MaxBlockBytes must cover TargetBlockBytes",
            ));
        }
        if self.max_block_bytes as u64 > crate::block::ABS_MAX_BLOCK_BYTES {
            return Err(err(
                ErrorKind::Invalid,
                "MaxBlockBytes exceeds the absolute decode cap",
            ));
        }
        if self.max_records_per_block == 0 {
            return Err(err(
                ErrorKind::Invalid,
                "MaxRecordsPerBlock must be positive",
            ));
        }
        if self.max_key_size == 0 || self.max_value_size == 0 {
            return Err(err(
                ErrorKind::Invalid,
                "MaxKeySize and MaxValueSize must be positive",
            ));
        }
        // A record must fit into one block with room for framing.
        if self.max_key_size as u64
            + self.max_value_size as u64
            + crate::record::RECORD_OVERHEAD as u64
            > self.max_block_bytes as u64
        {
            return Err(err(
                ErrorKind::Invalid,
                "MaxKeySize+MaxValueSize must fit into MaxBlockBytes",
            ));
        }
        if self.max_atomic_batch_bytes < self.max_block_bytes as i64 {
            return Err(err(
                ErrorKind::Invalid,
                "MaxAtomicBatchBytes must cover MaxBlockBytes",
            ));
        }
        if self.max_segment_size <= self.max_block_bytes as i64 {
            return Err(err(
                ErrorKind::Invalid,
                "MaxSegmentSize must exceed MaxBlockBytes",
            ));
        }
        if self.max_pending_bytes <= 0 {
            return Err(err(ErrorKind::Invalid, "MaxPendingBytes must be positive"));
        }
        if !(self.compaction_threshold > 0.0 && self.compaction_threshold <= 1.0) {
            return Err(err(
                ErrorKind::Invalid,
                "CompactionThreshold must be in (0,1]",
            ));
        }
        if self.compaction_min_free_bytes < 0 {
            return Err(err(
                ErrorKind::Invalid,
                "CompactionMinFreeBytes must be non-negative",
            ));
        }
        Ok(())
    }
}

/// Validated options. `Deref`s to [`Options`].
#[derive(Clone)]
pub(crate) struct NormalizedOptions(pub Options);

impl std::ops::Deref for NormalizedOptions {
    type Target = Options;
    fn deref(&self) -> &Options {
        &self.0
    }
}
