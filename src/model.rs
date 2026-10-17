use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Total order: physical milliseconds, logical counter, node identity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Version {
    pub millis: u64,
    pub logical: u32,
    pub node: [u8; 16],
}
impl Version {
    pub(crate) fn next(self, node: [u8; 16]) -> Result<Self> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if now > self.millis {
            Ok(Self {
                millis: now,
                logical: 0,
                node,
            })
        } else if let Some(logical) = self.logical.checked_add(1) {
            Ok(Self {
                millis: self.millis,
                logical,
                node,
            })
        } else {
            Ok(Self {
                millis: self
                    .millis
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("clock exhausted".into()))?,
                logical: 0,
                node,
            })
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}
impl rusqlite::types::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, ValueRef};
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(n) => ValueRef::Integer(*n),
            Self::Real(n) => ValueRef::Real(*n),
            Self::Text(s) => ValueRef::Text(s.as_bytes()),
            Self::Blob(b) => ValueRef::Blob(b),
        }))
    }
}
impl Value {
    pub(crate) fn read(v: rusqlite::types::ValueRef<'_>) -> rusqlite::Result<Self> {
        use rusqlite::types::ValueRef;
        Ok(match v {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(n) => Self::Integer(n),
            ValueRef::Real(n) => Self::Real(n),
            ValueRef::Text(s) => Self::Text(
                std::str::from_utf8(s)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            s.len(),
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .into(),
            ),
            ValueRef::Blob(b) => Self::Blob(b.to_vec()),
        })
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergePolicy {#[default] Lww, Counter, Set, Min, Max}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum PolicyOperation {Counter(i64),Add(Value),Remove(Value),Min(Value),Max(Value)}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PolicyIntent {pub table:String,pub column:String,pub key:Vec<Value>,pub before:Value,pub operation:PolicyOperation,pub id:[u8;16]}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    /// Persistent vector-search declaration; index contents are disposable.
    #[serde(default)]
    pub vector: Option<crate::VectorColumnConfig>,
    /// Include this TEXT column in the table's managed FTS5 index.
    #[serde(default)]
    pub fts: bool,
    pub policy: MergePolicy,
    pub id: String,
    pub name: String,
    pub primary_key: usize,
    pub declared_type: String,
    pub not_null: bool,
}
/// Original application key retained while SQLite uses the managed UUID key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationKey {
    pub columns: Vec<String>,
    pub collations: Vec<String>,
    pub descending: Vec<bool>,
    pub without_rowid: bool,
    pub strict: bool,
    pub conflict: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Table {
    pub application_key: Option<ApplicationKey>,
    pub id: String,
    pub name: String,
    pub sql: String,
    pub columns: Vec<Column>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    /// Local table replication settings, keyed by stable table ID. Missing means
    /// local-only. Persisted alongside the schema in Spool, excluded from wire data.
    pub table_replication: std::collections::BTreeMap<String, bool>,
    pub tables: Vec<Table>,
    pub indexes: Vec<String>,
    pub history: [u8; 32],
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cell {
    pub column: String,
    pub value: Value,
    pub version: Version,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowState {
    pub table: String,
    pub key: Vec<Value>,
    pub generation: Version,
    pub cells: Vec<Cell>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    pub table: String,
    pub key: Vec<u8>,
    pub generation: Version,
    pub version: Version,
}
/// Portable logical state. Local node identity and persistence cursors are not merged.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicaState {
    pub database_id: [u8; 16],
    pub schema: Schema,
    pub rows: Vec<RowState>,
    pub tombstones: Vec<Tombstone>,
    pub clock: Version,
}
#[derive(Debug, Clone, Default)]
pub struct TableMapping {
    pub old_name: String,
    pub new_name: String,
    pub columns: Vec<(String, String)>,
}
#[derive(Debug, Clone)]
pub struct Migration {
    pub sql: String,
    pub mappings: Vec<TableMapping>,
}
impl Migration {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            mappings: Vec::new(),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    Existing,
    CreateNew,
}
#[derive(Clone)]
pub struct Options {
    /// Node-local, rebuildable search indexes over ordinary source tables.
    pub search_indexes: Vec<crate::SearchIndex>,
    /// Byte budget for disposable indexing work; overflow triggers a rebuild.
    pub search_queue_bytes: usize,
    /// Maximum rows processed per maintenance turn.
    pub search_batch_rows: usize,
    pub search_interval: Duration,
    #[cfg(feature = "replication")]
    pub replication: Option<murmur_replication::Config>,
    pub mode: OpenMode,
    pub capture_interval: Duration,
    pub capture_bytes: usize,
    pub queue_bytes: usize,
    pub transaction_bytes: usize,
    pub segment_bytes: usize,
    /// Target plaintext bytes per independently compressed/encrypted Spool block.
    pub spool_block_bytes: usize,
    /// Spool hot block cache budget; zero disables the cache.
    pub spool_cache_bytes: usize,
    pub snapshot_deltas: usize,
    pub snapshot_bytes: usize,
    pub compression_level: i32,
    pub storage_workers: usize,
    pub data_key_max_age: Option<Duration>,
    pub database_id: Option<[u8; 16]>,
    pub node_id: Option<[u8; 16]>,
    pub fault_hooks: Option<std::sync::Arc<spool::FaultHooks>>,
    pub on_open_progress: Option<std::sync::Arc<dyn Fn(OpenProgress) + Send + Sync>>,
    pub scheduler: crate::scheduler::SchedulerConfig,
}
impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Options");
        d.field("search_indexes", &self.search_indexes)
            .field("search_queue_bytes", &self.search_queue_bytes)
            .field("search_batch_rows", &self.search_batch_rows)
            .field("search_interval", &self.search_interval)
            .field("scheduler", &self.scheduler);
        #[cfg(feature = "replication")]
        d.field("replication", &self.replication);
        d.field("mode", &self.mode)
            .field("capture_interval", &self.capture_interval)
            .field("capture_bytes", &self.capture_bytes)
            .field("queue_bytes", &self.queue_bytes)
            .field("transaction_bytes", &self.transaction_bytes)
            .field("segment_bytes", &self.segment_bytes)
            .field("spool_block_bytes", &self.spool_block_bytes)
            .field("spool_cache_bytes", &self.spool_cache_bytes)
            .field("snapshot_deltas", &self.snapshot_deltas)
            .field("snapshot_bytes", &self.snapshot_bytes)
            .field("compression_level", &self.compression_level)
            .field("storage_workers", &self.storage_workers)
            .field("data_key_max_age", &self.data_key_max_age)
            .field("database_id", &self.database_id)
            .field("node_id", &self.node_id)
            .field("fault_hooks", &self.fault_hooks)
            .field("on_open_progress", &self.on_open_progress.as_ref().map(|_| "<callback>"))
            .finish()
    }
}
impl Default for Options {
    fn default() -> Self {
        Self {
            search_indexes: Vec::new(),
            search_queue_bytes: 8 << 20,
            search_batch_rows: 256,
            search_interval: Duration::from_millis(100),
            #[cfg(feature = "replication")]
            replication: None,
            mode: OpenMode::Existing,
            capture_interval: Duration::from_secs(1),
            capture_bytes: 8 << 20,
            queue_bytes: 64 << 20,
            transaction_bytes: 64 << 20,
            segment_bytes: 256 << 20,
            spool_block_bytes: 64 << 10,
            spool_cache_bytes: 32 << 20,
            snapshot_deltas: 128,
            snapshot_bytes: 256 << 20,
            compression_level: 3,
            storage_workers: 2,
            data_key_max_age: None,
            database_id: None,
            node_id: None,
            fault_hooks: None,
            on_open_progress: None,
            scheduler: crate::scheduler::SchedulerConfig::default(),
        }
    }
}
impl Options {
    pub(crate) fn validate(&self) -> Result<()> {
        self.scheduler.validate()?;
        crate::search::validate_options(self)?;
        #[cfg(feature = "replication")]
        if let Some(config) = &self.replication { config.validate().map_err(|e| Error::Invalid(e.to_string()))?; }
        if self.capture_interval.is_zero()
            || self.capture_bytes == 0
            || self.queue_bytes < self.capture_bytes
            || self.transaction_bytes > self.queue_bytes
            || self.transaction_bytes == 0
            || self.queue_bytes > 128 << 20
            || self.spool_block_bytes == 0
            || self.spool_block_bytes > 64 << 20
            || self.segment_bytes <= 64 << 20
            || self.snapshot_deltas == 0
            || self.snapshot_bytes == 0
            || self.storage_workers == 0
        {
            return Err(Error::Invalid(
                "invalid worker limits (queue maximum 128 MiB)".into(),
            ));
        }
        Ok(())
    }
}
/// Options for [`crate::Database::restore`].
#[derive(Debug, Clone, Default)]
pub struct RestoreOptions {
    /// Replace the database identity on restore. `None` preserves the backup's
    /// database id (clone/reseed as a new replica of the same database).
    pub new_database_id: Option<[u8; 16]>,
    /// Replace the node identity on restore. `None` preserves the backup's
    /// node id. Clones that will exchange state must use a distinct node id.
    pub new_node_id: Option<[u8; 16]>,
}

/// Startup or reload work phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpenPhase {
    Opening,
    Rebuilding,
    Indexing,
    Finalizing,
    Ready,
    Failed,
    Cancelled,
}

/// Point-in-time snapshot of reload or startup progress.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenProgress {
    pub phase: OpenPhase,
    pub elapsed: Duration,
    pub phase_elapsed: Duration,
    pub processed_items: u64,
    pub current_table: Option<String>,
    pub rows_inserted: u64,
    pub rows_skipped: u64,
    pub error: Option<String>,
}

/// In-memory SQLite resource and size diagnostics.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteMetrics {
    pub memory_used_bytes: i64,
    pub memory_highwater_bytes: i64,
    pub page_size: i64,
    pub page_count: i64,
    pub database_size_bytes: i64,
    pub freelist_pages: i64,
    pub schema_version: i64,
    pub table_count: usize,
    pub index_count: usize,
    pub total_rows: u64,
    pub session_memory_bytes: usize,
    pub session_changeset_bytes: usize,
}

/// Spool persistence storage engine metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SpoolMetrics {
    pub keys: u64,
    pub segments: u64,
    pub disk_bytes: u64,
    pub live_bytes: u64,
    pub live_records: u64,
    pub dead_records: u64,
    pub pending_records: u64,
    pub pending_bytes: u64,
    pub blocks_written: u64,
    pub bytes_written: u64,
    pub compression_ratio: f64,
    pub compactions: u64,
    pub compaction_bytes: u64,
    pub flushes: u64,
    pub flush_errors: u64,
    pub reclaims: u64,
    pub reclaim_errors: u64,
    pub commits: u64,
    pub syncs: u64,
    pub commit_nanos: u64,
    pub sync_nanos: u64,
    pub storage_failures: u64,
    pub index_bytes_estimate: u64,
}

impl From<spool::Stats> for SpoolMetrics {
    fn from(s: spool::Stats) -> Self {
        Self {
            keys: s.keys,
            segments: s.segments,
            disk_bytes: s.disk_bytes,
            live_bytes: s.live_bytes,
            live_records: s.live_records,
            dead_records: s.dead_records,
            pending_records: s.pending_records,
            pending_bytes: s.pending_bytes,
            blocks_written: s.blocks_written,
            bytes_written: s.bytes_written,
            compression_ratio: s.compression_ratio,
            compactions: s.compactions,
            compaction_bytes: s.compaction_bytes,
            flushes: s.flushes,
            flush_errors: s.flush_errors,
            reclaims: s.reclaims,
            reclaim_errors: s.reclaim_errors,
            commits: s.commits,
            syncs: s.syncs,
            commit_nanos: s.commit_nanos,
            sync_nanos: s.sync_nanos,
            storage_failures: s.storage_failures,
            index_bytes_estimate: s.index_bytes_estimate,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Status {
    pub search_indexes: Vec<crate::SearchIndexStatus>,
    pub committed: u64,
    pub durable: u64,
    pub pending_bytes: usize,
    pub storage_error: Option<String>,
    pub checkpoint_pause: Duration,
    pub closed: bool,
    pub open_progress: Option<OpenProgress>,
    pub sqlite: Option<SqliteMetrics>,
    pub spool: Option<SpoolMetrics>,
    pub scheduler: Option<crate::scheduler::SchedulerSnapshot>,
    #[cfg(feature = "replication")]
    pub replication: Option<murmur_replication::Status>,
}
pub(crate) fn encode<T: Serialize>(t: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(t).map_err(|e| Error::Corrupt(e.to_string()))
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T> {
    postcard::from_bytes(b).map_err(|e| Error::Corrupt(e.to_string()))
}
pub(crate) fn id(seed: &[u8]) -> String {
    blake3::hash(seed).to_hex().to_string()
}
pub(crate) fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Identity {
    pub database: [u8; 16],
    pub node: [u8; 16],
    pub format: u32,
}
