use crate::{Error, Result, backup, merge, metadata, model::*, schema};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use spool::{Codec, Durability, Mutation, Store};
use std::{
    path::Path,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};
const FORMAT: u32 = 8;
fn supported_format(format: u32) -> bool { matches!(format, 6 | 7 | FORMAT) }
const CHUNK: usize = 64 << 10;
const MAX_PAYLOAD: usize = 512 << 20;
struct Zstd(i32);
impl Codec for Zstd {
    fn id(&self) -> u8 {
        128
    }
    fn name(&self) -> &str {
        "sqlite-session-zstd-v1"
    }
    fn compress(&self, b: &[u8]) -> spool::Result<Vec<u8>> {
        zstd::stream::encode_all(b, self.0)
            .map_err(|e| spool::Error::new(spool::ErrorKind::Io, e.to_string()))
    }
    fn decompress(&self, b: &[u8], max: usize) -> spool::Result<Vec<u8>> {
        // Spool supplies the authenticated frame's bounded plaintext size.
        // Decode directly into that capacity instead of repeatedly growing a
        // streaming output buffer for every 64 KiB point read.
        zstd::bulk::decompress(b, max)
            .map_err(|e| spool::Error::new(spool::ErrorKind::Corrupt, e.to_string()))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Event {
    Delta {
        bytes: Vec<u8>,
        version: Version,
        intents: Vec<PolicyIntent>,
    },
    SchemaCaptured {
        migration: String,
        schema: Schema,
        before: Vec<RowState>,
        after: Vec<RowState>,
        version: Version,
    },
    Schema {
        migration: String,
        schema: Schema,
        version: Version,
    },
    Replica(ReplicaState),
    ReplicaPatch {rows:Vec<RowState>,deleted:Vec<RowState>,tombstones:Vec<Tombstone>,clock:Version,records:Vec<(Vec<u8>,Option<Vec<u8>>)>,remint:bool},
    ReplicaRecords { state: ReplicaState, records: Vec<(Vec<u8>, Option<Vec<u8>>) > },
    TableReplication { table: String, enabled: bool, schema: Schema, before: Vec<RowState>, after: Vec<RowState>, version: Version },
    MigrationChunk {
        before: Vec<RowState>,
        after: Vec<RowState>,
        version: Version,
    },
}
#[derive(Debug, Serialize, Deserialize)]
struct Entry {
    format: u32,
    sequence: u64,
    cursor: u64,
    event: Event,
}
#[derive(Debug, Serialize, Deserialize)]
struct Snapshot {
    format: u32,
    sequence: u64,
    cursor: u64,
    history: Vec<Event>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Head {
    sequence: u64,
    cursor: u64,
    chunks: usize,
    hash: [u8; 32],
}
#[derive(Debug, Serialize, Deserialize)]
struct ChunkHeader {
    format: u32,
    chunks: usize,
    len: usize,
    hash: [u8; 32],
}
pub(crate) enum Work {
    Checkpoint {
        event: Event,
        cursor: u64,
        bytes: usize,
    },
    Network(Box<dyn FnOnce(&mut Opened) + Send>),
    Export {
        rows: Vec<RowState>,
        replicated_only: bool,
        reply: mpsc::Sender<Result<ReplicaState>>,
    },
    Merge {
        baseline: ReplicaState,
        cursor: u64,
        state: ReplicaState,
        reply: mpsc::Sender<Result<merge::Patch>>,
    },
    Backup {
        destination: std::path::PathBuf,
        reply: mpsc::Sender<Result<()>>,
    },
    Barrier(mpsc::Sender<Result<()>>),
    Retry(mpsc::Sender<Result<()>>),
    Rotate(mpsc::Sender<Result<()>>),
    RotateMasterKey {
        key: Vec<u8>,
        reply: mpsc::Sender<Result<()>>,
    },
    Stop(mpsc::Sender<Result<()>>),
}
pub(crate) struct Opened {
    pub schema_version: u64,
    pub snapshot_history: Option<(u64, Vec<Event>)>,
    pub last_reclaim: Option<Instant>,
    pub store: crate::logical_store::LogicalStore,
    #[cfg(feature = "replication")]
    pub replication_config: Option<murmur_replication::Config>,
    pub history: Vec<Event>,
    pub clock: Version,
    pub schema: Schema,
    pub identity: Identity,
    pub cursor: u64,
    pub sequence: u64,
    pub keys: Vec<Vec<u8>>,
    pub opts: spool::Options,
}
impl Drop for Opened {
    fn drop(&mut self) {
        let _ = self.store.close();
        zeroize::Zeroize::zeroize(&mut self.opts.master_key);
    }
}
fn spool_options(path: &Path, key: &[u8], o: &Options) -> spool::Options {
    let mut opts = spool::default_options(path.to_string_lossy());
    opts.master_key = key.to_vec();
    opts.target_block_bytes = o.spool_block_bytes;
    opts.read_cache_bytes = o.spool_cache_bytes;
    opts.codec = Some(Arc::new(Zstd(o.compression_level)));
    opts.reclaim_interval = None;
    opts.workers = o.storage_workers;
    opts.data_key_max_age = o.data_key_max_age;
    opts.durability = Durability::Sync;
    opts.max_segment_size = o.segment_bytes as i64;
    opts.max_atomic_batch_bytes = 768 << 20;
    opts.max_pending_bytes = 768 << 20;
    opts.faults = o.fault_hooks.clone();
    opts
}
fn payload_records(prefix: &str, payload: &[u8]) -> Result<Vec<Mutation>> {
    if payload.len() > MAX_PAYLOAD {
        return Err(Error::Limit("serialized record exceeds 512 MiB".into()));
    }
    let chunks = payload.len().div_ceil(CHUNK).max(1);
    let header = ChunkHeader {
        format: FORMAT,
        chunks,
        len: payload.len(),
        hash: *blake3::hash(payload).as_bytes(),
    };
    let mut records = vec![Mutation::put(
        format!("{prefix}/00000000").into_bytes(),
        encode(&header)?,
    )];
    for (i, b) in payload.chunks(CHUNK).enumerate() {
        records.push(Mutation::put(
            format!("{prefix}/{:08}", i + 1).into_bytes(),
            b.to_vec(),
        ));
    }
    Ok(records)
}
struct Assembly {
    header: ChunkHeader,
    payload: Vec<u8>,
    seen: usize,
    prefix: String,
    chunk_bytes: usize,
}
impl Assembly {
    fn start(prefix: &str, value: &[u8]) -> Result<Self> {
        let header: ChunkHeader = decode(value)?;
        if !supported_format(header.format)
            || header.len > MAX_PAYLOAD
            || (header.chunks != header.len.div_ceil(CHUNK).max(1)
                && header.chunks != header.len.div_ceil(4 << 20).max(1))
        {
            return Err(Error::Corrupt("invalid chunk descriptor".into()));
        }
        let chunk_bytes = if header.chunks == header.len.div_ceil(CHUNK).max(1) { CHUNK } else { 4 << 20 };
        Ok(Self {
            chunk_bytes,
            header,
            payload: Vec::new(),
            seen: 0,
            prefix: prefix.into(),
        })
    }
    fn push(&mut self, key: &str, value: &[u8]) -> Result<bool> {
        self.seen += 1;
        if key != format!("{}/{:08}", self.prefix, self.seen) || value.len() > self.chunk_bytes {
            return Err(Error::Corrupt("chunk order".into()));
        }
        self.payload.extend_from_slice(value);
        if self.payload.len() > self.header.len {
            return Err(Error::Corrupt("chunk length".into()));
        }
        Ok(self.seen == self.header.chunks)
    }
    fn finish(self) -> Result<Vec<u8>> {
        if self.seen != self.header.chunks
            || self.payload.len() != self.header.len
            || *blake3::hash(&self.payload).as_bytes() != self.header.hash
        {
            return Err(Error::Corrupt("incomplete or damaged record".into()));
        }
        Ok(self.payload)
    }
}
/// Format six recovers winning logical values, rather than applying raw
/// changesets against a baseline that may have changed through replication.
pub(crate) fn recover_current(opened: &Opened, progress: &crate::progress::OpenProgressReporter) -> Result<Connection> {
    progress.phase(OpenPhase::Rebuilding);
    let c = Connection::open_in_memory()?;
    crate::primary_key::register(&c)?;
    c.execute_batch("BEGIN")?;
    for table in &opened.schema.tables { c.execute_batch(&table.sql)?; }
    for table in &opened.schema.tables {
        let keys = opened.store.keys_with_prefix(format!("04/meta/row/{}/", table.id).as_bytes());
        if keys.is_empty() { continue; }
        progress.set_table(&table.name);
        let mut statement = c.prepare(&schema::row_insert_sql(table))?;
        for key in keys {
            let bytes = opened.store.get(&key)?.ok_or_else(|| Error::Corrupt("missing row record".into()))?;
            let meta: metadata::RowMeta = decode(&bytes)?;
            let primary_key: Vec<Value> = decode(&meta.key)?;
            if metadata::row_key(&table.id, &encode(&primary_key)?) != key {
                return Err(Error::Corrupt("metadata key collision".into()));
            }
            // These are already the authoritative winning rows. Rebuilding SQL
            // needs neither a second version lookup nor historical tombstones.
            let values = schema::cell_values(table, &meta.cells)?;
            statement.execute(rusqlite::params_from_iter(values))?;
            progress.add_rows(1, 0);
        }
    }
    progress.phase(OpenPhase::Indexing);
    for sql in &opened.schema.indexes { c.execute_batch(sql)?; }
    c.execute_batch("COMMIT")?;
    let check: String = c.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    if check != "ok" { return Err(Error::Corrupt(check)); }
    Ok(c)
}
pub(crate) fn open(path: &Path, key: &[u8], o: &Options, progress: &crate::progress::OpenProgressReporter) -> Result<Opened> {
    progress.phase(OpenPhase::Opening);
    if key.len() != 32 {
        return Err(Error::Invalid("application key must be 32 bytes".into()));
    }
    match o.mode {
        OpenMode::CreateNew => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::create_dir(path)?;
        }
        OpenMode::Existing => {
            if !path.join("manifest").is_file() {
                return Err(Error::Invalid("database storage does not exist".into()));
            }
            spool::validate_wrapping_key(path, key, &[], "").map_err(key_error)?;
        }
    }
    let opts = spool_options(path, key, o);
    let mut history = Vec::new();
    let mut s = Schema::default();
    let mut identity = None;
    let mut head: Option<Head> = None;
    let mut assembly: Option<Assembly> = None;
    let mut cursor = 0;
    let mut sequence = 0;
    let mut keys = Vec::new();
    let mut snapshot_loaded = false;
    let result = spool::open_and_load_ordered(&opts, &mut |records| {
        progress.add_processed_items(records.len() as u64);
        let inner = (|| -> Result<()> {
            for r in records {
                if r.deleted {
                    continue;
                }
                let key = std::str::from_utf8(&r.key)
                    .map_err(|_| Error::Corrupt("storage key".into()))?;
                if key == "00/identity" {
                    let i: Identity = decode(&r.value)?;
                    if !supported_format(i.format) {
                        return Err(Error::Corrupt("unsupported database format".into()));
                    }
                    identity = Some(i);
                } else if key == "00/schema/replication" || key == "00/schema/table-flags-v2" || key == "00/schema/config-owned" {
                    // Loaded after replay so later schema events cannot overwrite it.
                } else if key == "01/snapshot/head" {
                    head = Some(decode(&r.value)?);
                } else if key.starts_with("02/snapshot/") {
                    keys.push(r.key.clone());
                    let Some(h) = &head else {
                        // An unpublished snapshot is harmless; replay its log.
                        continue;
                    };
                    let prefix = format!("02/snapshot/{:020}", h.sequence);
                    if !key.starts_with(&format!("{prefix}/")) {
                        continue;
                    }
                    if key.ends_with("/00000000") {
                        assembly = Some(Assembly::start(&prefix, &r.value)?);
                    } else {
                        let a = assembly
                            .as_mut()
                            .ok_or_else(|| Error::Corrupt("snapshot descriptor missing".into()))?;
                        if a.push(key, &r.value)? {
                            let data = assembly.take().unwrap().finish()?;
                            if *blake3::hash(&data).as_bytes() != h.hash {
                                return Err(Error::Corrupt("snapshot head hash".into()));
                            }
                            let snapshot: Snapshot = decode(&data)?;
                            if !supported_format(snapshot.format)
                                || snapshot.sequence != h.sequence
                                || snapshot.cursor != h.cursor
                            {
                                return Err(Error::Corrupt("snapshot boundary".into()));
                            }
                            history = snapshot.history;
                            s = history
                                .iter()
                                .filter_map(|e| match e {
                                    Event::Schema { schema, .. } => Some(schema.clone()),
                                    _ => None,
                                })
                                .next_back()
                                .unwrap_or_default();
                            cursor = h.cursor;
                            sequence = h.sequence;
                            snapshot_loaded = true;
                        }
                    }
                } else if key.starts_with("03/log/") {
                    keys.push(r.key.clone());
                    if head.is_some() && !snapshot_loaded {
                        return Err(Error::Corrupt("snapshot incomplete".into()));
                    }
                    let log_seq = key
                        .split('/')
                        .nth(2)
                        .and_then(|n| n.parse::<u64>().ok())
                        .ok_or_else(|| Error::Corrupt("log key".into()))?;
                    if log_seq <= sequence {
                        continue;
                    }
                    let prefix = format!("03/log/{log_seq:020}");
                    if key.ends_with("/00000000") {
                        if assembly.is_some() {
                            return Err(Error::Corrupt("incomplete log".into()));
                        }
                        assembly = Some(Assembly::start(&prefix, &r.value)?);
                    } else {
                        let a = assembly
                            .as_mut()
                            .ok_or_else(|| Error::Corrupt("log descriptor missing".into()))?;
                        if a.push(key, &r.value)? {
                            let e: Entry = decode(&assembly.take().unwrap().finish()?)?;
                            if !supported_format(e.format)
                                || e.sequence != sequence + 1
                                || e.sequence != log_seq
                                || e.cursor < cursor
                            {
                                return Err(Error::Corrupt("log sequence gap".into()));
                            }
                            if let Event::Schema { schema, .. } = &e.event {
                                s = schema.clone();
                            }
                            history.push(e.event);
                            cursor = e.cursor;
                            sequence = e.sequence;
                        }
                    }
                } else if key.starts_with("04/meta/row/")
                    || key.starts_with("05/meta/tomb/")
                    || key == "06/clock"
                    || key.starts_with("07/replication/")
                    || key.starts_with("08/files/")
                    || key.starts_with("09/bridge/")
                    || key.starts_with("10/policy/")
                    || key.starts_with("11/blob/")
                    || key.starts_with("12/key-transition/")
                    || key.starts_with("05/meta/deleted/")
                {
                    // Authoritative metadata stays in Spool; do not load its values into Rust.
                } else {
                    return Err(Error::Corrupt("unknown storage record".into()));
                }
            }
            Ok(())
        })();
        inner.map_err(|e| spool::Error::new(spool::ErrorKind::Corrupt, e.to_string()))
    });
    let store = result.map_err(key_error)?;
    if assembly.is_some() || (head.is_some() && !snapshot_loaded) {
        let _ = store.close();
        return Err(Error::Corrupt("incomplete record".into()));
    }
    let mut identity = match identity {
        Some(i) => i,
        None if o.mode == OpenMode::CreateNew => {
            let i = Identity {
                database: o.database_id.unwrap_or_else(rand::random),
                node: o.node_id.unwrap_or_else(rand::random),
                format: FORMAT,
            };
            store.commit(
                &[Mutation::put(b"00/identity".to_vec(), encode(&i)?)],
                Durability::Sync,
            )?;
            i
        }
        None => {
            let _ = store.close();
            return Err(Error::Corrupt("database identity missing".into()));
        }
    };
    if o.database_id.is_some_and(|id| id != identity.database)
        || o.node_id.is_some_and(|id| id != identity.node)
    {
        let _ = store.close();
        return Err(Error::Invalid("database/node identity mismatch".into()));
    }
    // Mark the store before writing any extended schemas. Older binaries must
    // reject it rather than interpreting schema extensions as application SQL.
    if identity.format != FORMAT {
        identity.format = FORMAT;
        store.commit(&[Mutation::put(b"00/identity".to_vec(), encode(&identity)?)], Durability::Sync)?;
    }
    let clock = store
        .get(b"06/clock")?
        .map(|b| decode(&b))
        .transpose()?
        .unwrap_or_default();
    s.table_replication = store.get(crate::table_replication::SETTINGS_KEY)?.or(store.get(crate::table_replication::LEGACY_SETTINGS_KEY)?)
        .map(|bytes| decode(&bytes)).transpose()?.unwrap_or_default();
    Ok(Opened {
        schema_version: 0,
        snapshot_history: None,
        last_reclaim: None,
        store: crate::logical_store::LogicalStore::new(store),
        #[cfg(feature = "replication")]
        replication_config: o.replication.clone(),
        history,
        clock,
        schema: s,
        identity,
        cursor,
        sequence,
        keys,
        opts,
    })
}
fn key_error(e: spool::Error) -> Error {
    match e.kind() {
        spool::ErrorKind::WrongKey => Error::WrongKey,
        spool::ErrorKind::AuthFailed | spool::ErrorKind::Corrupt => Error::Corrupt(e.to_string()),
        _ => Error::Storage(e.to_string()),
    }
}
#[derive(Default)]
struct SnapshotProgress {
    count: usize,
    total: usize,
}
fn persist_checkpoint(
    opened: &mut Opened,
    o: &Options,
    status: &Arc<Mutex<Status>>,
    event: Event,
    cursor: u64,
    bytes: usize,
    progress: &mut SnapshotProgress,
) -> Result<()> {
    #[cfg(feature = "replication")]
    let enabled_table = match &event { Event::TableReplication {table, enabled:true, ..} => Some(table.clone()), _ => None };
    let local = matches!(&event, Event::Delta { .. } | Event::SchemaCaptured { .. } | Event::ReplicaPatch {remint:true,..} | Event::TableReplication {enabled:true,..} | Event::MigrationChunk { .. });
    let (event, mut metadata_changes, clock) = materialize(opened, event)?;
    #[cfg(feature = "replication")]
    if local {
        if let Some(config) = &o.replication {
            let schema = match &event { Event::Schema {schema,..} => schema, _ => &opened.schema };
            let delta = if let Some(table) = &enabled_table {
                let mut state = metadata::changed_state(&metadata_changes, schema.clone(), opened.identity.database, clock)?;
                state.rows.retain(|r| &r.table == table);
                state.tombstones.retain(|r| &r.table == table);
                state
            } else { metadata::changed_state(&metadata_changes, schema.clone(), opened.identity.database, clock)? };
            let mut auxiliary: Vec<murmur_replication::Record>=metadata_changes.iter().filter(|r|!r.deleted && (r.key.starts_with(b"10/policy/") || r.key.starts_with(b"09/bridge/owner/") || r.key.starts_with(b"09/bridge/shadow/") || r.key.starts_with(b"09/bridge/provenance/") || r.key.starts_with(b"08/files/meta/"))).map(|r|murmur_replication::Record::put(r.key.clone(),r.value.clone())).collect();
            if let Some(table) = &enabled_table {
                for prefix in [format!("10/policy/state/{table}/"), format!("09/bridge/owner/{table}/")] {
                    for key in opened.store.keys_with_prefix(prefix.as_bytes()) {
                        if let Some(value) = opened.store.get(&key)? { auxiliary.push(murmur_replication::Record::put(key, value)); }
                    }
                }
                for key in opened.store.keys_with_prefix(b"09/bridge/shadow/") {
                    if std::str::from_utf8(&key).ok().and_then(|key| key.split('/').nth(3)) == Some(table.as_str()) {
                        if let Some(value) = opened.store.get(&key)? { auxiliary.push(murmur_replication::Record::put(key, value)); }
                    }
                }
            }
            let records = crate::network::sign_local(opened, config, delta, auxiliary)?;
            metadata_changes.extend(records);
        }
    }
    #[cfg(not(feature = "replication"))]
    let _ = local;
    let next = opened
        .sequence
        .checked_add(1)
        .ok_or_else(|| Error::Corrupt("sequence exhausted".into()))?;
    let entry = Entry {
        format: FORMAT,
        sequence: next,
        cursor,
        event,
    };
    if let Event::Schema {schema, ..} = &entry.event {
        metadata_changes.push(Mutation::put(crate::table_replication::SETTINGS_KEY.to_vec(), encode(&schema.table_replication)?));
    }
    metadata::summarize_tombs(&opened.store, &mut metadata_changes)?;
    let payload = encode(&entry)?;
    let mut records = payload_records(&format!("03/log/{next:020}"), &payload)?;
    let log_keys = records.iter().map(|r| r.key.clone()).collect::<Vec<_>>();
    records.append(&mut metadata_changes);
    records.push(Mutation::put(b"06/clock".to_vec(), encode(&clock)?));
    opened.store.commit(&records, Durability::Sync)?;
    if let Event::Schema { schema, .. } = &entry.event {
        opened.schema = schema.clone();
        opened.schema_version = opened.schema_version.wrapping_add(1);
    }
    opened.clock = clock;
    opened.keys.extend(log_keys);
    opened.sequence = next;
    opened.cursor = cursor;
    progress.count += 1;
    progress.total += payload.len();
    {
        let mut s = status.lock().unwrap();
        s.durable = cursor;
        s.pending_bytes = s.pending_bytes.saturating_sub(bytes);
    }
    if progress.count >= o.snapshot_deltas || progress.total >= o.snapshot_bytes {
        snapshot(opened)?;
        *progress = SnapshotProgress::default();
    }
    Ok(())
}
pub(crate) fn start(
    mut opened: Opened,
    o: Options,
    status: Arc<Mutex<Status>>,
) -> Result<(Store, mpsc::Sender<Work>, thread::JoinHandle<()>)> {
    let store = opened.store.physical();
    let (tx, rx) = mpsc::channel();
    let handle = thread::Builder::new()
        .name("sqlite-checkpoints".into())
        .spawn(move || {
            let mut progress = SnapshotProgress::default();
            let mut failed: Option<String> = None;
            let mut retained: std::collections::VecDeque<(Event, u64, usize)> =
                std::collections::VecDeque::new();
            while let Ok(work) = rx.recv() {
                match work {
                    Work::Network(job) => job(&mut opened),
                    Work::Checkpoint {
                        event,
                        cursor,
                        bytes,
                    } => {
                        if failed.is_some() {
                            retained.push_back((event, cursor, bytes));
                            continue;
                        }
                        let result = persist_checkpoint(
                            &mut opened,
                            &o,
                            &status,
                            event.clone(),
                            cursor,
                            bytes,
                            &mut progress,
                        );
                        if let Err(e) = result {
                            if cursor > opened.cursor {
                                retained.push_back((event, cursor, bytes));
                            }
                            let message = e.to_string();
                            status.lock().unwrap().storage_error = Some(message.clone());
                            failed = Some(message);
                        }
                    }
                    Work::Retry(reply) => {
                        let result = (|| -> Result<()> {
                            if failed.is_none() {
                                return Ok(());
                            }
                            let _ = opened.store.close();
                            let mut reopen = o.clone();
                            reopen.mode = OpenMode::Existing;
                            let mut replacement = open(
                                Path::new(&opened.opts.path),
                                &opened.opts.master_key,
                                &reopen,
                                &crate::progress::OpenProgressReporter::new(None),
                            )?;
                            replacement.history.clear();
                            zeroize::Zeroize::zeroize(&mut opened.opts.master_key);
                            std::mem::swap(&mut opened, &mut replacement);
                            zeroize::Zeroize::zeroize(&mut replacement.opts.master_key);
                            status.lock().unwrap().durable = opened.cursor;
                            while let Some((event, cursor, bytes)) = retained.front().cloned() {
                                if cursor <= opened.cursor {
                                    {
                                        let mut status = status.lock().unwrap();
                                        status.pending_bytes =
                                            status.pending_bytes.saturating_sub(bytes);
                                    }
                                    retained.pop_front();
                                    continue;
                                }
                                let persist = persist_checkpoint(
                                    &mut opened,
                                    &o,
                                    &status,
                                    event,
                                    cursor,
                                    bytes,
                                    &mut progress,
                                );
                                if cursor <= opened.cursor {
                                    retained.pop_front();
                                }
                                persist?;
                            }
                            // Retry maintenance as well as checkpoint writes.
                            if progress.count >= o.snapshot_deltas
                                || progress.total >= o.snapshot_bytes
                            {
                                snapshot(&mut opened)?;
                                progress = SnapshotProgress::default();
                            }
                            status.lock().unwrap().storage_error = None;
                            failed = None;
                            Ok(())
                        })();
                        if let Err(e) = &result {
                            let message = e.to_string();
                            status.lock().unwrap().storage_error = Some(message.clone());
                            failed = Some(message);
                        }
                        let _ = reply.send(result);
                    }
                    Work::Rotate(reply) => {
                        let _ = reply.send(if let Some(e) = &failed {
                            Err(Error::Storage(e.clone()))
                        } else {
                            opened.store.rotate_key().map_err(Error::from)
                        });
                    }
                    Work::RotateMasterKey { mut key, reply } => {
                        let result = if let Some(e) = &failed {
                            Err(Error::Storage(e.clone()))
                        } else {
                            match opened.store.rotate_master_key(&key, "") {
                                Ok(()) => {
                                    zeroize::Zeroize::zeroize(&mut opened.opts.master_key);
                                    opened.opts.master_key = std::mem::take(&mut key);
                                    Ok(())
                                }
                                Err(e) => Err(Error::from(e)),
                            }
                        };
                        zeroize::Zeroize::zeroize(&mut key);
                        let _ = reply.send(result);
                    }
                    Work::Backup { destination, reply } => {
                        let result = if let Some(e) = &failed {
                            Err(Error::Storage(e.clone()))
                        } else {
                            backup_store(&opened, &destination)
                        };
                        let _ = reply.send(result);
                    }
                    Work::Barrier(reply) => {
                        let _ = reply.send(if let Some(e) = &failed {
                            Err(Error::Storage(e.clone()))
                        } else {
                            opened.store.flush().map_err(Error::from)
                        });
                    }
                    Work::Export { rows, replicated_only, reply } => {
                        let result = if let Some(e) = &failed {
                            Err(Error::Storage(e.clone()))
                        } else {
                            metadata::export(
                                &opened.store,
                                opened.schema.clone(),
                                opened.identity.database,
                                rows,
                                opened.clock,
                            ).map(|mut state| {
                                if replicated_only { crate::table_replication::filter(&mut state, &opened.schema); }
                                state
                            })
                        };
                        let _ = reply.send(result);
                    }
                    Work::Merge {
                        baseline,
                        cursor,
                        mut state,
                        reply,
                    } => {
                        let result = if let Some(e) = &failed {
                            Err(Error::Storage(e.clone()))
                        } else if cursor != opened.cursor {
                            Err(Error::Busy)
                        } else {
                            crate::table_replication::filter(&mut state, &opened.schema);
                            state.schema.table_replication = baseline.schema.table_replication.clone();
                            for local in &baseline.schema.tables {
                                if !crate::table_replication::enabled(&baseline.schema,&local.id) {
                                    if let Some(remote)=state.schema.tables.iter_mut().find(|t|t.id==local.id) {*remote=local.clone();}
                                    else {state.schema.tables.push(local.clone());}
                                }
                            }
                            state.schema.tables.sort_by(|a,b|a.name.cmp(&b.name));
                            merge::prepare(&baseline, &state, cursor)
                        };
                        let _ = reply.send(result);
                    }
                    Work::Stop(reply) => {
                        let result = opened.store.close().map_err(Error::from);
                        let _ = reply.send(if let Some(e) = failed {
                            Err(Error::Storage(e))
                        } else {
                            result
                        });
                        break;
                    }
                }
            }
            let _ = opened.store.close();
            zeroize::Zeroize::zeroize(&mut opened.opts.master_key);
        })?;
    Ok((store, tx, handle))
}

/// Minimum spacing between the physical reclaims piggybacked on storage
/// snapshots. Snapshots may run every second under retention pressure,
/// but segment compaction costs ~1s; obsolete keys are still deleted
/// every snapshot, only the segment rewrite is deferred.
const SNAPSHOT_RECLAIM_MIN_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) fn snapshot(o: &mut Opened) -> Result<()> {
    // The compacted history is just the latest schema event: when no
    // schema event landed since the last snapshot, reuse it instead of
    // re-reading thousands of log entries.
    let history = match &o.snapshot_history {
        Some((version, history)) if *version == o.schema_version => history.clone(),
        _ => {
            let history = compact_history(read_history(o)?)?;
            o.snapshot_history = Some((o.schema_version, history.clone()));
            history
        }
    };
    let snap = Snapshot {
        format: FORMAT,
        sequence: o.sequence,
        cursor: o.cursor,
        history,
    };
    let payload = encode(&snap)?;
    let prefix = format!("02/snapshot/{:020}", o.sequence);
    let records = payload_records(&prefix, &payload)?;
    o.store.commit(&records, Durability::Sync)?;
    let head = Head {
        sequence: o.sequence,
        cursor: o.cursor,
        chunks: records.len() - 1,
        hash: *blake3::hash(&payload).as_bytes(),
    };
    o.store.commit(
        &[Mutation::put(b"01/snapshot/head".to_vec(), encode(&head)?)],
        Durability::Sync,
    )?;
    // Head is durable before any covered record can be reclaimed.
    let current: std::collections::BTreeSet<_> = records.iter().map(|r| r.key.clone()).collect();
    let obsolete: Vec<_> = o
        .keys
        .iter()
        .filter(|k| !current.contains(*k))
        .cloned()
        .collect();
    for batch in obsolete.chunks(4096) {
        let deletes = batch
            .iter()
            .map(|k| Mutation::delete(k.clone()))
            .collect::<Vec<_>>();
        o.store.commit(&deletes, Durability::Sync)?;
    }
    o.keys = records.iter().map(|r| r.key.clone()).collect();
    if o.last_reclaim
        .map_or(true, |t| t.elapsed() >= SNAPSHOT_RECLAIM_MIN_INTERVAL)
    {
        o.store.reclaim()?;
        o.last_reclaim = Some(Instant::now());
    }
    Ok(())
}

/// Online backup via spool's consistent checkpoint. `dest` must be a new
/// directory. Same-filesystem destinations stay as hardlink checkpoints;
/// cross-filesystem destinations fall back to a temporary sibling checkpoint
/// copied to `dest`.
fn backup_store(opened: &Opened, dest: &Path) -> Result<()> {
    match opened.store.checkpoint(dest, None) {
        Ok(cp) => {
            let _ = cp.release();
            Ok(())
        }
        Err(e) => {
            let kind = e.kind();
            let message = e.to_string();
            if kind == spool::ErrorKind::Invalid {
                return Err(Error::Invalid(
                    "backup destination must be a new directory".into(),
                ));
            }
            let cross_filesystem =
                kind == spool::ErrorKind::Io && message.contains("link checkpoint segment");
            if !cross_filesystem || dest.exists() {
                return match kind {
                    spool::ErrorKind::Corrupt | spool::ErrorKind::AuthFailed => {
                        Err(Error::Corrupt(message))
                    }
                    _ => Err(Error::Storage(message)),
                };
            }
            backup_copy_fallback(opened, dest)
        }
    }
}

fn backup_copy_fallback(opened: &Opened, dest: &Path) -> Result<()> {
    let live = Path::new(&opened.opts.path);
    let parent = live
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    for _ in 0..8 {
        let tmp = parent.join(format!(
            ".backup-tmp-{}-{:016x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            rand::random::<u64>()
        ));
        if tmp.exists() {
            continue;
        }
        let cp = opened.store.checkpoint(&tmp, None).map_err(|e| {
            let message = e.to_string();
            match e.kind() {
                spool::ErrorKind::Invalid => {
                    Error::Invalid("backup destination must be a new directory".into())
                }
                spool::ErrorKind::Corrupt | spool::ErrorKind::AuthFailed => {
                    Error::Corrupt(message)
                }
                _ => Error::Storage(message),
            }
        })?;
        let result = backup::copy_checkpoint_files(&tmp, cp.files(), dest);
        let _ = cp.release();
        let _ = std::fs::remove_dir_all(&tmp);
        return result;
    }
    Err(Error::Invalid(
        "could not allocate backup temporary directory".into(),
    ))
}

// Build a single atomic Spool group from the collected changeset and its
// metadata updates. No SQLite connection is owned by this worker.
fn materialize(o: &Opened, event: Event) -> Result<(Event, Vec<Mutation>, Version)> {
    let (event, changes, version) = match event {
        Event::Delta { bytes, version, intents } => {
            let mut changes = metadata::delta(&o.store, &o.schema, &bytes, version)?;
            #[cfg(feature="replication")]
            {crate::high_low::promote(o,&mut changes,version)?;crate::policy::materialize(o, &mut changes, &intents, version)?;}
            (Event::Delta { bytes, version, intents }, changes, version)
        }
        Event::SchemaCaptured {
            migration,
            schema,
            before,
            after,
            version,
        } => {
            let changes = metadata::migration(&o.store, &before, &after, version)?;
            (
                Event::Schema {
                    migration,
                    schema,
                    version,
                },
                changes,
                version,
            )
        }
        Event::ReplicaPatch {rows,deleted,tombstones,clock,records,remint} => {
            #[cfg(feature="replication")]
            let records={let mut records=records;crate::network::monotonic_progress(o,&mut records)?;records};
            let mut changes=metadata::patch(&o.store,&rows,&deleted,&tombstones)?;
            changes.extend(records.iter().cloned().map(|(key,value)|match value{Some(v)=>Mutation::put(key,v),None=>Mutation::delete(key)}));
            (Event::ReplicaPatch{rows,deleted,tombstones,clock,records,remint},changes,clock)
        }
        Event::ReplicaRecords { state, records } => {
            let mut changes = metadata::replica(&o.store, &state)?;
            changes.extend(records.into_iter().map(|(key, value)| match value { Some(value) => Mutation::put(key, value), None => Mutation::delete(key) }));
            let version = state.clock;
            (Event::Replica(state), changes, version)
        }
        Event::Replica(state) => {
            let changes = metadata::replica(&o.store, &state)?;
            let version = state.clock;
            (Event::Replica(state), changes, version)
        }
        Event::TableReplication { table, enabled, schema, before, after, version } => {
            let changes = metadata::transition(&o.store, &table, enabled, &before, &after, version)?;
            (Event::Schema { migration: String::new(), schema, version }, changes, version)
        }
        Event::MigrationChunk {
            before,
            after,
            version,
        } => {
            let changes = metadata::migration(&o.store, &before, &after, version)?;
            (
                Event::MigrationChunk {
                    before: Vec::new(),
                    after: Vec::new(),
                    version,
                },
                changes,
                version,
            )
        }
        Event::Schema { .. } => {
            return Err(Error::Corrupt("schema input lacks captured rows".into()));
        }
    };
    Ok((event, changes, o.clock.max(version)))
}
fn read_payload(store: &crate::logical_store::LogicalStore, prefix: &str) -> Result<Vec<u8>> {
    let h = store
        .get(format!("{prefix}/00000000").as_bytes())?
        .ok_or_else(|| Error::Corrupt("missing record descriptor".into()))?;
    let mut assembly = Assembly::start(prefix, &h)?;
    for i in 1..=assembly.header.chunks {
        let key = format!("{prefix}/{i:08}");
        let value = store
            .get(key.as_bytes())?
            .ok_or_else(|| Error::Corrupt("missing record chunk".into()))?;
        assembly.push(&key, &value)?;
    }
    assembly.finish()
}
fn read_history(o: &Opened) -> Result<Vec<Event>> {
    let mut history = Vec::new();
    let mut covered = 0;
    if let Some(head) = o.store.get(b"01/snapshot/head")? {
        let head: Head = decode(&head)?;
        let snapshot: Snapshot = decode(&read_payload(
            &o.store,
            &format!("02/snapshot/{:020}", head.sequence),
        )?)?;
        history = snapshot.history;
        covered = head.sequence;
    }
    for key in o.store.keys_with_prefix(b"03/log/") {
        let key = std::str::from_utf8(&key).map_err(|_| Error::Corrupt("log key".into()))?;
        if !key.ends_with("/00000000") {
            continue;
        }
        let prefix = key.rsplit_once('/').unwrap().0;
        let sequence = prefix
            .rsplit_once('/')
            .unwrap()
            .1
            .parse::<u64>()
            .map_err(|_| Error::Corrupt("log sequence".into()))?;
        if sequence <= covered {
            continue;
        }
        let entry: Entry = decode(&read_payload(&o.store, prefix)?)?;
        history.push(entry.event);
    }
    Ok(history)
}
// Winning logical values are already durable Spool records. Format five only
// needs the latest schema manifest at a compacted recovery boundary; retained
// signed replication history has its own lifecycle and is not reclaimed here.
fn compact_history(history: Vec<Event>) -> Result<Vec<Event>> {
    Ok(history.into_iter().filter(|event|matches!(event,Event::Schema{..})).next_back().into_iter().collect())
}

#[cfg(test)]
mod recovery_codec_tests {
    use super::*;

    #[test]
    fn format_six_log_and_snapshot_upgrade_without_changing_schema_bytes() -> Result<()> {
        for (legacy_format, snapshot) in [(6, false), (6, true), (7, false), (7, true)] {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("legacy");
            let options = Options::default();
            let opts = spool_options(&path, &[9; 32], &options);
            let store = spool::open(&opts)?;
            let c = Connection::open_in_memory()?;
            c.execute_batch("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")?;
            let mut schema = schema::inspect(&c, &Schema::default(), &Migration::new("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)"))?;
            if legacy_format == 7 { schema.tables[0].columns.iter_mut().find(|c| c.name == "body").unwrap().fts = true; }
            let event = Event::Schema { migration: String::new(), schema, version: Version::default() };
            let (prefix, payload) = if snapshot {
                ("02/snapshot/00000000000000000001", encode(&Snapshot { format: legacy_format, sequence: 1, cursor: 1, history: vec![event] })?)
            } else {
                ("03/log/00000000000000000001", encode(&Entry { format: legacy_format, sequence: 1, cursor: 1, event })?)
            };
            let mut records = payload_records(prefix, &payload)?;
            let header = ChunkHeader { format: legacy_format, chunks: payload.len().div_ceil(CHUNK), len: payload.len(), hash: *blake3::hash(&payload).as_bytes() };
            records[0] = Mutation::put(format!("{prefix}/00000000").into_bytes(), encode(&header)?);
            records.push(Mutation::put(b"00/identity".to_vec(), encode(&Identity { database: [1;16], node: [2;16], format: legacy_format })?));
            if snapshot {
                records.push(Mutation::put(b"01/snapshot/head".to_vec(), encode(&Head { sequence: 1, cursor: 1, chunks: header.chunks, hash: header.hash })?));
            }
            store.commit(&records, Durability::Sync)?;
            store.close()?;
            let db = crate::Database::open(&path, [9;32], Options::default())?;
            assert_eq!(db.schema()?.tables[0].name, "docs");
            assert!(db.schema()?.tables[0].columns.iter().all(|c| c.vector.is_none()));
            assert_eq!(db.schema()?.tables[0].columns.iter().any(|c| c.fts), legacy_format == 7);
            db.set_column_fts("docs", "body", true)?;
            db.flush()?;
            db.close()?;
            let db = crate::Database::open(&path, [9;32], Options::default())?;
            assert!(db.schema()?.tables[0].columns.iter().any(|c| c.fts));
            db.close()?;
            let store = spool::open(&opts)?;
            let identity: Identity = decode(&store.get(b"00/identity")?.unwrap())?;
            assert_eq!(identity.format, FORMAT);
            store.close()?;
        }
        Ok(())
    }

    #[cfg(not(feature = "vector"))]
    #[test]
    fn feature_disabled_open_preserves_vector_declarations() -> Result<()> {
        let dir=tempfile::tempdir()?; let path=dir.path().join("vector-store");
        let opts=spool_options(&path, &[9;32], &Options::default());
        let store=spool::open(&opts)?;
        let c=Connection::open_in_memory()?;
        c.execute_batch("CREATE TABLE docs(id INTEGER PRIMARY KEY,embedding BLOB)")?;
        let mut schema=schema::inspect(&c,&Schema::default(),&Migration::new(""))?;
        schema.tables[0].columns.iter_mut().find(|c|c.name=="embedding").unwrap().vector=Some(crate::VectorColumnConfig{dimensions:2,distance:crate::DistanceMetric::L2});
        let payload=encode(&Entry{format:FORMAT,sequence:1,cursor:1,event:Event::Schema{migration:String::new(),schema,version:Version::default()}})?;
        let mut records=payload_records("03/log/00000000000000000001",&payload)?;
        records.push(Mutation::put(b"00/identity".to_vec(),encode(&Identity{database:[1;16],node:[2;16],format:FORMAT})?));
        store.commit(&records,Durability::Sync)?;store.close()?;
        match crate::Database::open(&path,[9;32],Options::default()) {
            Err(error)=>assert!(error.to_string().contains("vector Cargo feature"),"{error}"),
            Ok(db)=>{db.close()?;panic!("feature-disabled open accepted vector schema");}
        }
        let opened=open(&path,&[9;32],&Options::default(),&crate::progress::OpenProgressReporter::new(None))?;
        assert!(opened.schema.tables[0].columns.iter().any(|c|c.vector.is_some()));
        Ok(())
    }

    #[test]
    fn bounded_zstd_reads_preserve_existing_frames_and_reject_damage() {
        let codec = Zstd(3);
        for len in [0, 1, 64 << 10, 2 << 20] {
            let plain: Vec<u8> = (0..len).map(|n| (n % 251) as u8).collect();
            let frame = codec.compress(&plain).unwrap();
            assert_eq!(codec.decompress(&frame, len).unwrap(), plain);
            if len > 0 { assert!(codec.decompress(&frame, len - 1).is_err()); }
            assert!(codec.decompress(&frame[..frame.len()-1], len).is_err());
        }
        // Streaming encoders may write consecutive frames; old stores remain readable.
        let mut frames = codec.compress(b"first").unwrap();
        frames.extend(codec.compress(b"second").unwrap());
        assert_eq!(codec.decompress(&frames, 11).unwrap(), b"firstsecond");
        assert!(codec.decompress(&frames, 10).is_err());
        assert!(codec.decompress(&[0xff;16], 64 << 10).is_err());
    }
}
