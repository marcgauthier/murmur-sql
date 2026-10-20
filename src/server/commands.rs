//! `MURMUR ...` administrative commands, intercepted before SQLite.
//!
//! - `MURMUR UNLOCK KEY '<hex-or-passphrase>'` opens (or creates) the
//!   database and unlocks the connection. A 64-hex-char key is used raw;
//!   anything else is hashed with BLAKE3 like `murmur-admin --passphrase`.
//! - `MURMUR FLUSH` waits for a durable checkpoint (the over-wire `flush()`).
//! - `MURMUR SET TABLE <name> REPLICATED true|false` (step 5) flips a
//!   per-table replication flag and persists it to `config.toml`.
//! - `MURMUR SHOW TABLES REPLICATION` (step 5) lists the flags.
//! - `MURMUR SET COLUMN <table> <column> FTS true|false` changes replicated
//!   column schema metadata; `MURMUR SHOW COLUMNS FTS` lists local index status.
//! - `MURMUR REFRESH INDEXES` collects source writes and refreshes local indexes.
//! - `MURMUR BACKUP SAVE|LIST|UPLOAD|DOWNLOAD` manages single-file backup
//!   archives in `[backups].dir` (see `backup.rs` for the format).
//! - `MURMUR RESTORE FROM '<name>' KEY '<secret>'` live-restores an archive,
//!   preserving the pre-restore data dir for the operator.
//! - `MURMUR ROTATE KEY '<previous>' '<new>'` rewraps the key registry
//!   under a new master key; future unlocks require it.
//! - `MURMUR PEER ADD|REMOVE|RETIRE|SYNC` manages replication peers.
//! - `MURMUR SHOW PEERS` lists connected peers; `MURMUR SHOW REPLICATION`
//!   reports the local replication summary.
//! - `MURMUR SHOW RETENTION` lists per-peer retention state;
//!   `MURMUR RETENTION COLLECT` runs a bounded collection pass.
//! - `MURMUR POLICY COUNTER_ADD|SET_ADD|SET_REMOVE|MIN|MAX` applies one
//!   CRDT operation to a single-TEXT-key row.
//! - `MURMUR BRIDGE RELEASE <table> '<key>' [COLUMN <column>]` releases High
//!   ownership of one bridge-shielded field (or the whole row without
//!   `COLUMN`), so later Low updates flow through again. Returns one
//!   `released` row: `true` when applied, `false` when the engine is busy
//!   (retry).
//!
//! Peer, retention, policy, and bridge commands need an unlocked connection
//! and a running replication handle; without the `replication` Cargo feature
//! (or with replication disabled) they fail with `55000`.
//!
//! Keys never appear in logs or error messages.

use crate::config::Config;
use crate::server::query::user_error;
use crate::server::ServerState;
use pgwire::error::PgWireResult;
use std::net::SocketAddr;

/// Output of a MURMUR command: rows or a bare tag.
pub(crate) enum CommandOutput {
    Tag(&'static str),
    Rows {
        columns: Vec<crate::server::query::ColInfo>,
        rows: Vec<Vec<murmur::Value>>,
    },
}

/// Execute one MURMUR command. Runs on a blocking thread; `config` is a
/// snapshot taken by the caller.
pub(crate) fn execute(
    state: &ServerState,
    config: &Config,
    addr: SocketAddr,
    user: &str,
    command: &str,
) -> PgWireResult<CommandOutput> {
    let mut words = tokenize(command)?;
    match words.pop_front() {
        Some(verb) if verb.eq_ignore_ascii_case("UNLOCK") => unlock(state, config, addr, user, words),
        Some(verb) if verb.eq_ignore_ascii_case("SET") => {
            if words.front().is_some_and(|w| w.eq_ignore_ascii_case("COLUMN")) { set_column_search(state, words) }
            else { set_table(state, words) }
        }
        Some(verb) if verb.eq_ignore_ascii_case("REFRESH") => {
            if words.len() != 1 || !words[0].eq_ignore_ascii_case("INDEXES") {
                return Err(user_error("42601", "usage: MURMUR REFRESH INDEXES".into()));
            }
            state.require_db()?.refresh_indexes().map_err(crate::server::query::engine_error)?;
            Ok(CommandOutput::Tag("REFRESH"))
        },
        Some(verb) if verb.eq_ignore_ascii_case("SHOW") => show(state, words),
        Some(verb) if verb.eq_ignore_ascii_case("FLUSH") => {
            if !words.is_empty() {
                return Err(user_error("42601", "usage: MURMUR FLUSH".to_string()));
            }
            let db = state.require_db()?;
            db.flush().map_err(crate::server::query::engine_error)?;
            Ok(CommandOutput::Tag("FLUSH"))
        }
        Some(verb) if verb.eq_ignore_ascii_case("BACKUP") => backup(state, config, words),
        Some(verb) if verb.eq_ignore_ascii_case("RESTORE") => restore(state, config, words),
        Some(verb) if verb.eq_ignore_ascii_case("ROTATE") => rotate(state, words),
        Some(verb) if verb.eq_ignore_ascii_case("PEER") => peer(state, words),
        Some(verb) if verb.eq_ignore_ascii_case("RETENTION") => retention(state, words),
        Some(verb) if verb.eq_ignore_ascii_case("POLICY") => policy(state, words),
        Some(verb) if verb.eq_ignore_ascii_case("BRIDGE") => bridge(state, words),
        Some(other) => Err(user_error(
            "42601",
            format!("unknown MURMUR command {other:?}"),
        )),
        None => Err(user_error("42601", "empty MURMUR command".to_string())),
    }
}

fn unlock(
    state: &ServerState,
    config: &Config,
    addr: SocketAddr,
    user: &str,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    match words.pop_front() {
        Some(word) if word.eq_ignore_ascii_case("KEY") => {}
        _ => {
            return Err(user_error(
                "42601",
                "usage: MURMUR UNLOCK KEY '<hex-or-passphrase>'".to_string(),
            ));
        }
    }
    let Some(secret) = words.pop_front() else {
        return Err(user_error(
            "42601",
            "usage: MURMUR UNLOCK KEY '<hex-or-passphrase>'".to_string(),
        ));
    };
    if !words.is_empty() {
        return Err(user_error("42601", "unexpected trailing words in MURMUR UNLOCK".to_string()));
    }
    if secret.is_empty() {
        return Err(user_error("28000", "MURMUR UNLOCK needs a non-empty key".to_string()));
    }
    let key = secret_to_key(&secret);
    // The key is verified or rejected without ever being logged.
    match state.unlock(config, &key) {
        Ok(outcome) => {
            state.mark_unlocked(addr, user);
            tracing::info!(user = %user, %addr, ?outcome, "connection unlocked");
            Ok(CommandOutput::Tag("UNLOCK"))
        }
        Err(murmur::Error::WrongKey) => Err(user_error("28000", "incorrect key".to_string())),
        Err(e) => Err(crate::server::query::engine_error(e)),
    }
}

/// `MURMUR SET TABLE <name> REPLICATED true|false`: resolve the name to its
/// stable id, persist the flag to `config.toml`, then update memory and the
/// shared config snapshot. The file is written before memory changes so a
/// failed save leaves both consistent.
fn set_table(
    state: &ServerState,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    let usage = "usage: MURMUR SET TABLE <name> REPLICATED true|false";
    match words.pop_front() {
        Some(word) if word.eq_ignore_ascii_case("TABLE") => {}
        _ => return Err(user_error("42601", usage.to_string())),
    }
    let name = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    match words.pop_front() {
        Some(word) if word.eq_ignore_ascii_case("REPLICATED") => {}
        _ => return Err(user_error("42601", usage.to_string())),
    }
    let value = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    if !words.is_empty() {
        return Err(user_error("42601", usage.to_string()));
    }
    let replicated = match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "on" | "yes" => true,
        "false" | "0" | "off" | "no" => false,
        _ => return Err(user_error("22023", format!("REPLICATED takes true or false, not {value:?}"))),
    };
    let db = state.require_db()?;
    db.set_replication_sink(&name, replicated, Some(state.table_flags_sink()))
        .map_err(crate::server::query::engine_error)?;
    tracing::info!(table = %name, replicated, "table replication flag updated");
    Ok(CommandOutput::Tag("SET"))
}

fn set_column_search(state: &ServerState, mut words: std::collections::VecDeque<String>) -> PgWireResult<CommandOutput> {
    let usage = "usage: MURMUR SET COLUMN <table> <column> FTS true|false | VECTOR true DIMENSIONS <n> [DISTANCE l2|cosine|l1] | VECTOR false";
    words.pop_front();
    if words.len() < 4 { return Err(user_error("42601", usage.into())); }
    let table = words.pop_front().unwrap();
    let column = words.pop_front().unwrap();
    let kind = words.pop_front().unwrap().to_ascii_uppercase();
    if !matches!(kind.as_str(), "FTS" | "VECTOR") { return Err(user_error("42601", usage.into())); }
    let enabled = match words.pop_front().unwrap().to_ascii_lowercase().as_str() {
        "true" | "1" | "on" | "yes" => true,
        "false" | "0" | "off" | "no" => false,
        _ => return Err(user_error("22023", format!("{kind} takes true or false"))),
    };
    if kind == "FTS" {
        if !words.is_empty() { return Err(user_error("42601", usage.into())); }
        state.require_db()?.set_column_fts(&table, &column, enabled).map_err(crate::server::query::engine_error)?;
    } else {
        let config = if enabled {
            if !matches!(words.len(), 2 | 4) || !words[0].eq_ignore_ascii_case("DIMENSIONS") {
                return Err(user_error("42601", usage.into()));
            }
            let dimensions = words[1].parse::<usize>().map_err(|_| user_error("22023", "vector dimensions must be an integer from 1 to 8192".into()))?;
            let distance = if words.len() == 4 {
                if !words[2].eq_ignore_ascii_case("DISTANCE") { return Err(user_error("42601", usage.into())); }
                match words[3].to_ascii_lowercase().as_str() {
                    "l2" => murmur::DistanceMetric::L2, "cosine" => murmur::DistanceMetric::Cosine, "l1" => murmur::DistanceMetric::L1,
                    _ => return Err(user_error("22023", "vector distance must be l2, cosine, or l1".into())),
                }
            } else { murmur::DistanceMetric::L2 };
            Some(murmur::VectorColumnConfig { dimensions, distance })
        } else {
            if !words.is_empty() { return Err(user_error("42601", usage.into())); }
            None
        };
        state.require_db()?.set_column_vector(&table, &column, config).map_err(crate::server::query::engine_error)?;
    }
    Ok(CommandOutput::Tag("SET"))
}

fn show_column_fts(state: &ServerState, words: std::collections::VecDeque<String>) -> PgWireResult<CommandOutput> {
    if words.len() != 1 || !words[0].eq_ignore_ascii_case("FTS") {
        return Err(user_error("42601", "usage: MURMUR SHOW COLUMNS FTS".into()));
    }
    use murmur::Value;
    use crate::server::query::ColInfo;
    let db = state.require_db()?;
    // Read schema and status on the writer so configuration cannot change
    // between the two observations.
    let (schema, statuses) = db.schema_with_search_status().map_err(crate::server::query::engine_error)?;
    let rows = (|| -> Result<Vec<Vec<murmur::Value>>, murmur::Error> {
        let mut rows = Vec::new();
        for table in &schema.tables {
            let name = murmur::host::schema_index_name(table);
            let status = statuses.iter().find(|s| s.name == name);
            for column in &table.columns {
                if column.name == murmur::host::MANAGED_KEY_COLUMN { continue; }
                let mut row = vec![Value::Text(table.name.clone()), Value::Text(column.name.clone()), Value::Text(column.fts.to_string())];
                if column.fts {
                    let status = status.ok_or(murmur::Error::SchemaMismatch)?;
                    row.extend([
                        Value::Text(name.clone()), Value::Text(status.ready.to_string()), Value::Text(status.rebuilding.to_string()),
                        Value::Integer(status.pending_keys as i64), Value::Integer(status.queued_bytes as i64),
                        status.error.clone().map(Value::Text).unwrap_or(Value::Null),
                    ]);
                } else { row.extend(vec![Value::Null; 6]); }
                rows.push(row);
            }
        }
        rows.sort_by(|a,b| match (&a[0], &a[1], &b[0], &b[1]) {
            (Value::Text(at), Value::Text(ac), Value::Text(bt), Value::Text(bc)) => (at, ac).cmp(&(bt,bc)), _ => std::cmp::Ordering::Equal,
        });
        Ok(rows)
    })().map_err(crate::server::query::engine_error)?;
    let columns = ["table", "column", "enabled", "index_name", "ready", "rebuilding", "pending_keys", "queued_bytes", "error"].into_iter().map(|name| ColInfo {
        name: name.into(), decltype: Some(if matches!(name, "pending_keys" | "queued_bytes") { "INTEGER" } else { "TEXT" }.into()),
    }).collect();
    Ok(CommandOutput::Rows { columns, rows })
}

fn show_column_vectors(state: &ServerState, words: std::collections::VecDeque<String>) -> PgWireResult<CommandOutput> {
    if words.len() != 1 || !words[0].eq_ignore_ascii_case("VECTOR") {
        return Err(user_error("42601", "usage: MURMUR SHOW COLUMNS VECTOR".into()));
    }
    use murmur::Value;
    let (schema, statuses) = state.require_db()?.schema_with_search_status().map_err(crate::server::query::engine_error)?;
    let rows = (|| -> Result<Vec<Vec<murmur::Value>>, murmur::Error> {
        let mut rows = Vec::new();
        for table in &schema.tables {
            for column in &table.columns {
                if column.name == murmur::host::MANAGED_KEY_COLUMN { continue; }
                let mut row = vec![Value::Text(table.name.clone()), Value::Text(column.name.clone()), Value::Text(column.vector.is_some().to_string())];
                if let Some(config) = &column.vector {
                    let name = murmur::host::vector_index_name(table, column);
                    let status = statuses.iter().find(|s| s.name == name).ok_or(murmur::Error::SchemaMismatch)?;
                    let distance = match config.distance { murmur::DistanceMetric::L2 => "l2", murmur::DistanceMetric::Cosine => "cosine", murmur::DistanceMetric::L1 => "l1" };
                    row.extend([Value::Integer(config.dimensions as i64), Value::Text(distance.into()), Value::Text(name),
                        Value::Text(status.ready.to_string()), Value::Text(status.rebuilding.to_string()),
                        Value::Integer(status.pending_keys as i64), Value::Integer(status.queued_bytes as i64),
                        status.error.clone().map(Value::Text).unwrap_or(Value::Null)]);
                } else { row.extend(vec![Value::Null; 8]); }
                rows.push(row);
            }
        }
        rows.sort_by(|a,b| match (&a[0], &a[1], &b[0], &b[1]) {
            (Value::Text(at), Value::Text(ac), Value::Text(bt), Value::Text(bc)) => (at,ac).cmp(&(bt,bc)), _ => std::cmp::Ordering::Equal,
        });
        Ok(rows)
    })().map_err(crate::server::query::engine_error)?;
    let columns = ["table", "column", "enabled", "dimensions", "distance", "index_name", "ready", "rebuilding", "pending_keys", "queued_bytes", "error"].into_iter().map(|name| crate::server::query::ColInfo {
        name: name.into(), decltype: Some(if matches!(name,"dimensions"|"pending_keys"|"queued_bytes") {"INTEGER"} else {"TEXT"}.into()),
    }).collect();
    Ok(CommandOutput::Rows { columns, rows })
}

fn show(
    state: &ServerState,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    let usage = "usage: MURMUR SHOW TABLES REPLICATION | MURMUR SHOW COLUMNS FTS | MURMUR SHOW PEERS | MURMUR SHOW REPLICATION | MURMUR SHOW RETENTION | MURMUR SHOW STATUS | MURMUR SHOW METRICS [SQLITE|SPOOL]";
    match words.pop_front() {
        Some(w) if w.eq_ignore_ascii_case("TABLES") => show_tables(state, words),
        Some(w) if w.eq_ignore_ascii_case("METRICS") => show_metrics(state, words),
        Some(w) if w.eq_ignore_ascii_case("PEERS") => show_peers(state, words),
        Some(w) if w.eq_ignore_ascii_case("REPLICATION") => show_replication(state, words),
        Some(w) if w.eq_ignore_ascii_case("STATUS") => {
            if !words.is_empty() { return Err(user_error("42601", "usage: MURMUR SHOW STATUS".into())); }
            let snapshot = state.require_db()?.status();
            let json = serde_json::to_string(&snapshot)
                .map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;
            Ok(CommandOutput::Rows {
                columns: vec![crate::server::query::ColInfo { name: "status".into(), decltype: Some("TEXT".into()) }],
                rows: vec![vec![murmur::Value::Text(json)]],
            })
        }
        Some(w) if w.eq_ignore_ascii_case("RETENTION") => show_retention(state, words),
        Some(w) if w.eq_ignore_ascii_case("COLUMNS") => {
            if words.front().is_some_and(|w| w.eq_ignore_ascii_case("VECTOR")) { show_column_vectors(state, words) }
            else { show_column_fts(state, words) }
        },
        _ => Err(user_error("42601", usage.to_string())),
    }
}

/// `MURMUR SHOW TABLES REPLICATION`: one row per known table plus any stale
/// config entries whose tables no longer exist.
fn show_tables(
    state: &ServerState,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    let usage = "usage: MURMUR SHOW TABLES REPLICATION";
    match words.pop_front() {
        Some(b) if b.eq_ignore_ascii_case("REPLICATION") => {}
        _ => return Err(user_error("42601", usage.to_string())),
    }
    if !words.is_empty() {
        return Err(user_error("42601", usage.to_string()));
    }
    let db = state.require_db()?;
    let schema = db.schema().map_err(crate::server::query::engine_error)?;
    let mut named: Vec<(String, bool)> = schema
        .tables
        .iter()
        .map(|table| {
            let replicated = schema.table_replication.get(&table.id).copied().unwrap_or(false);
            (table.name.clone(), replicated)
        })
        .collect();
    named.sort();
    named.extend(
        schema
            .table_replication
            .iter()
            .filter(|(id, _)| !schema.tables.iter().any(|t| &t.id == *id))
            .map(|(id, replicated)| (id.clone(), *replicated)),
    );
    let rows = named
        .into_iter()
        .map(|(name, replicated)| {
            vec![
                murmur::Value::Text(name),
                murmur::Value::Text(replicated.to_string()),
            ]
        })
        .collect();
    use crate::server::query::ColInfo;
    Ok(CommandOutput::Rows {
        columns: vec![
            ColInfo { name: "table".into(), decltype: Some("TEXT".into()) },
            ColInfo { name: "replicated".into(), decltype: Some("TEXT".into()) },
        ],
        rows,
    })
}

/// `MURMUR SHOW METRICS [SQLITE|SPOOL]`: list current metrics from SQLite and/or Spool.
fn show_metrics(
    state: &ServerState,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    let usage = "usage: MURMUR SHOW METRICS [SQLITE|SPOOL]";
    let target = match words.pop_front() {
        None => None,
        Some(w) if w.eq_ignore_ascii_case("SQLITE") => Some("sqlite"),
        Some(w) if w.eq_ignore_ascii_case("SPOOL") => Some("spool"),
        _ => return Err(user_error("42601", usage.to_string())),
    };
    if !words.is_empty() {
        return Err(user_error("42601", usage.to_string()));
    }
    let db = state.require_db()?;
    let mut rows = Vec::new();

    if target.is_none() || target == Some("sqlite") {
        let m = db.sqlite_metrics().map_err(crate::server::query::engine_error)?;
        rows.push(metric_row("sqlite", "memory_used_bytes", m.memory_used_bytes.to_string()));
        rows.push(metric_row("sqlite", "memory_highwater_bytes", m.memory_highwater_bytes.to_string()));
        rows.push(metric_row("sqlite", "page_size", m.page_size.to_string()));
        rows.push(metric_row("sqlite", "page_count", m.page_count.to_string()));
        rows.push(metric_row("sqlite", "database_size_bytes", m.database_size_bytes.to_string()));
        rows.push(metric_row("sqlite", "freelist_pages", m.freelist_pages.to_string()));
        rows.push(metric_row("sqlite", "schema_version", m.schema_version.to_string()));
        rows.push(metric_row("sqlite", "table_count", m.table_count.to_string()));
        rows.push(metric_row("sqlite", "index_count", m.index_count.to_string()));
        rows.push(metric_row("sqlite", "total_rows", m.total_rows.to_string()));
        rows.push(metric_row("sqlite", "session_memory_bytes", m.session_memory_bytes.to_string()));
        rows.push(metric_row("sqlite", "session_changeset_bytes", m.session_changeset_bytes.to_string()));
    }

    if target.is_none() || target == Some("spool") {
        let s = db.spool_metrics();
        rows.push(metric_row("spool", "keys", s.keys.to_string()));
        rows.push(metric_row("spool", "segments", s.segments.to_string()));
        rows.push(metric_row("spool", "disk_bytes", s.disk_bytes.to_string()));
        rows.push(metric_row("spool", "live_bytes", s.live_bytes.to_string()));
        rows.push(metric_row("spool", "live_records", s.live_records.to_string()));
        rows.push(metric_row("spool", "dead_records", s.dead_records.to_string()));
        rows.push(metric_row("spool", "pending_records", s.pending_records.to_string()));
        rows.push(metric_row("spool", "pending_bytes", s.pending_bytes.to_string()));
        rows.push(metric_row("spool", "blocks_written", s.blocks_written.to_string()));
        rows.push(metric_row("spool", "bytes_written", s.bytes_written.to_string()));
        rows.push(metric_row("spool", "compression_ratio", format!("{:.4}", s.compression_ratio)));
        rows.push(metric_row("spool", "compactions", s.compactions.to_string()));
        rows.push(metric_row("spool", "compaction_bytes", s.compaction_bytes.to_string()));
        rows.push(metric_row("spool", "flushes", s.flushes.to_string()));
        rows.push(metric_row("spool", "flush_errors", s.flush_errors.to_string()));
        rows.push(metric_row("spool", "reclaims", s.reclaims.to_string()));
        rows.push(metric_row("spool", "reclaim_errors", s.reclaim_errors.to_string()));
        rows.push(metric_row("spool", "commits", s.commits.to_string()));
        rows.push(metric_row("spool", "syncs", s.syncs.to_string()));
        rows.push(metric_row("spool", "commit_nanos", s.commit_nanos.to_string()));
        rows.push(metric_row("spool", "sync_nanos", s.sync_nanos.to_string()));
        rows.push(metric_row("spool", "storage_failures", s.storage_failures.to_string()));
        rows.push(metric_row("spool", "index_bytes_estimate", s.index_bytes_estimate.to_string()));
    }

    use crate::server::query::ColInfo;
    Ok(CommandOutput::Rows {
        columns: vec![
            ColInfo { name: "subsystem".into(), decltype: Some("TEXT".into()) },
            ColInfo { name: "metric".into(), decltype: Some("TEXT".into()) },
            ColInfo { name: "value".into(), decltype: Some("TEXT".into()) },
        ],
        rows,
    })
}

fn metric_row(subsystem: &str, metric: &str, value: String) -> Vec<murmur::Value> {
    vec![
        murmur::Value::Text(subsystem.into()),
        murmur::Value::Text(metric.into()),
        murmur::Value::Text(value),
    ]
}

/// `MURMUR BACKUP SAVE`: checkpoint the live database, pack it into one
/// datetime-named archive in `[backups].dir`, and return its name and size.
fn backup_save(
    state: &ServerState,
    config: &Config,
    words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    use crate::server::backup;
    if !words.is_empty() {
        return Err(user_error("42601", "usage: MURMUR BACKUP SAVE".to_string()));
    }
    let db = state.require_db()?;
    let dir = std::path::Path::new(&config.backups.dir);
    backup::create_private_dir(dir)
        .map_err(|e| user_error("58000", format!("backup storage error: {e}")))?;
    let checkpoint = backup::staging_dir(dir, "checkpoint");
    let outcome = (|| -> PgWireResult<(String, u64)> {
        db.backup(&checkpoint)
            .map_err(crate::server::query::engine_error)?;
        let name = backup::datetime_name(std::time::SystemTime::now());
        let dest = backup::unique_path(dir, &name);
        let size = backup::pack_dir(&checkpoint, &dest).map_err(|e| e.user_error())?;
        let name = dest
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or(name);
        Ok((name, size))
    })();
    let _ = std::fs::remove_dir_all(&checkpoint);
    let (name, size) = outcome?;
    tracing::info!(backup = %name, size, "backup saved");
    use crate::server::query::ColInfo;
    Ok(CommandOutput::Rows {
        columns: vec![
            ColInfo { name: "name".into(), decltype: Some("TEXT".into()) },
            ColInfo { name: "size".into(), decltype: Some("INTEGER".into()) },
        ],
        rows: vec![vec![
            murmur::Value::Text(name),
            murmur::Value::Integer(as_int(size)?),
        ]],
    })
}

/// `MURMUR BACKUP LIST`: one row per backup file with size and mtime.
fn backup_list(
    state: &ServerState,
    config: &Config,
    words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    use crate::server::backup;
    if !words.is_empty() {
        return Err(user_error("42601", "usage: MURMUR BACKUP LIST".to_string()));
    }
    state.require_db()?;
    let dir = std::path::Path::new(&config.backups.dir);
    let mut rows = Vec::new();
    for entry in backup::list_entries(dir).map_err(|e| e.user_error())? {
        rows.push(vec![
            murmur::Value::Text(entry.name),
            murmur::Value::Integer(as_int(entry.size)?),
            murmur::Value::Text(entry.modified),
        ]);
    }
    use crate::server::query::ColInfo;
    Ok(CommandOutput::Rows {
        columns: vec![
            ColInfo { name: "name".into(), decltype: Some("TEXT".into()) },
            ColInfo { name: "size".into(), decltype: Some("INTEGER".into()) },
            ColInfo { name: "modified".into(), decltype: Some("TEXT".into()) },
        ],
        rows,
    })
}

/// `MURMUR BACKUP UPLOAD BEGIN|APPEND|FINISH|ABORT '<name>' ['<base64>']`:
/// chunked file upload. Chunks are base64 in SQL text; FINISH publishes the
/// file atomically and reports its size.
fn backup_upload(
    state: &ServerState,
    config: &Config,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    use crate::server::backup;
    let usage = "usage: MURMUR BACKUP UPLOAD BEGIN|APPEND|FINISH|ABORT '<name>' ['<base64-chunk>']";
    let step = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    let name = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    state.require_db()?;
    let dir = std::path::Path::new(&config.backups.dir);
    if step.eq_ignore_ascii_case("BEGIN") {
        if !words.is_empty() {
            return Err(user_error("42601", usage.to_string()));
        }
        backup::begin_upload(dir, &name).map_err(|e| e.user_error())?;
        return Ok(CommandOutput::Tag("UPLOAD"));
    }
    if step.eq_ignore_ascii_case("APPEND") {
        let chunk = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
        if !words.is_empty() {
            return Err(user_error("42601", usage.to_string()));
        }
        backup::append_upload(dir, &name, &chunk).map_err(|e| e.user_error())?;
        return Ok(CommandOutput::Tag("UPLOAD"));
    }
    if step.eq_ignore_ascii_case("FINISH") {
        if !words.is_empty() {
            return Err(user_error("42601", usage.to_string()));
        }
        let size = backup::finish_upload(dir, &name).map_err(|e| e.user_error())?;
        tracing::info!(backup = %name, size, "backup uploaded");
        use crate::server::query::ColInfo;
        return Ok(CommandOutput::Rows {
            columns: vec![
                ColInfo { name: "name".into(), decltype: Some("TEXT".into()) },
                ColInfo { name: "size".into(), decltype: Some("INTEGER".into()) },
            ],
            rows: vec![vec![
                murmur::Value::Text(name),
                murmur::Value::Integer(as_int(size)?),
            ]],
        });
    }
    if step.eq_ignore_ascii_case("ABORT") {
        if !words.is_empty() {
            return Err(user_error("42601", usage.to_string()));
        }
        backup::abort_upload(dir, &name).map_err(|e| e.user_error())?;
        return Ok(CommandOutput::Tag("UPLOAD"));
    }
    Err(user_error("42601", usage.to_string()))
}

/// `MURMUR BACKUP DOWNLOAD '<name>' [OFFSET <n>] [LIMIT <n>]`: one row with
/// the requested byte range as a BLOB. Clients loop until `offset + len ==
/// total`. LIMIT defaults to 1 MiB so a bare call cannot OOM the server.
fn backup_download(
    state: &ServerState,
    config: &Config,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    use crate::server::backup;
    let usage = "usage: MURMUR BACKUP DOWNLOAD '<name>' [OFFSET <bytes>] [LIMIT <bytes>]";
    let name = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    let mut offset = 0u64;
    let mut limit = backup::DOWNLOAD_CHUNK_DEFAULT;
    let mut seen_offset = false;
    let mut seen_limit = false;
    while let Some(keyword) = words.pop_front() {
        let value = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
        let parsed: u64 = value.parse().map_err(|_| {
            user_error("22023", format!("{} takes a non-negative byte count, not {value:?}", keyword.to_ascii_uppercase()))
        })?;
        if keyword.eq_ignore_ascii_case("OFFSET") && !seen_offset {
            offset = parsed;
            seen_offset = true;
        } else if keyword.eq_ignore_ascii_case("LIMIT") && !seen_limit {
            limit = parsed;
            seen_limit = true;
        } else {
            return Err(user_error("42601", usage.to_string()));
        }
    }
    state.require_db()?;
    let dir = std::path::Path::new(&config.backups.dir);
    let chunk = backup::read_chunk(dir, &name, offset, limit).map_err(|e| e.user_error())?;
    use crate::server::query::ColInfo;
    Ok(CommandOutput::Rows {
        columns: vec![
            ColInfo { name: "offset".into(), decltype: Some("INTEGER".into()) },
            ColInfo { name: "total".into(), decltype: Some("INTEGER".into()) },
            ColInfo { name: "chunk".into(), decltype: Some("BLOB".into()) },
        ],
        rows: vec![vec![
            murmur::Value::Integer(as_int(offset)?),
            murmur::Value::Integer(as_int(chunk.total)?),
            murmur::Value::Blob(chunk.data),
        ]],
    })
}

/// `MURMUR BACKUP ...` dispatcher. Every subcommand requires an unlocked
/// connection; the key gate stays uniform across reads and writes.
fn backup(
    state: &ServerState,
    config: &Config,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    let usage = "usage: MURMUR BACKUP SAVE|LIST|UPLOAD ...|DOWNLOAD ...";
    match words.pop_front() {
        Some(sub) if sub.eq_ignore_ascii_case("SAVE") => backup_save(state, config, words),
        Some(sub) if sub.eq_ignore_ascii_case("LIST") => backup_list(state, config, words),
        Some(sub) if sub.eq_ignore_ascii_case("UPLOAD") => backup_upload(state, config, words),
        Some(sub) if sub.eq_ignore_ascii_case("DOWNLOAD") => backup_download(state, config, words),
        _ => Err(user_error("42601", usage.to_string())),
    }
}

/// `MURMUR RESTORE FROM '<name>' KEY '<secret>'`: unpack the archive and
/// live-swap it into place. The key re-authenticates this destructive
/// operation and is verified before anything is unpacked or closed.
fn restore(
    state: &ServerState,
    config: &Config,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    use crate::server::backup;
    let usage = "usage: MURMUR RESTORE FROM '<name>' KEY '<secret>'";
    match words.pop_front() {
        Some(word) if word.eq_ignore_ascii_case("FROM") => {}
        _ => return Err(user_error("42601", usage.to_string())),
    }
    let name = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    match words.pop_front() {
        Some(word) if word.eq_ignore_ascii_case("KEY") => {}
        _ => return Err(user_error("42601", usage.to_string())),
    }
    let secret = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    if !words.is_empty() {
        return Err(user_error("42601", usage.to_string()));
    }
    if secret.is_empty() {
        return Err(user_error("28000", "MURMUR RESTORE needs a non-empty key".to_string()));
    }
    let key = secret_to_key(&secret);
    if let Err(e) = state.verify_key(&key) {
        return Err(match e {
            murmur::Error::WrongKey => user_error("28000", "incorrect key".to_string()),
            other => crate::server::query::engine_error(other),
        });
    }
    let dir = std::path::Path::new(&config.backups.dir);
    let archive = backup::resolve_file(dir, &name).map_err(|e| e.user_error())?;
    let unpacked = backup::staging_dir(dir, "unpack");
    let outcome = (|| -> PgWireResult<std::path::PathBuf> {
        backup::unpack_archive(&archive, &unpacked).map_err(|e| e.user_error())?;
        state
            .restore_from(config, &key, &unpacked)
            .map_err(crate::server::query::engine_error)
    })();
    let _ = std::fs::remove_dir_all(&unpacked);
    let replaced = outcome?;
    tracing::info!(backup = %name, replaced = %replaced.display(), "database restored");
    Ok(CommandOutput::Tag("RESTORE"))
}

/// `MURMUR ROTATE KEY '<previous>' '<new>'`: verify the previous master
/// key, then rewrap the key registry under the new one. A comma between the
/// keys is accepted (`'<previous>', '<new>'`). Sessions stay unlocked; new
/// connections and restarts require the new key.
fn rotate(
    state: &ServerState,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    let usage = "usage: MURMUR ROTATE KEY '<previous>' '<new>'";
    match words.pop_front() {
        Some(word) if word.eq_ignore_ascii_case("KEY") => {}
        _ => return Err(user_error("42601", usage.to_string())),
    }
    let mut previous = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    // Accept `ROTATE KEY '<previous>', '<new>'` as well as the plain form.
    if previous.ends_with(',') {
        previous.pop();
    } else if words.front().is_some_and(|w| w == ",") {
        words.pop_front();
    }
    let new = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    if !words.is_empty() {
        return Err(user_error("42601", usage.to_string()));
    }
    if previous.is_empty() {
        return Err(user_error("28000", "MURMUR ROTATE needs a non-empty previous key".to_string()));
    }
    if new.is_empty() {
        return Err(user_error("22023", "MURMUR ROTATE needs a non-empty new key".to_string()));
    }
    state.require_db()?;
    let previous_key = secret_to_key(&previous);
    let new_key = secret_to_key(&new);
    if let Err(e) = state.rotate_key(&previous_key, &new_key) {
        return Err(match e {
            murmur::Error::WrongKey => user_error("28000", "incorrect key".to_string()),
            murmur::Error::Invalid(message) => user_error("22023", message),
            other => crate::server::query::engine_error(other),
        });
    }
    tracing::info!("master key rotated");
    Ok(CommandOutput::Tag("ROTATE"))
}

fn as_int(value: u64) -> PgWireResult<i64> {
    i64::try_from(value)
        .map_err(|_| user_error("54000", "backup size exceeds int64".to_string()))
}

/// A 64-hex-char secret is the raw key; anything else is a passphrase hashed
/// with BLAKE3, matching `murmur-admin --passphrase`.
fn secret_to_key(secret: &str) -> [u8; 32] {
    if secret.len() == 64 && secret.bytes().all(|b| b.is_ascii_hexdigit()) {
        let mut key = [0u8; 32];
        for (i, chunk) in secret.as_bytes().chunks(2).enumerate() {
            key[i] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap_or("00"), 16)
                .unwrap_or(0);
        }
        return key;
    }
    *blake3::hash(secret.as_bytes()).as_bytes()
}

/// `MURMUR PEER ADD|REMOVE|RETIRE|SYNC`: peer membership administration.
/// Argument shapes are validated before the replication gate so usage errors
/// surface even when replication is disabled.
fn peer(
    state: &ServerState,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    let usage = "usage: MURMUR PEER ADD '<address>' | MURMUR PEER REMOVE '<node>' | MURMUR PEER RETIRE '<node>' | MURMUR PEER SYNC";
    let sub = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    if sub.eq_ignore_ascii_case("ADD") {
        let addr = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
        if !words.is_empty() {
            return Err(user_error("42601", usage.to_string()));
        }
        let addr: SocketAddr = addr
            .parse()
            .map_err(|_| user_error("22023", format!("invalid peer address: {addr:?}")))?;
        return peer_add(state, addr);
    }
    if sub.eq_ignore_ascii_case("REMOVE") || sub.eq_ignore_ascii_case("RETIRE") {
        let node = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
        if !words.is_empty() {
            return Err(user_error("42601", usage.to_string()));
        }
        let node = parse_node_id(&node)?;
        if sub.eq_ignore_ascii_case("REMOVE") {
            return peer_remove(state, node);
        }
        return peer_retire(state, node);
    }
    if sub.eq_ignore_ascii_case("SYNC") {
        if !words.is_empty() {
            return Err(user_error("42601", usage.to_string()));
        }
        return peer_sync(state);
    }
    Err(user_error("42601", usage.to_string()))
}

/// A node id is 32 lowercase-or-uppercase hex characters (16 bytes).
fn parse_node_id(word: &str) -> PgWireResult<[u8; 16]> {
    if word.len() != 32 || !word.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(user_error(
            "22023",
            format!("node id must be 32 hex characters, not {word:?}"),
        ));
    }
    let mut node = [0u8; 16];
    for (index, value) in node.iter_mut().enumerate() {
        *value = u8::from_str_radix(&word[index * 2..index * 2 + 2], 16)
            .map_err(|_| user_error("22023", format!("node id must be 32 hex characters, not {word:?}")))?;
    }
    Ok(node)
}

#[cfg(feature = "replication")]
fn replication_handle(state: &ServerState) -> PgWireResult<murmur::replication::Handle> {
    let db = state.require_db()?;
    db.replication()
        .ok_or_else(|| user_error("55000", "replication is not enabled".to_string()))
}

#[cfg(feature = "replication")]
fn replication_error(error: murmur::replication::Error) -> pgwire::error::PgWireError {
    match error {
        murmur::replication::Error::Invalid(message) => user_error("22023", message),
        murmur::replication::Error::Overloaded => {
            user_error("54000", "replication overloaded".to_string())
        }
        murmur::replication::Error::Auth(message) => user_error("28000", message),
        murmur::replication::Error::Unavailable(message) => user_error("08006", message),
        murmur::replication::Error::Protocol(message) => user_error("XX001", message),
        murmur::replication::Error::Storage(message) => {
            user_error("58000", format!("storage error: {message}"))
        }
        murmur::replication::Error::Io(error) => {
            user_error("58000", format!("storage error: {error}"))
        }
    }
}

/// Saturating `u64`/`usize` to wire `INTEGER`; diagnostics must not fail.
#[cfg(feature = "replication")]
fn sat_int(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(feature = "replication")]
fn peer_add(state: &ServerState, addr: SocketAddr) -> PgWireResult<CommandOutput> {
    replication_handle(state)?.add_peer(addr).map_err(replication_error)?;
    Ok(CommandOutput::Tag("PEER"))
}

#[cfg(feature = "replication")]
fn peer_remove(state: &ServerState, node: [u8; 16]) -> PgWireResult<CommandOutput> {
    replication_handle(state)?.remove_peer(node).map_err(replication_error)?;
    Ok(CommandOutput::Tag("PEER"))
}

#[cfg(feature = "replication")]
fn peer_retire(state: &ServerState, node: [u8; 16]) -> PgWireResult<CommandOutput> {
    replication_handle(state)?.retire_peer(node).map_err(replication_error)?;
    Ok(CommandOutput::Tag("PEER"))
}

#[cfg(feature = "replication")]
fn peer_sync(state: &ServerState) -> PgWireResult<CommandOutput> {
    replication_handle(state)?.force_sync().map_err(replication_error)?;
    Ok(CommandOutput::Tag("PEER"))
}

#[cfg(not(feature = "replication"))]
fn replication_disabled(state: &ServerState) -> PgWireResult<CommandOutput> {
    state.require_db()?;
    Err(user_error("55000", "replication is not enabled".to_string()))
}

#[cfg(not(feature = "replication"))]
fn peer_add(state: &ServerState, _addr: SocketAddr) -> PgWireResult<CommandOutput> {
    replication_disabled(state)
}

#[cfg(not(feature = "replication"))]
fn peer_remove(state: &ServerState, _node: [u8; 16]) -> PgWireResult<CommandOutput> {
    replication_disabled(state)
}

#[cfg(not(feature = "replication"))]
fn peer_retire(state: &ServerState, _node: [u8; 16]) -> PgWireResult<CommandOutput> {
    replication_disabled(state)
}

#[cfg(not(feature = "replication"))]
fn peer_sync(state: &ServerState) -> PgWireResult<CommandOutput> {
    replication_disabled(state)
}

/// `MURMUR SHOW PEERS`: one row per connected peer, ordered by node id.
#[cfg(feature = "replication")]
fn show_peers(
    state: &ServerState,
    words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    if !words.is_empty() {
        return Err(user_error("42601", "usage: MURMUR SHOW PEERS".to_string()));
    }
    use murmur::Value;
    use crate::server::query::ColInfo;
    let status = replication_handle(state)?.status();
    let mut peers = status.peers_diagnostics;
    peers.sort_by_cached_key(|p| murmur::replication::hex(&p.node_id));
    let rows = peers
        .into_iter()
        .map(|peer| {
            vec![
                Value::Text(murmur::replication::hex(&peer.node_id)),
                peer.address.map(|a| Value::Text(a.to_string())).unwrap_or(Value::Null),
                Value::Text(peer.connected.to_string()),
                peer.last_seen_millis_ago.map(sat_int).map(Value::Integer).unwrap_or(Value::Null),
                Value::Integer(sat_int(peer.sent_watermark)),
                Value::Text(peer.schema_agreed.to_string()),
                Value::Text(peer.push_mode.to_string()),
            ]
        })
        .collect();
    Ok(CommandOutput::Rows {
        columns: ["node_id", "address", "connected", "last_seen_millis_ago", "sent_watermark", "schema_agreed", "push_mode"]
            .into_iter()
            .map(|name| ColInfo {
                name: name.into(),
                decltype: Some(if matches!(name, "last_seen_millis_ago" | "sent_watermark") { "INTEGER" } else { "TEXT" }.into()),
            })
            .collect(),
        rows,
    })
}

#[cfg(not(feature = "replication"))]
fn show_peers(
    state: &ServerState,
    words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    if !words.is_empty() {
        return Err(user_error("42601", "usage: MURMUR SHOW PEERS".to_string()));
    }
    replication_disabled(state)
}

/// `MURMUR SHOW REPLICATION`: one row with the local replication summary.
/// The trailing `committed` column is the local engine committed-batch
/// cursor; earlier column indices are stable.
#[cfg(feature = "replication")]
fn show_replication(
    state: &ServerState,
    words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    if !words.is_empty() {
        return Err(user_error("42601", "usage: MURMUR SHOW REPLICATION".to_string()));
    }
    use murmur::Value;
    use crate::server::query::ColInfo;
    let status = replication_handle(state)?.status();
    let committed = state.require_db()?.status().committed;
    Ok(CommandOutput::Rows {
        columns: ["running", "address", "peers", "members", "staged_transactions", "applied_transactions", "last_error", "committed"]
            .into_iter()
            .map(|name| ColInfo {
                name: name.into(),
                decltype: Some(
                    if matches!(name, "peers" | "members" | "staged_transactions" | "applied_transactions" | "committed") {
                        "INTEGER"
                    } else {
                        "TEXT"
                    }
                    .into(),
                ),
            })
            .collect(),
        rows: vec![vec![
            Value::Text(status.running.to_string()),
            status.address.map(|a| Value::Text(a.to_string())).unwrap_or(Value::Null),
            Value::Integer(sat_int(status.peers as u64)),
            Value::Integer(sat_int(status.members as u64)),
            Value::Integer(sat_int(status.staged_transactions as u64)),
            Value::Integer(sat_int(status.applied_transactions)),
            status.last_error.map(Value::Text).unwrap_or(Value::Null),
            Value::Integer(sat_int(committed)),
        ]],
    })
}

#[cfg(not(feature = "replication"))]
fn show_replication(
    state: &ServerState,
    words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    if !words.is_empty() {
        return Err(user_error("42601", "usage: MURMUR SHOW REPLICATION".to_string()));
    }
    replication_disabled(state)
}

/// `MURMUR SHOW RETENTION`: one row per (peer, acknowledged origin), ordered
/// by node id then origin. Peers with no acknowledgements get one row with
/// NULL `origin`/`acknowledged`. The trailing counters are node-global.
#[cfg(feature = "replication")]
fn show_retention(
    state: &ServerState,
    words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    if !words.is_empty() {
        return Err(user_error("42601", "usage: MURMUR SHOW RETENTION".to_string()));
    }
    use murmur::Value;
    use crate::server::query::ColInfo;
    let status = replication_handle(state)?.retention_status().map_err(replication_error)?;
    let mut peers = status.peers;
    peers.sort_by_cached_key(|p| murmur::replication::hex(&p.node));
    let mut rows = Vec::new();
    for peer in &peers {
        let node = Value::Text(murmur::replication::hex(&peer.node));
        let seen = Value::Integer(sat_int(peer.last_seen_millis));
        let retired = Value::Text(peer.retired.to_string());
        let collected = Value::Integer(sat_int(status.collected_transactions));
        let last = Value::Integer(sat_int(status.last_collection_millis));
        if peer.acknowledged.is_empty() {
            rows.push(vec![node, seen, retired, Value::Null, Value::Null, collected, last]);
            continue;
        }
        let mut acks: Vec<_> = peer.acknowledged.iter().collect();
        acks.sort_by_cached_key(|(origin, _)| murmur::replication::hex(*origin));
        for (origin, sequence) in acks {
            rows.push(vec![
                node.clone(),
                seen.clone(),
                retired.clone(),
                Value::Text(murmur::replication::hex(origin)),
                Value::Integer(sat_int(*sequence)),
                collected.clone(),
                last.clone(),
            ]);
        }
    }
    Ok(CommandOutput::Rows {
        columns: ["node_id", "last_seen_millis", "retired", "origin", "acknowledged", "collected_transactions", "last_collection_millis"]
            .into_iter()
            .map(|name| ColInfo {
                name: name.into(),
                decltype: Some(if matches!(name, "node_id" | "retired" | "origin") { "TEXT" } else { "INTEGER" }.into()),
            })
            .collect(),
        rows,
    })
}

#[cfg(not(feature = "replication"))]
fn show_retention(
    state: &ServerState,
    words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    if !words.is_empty() {
        return Err(user_error("42601", "usage: MURMUR SHOW RETENTION".to_string()));
    }
    replication_disabled(state)
}

/// `MURMUR RETENTION COLLECT`: run one bounded collection pass now.
fn retention(
    state: &ServerState,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    let usage = "usage: MURMUR RETENTION COLLECT";
    match words.pop_front() {
        Some(sub) if sub.eq_ignore_ascii_case("COLLECT") => {}
        _ => return Err(user_error("42601", usage.to_string())),
    }
    if !words.is_empty() {
        return Err(user_error("42601", usage.to_string()));
    }
    retention_collect(state)
}

#[cfg(feature = "replication")]
fn retention_collect(state: &ServerState) -> PgWireResult<CommandOutput> {
    use murmur::Value;
    use crate::server::query::ColInfo;
    let collected = replication_handle(state)?.collect_retention().map_err(replication_error)?;
    Ok(CommandOutput::Rows {
        columns: vec![ColInfo { name: "collected".into(), decltype: Some("INTEGER".into()) }],
        rows: vec![vec![Value::Integer(sat_int(collected))]],
    })
}

#[cfg(not(feature = "replication"))]
fn retention_collect(state: &ServerState) -> PgWireResult<CommandOutput> {
    replication_disabled(state)
}

/// Parsed `MURMUR POLICY` operation: single-TEXT-key row, TEXT set elements,
/// inferred INTEGER/REAL/TEXT extrema literals.
#[cfg_attr(not(feature = "replication"), allow(dead_code))]
enum PolicyOp {
    CounterAdd(i64),
    SetAdd(String),
    SetRemove(String),
    Min(murmur::Value),
    Max(murmur::Value),
}

/// `MURMUR POLICY ...`: one CRDT operation on a single-TEXT-key row. The
/// column must carry the matching merge policy (see
/// `replication.merge_policies`); anything else fails in the engine.
fn policy(
    state: &ServerState,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    let usage = "usage: MURMUR POLICY COUNTER_ADD <table> <column> '<key>' <delta> | MURMUR POLICY SET_ADD|SET_REMOVE <table> <column> '<key>' '<value>' | MURMUR POLICY MIN|MAX <table> <column> '<key>' <literal>";
    let op = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    let table = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    let column = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    let key = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    let arg = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    if !words.is_empty() {
        return Err(user_error("42601", usage.to_string()));
    }
    if table.is_empty() || column.is_empty() {
        return Err(user_error("22023", "policy table and column must be non-empty".to_string()));
    }
    let op = if op.eq_ignore_ascii_case("COUNTER_ADD") {
        let delta: i64 = arg
            .parse()
            .map_err(|_| user_error("22023", format!("counter delta must be an integer, not {arg:?}")))?;
        PolicyOp::CounterAdd(delta)
    } else if op.eq_ignore_ascii_case("SET_ADD") {
        PolicyOp::SetAdd(arg)
    } else if op.eq_ignore_ascii_case("SET_REMOVE") {
        PolicyOp::SetRemove(arg)
    } else if op.eq_ignore_ascii_case("MIN") {
        PolicyOp::Min(policy_literal(&arg)?)
    } else if op.eq_ignore_ascii_case("MAX") {
        PolicyOp::Max(policy_literal(&arg)?)
    } else {
        return Err(user_error("42601", usage.to_string()));
    };
    policy_apply(state, &table, &column, &key, op)
}

/// INTEGER when the word parses as `i64`, else REAL when it parses as a
/// finite `f64`, else TEXT. NaN and infinities are rejected: extrema
/// comparisons cannot order them.
fn policy_literal(word: &str) -> PgWireResult<murmur::Value> {
    use murmur::Value;
    if let Ok(n) = word.parse::<i64>() {
        return Ok(Value::Integer(n));
    }
    if let Ok(f) = word.parse::<f64>() {
        if !f.is_finite() {
            return Err(user_error("22023", format!("policy value must be finite, not {word:?}")));
        }
        return Ok(Value::Real(f));
    }
    Ok(Value::Text(word.to_owned()))
}

#[cfg(feature = "replication")]
fn policy_apply(
    state: &ServerState,
    table: &str,
    column: &str,
    key: &str,
    op: PolicyOp,
) -> PgWireResult<CommandOutput> {
    use murmur::Value;
    let db = state.require_db()?;
    if db.replication().is_none() {
        return Err(user_error("55000", "replication is not enabled".to_string()));
    }
    // The transaction closure is `'static`, so it takes owned values.
    let table = table.to_owned();
    let column = column.to_owned();
    let key = vec![Value::Text(key.to_owned())];
    let result = match op {
        PolicyOp::CounterAdd(delta) => {
            db.transaction(move |sql| sql.counter_add(&table, key, &column, delta))
        }
        PolicyOp::SetAdd(value) => {
            db.transaction(move |sql| sql.set_add(&table, key, &column, Value::Text(value)))
        }
        PolicyOp::SetRemove(value) => {
            db.transaction(move |sql| sql.set_remove(&table, key, &column, Value::Text(value)))
        }
        PolicyOp::Min(value) => db.transaction(move |sql| sql.min(&table, key, &column, value)),
        PolicyOp::Max(value) => db.transaction(move |sql| sql.max(&table, key, &column, value)),
    };
    result.map_err(crate::server::query::engine_error)?;
    Ok(CommandOutput::Tag("POLICY"))
}

#[cfg(not(feature = "replication"))]
fn policy_apply(
    state: &ServerState,
    _table: &str,
    _column: &str,
    _key: &str,
    _op: PolicyOp,
) -> PgWireResult<CommandOutput> {
    replication_disabled(state)
}

/// `MURMUR BRIDGE RELEASE <table> '<key>' [COLUMN <column>]`: release High
/// ownership of one bridge-shielded field so later Low updates flow through
/// again. Without `COLUMN` the whole-row ownership is released. Only
/// single-TEXT-key rows are addressable, like `MURMUR POLICY`. Argument
/// shapes are validated before the replication gate so usage errors surface
/// even when replication is disabled.
fn bridge(
    state: &ServerState,
    mut words: std::collections::VecDeque<String>,
) -> PgWireResult<CommandOutput> {
    let usage = "usage: MURMUR BRIDGE RELEASE <table> '<key>' [COLUMN <column>]";
    match words.pop_front() {
        Some(sub) if sub.eq_ignore_ascii_case("RELEASE") => {}
        _ => return Err(user_error("42601", usage.to_string())),
    }
    let table = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    let key = words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?;
    let column = match words.pop_front() {
        None => None,
        Some(word) if word.eq_ignore_ascii_case("COLUMN") => {
            Some(words.pop_front().ok_or_else(|| user_error("42601", usage.to_string()))?)
        }
        _ => return Err(user_error("42601", usage.to_string())),
    };
    if !words.is_empty() {
        return Err(user_error("42601", usage.to_string()));
    }
    if table.is_empty() {
        return Err(user_error("22023", "bridge table must be non-empty".to_string()));
    }
    if matches!(&column, Some(name) if name.is_empty()) {
        return Err(user_error("22023", "bridge column must be non-empty".to_string()));
    }
    bridge_release(state, &table, &key, column.as_deref())
}

#[cfg(feature = "replication")]
fn bridge_release(
    state: &ServerState,
    table: &str,
    key: &str,
    column: Option<&str>,
) -> PgWireResult<CommandOutput> {
    use murmur::Value;
    use crate::server::query::ColInfo;
    let db = state.require_db()?;
    let handle = db
        .replication()
        .ok_or_else(|| user_error("55000", "replication is not enabled".to_string()))?;
    let bridge = handle
        .bridge()
        .ok_or_else(|| user_error("55000", "bridge is not enabled".to_string()))?;
    let schema = db.schema().map_err(crate::server::query::engine_error)?;
    let resolved = schema
        .tables
        .iter()
        .find(|t| t.name == table)
        .ok_or_else(|| user_error("22023", format!("unknown table: {table}")))?;
    if let Some(name) = column {
        if !resolved.columns.iter().any(|c| c.name == name) {
            return Err(user_error("22023", format!("unknown column: {table}.{name}")));
        }
    }
    let keys = murmur::host::application_columns(resolved);
    if keys.len() != 1 || !keys[0].declared_type.eq_ignore_ascii_case("TEXT") {
        return Err(user_error(
            "22023",
            format!("bridge release needs a single-TEXT-key table, not {table:?}"),
        ));
    }
    let encoded = murmur::host::encode(&vec![Value::Text(key.to_owned())]).map_err(crate::server::query::engine_error)?;
    let released = bridge
        .release(table.to_owned(), encoded, column.map(str::to_owned))
        .map_err(replication_error)?;
    Ok(CommandOutput::Rows {
        columns: vec![ColInfo { name: "released".into(), decltype: Some("TEXT".into()) }],
        rows: vec![vec![Value::Text(released.to_string())]],
    })
}

#[cfg(not(feature = "replication"))]
fn bridge_release(
    state: &ServerState,
    _table: &str,
    _key: &str,
    _column: Option<&str>,
) -> PgWireResult<CommandOutput> {
    replication_disabled(state)
}

/// Column shapes for row-returning MURMUR commands, for extended-protocol
/// Describe. Must match the Rows each command returns at Execute; `None`
/// means the command returns only a tag.
pub(crate) fn describe_columns(
    command: &str,
) -> Option<Vec<(&'static str, pgwire::api::Type)>> {
    use pgwire::api::Type;
    let mut words = tokenize(command).ok()?;
    let verb = words.pop_front()?.to_ascii_uppercase();
    match verb.as_str() {
        "SHOW" => {
            let sub = words.pop_front()?.to_ascii_uppercase();
            match sub.as_str() {
                "TABLES" => Some(vec![("table", Type::TEXT), ("replicated", Type::TEXT)]),
                "STATUS" => Some(vec![("status", Type::TEXT)]),
                "PEERS" => Some(vec![
                    ("node_id", Type::TEXT),
                    ("address", Type::TEXT),
                    ("connected", Type::TEXT),
                    ("last_seen_millis_ago", Type::INT8),
                    ("sent_watermark", Type::INT8),
                    ("schema_agreed", Type::TEXT),
                    ("push_mode", Type::TEXT),
                ]),
                "REPLICATION" => Some(vec![
                    ("running", Type::TEXT),
                    ("address", Type::TEXT),
                    ("peers", Type::INT8),
                    ("members", Type::INT8),
                    ("staged_transactions", Type::INT8),
                    ("applied_transactions", Type::INT8),
                    ("last_error", Type::TEXT),
                    ("committed", Type::INT8),
                ]),
                "RETENTION" => Some(vec![
                    ("node_id", Type::TEXT),
                    ("last_seen_millis", Type::INT8),
                    ("retired", Type::TEXT),
                    ("origin", Type::TEXT),
                    ("acknowledged", Type::INT8),
                    ("collected_transactions", Type::INT8),
                    ("last_collection_millis", Type::INT8),
                ]),
                "COLUMNS" if words.front().is_some_and(|w| w.eq_ignore_ascii_case("VECTOR")) => Some(vec![
                    ("table", Type::TEXT), ("column", Type::TEXT), ("enabled", Type::TEXT),
                    ("dimensions", Type::INT8), ("distance", Type::TEXT), ("index_name", Type::TEXT),
                    ("ready", Type::TEXT), ("rebuilding", Type::TEXT), ("pending_keys", Type::INT8),
                    ("queued_bytes", Type::INT8), ("error", Type::TEXT),
                ]),
                "COLUMNS" => Some(vec![
                    ("table", Type::TEXT), ("column", Type::TEXT), ("enabled", Type::TEXT),
                    ("index_name", Type::TEXT), ("ready", Type::TEXT), ("rebuilding", Type::TEXT),
                    ("pending_keys", Type::INT8), ("queued_bytes", Type::INT8), ("error", Type::TEXT),
                ]),
                "METRICS" => Some(vec![
                    ("subsystem", Type::TEXT),
                    ("metric", Type::TEXT),
                    ("value", Type::TEXT),
                ]),
                _ => None,
            }
        }
        "RETENTION" => {
            let sub = words.pop_front()?.to_ascii_uppercase();
            match sub.as_str() {
                "COLLECT" => Some(vec![("collected", Type::INT8)]),
                _ => None,
            }
        }
        "BRIDGE" => {
            let sub = words.pop_front()?.to_ascii_uppercase();
            match sub.as_str() {
                "RELEASE" => Some(vec![("released", Type::TEXT)]),
                _ => None,
            }
        }
        "BACKUP" => {
            let sub = words.pop_front()?.to_ascii_uppercase();
            match sub.as_str() {
                "SAVE" => Some(vec![("name", Type::TEXT), ("size", Type::INT8)]),
                "LIST" => Some(vec![
                    ("name", Type::TEXT),
                    ("size", Type::INT8),
                    ("modified", Type::TEXT),
                ]),
                "UPLOAD" => match words.pop_front()?.to_ascii_uppercase().as_str() {
                    "FINISH" => Some(vec![("name", Type::TEXT), ("size", Type::INT8)]),
                    _ => None,
                },
                "DOWNLOAD" => Some(vec![
                    ("offset", Type::INT8),
                    ("total", Type::INT8),
                    ("chunk", Type::BYTEA),
                ]),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Split a command into words. Single-quoted segments (with `''` escapes)
/// stay whole; double quotes are literal characters, not quoting.
fn tokenize(command: &str) -> PgWireResult<std::collections::VecDeque<String>> {
    let mut words = std::collections::VecDeque::new();
    let mut current = String::new();
    let mut chars = command.chars().peekable();
    let mut in_word = false;
    while let Some(c) = chars.next() {
        if c == '\'' {
            in_word = true;
            loop {
                match chars.next() {
                    None => {
                        return Err(user_error("42601", "unterminated quoted string".to_string()));
                    }
                    Some('\'') if chars.peek() == Some(&'\'') => {
                        chars.next();
                        current.push('\'');
                    }
                    Some('\'') => break,
                    Some(other) => current.push(other),
                }
            }
        } else if c.is_whitespace() || c == ';' {
            if in_word {
                words.push_back(std::mem::take(&mut current));
                in_word = false;
            }
        } else {
            in_word = true;
            current.push(c);
        }
    }
    if in_word {
        words.push_back(current);
    }
    Ok(words)
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_handles_quotes_and_semicolons() {
        let words = tokenize("UNLOCK KEY 'a''b; c';").unwrap();
        assert_eq!(words.into_iter().collect::<Vec<_>>(), ["UNLOCK", "KEY", "a'b; c"]);
        assert!(tokenize("UNLOCK 'abc").is_err());
    }

    #[test]
    fn hex_and_passphrase_keys() {
        let hex = "ab".repeat(32);
        let key = secret_to_key(&hex);
        assert_eq!(key, [0xab; 32]);
        assert_eq!(secret_to_key("words"), *blake3::hash(b"words").as_bytes());
        // Odd lengths and non-hex fall back to passphrase hashing.
        assert_eq!(secret_to_key("abc"), *blake3::hash(b"abc").as_bytes());
    }
}
