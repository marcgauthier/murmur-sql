//! Pure-Rust async client for Murmur over the PostgreSQL wire protocol.
//!
//! Built on `tokio-postgres`; it does not link the Murmur engine. Application
//! SQL goes through [`Client::query`], [`Client::execute`] or the underlying
//! [`tokio_postgres::Client`] ([`Client::inner`]). MURMUR administration
//! commands are helper methods that only issue MURMUR SQL.
//!
//! The server autocommits every statement; `BEGIN`/`COMMIT`/`ROLLBACK` are
//! accepted no-ops, so there is no multi-statement rollback.

mod search;
mod tls;
pub use search::*;

use std::time::Duration;

use base64::Engine as _;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinHandle;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{NoTls, Row, SimpleQueryMessage, config::SslMode};

pub use tokio_postgres;

const UPLOAD_CHUNK: usize = 256 << 10;
const DOWNLOAD_CHUNK: usize = 1 << 20;
const FILE_CHUNK: usize = 64 << 10;

/// Client error. Use [`Error::code`] for the Postgres SQLSTATE.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid [`Config`] or argument, detected locally.
    #[error("murmur: {0}")]
    Invalid(String),
    /// Database or driver error.
    #[error(transparent)]
    Db(#[from] tokio_postgres::Error),
    /// TLS setup error.
    #[error("murmur: tls: {0}")]
    Tls(String),
    /// Local file or stream error.
    #[error("murmur: io: {0}")]
    Io(#[from] std::io::Error),
    /// Unexpected server response.
    #[error("murmur: protocol: {0}")]
    Protocol(String),
}

impl Error {
    /// SQLSTATE carried by the server error, if any. Useful codes: `28000`
    /// (locked, wrong key, TLS required), `42P01` (unknown table), `23505`
    /// (unique violation), `42601` (syntax), `55000` (remote-only file).
    pub fn code(&self) -> Option<&str> {
        match self {
            Error::Db(e) => e.code().map(|c| c.code()),
            _ => None,
        }
    }

    /// True for the locked-connection error (statement before `MURMUR UNLOCK`).
    pub fn is_locked(&self) -> bool {
        match self {
            Error::Db(e) => {
                e.code().map(|c| c.code()) == Some("28000")
                    && e.as_db_error()
                        .is_some_and(|d| d.message().contains("locked"))
            }
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// TLS mode mirroring Postgres `sslmode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TlsMode {
    /// Plaintext (default).
    #[default]
    Disable,
    /// Encrypted, server certificate not verified.
    Require,
    /// Chain verified against `ca_file`; host name not checked.
    VerifyCa,
    /// Chain and host name verified against `ca_file`.
    VerifyFull,
}

/// TLS settings. `ca_file` is required for `VerifyCa`/`VerifyFull`;
/// `cert_file` and `key_file` (PEM) enable a client certificate and must be set together.
#[derive(Clone, Debug, Default)]
pub struct TlsConfig {
    pub mode: TlsMode,
    pub ca_file: String,
    pub cert_file: String,
    pub key_file: String,
}

/// Connection settings.
#[derive(Clone, Debug)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    /// Encryption key or passphrase for `MURMUR UNLOCK` (required).
    pub key: String,
    pub tls: TlsConfig,
    pub connect_timeout: Duration,
}

impl Config {
    /// Config with defaults (`127.0.0.1:5432`, database `murmur`, 10 s timeout).
    pub fn new(
        user: impl Into<String>,
        password: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5432,
            database: "murmur".into(),
            user: user.into(),
            password: password.into(),
            key: key.into(),
            tls: TlsConfig::default(),
            connect_timeout: Duration::from_secs(10),
        }
    }

    fn validate(&self) -> Result<()> {
        let bad = |m: &str| Err(Error::Invalid(m.to_string()));
        if self.user.is_empty() {
            return bad("user is required");
        }
        if self.password.is_empty() {
            return bad("password is required");
        }
        if self.key.is_empty() {
            return bad("key is required");
        }
        if self.port == 0 {
            return bad("invalid port 0");
        }
        if matches!(self.tls.mode, TlsMode::VerifyCa | TlsMode::VerifyFull)
            && self.tls.ca_file.is_empty()
        {
            return bad("verify-ca and verify-full need ca_file");
        }
        if self.tls.cert_file.is_empty() != self.tls.key_file.is_empty() {
            return bad("cert_file and key_file must be set together");
        }
        Ok(())
    }
}

/// An archive in the server's backup folder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupFile {
    pub name: String,
    pub size: i64,
    pub modified: String,
}

/// A table and its replication flag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableFlag {
    pub table: String,
    pub replicated: bool,
}

/// One measurement from the SQLite or Spool engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Metric {
    pub subsystem: String,
    pub metric: String,
    pub value: String,
}

/// A logical file; content lives in encrypted disk objects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileInfo {
    pub name: String,
    pub size: i64,
    pub modified_millis: i64,
    pub sha256: String,
    pub local: bool,
}

/// One connected replication peer from `MURMUR SHOW PEERS`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerInfo {
    pub node_id: String,
    pub address: Option<String>,
    pub connected: bool,
    pub last_seen_millis_ago: Option<i64>,
    pub sent_watermark: i64,
    pub schema_agreed: bool,
    pub push_mode: bool,
}

/// Local replication summary from `MURMUR SHOW REPLICATION` (always one row).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationStatus {
    pub running: bool,
    pub address: Option<String>,
    pub peers: i64,
    pub members: i64,
    pub staged_transactions: i64,
    pub applied_transactions: i64,
    pub last_error: Option<String>,
    /// Local engine committed-batch cursor.
    pub committed: i64,
}

/// One row from `MURMUR SHOW RETENTION`: a peer's acknowledgement of one
/// origin. Peers with no acknowledgements yield one row with `origin` and
/// `acknowledged` unset; the trailing counters repeat on every row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionEntry {
    pub node_id: String,
    pub last_seen_millis: i64,
    pub retired: bool,
    pub origin: Option<String>,
    pub acknowledged: Option<i64>,
    pub collected_transactions: i64,
    pub last_collection_millis: i64,
}

/// A `MIN`/`MAX` policy literal: INTEGER, finite REAL, or TEXT.
#[derive(Clone, Debug, PartialEq)]
pub enum PolicyValue {
    Integer(i64),
    Real(f64),
    Text(String),
}

impl From<i64> for PolicyValue {
    fn from(v: i64) -> Self {
        PolicyValue::Integer(v)
    }
}

impl From<i32> for PolicyValue {
    fn from(v: i32) -> Self {
        PolicyValue::Integer(i64::from(v))
    }
}

impl From<f64> for PolicyValue {
    fn from(v: f64) -> Self {
        PolicyValue::Real(v)
    }
}

impl From<String> for PolicyValue {
    fn from(v: String) -> Self {
        PolicyValue::Text(v)
    }
}

impl From<&str> for PolicyValue {
    fn from(v: &str) -> Self {
        PolicyValue::Text(v.to_string())
    }
}

/// A policy-op row key: one TEXT key (legacy form) or a composite
/// KEY ... VALUE list of INTEGER, REAL, or TEXT components.
#[derive(Clone, Debug, PartialEq)]
pub enum PolicyKey {
    One(String),
    Many(Vec<PolicyValue>),
}

impl From<&str> for PolicyKey {
    fn from(v: &str) -> Self {
        PolicyKey::One(v.to_string())
    }
}

impl From<String> for PolicyKey {
    fn from(v: String) -> Self {
        PolicyKey::One(v)
    }
}

impl From<Vec<PolicyValue>> for PolicyKey {
    fn from(v: Vec<PolicyValue>) -> Self {
        PolicyKey::Many(v)
    }
}

impl<const N: usize> From<[PolicyValue; N]> for PolicyKey {
    fn from(v: [PolicyValue; N]) -> Self {
        PolicyKey::Many(v.to_vec())
    }
}

/// `'...'` with embedded quotes doubled, for MURMUR command words.
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn check_backup_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 255
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(Error::Invalid(format!("invalid backup name {name:?}")))
    }
}

fn check_node_id(node: &str) -> Result<()> {
    if node.len() == 32 && node.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(Error::Invalid(format!(
            "node id must be 32 hex characters, not {node:?}"
        )))
    }
}

fn check_policy_target(table: &str, column: &str) -> Result<()> {
    if table.trim().is_empty() || column.trim().is_empty() {
        return Err(Error::Invalid(
            "policy table and column must not be empty".into(),
        ));
    }
    Ok(())
}

fn check_bridge_target(table: &str, column: Option<&str>) -> Result<()> {
    if table.trim().is_empty() || matches!(column, Some(name) if name.trim().is_empty()) {
        return Err(Error::Invalid(
            "bridge table and column must not be empty".into(),
        ));
    }
    Ok(())
}

/// Render a `MIN`/`MAX` literal the way the server parses it: integers bare,
/// finite reals bare (never integer-looking, so `100.0` stays REAL), text quoted.
fn policy_literal(value: &PolicyValue) -> Result<String> {
    match value {
        PolicyValue::Integer(n) => Ok(n.to_string()),
        PolicyValue::Real(f) => {
            if !f.is_finite() {
                return Err(Error::Invalid(format!(
                    "policy value must be finite, not {f:?}"
                )));
            }
            Ok(format!("{f:?}"))
        }
        PolicyValue::Text(s) => Ok(quote(s)),
    }
}

/// Render a policy key span plus the value separator: `(key_span,
/// value_prefix)`. Single keys keep the legacy form.
fn policy_key_span(key: &PolicyKey) -> Result<(String, &'static str)> {
    match key {
        PolicyKey::One(s) => Ok((quote(s), "")),
        PolicyKey::Many(parts) => {
            if parts.is_empty() {
                return Err(Error::Invalid(
                    "policy key needs at least one component".into(),
                ));
            }
            let mut span = String::from("KEY");
            for p in parts {
                span.push(' ');
                span.push_str(&policy_literal(p)?);
            }
            Ok((span, "VALUE "))
        }
    }
}

/// Render a policy key span with the KEY marker always present, for
/// operations without a legacy form.
fn policy_key_marked(key: &PolicyKey) -> Result<String> {
    match key {
        PolicyKey::One(s) => Ok(format!("KEY {}", quote(s))),
        PolicyKey::Many(parts) => {
            if parts.is_empty() {
                return Err(Error::Invalid(
                    "policy key needs at least one component".into(),
                ));
            }
            let mut span = String::from("KEY");
            for p in parts {
                span.push(' ');
                span.push_str(&policy_literal(p)?);
            }
            Ok(span)
        }
    }
}

fn check_policy_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(Error::Invalid("json path must not be empty".into()));
    }
    Ok(())
}

fn text_bool(value: &str) -> bool {
    matches!(value, "t" | "true")
}

/// Simple-protocol NULLs arrive as empty strings.
fn text_opt(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn text_int(value: &str, column: &'static str) -> Result<i64> {
    value
        .parse()
        .map_err(|_| Error::Protocol(format!("bad {column} {value:?}")))
}

fn text_opt_int(value: &str, column: &'static str) -> Result<Option<i64>> {
    if value.is_empty() {
        Ok(None)
    } else {
        text_int(value, column).map(Some)
    }
}

fn col_i64(row: &Row, idx: usize) -> Result<i64> {
    let ty = row.columns()[idx].type_();
    let v = if *ty == Type::INT8 {
        row.try_get::<_, i64>(idx)?
    } else if *ty == Type::INT4 {
        i64::from(row.try_get::<_, i32>(idx)?)
    } else if *ty == Type::INT2 {
        i64::from(row.try_get::<_, i16>(idx)?)
    } else {
        row.try_get::<_, String>(idx)?
            .parse()
            .map_err(|_| Error::Protocol(format!("column {idx} is not an integer")))?
    };
    Ok(v)
}

fn col_bool(row: &Row, idx: usize) -> Result<bool> {
    if *row.columns()[idx].type_() == Type::BOOL {
        Ok(row.try_get::<_, bool>(idx)?)
    } else {
        Ok(matches!(
            row.try_get::<_, String>(idx)?.as_str(),
            "t" | "true"
        ))
    }
}

fn file_info(row: &Row, base: usize) -> Result<FileInfo> {
    Ok(FileInfo {
        name: row.try_get(base)?,
        size: col_i64(row, base + 1)?,
        modified_millis: col_i64(row, base + 2)?,
        sha256: row.try_get(base + 3)?,
        local: col_bool(row, base + 4)?,
    })
}

/// One unlocked Murmur connection. Clone-free; share it behind an `Arc`.
pub struct Client {
    inner: tokio_postgres::Client,
    task: JoinHandle<()>,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Client {
    /// Connect, verify the server, and run `MURMUR UNLOCK` with `cfg.key`.
    /// A wrong key fails here with SQLSTATE `28000`.
    pub async fn connect(cfg: &Config) -> Result<Client> {
        cfg.validate()?;
        let mut pg = tokio_postgres::Config::new();
        pg.host(&cfg.host)
            .port(cfg.port)
            .user(&cfg.user)
            .password(&cfg.password)
            .dbname(&cfg.database)
            .connect_timeout(cfg.connect_timeout);
        let (inner, task) = if cfg.tls.mode == TlsMode::Disable {
            pg.ssl_mode(SslMode::Disable);
            let (client, conn) = pg.connect(NoTls).await?;
            (
                client,
                tokio::spawn(async move {
                    let _ = conn.await;
                }),
            )
        } else {
            pg.ssl_mode(SslMode::Require);
            let connector =
                tokio_postgres_rustls::MakeRustlsConnect::new(tls::client_config(&cfg.tls)?);
            let (client, conn) = pg.connect(connector).await?;
            (
                client,
                tokio::spawn(async move {
                    let _ = conn.await;
                }),
            )
        };
        let client = Client { inner, task };
        client
            .simple(&format!("MURMUR UNLOCK KEY {}", quote(&cfg.key)))
            .await?;
        Ok(client)
    }

    /// The underlying `tokio-postgres` client for application SQL.
    pub fn inner(&self) -> &tokio_postgres::Client {
        &self.inner
    }

    /// Run a query with `$1`, `$2`, ... parameters.
    pub async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>> {
        Ok(self.inner.query(sql, params).await?)
    }

    /// Run a statement with parameters and return the affected row count.
    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        Ok(self.inner.execute(sql, params).await?)
    }

    /// Run a parameterless command through the simple protocol; every value
    /// is returned as text (`NULL` becomes an empty string).
    pub async fn simple(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        let mut out = Vec::new();
        for msg in self.inner.simple_query(sql).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                out.push(
                    (0..row.len())
                        .map(|i| row.get(i).unwrap_or("").to_string())
                        .collect(),
                );
            }
        }
        Ok(out)
    }

    async fn typed(&self, sql: &str, params: &[(&(dyn ToSql + Sync), Type)]) -> Result<Vec<Row>> {
        let rows = self.inner.query_typed(sql, params).await?;
        Ok(rows)
    }

    /// Wait for a durable checkpoint of everything written so far.
    pub async fn flush(&self) -> Result<()> {
        self.simple("MURMUR FLUSH").await.map(|_| ())
    }

    /// Enable or disable replication for `table` (default: local-only).
    /// Enabling rebuilds the table with a read-only `id_murmur_primary_key`
    /// UUIDv5 key; disabling restores the application primary key.
    pub async fn set_table_replicated(&self, table: &str, replicated: bool) -> Result<()> {
        if table.trim().is_empty() {
            return Err(Error::Invalid("table name must not be empty".into()));
        }
        self.simple(&format!(
            "MURMUR SET TABLE {} REPLICATED {replicated}",
            quote(table)
        ))
        .await
        .map(|_| ())
    }

    /// Every known table and its replication flag.
    pub async fn table_replication(&self) -> Result<Vec<TableFlag>> {
        Ok(self
            .simple("MURMUR SHOW TABLES REPLICATION")
            .await?
            .into_iter()
            .filter(|r| r.len() >= 2)
            .map(|r| TableFlag {
                replicated: r[1] == "true" || r[1] == "t",
                table: r[0].clone(),
            })
            .collect())
    }

    /// Live SQLite and Spool metrics; `scope` may be `"sqlite"`, `"spool"` or empty.
    pub async fn metrics(&self, scope: &str) -> Result<Vec<Metric>> {
        let mut cmd = "MURMUR SHOW METRICS".to_string();
        if !scope.trim().is_empty() {
            cmd.push(' ');
            cmd.push_str(&scope.trim().to_uppercase());
        }
        Ok(self
            .simple(&cmd)
            .await?
            .into_iter()
            .filter(|r| r.len() >= 3)
            .map(|r| Metric {
                subsystem: r[0].clone(),
                metric: r[1].clone(),
                value: r[2].clone(),
            })
            .collect())
    }

    /// Rewrap the key registry under `new`; reconnect with the new key afterwards.
    pub async fn rotate_key(&self, previous: &str, new: &str) -> Result<()> {
        if previous.is_empty() || new.is_empty() {
            return Err(Error::Invalid(
                "rotate needs non-empty previous and new keys".into(),
            ));
        }
        if previous == new {
            return Err(Error::Invalid(
                "new key must differ from the previous key".into(),
            ));
        }
        self.simple(&format!(
            "MURMUR ROTATE KEY {} {}",
            quote(previous),
            quote(new)
        ))
        .await
        .map(|_| ())
    }

    /// Checkpoint the live database into a datetime-named archive.
    pub async fn backup_save(&self) -> Result<BackupFile> {
        let rows = self.simple("MURMUR BACKUP SAVE").await?;
        let r = rows
            .first()
            .filter(|r| r.len() >= 2)
            .ok_or_else(|| Error::Protocol("empty BACKUP SAVE reply".into()))?;
        Ok(BackupFile {
            name: r[0].clone(),
            size: r[1]
                .parse()
                .map_err(|_| Error::Protocol("bad backup size".into()))?,
            modified: String::new(),
        })
    }

    /// Backups available to restore, sorted by name.
    pub async fn backup_list(&self) -> Result<Vec<BackupFile>> {
        self.simple("MURMUR BACKUP LIST")
            .await?
            .into_iter()
            .filter(|r| r.len() >= 3)
            .map(|r| {
                Ok(BackupFile {
                    size: r[1]
                        .parse()
                        .map_err(|_| Error::Protocol("bad backup size".into()))?,
                    name: r[0].clone(),
                    modified: r[2].clone(),
                })
            })
            .collect()
    }

    /// Stream `source` into the server's backup folder; published on FINISH only.
    pub async fn backup_upload<R: AsyncRead + Unpin>(
        &self,
        name: &str,
        mut source: R,
    ) -> Result<()> {
        check_backup_name(name)?;
        let q = quote(name);
        self.simple(&format!("MURMUR BACKUP UPLOAD BEGIN {q}"))
            .await?;
        let result: Result<()> = async {
            let mut buf = vec![0u8; UPLOAD_CHUNK];
            loop {
                let n = read_full(&mut source, &mut buf).await?;
                if n == 0 {
                    break;
                }
                let encoded = base64::engine::general_purpose::STANDARD.encode(&buf[..n]);
                self.simple(&format!("MURMUR BACKUP UPLOAD APPEND {q} '{encoded}'"))
                    .await?;
                if n < buf.len() {
                    break;
                }
            }
            self.simple(&format!("MURMUR BACKUP UPLOAD FINISH {q}"))
                .await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = self
                .simple(&format!("MURMUR BACKUP UPLOAD ABORT {q}"))
                .await;
        }
        result
    }

    /// Stream a backup file from the server into `dest`.
    pub async fn backup_download<W: AsyncWrite + Unpin>(
        &self,
        name: &str,
        mut dest: W,
    ) -> Result<()> {
        check_backup_name(name)?;
        let mut offset: i64 = 0;
        loop {
            let sql = format!(
                "MURMUR BACKUP DOWNLOAD {} OFFSET {offset} LIMIT {DOWNLOAD_CHUNK}",
                quote(name)
            );
            let rows = self.typed(&sql, &[]).await?;
            let row = rows
                .first()
                .ok_or_else(|| Error::Protocol("empty BACKUP DOWNLOAD reply".into()))?;
            let (off, total) = (col_i64(row, 0)?, col_i64(row, 1)?);
            let chunk: Vec<u8> = row.try_get(2)?;
            if chunk.is_empty() {
                break;
            }
            dest.write_all(&chunk).await?;
            let next = off + chunk.len() as i64;
            if next >= total || next <= offset {
                break;
            }
            offset = next;
        }
        dest.flush().await?;
        Ok(())
    }

    /// Live-restore a backup; `key` re-authenticates this destructive step.
    pub async fn restore(&self, name: &str, key: &str) -> Result<()> {
        check_backup_name(name)?;
        if key.is_empty() {
            return Err(Error::Invalid("restore needs a non-empty key".into()));
        }
        self.simple(&format!(
            "MURMUR RESTORE FROM {} KEY {}",
            quote(name),
            quote(key)
        ))
        .await
        .map(|_| ())
    }

    /// Snapshot cursor over every logical file, sorted by name. Call
    /// [`FileCursor::close`] when finished early.
    pub async fn list_files(&self) -> Result<FileCursor<'_>> {
        FileCursor::open(self, "MURMUR FILE LIST", None).await
    }

    /// Case-sensitive LIKE search (`%`, `_`; backslash escapes).
    pub async fn search_files(&self, pattern: &str) -> Result<FileCursor<'_>> {
        FileCursor::open(self, "MURMUR FILE SEARCH $1", Some(pattern)).await
    }

    /// Upload bounded BYTEA chunks; a replacement is visible only after FINISH.
    pub async fn upload_file<R: AsyncRead + Unpin>(
        &self,
        name: &str,
        mut source: R,
    ) -> Result<FileInfo> {
        let rows = self
            .typed("MURMUR FILE UPLOAD BEGIN $1", &[(&name, Type::TEXT)])
            .await?;
        let token: String = rows
            .first()
            .ok_or_else(|| Error::Protocol("empty UPLOAD BEGIN reply".into()))?
            .try_get(0)?;
        let result: Result<FileInfo> = async {
            let mut buf = vec![0u8; FILE_CHUNK];
            loop {
                let n = read_full(&mut source, &mut buf).await?;
                if n == 0 {
                    break;
                }
                let chunk = &buf[..n];
                self.typed(
                    "MURMUR FILE UPLOAD APPEND $1 $2",
                    &[(&token, Type::TEXT), (&chunk, Type::BYTEA)],
                )
                .await?;
                if n < buf.len() {
                    break;
                }
            }
            let rows = self
                .typed("MURMUR FILE UPLOAD FINISH $1", &[(&token, Type::TEXT)])
                .await?;
            file_info(
                rows.first()
                    .ok_or_else(|| Error::Protocol("empty UPLOAD FINISH reply".into()))?,
                0,
            )
        }
        .await;
        if result.is_err() {
            let _ = self
                .typed("MURMUR FILE UPLOAD ABORT $1", &[(&token, Type::TEXT)])
                .await;
        }
        result
    }

    /// Stream the pinned file version into `dest`. Remote-only content fails
    /// with SQLSTATE `55000`; retry after the background peer fetch completes.
    pub async fn download_file<W: AsyncWrite + Unpin>(
        &self,
        name: &str,
        mut dest: W,
    ) -> Result<()> {
        let rows = self
            .typed("MURMUR FILE DOWNLOAD $1", &[(&name, Type::TEXT)])
            .await?;
        let head = rows
            .first()
            .ok_or_else(|| Error::Protocol("empty DOWNLOAD reply".into()))?;
        let token: String = head.try_get(0)?;
        let size = col_i64(head, 2)?;
        let result: Result<()> = async {
            let mut position: i64 = 0;
            loop {
                let rows = self
                    .typed("MURMUR FILE DOWNLOAD NEXT $1", &[(&token, Type::TEXT)])
                    .await?;
                let row = rows
                    .first()
                    .ok_or_else(|| Error::Protocol("empty DOWNLOAD NEXT reply".into()))?;
                let (offset, total) = (col_i64(row, 0)?, col_i64(row, 1)?);
                let chunk: Vec<u8> = row.try_get(2)?;
                let eof = col_bool(row, 3)?;
                if offset != position
                    || total != size
                    || chunk.len() as i64 > size - position
                    || (!eof && chunk.is_empty())
                {
                    return Err(Error::Protocol("inconsistent file download".into()));
                }
                dest.write_all(&chunk).await?;
                position += chunk.len() as i64;
                if eof {
                    if position != size {
                        return Err(Error::Protocol("unexpected end of file download".into()));
                    }
                    dest.flush().await?;
                    return Ok(());
                }
            }
        }
        .await;
        let _ = self
            .typed("MURMUR FILE CLOSE $1", &[(&token, Type::TEXT)])
            .await;
        result
    }
}

impl Client {
    /// Dial `addr` (`ip:port`) and admit the peer to the mesh, clearing any
    /// earlier `REMOVE` exclusion for its node id. Succeeds once queued; dial
    /// failures surface in [`show_replication`](Self::show_replication).
    /// Without enabled replication the server fails with `55000`.
    pub async fn peer_add(&self, addr: &str) -> Result<()> {
        if addr.parse::<std::net::SocketAddr>().is_err() {
            return Err(Error::Invalid(format!("invalid peer address {addr:?}")));
        }
        self.simple(&format!("MURMUR PEER ADD {}", quote(addr)))
            .await
            .map(|_| ())
    }

    /// Disconnect and durably exclude `node` (32 hex chars); reversible with
    /// [`peer_add`](Self::peer_add). The peer's retention obligation is kept.
    pub async fn peer_remove(&self, node: &str) -> Result<()> {
        check_node_id(node)?;
        self.simple(&format!("MURMUR PEER REMOVE {}", quote(node)))
            .await
            .map(|_| ())
    }

    /// Permanently exclude `node` and release its retention obligation.
    /// There is no un-retire; retiring the local node fails with `22023`.
    pub async fn peer_retire(&self, node: &str) -> Result<()> {
        check_node_id(node)?;
        self.simple(&format!("MURMUR PEER RETIRE {}", quote(node)))
            .await
            .map(|_| ())
    }

    /// Ask every connected peer for progress now. It only requests state;
    /// it does not wait for convergence.
    pub async fn peer_sync(&self) -> Result<()> {
        self.simple("MURMUR PEER SYNC").await.map(|_| ())
    }

    /// Connected peers ordered by node id (empty when peerless).
    pub async fn show_peers(&self) -> Result<Vec<PeerInfo>> {
        self.simple("MURMUR SHOW PEERS")
            .await?
            .into_iter()
            .filter(|r| r.len() >= 7)
            .map(|r| {
                Ok(PeerInfo {
                    node_id: r[0].clone(),
                    address: text_opt(r[1].clone()),
                    connected: text_bool(&r[2]),
                    last_seen_millis_ago: text_opt_int(&r[3], "last_seen_millis_ago")?,
                    sent_watermark: text_int(&r[4], "sent_watermark")?,
                    schema_agreed: text_bool(&r[5]),
                    push_mode: text_bool(&r[6]),
                })
            })
            .collect()
    }

    /// The local replication summary (one row).
    pub async fn show_replication(&self) -> Result<ReplicationStatus> {
        let rows = self.simple("MURMUR SHOW REPLICATION").await?;
        let r = rows
            .first()
            .filter(|r| r.len() >= 8)
            .ok_or_else(|| Error::Protocol("empty SHOW REPLICATION reply".into()))?;
        Ok(ReplicationStatus {
            running: text_bool(&r[0]),
            address: text_opt(r[1].clone()),
            peers: text_int(&r[2], "peers")?,
            members: text_int(&r[3], "members")?,
            staged_transactions: text_int(&r[4], "staged_transactions")?,
            applied_transactions: text_int(&r[5], "applied_transactions")?,
            last_error: text_opt(r[6].clone()),
            committed: text_int(&r[7], "committed")?,
        })
    }

    /// Per-peer retention state, ordered by node id then origin.
    pub async fn show_retention(&self) -> Result<Vec<RetentionEntry>> {
        self.simple("MURMUR SHOW RETENTION")
            .await?
            .into_iter()
            .filter(|r| r.len() >= 7)
            .map(|r| {
                Ok(RetentionEntry {
                    node_id: r[0].clone(),
                    last_seen_millis: text_int(&r[1], "last_seen_millis")?,
                    retired: text_bool(&r[2]),
                    origin: text_opt(r[3].clone()),
                    acknowledged: text_opt_int(&r[4], "acknowledged")?,
                    collected_transactions: text_int(&r[5], "collected_transactions")?,
                    last_collection_millis: text_int(&r[6], "last_collection_millis")?,
                })
            })
            .collect()
    }

    /// Run one bounded retention pass now; returns deleted log records.
    pub async fn retention_collect(&self) -> Result<i64> {
        let rows = self.simple("MURMUR RETENTION COLLECT").await?;
        let r = rows
            .first()
            .filter(|r| !r.is_empty())
            .ok_or_else(|| Error::Protocol("empty RETENTION COLLECT reply".into()))?;
        text_int(&r[0], "collected")
    }

    /// Add `delta` to a `counter` column on the `key` row, addressed by
    /// one TEXT key or a composite key list. The column must carry the
    /// matching `replication.merge_policies` entry (`22023` otherwise); a
    /// missing row fails with `02000`.
    pub async fn policy_counter_add(
        &self,
        table: &str,
        column: &str,
        key: impl Into<PolicyKey>,
        delta: i64,
    ) -> Result<()> {
        check_policy_target(table, column)?;
        let (ks, vp) = policy_key_span(&key.into())?;
        self.simple(&format!(
            "MURMUR POLICY COUNTER_ADD {} {} {} {vp}{delta}",
            quote(table),
            quote(column),
            ks
        ))
        .await
        .map(|_| ())
    }

    /// Add a TEXT element to a `set` column (see
    /// [`policy_counter_add`](Self::policy_counter_add) for requirements).
    pub async fn policy_set_add(
        &self,
        table: &str,
        column: &str,
        key: impl Into<PolicyKey>,
        value: &str,
    ) -> Result<()> {
        check_policy_target(table, column)?;
        let (ks, vp) = policy_key_span(&key.into())?;
        self.simple(&format!(
            "MURMUR POLICY SET_ADD {} {} {} {vp}{}",
            quote(table),
            quote(column),
            ks,
            quote(value)
        ))
        .await
        .map(|_| ())
    }

    /// Remove a TEXT element from a `set` column.
    pub async fn policy_set_remove(
        &self,
        table: &str,
        column: &str,
        key: impl Into<PolicyKey>,
        value: &str,
    ) -> Result<()> {
        check_policy_target(table, column)?;
        let (ks, vp) = policy_key_span(&key.into())?;
        self.simple(&format!(
            "MURMUR POLICY SET_REMOVE {} {} {} {vp}{}",
            quote(table),
            quote(column),
            ks,
            quote(value)
        ))
        .await
        .map(|_| ())
    }

    /// Lower a `min` column towards `value` (INTEGER, finite REAL, or TEXT).
    pub async fn policy_min(
        &self,
        table: &str,
        column: &str,
        key: impl Into<PolicyKey>,
        value: PolicyValue,
    ) -> Result<()> {
        check_policy_target(table, column)?;
        let literal = policy_literal(&value)?;
        let (ks, vp) = policy_key_span(&key.into())?;
        self.simple(&format!(
            "MURMUR POLICY MIN {} {} {} {vp}{literal}",
            quote(table),
            quote(column),
            ks
        ))
        .await
        .map(|_| ())
    }

    /// Raise a `max` column towards `value` (INTEGER, finite REAL, or TEXT).
    pub async fn policy_max(
        &self,
        table: &str,
        column: &str,
        key: impl Into<PolicyKey>,
        value: PolicyValue,
    ) -> Result<()> {
        check_policy_target(table, column)?;
        let literal = policy_literal(&value)?;
        let (ks, vp) = policy_key_span(&key.into())?;
        self.simple(&format!(
            "MURMUR POLICY MAX {} {} {} {vp}{literal}",
            quote(table),
            quote(column),
            ks
        ))
        .await
        .map(|_| ())
    }

    /// Write one flat path of a `json` column. `None` writes JSON null.
    pub async fn policy_json_set(
        &self,
        table: &str,
        column: &str,
        key: impl Into<PolicyKey>,
        path: &str,
        value: Option<PolicyValue>,
    ) -> Result<()> {
        check_policy_target(table, column)?;
        check_policy_path(path)?;
        let literal = match value {
            None => "NULL".to_string(),
            Some(v) => policy_literal(&v)?,
        };
        let ks = policy_key_marked(&key.into())?;
        self.simple(&format!(
            "MURMUR POLICY JSON_SET {} {} {} PATH {} VALUE {literal}",
            quote(table),
            quote(column),
            ks,
            quote(path)
        ))
        .await
        .map(|_| ())
    }

    /// Delete one flat path of a `json` column.
    pub async fn policy_json_remove(
        &self,
        table: &str,
        column: &str,
        key: impl Into<PolicyKey>,
        path: &str,
    ) -> Result<()> {
        check_policy_target(table, column)?;
        check_policy_path(path)?;
        let ks = policy_key_marked(&key.into())?;
        self.simple(&format!(
            "MURMUR POLICY JSON_REMOVE {} {} {} PATH {}",
            quote(table),
            quote(column),
            ks,
            quote(path)
        ))
        .await
        .map(|_| ())
    }

    /// Append one element to a `list` column. `None` appends JSON null.
    pub async fn policy_list_append(
        &self,
        table: &str,
        column: &str,
        key: impl Into<PolicyKey>,
        value: Option<PolicyValue>,
    ) -> Result<()> {
        check_policy_target(table, column)?;
        let literal = match value {
            None => "NULL".to_string(),
            Some(v) => policy_literal(&v)?,
        };
        let (ks, vp) = policy_key_span(&key.into())?;
        self.simple(&format!(
            "MURMUR POLICY LIST_APPEND {} {} {} {vp}{literal}",
            quote(table),
            quote(column),
            ks
        ))
        .await
        .map(|_| ())
    }

    /// Drop every list element equal to `value` (`None` matches JSON null).
    pub async fn policy_list_remove(
        &self,
        table: &str,
        column: &str,
        key: impl Into<PolicyKey>,
        value: Option<PolicyValue>,
    ) -> Result<()> {
        check_policy_target(table, column)?;
        let literal = match value {
            None => "NULL".to_string(),
            Some(v) => policy_literal(&v)?,
        };
        let (ks, vp) = policy_key_span(&key.into())?;
        self.simple(&format!(
            "MURMUR POLICY LIST_REMOVE {} {} {} {vp}{literal}",
            quote(table),
            quote(column),
            ks
        ))
        .await
        .map(|_| ())
    }

    /// Release High ownership of one bridge-shielded field on the
    /// single-TEXT-key `key` row, so later Low updates flow through again.
    /// Without `column` the whole-row ownership is released. Returns `true`
    /// when applied; `false` means the engine was busy, retry. Without
    /// enabled replication or a configured bridge the server fails with
    /// `55000`; a Low exporter, an unknown table, or a field that is not
    /// High-owned fails with `22023`.
    pub async fn bridge_release(
        &self,
        table: &str,
        key: &str,
        column: Option<&str>,
    ) -> Result<bool> {
        check_bridge_target(table, column)?;
        let mut sql = format!("MURMUR BRIDGE RELEASE {} {}", quote(table), quote(key));
        if let Some(name) = column {
            sql.push_str(&format!(" COLUMN {}", quote(name)));
        }
        let rows = self.simple(&sql).await?;
        let r = rows
            .first()
            .filter(|r| !r.is_empty())
            .ok_or_else(|| Error::Protocol("empty BRIDGE RELEASE reply".into()))?;
        Ok(text_bool(&r[0]))
    }
}

/// Fill `buf` unless EOF comes first; returns bytes read (0 at EOF).
async fn read_full<R: AsyncRead + Unpin>(r: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = r.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

/// Server-side file cursor. Fetches bounded pages; call [`close`](Self::close)
/// to release server state if you stop before exhaustion.
pub struct FileCursor<'a> {
    client: &'a Client,
    token: Option<String>,
    page: std::collections::VecDeque<FileInfo>,
}

impl<'a> FileCursor<'a> {
    async fn open(client: &'a Client, sql: &str, pattern: Option<&str>) -> Result<FileCursor<'a>> {
        let rows = match &pattern {
            Some(p) => client.typed(sql, &[(p, Type::TEXT)]).await?,
            None => client.typed(sql, &[]).await?,
        };
        let token: String = rows
            .first()
            .ok_or_else(|| Error::Protocol("empty cursor reply".into()))?
            .try_get(0)?;
        Ok(FileCursor {
            client,
            token: Some(token),
            page: Default::default(),
        })
    }

    /// Next file, or `None` once exhausted (the cursor then closes itself).
    pub async fn next(&mut self) -> Result<Option<FileInfo>> {
        if let Some(f) = self.page.pop_front() {
            return Ok(Some(f));
        }
        let Some(token) = self.token.clone() else {
            return Ok(None);
        };
        let rows = match self
            .client
            .typed("MURMUR FILE NEXT $1", &[(&token, Type::TEXT)])
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let _ = self.close().await;
                return Err(e);
            }
        };
        for row in &rows {
            self.page.push_back(file_info(row, 0)?);
        }
        if self.page.is_empty() {
            self.close().await?;
            return Ok(None);
        }
        Ok(self.page.pop_front())
    }

    /// Drain the remaining files.
    pub async fn collect(mut self) -> Result<Vec<FileInfo>> {
        let mut out = Vec::new();
        while let Some(f) = self.next().await? {
            out.push(f);
        }
        Ok(out)
    }

    /// Release the server-side cursor (idempotent).
    pub async fn close(&mut self) -> Result<()> {
        if let Some(token) = self.token.take() {
            self.page.clear();
            self.client
                .typed("MURMUR FILE CLOSE $1", &[(&token, Type::TEXT)])
                .await?;
        }
        Ok(())
    }
}
