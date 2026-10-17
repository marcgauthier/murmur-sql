//! Shared harness for the Rust `tests-live/` scenario ports.
//!
//! Each scenario drives one `murmur-server` child process per node over the
//! PostgreSQL wire protocol (plain SQL plus `MURMUR ...` commands). This
//! crate provides multi-process cluster management, certificate
//! provisioning, connect-with-watchdog, forensics, artifact retention, and
//! a raw-QUIC replication-protocol attacker fixture (`attacker`) reusing
//! the product wire implementation. See `tests-live/README.md` for the
//! full design.

pub use tokio_postgres;
// Product wire-format/crypto implementation, re-exported so hostile-peer
// scenarios use it through the harness instead of forking it (see
// `attacker`; no scenario may depend on `murmur-replication` directly).
pub use murmur_replication as replication;

pub mod artifacts;
pub mod attacker;
pub mod certs;
pub mod cluster;
pub mod forensics;
pub mod ports;
pub mod unlock;

pub use cluster::{
    BridgeNodeConfig, BridgeOptions, Cluster, ClusterOptions, Dissemination, FilesOptions,
    MergePolicy, Node, NodeClient, ReplicationOptions, SpoolOptions, SqliteOptions,
};

/// Harness error. Wire errors surface as [`Error::Db`]; match on
/// `tokio_postgres::Error::code()` for SQLSTATEs.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("harness setup: {0}")]
    Setup(String),
    #[error("node index {0} is stopped")]
    NodeStopped(usize),
    #[error("unknown node index {0}")]
    UnknownNode(usize),
    #[error("open timed out after {0:?} ({1})")]
    UnlockTimeout(std::time::Duration, &'static str),
    #[error(transparent)]
    Db(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Cert(#[from] rcgen::Error),
    #[error("attacker: {0}")]
    Attack(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Full server-side text of a wire error for assertions.
///
/// `tokio_postgres::Error`'s `Display` is a bare kind tag (`db error`,
/// `error serializing parameter 0`, ...): the server's SQLSTATE/message
/// (or the client-side parameter cause) lives in the source chain, so
/// `to_string()` never matches error text. Returns `CODE: message` for
/// server errors, else the display plus the full cause chain.
pub fn db_message(e: &tokio_postgres::Error) -> String {
    use std::error::Error as _;
    if let Some(db) = e.as_db_error() {
        let mut msg = format!("{}: {}", db.code().code(), db.message());
        if let Some(detail) = db.detail() {
            msg.push_str(&format!(" ({detail})"));
        }
        return msg;
    }
    let mut out = e.to_string();
    let mut cur = e.source();
    while let Some(c) = cur {
        out.push_str(&format!(": {c}"));
        cur = c.source();
    }
    out
}

/// Lowercase hex of bytes (node ids, keys, SAN URIs).
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Read a positive-integer seconds env knob with fallback (Go `EnvSeconds` parity).
pub fn env_seconds(name: &str, fallback_secs: u64) -> std::time::Duration {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(fallback_secs))
}

/// Poll `cond` every `poll` until it holds or `timeout` elapses.
/// Returns true when the condition was met (use with `assert!` + forensics).
pub fn wait_until(
    timeout: std::time::Duration,
    poll: std::time::Duration,
    mut cond: impl FnMut() -> bool,
) -> bool {
    let end = std::time::Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if std::time::Instant::now() >= end {
            return false;
        }
        std::thread::sleep(poll);
    }
}

/// Async variant of [`wait_until`] for conditions that query nodes.
pub async fn wait_until_async<F, Fut>(
    timeout: std::time::Duration,
    poll: std::time::Duration,
    mut cond: F,
) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let end = tokio::time::Instant::now() + timeout;
    loop {
        if cond().await {
            return true;
        }
        if tokio::time::Instant::now() >= end {
            return false;
        }
        tokio::time::sleep(poll).await;
    }
}
