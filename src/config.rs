//! `config.toml` schema for `murmur-server`.
//!
//! All sections are optional and default to the embedded engine's `Options`
//! defaults (see `README.md` settings table). Unknown keys are rejected so
//! typos fail fast instead of silently running with defaults.
//!
//! Durations accept strings with units (`"500ms"`, `"1s"`, `"5m"`, `"1h"`,
//! `"7d"`) or bare numbers meaning seconds. Byte sizes accept strings with
//! units (`"64KiB"`, `"64KB"`, `"256MiB"`, `"1GiB"`) or bare numbers meaning
//! bytes. `KB`/`MB`/`GB` are powers of 1000, `KiB`/`MiB`/`GiB` powers of 1024.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

pub(crate) mod units {
    use super::*;
    use serde::de::{self, Visitor};

    pub fn parse_bytes(s: &str) -> std::result::Result<usize, String> {
        let s = s.trim();
        if s.is_empty() {
            return Err("empty byte size".into());
        }
        let split = s
            .bytes()
            .position(|b| b.is_ascii_alphabetic())
            .unwrap_or(s.len());
        let (num, unit) = s.split_at(split);
        let num: u64 = num
            .trim()
            .parse()
            .map_err(|_| format!("invalid byte size {s:?}"))?;
        let mult: u64 = match unit.trim().to_ascii_lowercase().as_str() {
            "" | "b" => 1,
            "k" | "kb" => 1000,
            "kib" => 1024,
            "m" | "mb" => 1000 * 1000,
            "mib" => 1024 * 1024,
            "g" | "gb" => 1000 * 1000 * 1000,
            "gib" => 1024 * 1024 * 1024,
            other => return Err(format!("unknown byte-size unit {other:?}")),
        };
        let bytes = num
            .checked_mul(mult)
            .ok_or_else(|| format!("byte size {s:?} overflows"))?;
        usize::try_from(bytes).map_err(|_| format!("byte size {s:?} overflows"))
    }

    pub fn format_bytes(bytes: usize) -> String {
        const UNITS: [(u64, &str); 4] =
            [(1024 * 1024 * 1024, "GiB"), (1024 * 1024, "MiB"), (1024, "KiB"), (1, "B")];
        let bytes = bytes as u64;
        for (mult, suffix) in UNITS {
            if bytes >= mult && bytes % mult == 0 {
                return format!("{}{suffix}", bytes / mult);
            }
        }
        format!("{bytes}B")
    }

    pub fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
        let s = s.trim();
        if s.is_empty() {
            return Err("empty duration".into());
        }
        let split = s
            .bytes()
            .position(|b| b.is_ascii_alphabetic())
            .unwrap_or(s.len());
        let (num, unit) = s.split_at(split);
        let num: u64 = num
            .trim()
            .parse()
            .map_err(|_| format!("invalid duration {s:?}"))?;
        let millis: u64 = match unit.trim().to_ascii_lowercase().as_str() {
            "" | "s" | "sec" | "secs" | "second" | "seconds" => {
                num.checked_mul(1000).ok_or_else(|| format!("duration {s:?} overflows"))?
            }
            "ms" | "milli" | "millis" => num,
            "m" | "min" | "mins" | "minute" | "minutes" => num
                .checked_mul(60_000)
                .ok_or_else(|| format!("duration {s:?} overflows"))?,
            "h" | "hour" | "hours" => num
                .checked_mul(3_600_000)
                .ok_or_else(|| format!("duration {s:?} overflows"))?,
            "d" | "day" | "days" => num
                .checked_mul(86_400_000)
                .ok_or_else(|| format!("duration {s:?} overflows"))?,
            other => return Err(format!("unknown duration unit {other:?}")),
        };
        Ok(Duration::from_millis(millis))
    }

    pub fn format_duration(d: Duration) -> String {
        let millis = d.as_millis();
        for (mult, suffix) in [(86_400_000u128, "d"), (3_600_000, "h"), (60_000, "m"), (1000, "s")] {
            if millis >= mult && millis % mult == 0 {
                return format!("{}{suffix}", millis / mult);
            }
        }
        format!("{millis}ms")
    }

    pub mod bytes {
        use super::*;
        use serde::Serializer;

        pub fn serialize<S: Serializer>(v: &usize, s: S) -> std::result::Result<S::Ok, S::Error> {
            s.serialize_str(&format_bytes(*v))
        }

        pub fn deserialize<'d, D: serde::Deserializer<'d>>(d: D) -> std::result::Result<usize, D::Error> {
            struct V;
            impl Visitor<'_> for V {
                type Value = usize;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("a byte size like 8388608 or \"8MiB\"")
                }
                fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<usize, E> {
                    usize::try_from(v).map_err(|_| E::custom(format!("negative byte size {v}")))
                }
                fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<usize, E> {
                    usize::try_from(v).map_err(|_| E::custom(format!("byte size {v} overflows")))
                }
                fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<usize, E> {
                    parse_bytes(v).map_err(E::custom)
                }
            }
            d.deserialize_any(V)
        }
    }

    pub mod duration {
        use super::*;
        use serde::Serializer;

        pub fn serialize<S: Serializer>(v: &Duration, s: S) -> std::result::Result<S::Ok, S::Error> {
            s.serialize_str(&format_duration(*v))
        }

        pub fn deserialize<'d, D: serde::Deserializer<'d>>(
            d: D,
        ) -> std::result::Result<Duration, D::Error> {
            struct V;
            impl Visitor<'_> for V {
                type Value = Duration;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("a duration like 1 or \"500ms\"")
                }
                fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Duration, E> {
                    if v < 0 {
                        return Err(E::custom(format!("negative duration {v}")));
                    }
                    Ok(Duration::from_secs(v as u64))
                }
                fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Duration, E> {
                    Ok(Duration::from_secs(v))
                }
                fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Duration, E> {
                    parse_duration(v).map_err(E::custom)
                }
            }
            d.deserialize_any(V)
        }
    }

    pub mod opt_duration {
        use super::*;
        use serde::Serializer;

        pub fn serialize<S: Serializer>(
            v: &Option<Duration>,
            s: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            match v {
                Some(d) => s.serialize_some(&format_duration(*d)),
                None => s.serialize_none(),
            }
        }

        pub fn deserialize<'d, D: serde::Deserializer<'d>>(
            d: D,
        ) -> std::result::Result<Option<Duration>, D::Error> {
            Ok(Some(super::duration::deserialize(d)?))
        }
    }
}

fn is_false(b: &bool) -> bool {
    !*b
}

fn default_bind() -> String {
    "127.0.0.1:5432".into()
}

fn default_replication_bind() -> String {
    "127.0.0.1:9001".into()
}

/// Top-level server configuration, loaded from `config.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub engine: EngineConfig,
    pub auth: AuthConfig,
    pub replication: ReplicationConfig,
    pub logging: LoggingConfig,
    pub backups: BackupConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            engine: EngineConfig::default(),
            auth: AuthConfig::default(),
            replication: ReplicationConfig::default(),
            logging: LoggingConfig::default(),
            backups: BackupConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Postgres-wire listen address, e.g. `"127.0.0.1:5432"`.
    pub bind: String,
    /// Production mode requires TLS with client certificates (mTLS).
    #[serde(default, skip_serializing_if = "is_false")]
    pub production: bool,
    pub tls: TlsConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            production: false,
            tls: TlsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TlsConfig {
    #[serde(default, skip_serializing_if = "is_false")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_file: Option<String>,
    /// CA bundle verifying client certificates. Required for mTLS when
    /// `server.production` is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_file: None,
            key_file: None,
            ca_file: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    pub data_dir: String,
    /// Optional fixed identities as 32 lowercase hex chars. `None` keeps the
    /// stored identity on open or generates a fresh one on create.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "./data".into(),
            database_id: None,
            node_id: None,
        }
    }
}

/// Engine tuning knobs. Defaults mirror `Options::default()` exactly.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EngineConfig {
    #[serde(with = "units::duration")]
    pub capture_interval: Duration,
    #[serde(with = "units::bytes")]
    pub capture_bytes: usize,
    #[serde(with = "units::bytes")]
    pub queue_bytes: usize,
    #[serde(with = "units::bytes")]
    pub transaction_bytes: usize,
    #[serde(with = "units::bytes")]
    pub segment_bytes: usize,
    #[serde(with = "units::bytes")]
    pub spool_block_bytes: usize,
    #[serde(with = "units::bytes")]
    pub spool_cache_bytes: usize,
    pub snapshot_deltas: usize,
    #[serde(with = "units::bytes")]
    pub snapshot_bytes: usize,
    pub compression_level: i32,
    pub storage_workers: usize,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "units::opt_duration")]
    pub data_key_max_age: Option<Duration>,
    pub scheduler: SchedulerConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
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
            scheduler: SchedulerConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SchedulerConfig {
    pub local_share: u32,
    pub remote_share: u32,
    #[serde(with = "units::duration")]
    pub max_debt: Duration,
    pub maintenance_reserve_every: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            local_share: 90,
            remote_share: 10,
            max_debt: Duration::from_secs(1),
            maintenance_reserve_every: 100,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuthConfig {
    /// Users allowed to open Postgres-wire sessions. Empty means no one can
    /// authenticate; the key command alone never grants access.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub users: Vec<UserConfig>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { users: Vec::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
    pub name: String,
    /// Argon2 PHC hash (`$argon2id$...`), never a plaintext password. Use
    /// `murmur-admin hash-password` to generate one.
    pub password_hash: String,
}

/// Replication peers and per-table settings. The local node is described by
/// `bind`/`advertise`; remote nodes are `bootstrap` addresses plus the
/// trusted-identity maps. `tables` maps stable table IDs to replicated
/// (`true`) or local-only (`false`); missing entries mean local-only.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ReplicationConfig {
    #[serde(default, skip_serializing_if = "is_false")]
    pub enabled: bool,
    pub bind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertise: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bootstrap: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_chain_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,
    /// Ed25519 signing key, 64 hex chars, or use `signing_key_file`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_key_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_key_file: Option<String>,
    /// Node id (32 hex chars) to Ed25519 public key (64 hex chars).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub trusted_origins: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_peers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_networks: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trusted_snapshot_sources: Vec<String>,
    pub membership: bool,
    pub dissemination: String,
    pub accept_remote_schema: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub master: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_master: Option<String>,
    pub fanout: usize,
    pub repairs: usize,
    pub sessions: usize,
    pub connections: usize,
    #[serde(with = "units::bytes")]
    pub queue_bytes: usize,
    #[serde(with = "units::bytes")]
    pub peer_queue_bytes: usize,
    #[serde(with = "units::bytes")]
    pub staging_bytes: usize,
    pub staging_transfers: usize,
    #[serde(with = "units::bytes")]
    pub max_transaction_bytes: usize,
    #[serde(with = "units::bytes")]
    pub snapshot_bytes: usize,
    #[serde(with = "units::bytes")]
    pub snapshot_staging_bytes: usize,
    #[serde(with = "units::duration")]
    pub snapshot_transfer_lifetime: Duration,
    #[serde(with = "units::duration")]
    pub incoming_interval: Duration,
    pub batch_transactions: usize,
    #[serde(with = "units::bytes")]
    pub batch_bytes: usize,
    #[serde(default, skip_serializing_if = "is_false")]
    pub adaptive_sync: bool,
    #[serde(with = "units::duration")]
    pub anti_entropy_interval: Duration,
    #[serde(with = "units::duration")]
    pub retention: Duration,
    #[serde(with = "units::duration")]
    pub retention_interval: Duration,
    pub minimum_retained: u64,
    #[serde(with = "units::duration")]
    pub maximum_offline: Duration,
    /// `"table.column"` to one of `lww`, `counter`, `set`, `min`, `max`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub merge_policies: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tables: BTreeMap<String, bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<ReplicationFilesConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bridge: Option<ReplicationBridgeConfig>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_replication_bind(),
            advertise: None,
            bootstrap: Vec::new(),
            cert_chain_file: None,
            private_key_file: None,
            ca_file: None,
            signing_key_hex: None,
            signing_key_file: None,
            trusted_origins: BTreeMap::new(),
            allowed_peers: Vec::new(),
            allowed_networks: Vec::new(),
            trusted_snapshot_sources: Vec::new(),
            membership: true,
            dissemination: "gossip".into(),
            accept_remote_schema: true,
            master: false,
            schema_master: None,
            fanout: 4,
            repairs: 2,
            sessions: 32,
            connections: 64,
            queue_bytes: 64 << 20,
            peer_queue_bytes: 4 << 20,
            staging_bytes: 256 << 20,
            staging_transfers: 4096,
            max_transaction_bytes: 64 << 20,
            snapshot_bytes: 512 << 20,
            snapshot_staging_bytes: 512 << 20,
            snapshot_transfer_lifetime: Duration::from_secs(7 * 86400),
            incoming_interval: Duration::from_secs(1),
            batch_transactions: 64,
            batch_bytes: 4 << 20,
            adaptive_sync: false,
            anti_entropy_interval: Duration::from_secs(10),
            retention: Duration::from_secs(3600),
            retention_interval: Duration::from_secs(60),
            minimum_retained: 1000,
            maximum_offline: Duration::from_secs(7 * 86400),
            merge_policies: BTreeMap::new(),
            tables: BTreeMap::new(),
            files: None,
            bridge: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ReplicationFilesConfig {
    pub directory: String,
    /// 64 hex chars.
    pub key_hex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_key_hex: Option<String>,
    pub generation: u64,
    pub fetch_bind: String,
    #[serde(with = "units::bytes")]
    pub max_file_bytes: usize,
    #[serde(with = "units::bytes")]
    pub max_staging_bytes: usize,
    pub concurrent_fetches: usize,
    #[serde(with = "units::duration")]
    pub fetch_interval: Duration,
}

impl Default for ReplicationFilesConfig {
    fn default() -> Self {
        Self {
            directory: "./files".into(),
            key_hex: String::new(),
            previous_key_hex: None,
            generation: 0,
            fetch_bind: "127.0.0.1:0".into(),
            max_file_bytes: 4 << 30,
            max_staging_bytes: 1 << 30,
            concurrent_fetches: 2,
            fetch_interval: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ReplicationBridgeConfig {
    /// `low-exporter` or `high-receiver`.
    pub role: String,
    /// Database id the bridge serves, 32 hex chars.
    pub domain: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trusted_domains: Vec<String>,
    /// Node id to Ed25519 public key (64 hex chars).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub trusted_origins: BTreeMap<String, String>,
    pub directory: String,
    pub stream: String,
    /// X25519 recipient public key, 64 hex chars.
    pub recipient: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receiver_secret_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer_hex: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub trusted_signers: BTreeMap<String, String>,
    #[serde(with = "units::bytes")]
    pub max_bundle_bytes: usize,
    #[serde(with = "units::bytes")]
    pub max_payload_bytes: usize,
    #[serde(with = "units::bytes")]
    pub staging_bytes: usize,
}

impl Default for ReplicationBridgeConfig {
    fn default() -> Self {
        Self {
            role: "low-exporter".into(),
            domain: String::new(),
            trusted_domains: Vec::new(),
            trusted_origins: BTreeMap::new(),
            directory: "./bridge".into(),
            stream: "main".into(),
            recipient: String::new(),
            receiver_secret_hex: None,
            signer_hex: None,
            trusted_signers: BTreeMap::new(),
            max_bundle_bytes: 16 << 20,
            max_payload_bytes: 64 << 20,
            staging_bytes: 256 << 20,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LoggingConfig {
    pub dir: String,
    pub level: String,
    pub file_prefix: String,
    /// Only `daily` rotation is supported.
    pub rotation: String,
    pub retention_days: u32,
    #[serde(default, skip_serializing_if = "is_false")]
    pub console: bool,
    pub victorialogs: VictoriaLogsConfig,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            dir: "./logs".into(),
            level: "info".into(),
            file_prefix: "murmur".into(),
            rotation: "daily".into(),
            retention_days: 14,
            console: false,
            victorialogs: VictoriaLogsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct VictoriaLogsConfig {
    #[serde(default, skip_serializing_if = "is_false")]
    pub enabled: bool,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_token: Option<String>,
    /// Extra `key = "value"` stream labels attached to every pushed batch.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

impl Default for VictoriaLogsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: "http://127.0.0.1:9428".into(),
            username: None,
            password: None,
            bearer_token: None,
            labels: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BackupConfig {
    /// Folder holding remote-backup archives. Created on demand by
    /// `MURMUR BACKUP SAVE` and `MURMUR BACKUP UPLOAD`.
    pub dir: String,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self { dir: "./backups".into() }
    }
}

/// Decode `expected` bytes from lowercase or uppercase hex.
pub fn parse_hex(value: &str, expected: usize, what: &str) -> Result<Vec<u8>> {
    let value = value.trim();
    if value.len() != expected * 2 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::Invalid(format!(
            "{what} must be {} hex chars",
            expected * 2
        )));
    }
    (0..value.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&value[i..i + 2], 16)
                .map_err(|_| Error::Invalid(format!("{what} is not valid hex")))
        })
        .collect()
}

fn parse_socket(value: &str, what: &str) -> Result<std::net::SocketAddr> {
    value
        .trim()
        .parse()
        .map_err(|_| Error::Invalid(format!("{what} {value:?} is not a socket address")))
}

impl Config {
    /// Load and validate `config.toml` from `path`.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Invalid(format!("cannot read {}: {e}", path.display())))?;
        let config: Self = toml::from_str(&text)
            .map_err(|e| Error::Invalid(format!("cannot parse {}: {e}", path.display())))?;
        config.validate()?;
        Ok(config)
    }

    /// Structural validation. File existence for credentials is checked when
    /// the server builds its runtime state, not here.
    pub fn validate(&self) -> Result<()> {
        parse_socket(&self.server.bind, "server.bind")?;
        if self.storage.data_dir.trim().is_empty() {
            return Err(Error::Invalid("storage.data_dir must not be empty".into()));
        }
        if self.backups.dir.trim().is_empty() {
            return Err(Error::Invalid("backups.dir must not be empty".into()));
        }
        for (what, id) in [
            ("storage.database_id", &self.storage.database_id),
            ("storage.node_id", &self.storage.node_id),
        ] {
            if let Some(id) = id {
                parse_hex(id, 16, what)?;
            }
        }
        if self.server.production {
            if !self.server.tls.enabled {
                return Err(Error::Invalid(
                    "server.production requires server.tls.enabled".into(),
                ));
            }
            if self.server.tls.cert_file.is_none()
                || self.server.tls.key_file.is_none()
                || self.server.tls.ca_file.is_none()
            {
                return Err(Error::Invalid(
                    "server.production requires tls cert_file, key_file and ca_file (mTLS)".into(),
                ));
            }
        }
        if self.server.tls.enabled
            && (self.server.tls.cert_file.is_none() || self.server.tls.key_file.is_none())
        {
            return Err(Error::Invalid(
                "server.tls.enabled requires tls cert_file and key_file".into(),
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        for user in &self.auth.users {
            if user.name.trim().is_empty() {
                return Err(Error::Invalid("auth user name must not be empty".into()));
            }
            if !names.insert(user.name.clone()) {
                return Err(Error::Invalid(format!(
                    "duplicate auth user {:?}",
                    user.name
                )));
            }
            if !user.password_hash.starts_with("$argon2") {
                return Err(Error::Invalid(format!(
                    "auth user {:?} needs an Argon2 password hash, not a plaintext password",
                    user.name
                )));
            }
        }
        self.engine.to_options()?;
        #[cfg(not(feature = "replication"))]
        if self.replication.enabled {
            return Err(Error::Invalid(
                "replication.enabled needs a server built with the replication feature".into(),
            ));
        }
        self.replication.validate()?;
        self.logging.validate()?;
        Ok(())
    }

    /// Look up a user by Postgres-wire login name.
    pub fn user(&self, name: &str) -> Option<&UserConfig> {
        self.auth.users.iter().find(|u| u.name == name)
    }
}

impl EngineConfig {
    /// Convert to engine `Options`. Managed search indexes are not
    /// configurable in `config.toml` yet; engine defaults apply.
    pub fn to_options(&self) -> Result<crate::Options> {
        let mut options = crate::Options::default();
        options.capture_interval = self.capture_interval;
        options.capture_bytes = self.capture_bytes;
        options.queue_bytes = self.queue_bytes;
        options.transaction_bytes = self.transaction_bytes;
        options.segment_bytes = self.segment_bytes;
        options.spool_block_bytes = self.spool_block_bytes;
        options.spool_cache_bytes = self.spool_cache_bytes;
        options.snapshot_deltas = self.snapshot_deltas;
        options.snapshot_bytes = self.snapshot_bytes;
        options.compression_level = self.compression_level;
        options.storage_workers = self.storage_workers;
        options.data_key_max_age = self.data_key_max_age;
        options.scheduler.local_share = self.scheduler.local_share;
        options.scheduler.remote_share = self.scheduler.remote_share;
        options.scheduler.max_debt = self.scheduler.max_debt;
        options.scheduler.maintenance_reserve_every = self.scheduler.maintenance_reserve_every;
        options.validate()?;
        Ok(options)
    }
}

impl ReplicationConfig {
    /// Structural validation of addresses, identities, and enum spellings.
    /// Credential file contents are loaded when replication starts.
    pub fn validate(&self) -> Result<()> {
        parse_socket(&self.bind, "replication.bind")?;
        if let Some(advertise) = &self.advertise {
            parse_socket(advertise, "replication.advertise")?;
        }
        for addr in &self.bootstrap {
            parse_socket(addr, "replication.bootstrap")?;
        }
        for (node, key) in &self.trusted_origins {
            parse_hex(node, 16, "replication.trusted_origins node")?;
            parse_hex(key, 32, "replication.trusted_origins key")?;
        }
        for peer in self.allowed_peers.iter().chain(&self.trusted_snapshot_sources) {
            parse_hex(peer, 16, "replication peer id")?;
        }
        for net in &self.allowed_networks {
            parse_cidr(net)?;
        }
        if let Some(master) = &self.schema_master {
            parse_hex(master, 16, "replication.schema_master")?;
        }
        if self.master && self.schema_master.is_none() {
            return Err(Error::Invalid(
                "replication.master requires replication.schema_master".into(),
            ));
        }
        match self.dissemination.to_ascii_lowercase().as_str() {
            "gossip" | "plumtree" => {}
            other => return Err(Error::Invalid(format!("unknown replication.dissemination {other:?}"))),
        }
        for (column, policy) in &self.merge_policies {
            if !column.contains('.') {
                return Err(Error::Invalid(format!(
                    "replication.merge_policies key {column:?} must be \"table.column\""
                )));
            }
            match policy.to_ascii_lowercase().as_str() {
                "lww" | "counter" | "set" | "min" | "max" => {}
                _ => {
                    return Err(Error::Invalid(format!(
                        "unknown merge policy {policy:?} for {column:?}"
                    )));
                }
            }
        }
        for table in self.tables.keys() {
            if table.trim().is_empty() {
                return Err(Error::Invalid("replication.tables has an empty table name".into()));
            }
        }
        if self.enabled {
            for (what, file) in [
                ("replication.cert_chain_file", &self.cert_chain_file),
                ("replication.private_key_file", &self.private_key_file),
                ("replication.ca_file", &self.ca_file),
            ] {
                if file.as_deref().is_none_or(|f| f.trim().is_empty()) {
                    return Err(Error::Invalid(format!("{what} is required when replication is enabled")));
                }
            }
            match (&self.signing_key_hex, &self.signing_key_file) {
                (Some(hex), None) => {
                    parse_hex(hex, 32, "replication.signing_key_hex")?;
                }
                (None, Some(_)) => {}
                _ => {
                    return Err(Error::Invalid(
                        "set exactly one of replication.signing_key_hex or replication.signing_key_file".into(),
                    ));
                }
            }
        }
        if let Some(files) = &self.files {
            files.validate()?;
        }
        if let Some(bridge) = &self.bridge {
            bridge.validate()?;
        }
        Ok(())
    }
}

impl ReplicationFilesConfig {
    pub fn validate(&self) -> Result<()> {
        if self.directory.trim().is_empty() {
            return Err(Error::Invalid("replication.files.directory must not be empty".into()));
        }
        parse_hex(&self.key_hex, 32, "replication.files.key_hex")?;
        if let Some(previous) = &self.previous_key_hex {
            parse_hex(previous, 32, "replication.files.previous_key_hex")?;
        }
        parse_socket(&self.fetch_bind, "replication.files.fetch_bind")?;
        if self.max_file_bytes == 0 || self.concurrent_fetches == 0 || self.fetch_interval.is_zero() {
            return Err(Error::Invalid("invalid replication.files limits".into()));
        }
        Ok(())
    }
}

impl ReplicationBridgeConfig {
    pub fn validate(&self) -> Result<()> {
        match self.role.as_str() {
            "low-exporter" | "high-receiver" => {}
            other => return Err(Error::Invalid(format!("unknown replication.bridge.role {other:?}"))),
        }
        parse_hex(&self.domain, 16, "replication.bridge.domain")?;
        for domain in &self.trusted_domains {
            parse_hex(domain, 16, "replication.bridge.trusted_domains")?;
        }
        for (node, key) in self.trusted_origins.iter().chain(&self.trusted_signers) {
            parse_hex(node, 16, "replication.bridge signer node")?;
            parse_hex(key, 32, "replication.bridge signer key")?;
        }
        if self.directory.trim().is_empty() || self.stream.trim().is_empty() {
            return Err(Error::Invalid(
                "replication.bridge needs a directory and stream".into(),
            ));
        }
        parse_hex(&self.recipient, 32, "replication.bridge.recipient")?;
        if let Some(secret) = &self.receiver_secret_hex {
            parse_hex(secret, 32, "replication.bridge.receiver_secret_hex")?;
        }
        if let Some(signer) = &self.signer_hex {
            parse_hex(signer, 32, "replication.bridge.signer_hex")?;
        }
        if self.role == "low-exporter" && self.signer_hex.is_none() {
            return Err(Error::Invalid(
                "replication.bridge low-exporter requires signer_hex".into(),
            ));
        }
        if self.role == "high-receiver" {
            if self.receiver_secret_hex.is_none() {
                return Err(Error::Invalid(
                    "replication.bridge high-receiver requires receiver_secret_hex".into(),
                ));
            }
            if self.trusted_domains.is_empty() {
                return Err(Error::Invalid(
                    "replication.bridge high-receiver requires trusted_domains".into(),
                ));
            }
        }
        if self.max_bundle_bytes == 0
            || self.max_bundle_bytes > 16 << 20
            || self.max_payload_bytes == 0
            || self.max_payload_bytes > 64 << 20
            || self.staging_bytes < self.max_bundle_bytes
        {
            return Err(Error::Invalid("invalid replication.bridge limits".into()));
        }
        Ok(())
    }
}

fn parse_cidr(value: &str) -> Result<()> {
    let (addr, prefix) = value
        .trim()
        .split_once('/')
        .ok_or_else(|| Error::Invalid(format!("network {value:?} must be CIDR like 10.0.0.0/8")))?;
    let is_v4 = addr.parse::<std::net::Ipv4Addr>().is_ok();
    let is_v6 = addr.parse::<std::net::Ipv6Addr>().is_ok();
    if !is_v4 && !is_v6 {
        return Err(Error::Invalid(format!("network {value:?} has an invalid address")));
    }
    let prefix: u8 = prefix
        .parse()
        .map_err(|_| Error::Invalid(format!("network {value:?} has an invalid prefix")))?;
    let max = if is_v4 { 32 } else { 128 };
    if prefix > max {
        return Err(Error::Invalid(format!("network {value:?} has an invalid prefix")));
    }
    Ok(())
}

impl LoggingConfig {
    pub fn validate(&self) -> Result<()> {
        if self.dir.trim().is_empty() {
            return Err(Error::Invalid("logging.dir must not be empty".into()));
        }
        match self.level.to_ascii_lowercase().as_str() {
            "error" | "warn" | "info" | "debug" | "trace" => {}
            other => return Err(Error::Invalid(format!("unknown logging.level {other:?}"))),
        }
        if self.file_prefix.trim().is_empty() {
            return Err(Error::Invalid("logging.file_prefix must not be empty".into()));
        }
        if self.rotation != "daily" {
            return Err(Error::Invalid(format!(
                "unsupported logging.rotation {:?}; only \"daily\" is supported",
                self.rotation
            )));
        }
        if self.victorialogs.enabled {
            let url = self.victorialogs.url.trim();
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(Error::Invalid(
                    "logging.victorialogs.url must start with http:// or https://".into(),
                ));
            }
            if self.victorialogs.bearer_token.is_some()
                && (self.victorialogs.username.is_some() || self.victorialogs.password.is_some())
            {
                return Err(Error::Invalid(
                    "logging.victorialogs takes either bearer_token or username/password".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Set one `[replication.tables]` flag inside `config.toml`, preserving the
/// rest of the file (comments, formatting, other sections). The write is
/// atomic: a temporary file in the same directory is fsynced and renamed.
pub fn save_table_flag(path: impl AsRef<Path>, table: &str, replicated: bool) -> Result<()> {
    let table = table.trim();
    if table.is_empty() {
        return Err(Error::Invalid("table name must not be empty".into()));
    }
    save_table_flags(path, &BTreeMap::from([(table.to_owned(), replicated)]))
}

/// Set several `[replication.tables]` flags in one atomic rewrite.
pub fn save_table_flags(path: impl AsRef<Path>, flags: &BTreeMap<String, bool>) -> Result<()> {
    for table in flags.keys() {
        if table.trim().is_empty() {
            return Err(Error::Invalid("table name must not be empty".into()));
        }
    }
    let path = path.as_ref();
    let text = std::fs::read_to_string(path)
        .map_err(|e| Error::Invalid(format!("cannot read {}: {e}", path.display())))?;
    let mut doc = text
        .parse::<toml_edit::DocumentMut>()
        .map_err(|e| Error::Invalid(format!("cannot parse {}: {e}", path.display())))?;
    for (table, replicated) in flags {
        doc["replication"]["tables"][table] = toml_edit::value(*replicated);
    }
    let text = doc.to_string();
    // Revalidate the edited file so a concurrent hand edit cannot persist a
    // broken config silently.
    let config: Config =
        toml::from_str(&text).map_err(|e| Error::Invalid(format!("edited config is invalid: {e}")))?;
    config.validate()?;
    atomic_write(path, text.as_bytes())?;
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
    let mut temp = path.as_os_str().to_owned();
    temp.push(format!(".tmp-{}", std::process::id()));
    let temp = Path::new(&temp);
    std::fs::write(temp, bytes)
        .and_then(|_| {
            std::fs::File::open(temp)?.sync_all()?;
            std::fs::rename(temp, path)?;
            #[cfg(unix)]
            if let Some(dir) = dir {
                std::fs::File::open(dir)?.sync_all()?;
            }
            Ok(())
        })
        .map_err(|e| Error::Invalid(format!("cannot write {}: {e}", path.display())))
}

#[cfg(feature = "replication")]
impl ReplicationConfig {
    /// Build the runtime replication config, loading PEM credentials and the
    /// signing key from disk. Call only when `enabled` is true; structural
    /// validation must have passed first.
    pub fn to_replication_config(&self) -> Result<murmur_replication::Config> {
        use murmur_replication::Config;
        use std::collections::{BTreeMap, BTreeSet};
        let bind = parse_socket(&self.bind, "replication.bind")?;
        let chain = load_certs(required(&self.cert_chain_file, "replication.cert_chain_file")?)?;
        let private_key =
            load_private_key(required(&self.private_key_file, "replication.private_key_file")?)?;
        let roots = load_certs(required(&self.ca_file, "replication.ca_file")?)?;
        let credentials = murmur_replication::Credentials {
            chain,
            private_key: zeroize::Zeroizing::new(private_key),
            roots,
        };
        let signing_key = match (&self.signing_key_hex, &self.signing_key_file) {
            (Some(hex), None) => parse_key32(hex, "replication.signing_key_hex")?,
            (None, Some(file)) => load_signing_key(file)?,
            _ => {
                return Err(Error::Invalid(
                    "set exactly one of replication.signing_key_hex or replication.signing_key_file"
                        .into(),
                ));
            }
        };
        let mut config = Config::new(bind, credentials, signing_key);
        config.advertise = self
            .advertise
            .as_deref()
            .map(|addr| parse_socket(addr, "replication.advertise"))
            .transpose()?;
        config.bootstrap = self
            .bootstrap
            .iter()
            .map(|addr| parse_socket(addr, "replication.bootstrap"))
            .collect::<Result<_>>()?;
        config.trusted_origins = self
            .trusted_origins
            .iter()
            .map(|(node, key)| {
                Ok((
                    parse_id16(node, "replication.trusted_origins node")?,
                    parse_key32(key, "replication.trusted_origins key")?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        config.allowed_peers = self
            .allowed_peers
            .iter()
            .map(|peer| parse_id16(peer, "replication.allowed_peers"))
            .collect::<Result<BTreeSet<_>>>()?;
        config.allowed_networks = self
            .allowed_networks
            .iter()
            .map(|net| {
                net.trim().parse().map_err(|_| Error::Invalid(format!("network {net:?} must be CIDR")))
            })
            .collect::<Result<_>>()?;
        config.trusted_snapshot_sources = self
            .trusted_snapshot_sources
            .iter()
            .map(|peer| parse_id16(peer, "replication.trusted_snapshot_sources"))
            .collect::<Result<BTreeSet<_>>>()?;
        config.membership = self.membership;
        config.dissemination = match self.dissemination.to_ascii_lowercase().as_str() {
            "plumtree" => murmur_replication::Dissemination::Plumtree,
            _ => murmur_replication::Dissemination::Gossip,
        };
        config.accept_remote_schema = self.accept_remote_schema;
        config.master = self.master;
        config.schema_master = self
            .schema_master
            .as_deref()
            .map(|master| parse_id16(master, "replication.schema_master"))
            .transpose()?;
        config.fanout = self.fanout;
        config.repairs = self.repairs;
        config.sessions = self.sessions;
        config.connections = self.connections;
        config.queue_bytes = self.queue_bytes;
        config.peer_queue_bytes = self.peer_queue_bytes;
        config.staging_bytes = self.staging_bytes;
        config.staging_transfers = self.staging_transfers;
        config.max_transaction_bytes = self.max_transaction_bytes;
        config.snapshot_bytes = self.snapshot_bytes;
        config.snapshot_staging_bytes = self.snapshot_staging_bytes;
        config.snapshot_transfer_lifetime = self.snapshot_transfer_lifetime;
        config.incoming_interval = self.incoming_interval;
        config.batch_transactions = self.batch_transactions;
        config.batch_bytes = self.batch_bytes;
        config.adaptive_sync = self.adaptive_sync;
        config.anti_entropy_interval = self.anti_entropy_interval;
        config.retention = self.retention;
        config.retention_interval = self.retention_interval;
        config.minimum_retained = self.minimum_retained;
        config.maximum_offline = self.maximum_offline;
        config.merge_policies = self
            .merge_policies
            .iter()
            .map(|(column, policy)| {
                let (table, column) = column.rsplit_once('.').ok_or_else(|| {
                    Error::Invalid(format!(
                        "replication.merge_policies key {column:?} must be \"table.column\""
                    ))
                })?;
                let policy = match policy.to_ascii_lowercase().as_str() {
                    "counter" => murmur_replication::policies::Policy::Counter,
                    "set" => murmur_replication::policies::Policy::Set,
                    "min" => murmur_replication::policies::Policy::Min,
                    "max" => murmur_replication::policies::Policy::Max,
                    _ => murmur_replication::policies::Policy::Lww,
                };
                Ok(((table.to_owned(), column.to_owned()), policy))
            })
            .collect::<Result<_>>()?;
        config.files = self.files.as_ref().map(build_files).transpose()?;
        config.bridge = self.bridge.as_ref().map(build_bridge).transpose()?;
        config.validate().map_err(|e| Error::Invalid(e.to_string()))?;
        Ok(config)
    }
}

#[cfg(feature = "replication")]
fn required<'a>(value: &'a Option<String>, what: &str) -> Result<&'a str> {
    value
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| Error::Invalid(format!("{what} is required when replication is enabled")))
}

#[cfg(feature = "replication")]
fn parse_id16(value: &str, what: &str) -> Result<[u8; 16]> {
    let bytes = parse_hex(value, 16, what)?;
    let mut id = [0u8; 16];
    id.copy_from_slice(&bytes);
    Ok(id)
}

#[cfg(feature = "replication")]
fn parse_key32(value: &str, what: &str) -> Result<[u8; 32]> {
    let bytes = parse_hex(value, 32, what)?;
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

pub(crate) fn load_certs(path: &str) -> Result<Vec<Vec<u8>>> {
    let file = std::fs::File::open(path)
        .map_err(|e| Error::Invalid(format!("cannot read {path}: {e}")))?;
    let mut reader = std::io::BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| Error::Invalid(format!("cannot parse {path}: {e}")))?;
    if certs.is_empty() {
        return Err(Error::Invalid(format!("{path} holds no certificates")));
    }
    Ok(certs.into_iter().map(|cert| cert.to_vec()).collect())
}

pub(crate) fn load_private_key(path: &str) -> Result<Vec<u8>> {
    let file = std::fs::File::open(path)
        .map_err(|e| Error::Invalid(format!("cannot read {path}: {e}")))?;
    let mut reader = std::io::BufReader::new(file);
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| Error::Invalid(format!("cannot parse {path}: {e}")))?
        .ok_or_else(|| Error::Invalid(format!("{path} holds no private key")))?;
    Ok(key.secret_der().to_vec())
}

/// A signing-key file holds 64 hex chars (whitespace tolerated) or 32 raw
/// bytes.
#[cfg(feature = "replication")]
fn load_signing_key(path: &str) -> Result<[u8; 32]> {
    let bytes = std::fs::read(path).map_err(|e| Error::Invalid(format!("cannot read {path}: {e}")))?;
    if bytes.len() == 32 {
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        return Ok(key);
    }
    let text: String = bytes.iter().filter(|b| !b.is_ascii_whitespace()).map(|b| *b as char).collect();
    parse_key32(&text, "replication.signing_key_file")
}

#[cfg(feature = "replication")]
fn build_files(config: &ReplicationFilesConfig) -> Result<murmur_replication::files::Config> {
    let mut built = murmur_replication::files::Config::new(
        config.directory.clone().into(),
        parse_key32(&config.key_hex, "replication.files.key_hex")?,
        parse_socket(&config.fetch_bind, "replication.files.fetch_bind")?,
    );
    built.previous_key = config
        .previous_key_hex
        .as_deref()
        .map(|key| parse_key32(key, "replication.files.previous_key_hex"))
        .transpose()?
        .map(zeroize::Zeroizing::new);
    built.generation = config.generation;
    built.max_file_bytes = config.max_file_bytes as u64;
    built.max_staging_bytes = config.max_staging_bytes as u64;
    built.concurrent_fetches = config.concurrent_fetches;
    built.fetch_interval = config.fetch_interval;
    Ok(built)
}

#[cfg(feature = "replication")]
fn build_bridge(config: &ReplicationBridgeConfig) -> Result<murmur_replication::bridge::Config> {
    use murmur_replication::bridge::{Config, Role};
    use std::collections::{BTreeMap, BTreeSet};
    let role = match config.role.as_str() {
        "high-receiver" => Role::HighReceiver,
        _ => Role::LowExporter,
    };
    let parse_signers = |map: &BTreeMap<String, String>| {
        map.iter()
            .map(|(node, key)| Ok((parse_id16(node, "replication.bridge signer node")?, parse_key32(key, "replication.bridge signer key")?)))
            .collect::<Result<BTreeMap<_, _>>>()
    };
    Ok(Config {
        domain: parse_id16(&config.domain, "replication.bridge.domain")?,
        trusted_domains: config
            .trusted_domains
            .iter()
            .map(|domain| parse_id16(domain, "replication.bridge.trusted_domains"))
            .collect::<Result<BTreeSet<_>>>()?,
        trusted_origins: parse_signers(&config.trusted_origins)?,
        role,
        directory: config.directory.clone().into(),
        stream: config.stream.clone(),
        recipient: parse_key32(&config.recipient, "replication.bridge.recipient")?,
        receiver_secret: config
            .receiver_secret_hex
            .as_deref()
            .map(|secret| parse_key32(secret, "replication.bridge.receiver_secret_hex"))
            .transpose()?
            .map(zeroize::Zeroizing::new),
        signer: config
            .signer_hex
            .as_deref()
            .map(|signer| parse_key32(signer, "replication.bridge.signer_hex"))
            .transpose()?
            .map(zeroize::Zeroizing::new),
        trusted_signers: parse_signers(&config.trusted_signers)?,
        max_bundle_bytes: config.max_bundle_bytes,
        max_payload_bytes: config.max_payload_bytes,
        staging_bytes: config.staging_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parsers_accept_documented_forms() {
        assert_eq!(units::parse_bytes("64MiB").unwrap(), 64 << 20);
        assert_eq!(units::parse_bytes("64KB").unwrap(), 64_000);
        assert_eq!(units::parse_bytes("1024").unwrap(), 1024);
        assert_eq!(units::parse_bytes("2gib").unwrap(), 2 << 30);
        assert!(units::parse_bytes("10XB").is_err());
        assert!(units::parse_bytes("-1").is_err());
        assert_eq!(units::parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(units::parse_duration("7d").unwrap(), Duration::from_secs(7 * 86400));
        assert_eq!(units::parse_duration("5").unwrap(), Duration::from_secs(5));
        assert!(units::parse_duration("1fortnight").is_err());
        assert_eq!(units::format_bytes(64 << 20), "64MiB");
        assert_eq!(units::format_duration(Duration::from_secs(90)), "90s");
    }

    #[test]
    fn defaults_mirror_engine_options() {
        let options = Config::default().engine.to_options().unwrap();
        let expected = crate::Options::default();
        assert_eq!(options.capture_interval, expected.capture_interval);
        assert_eq!(options.capture_bytes, expected.capture_bytes);
        assert_eq!(options.queue_bytes, expected.queue_bytes);
        assert_eq!(options.transaction_bytes, expected.transaction_bytes);
        assert_eq!(options.segment_bytes, expected.segment_bytes);
        assert_eq!(options.spool_block_bytes, expected.spool_block_bytes);
        assert_eq!(options.spool_cache_bytes, expected.spool_cache_bytes);
        assert_eq!(options.snapshot_deltas, expected.snapshot_deltas);
        assert_eq!(options.snapshot_bytes, expected.snapshot_bytes);
        assert_eq!(options.compression_level, expected.compression_level);
        assert_eq!(options.storage_workers, expected.storage_workers);
        assert_eq!(options.data_key_max_age, expected.data_key_max_age);
        assert_eq!(options.scheduler.local_share, expected.scheduler.local_share);
        assert_eq!(options.scheduler.remote_share, expected.scheduler.remote_share);
        assert_eq!(options.scheduler.max_debt, expected.scheduler.max_debt);
        assert_eq!(
            options.scheduler.maintenance_reserve_every,
            expected.scheduler.maintenance_reserve_every
        );
    }

    #[test]
    fn invalid_values_fail_with_context() {
        let mut config = Config::default();
        config.server.bind = "not-an-addr".into();
        assert!(config.validate().is_err());
        let mut config = Config::default();
        config.engine.queue_bytes = 1;
        assert!(config.validate().is_err());
        let mut config = Config::default();
        config.auth.users.push(UserConfig {
            name: "admin".into(),
            password_hash: "plaintext".into(),
        });
        assert!(config.validate().unwrap_err().to_string().contains("Argon2"));
        let mut config = Config::default();
        config.server.production = true;
        assert!(config.validate().is_err());
        let mut config = Config::default();
        config.logging.rotation = "hourly".into();
        assert!(config.validate().is_err());
        let mut config = Config::default();
        config.replication.enabled = true;
        assert!(config.validate().is_err());
        // Unknown keys are rejected, not ignored.
        let parsed: std::result::Result<Config, _> = toml::from_str("[server]\nbindd = \"x\"\n");
        assert!(parsed.is_err());
    }

    #[test]
    fn table_flag_save_preserves_comments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            "# operator comment\n[server]\nbind = \"127.0.0.1:5432\"\n",
        )
        .unwrap();
        save_table_flag(&path, "scratch", false).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("# operator comment"));
        assert!(text.contains("127.0.0.1:5432"));
        let loaded = Config::load(&path).unwrap();
        assert_eq!(loaded.replication.tables.get("scratch"), Some(&false));
        save_table_flag(&path, "scratch", true).unwrap();
        let loaded = Config::load(&path).unwrap();
        assert_eq!(loaded.replication.tables.get("scratch"), Some(&true));
        assert!(save_table_flag(&path, "  ", true).is_err());
    }
}
