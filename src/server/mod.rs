//! `murmur-server`: one Murmur database behind the Postgres wire protocol.
//!
//! The server starts with storage closed. Each connection authenticates with
//! a configured user, then must send `MURMUR UNLOCK KEY '<key>'` before any
//! other statement. The first unlock opens (or creates and encrypts) the
//! database at `storage.data_dir`; later connections present the key too and
//! are verified against the running instance.

pub mod auth;
pub(crate) mod backup;
pub mod commands;
pub(crate) mod files;
pub(crate) mod infer;
pub mod query;
pub mod tls;

use crate::config::Config;
use pgwire::error::PgWireError;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// `server_version` reported to clients and by `SELECT version()`.
pub const PG_SERVER_VERSION: &str = concat!("16.6-murmur-", env!("CARGO_PKG_VERSION"));

/// Cap on tracked per-connection sessions; oldest entries are evicted.
const MAX_SESSIONS: usize = 65536;

struct Session {
    user: String,
    locked: bool,
}

struct DbSlot {
    db: Option<crate::Database>,
    key_hash: Option<[u8; 32]>,
}

/// Shared server state: config, sessions, and the database slot.
pub struct ServerState {
    #[cfg(feature = "replication")]
    files: std::sync::Mutex<files::Handles>,
    #[cfg(feature = "replication")]
    file_epoch: std::sync::atomic::AtomicU64,
    config_path: PathBuf,
    config: std::sync::Arc<std::sync::RwLock<Config>>,
    sessions: std::sync::Mutex<SessionTable>,
    slot: std::sync::Mutex<DbSlot>,
}

struct SessionTable {
    order: VecDeque<SocketAddr>,
    map: HashMap<SocketAddr, Session>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnlockOutcome {
    OpenedNew,
    OpenedExisting,
    AlreadyOpen,
}

impl ServerState {
    pub fn new(config_path: PathBuf, config: Config) -> Self {
        Self {
            config_path,
            #[cfg(feature = "replication")]
            files: std::sync::Mutex::new(files::Handles::default()),
            #[cfg(feature = "replication")]
            file_epoch: std::sync::atomic::AtomicU64::new(0),
            config: std::sync::Arc::new(std::sync::RwLock::new(config)),
            sessions: std::sync::Mutex::new(SessionTable {
                order: VecDeque::new(),
                map: HashMap::new(),
            }),
            slot: std::sync::Mutex::new(DbSlot {
                db: None,
                key_hash: None,
            }),
        }
    }

    pub(crate) fn disconnect(&self, addr: SocketAddr) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.map.remove(&addr);
        sessions.order.retain(|a| *a != addr);
        #[cfg(feature = "replication")]
        self.files.lock().unwrap().disconnect(addr);
    }
    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    pub fn config_snapshot(&self) -> Config {
        self.config.read().unwrap().clone()
    }

    pub fn user_hash(&self, name: &str) -> Option<String> {
        self.config
            .read()
            .unwrap()
            .user(name)
            .map(|u| u.password_hash.clone())
    }

    pub fn set_table_flag_snapshot(&self, id: &str, replicated: bool) {
        self.config
            .write()
            .unwrap()
            .replication
            .tables
            .insert(id.to_owned(), replicated);
    }

    /// Unknown connections are locked (fail closed). A session only unlocks
    /// the login that presented the key, so an ephemeral-port reuse by a
    /// different user starts locked again.
    pub fn is_locked(&self, addr: SocketAddr, user: &str) -> bool {
        self.sessions
            .lock()
            .unwrap()
            .map
            .get(&addr)
            .is_none_or(|s| s.locked || s.user != user)
    }

    pub fn mark_unlocked(&self, addr: SocketAddr, user: &str) {
        let mut sessions = self.sessions.lock().unwrap();
        if !sessions.map.contains_key(&addr) {
            sessions.order.push_back(addr);
            while sessions.order.len() > MAX_SESSIONS {
                if let Some(old) = sessions.order.pop_front() {
                    sessions.map.remove(&old);
                    #[cfg(feature = "replication")]
                    self.files.lock().unwrap().disconnect(old);
                }
            }
        }
        sessions.map.insert(
            addr,
            Session {
                user: user.to_owned(),
                locked: false,
            },
        );
    }

    /// Clone the open database or fail with the locked-connection error.
    pub fn require_db(&self) -> Result<crate::Database, PgWireError> {
        self.slot.lock().unwrap().db.clone().ok_or_else(|| {
            query::user_error(
                "28000",
                "connection is locked; send MURMUR UNLOCK KEY '<key>' first".to_string(),
            )
        })
    }

    /// Open (or create) the database with `key`, or verify `key` against the
    /// running instance. Blocking: performs storage I/O.
    pub fn unlock(&self, config: &Config, key: &[u8; 32]) -> Result<UnlockOutcome, crate::Error> {
        let hash = blake3::hash(key);
        let mut slot = self.slot.lock().unwrap();
        if slot.db.is_some() {
            return if slot.key_hash == Some(*hash.as_bytes()) {
                Ok(UnlockOutcome::AlreadyOpen)
            } else {
                Err(crate::Error::WrongKey)
            };
        }
        let data_dir = Path::new(&config.storage.data_dir);
        let (db, created) = Self::open_database(config, key, data_dir)?;
        self.apply_table_settings(&db, config)?;
        slot.db = Some(db);
        slot.key_hash = Some(*hash.as_bytes());
        Ok(if created {
            UnlockOutcome::OpenedNew
        } else {
            UnlockOutcome::OpenedExisting
        })
    }

    /// Rotate the master key: `previous` must match the running instance,
    /// then the key registry is rewrapped under `new` and future unlocks
    /// require it. Sessions stay unlocked; old backups keep the old key.
    pub fn rotate_key(&self, previous: &[u8; 32], new: &[u8; 32]) -> Result<(), crate::Error> {
        if previous == new {
            return Err(crate::Error::Invalid(
                "new key must differ from the previous key".into(),
            ));
        }
        let mut slot = self.slot.lock().unwrap();
        let db = slot.db.clone().ok_or(crate::Error::Closed)?;
        if slot.key_hash != Some(*blake3::hash(previous).as_bytes()) {
            return Err(crate::Error::WrongKey);
        }
        db.rotate_master_key(*new)?;
        // The registry is rewrapped; record the new key before flushing so a
        // flush failure cannot leave the hash behind the store.
        slot.key_hash = Some(*blake3::hash(new).as_bytes());
        db.flush()?;
        Ok(())
    }

    /// Verify `key` against the running instance without opening anything.
    pub fn verify_key(&self, key: &[u8; 32]) -> Result<(), crate::Error> {
        let hash = blake3::hash(key);
        let slot = self.slot.lock().unwrap();
        match (slot.db.is_some(), slot.key_hash) {
            (true, Some(known)) if known == *hash.as_bytes() => Ok(()),
            (true, _) => Err(crate::Error::WrongKey),
            (false, _) => Err(crate::Error::Closed),
        }
    }

    /// Build engine options from `config` and open `data_dir` with `key`.
    /// Returns the database and whether it was created new.
    fn open_database(
        config: &Config,
        key: &[u8; 32],
        data_dir: &Path,
    ) -> Result<(crate::Database, bool), crate::Error> {
        let mut options = config.engine.to_options()?;
        options.mode = if data_dir.exists() {
            crate::OpenMode::Existing
        } else {
            crate::OpenMode::CreateNew
        };
        if let Some(id) = &config.storage.database_id {
            options.database_id = Some(parse_id(id)?);
        }
        if let Some(id) = &config.storage.node_id {
            options.node_id = Some(parse_id(id)?);
        }
        #[cfg(feature = "replication")]
        if config.replication.enabled {
            options.replication = Some(config.replication.to_replication_config()?);
        }
        let created = options.mode == crate::OpenMode::CreateNew;
        let db = crate::Database::open_with_flags(
            data_dir,
            key,
            options,
            config.replication.tables.clone(),
        )?;
        Ok((db, created))
    }

    /// Live restore from an unpacked backup tree. Restores into fresh staging
    /// next to the live data dir, trial-opens it with the running config,
    /// then closes the live database and swaps directories. The pre-restore
    /// data dir is preserved with a `.replaced-<utc>` suffix for the operator
    /// to inspect or delete. Returns that preserved path.
    ///
    /// Holds the slot lock throughout; in-flight queries on old clones fail
    /// with Closed. Sessions stay unlocked: the key is unchanged.
    pub fn restore_from(
        &self,
        config: &Config,
        key: &[u8; 32],
        unpacked: &Path,
    ) -> Result<PathBuf, crate::Error> {
        let mut slot = self.slot.lock().unwrap();
        if slot.db.is_none() {
            return Err(crate::Error::Closed);
        }
        let data_dir = PathBuf::from(&config.storage.data_dir);
        let parent = data_dir.parent().filter(|p| !p.as_os_str().is_empty());
        let staging_base = match parent {
            Some(parent) => parent.to_path_buf(),
            None => PathBuf::from("."),
        };
        let staging = backup::staging_dir(&staging_base, "restoring");
        // The live database keeps serving until the swap below.
        if let Err(e) = crate::Database::restore(unpacked, &staging, key) {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(e);
        }
        // Trial open with the running config catches identity or option
        // mismatches before the live data is touched.
        match Self::open_database(config, key, &staging) {
            Ok((trial, _)) => {
                if let Err(e) = trial.close() {
                    let _ = std::fs::remove_dir_all(&staging);
                    return Err(e);
                }
            }
            Err(e) => {
                let _ = std::fs::remove_dir_all(&staging);
                return Err(e);
            }
        }
        #[cfg(feature = "replication")]
        {
            let mut files = self.files.lock().unwrap();
            self.file_epoch
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            files.clear();
        }
        if let Some(live) = slot.db.take() {
            let _ = live.close();
        }
        let stamp = backup::utc_compact(std::time::SystemTime::now());
        let data_name = data_dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "data".to_string());
        let mut replaced = staging_base.join(format!(".{data_name}.replaced-{stamp}"));
        for n in 2u32.. {
            if !replaced.exists() {
                break;
            }
            replaced = staging_base.join(format!(".{data_name}.replaced-{stamp}-{n}"));
        }
        if let Err(e) = std::fs::rename(&data_dir, &replaced) {
            let reopened = Self::open_database(config, key, &data_dir).map(|(db, _)| db);
            match reopened {
                Ok(db) => slot.db = Some(db),
                Err(reopen) => {
                    tracing::error!("restore failed and live database could not reopen: {reopen}");
                }
            }
            let _ = std::fs::remove_dir_all(&staging);
            return Err(crate::Error::Io(e));
        }
        if let Err(e) = std::fs::rename(&staging, &data_dir) {
            let _ = std::fs::rename(&replaced, &data_dir);
            if let Ok((db, _)) = Self::open_database(config, key, &data_dir) {
                slot.db = Some(db);
            }
            return Err(crate::Error::Io(e));
        }
        let (db, _) = Self::open_database(config, key, &data_dir)?;
        self.apply_table_settings(&db, config)?;
        slot.db = Some(db);
        Ok(replaced)
    }

    /// Make `config.toml` the authority for per-table replication flags. A
    /// non-empty config map overlays the in-memory schema; an empty config
    /// with legacy Spool settings imports them into the config file once.
    /// Missing entries always mean local-only.
    fn apply_table_settings(
        &self,
        db: &crate::Database,
        config: &Config,
    ) -> Result<(), crate::Error> {
        db.bind_table_config(&self.config_path, self.config.clone())?;
        if !config.replication.tables.is_empty() || db.table_config_owned()? {
            db.set_table_replication_memory(config.replication.tables.clone())?;
            db.mark_table_config_owned()?;
            return Ok(());
        }
        let schema = db.schema()?;
        let spool_map = schema.table_replication.clone();
        if spool_map.is_empty() {
            db.mark_table_config_owned()?;
            return Ok(());
        }
        crate::config::save_table_flags(&self.config_path, &spool_map)?;
        db.set_table_replication_memory(spool_map.clone())?;
        self.config.write().unwrap().replication.tables = spool_map;
        db.mark_table_config_owned()?;
        tracing::info!("imported legacy table replication flags into config.toml");
        Ok(())
    }
}

fn parse_id(hex: &str) -> Result<[u8; 16], crate::Error> {
    let bytes = crate::config::parse_hex(hex, 16, "storage id")?;
    let mut id = [0u8; 16];
    id.copy_from_slice(&bytes);
    Ok(id)
}

/// Bind, accept connections through pgwire, and serve until shutdown.
/// Exit codes: 0 ok, 1 startup error, 2 bind error.
pub async fn serve(config_path: PathBuf, config: Config) -> std::process::ExitCode {
    use std::process::ExitCode;
    // Fail fast on unreadable credentials before listening.
    let tls_acceptor = match tls::build_acceptor(&config.server.tls, config.server.production) {
        Ok(acceptor) => acceptor,
        Err(e) => {
            tracing::error!("cannot initialize server TLS: {e}");
            return ExitCode::from(1);
        }
    };
    #[cfg(feature = "replication")]
    if config.replication.enabled {
        if let Err(e) = config.replication.to_replication_config() {
            tracing::error!("cannot initialize replication: {e}");
            return ExitCode::from(1);
        }
    }
    let listener = match tokio::net::TcpListener::bind(&config.server.bind).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!(bind = %config.server.bind, "cannot bind server socket: {e}");
            return ExitCode::from(2);
        }
    };
    let bound = listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| config.server.bind.clone());
    // Printed for supervisors and tests; also logged to the file log.
    println!("listening on {bound}");
    tracing::info!(bind = %bound, data = %config.storage.data_dir, "murmur-server listening");
    let production = config.server.production;
    let state = std::sync::Arc::new(ServerState::new(config_path, config));
    let mut file_cleanup = tokio::time::interval(std::time::Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = file_cleanup.tick() => {
                #[cfg(feature="replication")]
                if let Ok(mut files)=state.files.try_lock(){files.expire();}
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((socket, peer)) => {
                        tracing::info!(%peer, "connection accepted");
                        let factory = std::sync::Arc::new(auth::HandlerFactory {
                            state: state.clone(),
                            production,
                        });
                        let tls_acceptor = tls_acceptor.clone();
                        let connection_state=state.clone();
                        tokio::spawn(async move {
                            if let Err(e) =
                                pgwire::tokio::process_socket(socket, tls_acceptor, factory).await
                            {
                                tracing::info!(%peer, "connection closed: {e}");
                            }
                            tokio::task::spawn_blocking(move||connection_state.disconnect(peer));
                        });
                    }
                    Err(e) => {
                        tracing::warn!("accept failed: {e}");
                    }
                }
            }
            _ = shutdown_signal() => {
                tracing::info!("shutdown requested");
                #[cfg(feature="replication")]
                {let closing=state.clone();let _=tokio::task::spawn_blocking(move||closing.files.lock().unwrap().shutdown()).await;}
                return ExitCode::SUCCESS;
            }
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
