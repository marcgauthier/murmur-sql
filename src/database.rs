use crate::{
    Error, Result,
    capture::Capture,
    model::*,
    schema,
    storage::{self, Event, Work},
};
use rusqlite::{
    Connection, Params, Row, Statement,
    hooks::{AuthAction, Authorization},
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
        mpsc,
    },
    thread,
    time::{Duration,Instant},
};
/// Scoped SQL access. Does not expose connection hooks or transaction controls.
pub struct Sql<'a> {
    pub(crate) connection: &'a Connection,
    pub(crate) schema: &'a Schema,
    #[cfg(feature="replication")]
    pub(crate) intents: Option<&'a std::cell::RefCell<Vec<PolicyIntent>>>,
    #[cfg(feature="replication")]
    pub(crate) access: &'a AtomicU8,
}
impl Sql<'_> {
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.prepare(sql)?.execute(params)
    }
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        for statement in crate::primary_key::statements(sql)? {
            let rewritten = crate::primary_key::rewrite(statement, self.schema)?;
            self.connection.execute_batch(&rewritten)?;
        }
        Ok(())
    }
    pub fn prepare(&self, sql: &str) -> rusqlite::Result<Statement<'_>> {
        self.connection.prepare(&crate::primary_key::rewrite(sql, self.schema)?)
    }
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, f: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.prepare(sql)?.query_row(params, f)
    }
    pub fn last_insert_rowid(&self) -> i64 {
        self.connection.last_insert_rowid()
    }
}
type Job = Box<dyn FnOnce(&mut Core) + Send>;
enum Message {
    Job(Job),
    Stop(mpsc::Sender<Result<()>>),
}
pub(crate) struct Inner {
    tx: mpsc::Sender<Message>,
    pub(crate) worker: mpsc::Sender<Work>,
    pub(crate) store: spool::Store,
    #[cfg(feature = "replication")]
    pub(crate) replication: Mutex<Option<murmur_replication::Handle>>,
    pub(crate) thread: Mutex<Option<thread::JoinHandle<()>>>,
    status: Arc<Mutex<Status>>,
    identity: Identity,
    pub(crate) sub_mgr: Arc<crate::subscription::SubscriptionManager>,
    pub(crate) scheduler: Arc<crate::scheduler::WriterScheduler>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.scheduler.close();
        #[cfg(feature = "replication")]
        if let Some(handle) = self.replication.get_mut().unwrap().take() { let _ = handle.close(); }
        let (tx, rx) = mpsc::channel();
        let _ = self.tx.send(Message::Stop(tx));
        let _ = rx.recv();
        if let Some(h) = self.thread.get_mut().unwrap().take() {
            let _ = h.join();
        }
    }
}
#[derive(Clone)]
pub struct Database {
    pub(crate) inner: Arc<Inner>,
}
impl Database {
    pub fn open(path: impl AsRef<std::path::Path>, key: impl AsRef<[u8]>, options: Options) -> Result<Self> {
        Self::open_inner(path, key, options, None)
    }
    pub(crate) fn open_with_flags(path: impl AsRef<std::path::Path>, key: impl AsRef<[u8]>, options: Options, flags: std::collections::BTreeMap<String,bool>) -> Result<Self> {
        Self::open_inner(path, key, options, Some(flags))
    }
    fn open_inner(
        path: impl AsRef<std::path::Path>,
        key: impl AsRef<[u8]>,
        #[allow(unused_mut)]
        mut options: Options,
        flags: Option<std::collections::BTreeMap<String,bool>>,
    ) -> Result<Self> {
        #[cfg(feature = "replication")]
        if let Some(config) = &options.replication {
            let id = murmur_replication::transport::node_id(config.credentials.chain.first().ok_or_else(||Error::Invalid("missing certificate".into()))?).map_err(|e| Error::Invalid(e.to_string()))?;
            options.node_id = Some(options.node_id.unwrap_or(id));
        }
        #[cfg(feature = "replication")]
        let network_config = options.replication.clone();
        options.validate()?;
        let scheduler_config = options.scheduler.clone();
        let path = path.as_ref().to_path_buf();
        let mut key = zeroize::Zeroizing::new(key.as_ref().to_vec());
        let (tx, rx) = mpsc::channel();
        let (start_tx, start_rx) = mpsc::channel();
        let status = Arc::new(Mutex::new(Status::default()));
        let state = status.clone();
        let progress = crate::progress::OpenProgressReporter::new(options.on_open_progress.clone());
        let p = progress.clone();
        let handle = thread::Builder::new()
            .name("sqlite-writer".into())
            .spawn(move || {
                let setup = (|| -> Result<Core> {
                    let mut opened = storage::open(&path, &key, &options, &p)?;
                    zeroize::Zeroize::zeroize(&mut *key);
                    let connection = storage::recover_current(&opened, &p)?;
                    opened.history.clear();
                    let identity = opened.identity.clone();
                    let schema = opened.schema.clone();
                    let cursor = opened.cursor;
                    let collection_clock = opened.clock;
                    let mut search = crate::search::Manager::new(&options);
                    let definitions = crate::search::schema_definitions(&options.search_indexes, &schema)?;
                    crate::search::Manager::register(&connection, &definitions)?;
                    search.sync_schema(&connection, &schema, options.mode == OpenMode::CreateNew)?;
                    search.refresh(&connection, &schema)?;
                    search.take_publications();
                    state.lock().unwrap().search_indexes = search.status();
                    p.phase(OpenPhase::Finalizing);
                    let capture = Some(schema::capture(&connection, &schema)?);
                    let access = Arc::new(AtomicU8::new(0));
                    configure_authorizer(&connection, &schema, access.clone(), search.guard.clone())?;
                    let (store, worker, worker_thread) =
                        storage::start(opened, options.clone(), state.clone())?;
                    let final_progress = p.finish(&Ok(()));
                    {
                        let mut s = state.lock().unwrap();
                        s.committed = cursor;
                        s.durable = cursor;
                        s.open_progress = final_progress;
                    }
                    let sub_mgr = Arc::new(crate::subscription::SubscriptionManager::new(identity.database, 1024));
                    let core = Core {
                        capture,
                        connection,
                        access,
                        schema,
                        identity,
                        options,
                        store,
                        worker,
                        worker_thread: Some(worker_thread),
                        status: state.clone(),
                        collection_clock,
                        captured_cursor: cursor,
                        last_capture: Instant::now(),
                        pending_intents: Vec::new(),
                        table_config_path: None,
                        table_config: None,
                        transition_failed: false,
                        search,
                        last_search: Instant::now(),
                        sub_mgr: sub_mgr.clone(),
                    };
                    {
                        let mut s = state.lock().unwrap();
                        s.sqlite = Some(core.sqlite_metrics());
                    }
                    Ok(core)
                })();
                let mut core = match setup {
                    Ok(c) => {
                        let _ = start_tx.send(Ok((c.identity.clone(), c.worker.clone(), c.store.clone(), c.sub_mgr.clone())));
                        c
                    }
                    Err(e) => {
                        p.finish(&Err(Error::Storage(e.to_string())));
                        let _ = start_tx.send(Err(e));
                        return;
                    }
                };
                loop {
                    let mut delay = core
                        .capture_interval()
                        .saturating_sub(core.last_capture.elapsed());
                    if core.search.pending() {
                        delay = delay.min(core.options.search_interval.saturating_sub(core.last_search.elapsed()));
                    }
                    match rx.recv_timeout(delay) {
                        Ok(Message::Job(job)) => job(&mut core),
                        Ok(Message::Stop(reply)) => {
                            let result = core.close();
                            let _ = reply.send(result);
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            let _ = core.close();
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    if core.status.lock().unwrap().closed {
                        break;
                    }
                    if core.last_capture.elapsed() >= core.capture_interval() {
                        let _ = core.checkpoint();
                        core.last_capture = Instant::now();
                    }
                    if core.search.pending() && core.last_search.elapsed() >= core.options.search_interval {
                        let _ = core.maintain_search(false);
                        core.last_search = Instant::now();
                    }
                }
            })?;
        let (identity, worker, store, sub_mgr) = match start_rx.recv().map_err(|_| Error::Closed)? {
            Ok(i) => i,
            Err(e) => {
                let _ = handle.join();
                return Err(e);
            }
        };
        let scheduler = Arc::new(crate::scheduler::WriterScheduler::new(scheduler_config));
        let database = Self {
            inner: Arc::new(Inner {
                tx,
                worker,
                store,
                scheduler,
                #[cfg(feature = "replication")]
                replication: Mutex::new(None),
                thread: Mutex::new(Some(handle)),
                status,
                identity,
                sub_mgr,
            }),
        };
        if let Some(flags) = flags {
            if !flags.is_empty() || database.table_config_owned()? {
                if let Err(error) = database.set_table_replication_memory(flags) { let _ = database.close(); return Err(error); }
            }
        }
        #[cfg(feature = "replication")]
        if let Some(config) = network_config {
            match crate::network::attach(&database, config) {
                Ok(handle) => *database.inner.replication.lock().unwrap() = Some(handle),
                Err(error) => { let _ = database.close(); return Err(error); }
            }
        }
        Ok(database)
    }
    /// Run a blocking Core/Spool channel wait cooperatively (see
    /// `murmur_replication::run_blocking`); inline without replication.
    pub(crate) fn blocking_wait<T>(wait: impl FnOnce() -> T) -> T {
        #[cfg(feature = "replication")]
        {
            murmur_replication::run_blocking(wait)
        }
        #[cfg(not(feature = "replication"))]
        {
            wait()
        }
    }
    pub(crate) fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Core) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        if self
            .inner
            .thread
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|t| t.thread().id() == thread::current().id())
        {
            return Err(Error::Invalid(
                "database callbacks cannot reenter the database".into(),
            ));
        }
        if self.inner.status.lock().unwrap().closed {
            return Err(Error::Closed);
        }
        let (tx, rx) = mpsc::channel();
        self.inner
            .tx
            .send(Message::Job(Box::new(move |c| {
                let _ = tx.send(f(c));
            })))
            .map_err(|_| Error::Closed)?;
        Self::blocking_wait(|| rx.recv()).map_err(|_| Error::Closed)?
    }
    #[cfg(feature="replication")]
    pub(crate) fn call_cancel<T:Send+'static>(&self,f:impl FnOnce(&mut Core)->Result<T>+Send+'static,cancelled:Arc<std::sync::atomic::AtomicBool>)->Result<T> {
        if cancelled.load(Ordering::Acquire){return Err(Error::Closed);}
        let (tx,rx)=mpsc::channel();let check=cancelled.clone();
        self.inner.tx.send(Message::Job(Box::new(move|core|{let result=if check.load(Ordering::Acquire){Err(Error::Closed)}else{f(core)};let _=tx.send(result);}))).map_err(|_|Error::Closed)?;
        Self::blocking_wait(|| loop {match rx.recv_timeout(Duration::from_millis(20)){Ok(result)=>return result,Err(mpsc::RecvTimeoutError::Disconnected)=>return Err(Error::Closed),Err(mpsc::RecvTimeoutError::Timeout)=>if cancelled.load(Ordering::Acquire){return Err(Error::Closed);}}})
    }
    pub fn transaction<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Sql<'_>) -> Result<T> + Send + 'static,
    {
        let ticket = self.inner.scheduler.admit(crate::scheduler::WriterClass::Local)?;
        let res = self.call(move |c| c.transaction(f));
        drop(ticket);
        res
    }
    pub fn read<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Sql<'_>) -> Result<T> + Send + 'static,
    {
        self.call(move |c| {
            c.access.store(Access::Read as u8, Ordering::Relaxed);
            let result = catch_unwind(AssertUnwindSafe(|| {
                f(&Sql {
                    connection: &c.connection,
                    schema: &c.schema,
                    #[cfg(feature="replication")] intents: None,
                    #[cfg(feature="replication")] access: &c.access,
                })
            }))
            .unwrap_or(Err(Error::CallbackPanicked));
            c.access.store(0, Ordering::Relaxed);
            result
        })
    }
    pub fn migrate(&self, migration: Migration) -> Result<()> {
        let ticket = self.inner.scheduler.admit(crate::scheduler::WriterClass::Maintenance)?;
        let res = self.call(move |c| c.migrate(migration));
        drop(ticket);
        res
    }
    /// Collect pending writes and wait for local search indexes to catch up.
    /// Does not wait for remote peers or require persistence to finish.
    pub fn refresh_indexes(&self) -> Result<()> {
        self.call(|c| { c.checkpoint()?; c.maintain_search(true) })
    }
    /// Replace explicit node-local index definitions atomically and rebuild them.
    /// Schema-derived column FTS indexes are retained. Supply explicit definitions
    /// again in Options when reopening the database.
    pub fn set_search_indexes(&self, definitions: Vec<crate::SearchIndex>) -> Result<()> {
        self.call(move |c| {
            c.writable()?;
            c.checkpoint()?;
            let mut options = c.options.clone();
            options.search_indexes = definitions;
            options.validate()?;
            let mut search = crate::search::Manager::new(&options);
            crate::search::Manager::register(&c.connection, &options.search_indexes)?;
            c.connection.execute_batch("BEGIN IMMEDIATE")?;
            let result = (|| -> Result<()> {
                c.search.drop_indexes(&c.connection)?;
                search.sync_schema(&c.connection, &c.schema, false)?;
                search.refresh(&c.connection, &c.schema)?;
                c.connection.execute_batch("COMMIT")?;
                Ok(())
            })();
            if let Err(error) = result {
                let _ = c.connection.execute_batch("ROLLBACK");
                return Err(error);
            }
            c.options = options;
            c.search = search;
            configure_authorizer(&c.connection, &c.schema, c.access.clone(), c.search.guard.clone())?;
            c.status.lock().unwrap().search_indexes = c.search.status();
            c.sub_mgr.notify_reset("search indexes reconfigured");
            Ok(())
        })
    }
    /// Persist a column's FTS declaration as replicated schema metadata.
    /// Requires TEXT and one INTEGER application primary key. Index contents
    /// remain node-local and follow source changes asynchronously.
    pub fn set_column_fts(&self, table: &str, column: &str, enabled: bool) -> Result<()> {
        let table = table.to_owned();
        let column = column.to_owned();
        let ticket = self.inner.scheduler.admit(crate::scheduler::WriterClass::Maintenance)?;
        let result = self.call(move |c| c.set_column_fts(&table, &column, enabled));
        drop(ticket);
        result
    }

    /// Persist or remove a vector declaration. Requires BLOB, one INTEGER
    /// application key, and the vector feature. None disables the declaration.
    pub fn set_column_vector(&self, table: &str, column: &str, config: Option<crate::VectorColumnConfig>) -> Result<()> {
        let table = table.to_owned(); let column = column.to_owned();
        let ticket = self.inner.scheduler.admit(crate::scheduler::WriterClass::Maintenance)?;
        let result = self.call(move |c| c.set_column_search(&table, &column, move |c| c.vector = config));
        drop(ticket);
        result
    }

    /// Persist a table's local replication setting with its schema in Spool.
    /// Tables default to local-only. Changing modes rebuilds the physical primary key.
    pub fn set_replication(&self, table_name: &str, enabled: bool) -> Result<()> {
        let table_name = table_name.to_owned();
        let ticket = self.inner.scheduler.admit(crate::scheduler::WriterClass::Maintenance)?;
        let result = self.call(move |c| c.set_table_replication(&table_name, enabled));
        drop(ticket); result
    }
    pub(crate) fn set_replication_config(&self, name: &str, enabled: bool, path: &std::path::Path) -> Result<()> {
        let name = name.to_owned(); let path = path.to_path_buf();
        let ticket = self.inner.scheduler.admit(crate::scheduler::WriterClass::Maintenance)?;
        let result = self.call(move |c| c.set_table_mode(&name, enabled, Some(&path)));
        drop(ticket); result
    }
    /// Alias matching the requested API spelling.
    #[allow(non_snake_case)]
    pub fn SetReplication(&self, table_name: &str, value: bool) -> Result<()> {
        self.set_replication(table_name, value)
    }
    /// Reconcile table structures against the authoritative config map before
    /// serving SQL or starting replication.
    pub(crate) fn set_table_replication_memory(
        &self,
        map: std::collections::BTreeMap<String, bool>,
    ) -> Result<()> {
        self.call(move |c| {
            let tables = c.schema.tables.iter().map(|t| (t.name.clone(), t.id.clone())).collect::<Vec<_>>();
            for (name, id) in tables { c.set_table_replication(&name, map.get(&id).copied().unwrap_or(false))?; }
            c.schema.table_replication = map;
            Ok(())
        })
    }
    pub(crate) fn bind_table_config(&self, path: &std::path::Path, config: Arc<std::sync::RwLock<crate::config::Config>>) -> Result<()> {
        let path=path.to_path_buf(); self.call(move |c|{c.table_config_path=Some(path);c.table_config=Some(config);Ok(())})
    }
    pub(crate) fn table_config_owned(&self) -> Result<bool> {
        Ok(self.inner.store.get(b"00/schema/config-owned")?.is_some())
    }
    pub(crate) fn mark_table_config_owned(&self) -> Result<()> {
        self.call(|c| {
            let (tx, rx) = mpsc::channel();
            c.worker.send(Work::Network(Box::new(move |opened| {
                let result = opened.store.commit(&[spool::Mutation::put(b"00/schema/config-owned".to_vec(), vec![1])], spool::Durability::Sync).map_err(Error::from);
                let _ = tx.send(result);
            }))).map_err(|_|Error::Closed)?;
            rx.recv().map_err(|_|Error::Closed)?
        })
    }
    /// Read the table's current local replication setting.
    pub fn replication_enabled(&self, table_name: &str) -> Result<bool> {
        let table_name = table_name.to_owned();
        self.call(move |c| {
            let table = c.schema.tables.iter().find(|t| t.name.eq_ignore_ascii_case(&table_name))
                .ok_or_else(|| Error::Invalid(format!("unknown table: {table_name}")))?;
            Ok(crate::table_replication::enabled(&c.schema, &table.id))
        })
    }
    pub fn flush(&self) -> Result<()> {
        self.call(|c| c.flush())
    }
    /// Online backup to a new directory via spool's consistent checkpoint.
    /// Flushes durable state first; writes after the checkpoint are not included.
    /// The backup keeps the live encryption key and opens with [`Database::open`].
    pub fn backup(&self, destination: impl AsRef<std::path::Path>) -> Result<()> {
        let destination = destination.as_ref().to_path_buf();
        self.call(move |c| {
            c.flush()?;
            let (tx, rx) = mpsc::channel();
            c.worker
                .send(Work::Backup {
                    destination,
                    reply: tx,
                })
                .map_err(|_| Error::Closed)?;
            rx.recv().map_err(|_| Error::Closed)?
        })
    }
    /// Restore a backup directory to a new live directory, preserving identity.
    /// Verifies the restored copy opens and rebuilds before returning.
    pub fn restore(
        backup: impl AsRef<std::path::Path>,
        destination: impl AsRef<std::path::Path>,
        key: impl AsRef<[u8]>,
    ) -> Result<()> {
        Self::restore_with_options(backup, destination, key, RestoreOptions::default())
    }
    /// Restore with optional fresh database/node identity for cloning.
    /// `None` preserves the backup's identity. Clones exchanging state must
    /// use a distinct node id with the same database id.
    pub fn restore_with_options(
        backup: impl AsRef<std::path::Path>,
        destination: impl AsRef<std::path::Path>,
        key: impl AsRef<[u8]>,
        options: RestoreOptions,
    ) -> Result<()> {
        crate::backup::restore_backup(
            backup.as_ref(),
            destination.as_ref(),
            key.as_ref(),
            &options,
        )
    }
    /// Verify a backup directory opens and rebuilds, without mutating it.
    pub fn verify_backup(
        backup: impl AsRef<std::path::Path>,
        key: impl AsRef<[u8]>,
    ) -> Result<()> {
        crate::backup::verify_backup_dir(backup.as_ref(), key.as_ref())
    }
    pub fn rotate_data_key(&self) -> Result<()> {
        self.call(|c| {
            c.flush()?;
            let (tx, rx) = mpsc::channel();
            c.worker.send(Work::Rotate(tx)).map_err(|_| Error::Closed)?;
            rx.recv().map_err(|_| Error::Closed)?
        })
    }
    /// Replace the 32-byte application (master) key. Data keys are
    /// rewrapped under the new key; bulk data is not re-encrypted.
    /// The old key stops working immediately: reopen with `new_key`.
    pub fn rotate_master_key(&self, new_key: [u8; 32]) -> Result<()> {
        let ticket = self.inner.scheduler.admit(crate::scheduler::WriterClass::Maintenance)?;
        let res = self.call(move |c| {
            c.flush()?;
            let (tx, rx) = mpsc::channel();
            c.worker
                .send(Work::RotateMasterKey {
                    key: new_key.to_vec(),
                    reply: tx,
                })
                .map_err(|_| Error::Closed)?;
            rx.recv().map_err(|_| Error::Closed)?
        });
        drop(ticket);
        res
    }
    /// Retry retained checkpoints after the storage problem is resolved.
    pub fn retry_storage(&self) -> Result<()> {
        self.call(|c| {
            if c.transition_failed { return Err(Error::Storage("table transition interrupted; close and reopen the database".into())); }
            let (tx, rx) = mpsc::channel();
            c.worker.send(Work::Retry(tx)).map_err(|_| Error::Closed)?;
            rx.recv().map_err(|_| Error::Closed)??;
            c.flush()
        })
    }
    pub fn export_state(&self) -> Result<ReplicaState> {
        self.call(|c| c.export(true))
    }
    pub fn merge_state(&self, state: ReplicaState) -> Result<()> {
        // Prepare joins off the database thread. If concurrent writes changed
        // the baseline, retry from a new durable boundary.
        for _ in 0..8 {
            let (worker, baseline, cursor) = self.call(|c| {
                // Local-only rows must remain in the join and persisted metadata.
                let baseline = c.export(false)?;
                Ok((
                    c.worker.clone(),
                    baseline,
                    c.status.lock().unwrap().committed,
                ))
            })?;
            let (tx, rx) = mpsc::channel();
            worker
                .send(Work::Merge {
                    baseline,
                    cursor,
                    state: state.clone(),
                    reply: tx,
                })
                .map_err(|_| Error::Closed)?;
            let patch = match rx.recv().map_err(|_| Error::Closed)? {
                Err(Error::Busy) => continue,
                result => result?,
            };
            match self.call(move |c| c.apply_patch(patch)) {
                Err(Error::Busy) => continue,
                result => return result,
            }
        }
        Err(Error::Busy)
    }
    pub fn open_progress(&self) -> Option<OpenProgress> {
        self.inner.status.lock().unwrap().open_progress.clone()
    }
    pub fn sqlite_metrics(&self) -> Result<SqliteMetrics> {
        self.call(|c| Ok(c.sqlite_metrics()))
    }
    pub fn spool_metrics(&self) -> SpoolMetrics {
        SpoolMetrics::from(self.inner.store.stats())
    }
    pub fn status(&self) -> Status {
        let mut s = self.inner.status.lock().unwrap().clone();
        s.spool = Some(SpoolMetrics::from(self.inner.store.stats()));
        s.scheduler = Some(self.inner.scheduler.snapshot());
        #[cfg(feature = "replication")]
        {
            s.replication = self.inner.replication.lock().unwrap().as_ref().map(|h| h.status());
        }
        s
    }
    pub fn scheduler(&self) -> Arc<crate::scheduler::WriterScheduler> {
        self.inner.scheduler.clone()
    }
    pub fn database_id(&self) -> [u8; 16] {
        self.inner.identity.database
    }
    pub fn node_id(&self) -> [u8; 16] {
        self.inner.identity.node
    }
    pub fn key_inventory(&self) -> spool::KeyInventory {
        self.inner.store.key_inventory()
    }
    pub fn schema(&self) -> Result<Schema> {
        self.call(|c| Ok(c.schema.clone()))
    }
    /// Create a reactive query subscription observing local and replicated changes.
    pub fn subscribe<T, Q, K>(
        &self,
        query: Q,
        key_fn: K,
        options: crate::subscription::SubscriptionOptions,
    ) -> Result<crate::subscription::Subscription<T>>
    where
        T: Clone + PartialEq + Send + 'static,
        Q: Fn(&Sql<'_>) -> rusqlite::Result<Vec<T>> + Send + Sync + 'static,
        K: Fn(&T) -> Vec<u8> + Send + Sync + 'static,
    {
        crate::subscription::start_subscription(self, query, key_fn, options)
    }
    #[cfg(feature = "replication")]
    pub fn replication(&self) -> Option<murmur_replication::Handle> { self.inner.replication.lock().unwrap().clone() }
    pub fn close(&self) -> Result<()> {
        if self
            .inner
            .thread
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|t| t.thread().id() == thread::current().id())
        {
            return Err(Error::Invalid(
                "database callbacks cannot close the database".into(),
            ));
        }
        if self.inner.status.lock().unwrap().closed {
            return Ok(());
        }
        self.inner.sub_mgr.close();
        self.inner.scheduler.close();
        #[cfg(feature = "replication")]
        if let Some(handle) = self.inner.replication.lock().unwrap().take() { handle.close().map_err(|e|Error::Storage(e.to_string()))?; }
        let (tx, rx) = mpsc::channel();
        self.inner
            .tx
            .send(Message::Stop(tx))
            .map_err(|_| Error::Closed)?;
        let result = rx.recv().map_err(|_| Error::Closed)?;
        if let Some(h) = self.inner.thread.lock().unwrap().take() {
            h.join().map_err(|_| Error::CallbackPanicked)?;
        }
        result
    }
}
pub(crate) struct Core {
    // Field order matters: sessions are destroyed before the connection.
    pub(crate) capture: Option<Capture>,
    pub(crate) connection: Connection,
    pub(crate) access: Arc<AtomicU8>,
    pub(crate) schema: Schema,
    pub(crate) identity: Identity,
    options: Options,
    pub(crate) store: spool::Store,
    pub(crate) worker: mpsc::Sender<Work>,
    worker_thread: Option<thread::JoinHandle<()>>,
    pub(crate) status: Arc<Mutex<Status>>,
    pub(crate) collection_clock: Version,
    pub(crate) captured_cursor: u64,
    last_capture: Instant,
    pending_intents: Vec<PolicyIntent>,
    pub(crate) table_config_path: Option<std::path::PathBuf>,
    pub(crate) table_config: Option<Arc<std::sync::RwLock<crate::config::Config>>>,
    transition_failed: bool,
    pub(crate) search: crate::search::Manager,
    last_search: Instant,
    pub(crate) sub_mgr: Arc<crate::subscription::SubscriptionManager>,
}
impl Core {
    pub(crate) fn maintain_search(&mut self, all: bool) -> Result<()> {
        let result = if all { self.search.refresh(&self.connection, &self.schema) }
            else { self.search.tick(&self.connection, &self.schema) };
        self.status.lock().unwrap().search_indexes = self.search.status();
        let published = self.search.take_publications();
        if !published.is_empty() {
            let cursor = self.status.lock().unwrap().committed;
            self.sub_mgr.notify_commit(cursor, &published);
        }
        result.map(|_| ())
    }
    fn sqlite_metrics(&self) -> SqliteMetrics {
        let memory_used_bytes = unsafe { rusqlite::ffi::sqlite3_memory_used() as i64 };
        let memory_highwater_bytes = unsafe { rusqlite::ffi::sqlite3_memory_highwater(0) as i64 };
        let page_size: i64 = self.connection.query_row("PRAGMA page_size", [], |r| r.get(0)).unwrap_or(0);
        let page_count: i64 = self.connection.query_row("PRAGMA page_count", [], |r| r.get(0)).unwrap_or(0);
        let database_size_bytes = page_size * page_count;
        let freelist_pages: i64 = self.connection.query_row("PRAGMA freelist_count", [], |r| r.get(0)).unwrap_or(0);
        let schema_version: i64 = self.connection.query_row("PRAGMA schema_version", [], |r| r.get(0)).unwrap_or(0);
        let table_count = self.schema.tables.len();
        let index_count = self.schema.indexes.len();
        let mut total_rows = 0u64;
        for table in &self.schema.tables {
            let count: i64 = self.connection.query_row(
                &format!("SELECT COUNT(*) FROM {}", crate::model::quote(&table.name)),
                [],
                |r| r.get(0),
            ).unwrap_or(0);
            total_rows = total_rows.saturating_add(count.max(0) as u64);
        }
        let session_memory_bytes = self.capture.as_ref().map(|c| c.memory()).unwrap_or(0);
        let session_changeset_bytes = self.capture.as_ref().map(|c| c.size()).unwrap_or(0);

        SqliteMetrics {
            memory_used_bytes,
            memory_highwater_bytes,
            page_size,
            page_count,
            database_size_bytes,
            freelist_pages,
            schema_version,
            table_count,
            index_count,
            total_rows,
            session_memory_bytes,
            session_changeset_bytes,
        }
    }
    fn writable(&self) -> Result<()> {
        if self.transition_failed { return Err(Error::Storage("table transition interrupted; close and reopen the database".into())); }
        let s = self.status.lock().unwrap();
        if s.closed {
            return Err(Error::Closed);
        }
        if let Some(e) = &s.storage_error {
            return Err(Error::Storage(e.clone()));
        }
        if s.pending_bytes >= self.options.queue_bytes {
            return Err(Error::Busy);
        }
        Ok(())
    }
    pub(crate) fn admit(&self, bytes: usize) -> Result<()> {
        let s = self.status.lock().unwrap();
        #[cfg(feature="replication")]
        if s.pending_bytes==0 && bytes>self.options.queue_bytes && self.options.replication.as_ref().is_some_and(|config|bytes<=config.max_transaction_bytes.saturating_mul(2).saturating_add(4<<20)) {return Ok(());}
        if s.pending_bytes
            .checked_add(bytes)
            .is_none_or(|n| n > self.options.queue_bytes)
        {
            return Err(Error::Busy);
        }
        Ok(())
    }
    pub(crate) fn bump(&self) -> Result<u64> {
        let mut s = self.status.lock().unwrap();
        s.committed = s
            .committed
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("commit cursor exhausted".into()))?;
        Ok(s.committed)
    }
    pub(crate) fn send(&self, event: Event, cursor: u64, bytes: usize) -> Result<()> {
        self.admit(bytes)?;
        self.status.lock().unwrap().pending_bytes += bytes;
        if self
            .worker
            .send(Work::Checkpoint {
                event,
                cursor,
                bytes,
            })
            .is_err()
        {
            self.status.lock().unwrap().storage_error = Some("checkpoint worker stopped".into());
            return Err(Error::Closed);
        }
        Ok(())
    }
    fn capture_interval(&self)->Duration {
        #[cfg(feature="replication")]
        if self.options.replication.as_ref().is_some_and(|c|c.adaptive_sync) {
            let pending=self.status.lock().unwrap().pending_bytes;
            return self.options.capture_interval.min(if pending>=self.options.queue_bytes/2 {Duration::from_millis(100)}else{Duration::from_secs(1)});
        }
        self.options.capture_interval
    }
    #[cfg(feature="replication")]
    pub(crate) fn advance_disjoint_patch(&mut self,patch:&mut crate::merge::Patch,keys:&std::collections::BTreeSet<(String,Vec<u8>)>)->Result<bool> {
        let committed={let status=self.status.lock().unwrap();if status.storage_error.is_some(){return Ok(false);}status.committed};
        if committed==patch.cursor{return Ok(true);}
        // An intervening capture/schema/publication has no retained key proof.
        // Fall back to a fresh merge rather than maintaining a metadata journal.
        if self.captured_cursor!=patch.cursor{return Ok(false);}
        if !self.pending_intents.is_empty(){return Ok(false);}
        let bytes=self.capture.as_mut().ok_or(Error::Closed)?.bytes()?;
        if crate::metadata::changeset_overlaps(&self.schema,&bytes,keys)?{return Ok(false);}
        // FIFO storage ordering publishes these unrelated local keys first.
        // No authoritative metadata is cached or rewritten for them by the patch.
        self.checkpoint()?;patch.cursor=committed;Ok(true)
    }
    pub(crate) fn checkpoint(&mut self) -> Result<()> {
        self.writable()?;
        let start = Instant::now();
        let cap = self.capture.as_mut().ok_or(Error::Closed)?;
        let estimate = cap.size();
        if estimate == 0 && self.status.lock().unwrap().committed == self.captured_cursor {
            self.last_capture = Instant::now();
            return Ok(());
        }
        self.admit(estimate)?;
        let bytes = self.capture.as_mut().unwrap().bytes()?;
        let version = self.collection_clock.next(self.identity.node)?;
        let length=bytes.len()+encode(&self.pending_intents)?.len();
        self.admit(length)?;
        let new = schema::capture(&self.connection, &self.schema)?;
        self.capture = Some(new);
        let cursor = self.status.lock().unwrap().committed;
        let intents=std::mem::take(&mut self.pending_intents);
        let affected = crate::metadata::tables_in_changeset(&bytes);
        self.search.enqueue(&bytes, &affected);
        self.status.lock().unwrap().search_indexes = self.search.status();
        self.send(Event::Delta { bytes, version, intents }, cursor, length)?;
        self.collection_clock = version;
        self.captured_cursor = cursor;
        self.sub_mgr.notify_commit(cursor, &affected);
        {
            let mut status = self.status.lock().unwrap();
            status.checkpoint_pause = start.elapsed();
            status.sqlite = Some(self.sqlite_metrics());
        }
        self.last_capture = Instant::now();
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        match self.checkpoint() {
            Err(Error::Busy) => {
                // Explicit durability requests may wait for storage. Ordinary
                // transaction admission continues to return Busy immediately.
                self.barrier()?;
                self.checkpoint()?;
            }
            result => result?,
        }
        self.barrier()
    }
    fn barrier(&self) -> Result<()> {
        let (tx, rx) = mpsc::channel();
        self.worker
            .send(Work::Barrier(tx))
            .map_err(|_| Error::Closed)?;
        rx.recv().map_err(|_| Error::Closed)?
    }
    fn export(&mut self, replicated_only: bool) -> Result<ReplicaState> {
        self.flush()?;
        let rows = schema::read_rows(&self.connection, &self.schema)?;
        let (tx, rx) = mpsc::channel();
        self.worker
            .send(Work::Export { rows, replicated_only, reply: tx })
            .map_err(|_| Error::Closed)?;
        rx.recv().map_err(|_| Error::Closed)?
    }
    fn finish_commit(&mut self) -> Result<()> {
        let cap = self.capture.as_ref().unwrap();
        if cap.memory() > self.options.transaction_bytes
            || cap.size() > self.options.transaction_bytes
        {
            return Err(Error::Limit(
                "transaction/session capture budget exceeded".into(),
            ));
        }
        self.admit(cap.size())?;
        Ok(())
    }
    fn transaction<T>(&mut self, f: impl FnOnce(&Sql<'_>) -> Result<T>) -> Result<T> {
        self.writable()?;
        #[cfg(feature="replication")]
        let intents=std::cell::RefCell::new(Vec::new());
        let before_size=self.capture.as_ref().unwrap().size();
        let before_changes = self.connection.total_changes();
        self.connection.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| {
            self.access.store(Access::Write as u8, Ordering::Relaxed);
            let value = catch_unwind(AssertUnwindSafe(|| {
                f(&Sql {
                    connection: &self.connection,
                    schema: &self.schema,
                    #[cfg(feature="replication")] intents: Some(&intents),
                    #[cfg(feature="replication")] access: &self.access,
                })
            }))
            .unwrap_or(Err(Error::CallbackPanicked));
            self.access.store(0, Ordering::Relaxed);
            let value = value?;
            let dirty = self.connection.total_changes() != before_changes;
            self.finish_commit().map_err(|error| {
                if matches!(error,Error::Limit(_)) && before_size>0 && self.capture.as_ref().unwrap().size().saturating_sub(before_size)<=self.options.transaction_bytes {Error::Busy}else{error}
            })?;
            #[cfg(feature="replication")]
            {
                let intent_bytes=encode(&*intents.borrow())?.len()+encode(&self.pending_intents)?.len();
                let captured=self.capture.as_ref().unwrap().size();
                if captured.saturating_add(intent_bytes)>self.options.transaction_bytes {return Err(Error::Limit("policy intent/session budget exceeded".into()));}
                self.admit(captured+intent_bytes)?;
            }
            self.connection.execute_batch("COMMIT")?;
            #[cfg(feature="replication")]
            self.pending_intents.extend(intents.take());
            if dirty {
                self.bump()?;
            }
            Ok(value)
        })();
        if result.is_err() {
            self.access.store(0, Ordering::Relaxed);
            let _ = self.connection.execute_batch("ROLLBACK");
        }
        result
    }
    fn set_column_fts(&mut self, table: &str, column: &str, enabled: bool) -> Result<()> {
        self.set_column_search(table, column, |c| c.fts = enabled)
    }
    fn set_column_search(&mut self, table: &str, column: &str, change: impl FnOnce(&mut Column)) -> Result<()> {
        self.writable()?;
        if column.eq_ignore_ascii_case(crate::primary_key::COLUMN) {
            return Err(Error::Invalid("column search declarations require an application column".into()));
        }
        let mut target = self.schema.clone();
        let table = target.tables.iter_mut().find(|t| t.name.eq_ignore_ascii_case(table))
            .ok_or_else(|| Error::Invalid(format!("unknown table: {table}")))?;
        let column = table.columns.iter_mut().find(|c| c.name.eq_ignore_ascii_case(column))
            .ok_or_else(|| Error::Invalid(format!("unknown column: {}.{column}", table.name)))?;
        change(column);
        if target == self.schema { return Ok(()); }
        // Validate before collecting writes or touching the current indexes.
        let mut validation = self.options.clone();
        validation.search_indexes = crate::search::schema_definitions(&validation.search_indexes, &target)?;
        validation.validate()?;
        crate::search::Manager::new(&validation).validate_schema(&target, false)?;
        self.checkpoint()?;
        // Set memory-only TEMP storage before starting the maintenance transaction.
        crate::search::Manager::register(&self.connection, &validation.search_indexes)?;
        target.history = *blake3::hash(&[self.schema.history.as_slice(), &encode(&target)?].concat()).as_bytes();
        let version = self.collection_clock.next(self.identity.node)?;
        let bytes = encode(&target)?.len();
        self.admit(bytes)?;
        let previous = self.search.clone();
        self.connection.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.search.sync_schema(&self.connection, &target, false)?;
            self.search.refresh(&self.connection, &target)?;
            self.connection.execute_batch("COMMIT")?;
            Ok(())
        })();
        if let Err(error) = result {
            let _ = self.connection.execute_batch("ROLLBACK");
            self.search = previous;
            self.search.publish_guard();
            return Err(error);
        }
        self.schema = target.clone();
        self.collection_clock = version;
        configure_authorizer(&self.connection, &self.schema, self.access.clone(), self.search.guard.clone())?;
        self.status.lock().unwrap().search_indexes = self.search.status();
        let cursor = self.bump()?;
        self.send(Event::SchemaCaptured { migration: String::new(), schema: target, before: Vec::new(), after: Vec::new(), version }, cursor, bytes)?;
        self.captured_cursor = cursor;
        self.sub_mgr.notify_reset("column search setting changed");
        Ok(())
    }

    fn set_table_replication(&mut self, name: &str, enabled: bool) -> Result<()> {
        self.set_table_mode(name, enabled, None)
    }
    fn set_table_mode(&mut self, name: &str, enabled: bool, config: Option<&std::path::Path>) -> Result<()> {
        self.writable()?;
        let old = self.schema.tables.iter().find(|t| t.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::Invalid(format!("unknown table: {name}")))?.clone();
        if old.application_key.is_some() == enabled {
            if self.schema.table_replication.get(&old.id) != Some(&enabled) {
                self.flush()?;
                let mut schema=self.schema.clone();schema.table_replication.insert(old.id.clone(),enabled);
                let version=self.collection_clock.next(self.identity.node)?;
                let bytes=encode(&schema)?.len();self.admit(bytes)?;let cursor=self.bump()?;
                self.send(Event::SchemaCaptured{migration:String::new(),schema:schema.clone(),before:Vec::new(),after:Vec::new(),version},cursor,bytes)?;
                self.captured_cursor=cursor;self.schema=schema;self.collection_clock=version;
            }
            self.barrier()?;
            if let Some(path) = config { crate::config::save_table_flag(path, &old.id, enabled)?; }
            return Ok(());
        }
        self.flush()?;
        let key = match &old.application_key {
            Some(key) => key.clone(),
            None => crate::primary_key::inspect_key(&self.connection, &old)?,
        };
        if key.collations.iter().any(|c| !["BINARY", "NOCASE", "RTRIM"].iter().any(|s| c.eq_ignore_ascii_case(s))) {
            return Err(Error::Invalid("unsupported application primary-key collation".into()));
        }
        let before = schema::read_table_rows(&self.connection, &old)?;
        let mut replacement = old.clone();
        replacement.sql = crate::primary_key::definition(&old, &key, enabled)?;
        replacement.application_key = if enabled { Some(key.clone()) } else { None };
        for column in &mut replacement.columns {
            column.primary_key = if enabled { 0 } else {key.columns.iter().position(|n| n == &column.name).map(|i| i+1).unwrap_or(0)};
            if enabled && key.columns.contains(&column.name) { column.not_null = true; }
        }
        if enabled {
            replacement.columns.push(Column { vector: None, fts: false, id: id(&[old.id.as_bytes(), crate::primary_key::COLUMN.as_bytes()].concat()),
                name: crate::primary_key::COLUMN.into(), primary_key: 1, declared_type: "TEXT".into(), not_null: true, policy: MergePolicy::Lww });
        } else { replacement.columns.retain(|c| c.name != crate::primary_key::COLUMN); }
        let mut intended = self.schema.clone();
        *intended.tables.iter_mut().find(|t| t.id == old.id).unwrap() = replacement.clone();
        intended.table_replication.insert(old.id.clone(), enabled);
        let mut after = Vec::with_capacity(before.len());
        for row in &before {
            let mut row = row.clone();
            let app_values = key.columns.iter().map(|n| {
                let column = old.columns.iter().find(|c| c.name == *n).ok_or(Error::SchemaMismatch)?;
                row.cells.iter().find(|c| c.column == column.id).map(|c|c.value.clone()).ok_or(Error::SchemaMismatch)
            }).collect::<Result<Vec<_>>>()?;
            if enabled {
                let value = Value::Text(crate::primary_key::uuid(&old.name, &key.collations, &app_values)?);
                row.key = vec![value.clone()];
                row.cells.push(Cell {column: replacement.columns.last().unwrap().id.clone(), value, version: Version::default()});
            } else {
                row.key = app_values;
                let managed = old.columns.iter().find(|c| c.name == crate::primary_key::COLUMN).ok_or(Error::SchemaMismatch)?;
                row.cells.retain(|c| c.column != managed.id);
            }
            after.push(row);
        }
        let previous_search = self.search.clone();
        self.capture.take();
        self.connection.execute_batch("BEGIN IMMEDIATE")?;
        let mut published = false;
        let result = (|| -> Result<()> {
            // Schema maintenance uses the same database and never installs triggers.
            self.connection.execute_batch(&format!("DROP TABLE {}", quote(&old.name)))?;
            self.connection.execute_batch(&replacement.sql)?;
            for row in &after { schema::put_user_row(&self.connection, &replacement, row)?; }
            for sql in &self.schema.indexes {
                if schema::index_table(sql).is_some_and(|name| name.eq_ignore_ascii_case(&old.name)) {
                    if !enabled && crate::primary_key::references_managed_column(sql)? {continue;}
                    self.connection.execute_batch(sql)?;
                }
            }
            intended = schema::inspect(&self.connection, &intended, &Migration::new(""))?;
            // A node-local replication mode change is not an application DDL migration.
            intended.history = self.schema.history;
            self.search.sync_schema(&self.connection, &intended, true)?;
            self.search.refresh(&self.connection, &intended)?;
            // Collection time precedes storage queueing and survives retries.
            let version = self.collection_clock.next(self.identity.node)?;
            let bytes = encode(&before)?.len() + encode(&after)?.len() + encode(&intended)?.len();
            self.admit(bytes)?;
            let cursor = self.bump()?;
            self.send(Event::TableReplication { table: old.id.clone(), enabled, schema: intended.clone(), before, after, version }, cursor, bytes)?;
            self.captured_cursor = cursor;
            published = true;
            // The durable schema event is also the transition journal. Config
            // remains authoritative if a crash occurs before its atomic rename.
            self.barrier()?;
            if let Some(path) = config { crate::config::save_table_flag(path, &old.id, enabled)?; }
            self.connection.execute_batch("COMMIT")?;
            self.schema = intended.clone();
            self.collection_clock = version;
            Ok(())
        })();
        if result.is_err() {
            let _ = self.connection.execute_batch("ROLLBACK");
            self.search = previous_search;
            self.search.publish_guard();
            // A committed Spool transition cannot be undone in memory. Fail
            // closed until reopen, where configuration chooses the final mode.
            if published {
                self.transition_failed = true;
                self.status.lock().unwrap().storage_error = Some("table transition interrupted; close and reopen the database".into());
            }
        }
        self.capture = Some(schema::capture(&self.connection, &self.schema)?);
        configure_authorizer(&self.connection, &self.schema, self.access.clone(), self.search.guard.clone())?;
        self.status.lock().unwrap().search_indexes = self.search.status();
        if result.is_ok() { self.sub_mgr.notify_reset("table replication mode changed"); }
        result
    }
    fn migrate(&mut self, m: Migration) -> Result<()> {
        self.writable()?;
        self.checkpoint()?;
        let previous_search = self.search.clone();
        self.capture.take();

        let sql_keywords = crate::schema::keywords(&m.sql);
        let index_only = !sql_keywords.is_empty()
            && m.mappings.is_empty()
            && sql_keywords.iter().all(|k| {
                [
                    "CREATE", "UNIQUE", "INDEX", "IF", "NOT", "EXISTS", "ON", "DROP", "ASC", "DESC", "WHERE", "COLLATE",
                ]
                .contains(&k.as_str())
            });

        let candidate_tables: Vec<Table> = if index_only {
            Vec::new()
        } else {
            let sql_lower = m.sql.to_ascii_lowercase();
            let mut candidates = Vec::new();
            for t in &self.schema.tables {
                let name_lower = t.name.to_ascii_lowercase();
                let in_sql = sql_lower.contains(&name_lower);
                let in_map = m.mappings.iter().any(|map| map.old_name.eq_ignore_ascii_case(&t.name) || map.new_name.eq_ignore_ascii_case(&t.name));
                if in_sql || in_map {
                    candidates.push(t.clone());
                }
            }
            candidates
        };

        let mut before_candidate_rows = Vec::new();
        for t in &candidate_tables {
            before_candidate_rows.extend(schema::read_table_rows(&self.connection, t)?);
        }

        let total_changes_before = self.connection.total_changes();

        self.connection.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<(Schema, Vec<(Vec<RowState>, Vec<RowState>)>, Version)> {
            self.access
                .store(Access::Migration as u8, Ordering::Relaxed);
            let sql_result = (|| -> rusqlite::Result<()> {
                for statement in crate::primary_key::statements(&m.sql)? {
                    self.connection.execute_batch(&crate::primary_key::rewrite(statement, &self.schema)?)?;
                }
                Ok(())
            })();
            self.access.store(0, Ordering::Relaxed);
            sql_result?;
            #[allow(unused_mut)]
            let mut new = schema::inspect(&self.connection, &self.schema, &m)?;
            #[cfg(feature="replication")]
            crate::policy::configure(&mut new, self.options.replication.as_ref())?;
            self.search.sync_schema(&self.connection, &new, true)?;
            self.search.refresh(&self.connection, &new)?;

            let total_changes_after = self.connection.total_changes();
            let dml_occurred = total_changes_before != total_changes_after;

            let mut affected_before = Vec::new();
            let mut affected_after = Vec::new();

            if !index_only || dml_occurred {
                for old_t in &self.schema.tables {
                    let new_t = new.tables.iter().find(|t| t.id == old_t.id);
                    match new_t {
                        None => {
                            let b_rows = before_candidate_rows.iter().filter(|r| r.table == old_t.id).cloned().collect::<Vec<_>>();
                            affected_before.extend(b_rows);
                        }
                        Some(new_t) => {
                            let schema_changed = old_t.name != new_t.name || old_t.columns != new_t.columns;
                            let in_candidates = candidate_tables.iter().any(|t| t.id == old_t.id);
                            if schema_changed || (dml_occurred && in_candidates) {
                                let b_rows = before_candidate_rows.iter().filter(|r| r.table == old_t.id).cloned().collect::<Vec<_>>();
                                affected_before.extend(b_rows);
                                affected_after.extend(schema::read_table_rows(&self.connection, new_t)?);
                            }
                        }
                    }
                }
                for new_t in &new.tables {
                    if !self.schema.tables.iter().any(|t| t.id == new_t.id) {
                        affected_after.extend(schema::read_table_rows(&self.connection, new_t)?);
                    }
                }
            }

            let max_chunk_bytes = (self.options.transaction_bytes / 2).min(2 * 1024 * 1024);
            let chunks = schema::chunk_migration_rows(affected_before, affected_after, 2000, max_chunk_bytes);
            let version = self.collection_clock.next(self.identity.node)?;
            self.connection.execute_batch("COMMIT")?;
            Ok((new, chunks, version))
        })();

        match result {
            Ok((new, chunks, version)) => {
                self.collection_clock = version;
                self.schema = new.clone();
                configure_authorizer(&self.connection, &self.schema, self.access.clone(), self.search.guard.clone())?;
                self.status.lock().unwrap().search_indexes = self.search.status();
                self.capture = Some(schema::capture(&self.connection, &self.schema)?);

                if chunks.is_empty() {
                    let bytes = m.sql.len() + encode(&new)?.len();
                    if bytes > self.options.transaction_bytes {
                        return Err(Error::Limit("migration capture budget exceeded".into()));
                    }
                    let cursor = self.bump()?;
                    self.send(
                        Event::SchemaCaptured {
                            migration: m.sql,
                            schema: new,
                            before: Vec::new(),
                            after: Vec::new(),
                            version,
                        },
                        cursor,
                        bytes,
                    )?;
                    self.captured_cursor = cursor;
                } else {
                    for (i, (chunk_before, chunk_after)) in chunks.into_iter().enumerate() {
                        let is_first = i == 0;
                        let chunk_bytes = if is_first {
                            encode(&chunk_before)?.len() + encode(&chunk_after)?.len() + m.sql.len() + encode(&new)?.len()
                        } else {
                            encode(&chunk_before)?.len() + encode(&chunk_after)?.len()
                        };
                        if chunk_bytes > self.options.transaction_bytes {
                            return Err(Error::Limit("migration capture budget exceeded".into()));
                        }
                        let cursor = self.bump()?;
                        if is_first {
                            self.send(
                                Event::SchemaCaptured {
                                    migration: m.sql.clone(),
                                    schema: new.clone(),
                                    before: chunk_before,
                                    after: chunk_after,
                                    version,
                                },
                                cursor,
                                chunk_bytes,
                            )?;
                        } else {
                            self.send(
                                Event::MigrationChunk {
                                    before: chunk_before,
                                    after: chunk_after,
                                    version,
                                },
                                cursor,
                                chunk_bytes,
                            )?;
                        }
                        self.captured_cursor = cursor;
                        self.barrier()?;
                    }
                }
                self.sub_mgr.notify_reset("schema migration applied");
                Ok(())
            }
            Err(e) => {
                self.access.store(0, Ordering::Relaxed);
                let _ = self.connection.execute_batch("ROLLBACK");
                self.search = previous_search;
                self.search.publish_guard();
                self.status.lock().unwrap().search_indexes = self.search.status();
                self.capture = Some(schema::capture(&self.connection, &self.schema)?);
                Err(e)
            }
        }
    }
    fn apply_patch(&mut self, p: crate::merge::Patch) -> Result<()> {
        self.writable()?;
        if self.status.lock().unwrap().committed != p.cursor {
            return Err(Error::Busy);
        }
        if p.rows.is_empty()
            && p.deleted.is_empty()
            && p.tombstones.is_empty()
            && p.clock <= self.collection_clock
        {
            return Ok(());
        }
        self.checkpoint()?;
        let state=p.state.ok_or_else(||Error::Corrupt("full import state missing".into()))?;
        let bytes = encode(&state)?.len();
        self.admit(bytes)?;
        self.connection.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for row in &p.deleted {
                let t = self
                    .schema
                    .tables
                    .iter()
                    .find(|t| t.id == row.table)
                    .ok_or(Error::SchemaMismatch)?;
                self.connection.execute(
                    &format!(
                        "DELETE FROM {} WHERE {}",
                        quote(&t.name),
                        schema::pk_columns(t)
                            .iter()
                            .map(|c| format!("{} IS ?", quote(&c.name)))
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    ),
                    rusqlite::params_from_iter(&row.key),
                )?;
            }
            for row in &p.rows {
                let t = self
                    .schema
                    .tables
                    .iter()
                    .find(|t| t.id == row.table)
                    .ok_or(Error::SchemaMismatch)?;
                schema::put_user_row(&self.connection, t, row)?;
            }
            self.finish_commit()?;
            self.connection.execute_batch("COMMIT")?;
            self.search.rows(&self.schema, &p.rows);
            self.search.rows(&self.schema, &p.deleted);
            self.status.lock().unwrap().search_indexes = self.search.status();
            self.capture = Some(schema::capture(&self.connection, &self.schema)?);
            let cursor = self.bump()?;
            self.collection_clock = self.collection_clock.max(p.clock);
            self.send(Event::Replica(state), cursor, bytes)?;
            self.captured_cursor = cursor;
            let affected: Vec<String> = self.schema.tables.iter().map(|t| t.name.clone()).collect();
            self.sub_mgr.notify_commit(cursor, &affected);
            Ok(())
        })();
        if result.is_err() {
            let _ = self.connection.execute_batch("ROLLBACK");
        }
        result
    }
    fn close(&mut self) -> Result<()> {
        let flush = self.flush();
        let (tx, rx) = mpsc::channel();
        let stopped = if self.worker.send(Work::Stop(tx)).is_ok() {
            rx.recv().unwrap_or(Err(Error::Closed))
        } else {
            Err(Error::Closed)
        };
        if let Some(handle) = self.worker_thread.take() {
            let _ = handle.join();
        }
        self.status.lock().unwrap().closed = true;
        flush.and(stopped)
    }
}
#[derive(Clone, Copy, PartialEq)]
enum Access {
    Read = 1,
    Write = 2,
    Migration = 3,
}
pub(crate) fn configure_authorizer(c: &Connection, s: &Schema, mode: Arc<AtomicU8>, search: crate::search::Guard) -> Result<()> {
    let pk = s
        .tables
        .iter()
        .flat_map(|t| {
            t.columns
                .iter()
                .filter(|c| t.application_key.is_some() && (c.primary_key > 0 || crate::primary_key::application_columns(t).iter().any(|key| key.id == c.id)))
                .map(|c| (t.name.clone(), c.name.clone()))
        })
        .collect::<Vec<_>>();
    let policies:Vec<_>=s.tables.iter().flat_map(|t|t.columns.iter().filter(|c|c.policy!=MergePolicy::Lww).map(|c|(t.name.clone(),c.name.clone()))).collect();
    let has_search = !search.read().unwrap().is_empty();
    c.authorizer(Some(move |context: rusqlite::hooks::AuthContext<'_>| {
        let access = match mode.load(Ordering::Relaxed) {
            1 => Access::Read,
            2 => Access::Write,
            3 => Access::Migration,
            _ => return Authorization::Allow,
        };
        let internal = |name: &str| name.to_ascii_lowercase().starts_with("_crdt_");
        // Managed TEMP indexes are read-only to application SQL. Module maintenance
        // runs with Access=0 on this connection, outside application callbacks.
        // Ordinary MAIN-table operations cannot address managed TEMP objects.
        // Schema validation separately rejects name collisions at migration time.
        if has_search && context.database_name != Some("main") {
            let guard = search.read().unwrap();
            match context.action {
                AuthAction::Read { table_name, .. } if guard.get(&table_name.to_ascii_lowercase()) == Some(&false) => return Authorization::Deny,
                AuthAction::Insert { table_name } | AuthAction::Delete { table_name }
                | AuthAction::Update { table_name, .. } | AuthAction::DropTable { table_name }
                | AuthAction::AlterTable { table_name, .. }
                | AuthAction::CreateTable { table_name } if guard.contains_key(&table_name.to_ascii_lowercase()) => return Authorization::Deny,
                AuthAction::CreateIndex { table_name, .. } | AuthAction::DropIndex { table_name, .. }
                    if guard.contains_key(&table_name.to_ascii_lowercase()) => return Authorization::Deny,
                AuthAction::Reindex { index_name }
                    if guard.contains_key(&index_name.to_ascii_lowercase()) => return Authorization::Deny,
                _ => {}
            }
        }
        let allowed = match context.action {
            AuthAction::Select | AuthAction::Recursive => true,
            AuthAction::Read { table_name, .. } => !internal(table_name),
            AuthAction::Insert { table_name } | AuthAction::Delete { table_name } => {
                access != Access::Read && (!internal(table_name))
            }
            AuthAction::Update {
                table_name,
                column_name,
            } => {
                access != Access::Read
                    && (!internal(table_name))
                    && !column_name.eq_ignore_ascii_case(crate::primary_key::COLUMN)
                    && !pk.iter().any(|(t,c)|t.eq_ignore_ascii_case(table_name)&&c.eq_ignore_ascii_case(column_name))
                    && (access == Access::Migration || !policies.contains(&(table_name.into(),column_name.into())))
            }
            AuthAction::Pragma {
                pragma_name,
                pragma_value,
            } => {
                (["table_xinfo", "table_info", "index_list", "index_info"].contains(&pragma_name)
                    && pragma_value.is_some_and(|name| !internal(name)))
                    || (pragma_name == "data_version" && pragma_value.is_none() && context.database_name == Some("temp"))
            }
            AuthAction::Reindex { index_name } => {
                access == Access::Migration && !internal(index_name)
            }
            AuthAction::Function { function_name } => {
                let normalized = function_name.to_ascii_lowercase();
                let function_name = normalized.as_str();
                if function_name.to_ascii_lowercase().starts_with("_crdt_") {
                    false
                } else {
                    !["load_extension", "writefile", "readfile"].contains(&function_name)
                        && (access != Access::Migration
                            || [
                                "_murmur_uuid",
                                "_murmur_affinity",
                                "abs",
                                "coalesce",
                                "ifnull",
                                "nullif",
                                "lower",
                                "upper",
                                "length",
                                "substr",
                                "substring",
                                "replace",
                                "trim",
                                "ltrim",
                                "rtrim",
                                "round",
                                "min",
                                "max",
                                "typeof",
                                "hex",
                                "unhex",
                                "quote",
                                "count",
                                "sum",
                                "total",
                                "avg",
                                "printf",
                                "sqlite_rename_table",
                                "sqlite_rename_column",
                                "sqlite_rename_test",
                                "sqlite_drop_column",
                                "sqlite_rename_quotefix",
                                "like",
                                "glob",
                            ]
                            .contains(&function_name))
                }
            }
            AuthAction::CreateTable { table_name }
            | AuthAction::DropTable { table_name }
            | AuthAction::AlterTable { table_name, .. } => {
                access == Access::Migration && !internal(table_name)
            }
            AuthAction::CreateIndex {
                table_name,
                index_name,
            }
            | AuthAction::DropIndex {
                table_name,
                index_name,
            } => access == Access::Migration && !internal(table_name) && !internal(index_name),
            _ => false,
        };
        if allowed {
            Authorization::Allow
        } else {
            Authorization::Deny
        }
    }))?;
    Ok(())
}
