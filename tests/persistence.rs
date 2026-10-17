use sqlite_session::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
fn options() -> Options {
    Options {
        mode: OpenMode::CreateNew,
        capture_interval: Duration::from_millis(30),
        ..Options::default()
    }
}
fn create(path: &std::path::Path) -> Result<Database> {
    let db = Database::open(path, [7; 32], options())?;
    db.migrate(Migration::new("CREATE TABLE items(id TEXT PRIMARY KEY NOT NULL, a TEXT NOT NULL, b INTEGER NOT NULL); CREATE INDEX items_b ON items(b);"))?;
    db.set_replication("items", true)?;
    Ok(db)
}
fn count(db: &Database) -> Result<i64> {
    db.read(|c| Ok(c.query_row("SELECT count(*) FROM items", [], |r| r.get(0))?))
}
#[test]
fn encrypted_recovery_and_key_check() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = create(&path)?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES('one','secret-test-value',1)", [])?;
        Ok(())
    })?;
    let state = db.export_state()?;
    let node = db.node_id();
    db.close()?;
    let manifest = std::fs::read(path.join("manifest"))?;
    let keys = std::fs::read(path.join("keys.enc"))?;
    assert!(matches!(
        Database::open(&path, [8; 32], Options::default()),
        Err(Error::WrongKey)
    ));
    assert_eq!(manifest, std::fs::read(path.join("manifest"))?);
    assert_eq!(keys, std::fs::read(path.join("keys.enc"))?);
    for e in std::fs::read_dir(path.join("segments"))? {
        let b = std::fs::read(e?.path())?;
        assert!(!b.windows(17).any(|s| s == b"secret-test-value"));
    }
    let db = Database::open(&path, [7; 32], Options::default())?;
    assert_eq!(db.export_state()?, state);
    assert_eq!(db.node_id(), node);
    assert_eq!(count(&db)?, 1);
    db.close()
}
#[test]
fn rollback_and_access_controls() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = create(&tmp.path().join("db"))?;
    assert!(
        db.transaction::<(), _>(|c| {
            c.execute("INSERT INTO items VALUES('bad','bad',1)", [])?;
            Err(Error::Invalid("rollback".into()))
        })
        .is_err()
    );
    assert_eq!(count(&db)?, 0);
    assert!(
        db.transaction::<(), _>(|c| {
            c.execute("INSERT INTO items VALUES('panic','bad',1)", [])?;
            panic!("rollback")
        })
        .is_err()
    );
    assert_eq!(count(&db)?, 0);
    for sql in [
        "CREATE TABLE bypass(id INTEGER PRIMARY KEY)",
        "BEGIN",
        "PRAGMA writable_schema=ON",
        "INSERT INTO _crdt_clock VALUES(1,x'00')",
    ] {
        let sql = sql.to_string();
        assert!(
            db.transaction(move |c| {
                c.execute_batch(&sql)?;
                Ok(())
            })
            .is_err()
        );
    }
    assert!(
        db.read(|c| {
            c.execute("INSERT INTO items VALUES('bad','bad',1)", [])?;
            Ok(())
        })
        .is_err()
    );
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES('ok','ok',2)", [])?;
        Ok(())
    })?;
    assert!(
        db.transaction(|c| {
            c.execute("UPDATE items SET id='new' WHERE id='ok'", [])?;
            Ok(())
        })
        .is_err()
    );
    db.close()
}
#[test]
fn migrations_preserve_mapped_versions() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = create(&path)?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES('one','old',1)", [])?;
        Ok(())
    })?;
    let old = db.export_state()?;
    // Renames require a local-only table; re-enable afterwards so exports
    // include the rows again.
    db.set_replication("items", false)?;
    db.migrate(Migration{sql:"ALTER TABLE items RENAME TO things; ALTER TABLE things RENAME COLUMN a TO title; ALTER TABLE things ADD COLUMN extra TEXT NOT NULL DEFAULT 'x'; UPDATE things SET b=b+1;".into(),mappings:vec![TableMapping{old_name:"items".into(),new_name:"things".into(),columns:vec![("a".into(),"title".into())]}]})?;
    db.set_replication("things", true)?;
    let new = db.export_state()?;
    assert!(new.rows[0].generation > old.rows[0].generation);
    let a = old.schema.tables[0]
        .columns
        .iter()
        .find(|c| c.name == "a")
        .unwrap();
    assert_eq!(
        old.rows[0].cells.iter().find(|c| c.column == a.id).map(|c|&c.value),
        new.rows[0].cells.iter().find(|c| c.column == a.id).map(|c|&c.value)
    );
    db.close()?;
    let db = Database::open(path, [7; 32], Options::default())?;
    assert_eq!(db.export_state()?, new);
    db.close()
}
#[test]
fn snapshots_reclaim_without_losing_history() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let mut opts = options();
    opts.snapshot_deltas = 2;
    let db = Database::open(&path, [7; 32], opts)?;
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
    ))?;
    db.set_replication("items", true)?;
    for n in 0..8 {
        db.transaction(move |c| {
            c.execute(
                "INSERT INTO items VALUES(?,?)",
                rusqlite::params![n, format!("v{n}")],
            )?;
            Ok(())
        })?;
        db.flush()?;
    }
    // Drops require a local-only table; re-enable before exporting.
    db.set_replication("items", false)?;
    db.migrate(Migration::new(
        "ALTER TABLE items DROP COLUMN value; CREATE INDEX items_id ON items(id)",
    ))?;
    db.set_replication("items", true)?;
    let state = db.export_state()?;
    db.close()?;
    let db = Database::open(path, [7; 32], Options::default())?;
    assert_eq!(state, db.export_state()?);
    db.close()
}
#[test]
fn storage_failure_stops_mutations_and_preserves_reads() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let armed = Arc::new(AtomicBool::new(false));
    let flag = armed.clone();
    let mut opts = options();
    opts.fault_hooks = Some(Arc::new(spool::FaultHooks {
        segment_sync: Some(Arc::new(move || {
            if flag.load(Ordering::SeqCst) {
                Err("injected sync failure".into())
            } else {
                Ok(())
            }
        })),
        ..Default::default()
    }));
    let db = Database::open(tmp.path().join("db"), [7; 32], opts)?;
    db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY)"))?;
    db.set_replication("items", true)?;
    db.flush()?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES(1)", [])?;
        Ok(())
    })?;
    armed.store(true, Ordering::SeqCst);
    assert!(db.flush().is_err());
    assert!(
        db.transaction(|c| {
            c.execute("INSERT INTO items VALUES(2)", [])?;
            Ok(())
        })
        .is_err()
    );
    assert_eq!(count(&db)?, 1);
    assert!(db.status().storage_error.is_some());
    armed.store(false, Ordering::SeqCst);
    db.retry_storage()?;
    assert!(db.status().storage_error.is_none());
    let expected = db.export_state()?;
    db.close()?;
    let recovered = Database::open(tmp.path().join("db"), [7; 32], Options::default())?;
    assert_eq!(expected, recovered.export_state()?);
    recovered.close()
}
#[test]
fn periodic_checkpoint() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = create(&tmp.path().join("db"))?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES('one','a',1)", [])?;
        Ok(())
    })?;
    let limit = std::time::Instant::now() + Duration::from_secs(3);
    while db.status().durable < db.status().committed && std::time::Instant::now() < limit {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(db.status().durable, db.status().committed);
    db.close()
}
#[test]
fn crash_child() {
    if let Ok(path) = std::env::var("SQLITE_SESSION_CRASH_PATH") {
        let db = create(std::path::Path::new(&path)).unwrap();
        db.transaction(|c| {
            c.execute("INSERT INTO items VALUES('durable','a',1)", [])?;
            Ok(())
        })
        .unwrap();
        db.flush().unwrap();
        db.transaction(|c| {
            c.execute("INSERT INTO items VALUES('pending','a',2)", [])?;
            Ok(())
        })
        .unwrap();
        std::process::exit(0);
    }
}
#[test]
fn process_crash_recovers_committed_checkpoint() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let status = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "crash_child", "--nocapture"])
        .env("SQLITE_SESSION_CRASH_PATH", &path)
        .status()?;
    assert!(status.success());
    let db = Database::open(path, [7; 32], Options::default())?;
    assert_eq!(
        db.read(|c| Ok(c.query_row(
            "SELECT count(*) FROM items WHERE id='durable'",
            [],
            |r| r.get::<_, i64>(0)
        )?))?,
        1
    );
    db.close()
}
#[test]
fn snapshot_failure_boundaries_are_retryable() -> Result<()> {
    for (hook, nth) in [
        ("append", 1),
        ("append", 2),
        ("append", 3),
        ("append", 4),
        ("sync", 1),
        ("sync", 2),
        ("sync", 3),
    ] {
        let tmp = tempfile::tempdir()?;
        let armed = Arc::new(AtomicBool::new(false));
        let flag = armed.clone();
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = hits.clone();
        let f: spool::FaultHook = Arc::new(move || {
            if flag.load(Ordering::SeqCst) && counter.fetch_add(1, Ordering::SeqCst) + 1 == nth {
                Err("checkpoint boundary failure".into())
            } else {
                Ok(())
            }
        });
        let mut faults = spool::FaultHooks::default();
        match hook {
            "append" => faults.append = Some(f),
            "sync" => faults.segment_sync = Some(f),
            "manifest" => faults.manifest_persist = Some(f),
            "compact" => faults.compaction_stage = Some(f),
            "rename" => faults.rename = Some(f),
            "dirsync" => faults.dir_sync = Some(f),
            "delete" => faults.delete = Some(f),
            _ => unreachable!(),
        }
        let mut opts = options();
        opts.snapshot_deltas = 1;
        opts.fault_hooks = Some(Arc::new(faults));
        let path = tmp.path().join("db");
        let db = Database::open(&path, [7; 32], opts)?;
        db.migrate(Migration::new(
            "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
        ))?;
    db.set_replication("items", true)?;
        db.flush()?;
        db.transaction(|c| {
            c.execute("INSERT INTO items VALUES(1,'pending')", [])?;
            Ok(())
        })?;
        armed.store(true, Ordering::SeqCst);
        let failed = db.flush().is_err();
        armed.store(false, Ordering::SeqCst);
        assert!(failed, "hook {hook} occurrence {nth} was not exercised");
        db.retry_storage()?;
        let expected = db.export_state()?;
        db.close()?;
        let db = Database::open(path, [7; 32], Options::default())?;
        assert_eq!(expected, db.export_state()?, "hook {hook}");
        db.close()?;
    }
    Ok(())
}
#[test]
fn schema_validation_and_noop_versions() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = create(&tmp.path().join("db"))?;
    for sql in [
        "CREATE TABLE missing(a TEXT)",
        "CREATE TABLE badpk(id TEXT PRIMARY KEY)",
        "CREATE TABLE badunique(id INTEGER PRIMARY KEY,a TEXT UNIQUE)",
        "CREATE TABLE badcheck(id INTEGER PRIMARY KEY,a INTEGER CHECK(a>0))",
        "CREATE VIRTUAL TABLE search USING fts5(body)",
        "CREATE TRIGGER application AFTER INSERT ON items BEGIN DELETE FROM items; END",
        "CREATE TABLE _CRDT_bypass(id INTEGER PRIMARY KEY)",
    ] {
        assert!(db.migrate(Migration::new(sql)).is_err(), "{sql}");
    }
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES('one','original',1)", [])?;
        Ok(())
    })?;
    let before = db.export_state()?;
    db.transaction(|c| {
        c.execute("UPDATE items SET a='temporary' WHERE id='one'", [])?;
        c.execute("UPDATE items SET a='original' WHERE id='one'", [])?;
        Ok(())
    })?;
    let after = db.export_state()?;
    assert_eq!(before.rows[0].cells, after.rows[0].cells);
    db.close()
}
#[test]
fn corruption_fails_closed() -> Result<()> {
    use std::io::{Read, Seek, SeekFrom, Write};
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = create(&path)?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES('one','data',1)", [])?;
        Ok(())
    })?;
    db.close()?;
    let segment = std::fs::read_dir(path.join("segments"))?
        .next()
        .unwrap()?
        .path();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment)?;
    file.seek(SeekFrom::Start(80))?;
    let mut byte = [0];
    file.read_exact(&mut byte)?;
    byte[0] ^= 0x40;
    file.seek(SeekFrom::Start(80))?;
    file.write_all(&byte)?;
    file.sync_all()?;
    assert!(Database::open(path, [7; 32], Options::default()).is_err());
    Ok(())
}
#[test]
fn queue_and_transaction_budgets_rollback() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut opts = options();
    opts.capture_bytes = 8192;
    opts.queue_bytes = 65536;
    opts.transaction_bytes = 32768;
    let db = Database::open(tmp.path().join("db"), [7; 32], opts)?;
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY,value BLOB)",
    ))?;
    db.set_replication("items", true)?;
    db.flush()?;
    assert!(matches!(
        db.transaction(|c| {
            c.execute("INSERT INTO items VALUES(1,zeroblob(40000))", [])?;
            Ok(())
        }),
        Err(Error::Limit(_))
    ));
    assert_eq!(count(&db)?, 0);
    db.close()
}
#[test]
fn data_key_rotation_and_table_rebuild() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = create(&path)?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES('one','keep',1)", [])?;
        Ok(())
    })?;
    let before = db.export_state()?;
    db.rotate_data_key()?;
    // Table rebuilds drop and rename; they require a local-only table.
    db.set_replication("items", false)?;
    db.migrate(Migration{sql:"CREATE TABLE replacement(id TEXT PRIMARY KEY NOT NULL,a TEXT NOT NULL,b INTEGER NOT NULL); INSERT INTO replacement SELECT id,a,b+1 FROM items; DROP TABLE items; ALTER TABLE replacement RENAME TO items; CREATE INDEX items_b ON items(b);".into(),mappings:Vec::new()})?;
    db.set_replication("items", true)?;
    let after = db.export_state()?;
    assert!(after.rows[0].generation > before.rows[0].generation);
    db.close()?;
    let db = Database::open(path, [7; 32], Options::default())?;
    assert_eq!(after, db.export_state()?);
    db.close()
}
#[test]
fn master_key_rotation_rewraps_keyring() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = create(&path)?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES('one','keep',1)", [])?;
        Ok(())
    })?;
    db.rotate_master_key([9; 32])?;
    assert_eq!(count(&db)?, 1);
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES('two','after',2)", [])?;
        Ok(())
    })?;
    db.close()?;
    assert!(matches!(
        Database::open(&path, [7; 32], Options::default()),
        Err(Error::WrongKey)
    ));
    let db = Database::open(&path, [9; 32], Options::default())?;
    assert_eq!(count(&db)?, 2);
    db.close()
}
#[test]
fn delete_reinsert_in_one_transaction_and_composite_keys() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = Database::open(tmp.path().join("db"), [7; 32], options())?;
    db.migrate(Migration::new("CREATE TABLE pairs(a TEXT NOT NULL,b INTEGER NOT NULL,v BLOB,PRIMARY KEY(a,b)) WITHOUT ROWID"))?;
    db.set_replication("pairs", true)?;
    db.transaction(|c| {
        c.execute("INSERT INTO pairs VALUES('x',1,x'00ff')", [])?;
        Ok(())
    })?;
    let old = db.export_state()?;
    db.transaction(|c| {
        c.execute("DELETE FROM pairs", [])?;
        c.execute("INSERT INTO pairs VALUES('x',1,x'0102')", [])?;
        Ok(())
    })?;
    let new = db.export_state()?;
    // A session coalesces delete/reinsert into a net update.
    assert_eq!(new.rows[0].generation, old.rows[0].generation);
    assert_eq!(new.tombstones.len(), 0);
    db.merge_state(old)?;
    assert_eq!(db.export_state()?, new);
    db.close()
}
#[test]
fn bounded_queue_rejects_writes_without_blocking_sqlite() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let armed = Arc::new(AtomicBool::new(false));
    let flag = armed.clone();
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let hold = gate.clone();
    let hook: spool::FaultHook = Arc::new(move || {
        if flag.load(Ordering::SeqCst) {
            let (mut ready, cond) = (hold.0.lock().unwrap(), &hold.1);
            while !*ready {
                ready = cond.wait(ready).unwrap();
            }
        }
        Ok(())
    });
    let mut opts = options();
    opts.capture_bytes = 4096;
    opts.queue_bytes = 32768;
    opts.transaction_bytes = 16384;
    opts.capture_interval = Duration::from_secs(60);
    opts.fault_hooks = Some(Arc::new(spool::FaultHooks {
        append: Some(hook),
        ..Default::default()
    }));
    let db = Database::open(tmp.path().join("db"), [7; 32], opts)?;
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY,value BLOB)",
    ))?;
    db.set_replication("items", true)?;
    db.flush()?;
    armed.store(true, Ordering::SeqCst);
    let mut busy = false;
    let mut unexpected = None;
    for id in 0..100 {
        match db.transaction(move |c| {
            c.execute("INSERT INTO items VALUES(?,zeroblob(2048))", [id])?;
            Ok(())
        }) {
            Ok(()) => {}
            Err(Error::Busy) => {
                busy = true;
                break;
            }
            Err(e) => {
                unexpected = Some(e);
                break;
            }
        }
    }
    let pending = db.status().pending_bytes;
    let rows = count(&db);
    *gate.0.lock().unwrap() = true;
    armed.store(false, Ordering::SeqCst);
    gate.1.notify_all();
    if let Some(e) = unexpected {
        return Err(e);
    }
    assert!(busy);
    assert!(pending <= 32768);
    assert!(rows? > 0);
    db.flush()?;
    db.close()
}
#[test]
fn crash_during_checkpoint_child() {
    let Ok(path) = std::env::var("SQLITE_SESSION_BOUNDARY_PATH") else {
        return;
    };
    let target: usize = std::env::var("SQLITE_SESSION_BOUNDARY_NTH")
        .unwrap()
        .parse()
        .unwrap();
    let kind = std::env::var("SQLITE_SESSION_BOUNDARY_HOOK").unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let flag = armed.clone();
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = hits.clone();
    let hook: spool::FaultHook = Arc::new(move || {
        if flag.load(Ordering::SeqCst) && counter.fetch_add(1, Ordering::SeqCst) + 1 == target {
            std::process::exit(42);
        }
        Ok(())
    });
    let mut faults = spool::FaultHooks::default();
    if kind == "append" {
        faults.append = Some(hook);
    } else {
        faults.segment_sync = Some(hook);
    }
    let mut opts = options();
    opts.capture_interval = Duration::from_secs(3600);
    opts.snapshot_deltas = 1;
    opts.fault_hooks = Some(Arc::new(faults));
    let db = Database::open(path, [7; 32], opts).unwrap();
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)",
    ))
    .unwrap();
    db.set_replication("items", true).unwrap();
    db.flush().unwrap();
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES(1,'durable')", [])?;
        Ok(())
    })
    .unwrap();
    db.flush().unwrap();
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES(2,'latest')", [])?;
        Ok(())
    })
    .unwrap();
    armed.store(true, Ordering::SeqCst);
    let _ = db.flush();
    panic!("fault boundary was not exercised");
}
#[test]
fn process_crashes_during_snapshot_and_log_publication() -> Result<()> {
    for (hook, nth) in [
        ("append", 1),
        ("append", 2),
        ("append", 3),
        ("append", 4),
        ("sync", 1),
        ("sync", 2),
        ("sync", 3),
    ] {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("db");
        let status = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", "crash_during_checkpoint_child", "--nocapture"])
            .env("SQLITE_SESSION_BOUNDARY_PATH", &path)
            .env("SQLITE_SESSION_BOUNDARY_HOOK", hook)
            .env("SQLITE_SESSION_BOUNDARY_NTH", nth.to_string())
            .status()?;
        assert_eq!(status.code(), Some(42));
        let db = Database::open(path, [7; 32], Options::default())?;
        let state = db.export_state()?;
        assert!((1..=2).contains(&state.rows.len()));
        assert!(state.rows.iter().any(|r| r.cells.iter().any(|c|state.schema.tables.iter().flat_map(|t|&t.columns).any(|col|col.name=="id"&&col.id==c.column)&&c.value==Value::Integer(1))));
        assert!(state.rows.iter().all(|r| r.cells.len() == 3));
        db.close()?;
    }
    Ok(())
}

#[test]
fn application_database_has_no_crdt_tables_or_triggers() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = create(&tmp.path().join("db"))?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES('one','value',1)", [])?;
        Ok(())
    })?;
    db.flush()?;
    let internal: i64 = db.read(|c| {
        Ok(c.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='trigger' OR name GLOB '_crdt_*'",
            [],
            |r| r.get(0),
        )?)
    })?;
    assert_eq!(internal, 0);
    db.close()
}

#[test]
fn versions_use_collection_time_while_worker_is_blocked() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let armed = Arc::new(AtomicBool::new(false));
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    // Release the worker even if an assertion or operation fails.
    struct Release(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.0.lock().unwrap() = true;
            self.0.1.notify_all();
        }
    }
    let flag = armed.clone();
    let hold = gate.clone();
    let (entered, receive) = std::sync::mpsc::channel();
    let hook: spool::FaultHook = Arc::new(move || {
        if flag.load(Ordering::SeqCst) {
            let mut ready = hold.0.lock().unwrap();
            if !*ready {
                let _ = entered.send(());
                while !*ready {
                    ready = hold.1.wait(ready).unwrap();
                }
            }
        }
        Ok(())
    });
    let opts = Options {
        mode: OpenMode::CreateNew,
        capture_interval: Duration::from_millis(30),
        capture_bytes: 4096,
        fault_hooks: Some(Arc::new(spool::FaultHooks {
            append: Some(hook),
            ..Default::default()
        })),
        ..Options::default()
    };
    let db = Database::open(tmp.path().join("db"), [7; 32], opts)?;
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY,value BLOB)",
    ))?;
    db.set_replication("items", true)?;
    db.flush()?;
    let release = Release(gate);
    armed.store(true, Ordering::SeqCst);
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES(1,zeroblob(5000))", [])?;
        Ok(())
    })?;
    receive
        .recv_timeout(Duration::from_secs(5))
        .expect("worker did not reach append hook");
    let previous_pending=db.status().pending_bytes;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES(2,zeroblob(5000))", [])?;
        Ok(())
    })?;
    let deadline=std::time::Instant::now()+Duration::from_secs(2);
    while db.status().pending_bytes<=previous_pending {
        assert!(std::time::Instant::now()<deadline,"second scheduled capture did not queue");
        std::thread::sleep(Duration::from_millis(5));
    }
    let collected_by = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let pending = db.status().pending_bytes;
    std::thread::sleep(Duration::from_millis(50));
    armed.store(false, Ordering::SeqCst);
    drop(release);
    let state = db.export_state()?;
    assert!(pending > 0);
    let row = state
        .rows
        .iter()
        .find(|r| r.cells.iter().any(|c|state.schema.tables.iter().flat_map(|t|&t.columns).any(|col|col.name=="id"&&col.id==c.column)&&c.value==Value::Integer(2)))
        .unwrap();
    assert!(
        row.generation.millis <= collected_by,
        "version was assigned after collection"
    );
    assert!(row.cells.iter().all(|cell| cell.version == row.generation));
    db.close()?;
    let reopened = Database::open(tmp.path().join("db"), [7; 32], Options::default())?;
    assert_eq!(reopened.export_state()?, state);
    reopened.close()
}
