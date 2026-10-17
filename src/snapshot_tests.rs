//! Snapshot durability tests use the real worker, SQLite state and encrypted Spool.
use super::*;
use net::{protocol::*, snapshot as snap};

fn config(donor: [u8; 16]) -> net::Config {
    let mut c = net::Config::new(
        "127.0.0.1:0".parse().unwrap(),
        net::Credentials {
            chain: vec![],
            roots: vec![],
            private_key: vec![].into(),
        },
        [11; 32],
    );
    c.trusted_origins.insert(donor, public_key(&[11; 32]));
    c.trusted_snapshot_sources.insert(donor);
    c.minimum_retained = 2;
    c
}
fn adapter(db: &Database, c: &net::Config) -> Adapter {
    let a = Adapter {last_reclaim_ms:std::sync::atomic::AtomicU64::new(0),
        last_apply_barrier_ms:std::sync::atomic::AtomicU64::new(0),
        progress_cache:std::sync::Mutex::new(None),
        merge_deferrals:std::sync::atomic::AtomicUsize::new(0),
        cancelled:Arc::new(std::sync::atomic::AtomicBool::new(false)),
        inner: Arc::downgrade(&db.inner),
        worker: db.inner.worker.clone(),
        identity: (db.database_id(), db.node_id()),
    };
    let c = c.clone();
    a.spool(move |o| {
        o.replication_config = Some(c);
        Ok(())
    })
    .unwrap();
    a
}
fn open(path: &std::path::Path, node: u8, mode: OpenMode) -> Database {
    Database::open(
        path,
        [9; 32],
        Options {
            mode,
            capture_interval: std::time::Duration::from_secs(3600),
            database_id: Some([7; 16]),
            node_id: Some([node; 16]),
            ..Options::default()
        },
    )
    .unwrap()
}
fn create(path: &std::path::Path, node: u8) -> Database {
    let db = open(path, node, OpenMode::CreateNew);
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER,payload BLOB)",
    ))
    .unwrap();
    db.set_replication("items", true).unwrap();
    db.transaction(|s| {
        s.execute("INSERT INTO items VALUES(1,10,zeroblob(180000))", [])?;
        s.execute("INSERT INTO items VALUES(2,20,x'')", [])?;
        Ok(())
    })
    .unwrap();
    db.flush().unwrap();
    db
}
fn stage(a: &Adapter, b: &Adapter, c: &net::Config, m: &snap::Manifest, skip: Option<u32>) {
    snap::accept(b, c, m.donor, m.clone(), net::retention::now_millis()).unwrap();
    for i in (0..m.chunks.len() as u32).rev() {
        if Some(i) != skip {
            let data = a
                .get(&snap::chunk(snap::OUT, m.transfer, i))
                .unwrap()
                .unwrap();
            snap::receive(
                b,
                m.donor,
                m.transfer,
                i,
                data,
                net::retention::now_millis(),
            )
            .unwrap();
        }
    }
}

#[test]
fn snapshot_application_refreshes_managed_search_indexes() {
    let temp = tempfile::tempdir().unwrap();
    let c = config([1; 16]);
    let indexes = || {
        #[allow(unused_mut)]
        let mut definitions: Vec<crate::SearchIndex> = vec![crate::Fts5Index::new("docs_fts", "docs", vec!["title".into()]).into()];
        #[cfg(feature = "vector")]
        definitions.push(crate::VectorIndex::new("docs_vec", "docs", "embedding", 2).into());
        definitions
    };
    let mut databases = Vec::new();
    for node in 1..=2 {
        let db = Database::open(temp.path().join(format!("search-{node}")), [9; 32], Options {
            mode: OpenMode::CreateNew, database_id: Some([7; 16]), node_id: Some([node; 16]),
            search_indexes: indexes(), ..Options::default()
        }).unwrap();
        db.migrate(Migration::new("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT,embedding BLOB)")).unwrap();
    db.set_replication("docs", true).unwrap();
        db.flush().unwrap();
        databases.push(db);
    }
    let a = &databases[0]; let b = &databases[1];
    let aa = adapter(a, &c); let bb = adapter(b, &c);
    a.transaction(|sql| {
        let embedding = [1f32, 0.].into_iter().flat_map(f32::to_le_bytes).collect::<Vec<_>>();
        sql.execute("INSERT INTO docs VALUES(1,'snapshot',?)", [embedding])?;
        Ok(())
    }).unwrap();
    #[cfg(feature = "vector")]
    {
        a.set_column_vector("docs", "embedding", Some(crate::VectorColumnConfig {dimensions: 2, distance: crate::DistanceMetric::Cosine})).unwrap();
        a.flush().unwrap();
        assert!(bb.accept_schema(aa.schema().unwrap()).unwrap());
        b.flush().unwrap();
    }
    a.flush().unwrap();
    let m = aa.create_snapshot([2; 16], &c).unwrap();
    stage(&aa, &bb, &c, &m, None);
    assert!(snap::finish(&bb, &c, [1; 16], m.transfer, net::retention::now_millis()).unwrap());
    b.refresh_indexes().unwrap();
    assert_eq!(b.read(|sql| Ok(sql.query_row("SELECT rowid FROM docs_fts WHERE docs_fts MATCH 'snapshot'", [], |r| r.get::<_, i64>(0))?)).unwrap(), 1);
    #[cfg(feature = "vector")]
    assert_eq!(b.read(|sql| Ok(sql.query_row("SELECT rowid FROM docs_vec WHERE embedding MATCH '[1,0]' AND k=1", [], |r| r.get::<_, i64>(0))?)).unwrap(), 1);
    #[cfg(feature = "vector")]
    {
        let schema = b.schema().unwrap(); let table = &schema.tables[0];
        let column = table.columns.iter().find(|c| c.name == "embedding").unwrap();
        assert_eq!(column.vector.as_ref().unwrap().distance, crate::DistanceMetric::Cosine);
        let name = crate::search::vector_index_name(table,column);
        assert_eq!(b.read(move |sql| Ok(sql.query_row(&format!("SELECT rowid FROM temp.\"{name}\" WHERE embedding MATCH '[1,0]' AND k=1"), [], |r| r.get::<_,i64>(0))?)).unwrap(),1);
    }
    assert_eq!(a.export_state().unwrap().rows, b.export_state().unwrap().rows);
    a.close().unwrap(); b.close().unwrap();
}
#[test]
fn sender_and_receiver_reopen_resumes_only_missing_chunks_and_receipt_is_durable() {
    let temp = tempfile::tempdir().unwrap();
    let ap = temp.path().join("a");
    let bp = temp.path().join("b");
    let c = config([1; 16]);
    let a = create(&ap, 1);
    let b = open(&bp, 2, OpenMode::CreateNew);
    b.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER,payload BLOB)",
    ))
    .unwrap();
    b.set_replication("items", true).unwrap();
    b.flush().unwrap();
    let aa = adapter(&a, &c);
    let bb = adapter(&b, &c);
    a.transaction(|s| {
        s.execute("DELETE FROM items WHERE id=2", [])?;
        Ok(())
    })
    .unwrap();
    a.flush().unwrap();
    aa.commit(vec![Record::put(
        progress_key([1; 16]),
        net::encode(&7u64).unwrap(),
    )])
    .unwrap();
    let m = aa.create_snapshot([2; 16], &c).unwrap();
    assert!(m.chunks.len() > 2);
    stage(&aa, &bb, &c, &m, Some(1));
    assert!(!snap::finish(&bb, &c, [1; 16], m.transfer, net::retention::now_millis()).unwrap());
    a.close().unwrap();
    b.close().unwrap();
    let a = open(&ap, 1, OpenMode::Existing);
    let b = open(&bp, 2, OpenMode::Existing);
    let aa = adapter(&a, &c);
    let bb = adapter(&b, &c);
    let resumed = aa.create_snapshot([2; 16], &c).unwrap();
    assert_eq!(resumed.transfer, m.transfer);
    assert_eq!(
        snap::missing(&bb, &snap::incoming(&bb).unwrap().unwrap()).unwrap(),
        vec![1]
    );
    let data = aa
        .get(&snap::chunk(snap::OUT, m.transfer, 1))
        .unwrap()
        .unwrap();
    snap::receive(
        &bb,
        [1; 16],
        m.transfer,
        1,
        data,
        net::retention::now_millis(),
    )
    .unwrap();
    assert!(snap::finish(&bb, &c, [1; 16], m.transfer, net::retention::now_millis()).unwrap());
    assert_eq!(
        a.export_state().unwrap().rows,
        b.export_state().unwrap().rows
    );
    assert_eq!(
        a.export_state().unwrap().tombstones,
        b.export_state().unwrap().tombstones
    );
    a.transaction(|s| {
        s.execute("UPDATE items SET n=30 WHERE id=1", [])?;
        Ok(())
    })
    .unwrap();
    a.flush().unwrap(); // Capture a later catch-up target.
    aa.commit(vec![Record::put(
        progress_key([1; 16]),
        net::encode(&9u64).unwrap(),
    )])
    .unwrap();
    aa.acknowledge_snapshot([2; 16], m.transfer, m.digest)
        .unwrap();
    let pin: snap::Pin =
        net::decode(&aa.get(&snap::pin_key(m.transfer)).unwrap().unwrap()).unwrap();
    assert!(pin.target.as_ref().unwrap()[&[1; 16]] > pin.boundary[&[1; 16]]);
    aa.acknowledge_snapshot([2; 16], m.transfer, m.digest)
        .unwrap();
    assert_eq!(
        aa.get(&snap::pin_key(m.transfer)).unwrap(),
        Some(net::encode(&pin).unwrap())
    );
    assert!(
        aa.acknowledge_snapshot([3; 16], m.transfer, m.digest)
            .is_err()
    );
    assert!(
        aa.acknowledge_snapshot([2; 16], m.transfer, [0; 32])
            .is_err()
    );
    snap::maintain(&aa, net::retention::now_millis()).unwrap();
    assert!(aa.keys(snap::OUT).unwrap().is_empty());
    assert!(aa.get(&snap::pin_key(m.transfer)).unwrap().is_some());
    b.close().unwrap();
    let b = open(&bp, 2, OpenMode::Existing);
    let bb = adapter(&b, &c);
    let before = b.export_state().unwrap();
    assert!(snap::finish(&bb, &c, [1; 16], m.transfer, net::retention::now_millis()).unwrap());
    assert_eq!(b.export_state().unwrap(), before);
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn serialized_prune_rechecks_new_transfer_pin_and_capture_budget() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create(&tmp.path().join("a"), 1);
    let c = config([1; 16]);
    let a = adapter(&db, &c);
    // Seed independent-origin history before capture; newer appends must stay live.
    let origin = [8; 16];
    let head = Progress {
        origin,
        applied: 4,
        observed: 4,
        retained_from: 1,
        retained_through: 4,
    };
    let mut r = vec![
        Record::put(progress_key(origin), net::encode(&4u64).unwrap()),
        Record::put(retained_key(origin), net::encode(&head).unwrap()),
    ];
    for i in 1..=8 {
        r.push(Record::put(log_key(origin, i), vec![1]));
    }
    a.commit(r).unwrap();
    let m = a.create_snapshot([2; 16], &c).unwrap();
    a.commit(vec![
        Record::put(progress_key(origin), net::encode(&8u64).unwrap()),
        Record::put(
            retained_key(origin),
            net::encode(&Progress {
                applied: 8,
                observed: 8,
                retained_through: 8,
                ..head
            })
            .unwrap(),
        ),
    ])
    .unwrap();
    a.retention_checkpoint().unwrap();
    assert!(a.prune_history(origin, 5).is_err());
    assert_eq!(a.prune_history(origin, 4).unwrap(), 4);
    assert!(a.get(&log_key(origin, 5)).unwrap().is_some());
    let mut tiny = c.clone();
    tiny.snapshot_staging_bytes = 1;
    assert!(a.create_snapshot([3; 16], &tiny).is_err());
    assert!(a.get(&snap::pin_key(m.transfer)).unwrap().is_some());
    db.close().unwrap();
    let db = open(&tmp.path().join("a"), 1, OpenMode::Existing);
    let a = adapter(&db, &c);
    assert!(a.prune_history(origin, 5).is_err());
    db.close().unwrap();
}
#[test]
fn snapshot_waits_for_local_capture_preserves_local_only_tables_and_concurrent_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let c = config([1; 16]);
    let a = create(&tmp.path().join("a"), 1);
    let b = open(&tmp.path().join("b"), 2, OpenMode::CreateNew);
    b.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER,payload BLOB)",
    ))
    .unwrap();
    b.set_replication("items", true).unwrap();
    b.merge_state(a.export_state().unwrap()).unwrap();
    b.flush().unwrap();
    let aa = adapter(&a, &c);
    let bb = adapter(&b, &c);
    b.migrate(Migration::new(
        "CREATE TABLE private(id INTEGER PRIMARY KEY,n INTEGER)",
    ))
    .unwrap();
    b.set_replication("private", false).unwrap();
    b.transaction(|s| {
        s.execute("INSERT INTO private VALUES(1,99)", [])?;
        Ok(())
    })
    .unwrap();
    b.flush().unwrap();
    a.transaction(|s| {
        s.execute("DELETE FROM items WHERE id=2", [])?;
        s.execute("UPDATE items SET n=50 WHERE id=1", [])?;
        Ok(())
    })
    .unwrap();
    a.flush().unwrap();
    let m = aa.create_snapshot([2; 16], &c).unwrap();
    stage(&aa, &bb, &c, &m, None);
    b.transaction(|s| {
        s.execute("INSERT INTO items VALUES(3,88,x'')", [])?;
        Ok(())
    })
    .unwrap();
    // The local capture is queued before the point read and remote patch.
    // Completion waits for their durability without a separate flush/retry tick.
    let applied = snap::finish(&bb, &c, [1; 16], m.transfer, net::retention::now_millis()).unwrap();
    assert!(applied);
    assert!(bb.get(&snap::receipt_key(m.transfer)).unwrap().is_some());
    b.flush().unwrap();
    assert!(snap::finish(&bb, &c, [1; 16], m.transfer, net::retention::now_millis()).unwrap());
    assert_eq!(
        b.read(|s| Ok(s.query_row("SELECT n FROM private", [], |r| r.get::<_, i64>(0))?))
            .unwrap(),
        99
    );
    assert_eq!(
        b.read(|s| Ok(s.query_row("SELECT n FROM items WHERE id=3", [], |r| r.get::<_, i64>(0))?))
            .unwrap(),
        88
    );
    assert_eq!(
        b.read(|s| Ok(
            s.query_row("SELECT count(*) FROM items WHERE id=2", [], |r| r
                .get::<_, i64>(0))?
        ))
        .unwrap(),
        0
    );
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn capture_failure_never_publishes_partial_manifest_or_pin() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for failure in [1] {
        let tmp = tempfile::tempdir().unwrap();
        let remaining = Arc::new(AtomicUsize::new(0));
        let counter = remaining.clone();
        let hook: spool::FaultHook = Arc::new(move || {
            let left = counter.load(Ordering::SeqCst);
            if left > 0 && counter.fetch_sub(1, Ordering::SeqCst) == 1 {
                Err("snapshot append failure".into())
            } else {
                Ok(())
            }
        });
        let path = tmp.path().join("a");
        let db = Database::open(
            &path,
            [9; 32],
            Options {
                mode: OpenMode::CreateNew,
                database_id: Some([7; 16]),
                node_id: Some([1; 16]),
                capture_interval: std::time::Duration::from_secs(3600),
                fault_hooks: Some(Arc::new(spool::FaultHooks {
                    append: Some(hook),
                    ..Default::default()
                })),
                ..Options::default()
            },
        )
        .unwrap();
        db.migrate(Migration::new(
            "CREATE TABLE items(id INTEGER PRIMARY KEY,payload BLOB)",
        ))
        .unwrap();
    db.set_replication("items", true).unwrap();
        db.transaction(|s| {
            s.execute("INSERT INTO items VALUES(1,zeroblob(180000))", [])?;
            Ok(())
        })
        .unwrap();
        db.flush().unwrap();
        let c = config([1; 16]);
        let a = adapter(&db, &c);
        remaining.store(failure, Ordering::SeqCst);
        assert!(a.create_snapshot([2; 16], &c).is_err());
        assert!(a.keys(snap::OUT).unwrap().is_empty());
        assert!(a.keys(snap::PINS).unwrap().is_empty());
        remaining.store(0, Ordering::SeqCst);
        // Append failures are terminal for the current Spool handle; reopening
        // is the supported recovery path, even if close reports that failure.
        let _ = db.close();
        let db = open(&path, 1, OpenMode::Existing);
        let a = adapter(&db, &c);
        assert!(a.keys(snap::OUT).unwrap().is_empty());
        assert!(a.keys(snap::PINS).unwrap().is_empty());
        let m = a.create_snapshot([2; 16], &c).unwrap();
        assert!(m.chunks.len() > 2);
        db.close().unwrap();
    }
}
#[test]
fn snapshot_crash_child() {
    let Some(base) = std::env::var_os("MURMUR_SNAPSHOT_CRASH_PATH") else {
        return;
    };
    let boundary = std::env::var("MURMUR_SNAPSHOT_CRASH_BOUNDARY").unwrap();
    let base = std::path::PathBuf::from(base);
    let c = config([1; 16]);
    let a = open(&base.join("a"), 1, OpenMode::Existing);
    let b = open(&base.join("b"), 2, OpenMode::Existing);
    let aa = adapter(&a, &c);
    let bb = adapter(&b, &c);
    let m = aa.create_snapshot([2; 16], &c).unwrap();
    if boundary == "published" {
        std::process::exit(73);
    }
    stage(&aa, &bb, &c, &m, Some(1));
    if boundary == "partial" {
        std::process::exit(73);
    }
    stage(&aa, &bb, &c, &m, None);
    assert!(snap::finish(&bb, &c, [1; 16], m.transfer, net::retention::now_millis()).unwrap());
    if boundary == "applied" {
        std::process::exit(73);
    }
    assert_eq!(boundary, "acknowledged");
    aa.commit(vec![Record::put(
        progress_key([1; 16]),
        net::encode(&9u64).unwrap(),
    )])
    .unwrap();
    aa.acknowledge_snapshot([2; 16], m.transfer, m.digest)
        .unwrap();
    std::process::exit(73);
}
#[test]
#[ignore = "subprocess snapshot crash matrix; run with --ignored"]
fn process_crashes_during_snapshot_transfer_preserve_resume_and_completion() {
    for boundary in ["published", "partial", "applied", "acknowledged"] {
        let tmp = tempfile::tempdir().unwrap();
        let c = config([1; 16]);
        let a = create(&tmp.path().join("a"), 1);
        let b = open(&tmp.path().join("b"), 2, OpenMode::CreateNew);
        b.migrate(Migration::new(
            "CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER,payload BLOB)",
        ))
        .unwrap();
    b.set_replication("items", true).unwrap();
        b.flush().unwrap();
        let aa = adapter(&a, &c);
        aa.commit(vec![Record::put(
            progress_key([1; 16]),
            net::encode(&7u64).unwrap(),
        )])
        .unwrap();
        a.close().unwrap();
        b.close().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "network::snapshot_tests::snapshot_crash_child",
                "--nocapture",
            ])
            .env("MURMUR_SNAPSHOT_CRASH_PATH", tmp.path())
            .env("MURMUR_SNAPSHOT_CRASH_BOUNDARY", boundary)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(73), "{boundary}");
        let a = open(&tmp.path().join("a"), 1, OpenMode::Existing);
        let b = open(&tmp.path().join("b"), 2, OpenMode::Existing);
        let aa = adapter(&a, &c);
        let bb = adapter(&b, &c);
        let keys = aa.keys(snap::OUT).unwrap();
        let bytes = aa
            .get(keys.iter().find(|k| k.ends_with(b"/header")).unwrap())
            .unwrap()
            .unwrap();
        let m: snap::Manifest = net::decode(&bytes).unwrap();
        let pin: snap::Pin =
            net::decode(&aa.get(&snap::pin_key(m.transfer)).unwrap().unwrap()).unwrap();
        assert_eq!(pin.target.is_some(), boundary == "acknowledged");
        if boundary == "partial" {
            assert_eq!(
                snap::missing(&bb, &snap::incoming(&bb).unwrap().unwrap()).unwrap(),
                vec![1]
            );
        }
        if matches!(boundary, "applied" | "acknowledged") {
            assert!(bb.get(&snap::receipt_key(m.transfer)).unwrap().is_some());
            assert!(
                snap::finish(&bb, &c, [1; 16], m.transfer, net::retention::now_millis()).unwrap()
            );
        } else {
            stage(&aa, &bb, &c, &m, None);
            assert!(
                snap::finish(&bb, &c, [1; 16], m.transfer, net::retention::now_millis()).unwrap()
            );
        }
        assert_eq!(
            a.export_state().unwrap().rows,
            b.export_state().unwrap().rows
        );
        a.close().unwrap();
        b.close().unwrap();
    }
}

#[test]
fn progress_resolves_racing_file_publication_at_serialized_commit_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    let c = config([1; 16]);
    let db = create(&tmp.path().join("db"), 1);
    let a = adapter(&db, &c);
    a.commit(vec![Record::put(
        progress_key([1; 16]),
        net::encode(&7u64).unwrap(),
    )])
    .unwrap();
    let (payload, boundary) = a.snapshot_with_progress().unwrap();
    assert_eq!(
        boundary
            .iter()
            .find(|p| p.origin == [1; 16])
            .unwrap()
            .applied,
        7
    );
    // A previously prepared snapshot records 7; a file publication commits 9
    // without moving SQLite's cursor before that snapshot's apply event.
    a.commit(vec![Record::put(
        progress_key([1; 16]),
        net::encode(&9u64).unwrap(),
    )])
    .unwrap();
    assert!(
        a.apply(
            vec![payload],
            vec![Record::put(
                progress_key([1; 16]),
                net::encode(&7u64).unwrap()
            )]
        )
        .unwrap()
    );
    assert_eq!(
        net::decode::<u64>(&a.get(&progress_key([1; 16])).unwrap().unwrap()).unwrap(),
        9
    );
    db.close().unwrap();
    let db = open(&tmp.path().join("db"), 1, OpenMode::Existing);
    let a = adapter(&db, &c);
    assert_eq!(
        net::decode::<u64>(&a.get(&progress_key([1; 16])).unwrap().unwrap()).unwrap(),
        9
    );
    db.close().unwrap();
}

#[test]
fn incremental_merge_reads_only_affected_rows_and_matches_full_join() {
    let temp=tempfile::tempdir().unwrap();let db=create(&temp.path().join("db"),1);let cfg=config(db.node_id());let a=adapter(&db,&cfg);
    db.transaction(|s|{for id in 3..=10_000 {s.execute("INSERT INTO items VALUES(?,0,x'')",[id])?;}Ok(())}).unwrap();db.flush().unwrap();
    a.spool(|o| {
        let full=metadata::state(&o.store,o.schema.clone(),o.identity.database,o.clock)?;
        let mut remote=full.clone();remote.rows.retain(|row|row.cells.iter().any(|cell|full.schema.tables.iter().flat_map(|t|&t.columns).any(|c|c.id==cell.column&&c.name=="id")&&cell.value==Value::Integer(1)));remote.tombstones.clear();
        let version=Version{millis:full.clock.millis+1,logical:0,node:[2;16]};remote.clock=version;
        let row=&mut remote.rows[0];let column=o.schema.tables[0].columns.iter().find(|c|c.name=="n").unwrap();
        let cell=row.cells.iter_mut().find(|c|c.column==column.id).unwrap();cell.value=Value::Integer(99);cell.version=version;
        let keys=std::collections::BTreeSet::from([(row.table.clone(),encode(&row.key)?)]);let before=o.store.reads();
        let affected=metadata::affected_state(&o.store,o.schema.clone(),o.identity.database,o.clock,&keys)?;
        assert_eq!(o.store.reads()-before,2,"merge loaded unrelated metadata");
        let incremental=merge::prepare_delta(&affected,merge::join(&affected,&remote)?,o.cursor)?;
        let oracle=merge::prepare(&full,&remote,o.cursor)?;
        assert_eq!(incremental.rows,oracle.rows);assert_eq!(incremental.deleted,oracle.deleted);assert_eq!(incremental.tombstones,oracle.tombstones);assert!(incremental.state.is_none());Ok(())
    }).unwrap();db.close().unwrap();
}

#[test]
fn streamed_snapshot_resumes_incremental_installation_after_restart() {
    let temp=tempfile::tempdir().unwrap();let donor=create(&temp.path().join("a"),1);let receiver=create(&temp.path().join("b"),2);let cfg=config(donor.node_id());
    donor.transaction(|s|{for id in 3..=12 {s.execute("INSERT INTO items VALUES(?,100,zeroblob(700000))",[id])?;}Ok(())}).unwrap();donor.flush().unwrap();
    let a=adapter(&donor,&cfg);let b=adapter(&receiver,&cfg);let m=a.create_snapshot(receiver.node_id(),&cfg).unwrap();stage(&a,&b,&cfg,&m,None);
    assert!(!snap::finish(&b,&cfg,donor.node_id(),m.transfer,net::retention::now_millis()).unwrap());
    assert!(snap::incoming(&b).unwrap().is_some());
    let visible=receiver.read(|s|Ok(s.query_row("SELECT count(*) FROM items",[],|r|r.get::<_,i64>(0))?)).unwrap();assert!(visible>2 && visible<12);
    receiver.close().unwrap();drop(b);drop(receiver);
    let receiver=open(&temp.path().join("b"),2,OpenMode::Existing);let b=adapter(&receiver,&cfg);
    let mut done=false;for _ in 0..32 {if snap::finish(&b,&cfg,donor.node_id(),m.transfer,net::retention::now_millis()).unwrap(){done=true;break;}}
    assert!(done);assert_eq!(receiver.read(|s|Ok(s.query_row("SELECT count(*) FROM items",[],|r|r.get::<_,i64>(0))?)).unwrap(),12);
    assert!(b.get(&snap::receipt_key(m.transfer)).unwrap().is_some());donor.close().unwrap();receiver.close().unwrap();
}

#[test]
fn oversized_local_signed_content_is_rejected_before_durable_publication() {
    let temp=tempfile::tempdir().unwrap();let db=create(&temp.path().join("db"),1);let mut cfg=config(db.node_id());cfg.max_transaction_bytes=16;let a=adapter(&db,&cfg);
    a.spool(move|o|{let state=metadata::state(&o.store,o.schema.clone(),o.identity.database,o.clock)?;assert!(matches!(sign_local(o,&cfg,state,Vec::new()),Err(Error::Limit(_))));Ok(())}).unwrap();db.close().unwrap();
}
