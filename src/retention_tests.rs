//! Real serialized-worker retention tests; no transport or wall-clock timers.
use super::*;
use net::protocol::{Progress, log_key, progress_key, retained_key};

fn adapter(db: &Database) -> Adapter {
    let adapter = Adapter {last_reclaim_ms:std::sync::atomic::AtomicU64::new(0),
        last_apply_barrier_ms:std::sync::atomic::AtomicU64::new(0),
        progress_cache:std::sync::Mutex::new(None),
        merge_deferrals:std::sync::atomic::AtomicUsize::new(0),
        cancelled:Arc::new(std::sync::atomic::AtomicBool::new(false)),
        inner: Arc::downgrade(&db.inner),
        worker: db.inner.worker.clone(),
        identity: (db.database_id(), db.node_id()),
    };
    adapter
        .spool(|o| {
            let mut cfg = net::Config::new(
                "127.0.0.1:0".parse().unwrap(),
                net::Credentials {
                    chain: vec![],
                    private_key: vec![].into(),
                    roots: vec![],
                },
                [1; 32],
            );
            cfg.minimum_retained = 2;
            o.replication_config = Some(cfg);
            Ok(())
        })
        .unwrap();
    adapter
}
fn seed_history(a: &Adapter, count: u64) {
    let origin = [3; 16];
    let mut records = vec![
        Record::put(
            retained_key(origin),
            net::encode(&Progress {
                origin,
                observed: count,
                applied: count,
                retained_from: 1,
                retained_through: count,
            })
            .unwrap(),
        ),
        Record::put(progress_key(origin), net::encode(&count).unwrap()),
    ];
    for seq in 1..=count {
        records.push(Record::put(log_key(origin, seq), vec![1]));
        records.push(Record::put(
            net::retention::time_key(origin, seq),
            net::encode(&0u64).unwrap(),
        ));
    }
    a.commit(records).unwrap();
}
fn create(path: &std::path::Path) -> Database {
    let db = Database::open(
        path,
        [7; 32],
        Options {
            mode: OpenMode::CreateNew,
            ..Options::default()
        },
    )
    .unwrap();
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER)",
    ))
    .unwrap();
    db.set_replication("items", true).unwrap();
    db.transaction(|s| {
        s.execute("INSERT INTO items VALUES(1,1)", [])?;
        Ok(())
    })
    .unwrap();
    db.flush().unwrap();
    db.transaction(|s| {
        s.execute("DELETE FROM items WHERE id=1", [])?;
        s.execute("INSERT INTO items VALUES(2,2)", [])?;
        Ok(())
    })
    .unwrap();
    db.flush().unwrap();
    db
}

#[test]
fn pruning_requires_durable_snapshot_coverage_and_preserves_newer_retained_head() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("db");
    let db = create(&path);
    let a = adapter(&db);
    seed_history(&a, 4);
    assert!(a.prune_history([3; 16], 2).is_err());
    assert_eq!(a.keys(b"07/replication/log/").unwrap().len(), 4);
    a.retention_checkpoint().unwrap();
    seed_history(&a, 6);
    assert_eq!(a.prune_history([3; 16], 2).unwrap(), 2);
    let head: Progress = net::decode(&a.get(&retained_key([3; 16])).unwrap().unwrap()).unwrap();
    assert_eq!(
        (head.retained_from, head.retained_through, head.applied),
        (3, 6, 6)
    );
    assert!(a.prune_history([3; 16], 5).is_err()); // Beyond captured coverage.
    assert_eq!(a.prune_history([3; 16], 2).unwrap(), 0);
    let expected = db.export_state().unwrap();
    db.close().unwrap();
    let recovered = Database::open(&path, [7; 32], Options::default()).unwrap();
    let a = adapter(&recovered);
    assert_eq!(recovered.export_state().unwrap(), expected);
    assert_eq!(
        net::decode::<u64>(&a.get(&net::retention::floor_key([3; 16])).unwrap().unwrap()).unwrap(),
        2
    );
    assert_eq!(a.keys(b"07/replication/log/").unwrap().len(), 4);
    recovered.close().unwrap();
}

#[test]
fn retention_crash_child() {
    let Some(path) = std::env::var_os("MURMUR_RETENTION_CRASH_PATH") else {
        return;
    };
    let boundary = std::env::var("MURMUR_RETENTION_CRASH_BOUNDARY").unwrap();
    let db = Database::open(std::path::PathBuf::from(path), [7; 32], Options::default()).unwrap();
    let a = adapter(&db);
    if boundary == "snapshot" {
        a.spool(crate::storage::snapshot).unwrap();
        std::process::exit(73);
    }
    a.retention_checkpoint().unwrap();
    if boundary == "coverage" {
        std::process::exit(73);
    }
    a.prune_history([3; 16], 4).unwrap();
    assert_eq!(boundary, "prune");
    std::process::exit(73);
}

#[test]
#[ignore = "subprocess crash matrix; run with --ignored"]
fn process_crashes_between_retention_snapshot_coverage_and_atomic_pruning() {
    for boundary in ["snapshot", "coverage", "prune"] {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("db");
        let db = create(&path);
        let a = adapter(&db);
        seed_history(&a, 6);
        let expected = db.export_state().unwrap();
        db.close().unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "network::retention_tests::retention_crash_child",
                "--nocapture",
            ])
            .env("MURMUR_RETENTION_CRASH_PATH", &path)
            .env("MURMUR_RETENTION_CRASH_BOUNDARY", boundary.to_string())
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(73),
            "boundary {boundary}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let recovered = Database::open(&path, [7; 32], Options::default()).unwrap();
        let a = adapter(&recovered);
        assert_eq!(recovered.export_state().unwrap(), expected);
        let floor = a.get(&net::retention::floor_key([3; 16])).unwrap();
        if boundary != "prune" {
            assert!(floor.is_none());
            assert_eq!(a.keys(b"07/replication/log/").unwrap().len(), 6);
        } else {
            assert_eq!(net::decode::<u64>(&floor.unwrap()).unwrap(), 4);
            assert_eq!(a.keys(b"07/replication/log/").unwrap().len(), 2);
        }
        if boundary == "coverage" {
            assert!(a.get(net::retention::COVERAGE_KEY).unwrap().is_some());
        }
        recovered.close().unwrap();
    }
}
