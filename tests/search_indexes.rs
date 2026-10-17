use sqlite_session::*;
use std::time::{Duration, Instant};

fn definitions() -> Vec<SearchIndex> {
    vec![Fts5Index::new("docs_fts", "docs", vec!["title".into(), "body".into()]).into()]
}
fn options() -> Options {
    Options {
        mode: OpenMode::CreateNew,
        database_id: Some([81; 16]),
        search_indexes: definitions(),
        capture_interval: Duration::from_secs(60),
        search_interval: Duration::from_secs(60),
        search_batch_rows: 2,
        ..Options::default()
    }
}
fn create(path: &std::path::Path, mut o: Options) -> Result<Database> {
    o.mode = OpenMode::CreateNew;
    let db = Database::open(path, [9; 32], o)?;
    db.migrate(Migration::new(
        "CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT NOT NULL,body TEXT,embedding BLOB)",
    ))?;
    db.set_replication("docs", true)?;
    Ok(db)
}
fn matches(db: &Database, term: &str) -> Result<Vec<i64>> {
    let term = term.to_owned();
    db.read(move |sql| {
        let mut q =
            sql.prepare("SELECT rowid FROM docs_fts WHERE docs_fts MATCH ? ORDER BY rowid")?;
        Ok(q.query_map([term], |r| r.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    })
}

#[test]
fn fts_is_eventual_read_only_and_source_only_in_exports() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = create(&tmp.path().join("db"), options())?;
    db.transaction(|sql| {
        sql.execute("INSERT INTO docs VALUES(1,'hello','world',NULL)", [])?;
        sql.execute("INSERT INTO docs VALUES(2,'hello','second',NULL)", [])?;
        Ok(())
    })?;
    assert!(matches(&db, "hello")?.is_empty());
    assert!(
        db.transaction(|sql| {
            sql.execute(
                "INSERT INTO docs_fts(rowid,title) VALUES(3,'forbidden')",
                [],
            )?;
            Ok(())
        })
        .is_err()
    );
    assert!(
        db.transaction(|sql| {
            sql.execute("DELETE FROM temp.docs_fts_data", [])?;
            Ok(())
        })
        .is_err()
    );
    assert!(
        db.migrate(Migration::new("DROP TABLE temp.docs_fts"))
            .is_err()
    );
    assert!(
        db.migrate(Migration::new(
            "CREATE INDEX temp.evil ON docs_fts_content(c0)"
        ))
        .is_err()
    );
    db.refresh_indexes()?;
    assert_eq!(matches(&db, "hello")?, vec![1, 2]);
    let state = db.export_state()?;
    assert_eq!(state.schema.tables.len(), 1);
    assert_eq!(state.rows.len(), 2);
    assert!(state.schema.tables.iter().all(|t| t.name == "docs"));
    assert_eq!(
        db.read(|sql| Ok(sql.query_row(
            "SELECT count(*) FROM main.sqlite_schema WHERE name LIKE 'docs_fts%'",
            [],
            |r| r.get::<_, i64>(0)
        )?))?,
        0
    );
    assert!(
        db.transaction(|sql| {
            sql.execute("UPDATE docs SET title='rollback' WHERE id=1", [])?;
            Err::<(), _>(Error::Invalid("rollback".into()))
        })
        .is_err()
    );
    db.transaction(|sql| {
        sql.execute("UPDATE docs SET title='intermediate' WHERE id=1", [])?;
        sql.execute("UPDATE docs SET title='updated' WHERE id=1", [])?;
        sql.execute("DELETE FROM docs WHERE id=2", [])?;
        sql.execute("INSERT INTO docs VALUES(3,'vanishing',NULL,NULL)", [])?;
        sql.execute("DELETE FROM docs WHERE id=3", [])?;
        Ok(())
    })?;
    db.refresh_indexes()?;
    assert!(matches(&db, "hello")?.is_empty());
    assert_eq!(matches(&db, "updated")?, vec![1]);
    assert!(matches(&db, "rollback OR vanishing")?.is_empty());
    assert!(db.status().search_indexes[0].ready);
    db.close()
}

#[test]
fn indexes_rebuild_on_recovery_restore_and_reconfiguration() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = create(&path, options())?;
    db.transaction(|sql| {
        sql.execute(
            "INSERT INTO docs VALUES(-5,'durable','searchable',NULL)",
            [],
        )?;
        Ok(())
    })?;
    let expected = db.export_state()?;
    let backup = tmp.path().join("backup");
    db.backup(&backup)?;
    db.close()?;
    let db = Database::open(
        &path,
        [9; 32],
        Options {
            mode: OpenMode::Existing,
            ..options()
        },
    )?;
    assert_eq!(matches(&db, "durable")?, vec![-5]);
    assert_eq!(db.export_state()?, expected);
    assert!(
        db.set_search_indexes(vec![
            Fts5Index::new("bad", "docs", vec!["absent".into()]).into()
        ])
        .is_err()
    );
    assert_eq!(matches(&db, "durable")?, vec![-5]);
    db.set_search_indexes(vec![])?;
    // Renames require a local-only table.
    db.set_replication("docs", false)?;
    db.migrate(Migration::new(
        "ALTER TABLE docs RENAME COLUMN title TO heading",
    ))?;
    db.set_replication("docs", true)?;
    db.set_search_indexes(vec![
        Fts5Index::new("docs_fts", "docs", vec!["heading".into()]).into(),
    ])?;
    assert_eq!(matches(&db, "durable")?, vec![-5]);
    db.close()?;
    let restored = tmp.path().join("restored");
    Database::restore(&backup, &restored, [9; 32])?;
    let db = Database::open(
        &restored,
        [9; 32],
        Options {
            mode: OpenMode::Existing,
            ..options()
        },
    )?;
    assert_eq!(matches(&db, "searchable")?, vec![-5]);
    db.close()
}

#[test]
fn migrations_validate_sources_and_rollback_indexes() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = create(&tmp.path().join("db"), options())?;
    db.transaction(|sql| {
        sql.execute("INSERT INTO docs VALUES(1,'original',NULL,NULL)", [])?;
        Ok(())
    })?;
    db.refresh_indexes()?;
    for migration in [
        "ALTER TABLE docs DROP COLUMN title",
        "DROP TABLE docs",
        "CREATE TABLE docs_fts(id INTEGER PRIMARY KEY)",
    ] {
        assert!(db.migrate(Migration::new(migration)).is_err());
        assert_eq!(matches(&db, "original")?, vec![1]);
    }
    db.migrate(Migration::new(
        "UPDATE docs SET title='migrated'; ALTER TABLE docs ADD COLUMN extra TEXT",
    ))?;
    assert_eq!(matches(&db, "migrated")?, vec![1]);
    assert!(matches(&db, "original")?.is_empty());
    assert!(
        db.migrate(Migration::new(
            "CREATE VIRTUAL TABLE unmanaged USING fts5(text)"
        ))
        .is_err()
    );
    db.close()
}

#[test]
fn bounded_queue_rebuilds_and_background_maintenance_publishes() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = create(
        &tmp.path().join("db"),
        Options {
            search_queue_bytes: 32,
            capture_interval: Duration::from_millis(10),
            search_interval: Duration::from_millis(10),
            ..options()
        },
    )?;
    db.transaction(|sql| {
        for id in -3..12 {
            sql.execute(
                "INSERT INTO docs VALUES(?,'overflow','background',NULL)",
                [id],
            )?;
        }
        Ok(())
    })?;
    db.flush()?;
    assert!(db.status().search_indexes[0].queued_bytes <= 32);
    let deadline = Instant::now() + Duration::from_secs(5);
    while matches(&db, "background").unwrap_or_default().len() != 15 {
        assert!(
            Instant::now() < deadline,
            "{:?}",
            db.status().search_indexes
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    db.transaction(|sql| {
        sql.execute("DELETE FROM docs WHERE id=-3", [])?;
        Ok(())
    })?;
    db.refresh_indexes()?;
    assert_eq!(matches(&db, "background")?.len(), 14);
    db.close()
}

#[test]
fn offline_conflicts_and_local_replication_settings_use_resolved_rows() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let a = create(
        &tmp.path().join("a"),
        Options {
            node_id: Some([1; 16]),
            ..options()
        },
    )?;
    let b = create(
        &tmp.path().join("b"),
        Options {
            node_id: Some([2; 16]),
            ..options()
        },
    )?;
    a.transaction(|sql| {
        sql.execute("INSERT INTO docs VALUES(1,'initial','original',NULL)", [])?;
        Ok(())
    })?;
    b.merge_state(a.export_state()?)?;
    a.transaction(|sql| {
        sql.execute("UPDATE docs SET title='froma'", [])?;
        Ok(())
    })?;
    b.transaction(|sql| {
        sql.execute("UPDATE docs SET body='fromb'", [])?;
        Ok(())
    })?;
    let ax = a.export_state()?;
    let bx = b.export_state()?;
    a.merge_state(bx.clone())?;
    b.merge_state(ax)?;
    a.merge_state(bx)?;
    for db in [&a, &b] {
        db.refresh_indexes()?;
        assert_eq!(matches(db, "froma AND fromb")?, vec![1]);
    }
    b.set_replication("docs", false)?;
    a.transaction(|sql| {
        sql.execute("DELETE FROM docs", [])?;
        Ok(())
    })?;
    b.merge_state(a.export_state()?)?;
    b.refresh_indexes()?;
    assert_eq!(matches(&b, "froma")?, vec![1]);
    assert!(b.export_state()?.rows.is_empty());
    b.set_replication("docs", true)?;
    b.merge_state(a.export_state()?)?;
    b.refresh_indexes()?;
    assert_eq!(matches(&b, "froma")?, vec![1]);
    a.close()?;
    b.close()
}

#[test]
fn search_subscription_observes_publication_after_source_notification() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = create(&tmp.path().join("db"), options())?;
    let mut subscription = db.subscribe(
        |sql| {
            let mut q = sql.prepare(
                "SELECT rowid FROM docs_fts WHERE docs_fts MATCH 'published' ORDER BY rowid",
            )?;
            q.query_map([], |r| r.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()
        },
        |id| id.to_le_bytes().to_vec(),
        SubscriptionOptions {
            emit_unchanged: true,
            ..SubscriptionOptions::default()
        },
    )?;
    assert!(
        matches!(subscription.recv_timeout(Duration::from_secs(2))?, SubscriptionEvent::Initial { rows, .. } if rows.is_empty())
    );
    db.transaction(|sql| {
        sql.execute("INSERT INTO docs VALUES(1,'published',NULL,NULL)", [])?;
        Ok(())
    })?;
    db.flush()?;
    let source = subscription.recv_timeout(Duration::from_secs(2))?;
    assert!(matches!(source, SubscriptionEvent::Update { ref rows, .. } if rows.is_empty()));
    db.refresh_indexes()?;
    assert!(
        matches!(subscription.recv_timeout(Duration::from_secs(2))?, SubscriptionEvent::Update { rows, .. } if rows == vec![1])
    );
    subscription.close();
    db.close()
}

#[test]
fn invalid_keys_and_configuration_are_rejected_without_schema_changes() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = Database::open(
        tmp.path().join("db"),
        [9; 32],
        Options {
            search_indexes: vec![],
            ..options()
        },
    )?;
    db.migrate(Migration::new("CREATE TABLE text_keys(id TEXT PRIMARY KEY NOT NULL,title TEXT); CREATE TABLE composite(a INTEGER NOT NULL,b INTEGER NOT NULL,title TEXT,PRIMARY KEY(a,b))"))?;
    for table in ["text_keys", "composite"] {
        assert!(
            db.set_search_indexes(vec![
                Fts5Index::new("bad", table, vec!["title".into()]).into()
            ])
            .is_err()
        );
    }
    assert!(db.status().search_indexes.is_empty());
    assert_eq!(db.schema()?.tables.len(), 2);
    db.migrate(Migration::new(
        "CREATE TABLE docs_fts_data(id INTEGER PRIMARY KEY,title TEXT)",
    ))?;
    assert!(
        db.set_search_indexes(vec![
            Fts5Index::new("docs_fts", "docs_fts_data", vec!["title".into()]).into()
        ])
        .is_err()
    );
    db.transaction(|sql| {
        sql.execute("INSERT INTO docs_fts_data VALUES(1,'source')", [])?;
        Ok(())
    })?;
    db.close()
}

#[test]
#[ignore = "manual application-write throughput comparison; run with --ignored --nocapture"]
fn application_write_cost_with_indexes() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut plain = Vec::new();
    let mut indexed = Vec::new();
    for round in 0..3 {
        for enabled in [false, true] {
            let mut o = options();
            if !enabled {
                o.search_indexes.clear();
            }
            let db = create(&tmp.path().join(format!("bench-{round}-{enabled}")), o)?;
            let start = Instant::now();
            for transaction in 0..200 {
                db.transaction(move |sql| {
                    let mut insert = sql.prepare(
                        "INSERT INTO docs VALUES(?,'benchmark title','benchmark body',NULL)",
                    )?;
                    for offset in 0..64 {
                        insert.execute([transaction * 64 + offset])?;
                    }
                    Ok(())
                })?;
            }
            let elapsed = start.elapsed().as_secs_f64();
            if enabled {
                indexed.push(elapsed);
            } else {
                plain.push(elapsed);
            }
            // Maintenance has not run, and no search SQL was added to commits.
            if enabled {
                assert!(matches(&db, "benchmark")?.is_empty());
            }
            db.close()?;
        }
    }
    plain.sort_by(f64::total_cmp);
    indexed.sort_by(f64::total_cmp);
    println!(
        "12,800 source writes, median of 3: plain={:.3}s indexed={:.3}s ratio={:.3}",
        plain[1],
        indexed[1],
        indexed[1] / plain[1]
    );
    Ok(())
}

#[cfg(feature = "vector")]
fn vector_options() -> Options {
    let mut o = options();
    o.search_indexes
        .push(VectorIndex::new("docs_vec", "docs", "embedding", 2).into());
    o
}
#[cfg(feature = "vector")]
fn nearest(db: &Database) -> Result<Vec<i64>> {
    db.read(|sql| {
        let mut q = sql.prepare(
            "SELECT rowid FROM docs_vec WHERE embedding MATCH '[1,0]' AND k=2 ORDER BY distance",
        )?;
        Ok(q.query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    })
}
#[cfg(feature = "vector")]
#[test]
fn vector_knn_recovery_nulls_and_invalid_embedding_retry() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = create(&path, vector_options())?;
    db.transaction(|sql| {
        let one = [1f32, 0.]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<_>>();
        let two = [0f32, 1.]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<_>>();
        sql.execute("INSERT INTO docs VALUES(1,'one',NULL,?)", [one])?;
        sql.execute("INSERT INTO docs VALUES(2,'two',NULL,?)", [two])?;
        sql.execute("INSERT INTO docs VALUES(3,'null',NULL,NULL)", [])?;
        Ok(())
    })?;
    db.refresh_indexes()?;
    assert_eq!(nearest(&db)?, vec![1, 2]);
    db.set_search_indexes(vector_options().search_indexes)?;
    assert_eq!(nearest(&db)?, vec![1, 2]);
    assert!(
        db.transaction(|sql| {
            sql.execute("DELETE FROM docs_vec", [])?;
            Ok(())
        })
        .is_err()
    );
    for (metric, expected) in [
        (DistanceMetric::L2, 2f64.sqrt()),
        (DistanceMetric::L1, 2.),
        (DistanceMetric::Cosine, 1.),
    ] {
        let mut definitions = vector_options().search_indexes;
        if let SearchIndex::Vector(index) = &mut definitions[1] {
            index.distance = metric;
        }
        db.set_search_indexes(definitions)?;
        let distance = db.read(|sql| {
            let mut query = sql.prepare("SELECT distance FROM docs_vec WHERE embedding MATCH '[1,0]' AND k=2 ORDER BY distance")?;
            let distances = query.query_map([], |r| r.get::<_, f64>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(distances[1])
        })?;
        assert!(
            (distance - expected).abs() < 0.00001,
            "{metric:?}: {distance}"
        );
    }
    db.set_search_indexes(vector_options().search_indexes)?;
    db.transaction(|sql| {
        sql.execute("UPDATE docs SET embedding=x'00' WHERE id=1", [])?;
        Ok(())
    })?;
    assert!(db.refresh_indexes().is_err());
    assert!(nearest(&db).is_err());
    assert!(
        db.status()
            .search_indexes
            .iter()
            .find(|i| i.name == "docs_vec")
            .unwrap()
            .error
            .is_some()
    );
    assert_eq!(matches(&db, "one")?, vec![1]);
    db.transaction(|sql| {
        sql.execute(
            "UPDATE docs SET embedding=? WHERE id=1",
            [[2f32, 0.]
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>()],
        )?;
        Ok(())
    })?;
    db.refresh_indexes()?;
    assert_eq!(nearest(&db)?, vec![1, 2]);
    assert!(
        db.migrate(Migration::new("UPDATE docs SET embedding=x'00' WHERE id=1"))
            .is_err()
    );
    assert_eq!(nearest(&db)?, vec![1, 2]);
    db.close()?;
    let db = Database::open(
        &path,
        [9; 32],
        Options {
            mode: OpenMode::Existing,
            ..vector_options()
        },
    )?;
    assert_eq!(nearest(&db)?, vec![1, 2]);
    db.transaction(|sql| {
        sql.execute("DELETE FROM docs WHERE id=1", [])?;
        Ok(())
    })?;
    db.refresh_indexes()?;
    assert_eq!(nearest(&db)?, vec![2]);
    db.close()
}

#[cfg(not(feature = "vector"))]
#[test]
fn vector_configuration_requires_feature() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let o = Options {
        search_indexes: vec![VectorIndex::new("v", "docs", "embedding", 2).into()],
        ..options()
    };
    assert!(matches!(
        Database::open(tmp.path().join("db"), [9; 32], o),
        Err(Error::Invalid(_))
    ));
    Ok(())
}
