use sqlite_session::*;
fn create(path: &std::path::Path, node: u8) -> Result<Database> {
    let db = Database::open(
        path,
        [7; 32],
        Options {
            mode: OpenMode::CreateNew,
            database_id: Some([1; 16]),
            node_id: Some([node; 16]),
            ..Options::default()
        },
    )?;
    db.migrate(Migration::new("CREATE TABLE devices(id TEXT PRIMARY KEY NOT NULL, hostname TEXT NOT NULL, status TEXT NOT NULL)"))?;
    db.set_replication("devices", true)?;
    Ok(db)
}
#[test]
fn cells_deletions_reinsert_and_duplicate_delivery() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let a = create(&tmp.path().join("a"), 1)?;
    let b = create(&tmp.path().join("b"), 2)?;
    a.transaction(|c| {
        c.execute("INSERT INTO devices VALUES('one','initial','offline')", [])?;
        Ok(())
    })?;
    b.merge_state(a.export_state()?)?;
    a.transaction(|c| {
        c.execute("UPDATE devices SET hostname='router' WHERE id='one'", [])?;
        Ok(())
    })?;
    b.transaction(|c| {
        c.execute("UPDATE devices SET status='online' WHERE id='one'", [])?;
        Ok(())
    })?;
    let ax = a.export_state()?;
    let bx = b.export_state()?;
    a.merge_state(bx.clone())?;
    b.merge_state(ax.clone())?;
    a.merge_state(bx)?;
    b.merge_state(ax)?;
    assert_eq!(a.export_state()?.rows, b.export_state()?.rows);
    let baseline = a.export_state()?;
    a.transaction(|c| {
        c.execute("DELETE FROM devices WHERE id='one'", [])?;
        Ok(())
    })?;
    b.transaction(|c| {
        c.execute(
            "UPDATE devices SET status='newer-update' WHERE id='one'",
            [],
        )?;
        Ok(())
    })?;
    let deleted = a.export_state()?;
    let updated = b.export_state()?;
    a.merge_state(updated)?;
    b.merge_state(deleted)?;
    assert!(a.export_state()?.rows.is_empty());
    assert!(b.export_state()?.rows.is_empty());
    b.transaction(|c| {
        c.execute("INSERT INTO devices VALUES('one','reborn','online')", [])?;
        Ok(())
    })?;
    a.merge_state(b.export_state()?)?;
    a.merge_state(baseline)?;
    let aa = a.export_state()?;
    let bb = b.export_state()?;
    assert_eq!(aa.rows, bb.rows);
    assert_eq!(aa.tombstones, bb.tombstones);
    a.close()?;
    b.close()
}
#[test]
fn schema_and_identity_mismatch_rejected() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let a = create(&tmp.path().join("a"), 1)?;
    let b = create(&tmp.path().join("b"), 2)?;
    let mut state = b.export_state()?;
    state.database_id = [9; 16];
    assert!(matches!(a.merge_state(state), Err(Error::SchemaMismatch)));
    b.migrate(Migration::new("ALTER TABLE devices ADD COLUMN extra TEXT"))?;
    assert!(matches!(
        a.merge_state(b.export_state()?),
        Err(Error::SchemaMismatch)
    ));
    a.close()?;
    b.close()
}
#[test]
fn three_replicas_converge_under_permutations() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut replicas = Vec::new();
    for n in 0..3 {
        replicas.push(create(&tmp.path().join(format!("n{n}")), n + 1)?);
    }
    replicas[0].transaction(|c| {
        for i in 0..8 {
            c.execute(
                "INSERT INTO devices VALUES(?, 'initial', 'offline')",
                [format!("d{i}")],
            )?;
        }
        Ok(())
    })?;
    let baseline = replicas[0].export_state()?;
    for db in &replicas[1..] {
        db.merge_state(baseline.clone())?;
    }
    for (n, db) in replicas.iter().enumerate() {
        db.transaction(move |c| {
            for i in 0..8 {
                if (i + n) % 4 == 0 {
                    c.execute("DELETE FROM devices WHERE id=?", [format!("d{i}")])?;
                } else if i % 2 == 0 {
                    c.execute(
                        "UPDATE devices SET hostname=? WHERE id=?",
                        rusqlite::params![format!("n{n}"), format!("d{i}")],
                    )?;
                } else {
                    c.execute(
                        "UPDATE devices SET status=? WHERE id=?",
                        rusqlite::params![format!("s{n}"), format!("d{i}")],
                    )?;
                }
            }
            c.execute(
                "INSERT INTO devices VALUES('concurrent', ?, 'new')",
                [format!("n{n}")],
            )?;
            Ok(())
        })?;
    }
    let states = replicas
        .iter()
        .map(Database::export_state)
        .collect::<Result<Vec<_>>>()?;
    for (n, db) in replicas.iter().enumerate() {
        for index in [n, (n + 2) % 3, (n + 1) % 3, n, (n + 1) % 3] {
            db.merge_state(states[index].clone())?;
        }
    }
    let result = replicas[0].export_state()?;
    for db in &replicas {
        assert_eq!(result, db.export_state()?);
        db.close()?;
    }
    Ok(())
}
#[test]
fn altered_payload_with_same_version_is_rejected() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = create(&tmp.path().join("a"), 1)?;
    db.transaction(|c| {
        c.execute("INSERT INTO devices VALUES('one','original','online')", [])?;
        Ok(())
    })?;
    let baseline = db.export_state()?;
    let mut invalid = baseline.clone();
    invalid.rows[0].cells[1].value = Value::Text("tampered".into());
    assert!(matches!(db.merge_state(invalid), Err(Error::Corrupt(_))));
    assert_eq!(db.export_state()?, baseline);
    db.close()
}

#[test]
fn seeded_partitioned_edits_and_stale_replays_match_independent_model_after_restart() -> Result<()>
{
    use rand::{Rng, SeedableRng};
    use std::collections::BTreeMap;
    for seed in [1, 42, 999] {
        let tmp = tempfile::tempdir()?;
        let mut replicas = (0..3)
            .map(|n| create(&tmp.path().join(format!("n{n}")), n + 1))
            .collect::<Result<Vec<_>>>()?;
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut expected = BTreeMap::new();
        let mut deliveries = Vec::new();
        // Nodes own disjoint key ranges, giving an independent SQL oracle while
        // exercising deletion generations, reinsertion, and stale delivery.
        for (node, db) in replicas.iter().enumerate() {
            for step in 0..60 {
                let key = format!("{node}-{}", rng.gen_range(0..12));
                if rng.gen_bool(0.3) {
                    let key_for_sql = key.clone();
                    db.transaction(move |sql| {
                        sql.execute("DELETE FROM devices WHERE id=?", [key_for_sql])?;
                        Ok(())
                    })?;
                    expected.remove(&key);
                } else {
                    let host = format!("host-{seed}-{node}-{step}");
                    let status = format!("status-{}", rng.gen_range(0..4));
                    let args = (key.clone(), host.clone(), status.clone());
                    db.transaction(move|sql|{sql.execute("INSERT INTO devices VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET hostname=excluded.hostname,status=excluded.status",rusqlite::params![args.0,args.1,args.2])?;Ok(())})?;
                    expected.insert(key, (host, status));
                }
                if step % 15 == 14 {
                    deliveries.push(db.export_state()?);
                }
            }
        }
        for db in &replicas {
            for _ in 0..30 {
                db.merge_state(deliveries[rng.gen_range(0..deliveries.len())].clone())?;
            }
            for state in deliveries.iter().rev() {
                db.merge_state(state.clone())?;
            }
        }
        let state = replicas[0].export_state()?;
        for db in &replicas {
            let sql = db.read(|sql| {
                let mut st = sql.prepare("SELECT id,hostname,status FROM devices ORDER BY id")?;
                let rows = st.query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        (r.get::<_, String>(1)?, r.get::<_, String>(2)?),
                    ))
                })?;
                Ok(rows.collect::<rusqlite::Result<BTreeMap<_, _>>>()?)
            })?;
            assert_eq!(sql, expected, "seed {seed}");
            assert_eq!(db.export_state()?, state);
            db.close()?;
        }
        replicas.clear();
        for n in 0..3 {
            let db = Database::open(
                tmp.path().join(format!("n{n}")),
                [7; 32],
                Options::default(),
            )?;
            assert_eq!(db.export_state()?, state, "recovered seed {seed}");
            db.close()?;
        }
    }
    Ok(())
}
