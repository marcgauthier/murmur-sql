use sqlite_session::*;
fn config(dimensions: usize, distance: DistanceMetric) -> Option<VectorColumnConfig> {
    Some(VectorColumnConfig {
        dimensions,
        distance,
    })
}
#[cfg(feature = "vector")]
fn name(db: &Database) -> String {
    let schema = db.schema().unwrap();
    let t = &schema.tables[0];
    let c = t.columns.iter().find(|c| c.name == "embedding").unwrap();
    format!("murmur_vec_{}_{}", t.id, c.id)
}
#[cfg(feature = "vector")]
fn nearest(db: &Database, index: &str) -> Result<Vec<(i64, f64)>> {
    let sql = format!(
        "SELECT rowid,distance FROM temp.\"{index}\" WHERE embedding MATCH '[1,0]' AND k=10 ORDER BY distance"
    );
    db.read(move |c| {
        let mut s = c.prepare(&sql)?;
        Ok(s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    })
}
#[cfg(feature = "vector")]
#[test]
fn vector_declarations_rebuild_persist_and_validate_atomically() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let db = Database::open(
        &path,
        [9; 32],
        Options {
            mode: OpenMode::CreateNew,
            ..Options::default()
        },
    )?;
    db.migrate(Migration::new(
        "CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT,embedding BLOB,other BLOB)",
    ))?;
    db.transaction(|c| {c.execute("INSERT INTO docs VALUES(1,'hello',x'0000803f00000000',NULL),(2,'world',x'000000000000803f',NULL),(3,'none',NULL,NULL)",[])?;Ok(())})?;
    db.set_column_fts("docs", "title", true)?;
    db.set_column_vector("DOCS", "EMBEDDING", config(2, DistanceMetric::L2))?;
    let index = name(&db);
    assert_eq!(
        nearest(&db, &index)?
            .iter()
            .map(|x| x.0)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let history = db.schema()?.history;
    db.set_column_vector("docs", "embedding", config(2, DistanceMetric::L2))?;
    assert_eq!(history, db.schema()?.history);
    for (metric, want) in [
        (DistanceMetric::L2, 2f64.sqrt()),
        (DistanceMetric::Cosine, 1.),
        (DistanceMetric::L1, 2.),
    ] {
        db.set_column_vector("docs", "embedding", config(2, metric))?;
        assert!((nearest(&db, &index)?[1].1 - want).abs() < 1e-5);
    }
    assert!(
        db.set_column_vector("docs", "embedding", config(3, DistanceMetric::L2))
            .is_err()
    );
    assert_eq!(nearest(&db, &index)?.len(), 2);
    assert_eq!(
        db.schema()?.tables[0]
            .columns
            .iter()
            .find(|c| c.name == "embedding")
            .unwrap()
            .vector,
        config(2, DistanceMetric::L1)
    );
    for (col, dims) in [
        ("title", 2),
        ("id", 2),
        ("embedding", 0),
        ("embedding", 8193),
        ("missing", 2),
    ] {
        assert!(
            db.set_column_vector("docs", col, config(dims, DistanceMetric::L2))
                .is_err()
        );
    }
    db.set_column_vector("docs", "other", config(2, DistanceMetric::L2))?;
    db.set_search_indexes(vec![])?;
    assert_eq!(db.status().search_indexes.len(), 3);
    assert!(
        db.set_search_indexes(vec![
            VectorIndex::new(&index, "docs", "embedding", 2).into()
        ])
        .is_err()
    );
    db.set_replication("docs", true)?;
    db.transaction(|c| {
        c.execute("UPDATE docs SET embedding=x'00' WHERE id=1", [])?;
        Ok(())
    })?;
    assert!(db.refresh_indexes().is_err());
    assert!(nearest(&db, &index).is_err());
    db.transaction(|c| {
        c.execute(
            "UPDATE docs SET embedding=x'0000803f00000000' WHERE id=1",
            [],
        )?;
        c.execute("DELETE FROM docs WHERE id=2", [])?;
        Ok(())
    })?;
    db.refresh_indexes()?;
    assert_eq!(nearest(&db, &index)?.len(), 1);
    assert!(
        db.transaction({
            let index = index.clone();
            move |c| {
                c.execute(&format!("DELETE FROM temp.\"{index}\""), [])?;
                Ok(())
            }
        })
        .is_err()
    );
    db.set_replication("docs", false)?;
    db.flush()?;
    let backup = dir.path().join("backup");
    db.backup(&backup)?;
    db.close()?;
    let db = Database::open(&path, [9; 32], Options::default())?;
    assert_eq!(nearest(&db, &index)?.len(), 1);
    db.set_column_vector("docs", "embedding", None)?;
    db.migrate(Migration::new("ALTER TABLE docs DROP COLUMN other"))?;
    assert_eq!(db.status().search_indexes.len(), 1);
    db.close()?;
    let restored = dir.path().join("restored");
    Database::restore(&backup, &restored, [9; 32])?;
    let db = Database::open(restored, [9; 32], Options::default())?;
    assert_eq!(nearest(&db, &index)?.len(), 1);
    db.close()
}
#[cfg(not(feature = "vector"))]
#[test]
fn disabled_build_rejects_vector_schema_settings() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Database::open(
        dir.path().join("db"),
        [9; 32],
        Options {
            mode: OpenMode::CreateNew,
            ..Options::default()
        },
    )?;
    db.migrate(Migration::new(
        "CREATE TABLE docs(id INTEGER PRIMARY KEY,embedding BLOB)",
    ))?;
    assert!(
        db.set_column_vector("docs", "embedding", config(2, DistanceMetric::L2))
            .is_err()
    );
    assert!(
        db.schema()?.tables[0]
            .columns
            .iter()
            .all(|c| c.vector.is_none())
    );
    db.close()
}

#[cfg(feature = "vector")]
#[test]
fn rejects_unsupported_keys_names_and_non_finite_values() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Database::open(
        dir.path().join("db"),
        [9; 32],
        Options {
            mode: OpenMode::CreateNew,
            ..Options::default()
        },
    )?;
    db.migrate(Migration::new("CREATE TABLE docs(id INTEGER PRIMARY KEY,embedding BLOB,\"bad-name\" BLOB); CREATE TABLE keyed(id TEXT NOT NULL PRIMARY KEY,embedding BLOB); CREATE TABLE composite(a INTEGER NOT NULL,b INTEGER NOT NULL,embedding BLOB,PRIMARY KEY(a,b))"))?;
    for (table, column) in [
        ("keyed", "embedding"),
        ("composite", "embedding"),
        ("docs", "bad-name"),
    ] {
        assert!(
            db.set_column_vector(table, column, config(2, DistanceMetric::L2))
                .is_err()
        );
    }
    db.set_column_vector("docs", "embedding", config(2, DistanceMetric::L2))?;
    for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        db.transaction(move |c| {
            let bytes = [value, 0.]
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>();
            c.execute(
                "INSERT OR REPLACE INTO docs(id,embedding) VALUES(1,?)",
                [bytes],
            )?;
            Ok(())
        })?;
        assert!(db.refresh_indexes().is_err());
        db.transaction(|c| {
            c.execute(
                "UPDATE docs SET embedding=x'0000803f00000000' WHERE id=1",
                [],
            )?;
            Ok(())
        })?;
        db.refresh_indexes()?;
    }
    db.migrate(Migration::new("ALTER TABLE docs RENAME TO renamed"))?;
    assert!(db.status().search_indexes.is_empty());
    db.set_column_vector("renamed", "embedding", config(2, DistanceMetric::L2))?;
    db.migrate(Migration::new("DROP TABLE renamed"))?;
    assert!(db.status().search_indexes.is_empty());
    db.close()
}
