use sqlite_session::*;

fn open(path: &std::path::Path, mode: OpenMode) -> Result<Database> {
    Database::open(
        path,
        [9; 32],
        Options {
            mode,
            capture_interval: std::time::Duration::from_secs(60),
            search_interval: std::time::Duration::from_secs(60),
            ..Options::default()
        },
    )
}
fn index(db: &Database, table: &str) -> String {
    format!(
        "murmur_fts_{}",
        db.schema()
            .unwrap()
            .tables
            .iter()
            .find(|t| t.name == table)
            .unwrap()
            .id
    )
}
fn count(db: &Database, name: &str, term: &str) -> Result<i64> {
    let sql = format!("SELECT count(*) FROM temp.\"{name}\" WHERE \"{name}\" MATCH ?");
    let term = term.to_owned();
    db.read(move |c| Ok(c.query_row(&sql, [term], |r| r.get(0))?))
}

#[test]
fn column_fts_persists_rebuilds_and_tracks_changes() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let db = open(&path, OpenMode::CreateNew)?;
    db.migrate(Migration::new(
        "CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT,body TEXT)",
    ))?;
    db.transaction(|c| {
        c.execute(
            "INSERT INTO docs VALUES(1,'hello','world'),(2,NULL,'second')",
            [],
        )?;
        Ok(())
    })?;
    db.set_column_fts("DOCS", "TITLE", true)?;
    let name = index(&db, "docs");
    assert_eq!(count(&db, &name, "hello")?, 1);
    let history = db.schema()?.history;
    db.set_column_fts("docs", "title", true)?;
    assert_eq!(db.schema()?.history, history);
    db.set_column_fts("docs", "body", true)?;
    assert_eq!(count(&db, &name, "world OR second")?, 2);
    assert!(
        db.transaction({
            let name = name.clone();
            move |c| {
                c.execute(&format!("DELETE FROM temp.\"{name}\""), [])?;
                Ok(())
            }
        })
        .is_err()
    );
    db.set_search_indexes(vec![
        Fts5Index::new("explicit", "docs", vec!["title".into()]).into(),
    ])?;
    assert_eq!(db.status().search_indexes.len(), 2);
    db.set_search_indexes(vec![])?;
    assert_eq!(db.status().search_indexes.len(), 1);
    db.set_replication("docs", true)?;
    assert!(db.set_column_fts("docs", "id_murmur_primary_key", true).is_err());
    assert_eq!(count(&db, &name, "hello")?, 1);
    db.transaction(|c| {
        c.execute("UPDATE docs SET title='changed' WHERE id=1", [])?;
        c.execute("DELETE FROM docs WHERE id=2", [])?;
        Ok(())
    })?;
    db.refresh_indexes()?;
    assert_eq!(count(&db, &name, "hello OR second")?, 0);
    assert_eq!(count(&db, &name, "changed")?, 1);
    db.set_replication("docs", false)?;
    db.flush()?;
    let backup = dir.path().join("backup");
    db.backup(&backup)?;
    db.close()?;
    let db = open(&path, OpenMode::Existing)?;
    assert_eq!(count(&db, &name, "changed")?, 1);
    assert!(
        db.schema()?.tables[0]
            .columns
            .iter()
            .find(|c| c.name == "body")
            .unwrap()
            .fts
    );
    db.set_column_fts("docs", "title", false)?;
    assert_eq!(count(&db, &name, "changed")?, 0);
    assert_eq!(count(&db, &name, "world")?, 1);
    db.set_column_fts("docs", "body", false)?;
    assert!(db.status().search_indexes.is_empty());
    db.close()?;
    let restored = dir.path().join("restored");
    Database::restore(&backup, &restored, [9; 32])?;
    let db = open(&restored, OpenMode::Existing)?;
    assert_eq!(count(&db, &name, "changed")?, 1);
    db.close()
}

#[test]
fn rejects_invalid_declarations_and_keeps_local_ddl_working() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = open(&dir.path().join("db"), OpenMode::CreateNew)?;
    db.migrate(Migration::new("CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT,n INTEGER); CREATE TABLE keyed(id TEXT NOT NULL PRIMARY KEY,body TEXT); CREATE TABLE composite(a INTEGER NOT NULL,b INTEGER NOT NULL,body TEXT,PRIMARY KEY(a,b))"))?;
    for (table, col) in [
        ("absent", "body"),
        ("docs", "absent"),
        ("docs", "n"),
        ("docs", "id"),
        ("keyed", "body"),
        ("composite", "body"),
    ] {
        assert!(
            db.set_column_fts(table, col, true).is_err(),
            "{table}.{col}"
        );
    }
    db.set_column_fts("docs", "body", true)?;
    let name = index(&db, "docs");
    assert!(
        db.set_search_indexes(vec![
            Fts5Index::new(&name, "docs", vec!["body".into()]).into()
        ])
        .is_err()
    );
    assert_eq!(db.status().search_indexes.len(), 1);
    db.migrate(Migration::new("ALTER TABLE docs ADD COLUMN extra TEXT"))?;
    assert!(
        db.schema()?
            .tables
            .iter()
            .find(|t| t.name == "docs")
            .unwrap()
            .columns
            .iter()
            .find(|c| c.name == "body")
            .unwrap()
            .fts
    );
    db.migrate(Migration::new("ALTER TABLE docs DROP COLUMN body"))?;
    assert!(db.status().search_indexes.is_empty());
    db.set_column_fts("docs", "extra", true)?;
    db.migrate(Migration::new("ALTER TABLE docs RENAME TO renamed"))?;
    assert!(
        db.status().search_indexes.is_empty(),
        "unmapped rename creates a fresh identity"
    );
    db.set_column_fts("renamed", "extra", true)?;
    db.migrate(Migration::new("DROP TABLE renamed"))?;
    assert!(db.status().search_indexes.is_empty());
    db.close()
}
