use sqlite_session::*;

fn open(path: &std::path::Path, mode: OpenMode) -> Result<Database> {
    Database::open(
        path,
        [7; 32],
        Options {
            mode,
            ..Options::default()
        },
    )
}
fn key(db: &Database, id: i64) -> Result<String> {
    db.read(move |sql| {
        Ok(sql.query_row(
            "SELECT id_murmur_primary_key FROM items WHERE id=?",
            [id],
            |r| r.get(0),
        )?)
    })
}
#[test]
fn populated_round_trip_restores_sqlite_keys_and_reopens() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("db");
    let db = open(&path, OpenMode::CreateNew)?;
    db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT DEFAULT 'default'); CREATE INDEX items_value ON items(value)"))?;
    assert!(!db.replication_enabled("items")?);
    db.transaction(|sql| {
        sql.execute("INSERT INTO items(value) VALUES('first')", [])?;
        Ok(())
    })?;
    assert_eq!(db.schema()?.tables[0].columns.len(), 2);
    db.set_replication("items", true)?;
    let first = key(&db, 1)?;
    db.migrate(Migration::new(
        "CREATE INDEX items_managed ON items(id_murmur_primary_key)",
    ))?;
    assert_eq!(uuid::Uuid::parse_str(&first).unwrap().get_version_num(), 5);
    let table = db.schema()?.tables.remove(0);
    assert_eq!(
        table
            .columns
            .iter()
            .filter(|c| c.primary_key > 0)
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        ["id_murmur_primary_key"]
    );
    assert_eq!(table.application_key.as_ref().unwrap().columns, ["id"]);
    db.transaction(|sql| {
        sql.execute("INSERT INTO items(id) VALUES(?)", [2])?;
        Ok(())
    })?;
    db.close()?;
    let db = open(&path, OpenMode::Existing)?;
    assert_eq!(key(&db, 1)?, first);
    assert!(db.replication_enabled("items")?);
    db.set_replication("items", false)?;
    assert!(db.schema()?.tables[0].application_key.is_none());
    assert!(
        db.schema()?
            .indexes
            .iter()
            .any(|sql| sql.contains("items_value"))
    );
    assert!(
        !db.schema()?
            .indexes
            .iter()
            .any(|sql| sql.contains("items_managed"))
    );
    db.transaction(|sql| {
        sql.execute("DELETE FROM items WHERE id=1", [])?;
        sql.execute("UPDATE items SET id=5 WHERE id=2", [])?;
        sql.execute("INSERT INTO items(value) VALUES('allocated')", [])?;
        Ok(())
    })?;
    db.set_replication("items", true)?;
    assert_ne!(key(&db, 5)?, first);
    let state = db.export_state()?;
    assert_eq!(state.rows.len(), 2);
    assert!(!state.tombstones.is_empty());
    db.close()?;
    let db = open(&path, OpenMode::Existing)?;
    assert!(db.replication_enabled("items")?);
    assert_eq!(
        db.read(
            |sql| Ok(sql.query_row("SELECT count(*) FROM items", [], |r| r.get::<_, i64>(0))?)
        )?,
        2
    );
    db.close()
}
#[test]
fn sql_entrypoints_generate_read_only_keys_and_preserve_conflicts() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = open(&temp.path().join("db"), OpenMode::CreateNew)?;
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
    ))?;
    db.set_replication("items", true)?;
    db.transaction(|sql|{
        sql.execute_batch("INSERT INTO items VALUES(1,'one'),(2,'two'); INSERT INTO items(id,value) SELECT 3,'three';")?;
        sql.prepare("INSERT INTO items(id,value) VALUES(?1,?2)")?.execute(rusqlite::params![4,"four"])?;
        let returned:String=sql.query_row("INSERT INTO items(id,value) VALUES(5,'five') RETURNING id_murmur_primary_key",[],|r|r.get(0))?;
        assert_eq!(returned.len(),36);
        sql.execute("INSERT INTO items(id,value) VALUES(1,'changed') ON CONFLICT(id) DO UPDATE SET value=excluded.value",[])?;
        sql.execute("WITH input(id,v) AS (VALUES(6,'six')) INSERT INTO items(id,value) SELECT id,v FROM input",[])?;
        sql.execute("REPLACE INTO items VALUES(2,'replaced')",[])?;
        sql.execute("INSERT INTO main.items AS target(id,value) VALUES(7,'seven')",[])?;
        Ok(())
    })?;
    let first = key(&db, 1)?;
    for statement in [
        "INSERT INTO items(value) VALUES('missing')",
        "INSERT INTO items(id) VALUES(NULL)",
        "INSERT INTO items(id,id_murmur_primary_key) VALUES(8,'forged')",
        "UPDATE items SET id_murmur_primary_key='forged' WHERE id=1",
        "UPDATE items SET id=9 WHERE id=1",
        "UPDATE items SET ID=9 WHERE id=1",
        "INSERT INTO items(id) VALUES(1) ON CONFLICT(id) DO UPDATE SET id_murmur_primary_key='forged'",
    ] {
        assert!(
            db.transaction(move |sql| {
                sql.execute(statement, [])?;
                Ok(())
            })
            .is_err(),
            "{statement}"
        );
    }
    assert_eq!(key(&db, 1)?, first);
    assert!(
        db.migrate(Migration::new("UPDATE items SET id=9 WHERE id=1"))
            .is_err()
    );
    db.migrate(Migration::new("ALTER TABLE items ADD COLUMN extra TEXT; INSERT INTO items(id,value) VALUES(8,'migration')"))?;
    db.set_replication("items", false)?;
    assert!(
        db.schema()?.tables[0]
            .columns
            .iter()
            .any(|c| c.name == "extra")
    );
    db.close()
}
#[test]
fn composite_text_blob_and_collations_are_deterministic() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let make = |name: &str| -> Result<Database> {
        let db = open(&temp.path().join(name), OpenMode::CreateNew)?;
        db.migrate(Migration::new("CREATE TABLE records(tenant TEXT NOT NULL COLLATE NOCASE, id BLOB NOT NULL, v TEXT, PRIMARY KEY(tenant,id)) WITHOUT ROWID"))?;
        db.set_replication("records", true)?;
        Ok(db)
    };
    let a = make("a")?;
    let b = make("b")?;
    let insert = |db: &Database, tenant: &str| -> Result<String> {
        let tenant = tenant.to_owned();
        db.transaction(move|sql|Ok(sql.query_row("INSERT INTO records(tenant,id,v) VALUES(?1,?2,'v') RETURNING id_murmur_primary_key",rusqlite::params![tenant,vec![0u8,1,255]],|r|r.get(0))?))
    };
    assert_eq!(insert(&a, "TENANT")?, insert(&b, "tenant")?);
    a.set_replication("records", false)?;
    assert!(a.schema()?.tables[0].sql.contains("WITHOUT ROWID"));
    a.close()?;
    b.close()
}
#[test]
fn no_primary_key_and_reserved_column_are_rejected() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = open(&temp.path().join("db"), OpenMode::CreateNew)?;
    for ddl in [
        "CREATE TABLE bad(value TEXT)",
        "CREATE TABLE bad(id INTEGER PRIMARY KEY,id_murmur_primary_key TEXT)",
    ] {
        assert!(db.migrate(Migration::new(ddl)).is_err());
    }
    db.close()
}
#[test]
fn text_affinity_and_rtrim_match_existing_rows() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = open(&temp.path().join("db"), OpenMode::CreateNew)?;
    db.migrate(Migration::new(
        "CREATE TABLE words(id TEXT NOT NULL COLLATE RTRIM PRIMARY KEY,v TEXT)",
    ))?;
    db.transaction(|sql| {
        sql.execute("INSERT INTO words VALUES(1,'before')", [])?;
        Ok(())
    })?;
    db.set_replication("words", true)?;
    let before = db.read(|sql| {
        Ok(
            sql.query_row("SELECT id_murmur_primary_key FROM words", [], |r| {
                r.get::<_, String>(0)
            })?,
        )
    })?;
    let after=db.transaction(|sql|Ok(sql.query_row("INSERT INTO words(id,v) VALUES('1   ','after') ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING id_murmur_primary_key",[],|r|r.get::<_,String>(0))?))?;
    assert_eq!(before, after);
    db.close()
}

#[test]
fn named_constraints_defaults_and_strict_survive_conversion() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = open(&temp.path().join("db"), OpenMode::CreateNew)?;
    db.migrate(Migration::new("CREATE TABLE items(id INTEGER CONSTRAINT app_key PRIMARY KEY ON CONFLICT IGNORE, value TEXT DEFAULT 'default') STRICT"))?;
    db.set_replication("items", true)?;
    db.transaction(|s| {
        assert_eq!(s.execute("INSERT INTO items(id) VALUES(1)", [])?, 1);
        assert_eq!(s.execute("INSERT INTO items(id) VALUES(1)", [])?, 0);
        Ok(())
    })?;
    db.set_replication("items", false)?;
    assert!(db.schema()?.tables[0].sql.ends_with("STRICT"));
    db.migrate(Migration::new(
        "CREATE TABLE secondary(id TEXT NOT NULL PRIMARY KEY) STRICT, WITHOUT ROWID",
    ))?;
    db.set_replication("secondary", true)?;
    let table = db
        .schema()?
        .tables
        .into_iter()
        .find(|t| t.name == "secondary")
        .unwrap();
    let key = table.application_key.unwrap();
    assert!(key.strict && key.without_rowid);
    db.set_replication("secondary", false)?;
    assert_eq!(
        db.read(|s| Ok(s.query_row("SELECT value FROM items", [], |r| r.get::<_, String>(0))?))?,
        "default"
    );
    db.close()
}
#[test]
fn volatile_key_expressions_are_evaluated_once_and_numeric_affinity_matches() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = open(&temp.path().join("db"), OpenMode::CreateNew)?;
    db.migrate(Migration::new(
        "CREATE TABLE items(id NUMERIC NOT NULL PRIMARY KEY,v TEXT)",
    ))?;
    db.set_replication("items", true)?;
    db.transaction(|s| {
        s.execute(
            "INSERT INTO items(id) VALUES(random()),('3.0'),('0x10'),(x'0102')",
            [],
        )?;
        Ok(())
    })?;
    for row in db.export_state()?.rows {
        let table = &db.schema()?.tables[0];
        let idcol = table.columns.iter().find(|c| c.name == "id").unwrap();
        let value = &row
            .cells
            .iter()
            .find(|c| c.column == idcol.id)
            .unwrap()
            .value;
        let uuidcol = table
            .columns
            .iter()
            .find(|c| c.name == "id_murmur_primary_key")
            .unwrap();
        assert_eq!(
            row.key[0],
            row.cells
                .iter()
                .find(|c| c.column == uuidcol.id)
                .unwrap()
                .value
        );
        assert!(!matches!(value, Value::Null));
    }
    // Promotion independently hashes the values actually stored by SQLite.
    let before = db.read(|s| {
        let mut q =
            s.prepare("SELECT id_murmur_primary_key FROM items ORDER BY id_murmur_primary_key")?;
        Ok(q.query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    })?;
    db.set_replication("items", false)?;
    db.set_replication("items", true)?;
    let after = db.read(|s| {
        let mut q =
            s.prepare("SELECT id_murmur_primary_key FROM items ORDER BY id_murmur_primary_key")?;
        Ok(q.query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    })?;
    assert_eq!(before, after);
    db.close()
}
#[test]
fn peer_merge_and_reenable_propagate_local_deletions_and_reject_forged_ids() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let make = |name: &str, node| -> Result<Database> {
        let db = Database::open(
            temp.path().join(name),
            [7; 32],
            Options {
                mode: OpenMode::CreateNew,
                database_id: Some([1; 16]),
                node_id: Some([node; 16]),
                ..Options::default()
            },
        )?;
        db.migrate(Migration::new(
            "CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)",
        ))?;
        db.set_replication("items", true)?;
        Ok(db)
    };
    let a = make("a", 1)?;
    let b = make("b", 2)?;
    a.transaction(|s| {
        s.execute("INSERT INTO items VALUES(1,'one'),(2,'two')", [])?;
        Ok(())
    })?;
    b.merge_state(a.export_state()?)?;
    let original = key(&a, 1)?;
    a.set_replication("items", false)?;
    a.transaction(|s| {
        s.execute("DELETE FROM items WHERE id=2", [])?;
        s.execute("UPDATE items SET value='local edit' WHERE id=1", [])?;
        Ok(())
    })?;
    a.set_replication("items", true)?;
    assert_eq!(key(&a, 1)?, original);
    b.merge_state(a.export_state()?)?;
    assert_eq!(
        b.read(|s| Ok(s.query_row("SELECT count(*) FROM items", [], |r| r.get::<_, i64>(0))?))?,
        1
    );
    assert_eq!(
        b.read(
            |s| Ok(s.query_row("SELECT value FROM items WHERE id=1", [], |r| r
                .get::<_, String>(0))?)
        )?,
        "local edit"
    );
    let mut invalid = a.export_state()?;
    invalid.rows[0].key = vec![Value::Text("forged".into())];
    assert!(b.merge_state(invalid).is_err());
    a.close()?;
    b.close()
}
