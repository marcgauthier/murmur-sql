use sqlite_session::*;

#[test]
fn disabled_table_persists_locally_and_setting_survives_mapped_migrations() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = Database::open(
        &path,
        [7; 32],
        Options {
            mode: OpenMode::CreateNew,
            ..Options::default()
        },
    )?;
    db.migrate(Migration::new("CREATE TABLE local(id INTEGER PRIMARY KEY,n INTEGER); CREATE TABLE shared(id INTEGER PRIMARY KEY)"))?;
    assert!(!db.replication_enabled("local")?);
    db.set_replication("shared", true)?;
    assert!(db.replication_enabled("shared")?);
    db.set_replication("local", false)?;
    db.transaction(|s| {
        s.execute("INSERT INTO local VALUES(1,42)", [])?;
        Ok(())
    })?;
    let id = db
        .export_state()?
        .schema
        .tables
        .iter()
        .find(|t| t.name == "local")
        .unwrap()
        .id
        .clone();
    db.migrate(Migration {
        sql: "ALTER TABLE local RENAME TO renamed; ALTER TABLE renamed ADD COLUMN extra TEXT"
            .into(),
        mappings: vec![TableMapping {
            old_name: "local".into(),
            new_name: "renamed".into(),
            columns: vec![],
        }],
    })?;
    assert!(!db.replication_enabled("renamed")?);
    assert!(db.replication_enabled("shared")?);
    assert_eq!(
        db.export_state()?
            .schema
            .tables
            .iter()
            .find(|t| t.name == "renamed")
            .unwrap()
            .id,
        id
    );
    db.flush()?;
    db.close()?;
    let db = Database::open(&path, [7; 32], Options::default())?;
    assert!(!db.replication_enabled("RENAMED")?);
    assert!(db.replication_enabled("shared")?);
    assert_eq!(
        db.read(
            |s| Ok(s.query_row("SELECT n FROM renamed WHERE id=1", [], |r| r
                .get::<_, i64>(0))?)
        )?,
        42
    );
    db.SetReplication("renamed", true)?;
    db.close()?;
    let db = Database::open(&path, [7; 32], Options::default())?;
    assert!(db.replication_enabled("renamed")?);
    db.close()
}

#[test]
fn unknown_table_setting_fails_without_changing_existing_policy() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = Database::open(
        tmp.path().join("db"),
        [7; 32],
        Options {
            mode: OpenMode::CreateNew,
            ..Options::default()
        },
    )?;
    db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY)"))?;
    assert!(db.set_replication("missing", false).is_err());
    assert!(!db.replication_enabled("items")?);
    db.set_replication("items", false)?;
    db.set_replication("items", false)?;
    assert!(!db.replication_enabled("items")?);
    db.close()
}

#[test]
fn replicated_tables_allow_additive_ddl_but_reject_drops_and_renames() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = Database::open(
        tmp.path().join("db"),
        [7; 32],
        Options {
            mode: OpenMode::CreateNew,
            ..Options::default()
        },
    )?;
    db.migrate(Migration::new(
        "CREATE TABLE shared(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE local(id INTEGER PRIMARY KEY, v TEXT)",
    ))?;
    db.set_replication("shared", true)?;
    db.set_replication("local", false)?;

    // Additive DDL works on replicated tables.
    db.migrate(Migration::new(
        "ALTER TABLE shared ADD COLUMN extra TEXT; CREATE INDEX shared_v ON shared(v)",
    ))?;
    db.migrate(Migration::new("DROP INDEX shared_v"))?;
    db.migrate(Migration::new(
        "CREATE TABLE added(id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
    ))?;

    // Drops and renames are rejected while the table replicates ...
    for ddl in [
        "DROP TABLE shared",
        "DROP TABLE IF EXISTS shared",
        "ALTER TABLE shared RENAME TO moved",
        "ALTER TABLE shared RENAME COLUMN v TO w",
        "ALTER TABLE shared RENAME v TO w",
        "ALTER TABLE shared DROP COLUMN v",
        "ALTER TABLE shared DROP v",
        "CREATE TABLE swap(id INTEGER PRIMARY KEY); DROP TABLE shared; ALTER TABLE swap RENAME TO shared",
    ] {
        let error = db.migrate(Migration::new(ddl)).unwrap_err();
        assert!(
            matches!(error, Error::Invalid(_)),
            "{ddl}: expected Invalid, got {error:?}"
        );
    }
    // ... including through quoting, schema prefixes, and comments.
    for ddl in [
        "DROP TABLE \"shared\"",
        "DROP TABLE main.shared",
        "/* lead */ ALTER TABLE [shared] RENAME TO moved",
        "ALTER TABLE shared RENAME COLUMN \"v\" TO w",
    ] {
        assert!(
            db.migrate(Migration::new(ddl)).is_err(),
            "{ddl}: expected rejection"
        );
    }
    // temp. targets are not schema tables: the gate skips them and
    // SQLite reports the missing table itself.
    let error = db.migrate(Migration::new("DROP TABLE temp.ghost")).unwrap_err();
    assert!(matches!(error, Error::Sql(_)), "got {error:?}");

    // ... but succeed once replication is disabled for the table.
    db.set_replication("shared", false)?;
    db.migrate(Migration::new("ALTER TABLE shared RENAME COLUMN v TO w"))?;
    db.migrate(Migration::new("ALTER TABLE shared DROP COLUMN extra"))?;
    db.migrate(Migration::new("ALTER TABLE shared RENAME TO moved"))?;
    // An unmapped rename remains local-only under the default.
    assert!(!db.replication_enabled("moved")?);
    db.set_replication("moved", false)?;
    db.migrate(Migration::new("DROP TABLE moved"))?;
    assert!(!db.replication_enabled("added")?);
    db.close()
}

#[test]
fn schema_inspection_rejects_unsupported_definitions() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = Database::open(
        tmp.path().join("db"),
        [7; 32],
        Options {
            mode: OpenMode::CreateNew,
            ..Options::default()
        },
    )?;
    // Each definition is attempted on a fresh table name so one rejection
    // cannot mask another.
    for (name, ddl) in [
        ("u1", "CREATE TABLE u1(id INTEGER PRIMARY KEY, v TEXT UNIQUE)"),
        ("u2", "CREATE TABLE u2(id INTEGER PRIMARY KEY, v TEXT); CREATE UNIQUE INDEX u2_v ON u2(v)"),
        ("u3", "CREATE TABLE u3(id INTEGER PRIMARY KEY, v TEXT, CONSTRAINT c UNIQUE(v))"),
        ("n1", "CREATE TABLE n1(id TEXT PRIMARY KEY, v TEXT)"),
        ("n2", "CREATE TABLE n2(a TEXT NOT NULL, b TEXT, PRIMARY KEY(a, b))"),
        ("f1", "CREATE TABLE f1(id INTEGER PRIMARY KEY, o INTEGER REFERENCES other(id))"),
        ("c1", "CREATE TABLE c1(id INTEGER PRIMARY KEY, v INTEGER CHECK(v > 0))"),
        ("a1", "CREATE TABLE a1(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)"),
        ("t1", "CREATE TABLE t1(id INTEGER PRIMARY KEY); CREATE TRIGGER t1_tr AFTER INSERT ON t1 BEGIN SELECT 1; END"),
        ("v1", "CREATE TABLE v1(id INTEGER PRIMARY KEY); CREATE VIEW vv AS SELECT * FROM v1"),
        ("g1", "CREATE TABLE g1(id INTEGER PRIMARY KEY, v TEXT GENERATED ALWAYS AS ('x'))"),
        ("g2", "CREATE TABLE g2(id INTEGER PRIMARY KEY, v TEXT AS ('x'))"),
    ] {
        let error = db.migrate(Migration::new(ddl)).unwrap_err();
        // Triggers and views are denied by the authorizer before inspection
        // runs; everything else fails schema inspection.
        let allowed = matches!(error, Error::Invalid(_))
            || ((name == "t1" || name == "v1") && matches!(error, Error::Sql(_)));
        assert!(allowed, "{name}: unexpected {error:?}");
    }
    // A nullable single INTEGER PRIMARY KEY is accepted by SQLite convention.
    db.migrate(Migration::new("CREATE TABLE ok(id INTEGER PRIMARY KEY)"))?;
    db.set_replication("ok", true)?;
    // Ordinary writes still work and replicated key updates stay denied.
    db.transaction(|sql| {
        sql.execute("INSERT INTO ok VALUES(1)", [])?;
        assert!(sql.execute("UPDATE ok SET id = 2 WHERE id = 1", []).is_err());
        Ok(())
    })?;
    db.close()
}
