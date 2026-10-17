use sqlite_session::*;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Item {
    id: String,
    val: String,
    score: i64,
}

fn options() -> Options {
    Options {
        mode: OpenMode::CreateNew,
        capture_interval: Duration::from_millis(20),
        ..Options::default()
    }
}

fn create_db(path: &std::path::Path) -> Result<Database> {
    let db = Database::open(path, [42; 32], options())?;
    db.migrate(Migration::new(
        "CREATE TABLE items(id TEXT PRIMARY KEY NOT NULL, val TEXT NOT NULL, score INTEGER NOT NULL);",
    ))?;
    Ok(db)
}

#[test]
fn subscription_initial_and_row_diffs() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = create_db(&path)?;

    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES('k1', 'val1', 10)", [])?;
        Ok(())
    })?;
    db.flush()?;

    let sub = db.subscribe(
        |sql| {
            let mut stmt = sql.prepare("SELECT id, val, score FROM items ORDER BY id")?;
            let rows = stmt
                .query_map([], |r| {
                    Ok(Item {
                        id: r.get(0)?,
                        val: r.get(1)?,
                        score: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        },
        |item| item.id.as_bytes().to_vec(),
        SubscriptionOptions::default(),
    )?;

    // 1. Initial snapshot
    let event = sub.recv_timeout(Duration::from_secs(2))?;
    match event {
        SubscriptionEvent::Initial { rows, cursor } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].id, "k1");
            assert_eq!(rows[0].val, "val1");
            assert!(cursor.sequence >= 1);
        }
        _ => panic!("expected initial event"),
    }

    // 2. Insert item -> Added change
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES('k2', 'val2', 20)", [])?;
        Ok(())
    })?;
    db.flush()?;

    let event = sub.recv_timeout(Duration::from_secs(2))?;
    match event {
        SubscriptionEvent::Update {
            rows,
            changes,
            cursor: _,
        } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].change_type, ChangeType::Added);
            assert_eq!(changes[0].key, b"k2");
            assert_eq!(changes[0].row.as_ref().unwrap().val, "val2");
        }
        _ => panic!("expected update event with Added"),
    }

    // 3. Update item -> Updated change
    db.transaction(|c| {
        c.execute("UPDATE items SET val = 'val1-updated' WHERE id = 'k1'", [])?;
        Ok(())
    })?;
    db.flush()?;

    let event = sub.recv_timeout(Duration::from_secs(2))?;
    match event {
        SubscriptionEvent::Update {
            rows,
            changes,
            cursor: _,
        } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].change_type, ChangeType::Updated);
            assert_eq!(changes[0].key, b"k1");
            assert_eq!(changes[0].row.as_ref().unwrap().val, "val1-updated");
        }
        _ => panic!("expected update event with Updated"),
    }

    // 4. Delete item -> Removed change
    db.transaction(|c| {
        c.execute("DELETE FROM items WHERE id = 'k2'", [])?;
        Ok(())
    })?;
    db.flush()?;

    let event = sub.recv_timeout(Duration::from_secs(2))?;
    match event {
        SubscriptionEvent::Update {
            rows,
            changes,
            cursor: _,
        } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].change_type, ChangeType::Removed);
            assert_eq!(changes[0].key, b"k2");
            assert!(changes[0].row.is_none());
        }
        _ => panic!("expected update event with Removed"),
    }

    db.close()
}

#[test]
fn subscription_table_filtering() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = Database::open(&path, [42; 32], options())?;
    db.migrate(Migration::new(
        "CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL); CREATE TABLE logs(id TEXT PRIMARY KEY NOT NULL, msg TEXT NOT NULL);",
    ))?;

    // Subscribe ONLY to users table
    let sub = db.subscribe(
        |sql| {
            let mut stmt = sql.prepare("SELECT id, name FROM users ORDER BY id")?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        },
        |u| u.0.as_bytes().to_vec(),
        SubscriptionOptions {
            tables: vec!["users".into()],
            ..Default::default()
        },
    )?;

    // Consume Initial event
    let init = sub.recv_timeout(Duration::from_secs(2))?;
    assert!(matches!(init, SubscriptionEvent::Initial { .. }));

    // Write to `logs` table -> should NOT notify `users` subscription
    db.transaction(|c| {
        c.execute("INSERT INTO logs VALUES('l1', 'audit event')", [])?;
        Ok(())
    })?;
    db.flush()?;

    // recv_timeout should time out (no events generated for logs write)
    assert!(matches!(
        sub.recv_timeout(Duration::from_millis(150)),
        Err(Error::Busy)
    ));

    // Write to `users` table -> should notify
    db.transaction(|c| {
        c.execute("INSERT INTO users VALUES('u1', 'Alice')", [])?;
        Ok(())
    })?;
    db.flush()?;

    let ev = sub.recv_timeout(Duration::from_secs(2))?;
    match ev {
        SubscriptionEvent::Update { rows, changes, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].change_type, ChangeType::Added);
        }
        _ => panic!("expected Update event for users"),
    }

    db.close()
}

#[test]
fn subscription_resume_cursor_validation() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = create_db(&path)?;

    let sub = db.subscribe(
        |sql| {
            let mut stmt = sql.prepare("SELECT id, val, score FROM items")?;
            let rows = stmt
                .query_map([], |r| {
                    Ok(Item {
                        id: r.get(0)?,
                        val: r.get(1)?,
                        score: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        },
        |item| item.id.as_bytes().to_vec(),
        SubscriptionOptions::default(),
    )?;

    let valid_cursor = match sub.recv_timeout(Duration::from_secs(2))? {
        SubscriptionEvent::Initial { cursor, .. } => cursor,
        _ => panic!("expected initial"),
    };

    drop(sub);

    // Resuming with valid cursor succeeds
    let resumed = db.subscribe(
        |sql| {
            let mut stmt = sql.prepare("SELECT id, val, score FROM items")?;
            let rows = stmt
                .query_map([], |r| {
                    Ok(Item {
                        id: r.get(0)?,
                        val: r.get(1)?,
                        score: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        },
        |item| item.id.as_bytes().to_vec(),
        SubscriptionOptions {
            resume_from: Some(valid_cursor),
            ..Default::default()
        },
    );
    assert!(resumed.is_ok());

    // Resuming with wrong database_id fails
    let mut bad_db_cursor = valid_cursor;
    bad_db_cursor.database_id = [99; 16];
    assert!(matches!(
        db.subscribe(
            |_| Ok(Vec::<Item>::new()),
            |i| i.id.as_bytes().to_vec(),
            SubscriptionOptions {
                resume_from: Some(bad_db_cursor),
                ..Default::default()
            }
        ),
        Err(Error::Invalid(_))
    ));

    // Resuming with future sequence fails
    let mut future_cursor = valid_cursor;
    future_cursor.sequence = 999_999;
    assert!(matches!(
        db.subscribe(
            |_| Ok(Vec::<Item>::new()),
            |i| i.id.as_bytes().to_vec(),
            SubscriptionOptions {
                resume_from: Some(future_cursor),
                ..Default::default()
            }
        ),
        Err(Error::Invalid(_))
    ));

    db.close()
}

#[test]
fn subscription_slow_consumer_reset() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = create_db(&path)?;

    // Create subscription with small buffer size = 2
    let sub = db.subscribe(
        |sql| {
            let mut stmt = sql.prepare("SELECT id, val, score FROM items ORDER BY id")?;
            let rows = stmt
                .query_map([], |r| {
                    Ok(Item {
                        id: r.get(0)?,
                        val: r.get(1)?,
                        score: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        },
        |item| item.id.as_bytes().to_vec(),
        SubscriptionOptions {
            buffer_size: 2,
            ..Default::default()
        },
    )?;

    // Read initial event
    let init = sub.recv_timeout(Duration::from_secs(2))?;
    assert!(matches!(init, SubscriptionEvent::Initial { .. }));

    // Generate multiple updates without calling recv() so buffer overflows
    for i in 0..5 {
        db.transaction(move |c| {
            c.execute(
                &format!("INSERT INTO items VALUES('k{}', 'val{}', {})", i, i, i),
                [],
            )?;
            Ok(())
        })?;
        db.flush()?;
    }

    // Give background worker time to process and encounter buffer overflow
    std::thread::sleep(Duration::from_millis(300));

    // The consumer should receive a Reset event
    let event = sub.recv_timeout(Duration::from_secs(2))?;
    match event {
        SubscriptionEvent::Reset { reason, .. } => {
            assert!(
                reason.contains("buffer overflow") || reason.contains("fell behind"),
                "expected buffer overflow reason, got: {}",
                reason
            );
        }
        other => panic!("expected Reset event due to slow consumer, got: {:?}", other),
    }

    db.close()
}

#[test]
fn subscription_migration_triggers_reset() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = create_db(&path)?;

    let sub = db.subscribe(
        |sql| {
            let mut stmt = sql.prepare("SELECT id, val, score FROM items")?;
            let rows = stmt
                .query_map([], |r| {
                    Ok(Item {
                        id: r.get(0)?,
                        val: r.get(1)?,
                        score: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        },
        |item| item.id.as_bytes().to_vec(),
        SubscriptionOptions::default(),
    )?;

    let init = sub.recv_timeout(Duration::from_secs(2))?;
    assert!(matches!(init, SubscriptionEvent::Initial { .. }));

    // Run schema migration
    db.migrate(Migration::new(
        "ALTER TABLE items ADD COLUMN extra TEXT DEFAULT '';",
    ))?;

    // Subscription should receive Reset event
    let event = sub.recv_timeout(Duration::from_secs(2))?;
    assert!(matches!(event, SubscriptionEvent::Reset { .. }));

    db.close()
}

#[test]
fn subscription_database_close_clean_shutdown() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = create_db(&path)?;

    let mut sub = db.subscribe(
        |sql| {
            let mut stmt = sql.prepare("SELECT id, val, score FROM items")?;
            let rows = stmt
                .query_map([], |r| {
                    Ok(Item {
                        id: r.get(0)?,
                        val: r.get(1)?,
                        score: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        },
        |item| item.id.as_bytes().to_vec(),
        SubscriptionOptions::default(),
    )?;

    let init = sub.recv_timeout(Duration::from_secs(2))?;
    assert!(matches!(init, SubscriptionEvent::Initial { .. }));

    // Closing the database should notify reset or close to subscribers
    db.close()?;

    let res = sub.recv_timeout(Duration::from_millis(500));
    assert!(matches!(res, Ok(SubscriptionEvent::Reset { .. }) | Err(Error::Closed)));

    sub.close();
    Ok(())
}

#[test]
fn replication_mode_conversion_resets_subscriptions() -> Result<()> {
    let temp=tempfile::tempdir()?;
    let db=create_db(&temp.path().join("db"))?;
    let sub=db.subscribe(
        |sql|Ok(vec![sql.query_row("SELECT count(*) FROM items",[],|r|r.get::<_,i64>(0))?]),
        |count|count.to_be_bytes().to_vec(),
        SubscriptionOptions::default(),
    )?;
    assert!(matches!(sub.recv_timeout(Duration::from_secs(2))?,SubscriptionEvent::Initial{..}));
    db.set_replication("items",true)?;
    assert!(matches!(sub.recv_timeout(Duration::from_secs(2))?,SubscriptionEvent::Reset{..}));
    db.close()
}
