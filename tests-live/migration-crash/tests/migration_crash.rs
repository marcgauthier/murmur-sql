//! Rust port of the Go `migration-crash` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/migration-crash/`
//! (`migration_crash_test.go`). An isolated `murmur-server` process recovers
//! from a crash at the schema-manifest store boundary while its two peers
//! stay on the old epoch; afterwards it rejoins, the peers adopt the new
//! schema and rebind, and a migrated-field write converges everywhere. Two
//! cases: `before-store` (recovery reopens old and retries the migration)
//! and `after-store` (recovery reopens already migrated). Plain SQL replaces
//! the Go typed-records HTTP API, and child stop/restart replaces the
//! mid-migration process exit (see the scenario README.md for deviations).

use std::time::Duration;

use murmur_live_harness::{wait_until_async, Cluster, ClusterOptions};

/// Base table DDL (Go: typed records with a Name field).
const TABLE: &str = "live_typed_records";
const TABLE_DDL: &str =
    "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, counter INTEGER NOT NULL)";
/// Additive migration DDL (Go: `MigrateTypedRecords` registers Note).
const NOTE_DDL: &str = "ALTER TABLE live_typed_records ADD COLUMN note TEXT";
/// Seeded rows (Go: `seedRows = 24`).
const SEED_ROWS: usize = 24;
/// Seed convergence deadline (Go: 45 s).
const SEED_TIMEOUT: Duration = Duration::from_secs(45);
/// Post-rejoin convergence deadline (Go: 90 s).
const REJOIN_TIMEOUT: Duration = Duration::from_secs(90);
/// Peer-adoption deadline (Go: 45 s `waitEpoch`).
const EPOCH_TIMEOUT: Duration = Duration::from_secs(45);
/// Final epoch+field convergence deadline (Go: 45 s).
const FINAL_TIMEOUT: Duration = Duration::from_secs(45);
/// Poll interval (Go: 250 ms).
const POLL: Duration = Duration::from_millis(250);

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
async fn names(cluster: &Cluster, idx: usize) -> Result<Vec<String>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT name FROM live_typed_records ORDER BY name", &[])
        .await
        .map_err(|e| format!("node{} read names: {e}", idx + 1))?;
    rows.iter()
        .map(|row| {
            row.try_get::<_, String>(0)
                .map_err(|e| format!("node{} decode name: {e}", idx + 1))
        })
        .collect()
}

/// Insert one record (Go: `Cluster.TypedInsertWithID`).
async fn insert_row(cluster: &Cluster, idx: usize, name: &str) -> Result<(), String> {
    cluster
        .node(idx)
        .execute("INSERT INTO live_typed_records(name, counter) VALUES($1, 0)", &[&name])
        .await
        .map_err(|e| format!("node{} insert failed: {e}", idx + 1))?;
    cluster
        .node(idx)
        .flush()
        .await
        .map_err(|e| format!("node{} flush after insert failed: {e}", idx + 1))?;
    Ok(())
}

/// Write the migrated Note field (Go: `Cluster.TypedSetNote`).
async fn set_note(cluster: &Cluster, idx: usize, name: &str, note: &str) -> Result<(), String> {
    let changed = cluster
        .node(idx)
        .execute(
            "UPDATE live_typed_records SET note = $1 WHERE name = $2",
            &[&note, &name],
        )
        .await
        .map_err(|e| format!("node{} set note failed: {e}", idx + 1))?;
    if changed != 1 {
        return Err(format!("node{} set note hit {changed} rows", idx + 1));
    }
    cluster
        .node(idx)
        .flush()
        .await
        .map_err(|e| format!("node{} flush after set note failed: {e}", idx + 1))?;
    Ok(())
}

/// Column presence (Go: schema-manifest epoch assertions; established port
/// pattern). Absent = epoch 1, present = epoch 2. `PRAGMA table_info` is on
/// the server's side-effect-free inspection allowlist.
async fn has_column(cluster: &Cluster, idx: usize, table: &str, column: &str) -> bool {
    match cluster.node(idx).simple(&format!("PRAGMA table_info({table})")).await {
        Ok(rows) => rows.iter().any(|r| r.get(1).is_some_and(|n| n == column)),
        Err(_) => false,
    }
}

/// Schema epoch of one node (Go: `schema_epoch` in `/v1/status`).
async fn epoch(cluster: &Cluster, idx: usize) -> u64 {
    if has_column(cluster, idx, TABLE, "note").await {
        2
    } else {
        1
    }
}

/// Require an exact epoch (Go: `assertEpoch`).
async fn assert_epoch(cluster: &Cluster, idx: usize, want: u64) -> Result<(), String> {
    let got = epoch(cluster, idx).await;
    if got != want {
        return Err(format!("node{} schema epoch = {got}, want {want}", idx + 1));
    }
    Ok(())
}

/// Note read that surfaces SQL errors (Go: `TypedNote`).
async fn try_read_note(cluster: &Cluster, idx: usize, name: &str) -> Result<Option<String>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT note FROM live_typed_records WHERE name = $1", &[&name])
        .await
        .map_err(|e| format!("{e:?}"))?;
    match rows.first() {
        Some(row) => row
            .try_get::<_, Option<String>>(0)
            .map_err(|e| format!("{e:?}")),
        None => Ok(None),
    }
}

/// Apply the additive migration unless the column is already present
/// (Go `MigrateTypedRecords` is an idempotent manifest bind/rebind; Rust
/// DDL errors on a duplicate column, so adoption is checked first).
async fn migrate_unless_adopted(cluster: &Cluster, idx: usize) -> Result<(), String> {
    if has_column(cluster, idx, TABLE, "note").await {
        return Ok(());
    }
    cluster
        .node(idx)
        .simple(NOTE_DDL)
        .await
        .map_err(|e| format!("migrate node{}: {e:?}", idx + 1))?;
    cluster
        .node(idx)
        .flush()
        .await
        .map_err(|e| format!("flush migration on node{}: {e:?}", idx + 1))?;
    Ok(())
}

/// Wait until every node holds exactly `want` (Go: `waitConverged`).
async fn wait_converged(cluster: &Cluster, want: &[String], timeout: Duration) -> Result<(), String> {
    let converged = wait_until_async(timeout, POLL, || async {
        for i in 0..cluster.len() {
            match names(cluster, i).await {
                Ok(got) if got == want => {}
                _ => return false,
            }
        }
        true
    })
    .await;
    if !converged {
        return Err(format!(
            "nodes did not converge on {} managed record names within {timeout:?}",
            want.len()
        ));
    }
    Ok(())
}

/// Wait until the listed nodes adopt `want` (Go: `waitEpoch`).
async fn wait_epoch(cluster: &Cluster, nodes: &[usize], want: u64, timeout: Duration) -> Result<(), String> {
    let converged = wait_until_async(timeout, POLL, || async {
        for i in nodes {
            if epoch(cluster, *i).await != want {
                return false;
            }
        }
        true
    })
    .await;
    if !converged {
        return Err(format!(
            "nodes {nodes:?} did not adopt schema epoch {want} within {timeout:?}"
        ));
    }
    Ok(())
}

/// One crash-boundary case (Go: `runSchemaCrashCase`). When
/// `migrate_before_restart` is false the isolated node stops before the
/// store (recovery reopens at epoch 1 and retries); when true it stores
/// the migration first (recovery reopens at epoch 2).
async fn run_schema_crash_case(phase: &str, migrate_before_restart: bool) {
    let mut cluster = Cluster::new(ClusterOptions {
        name: format!("migration-crash-{phase}"),
        num_nodes: 3,
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    for i in 0..cluster.len() {
        let info = cluster.node_info(i);
        eprintln!(
            "{} dir={} sql={} repl={}",
            info.label,
            info.dir.display(),
            info.sql_addr,
            info.repl_addr
        );
    }
    let result = run_body(&mut cluster, phase, migrate_before_restart).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster
            .dump_forensics(&format!("migration-crash-{phase}-no-convergence"))
            .await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

async fn run_body(
    cluster: &mut Cluster,
    phase: &str,
    migrate_before_restart: bool,
) -> Result<(), String> {
    // Same base schema on every node (Go: typed fixture tables), flagged
    // replicated on each node (row convergence needs the per-node flag).
    for i in 0..cluster.len() {
        cluster
            .ensure_migrated(i, TABLE_DDL)
            .await
            .map_err(|e| format!("migrate node{}: {e}", i + 1))?;
    }
    for i in 0..cluster.len() {
        cluster
            .node(i)
            .simple(&format!("MURMUR SET TABLE {TABLE} REPLICATED true"))
            .await
            .map_err(|e| format!("node{} SET TABLE REPLICATED: {e}", i + 1))?;
        cluster
            .node(i)
            .flush()
            .await
            .map_err(|e| format!("node{} flush after SET TABLE: {e}", i + 1))?;
    }

    // Seed on node 2, converge, all old.
    let mut want_names = Vec::with_capacity(SEED_ROWS);
    for i in 0..SEED_ROWS {
        let name = format!("seed-{i:02}");
        insert_row(cluster, 1, &name).await?;
        want_names.push(name);
    }
    want_names.sort();
    wait_converged(cluster, &want_names, SEED_TIMEOUT).await?;
    for idx in 0..cluster.len() {
        assert_epoch(cluster, idx, 1).await?;
    }

    // Keep the two old-schema peers connected to each other while
    // isolating node 1 so neither can observe its recovery early.
    for peer in [1, 2] {
        cluster
            .remove_peer(0, peer)
            .await
            .map_err(|e| format!("isolate node1 from node{}: {e:?}", peer + 1))?;
        cluster
            .remove_peer(peer, 0)
            .await
            .map_err(|e| format!("isolate node{} from node1: {e:?}", peer + 1))?;
    }

    // Crash at the schema-manifest store boundary: stop/restart the child
    // on either side of the synchronous store (see README).
    if migrate_before_restart {
        migrate_unless_adopted(cluster, 0).await?;
    }
    cluster.stop_node(0).await;
    cluster.start_node(0).await.map_err(|e| format!("restart node1: {e}"))?;
    let restart_epoch = if migrate_before_restart { 2 } else { 1 };
    assert_epoch(cluster, 0, restart_epoch).await?;
    for idx in [1, 2] {
        assert_epoch(cluster, idx, 1).await?;
    }
    eprintln!("node1 recovered at schema manifest {phase} boundary while both peers remained on epoch 1");

    // Recovered rows readable before reconnect.
    let got = names(cluster, 0).await?;
    if got != want_names {
        return Err(format!("recovered rows = {got:?}, want {want_names:?}"));
    }
    if restart_epoch == 1 {
        migrate_unless_adopted(cluster, 0).await?;
    }
    assert_epoch(cluster, 0, 2).await?;
    match try_read_note(cluster, 0, &want_names[0]).await? {
        None => {}
        Some(v) => {
            return Err(format!(
                "new field after local recovery = {v:?}; want empty (NULL) default"
            ))
        }
    }

    // Rejoin and converge (fresh ports after the restart need a re-mesh).
    for peer in [1, 2] {
        cluster
            .add_peer(0, peer)
            .await
            .map_err(|e| format!("rejoin node1 to node{}: {e:?}", peer + 1))?;
        cluster
            .add_peer(peer, 0)
            .await
            .map_err(|e| format!("rejoin node{} to node1: {e:?}", peer + 1))?;
    }
    cluster.sync_all().await.map_err(|e| format!("sync after rejoin: {e}"))?;
    wait_converged(cluster, &want_names, REJOIN_TIMEOUT).await?;
    wait_epoch(cluster, &[1, 2], 2, EPOCH_TIMEOUT).await?;
    for idx in [1, 2] {
        migrate_unless_adopted(cluster, idx).await?;
        if !has_column(cluster, idx, TABLE, "note").await {
            return Err(format!("node{} did not rebind to the adopted v2 schema", idx + 1));
        }
    }

    // A migrated-field write converges with both peers at epoch 2.
    set_note(cluster, 0, &want_names[0], &format!("{phase}-recovered")).await?;
    let want_note = format!("{phase}-recovered");
    let deadline = tokio::time::Instant::now() + FINAL_TIMEOUT;
    let (converged, last_state) = loop {
        let mut states = Vec::new();
        let mut ready = true;
        for idx in 0..cluster.len() {
            let ep = epoch(cluster, idx).await;
            let note = try_read_note(cluster, idx, &want_names[0]).await;
            states.push(format!("node{} epoch={ep} note={note:?}", idx + 1));
            let note_ok = matches!(&note, Ok(Some(v)) if v == &want_note);
            if ep != 2 || !note_ok {
                ready = false;
            }
        }
        if ready {
            break (true, format!("{states:?}"));
        }
        if tokio::time::Instant::now() >= deadline {
            break (false, format!("{states:?}"));
        }
        tokio::time::sleep(POLL).await;
    };
    if !converged {
        return Err(format!(
            "schema epoch and migrated field did not converge after {phase} crash: {last_state}"
        ));
    }
    eprintln!("{phase} schema-store crash recovered and converged with both peers");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn migrated_peer_crash_before_store_reopens_and_converges() {
    run_schema_crash_case("before-store", false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn migrated_peer_crash_after_store_reopens_and_converges() {
    run_schema_crash_case("after-store", true).await;
}
