//! Rust port of the Go `open-progress` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/open-progress/`
//! (`open_progress_test.go`: `TestEmbeddedOpenProgressLive` and
//! `TestEmbeddedOpenProgressLiveExtra`). The Go test records in-process
//! `on_open_progress` callbacks across reopens; over the PostgreSQL wire
//! there is no intra-open progress API, so this port keeps the wire-level
//! intent: a real encrypted store holding 6000 rows x 2 tables (~30 MB) is
//! populated, closed and reopened under the unlock watchdog, and every
//! reopen must recover identical contents. Failed unlocks (wrong key,
//! short key) must report the failure without a handle, the store must
//! recover afterwards, and a fresh empty database must open. Phase
//! sequences, heartbeats, per-table tracking, per-phase timing and
//! status-snapshot isolation have no wire counterpart and are dropped
//! (see the scenario README.md).

use std::time::{Duration, Instant};

use murmur_live_harness::{unlock, Cluster, ClusterOptions};

/// Rows per table (Go: `rowsPerTable = 6000`).
const ROWS_PER_TABLE: i64 = 6000;
/// Application key (Go: `bytes.Repeat([]byte{0x63}, 32)`).
const KEY: [u8; 32] = [0x63; 32];
/// Wrong 32-byte key for the open-failure leg.
const WRONG_KEY: [u8; 32] = [0x44; 32];
/// Short key for the open-failure leg (Go: `[]byte{1, 2, 3}`).
const SHORT_KEY_HEX: &str = "010203";
/// PG credentials the harness provisions (`admin` / `test-password`;
/// see `harness/src/cluster.rs`).
const PG_USER: &str = "admin";
const PG_PASSWORD: &str = "test-password";

/// Migrate both tables on the single node (Go: the `populate` role schema).
async fn migrate(cluster: &Cluster) -> Result<(), String> {
    for table in ["requests", "events"] {
        cluster
            .ensure_migrated(
                0,
                &format!(
                    "CREATE TABLE {table}(id INTEGER PRIMARY KEY, message TEXT NOT NULL, ordinal INTEGER NOT NULL)"
                ),
            )
            .await
            .map_err(|e| format!("migrate {table}: {e}"))?;
    }
    Ok(())
}

/// Fill both tables (Go: the `populate` role). One durability barrier at
/// the end; rows are visible locally as they commit.
async fn populate(cluster: &Cluster) -> Result<(), String> {
    for table in ["requests", "events"] {
        for i in 0..ROWS_PER_TABLE {
            let message = format!("realistic diagnostic {i} {}", "trace ".repeat(512));
            cluster
                .node(0)
                .execute(
                    &format!("INSERT INTO {table}(id, message, ordinal) VALUES($1, $2, $3)"),
                    &[&i, &message, &i],
                )
                .await
                .map_err(|e| format!("populate: insert {table} {i}: {e}"))?;
        }
    }
    cluster
        .node(0)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("populate: flush: {e}"))?;
    eprintln!("populate: {ROWS_PER_TABLE} rows x2 tables");
    Ok(())
}

/// Count + ordinal-sum check per table (Go: `verifyProgressTable`).
async fn verify_contents(cluster: &Cluster, name: &str) -> Result<(), String> {
    for table in ["requests", "events"] {
        let rows = cluster
            .node(0)
            .simple(&format!("SELECT COUNT(*), COALESCE(SUM(ordinal), 0) FROM {table}"))
            .await
            .map_err(|e| format!("{name}: verify {table}: {e}"))?;
        let row = rows
            .first()
            .ok_or_else(|| format!("{name}: verify {table}: no rows"))?;
        let count: i64 = row
            .first()
            .and_then(|c| c.parse().ok())
            .ok_or_else(|| format!("{name}: verify {table}: bad count {row:?}"))?;
        let sum: i64 = row
            .get(1)
            .and_then(|c| c.parse().ok())
            .ok_or_else(|| format!("{name}: verify {table}: bad sum {row:?}"))?;
        if count != ROWS_PER_TABLE {
            return Err(format!("{name}: wrong count {table}: {count}"));
        }
        if sum != ROWS_PER_TABLE * (ROWS_PER_TABLE - 1) / 2 {
            return Err(format!("{name}: wrong contents {table}: sum={sum}"));
        }
    }
    Ok(())
}

/// Connect through the Rust client's SQL unlock path.
async fn connect_key(cluster: &Cluster, key: &str) -> murmur_live_harness::murmur_client::Result<murmur_live_harness::murmur_client::Client> {
    let addr = cluster.node_info(0).sql_addr;
    let mut config = murmur_live_harness::murmur_client::Config::new(PG_USER, PG_PASSWORD, key);
    config.host = addr.ip().to_string();
    config.port = addr.port();
    config.connect_timeout = Duration::from_secs(5);
    murmur_live_harness::murmur_client::Client::connect(&config).await
}

/// Reload leg (Go: the `reload` role): stop + restart under the unlock
/// watchdog, then contents must verify exactly.
async fn reload_and_verify(cluster: &mut Cluster, name: &str) -> Result<(), String> {
    let start = Instant::now();
    cluster.stop_node(0).await;
    cluster.start_node(0).await.map_err(|e| format!("{name}: restart under watchdog: {e}"))?;
    eprintln!("{name}: reopened under watchdog in {:?}", start.elapsed());
    verify_contents(cluster, name).await?;
    eprintln!("PASS {name}: contents identical after reopen");
    Ok(())
}

/// Open-failure leg (Go: the `open-failure` role): the unlock must fail,
/// no usable handle is returned, and the correct key still works after.
async fn open_failure_legs(cluster: &Cluster) -> Result<(), String> {
    for key in [murmur_live_harness::hex(&WRONG_KEY), SHORT_KEY_HEX.into()] {
        match connect_key(cluster, &key).await {
            Ok(_) => return Err("wrong unlock key unexpectedly accepted".into()),
            Err(error) => {
                let message = murmur_live_harness::db_message(&error);
                if error.code() != Some("28000") || !message.contains("incorrect key") {
                    return Err(format!("unlock failure did not identify wrong key: {message}"));
                }
            }
        }
    }
    let client = connect_key(cluster, &murmur_live_harness::hex(&KEY)).await
        .map_err(|e| format!("correct key after failures: {e}"))?;
    client.simple("SELECT 1").await.map_err(|e| e.to_string())?;
    eprintln!("PASS open-failure: wrong/short keys rejected, correct key accepted");
    Ok(())
}

/// Watchdog leg: an unlock that can never succeed must fail closed after
/// the stall window instead of hanging. The stall knob is tightened for
/// this leg only and restored afterwards.
async fn watchdog_leg(cluster: &Cluster) -> Result<(), String> {
    const STALL_ENV: &str = "MURMUR_LIVE_UNLOCK_STALL_SECONDS";
    let saved = std::env::var(STALL_ENV).ok();
    std::env::set_var(STALL_ENV, "2");
    let addr = cluster.node_info(0).sql_addr;
    let start = Instant::now();
    let outcome = unlock::connect_with_watchdog(addr, PG_USER, PG_PASSWORD, &murmur_live_harness::hex(&WRONG_KEY)).await;
    match saved {
        Some(v) => std::env::set_var(STALL_ENV, v),
        None => std::env::remove_var(STALL_ENV),
    }
    match outcome {
        Ok(_) => Err("watchdog: unlock with wrong key unexpectedly succeeded".into()),
        Err(e) => {
            eprintln!("PASS watchdog: unopenable unlock failed closed in {:?}: {e}", start.elapsed());
            Ok(())
        }
    }
}

/// Empty-database leg (Go: the `empty-db` role): a fresh database opens
/// under the watchdog with zero rows and answers queries.
async fn empty_db_leg() -> Result<(), String> {
    let cluster = Cluster::new(ClusterOptions {
        name: "open-progress-empty".into(),
        num_nodes: 1,
        key: KEY,
        ..Default::default()
    })
    .await
    .map_err(|e| format!("empty-db cluster setup: {e}"))?;
    let result = async {
        cluster
            .ensure_migrated(0, "CREATE TABLE probe(id INTEGER PRIMARY KEY)")
            .await
            .map_err(|e| format!("empty-db migrate: {e}"))?;
        let rows = cluster
            .node(0)
            .simple("SELECT COUNT(*) FROM probe")
            .await
            .map_err(|e| format!("empty-db count: {e}"))?;
        let count = rows.first().and_then(|r| r.first().cloned()).unwrap_or_default();
        if count != "0" {
            return Err(format!("empty-db: fresh table holds {count} rows, want 0"));
        }
        eprintln!("PASS empty-db: fresh database opened under watchdog with zero rows");
        Ok::<(), String>(())
    }
    .await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("open-progress-empty").await;
    }
    cluster.finish(failed).await;
    result
}

async fn run_body(cluster: &mut Cluster) -> Result<(), String> {
    // Main flow (Go: populate, reload, open-failure, reload). The cancel
    // role has no counterpart: opens over the wire cannot be cancelled.
    migrate(cluster).await?;
    populate(cluster).await?;
    verify_contents(cluster, "populate").await?;
    reload_and_verify(cluster, "reload-1").await?;
    open_failure_legs(cluster).await?;
    watchdog_leg(cluster).await?;
    reload_and_verify(cluster, "reload-2-recovery").await?;

    // Fresh empty database opens under the watchdog.
    empty_db_leg().await?;

    eprintln!("PASS open-progress: all legs green");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_progress() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "open-progress".into(),
        num_nodes: 1,
        key: KEY,
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 1, "this scenario is written for 1 node");
    let result = run_body(&mut cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("open-progress-failed").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
