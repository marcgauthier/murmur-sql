//! Rust port of the Go `txchunk-resume` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/txchunk-resume/`
//! (`txchunk_resume_test.go`). Three `murmur-server` processes mesh; a single
//! transaction larger than the replication frame travels as ~64 KiB
//! transaction chunks; the receiving node is stopped mid-transfer and
//! restarted, and the retry must complete with all-or-nothing visibility, the
//! applied watermark must never advance before completion, and all digests
//! must converge without any snapshot transfer. Plain SQL replaces the Go
//! typed-records HTTP API, process stop/restart replaces SIGKILL, and a
//! staged-gauge/head-start trigger replaces the inbound-byte kill trigger
//! (see the scenario README.md for the deviations).

use std::time::Duration;

use murmur_live_harness::{hex, Cluster, ClusterOptions, ReplicationOptions, SqliteOptions};

/// Role indices (Go: `sender`/`victim`/`witness`).
const SENDER: usize = 0;
const VICTIM: usize = 1;
const WITNESS: usize = 2;

/// Positive-control convergence deadline (Go: 120 s).
const POSITIVE_TIMEOUT: Duration = Duration::from_secs(120);
/// Baseline ack-settle deadline (Go: `waitHave` 15 s).
const HAVE_TIMEOUT: Duration = Duration::from_secs(15);
/// Ack-settle poll interval.
const HAVE_POLL: Duration = Duration::from_millis(200);
/// Ack value must hold this long to count as settled (Rust-only; Go reads an
/// absolute `local_seq`, which has no same-numbering Rust equivalent).
const HAVE_STABLE_WINDOW: Duration = Duration::from_secs(2);
/// Partition deadline on the victim (Go: 15 s).
const SPLIT_PEERS_TIMEOUT: Duration = Duration::from_secs(15);
/// Survivor trial-visibility deadline (Go: 60 s).
const SURVIVOR_TIMEOUT: Duration = Duration::from_secs(60);
/// Prefix-count poll interval (Go: 100 ms).
const PREFIX_POLL: Duration = Duration::from_millis(100);
/// Mid-transfer stop deadline (Go: 15 s byte-watch).
const KILL_TIMEOUT: Duration = Duration::from_secs(15);
/// Mid-transfer stop poll interval (Go: 5 ms).
const KILL_POLL: Duration = Duration::from_millis(5);
/// Post-restart mesh deadline (Go: `WaitNodeReady`; 30 s matches the
/// established Rust heal leg).
const HEAL_PEERS_TIMEOUT: Duration = Duration::from_secs(30);
/// Retry-completion deadline (Go: 120 s).
const RETRY_TIMEOUT: Duration = Duration::from_secs(120);
/// Retry-completion poll interval (Go: 25 ms).
const RETRY_POLL: Duration = Duration::from_millis(25);
/// Watermark-advance publication bound (Go: 10 s).
const PUBLICATION_BOUND: Duration = Duration::from_secs(10);
/// Reconvergence deadline after a too-slow/too-late attempt (Go: 60 s).
const ATTEMPT_CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Final convergence deadline (Go: 60 s).
const FINAL_TIMEOUT: Duration = Duration::from_secs(60);
/// Convergence poll interval (Go: 200 ms).
const CONVERGE_POLL: Duration = Duration::from_millis(200);
/// Topology poll interval (Go: 100 ms).
const TOPO_POLL: Duration = Duration::from_millis(100);
/// Post-heal delay before the stop lands when the staged gauge never fires
/// (the multi-megabyte trial transfer takes seconds, so a stop this early is
/// near-certainly mid-transfer; post-restart receipt validates the trial).
const HEAD_START: Duration = Duration::from_millis(1500);

/// `MURMUR SHOW REPLICATION` column indices (no header over `simple()`).
const REPL_PEERS: usize = 2;
const REPL_STAGED: usize = 4;
const REPL_APPLIED: usize = 5;
const REPL_COMMITTED: usize = 7;

/// `MURMUR SHOW RETENTION` column indices (no header over `simple()`).
const RET_NODE: usize = 0;
const RET_ORIGIN: usize = 3;
const RET_ACKED: usize = 4;

/// One replicated row: Go's `TypedContentionRow{ID, Name, Phone}`.
type BigRow = (String, String, String);

/// All rows on one node (Go: `Cluster.TypedContentionRows`).
async fn rows(cluster: &Cluster, idx: usize) -> Result<Vec<BigRow>, String> {
    let db_rows = cluster
        .node(idx)
        .query("SELECT id, name, phone FROM records", &[])
        .await
        .map_err(|e| format!("node{} SELECT rows: {e}", idx + 1))?;
    db_rows
        .iter()
        .map(|row| {
            let id: String = row
                .try_get(0)
                .map_err(|e| format!("node{} decode id: {e}", idx + 1))?;
            let name: String = row
                .try_get(1)
                .map_err(|e| format!("node{} decode name: {e}", idx + 1))?;
            let phone: String = row
                .try_get(2)
                .map_err(|e| format!("node{} decode phone: {e}", idx + 1))?;
            Ok((id, name, phone))
        })
        .collect()
}

/// Canonical digest of a row set (Go: ordered `TypedContentionDigest`).
fn digest(mut rows: Vec<BigRow>) -> String {
    rows.sort();
    format!("{rows:?}")
}

/// Row count on one node (cheap pre-check before full digest reads).
async fn row_count(cluster: &Cluster, idx: usize) -> Result<usize, String> {
    let db_rows = cluster
        .node(idx)
        .query("SELECT COUNT(*) FROM records", &[])
        .await
        .map_err(|e| format!("node{} SELECT count: {e}", idx + 1))?;
    let n: i64 = db_rows
        .first()
        .ok_or_else(|| format!("node{} count returned no rows", idx + 1))?
        .try_get(0)
        .map_err(|e| format!("node{} decode count: {e}", idx + 1))?;
    Ok(n as usize)
}

/// Rows on one node whose name starts with `prefix`
/// (Go: `TypedContentionPrefixCount`).
async fn prefix_count(cluster: &Cluster, idx: usize, prefix: &str) -> Result<usize, String> {
    // The length is inlined as a literal (injection-safe: `usize`): the
    // server reports expression-position parameters as type `Unknown` and
    // the driver refuses to serialize non-string Rust types for them
    // (`error serializing parameter 0`), so only `&str` params may sit in
    // positions the server cannot infer from a column.
    let db_rows = cluster
        .node(idx)
        .query(
            &format!(
                "SELECT COUNT(*) FROM records WHERE substr(name, 1, {}) = $1",
                prefix.len()
            ),
            &[&prefix],
        )
        .await
        .map_err(|e| format!("node{} prefix count: {e}", idx + 1))?;
    let n: i64 = db_rows
        .first()
        .ok_or_else(|| format!("node{} prefix count returned no rows", idx + 1))?
        .try_get(0)
        .map_err(|e| format!("node{} decode prefix count: {e}", idx + 1))?;
    Ok(n as usize)
}

/// Wait until one node holds exactly `want` prefixed rows
/// (Go: `waitPrefixCount`).
async fn wait_prefix_count(
    cluster: &Cluster,
    idx: usize,
    prefix: &str,
    want: usize,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if prefix_count(cluster, idx, prefix).await? == want {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "node{} {prefix:?} rows = {}, want {want} within {timeout:?}",
                idx + 1,
                prefix_count(cluster, idx, prefix).await?,
            ));
        }
        tokio::time::sleep(PREFIX_POLL).await;
    }
}

/// Wait until every node holds exactly `want` rows with equal digests
/// (Go: `waitConverged`). Counts are checked first so the multi-megabyte
/// digest reads only run once the counts already match (same pass
/// criteria, cheaper polling).
async fn wait_converged(
    cluster: &Cluster,
    want: usize,
    timeout: Duration,
    what: &str,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut counts = Vec::new();
        for i in 0..cluster.len() {
            counts.push(row_count(cluster, i).await?);
        }
        if counts.iter().all(|&n| n == want) {
            let mut first = String::new();
            let mut equal = true;
            for (i, idx) in (0..cluster.len()).enumerate() {
                let d = digest(rows(cluster, idx).await?);
                if i == 0 {
                    first = d;
                } else if d != first {
                    equal = false;
                    break;
                }
            }
            if equal {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            for i in 0..cluster.len() {
                let node_rows = rows(cluster, i).await?;
                eprintln!(
                    "node{} at timeout: count={} digest={}",
                    i + 1,
                    node_rows.len(),
                    digest(node_rows)
                );
            }
            return Err(format!(
                "nodes did not converge on {want} rows with equal digests within {timeout:?} ({what})"
            ));
        }
        tokio::time::sleep(CONVERGE_POLL).await;
    }
}

/// One `MURMUR SHOW REPLICATION` row (single-row output).
async fn show_replication(cluster: &Cluster, idx: usize) -> Result<Vec<String>, String> {
    cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node{} SHOW REPLICATION: {e}", idx + 1))?
        .into_iter()
        .next()
        .ok_or_else(|| format!("node{} SHOW REPLICATION returned no rows", idx + 1))
}

fn show_col(row: &[String], idx: usize, col: usize, what: &str) -> Result<u64, String> {
    row.get(col)
        .ok_or_else(|| format!("node{} SHOW REPLICATION has no {what} column", idx + 1))?
        .parse::<u64>()
        .map_err(|e| format!("node{} parse {what}: {e}", idx + 1))
}

/// Wait until one node reports exactly `want` replication peers
/// (Go: `waitConnectedPeers` over `/v1/status connected_peers`).
async fn wait_connected_peers(
    cluster: &Cluster,
    idx: usize,
    want: usize,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let row = show_replication(cluster, idx).await?;
        let peers = show_col(&row, idx, REPL_PEERS, "peers")? as usize;
        if peers == want {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "node{} connected peers = {peers}, want {want} within {timeout:?}",
                idx + 1,
            ));
        }
        tokio::time::sleep(TOPO_POLL).await;
    }
}

/// The sender's record of a peer's applied watermark for one origin
/// (Go: `peerHave` over `/v1/debug/peers Have`), or 0 when no record
/// exists yet.
async fn peer_have(
    cluster: &Cluster,
    sender: usize,
    peer: [u8; 16],
    origin: [u8; 16],
) -> Result<u64, String> {
    let (peer_hex, origin_hex) = (hex(&peer), hex(&origin));
    let rows = cluster
        .node(sender)
        .simple("MURMUR SHOW RETENTION")
        .await
        .map_err(|e| format!("node{} SHOW RETENTION: {e}", sender + 1))?;
    for row in &rows {
        let node_match = row.get(RET_NODE).is_some_and(|n| n == &peer_hex);
        let origin_match = row.get(RET_ORIGIN).is_some_and(|o| o == &origin_hex);
        if node_match && origin_match {
            let ack = row
                .get(RET_ACKED)
                .ok_or_else(|| format!("node{} SHOW RETENTION has no acknowledged column", sender + 1))?;
            if ack.is_empty() {
                return Ok(0);
            }
            return ack
                .parse::<u64>()
                .map_err(|e| format!("node{} parse acknowledged: {e}", sender + 1));
        }
    }
    Ok(0)
}

/// Quiet-baseline ack for one origin: the mesh is converged at every call
/// site, so the current ack settles at the sender's head within
/// milliseconds; require it to hold still across `HAVE_STABLE_WINDOW`
/// before treating it as the absolute baseline (Go compares against an
/// absolute `local_seq`, which has no same-numbering Rust equivalent).
async fn wait_have_settled(
    cluster: &Cluster,
    sender: usize,
    peer: [u8; 16],
    origin: [u8; 16],
) -> Result<u64, String> {
    let deadline = tokio::time::Instant::now() + HAVE_TIMEOUT;
    let mut current = peer_have(cluster, sender, peer, origin).await?;
    let mut stable_since = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now().duration_since(stable_since) >= HAVE_STABLE_WINDOW {
            return Ok(current);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "sender view of peer watermark for origin did not settle within {HAVE_TIMEOUT:?}"
            ));
        }
        tokio::time::sleep(HAVE_POLL).await;
        let next = peer_have(cluster, sender, peer, origin).await?;
        if next != current {
            current = next;
            stable_since = tokio::time::Instant::now();
        }
    }
}

/// Currently staged (received, not yet applied) transactions on one node:
/// a gauge, used as the transfer-start signal (Go watches the victim's
/// inbound frame bytes, which have no Rust equivalent).
async fn staged(cluster: &Cluster, idx: usize) -> Result<u64, String> {
    let row = show_replication(cluster, idx).await?;
    show_col(&row, idx, REPL_STAGED, "staged_transactions")
}

/// Cumulative log-applied transactions on one node. Snapshot application
/// bypasses this counter, so post-restart advancement proves the trial
/// arrived and applied as a log transaction, not as a snapshot.
/// (`received_transactions` has no wire equivalent: `MURMUR SHOW METRICS`
/// only covers SQLite/Spool.)
async fn applied(cluster: &Cluster, idx: usize) -> Result<u64, String> {
    let row = show_replication(cluster, idx).await?;
    show_col(&row, idx, REPL_APPLIED, "applied_transactions")
}

/// Local commit cursor (Go: `nodeStatus LocalSeq` advance check).
async fn committed(cluster: &Cluster, idx: usize) -> Result<u64, String> {
    let row = show_replication(cluster, idx).await?;
    show_col(&row, idx, REPL_COMMITTED, "committed")
}

/// Commit one multi-megabyte transaction: `rows` inserts with large `phone`
/// values (Go: `buildBigRows` + `TypedContentionInsertMany`). Large `phone`
/// values carry the payload; `name` carries the prefix. Fails loudly
/// unless the payload exceeds the 8 MiB replication frame (Go parity; the
/// Rust engine chunks transactions at 1 MiB, so this trips chunking with
/// wide margin).
async fn commit_big_tx(
    cluster: &Cluster,
    node: usize,
    prefix: &str,
    id_base: i64,
    rows: usize,
    value_bytes: usize,
) -> Result<(), String> {
    let mut values: Vec<BigRow> = Vec::with_capacity(rows);
    let mut payload_bytes = 0usize;
    for i in 0..rows {
        let mut row = vec![0u8; value_bytes];
        row[..8].copy_from_slice(b"payload-");
        for (j, b) in row.iter_mut().enumerate().skip(8) {
            *b = b'a' + ((id_base + i as i64 + j as i64) % 26) as u8;
        }
        let phone = String::from_utf8(row).expect("ascii payload");
        let id = format!("{:032x}", id_base + i as i64);
        let name = format!("{prefix}{i:06}");
        payload_bytes += id.len() + name.len() + phone.len() + 8;
        values.push((id, name, phone));
    }
    let payload_mb = payload_bytes as f64 / (1 << 20) as f64;
    eprintln!("big transaction {payload_mb:.1} MiB ({rows} rows x {value_bytes} B values)");
    if payload_bytes <= 8 << 20 {
        return Err(format!(
            "big transaction is {payload_bytes} bytes, want > 8 MiB so it must travel chunked"
        ));
    }
    // One transaction over the shared connection: the driver is sequential,
    // so nothing else queries mid-transaction.
    let client = cluster.node(node).inner();
    client
        .batch_execute("BEGIN")
        .await
        .map_err(|e| format!("node{} big-tx BEGIN ({prefix}): {e}", node + 1))?;
    for (id, name, phone) in &values {
        client
            .execute(
                "INSERT INTO records(id, name, phone) VALUES($1, $2, $3)",
                &[id, name, phone],
            )
            .await
            .map_err(|e| format!("node{} big-tx insert ({prefix}): {e}", node + 1))?;
    }
    client
        .batch_execute("COMMIT")
        .await
        .map_err(|e| format!("node{} big-tx COMMIT ({prefix}): {e}", node + 1))?;
    cluster
        .node(node)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("node{} flush after big-tx ({prefix}): {e}", node + 1))?;
    Ok(())
}

/// Isolate `victim` from both peers in both directions (Go: `partition`).
async fn partition(cluster: &Cluster, victim: usize) -> Result<(), String> {
    for peer in 0..cluster.len() {
        if peer == victim {
            continue;
        }
        cluster
            .remove_peer(victim, peer)
            .await
            .map_err(|e| format!("remove peer {victim}->{peer}: {e}"))?;
        cluster
            .remove_peer(peer, victim)
            .await
            .map_err(|e| format!("remove peer {peer}->{victim}: {e}"))?;
    }
    Ok(())
}

/// Rejoin `victim` with both peers in both directions (Go: `heal`).
async fn heal(cluster: &Cluster, victim: usize) -> Result<(), String> {
    for peer in 0..cluster.len() {
        if peer == victim {
            continue;
        }
        cluster
            .add_peer(victim, peer)
            .await
            .map_err(|e| format!("add peer {victim}->{peer}: {e}"))?;
        cluster
            .add_peer(peer, victim)
            .await
            .map_err(|e| format!("add peer {peer}->{victim}: {e}"))?;
    }
    Ok(())
}

/// Stop the victim once the trial transfer to it is provably underway but
/// incomplete: stop as soon as the victim stages anything while showing none
/// of the trial rows, or once the head-start elapses (the multi-megabyte
/// trial takes seconds to transfer, so a stop this early lands mid-transfer).
/// Returns true when the victim converged first (the stop would prove
/// nothing; the caller retries with fresh rows), mirroring Go's
/// `killOnceBytesFlow`, which keys on inbound frame bytes instead.
async fn stop_once_push_flows(
    cluster: &mut Cluster,
    prefix: &str,
    rows: usize,
) -> Result<bool, String> {
    let start = tokio::time::Instant::now();
    let deadline = start + KILL_TIMEOUT;
    loop {
        if tokio::time::Instant::now() >= deadline {
            let n = prefix_count(cluster, VICTIM, prefix).await?;
            return Err(format!(
                "no push to the victim observed within {KILL_TIMEOUT:?} (trial rows visible: {n}/{rows})"
            ));
        }
        if prefix_count(cluster, VICTIM, prefix).await? == rows {
            return Ok(true);
        }
        if staged(cluster, VICTIM).await? > 0 {
            eprintln!("stopping victim mid-transfer: trial staged but not yet visible");
            cluster.stop_node(VICTIM).await;
            return Ok(false);
        }
        if start.elapsed() >= HEAD_START {
            eprintln!("stopping victim {HEAD_START:?} after the heal: multi-megabyte trial is mid-transfer");
            cluster.stop_node(VICTIM).await;
            return Ok(false);
        }
        tokio::time::sleep(KILL_POLL).await;
    }
}

/// Restart the victim in place and rejoin the mesh (Go: `restartNode`).
async fn restart_node(cluster: &mut Cluster, idx: usize) -> Result<(), String> {
    cluster
        .start_node(idx)
        .await
        .map_err(|e| format!("node{idx} restart failed: {e}"))?;
    // The restarted node rebinds fresh ports: re-mesh so every
    // ordered pair dials the current addresses.
    cluster
        .mesh()
        .await
        .map_err(|e| format!("re-mesh after restart: {e}"))?;
    let len = cluster.len();
    for i in 0..len {
        wait_connected_peers(cluster, i, len - 1, HEAL_PEERS_TIMEOUT).await?;
    }
    Ok(())
}

/// Poll the retry to completion and enforce the resume contract on every
/// sample: the sender always shows the full trial (its commit was durable)
/// and the victim shows none or all of it (never a partial transaction).
/// The victim's applied watermark for the trial origin (as acked to the
/// sender) advances on durable apply; the first advanced-watermark sample
/// must precede full visibility by at most the publication bound.
/// Returns the victim's post-restart applied counter (Go: `watchRetry`).
/// Counters reset on restart, so any post-restart value is retry activity.
async fn watch_retry(
    cluster: &Cluster,
    prefix: &str,
    rows: usize,
    have_pre: u64,
) -> Result<u64, String> {
    let victim_id = cluster.node_id(VICTIM);
    let sender_id = cluster.node_id(SENDER);
    let deadline = tokio::time::Instant::now() + RETRY_TIMEOUT;
    let mut first_advanced: Option<tokio::time::Instant> = None;
    let mut first_full: Option<tokio::time::Instant> = None;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "retry did not complete within {RETRY_TIMEOUT:?} (victim rows: {}/{rows}, watermark: {}, want >= {})",
                prefix_count(cluster, VICTIM, prefix).await?,
                peer_have(cluster, SENDER, victim_id, sender_id).await?,
                have_pre + 1,
            ));
        }
        let now = tokio::time::Instant::now();
        let have = peer_have(cluster, SENDER, victim_id, sender_id).await?;
        let vis_victim = prefix_count(cluster, VICTIM, prefix).await?;
        let vis_sender = prefix_count(cluster, SENDER, prefix).await?;
        let applied_now = applied(cluster, VICTIM).await?;
        if vis_sender != rows {
            return Err(format!(
                "sender shows {vis_sender}/{rows} trial rows after committing them"
            ));
        }
        if vis_victim != 0 && vis_victim != rows {
            return Err(format!(
                "victim shows {vis_victim}/{rows} trial rows: partial transaction visible"
            ));
        }
        if have > have_pre && first_advanced.is_none() {
            first_advanced = Some(now);
        }
        if vis_victim == rows && first_full.is_none() {
            first_full = Some(now);
        }
        if vis_victim == rows && have >= have_pre + 1 {
            match (first_advanced, first_full) {
                (Some(advanced), Some(full)) => {
                    let lag = full.saturating_duration_since(advanced);
                    eprintln!("watermark advanced {lag:?} before full trial visibility");
                    if lag > PUBLICATION_BOUND {
                        return Err(format!(
                            "watermark advanced {lag:?} before completion, want <= {PUBLICATION_BOUND:?} (early advance)"
                        ));
                    }
                }
                (advanced, full) => {
                    return Err(format!(
                        "converged without observing watermark advance (advanced={advanced:?} full={full:?})"
                    ));
                }
            }
            return Ok(applied_now);
        }
        tokio::time::sleep(RETRY_POLL).await;
    }
}

/// Positive-integer env knob with fallback (Go: `envInt`).
fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

async fn run_body(cluster: &mut Cluster, rows_per_tx: usize, value_bytes: usize, max_attempts: usize) -> Result<(), String> {
    let victim_id = cluster.node_id(VICTIM);
    let sender_id = cluster.node_id(SENDER);

    // Same schema on every node (Go: TypedRecords fixture tables), then
    // replicate it: row convergence needs the per-table flag on each node.
    for i in 0..cluster.len() {
        cluster
            .ensure_migrated(
                i,
                "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL)",
            )
            .await
            .map_err(|e| format!("migrate node{}: {e}", i + 1))?;
    }
    for i in 0..cluster.len() {
        cluster
            .node(i)
            .simple("MURMUR SET TABLE records REPLICATED true")
            .await
            .map_err(|e| format!("node{} SET TABLE REPLICATED: {e}", i + 1))?;
        cluster
            .node(i)
            .simple("MURMUR FLUSH")
            .await
            .map_err(|e| format!("node{} flush after SET TABLE: {e}", i + 1))?;
    }

    // Positive control (honest behavior works): a multi-chunk
    // transaction replicates to every node with no stopping.
    commit_big_tx(cluster, SENDER, "base-", 0, rows_per_tx, value_bytes).await?;
    wait_converged(cluster, rows_per_tx, POSITIVE_TIMEOUT, "positive control").await?;
    eprintln!("positive control: multi-chunk transaction replicated to all nodes");

    // Resume trials. Each attempt commits a fresh big transaction
    // while the victim is partitioned, heals, stops the victim once
    // the trial transfer to it starts flowing, restarts it, and
    // watches the retry.
    let mut expected_rows = rows_per_tx;
    let mut resumed = false;
    for attempt in 1..=max_attempts {
        if resumed {
            break;
        }
        let prefix = format!("a{attempt}-");
        eprintln!("resume attempt {attempt}/{max_attempts}");

        // Quiet baseline BEFORE partitioning: the victim must have
        // acked everything the sender has before the trial starts.
        // The mesh is converged at every loop top (positive control
        // or the previous attempt's reconvergence wait).
        let seq_pre = committed(cluster, SENDER).await?;
        let have_pre = wait_have_settled(cluster, SENDER, victim_id, sender_id).await?;

        partition(cluster, VICTIM).await?;
        wait_connected_peers(cluster, VICTIM, 0, SPLIT_PEERS_TIMEOUT).await?;

        commit_big_tx(
            cluster,
            SENDER,
            &prefix,
            attempt as i64 * 1_000_000,
            rows_per_tx,
            value_bytes,
        )
        .await?;
        let seq_tx = committed(cluster, SENDER).await?;
        if seq_tx <= seq_pre {
            return Err(format!(
                "attempt {attempt}: sender commit cursor did not advance ({seq_pre} -> {seq_tx})"
            ));
        }
        expected_rows += rows_per_tx;

        // Survivors converge on the trial rows; the victim must miss them.
        wait_prefix_count(cluster, SENDER, &prefix, rows_per_tx, SURVIVOR_TIMEOUT).await?;
        wait_prefix_count(cluster, WITNESS, &prefix, rows_per_tx, SURVIVOR_TIMEOUT).await?;
        let leaked = prefix_count(cluster, VICTIM, &prefix).await?;
        if leaked != 0 {
            return Err(format!(
                "attempt {attempt}: victim sees {leaked} trial rows while partitioned, want 0"
            ));
        }

        // Heal, then stop once the trial transfer to the victim is
        // flowing.
        heal(cluster, VICTIM).await?;
        if stop_once_push_flows(cluster, &prefix, rows_per_tx).await? {
            eprintln!("attempt {attempt}: victim converged before the stop landed; retrying with fresh rows");
            wait_converged(cluster, expected_rows, ATTEMPT_CONVERGE_TIMEOUT, "too-slow reconverge").await?;
            continue;
        }
        restart_node(cluster, VICTIM).await?;

        let applied_post = watch_retry(cluster, &prefix, rows_per_tx, have_pre).await?;
        eprintln!("attempt {attempt}: victim applied {applied_post} transaction(s) after restart");
        if applied_post < 1 {
            // The victim applied nothing after restarting, so it must
            // have applied the trial before the stop: the trial proves
            // nothing about resume; retry fresh.
            eprintln!("attempt {attempt}: no post-restart apply; stop landed too late, retrying");
            wait_converged(cluster, expected_rows, ATTEMPT_CONVERGE_TIMEOUT, "too-late reconverge").await?;
            continue;
        }
        resumed = true;

        // The retry must have used the log/chunk path, not snapshots:
        // snapshot application bypasses the applied counter, so
        // post-restart apply proves log-path resume. The engine only
        // requests snapshots from explicitly trusted sources, and this
        // cluster never configures any, so no snapshot could have been
        // taken anywhere. (`chunk_need_sent/served` resume-path
        // diagnostics have no wire equivalent.)
    }
    if !resumed {
        return Err(format!(
            "no valid resume trial in {max_attempts} attempts (stop never landed before convergence)"
        ));
    }

    // Final: exact row counts and identical ordered digests everywhere.
    wait_converged(cluster, expected_rows, FINAL_TIMEOUT, "final").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chunked_transaction_resumes_after_receiver_restart() {
    let rows = env_usize("MURMUR_TXCHUNK_ROWS", 1200);
    let value_bytes = env_usize("MURMUR_TXCHUNK_VALUE_BYTES", 9000);
    let max_attempts = env_usize("MURMUR_TXCHUNK_ATTEMPTS", 3);
    // Single-unit shipping: the background capture tick (1 s) would
    // stream the unflushed trial as incremental captures and the
    // default 4 MiB batch unit would sign/apply a 10 MiB session as
    // three units, so the trial would converge incrementally and the
    // all-or-nothing resume contract could never hold. Stretching the
    // tick past the trial and raising the batch unit past the trial
    // size restores Go's one-transaction shape: each trial ships on
    // its flush as one ~10 MiB signed transaction (still chunked over
    // the wire past the 1 MiB engine threshold) and applies atomically
    // (see the README deviation).
    let mut cluster = Cluster::new(ClusterOptions {
        name: "txchunk-resume".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            batch_bytes: Some(16 << 20),
            ..Default::default()
        }),
        sqlite: Some(SqliteOptions {
            capture_interval: Some(Duration::from_secs(3600)),
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    let result = run_body(&mut cluster, rows, value_bytes, max_attempts).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("txchunk-resume-no-convergence").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
