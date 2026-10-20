//! Rust port of the Go `cert-lifecycle` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/cert-lifecycle/`
//! (`cert_lifecycle_test.go`). A node's certificate is swapped for a
//! short-expiry cert minted for the same NodeID; the mesh must stay
//! connected pre-expiry. After NotAfter passes, fresh handshakes must
//! reject the node (no session, `peers` 1->0 on the survivor, markers
//! never cross during the isolation window), with the served material
//! proven genuinely expired at assert time. Rotating to a fresh cert must
//! fully reconverge the mesh. Plain SQL replaces the Go typed-records HTTP
//! API, and QUIC session state replaces `debug/peers` / `connected_peers`
//! (see the scenario README.md for the deviations).

use std::time::{Duration, Instant};

use murmur_live_harness::tokio_postgres::types::ToSql;
use murmur_live_harness::{env_seconds, hex, Cluster, ClusterOptions};

/// Roles (Go: `a, b := 0, 1`).
const A: usize = 0;
const B: usize = 1;
/// Convergence poll interval (Go: 100 ms).
const POLL_FAST: Duration = Duration::from_millis(100);
/// Session poll interval (Go: 200 ms).
const POLL: Duration = Duration::from_millis(200);
/// Isolation-window poll (Go: 500 ms).
const WINDOW_POLL: Duration = Duration::from_millis(500);
/// Convergence deadlines (Go: 60 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Session-establishment deadline (Go: 30 s pre-expiry checks).
const PEER_TIMEOUT: Duration = Duration::from_secs(30);
/// Rejection deadlines (Go: 45 s).
const REJECT_TIMEOUT: Duration = Duration::from_secs(45);
/// Pre-expiry safety margin before NotAfter (Go: 5 s).
const PRE_EXPIRY_MARGIN: Duration = Duration::from_secs(5);
/// Post-expiry settle past NotAfter before forcing fresh handshakes (Go:
/// 2 s).
const POST_EXPIRY_SETTLE: Duration = Duration::from_secs(2);

/// Sorted names on one node (Go: `TypedNames`).
async fn names(cluster: &Cluster, idx: usize) -> Result<Vec<String>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT name FROM records ORDER BY name", &[])
        .await
        .map_err(|e| format!("node{} SELECT names: {e}", idx + 1))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(
            row.try_get::<_, String>(0)
                .map_err(|e| format!("node{} decode name: {e}", idx + 1))?,
        );
    }
    Ok(out)
}

/// Insert one marker row (Go: `TypedInsert`).
async fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) -> Result<(), String> {
    let params = [&id as &(dyn ToSql + Sync), &name as _];
    cluster
        .node(node)
        .execute("INSERT INTO records(id, name) VALUES($1, $2)", &params)
        .await
        .map_err(|e| format!("node{} insert '{name}': {e}", node + 1))?;
    cluster
        .node(node)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("node{} flush after '{name}': {e}", node + 1))?;
    Ok(())
}

/// Wait until both nodes hold exactly `want` rows with equal ordered names
/// (Go: `waitConverged`).
async fn wait_converged(cluster: &Cluster, want: &[String], timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if names(cluster, A).await? == *want && names(cluster, B).await? == *want {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("nodes did not converge on {want:?} within {timeout:?}"));
        }
        tokio::time::sleep(POLL_FAST).await;
    }
}

/// Whether `from` currently holds an open session to `target` (Go:
/// `debug/peers` `Connected`; here `MURMUR SHOW PEERS`).
async fn peer_connected(cluster: &Cluster, from: usize, target: usize) -> Result<bool, String> {
    let want = hex(&cluster.node_id(target));
    let rows = cluster
        .node(from)
        .simple("MURMUR SHOW PEERS")
        .await
        .map_err(|e| format!("node{} SHOW PEERS: {e}", from + 1))?;
    Ok(rows.iter().any(|row| {
        row.first().is_some_and(|id| id.eq_ignore_ascii_case(&want))
            && row.get(2).is_some_and(|c| c == "true")
    }))
}

/// Connected sessions on one node (Go: `/v1/status` `connected_peers`;
/// here the `peers` column of `MURMUR SHOW REPLICATION`).
async fn connected_peers(cluster: &Cluster, idx: usize) -> Result<i64, String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node{} SHOW REPLICATION: {e}", idx + 1))?;
    let peers = rows
        .first()
        .and_then(|row| row.get(2).cloned())
        .unwrap_or_default();
    peers
        .parse::<i64>()
        .map_err(|_| format!("node{} unparsable peers {peers:?}", idx + 1))
}

async fn wait_peer_connected(
    cluster: &Cluster,
    from: usize,
    target: usize,
    want: bool,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if peer_connected(cluster, from, target).await? == want {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "peer {} Connected != {want} on node{} within {timeout:?}",
                hex(&cluster.node_id(target)),
                from + 1
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn wait_connected_peers(
    cluster: &Cluster,
    idx: usize,
    want: i64,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if connected_peers(cluster, idx).await? == want {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "connected_peers != {want} on node{} within {timeout:?}",
                idx + 1
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Leaf certificate bytes node B currently serves (Go: `fetchServedCert` +
/// `parseNotAfter`). The harness rewrites `tls/node.pem` from the same
/// material the restarted server loads, so file bytes always equal the
/// served leaf; expiry itself is proven against the minted TTL clock (see
/// the scenario README.md).
fn served_leaf(cluster: &Cluster) -> Result<Vec<u8>, String> {
    let path = cluster.node_info(B).dir.join("tls").join("node.pem");
    std::fs::read(&path).map_err(|e| format!("read served leaf {}: {e}", path.display()))
}

async fn run_body(cluster: &mut Cluster, ttl: Duration, window: Duration) -> Result<(), String> {
    for i in 0..cluster.len() {
        cluster
            .ensure_migrated(i, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
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

    // Phase 0: positive control, healthy mesh converges.
    insert(cluster, A, 1, "marker-init").await?;
    wait_converged(cluster, &["marker-init".to_string()], CONVERGE_TIMEOUT).await?;
    wait_peer_connected(cluster, A, B, true, PEER_TIMEOUT).await?;
    if connected_peers(cluster, A).await? != 1 {
        return Err(format!(
            "pre-expiry connected_peers(A) = {}, want 1",
            connected_peers(cluster, A).await?
        ));
    }

    // Phase 1: crash B (no graceful QUIC close: the survivor keeps a zombie
    // session), rotate to a short-expiry cert for the same NodeID.
    cluster.kill_node(B).await;
    let leaf_before = served_leaf(cluster)?;
    let rotate_start = Instant::now();
    cluster
        .rotate_node_cert(B, Some(ttl))
        .await
        .map_err(|e| format!("rotate node B to short cert: {e}"))?;
    let minted_at = Instant::now();
    cluster
        .mesh()
        .await
        .map_err(|e| format!("re-mesh after rotation: {e}"))?;
    // Stale-cert guard: the served leaf must be fresh material (rewritten
    // at rotation time), not the pre-rotation cert.
    let leaf_short = served_leaf(cluster)?;
    if leaf_short == leaf_before {
        return Err("served leaf unchanged by rotation (stale cert?)".into());
    }
    let mtime = std::fs::metadata(cluster.node_info(B).dir.join("tls").join("node.pem"))
        .and_then(|m| m.modified())
        .map_err(|e| format!("served leaf mtime: {e}"))?;
    if mtime.elapsed().unwrap_or(Duration::MAX) > rotate_start.elapsed() + Duration::from_secs(5) {
        return Err("served leaf mtime predates the rotation (stale cert?)".into());
    }
    let not_after = minted_at + ttl;
    eprintln!("short cert expires in {ttl:?}");

    // Assert connected pre-expiry: session plus a converged marker, all
    // within NotAfter minus the safety margin.
    let pre_deadline = not_after
        .checked_duration_since(Instant::now())
        .and_then(|left| left.checked_sub(PRE_EXPIRY_MARGIN))
        .filter(|left| !left.is_zero());
    let Some(pre_budget) = pre_deadline else {
        return Err("no pre-expiry budget left".into());
    };
    wait_peer_connected(cluster, A, B, true, pre_budget.min(PEER_TIMEOUT)).await?;
    insert(cluster, A, 2, "marker-pre-expiry").await?;
    let pre_left = not_after
        .checked_duration_since(Instant::now())
        .and_then(|left| left.checked_sub(PRE_EXPIRY_MARGIN))
        .unwrap_or_default();
    wait_converged(
        cluster,
        &["marker-init".to_string(), "marker-pre-expiry".to_string()],
        pre_left,
    )
    .await?;

    // Phase 2: wait out the TTL, then force fresh handshakes by restarting
    // B on the same expired leaf. An established session is not
    // re-validated mid-connection, so the restart forces both sides to
    // re-handshake. B's server starts fine with an expired cert (open
    // checks the SAN identity, not validity); its handshakes must fail.
    eprintln!("waiting for cert expiry");
    let deadline = not_after + POST_EXPIRY_SETTLE;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    cluster.stop_node(B).await;
    cluster
        .start_node(B)
        .await
        .map_err(|e| format!("restart node B on expired cert: {e}"))?;
    cluster
        .mesh()
        .await
        .map_err(|e| format!("re-mesh after expiry restart: {e}"))?;

    // B is rejected: survivor sees no session and connected_peers 1->0.
    wait_peer_connected(cluster, A, B, false, REJECT_TIMEOUT).await?;
    wait_connected_peers(cluster, A, 0, REJECT_TIMEOUT).await?;
    // B's own view agrees (its PG port is unaffected by the QUIC cert).
    wait_peer_connected(cluster, B, A, false, REJECT_TIMEOUT).await?;

    // Non-vacuity: at assert time, the cert B serves is expired: the
    // served material is byte-identical to the short leaf minted in phase
    // 1, and its TTL has fully elapsed.
    if served_leaf(cluster)? != leaf_short {
        return Err("served leaf changed across the expiry restart (wrong cert under test?)".into());
    }
    if Instant::now() < not_after {
        return Err("asserting rejection before the served cert expired".into());
    }
    eprintln!("rejection confirmed while serving the expired short cert");

    // An A-side marker written post-expiry must never reach B during the
    // isolation window, while A keeps serving it.
    insert(cluster, A, 3, "marker-post-expiry").await?;
    let window_end = Instant::now() + window;
    loop {
        if names(cluster, B).await?.iter().any(|n| n == "marker-post-expiry") {
            return Err("post-expiry marker reached expired node B".into());
        }
        if connected_peers(cluster, A).await? != 0 {
            return Err(format!(
                "connected_peers(A) = {} during isolation window, want 0",
                connected_peers(cluster, A).await?
            ));
        }
        if Instant::now() >= window_end {
            break;
        }
        tokio::time::sleep(WINDOW_POLL).await;
    }
    if names(cluster, B).await?.iter().any(|n| n == "marker-post-expiry") {
        return Err("post-expiry marker reached expired node B".into());
    }
    if names(cluster, A).await?.len() != 3 {
        return Err(format!(
            "A markers = {}, want 3",
            names(cluster, A).await?.len()
        ));
    }

    // Phase 3: rotate to a fresh cert and assert full reconvergence.
    cluster.stop_node(B).await;
    cluster
        .rotate_node_cert(B, Some(Duration::from_secs(24 * 3600)))
        .await
        .map_err(|e| format!("rotate node B to fresh cert: {e}"))?;
    cluster
        .mesh()
        .await
        .map_err(|e| format!("re-mesh after fresh rotation: {e}"))?;
    wait_peer_connected(cluster, A, B, true, CONVERGE_TIMEOUT).await?;
    wait_converged(
        cluster,
        &[
            "marker-init".to_string(),
            "marker-post-expiry".to_string(),
            "marker-pre-expiry".to_string(),
        ],
        CONVERGE_TIMEOUT,
    )
    .await?;
    // Bidirectional proof: a B-side write converges too.
    insert(cluster, B, 4, "marker-rotated").await?;
    let want_all = [
        "marker-init".to_string(),
        "marker-post-expiry".to_string(),
        "marker-pre-expiry".to_string(),
        "marker-rotated".to_string(),
    ];
    wait_converged(cluster, &want_all, CONVERGE_TIMEOUT).await?;
    let (a_names, b_names) = (names(cluster, A).await?, names(cluster, B).await?);
    if a_names != b_names {
        return Err(format!("post-rotation names differ: A={a_names:?} B={b_names:?}"));
    }
    eprintln!("rotation healed mesh; names={a_names:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cert_expiry_rejects_and_rotation_heals() {
    let ttl = env_seconds("MURMUR_CERT_LIFECYCLE_TTL_S", 25);
    let window = env_seconds("MURMUR_CERT_LIFECYCLE_WINDOW_S", 10);
    assert!(
        ttl >= Duration::from_secs(15),
        "short-cert TTL {ttl:?} too small to converge pre-expiry; raise MURMUR_CERT_LIFECYCLE_TTL_S"
    );

    // Pinned adversarial id order (survivor A < restarted B): the survivor's
    // pre-restart session is then in its tie-break-preferred direction, so a
    // re-mesh that keeps zombies over fresh handshakes stalls every rotation
    // leg deterministically instead of flaking 50/50 on random ids.
    let mut cluster = Cluster::new(ClusterOptions {
        name: "cert-lifecycle".into(),
        num_nodes: 2,
        node_ids: vec![[0x01; 16], [0x02; 16]],
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 2, "this scenario is written for 2 nodes");
    let result = run_body(&mut cluster, ttl, window).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("cert-lifecycle-rejection").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
