//! Rust port of the Go `origin-signatures` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/origin-signatures/`
//! (`origin_signatures_test.go`:
//! `TestForwardedOriginSurvivesOfflineOriginAndHostileRelay`). Three
//! `murmur-server` processes share one database id; A commits, B receives,
//! A stops, and C receives A's transaction exclusively through B. A relay
//! impersonating B (raw QUIC, product wire protocol via the shared harness
//! attacker fixture) then serves capture plus modified / unsigned /
//! cross-DBID / fabricated / chunk-identity attacks, all of which C must
//! reject with rows, counters, and commit cursor untouched. Restart checks
//! durability, a signed-duplicate replay checks idempotency, and an
//! untrusted snapshot manifest checks donor trust. Plain SQL replaces the
//! Go typed-records HTTP API.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use murmur_live_harness::attacker::Attacker;
use murmur_live_harness::replication::protocol::{self, Chunk, Message, Transaction};
use murmur_live_harness::replication::{retention, snapshot};
use murmur_live_harness::{tokio, wait_until_async, Cluster, ClusterOptions};

const A: usize = 0;
const B: usize = 1;
const C: usize = 2;
/// Convergence deadline per phase (Go: 30 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 200 ms).
const POLL: Duration = Duration::from_millis(200);
/// Session attach/teardown observation bound (Go: 5 s).
const PEER_TIMEOUT: Duration = Duration::from_secs(5);
/// Settle window after the idempotent re-offer (replay legs: 3-10 s).
const SETTLE: Duration = Duration::from_secs(5);
/// Served-batch capture window (Need answers are immediate).
const CAPTURE_TIMEOUT: Duration = Duration::from_secs(10);
/// Chunk-availability probe window.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Post-send drain asserting no `SnapshotApplied`/no `Error` (Go: 3 s).
const DRAIN_TIMEOUT: Duration = Duration::from_secs(3);
/// Per-leg rejection budget. C redials stopped B every 5 s and dial
/// failures overwrite `last_error`, so a missed rejection text is resent
/// on a fresh session until this budget elapses.
const REJECT_TIMEOUT: Duration = Duration::from_secs(30);
/// `last_error` poll window per resend attempt.
const ATTEMPT_WINDOW: Duration = Duration::from_secs(2);
/// Session pump slice while polling (drains control traffic so the
/// victim's send queue never wedges mid-leg).
const PUMP: Duration = Duration::from_millis(100);
/// Largest transaction the victim accepts (product default).
const MAX_TRANSACTION: usize = 64 << 20;

/// Forged transaction identity (asserted distinct from every real id).
const FORGED_ID: [u8; 16] = [0xF0; 16];
/// Chunk-proof identity (must differ from `CHUNK_ID`: that mismatch is the
/// chunk-identity attack).
const PROOF_ID: [u8; 16] = [0xF2; 16];
/// Chunk envelope identity (tampered relative to its proof).
const CHUNK_ID: [u8; 16] = [0xF3; 16];
/// Wrong database id for the cross-DBID leg.
const CROSS_DB: [u8; 16] = [0xDB; 16];

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
async fn names(cluster: &Cluster, idx: usize) -> Result<Vec<String>, String> {
    let rows = cluster
        .node(idx)
        .query("SELECT name FROM records ORDER BY name", &[])
        .await
        .map_err(|e| format!("node{} read names: {e}", idx + 1))?;
    rows.iter()
        .map(|row| {
            row.try_get::<_, String>(0)
                .map_err(|e| format!("node{} decode name: {e}", idx + 1))
        })
        .collect()
}

/// One single-row transaction (Go: `TypedInsertWithID`).
async fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) -> Result<(), String> {
    cluster
        .node(node)
        .execute("INSERT INTO records(id, name) VALUES($1, $2)", &[&id, &name])
        .await
        .map_err(|e| format!("node{node} insert failed: {e}"))?;
    cluster
        .node(node)
        .simple("MURMUR FLUSH")
        .await
        .map_err(|e| format!("node{node} flush after insert: {e}"))?;
    Ok(())
}

/// Wait until `node` holds exactly `want` (Go: `waitRows` + `TypedNames`).
async fn wait_names(cluster: &Cluster, node: usize, want: &[&str], timeout: Duration) -> Result<(), String> {
    let converged = wait_until_async(timeout, POLL, || async {
        names(cluster, node)
            .await
            .map(|got| got.iter().map(String::as_str).collect::<Vec<_>>() == want)
            .unwrap_or(false)
    })
    .await;
    if !converged {
        let got = names(cluster, node).await.unwrap_or_default();
        return Err(format!("node{} did not converge on {want:?} within {timeout:?} (has {got:?})", node + 1));
    }
    Ok(())
}

/// Connected replication sessions on one node (Go: `connected_peers`).
async fn connected_peers(cluster: &Cluster, idx: usize) -> Result<u64, String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node{} SHOW REPLICATION: {e}", idx + 1))?;
    let cell = rows
        .first()
        .and_then(|r| r.get(2).cloned())
        .ok_or_else(|| format!("node{} SHOW REPLICATION returned no peers", idx + 1))?;
    cell.parse::<u64>()
        .map_err(|e| format!("node{} peers {cell:?}: {e}", idx + 1))
}

/// Staged (`received`) and applied counters on one node. `received`
/// only moves when a batch passes verification into staging, so it is
/// the Rust equivalent of Go's `batches_received_total` must-not-move
/// assert; `applied` covers Go's nothing-applied / generation legs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Counters {
    received: u64,
    applied: u64,
}

async fn counters(cluster: &Cluster, idx: usize) -> Result<Counters, String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node{} SHOW REPLICATION: {e}", idx + 1))?;
    let row = rows
        .first()
        .ok_or_else(|| format!("node{} SHOW REPLICATION returned no rows", idx + 1))?;
    if row.len() < 6 {
        return Err(format!("node{} SHOW REPLICATION has {} columns, want >= 6", idx + 1, row.len()));
    }
    let received = row[4]
        .parse::<u64>()
        .map_err(|e| format!("node{} staged {:?}: {e}", idx + 1, row[4]))?;
    let applied = row[5]
        .parse::<u64>()
        .map_err(|e| format!("node{} applied {:?}: {e}", idx + 1, row[5]))?;
    Ok(Counters { received, applied })
}

/// Wait until no batch is staged and the applied cursor holds still:
/// `staged` is a gauge, so a baseline sampled mid-flight would demand
/// in-flight verified data never apply. A timeout means the node never
/// quiesced (a liveness bug), not a slow test.
async fn wait_counters_quiet(
    cluster: &Cluster,
    idx: usize,
    what: &str,
) -> Result<Counters, String> {
    let start = Instant::now();
    let mut last = counters(cluster, idx).await?;
    let mut stable = 0u32;
    loop {
        let current = counters(cluster, idx).await?;
        if current.received == 0 && current.applied == last.applied {
            stable += 1;
            if stable >= 3 {
                return Ok(current);
            }
        } else {
            stable = 0;
        }
        last = current;
        if start.elapsed() >= Duration::from_secs(30) {
            return Err(format!(
                "{what}: node{} counters never quiesced (staged={}, applied={})",
                idx + 1,
                last.received,
                last.applied
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn assert_counters(cluster: &Cluster, idx: usize, base: &Counters, ctx: &str) -> Result<(), String> {
    let got = counters(cluster, idx).await?;
    if got.received != base.received || got.applied != base.applied {
        return Err(format!(
            "node{} counters moved {ctx}: received {} -> {}, applied {} -> {} (a batch staged or applied)",
            idx + 1,
            base.received,
            got.received,
            base.applied,
            got.applied
        ));
    }
    Ok(())
}

/// Latest replication error on one node (attack rejections land here).
async fn last_error(cluster: &Cluster, idx: usize) -> Result<Option<String>, String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node{} SHOW REPLICATION: {e}", idx + 1))?;
    Ok(rows
        .first()
        .and_then(|r| r.get(6))
        .filter(|s| !s.is_empty())
        .cloned())
}

/// Local commit cursor (Go: `state_generation` advance check).
async fn committed(cluster: &Cluster, idx: usize) -> Result<u64, String> {
    let rows = cluster
        .node(idx)
        .simple("MURMUR SHOW REPLICATION")
        .await
        .map_err(|e| format!("node{} SHOW REPLICATION: {e}", idx + 1))?;
    rows.first()
        .and_then(|r| r.get(7))
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| format!("node{} SHOW REPLICATION returned no commit cursor", idx + 1))
}

/// Everything a rejected attack must leave untouched.
#[derive(Clone, Copy, Debug)]
struct Baseline {
    counters: Counters,
    committed: u64,
}

async fn baseline(cluster: &Cluster, idx: usize) -> Result<Baseline, String> {
    Ok(Baseline {
        counters: counters(cluster, idx).await?,
        committed: committed(cluster, idx).await?,
    })
}

/// Rows, counters, and commit cursor unchanged (Go: typed RIME state, HLC,
/// generation, and receipts unchanged).
async fn assert_protected(cluster: &Cluster, base: &Baseline, ctx: &str) -> Result<(), String> {
    assert_counters(cluster, C, &base.counters, ctx).await?;
    let got = committed(cluster, C).await?;
    if got != base.committed {
        return Err(format!(
            "node{} commit cursor moved {ctx}: {} -> {got} (a forgery published)",
            C + 1,
            base.committed
        ));
    }
    if names(cluster, C).await? != vec!["from-A".to_string()] {
        return Err(format!("node{} rows changed {ctx} (a forgery applied)", C + 1));
    }
    Ok(())
}

/// Wait until `idx` has no connected sessions (teardown observation, and
/// the required gate before every relay dial: the engine closes a second
/// inbound session for an id with a lingering entry as a duplicate).
async fn wait_quiet(cluster: &Cluster, idx: usize, what: &str) -> Result<(), String> {
    let converged = wait_until_async(PEER_TIMEOUT, Duration::from_millis(50), || async {
        connected_peers(cluster, idx).await.unwrap_or(u64::MAX) == 0
    })
    .await;
    if !converged {
        return Err(format!("{what}: node{} sessions did not tear down", idx + 1));
    }
    Ok(())
}

/// Served-origin-proof capture (Go: reading A's committed batch from the
/// stopped origin's store). The relay fetches A's batches from C over a
/// `Need` session and verifies every served batch under A's origin key.
/// Returns the row transaction (highest non-empty sequence) plus C's
/// schema bytes from the handshake (for the untrusted-manifest leg).
async fn capture_original(
    cluster: &Cluster,
    relay: &Attacker,
) -> Result<(Transaction, Vec<u8>), String> {
    wait_quiet(cluster, C, "origin-proof capture").await?;
    let db = cluster.database_id();
    let (id_a, id_c) = (cluster.node_id(A), cluster.node_id(C));
    let addr = cluster.node_info(C).repl_addr;
    let mut s = relay
        .dial(addr, db, id_c)
        .await
        .map_err(|e| format!("capture dial: {e}"))?;
    s.send(&Message::Need {
        origin: id_a,
        from: 1,
        through: u64::MAX,
    })
    .await
    .map_err(|e| format!("capture need: {e}"))?;
    let mut frames = s
        .collect_until(CAPTURE_TIMEOUT, |m| {
            matches!(m, Message::Batches(txs) if txs.iter().any(|t| t.origin == id_a && !t.payload.is_empty()))
        })
        .await;
    // Drain stragglers so the highest served sequence wins, not the first.
    frames.extend(s.collect_until(Duration::from_secs(2), |_| false).await);
    let schema = s.welcome_schema().to_vec();
    s.close();

    let trusted = BTreeMap::from([(id_a, cluster.node_origin_pubkey(A))]);
    let mut by_seq: BTreeMap<u64, Transaction> = BTreeMap::new();
    for message in &frames {
        if let Message::Batches(txs) = message {
            for tx in txs {
                if tx.origin != id_a {
                    continue;
                }
                tx.verify(db, &trusted, MAX_TRANSACTION)
                    .map_err(|e| format!("served batch failed origin verification: {e}"))?;
                by_seq.insert(tx.sequence, tx.clone());
            }
        }
    }
    if by_seq.is_empty() {
        return Err("C served no batches for offline origin A".into());
    }
    eprintln!(
        "capture: C served verified origin-A batches at sequences {:?}",
        by_seq.keys().collect::<Vec<_>>()
    );
    by_seq
        .into_values()
        .rev()
        .find(|tx| !tx.payload.is_empty())
        .map(|tx| (tx, schema))
        .ok_or_else(|| "C served only empty origin-A batches".to_string())
}

/// Send one hostile message, retrying across fresh relay sessions until
/// the victim records a *changed* `last_error` containing `want`
/// (authentication failures close the session, so every attempt dials
/// anew; redial noise can transiently overwrite `last_error`, so a miss
/// resends instead of failing). Returns the new error for chaining.
async fn expect_rejection(
    cluster: &Cluster,
    relay: &Attacker,
    make: impl Fn() -> Message,
    prev: &Option<String>,
    want: &str,
    what: &str,
) -> Result<Option<String>, String> {
    let db = cluster.database_id();
    let victim = cluster.node_id(C);
    let addr = cluster.node_info(C).repl_addr;
    let deadline = Instant::now() + REJECT_TIMEOUT;
    loop {
        wait_quiet(cluster, C, what).await?;
        let mut s = relay
            .dial(addr, db, victim)
            .await
            .map_err(|e| format!("{what}: relay dial: {e}"))?;
        if s.send(&make()).await.is_ok() {
            let attempt_end = Instant::now() + ATTEMPT_WINDOW;
            loop {
                let cur = last_error(cluster, C).await?;
                if cur != *prev && cur.as_ref().is_some_and(|e| e.contains(want)) {
                    eprintln!("{what}: rejected with {cur:?}");
                    s.close();
                    return Ok(cur);
                }
                for message in s.collect_until(PUMP, |_| false).await {
                    if matches!(message, Message::Error { .. }) {
                        eprintln!("{what}: wire error frame: {message:?}");
                    }
                }
                if Instant::now() >= attempt_end {
                    break;
                }
            }
        }
        drop(s);
        if Instant::now() >= deadline {
            let cur = last_error(cluster, C).await.ok().flatten();
            return Err(format!(
                "{what}: no {want:?} rejection within {REJECT_TIMEOUT:?} (last_error: {cur:?})"
            ));
        }
    }
}

/// Chunk-availability probe: the forged chunk identities must have no
/// durable staging (Go: tampering a chunk identity fails before durable
/// staging or cleanup).
async fn probe_availability(
    cluster: &Cluster,
    relay: &Attacker,
    forged_seq: u64,
    chunk_seq: u64,
) -> Result<(), String> {
    wait_quiet(cluster, C, "chunk-availability probe").await?;
    let db = cluster.database_id();
    let (id_a, id_c) = (cluster.node_id(A), cluster.node_id(C));
    let addr = cluster.node_info(C).repl_addr;
    let mut s = relay
        .dial(addr, db, id_c)
        .await
        .map_err(|e| format!("probe dial: {e}"))?;
    s.send(&Message::ChunkAvailabilityRequest { offset: 0 })
        .await
        .map_err(|e| format!("probe request: {e}"))?;
    let pages = s
        .collect_until(PROBE_TIMEOUT, |m| {
            matches!(m, Message::ChunkAvailabilityPage { more: false, .. })
        })
        .await;
    s.close();
    if !pages
        .iter()
        .any(|m| matches!(m, Message::ChunkAvailabilityPage { .. }))
    {
        return Err("C answered no chunk-availability page".into());
    }
    for message in &pages {
        if let Message::ChunkAvailabilityPage { entries, .. } = message {
            for (origin, seq, indexes) in entries {
                if *origin == id_a && (*seq == forged_seq || *seq == chunk_seq) {
                    return Err(format!(
                        "forged chunks durably staged for origin A seq {seq}: {indexes:?}"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Signed-duplicate replay (Go: after restart): the original batch is
/// absorbed idempotently — no error frame, nothing counted twice, rows
/// and cursor unchanged.
async fn replay_original(
    cluster: &Cluster,
    relay: &Attacker,
    original: &Transaction,
) -> Result<(), String> {
    wait_quiet(cluster, C, "signed-duplicate replay").await?;
    let db = cluster.database_id();
    let id_c = cluster.node_id(C);
    let addr = cluster.node_info(C).repl_addr;
    let mut s = relay
        .dial(addr, db, id_c)
        .await
        .map_err(|e| format!("replay dial: {e}"))?;
    s.send(&Message::Batches(vec![original.clone()]))
        .await
        .map_err(|e| format!("replay send: {e}"))?;
    let tail = s.collect_until(DRAIN_TIMEOUT, |_| false).await;
    s.close();
    if let Some((code, text)) = tail.iter().find_map(|m| match m {
        Message::Error { code, message } => Some((*code, message.clone())),
        _ => None,
    }) {
        return Err(format!("signed duplicate drew error {code}: {text}"));
    }
    Ok(())
}

async fn run_body(cluster: &mut Cluster) -> Result<(), String> {
    // Same table on every node (Go: TypedRecords fixture tables), flagged
    // replicated on each node (forwarding needs the per-node flag).
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

    // A commits; B receives; A stops.
    cluster.add_peer(A, B).await.map_err(|e| format!("mesh A->B: {e}"))?;
    cluster.add_peer(B, A).await.map_err(|e| format!("mesh B->A: {e}"))?;
    cluster.sync_all().await.map_err(|e| format!("sync A<->B: {e}"))?;
    insert(cluster, A, 1, "from-A").await?;
    wait_names(cluster, B, &["from-A"], CONVERGE_TIMEOUT).await?;
    cluster.stop_node(A).await;

    // C learns A's transaction exclusively from B while A remains stopped.
    cluster.add_peer(B, C).await.map_err(|e| format!("mesh B->C: {e}"))?;
    cluster.add_peer(C, B).await.map_err(|e| format!("mesh C->B: {e}"))?;
    cluster.sync_all().await.map_err(|e| format!("sync B<->C: {e}"))?;
    wait_names(cluster, C, &["from-A"], CONVERGE_TIMEOUT).await?;
    let (b_names, c_names) = (names(cluster, B).await?, names(cluster, C).await?);
    if b_names != c_names {
        return Err("forwarded state differs between B and C".into());
    }

    // Honest baseline on C (Go: `before := status` + honest receipt).
    // The applied row is the receipt proof: receipts commit atomically
    // with the apply, and there is no public receipt read API. Drain
    // first: `staged` is a gauge, and a baseline sampled mid-flight
    // would demand in-flight verified data never apply.
    wait_counters_quiet(cluster, C, "honest convergence").await?;
    let base = counters(cluster, C).await?;
    if names(cluster, C).await? != vec!["from-A".to_string()] {
        return Err("honest transaction missing on C".into());
    }

    // B stops; from here C is alone with its forwarded state. Batches
    // B sent before dying still apply afterwards (verified data must
    // never be dropped), so drain to quiescence and require only that
    // the applied cursor never moves backwards; the attack legs below
    // re-baseline and assert exact stillness instead.
    cluster.stop_node(B).await;
    wait_quiet(cluster, C, "B shutdown").await?;
    let drained = wait_counters_quiet(cluster, C, "B shutdown drain").await?;
    if drained.applied < base.applied {
        return Err(format!(
            "node{} applied cursor moved backwards after B shutdown: {} -> {}",
            C + 1,
            base.applied,
            drained.applied
        ));
    }
    let relay_base = baseline(cluster, C).await?;

    // Hostile relay, authenticated as B (fresh TLS leaf for B's id, B's
    // origin seed for signatures the victim accepts). Forged identities
    // must not collide with real ones.
    let db = cluster.database_id();
    let (id_a, id_b, id_c) = (cluster.node_id(A), cluster.node_id(B), cluster.node_id(C));
    for forged in [FORGED_ID, PROOF_ID, CHUNK_ID, CROSS_DB] {
        if [id_a, id_b, id_c, db].contains(&forged) {
            return Err("forged test identity collides with a real id".into());
        }
    }
    if PROOF_ID == CHUNK_ID {
        return Err("chunk proof/envelope test identities must differ".into());
    }
    let seed_a = cluster.node_origin_seed(A);
    let seed_b = cluster.node_origin_seed(B);
    let relay = Attacker::impersonate(cluster, B, seed_b)
        .map_err(|e| format!("relay identity: {e}"))?;

    // Served-origin-proof capture: fetch A's batches from C, verified
    // under A's origin key.
    let (original, c_schema) = capture_original(cluster, &relay).await?;
    eprintln!(
        "capture: original origin-A seq {} id {} ({} payload bytes)",
        original.sequence,
        murmur_live_harness::hex(&original.id),
        original.payload.len()
    );
    let forged_seq = original.sequence + 1;
    let chunk_seq = original.sequence + 2;

    // Modified batch: fresh identity, tampered payload bytes, original
    // signature (Go: `modified`). Fails the payload digest.
    let mut modified = original.clone();
    modified.sequence = forged_seq;
    modified.id = FORGED_ID;
    modified.payload[0] ^= 0x01;
    let prev = last_error(cluster, C).await?;
    let prev = expect_rejection(
        cluster,
        &relay,
        || Message::Batches(vec![modified.clone()]),
        &prev,
        "payload digest",
        "modified batch",
    )
    .await?;
    assert_protected(cluster, &relay_base, "after the modified batch").await?;

    // Unsigned batch (Go: `unsigned`). Adjacent legs use distinct
    // rejection texts (ed25519 messages are not asserted verbatim).
    let mut unsigned = original.clone();
    unsigned.sequence = forged_seq;
    unsigned.id = FORGED_ID;
    unsigned.signature.clear();
    let prev = expect_rejection(
        cluster,
        &relay,
        || Message::Batches(vec![unsigned.clone()]),
        &prev,
        "authentication",
        "unsigned batch",
    )
    .await?;
    assert_protected(cluster, &relay_base, "after the unsigned batch").await?;

    // Cross-DBID batch: valid A signature (verified locally first, so the
    // rejection isolates the database check), wrong database (Go: `crossDB`).
    let mut cross = original.clone();
    cross.database = CROSS_DB;
    cross.sequence = forged_seq;
    cross.id = FORGED_ID;
    cross
        .sign(&seed_a)
        .map_err(|e| format!("cross-DBID sign: {e}"))?;
    cross
        .verify(
            CROSS_DB,
            &BTreeMap::from([(id_a, cluster.node_origin_pubkey(A))]),
            MAX_TRANSACTION,
        )
        .map_err(|e| format!("cross-DBID fixture must verify under its own DBID: {e}"))?;
    let prev = expect_rejection(
        cluster,
        &relay,
        || Message::Batches(vec![cross.clone()]),
        &prev,
        "transaction identity",
        "cross-DBID batch",
    )
    .await?;
    assert_protected(cluster, &relay_base, "after the cross-DBID batch").await?;

    // Fabricated batch: B's valid signature over an origin-A sequence
    // (Go: `fabricated`, signed with B's key). The relay's own key must
    // not authorize another origin's log.
    let mut fabricated = original.clone();
    fabricated.sequence = forged_seq;
    fabricated.id = FORGED_ID;
    fabricated
        .sign(&seed_b)
        .map_err(|e| format!("fabricated sign: {e}"))?;
    fabricated
        .verify(
            db,
            &BTreeMap::from([(id_a, cluster.node_origin_pubkey(B))]),
            MAX_TRANSACTION,
        )
        .map_err(|e| format!("fabricated fixture must carry a valid B signature: {e}"))?;
    let prev = expect_rejection(
        cluster,
        &relay,
        || Message::Batches(vec![fabricated.clone()]),
        &prev,
        "authentication",
        "fabricated batch",
    )
    .await?;
    assert_protected(cluster, &relay_base, "after the fabricated batch").await?;

    // Chunk-identity batch (Go: forged `TransactionChunk` with a tampered
    // identity). The proof is a valid A-signed header for the forged
    // sequence; the envelope claims a different id, so binding fails
    // before anything stages.
    let mut proof = Transaction {
        database: db,
        origin: id_a,
        sequence: chunk_seq,
        id: PROOF_ID,
        millis: original.millis,
        logical: original.logical,
        schema: original.schema,
        payload_digest: [0; 32],
        payload: Vec::new(),
        signature: Vec::new(),
    };
    proof
        .sign(&seed_a)
        .map_err(|e| format!("chunk proof sign: {e}"))?;
    proof
        .verify_header(db, &BTreeMap::from([(id_a, cluster.node_origin_pubkey(A))]))
        .map_err(|e| format!("chunk proof fixture must verify: {e}"))?;
    let chunk = Chunk {
        proof,
        origin: id_a,
        sequence: chunk_seq,
        id: CHUNK_ID,
        index: 0,
        count: 1,
        total: 100,
        digest: [7; 32],
        data: vec![7; 100],
    };
    let _prev = expect_rejection(
        cluster,
        &relay,
        || Message::TransactionChunk(chunk.clone()),
        &prev,
        "chunk proof identity",
        "chunk-identity batch",
    )
    .await?;
    assert_protected(cluster, &relay_base, "after the chunk-identity batch").await?;

    // Neither the batch legs nor the chunk leg may have staged anything.
    probe_availability(cluster, &relay, forged_seq, chunk_seq).await?;
    assert_protected(cluster, &relay_base, "after the availability probe").await?;
    eprintln!("hostile relay rejected: rows, counters, and cursor untouched");

    // Restart C: the honest row is durable (A and B stay stopped).
    cluster.stop_node(C).await;
    cluster.start_node(C).await.map_err(|e| format!("restart node3: {e}"))?;
    if names(cluster, C).await? != vec!["from-A".to_string()] {
        return Err("honest row lost on restart".into());
    }
    // Fresh process: counters restart at zero (verified premise for the
    // idempotency legs below, not an assumption).
    let post = baseline(cluster, C).await?;
    if (post.counters.received, post.counters.applied) != (0, 0) {
        return Err("fresh-process counters must start at zero".into());
    }

    // Signed-duplicate replay over the relay: idempotent across restart
    // (no error frame, nothing counted twice, rows and cursor unchanged).
    replay_original(cluster, &relay, &original).await?;
    assert_protected(cluster, &post, "after the signed-duplicate replay").await?;

    // An authenticated relay is not automatically a trusted snapshot
    // source: a manifest with a valid B signature from donor B is still
    // refused (C trusts no snapshot donors). The schema matches C's own
    // so the refusal isolates donor trust.
    let (manifest, _) = snapshot::build(
        (db, id_b),
        id_c,
        vec![0xAB; 100],
        Vec::new(),
        protocol::schema_digest(&c_schema),
        relay.config(),
        retention::now_millis(),
    )
    .map_err(|e| format!("untrusted manifest build: {e}"))?;
    if manifest.donor != id_b || manifest.chunks.len() != 1 || manifest.signature.is_empty() {
        return Err("untrusted manifest fixture malformed".into());
    }
    let prev = last_error(cluster, C).await?;
    expect_rejection(
        cluster,
        &relay,
        || Message::SnapshotManifest(manifest.clone()),
        &prev,
        "snapshot identity",
        "untrusted snapshot manifest",
    )
    .await?;
    assert_protected(cluster, &post, "after the untrusted snapshot manifest").await?;
    eprintln!("forwarded origin survived offline origin, hostile relay, and restart");

    // B rejoins and re-offers the transaction C already holds; C must
    // absorb the duplicate idempotently: no error, nothing counted
    // twice, rows unchanged (the engine dedups before counting, so
    // `received` stays 0).
    cluster.start_node(B).await.map_err(|e| format!("restart node2: {e}"))?;
    cluster.add_peer(B, C).await.map_err(|e| format!("re-mesh B->C: {e}"))?;
    cluster.add_peer(C, B).await.map_err(|e| format!("re-mesh C->B: {e}"))?;
    cluster.sync_all().await.map_err(|e| format!("sync after B rejoin: {e}"))?;
    tokio::time::sleep(SETTLE).await;
    assert_protected(cluster, &post, "after the idempotent re-offer").await?;
    eprintln!("honest re-offer absorbed idempotently after the relay legs");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarded_origin_survives_offline_origin_and_hostile_relay() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "origin-signatures".into(),
        num_nodes: 3,
        manual_peers: true,
        ..Default::default()
    })
    .await
    .expect("cluster setup");
    assert_eq!(cluster.len(), 3, "this scenario is written for 3 nodes");
    let result = run_body(&mut cluster).await;
    let failed = result.is_err();
    if let Err(e) = &result {
        eprintln!("scenario failed: {e}");
    }
    if failed {
        cluster.dump_forensics("origin-signatures").await;
    }
    cluster.finish(failed).await;
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
