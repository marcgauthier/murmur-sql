//! Replication history lifecycle. All authoritative state is in the backend.
use crate::{protocol::*, *};

const PEERS: &[u8] = b"07/replication/retention/peers/";
pub const COVERAGE_KEY: &[u8] = b"07/replication/retention/coverage";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerRetention {
    pub node: NodeId,
    pub last_seen_millis: u64,
    pub retired: bool,
    pub acknowledged: BTreeMap<NodeId, u64>,
}
#[derive(Clone, Debug, Default)]
pub struct RetentionStatus {
    pub peers: Vec<PeerRetention>,
    pub collected_transactions: u64,
    pub last_collection_millis: u64,
}
pub fn time_key(origin: NodeId, sequence: u64) -> Vec<u8> {
    format!(
        "07/replication/retention/time/{}/{sequence:020}",
        hex(&origin)
    )
    .into_bytes()
}
pub fn floor_key(origin: NodeId) -> Vec<u8> {
    format!("07/replication/retention/floor/{}", hex(&origin)).into_bytes()
}
fn peer_key(node: NodeId) -> Vec<u8> {
    [PEERS, hex(&node).as_bytes()].concat()
}
pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

pub(crate) fn peer(backend: &dyn Backend, node: NodeId) -> Result<Option<PeerRetention>> {
    backend
        .get(&peer_key(node))?
        .map(|bytes| decode(&bytes))
        .transpose()
}
pub(crate) fn touch(backend: &dyn Backend, node: NodeId, progress: &[Progress]) -> Result<()> {
    let mut state = peer(backend, node)?.unwrap_or(PeerRetention {
        node,
        last_seen_millis: 0,
        retired: false,
        acknowledged: BTreeMap::new(),
    });
    if state.retired {
        return Err(Error::Auth("retired peer".into()));
    }
    let now = now_millis();
    let mut changed = state.last_seen_millis == 0;
    for entry in progress {
        let value = state.acknowledged.entry(entry.origin).or_default();
        changed |= entry.applied > *value;
        *value = (*value).max(entry.applied);
    }
    if state.acknowledged.len() > 4096 {
        return Err(Error::Overloaded);
    }
    if !changed && now.saturating_sub(state.last_seen_millis) < 10_000 {
        return Ok(());
    }
    state.last_seen_millis = state.last_seen_millis.max(now);
    backend.commit(vec![Record::put(peer_key(node), encode(&state)?)])
}
pub(crate) fn retire(backend: &dyn Backend, node: NodeId) -> Result<()> {
    let mut state = peer(backend, node)?.unwrap_or(PeerRetention {
        node,
        last_seen_millis: now_millis(),
        retired: false,
        acknowledged: BTreeMap::new(),
    });
    state.retired = true;
    // Exclusion and removal of the retention obligation are one durable action.
    backend.commit(vec![
        Record::put(peer_key(node), encode(&state)?),
        Record::put(
            format!("07/replication/excluded/{}", hex(&node)).into_bytes(),
            encode(&node)?,
        ),
    ])
}
pub(crate) fn status(backend: &dyn Backend) -> Result<RetentionStatus> {
    let mut peers = Vec::new();
    for key in backend.keys(PEERS)? {
        if let Some(bytes) = backend.get(&key)? {
            peers.push(decode(&bytes)?);
        }
    }
    Ok(RetentionStatus {
        peers,
        collected_transactions: backend
            .get(b"07/replication/retention/collected")?
            .map(|bytes| decode(&bytes))
            .transpose()?
            .unwrap_or(0),
        last_collection_millis: backend
            .get(b"07/replication/retention/last")?
            .map(|bytes| decode(&bytes))
            .transpose()?
            .unwrap_or(0),
    })
}

/// Bounded pass: at most 256 deletions per origin, 2048 per pass. Old logs
/// without receipt-time metadata get a new conservative retention start time.
pub(crate) fn collect(backend: &dyn Backend, config: &Config) -> Result<u64> {
    collect_at(backend, config, now_millis())
}

fn collect_at(backend: &dyn Backend, config: &Config, now: u64) -> Result<u64> {
    let peers = status(backend)?.peers;
    let mut plans = Vec::new();
    let mut budget = 2048usize;
    let mut timestamps = Vec::new();
    let cursor_key=b"07/replication/retention/origin-cursor";
    let previous=backend.get(cursor_key)?;
    let mut origins=backend.keys(b"07/replication/retained/")?;origins.sort();
    if let Some(previous)=&previous {let split=origins.partition_point(|key|key<=previous);origins.rotate_left(split);}
    let mut cursor=None;
    for key in origins {
        cursor=Some(key.clone());
        if budget == 0 {
            break;
        }
        let Some(bytes) = backend.get(&key)? else {
            continue;
        };
        let head: Progress = decode(&bytes)?;
        let keys = backend.keys_page(format!("07/replication/log/{}/", hex(&head.origin)).as_bytes(),None,256)?;
        let keep = usize::try_from(config.minimum_retained).unwrap_or(usize::MAX);
        let retained=head.retained_through.saturating_sub(head.retained_from).saturating_add(1);
        let eligible = usize::try_from(retained).unwrap_or(usize::MAX).saturating_sub(keep).min(keys.len()).min(budget);
        if eligible == 0 {
            continue;
        }
        let peer_floor = peers
            .iter()
            .filter(|peer| {
                !peer.retired
                    && now.saturating_sub(peer.last_seen_millis) <= millis(config.maximum_offline)
            })
            .map(|peer| peer.acknowledged.get(&head.origin).copied().unwrap_or(0))
            .min()
            .unwrap_or(u64::MAX);
        let bridge_floor = if let Some(bridge) = config
            .bridge
            .as_ref()
            .filter(|bridge| bridge.role == bridge::Role::LowExporter)
        {
            backend
                .get(
                    format!("09/bridge/exported/{}/{}", bridge.stream, hex(&head.origin))
                        .as_bytes(),
                )?
                .map(|bytes| decode::<u64>(&bytes))
                .transpose()?
                .unwrap_or(0)
        } else {
            u64::MAX
        };
        let snapshot_floor = crate::snapshot::prune_limit(backend, head.origin, now)?;
        let mut through = None;
        let mut prefix_blocked = false;
        for key in keys.into_iter().take(eligible) {
            // Only inspect indexed keys and the small persistence-time record.
            // Cleanup must not decode a potentially huge transaction payload.
            let sequence = std::str::from_utf8(&key)
                .ok()
                .and_then(|key| key.rsplit('/').next())
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| Error::Protocol("retention log sequence".into()))?;
            if sequence == 0 || key != log_key(head.origin, sequence) {
                return Err(Error::Protocol("retention log identity".into()));
            }
            let time = backend
                .get(&time_key(head.origin, sequence))?
                .map(|bytes| decode::<u64>(&bytes))
                .transpose()?;
            let Some(time) = time else {
                prefix_blocked = true;
                timestamps.push(Record::put(time_key(head.origin, sequence), encode(&now)?));
                if timestamps.len() >= 256 {
                    break;
                }
                continue;
            };
            if prefix_blocked {
                continue;
            }
            if now.saturating_sub(time) < millis(config.retention)
                || sequence > head.applied
                || sequence > peer_floor
                || sequence > bridge_floor
                || sequence > snapshot_floor
            {
                break;
            }
            through = Some(sequence);
        }
        if let Some(through) = through {
            plans.push((head.origin, through));
            budget -= eligible;
        }
        if timestamps.len() >= 256 {
            break;
        }
    }
    if !timestamps.is_empty() {
        backend.commit(timestamps)?;
    }
    if let Some(cursor)=cursor {backend.commit(vec![Record::put(cursor_key.to_vec(),cursor)])?;}
    let mut count = 0u64;
    if !plans.is_empty() {
        // Backend must snapshot durable logical state and its applied progress
        // on the serialized storage worker before allowing any deletion.
        backend.retention_checkpoint()?;
        for (origin, through) in plans {
            count = count.saturating_add(backend.prune_history(origin, through)?);
        }
    }
    backend.commit(vec![Record::put(
        b"07/replication/retention/last".to_vec(),
        encode(&now)?,
    )])?;
    // Reclaim after every pass, including quiet ones: deleting a sealed
    // file can unblock tombstone drops (and further file deletions)
    // whose older values lived in that file, so a cross-file delete
    // cascade needs reclaims with no fresh prunes to drain. Skipping
    // quiet passes strands that debt forever once pruning completes.
    // Backends rate-limit physical reclaims; see `reclaim_retention`.
    backend.reclaim_retention()?;
    Ok(count)
}

/// Upgrade acknowledgements recorded by the previous replication worker. Do not
/// forget offline peers when retention is first enabled on an existing store.
pub(crate) fn migrate_peers(backend: &dyn Backend) -> Result<()> {
    let keys = backend.keys(b"07/replication/peer/")?;
    let mut peers = BTreeMap::<NodeId, PeerRetention>::new();
    for key in &keys {
        let name =
            std::str::from_utf8(key).map_err(|_| Error::Protocol("legacy peer key".into()))?;
        let parts: Vec<_> = name.split('/').collect();
        if parts.len() != 5 {
            return Err(Error::Protocol("legacy peer key".into()));
        }
        let node = parse_node(parts[3])?;
        let origin = parse_node(parts[4])?;
        let Some(bytes) = backend.get(key)? else {
            continue;
        };
        let (applied, seen): (u64, u64) = decode(&bytes)?;
        if !peers.contains_key(&node) {
            peers.insert(
                node,
                peer(backend, node)?.unwrap_or(PeerRetention {
                    node,
                    last_seen_millis: seen,
                    retired: false,
                    acknowledged: BTreeMap::new(),
                }),
            );
        }
        let state = peers.get_mut(&node).unwrap();
        state.last_seen_millis = state.last_seen_millis.max(seen);
        let value = state.acknowledged.entry(origin).or_default();
        *value = (*value).max(applied);
    }
    for state in peers.values() {
        backend.commit(vec![Record::put(peer_key(state.node), encode(state)?)])?;
    }
    // New records are durable before the old representation is removed.
    for batch in keys.chunks(128) {
        backend.commit(batch.iter().cloned().map(Record::delete).collect())?;
    }
    Ok(())
}
fn parse_node(name: &str) -> Result<NodeId> {
    if name.len() != 32 || !name.is_ascii() {
        return Err(Error::Protocol("node id".into()));
    }
    let mut node = [0; 16];
    for (index, value) in node.iter_mut().enumerate() {
        *value = u8::from_str_radix(&name[index * 2..index * 2 + 2], 16)
            .map_err(|_| Error::Protocol("node id".into()))?;
    }
    Ok(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };

    #[derive(Default)]
    struct TestBackend {
        records: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
        events: Mutex<Vec<String>>,
        checkpoint_fails: AtomicBool,
    }
    impl Backend for TestBackend {
        fn identity(&self) -> (NodeId, NodeId) {
            ([1; 16], [2; 16])
        }
        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(self.records.lock().unwrap().get(key).cloned())
        }
        fn keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
            Ok(self
                .records
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }
        fn commit(&self, records: Vec<Record>) -> Result<()> {
            let mut state = self.records.lock().unwrap();
            for r in records {
                match r.value {
                    Some(v) => {
                        state.insert(r.key, v);
                    }
                    None => {
                        state.remove(&r.key);
                    }
                }
            }
            Ok(())
        }
        fn retention_checkpoint(&self) -> Result<()> {
            self.events.lock().unwrap().push("snapshot".into());
            if self.checkpoint_fails.load(Ordering::SeqCst) {
                Err(Error::Storage("snapshot failed".into()))
            } else {
                Ok(())
            }
        }
        fn prune_history(&self, origin: NodeId, through: u64) -> Result<u64> {
            self.events
                .lock()
                .unwrap()
                .push(format!("prune:{}:{through}", origin[0]));
            Ok(through)
        }
        fn reclaim_retention(&self) -> Result<()> {
            self.events.lock().unwrap().push("reclaim".into());
            Ok(())
        }
        fn publish(&self, _: Vec<Record>) -> Result<()> {
            unreachable!()
        }
        fn import_bridge(&self, _: bridge::Header, _: Vec<u8>) -> Result<bool> {
            unreachable!()
        }
        fn release_ownership(&self, _: String, _: Vec<u8>, _: Option<String>) -> Result<bool> {
            unreachable!()
        }
        fn snapshot(&self) -> Result<Vec<u8>> {
            unreachable!()
        }
        fn snapshot_with_progress(&self) -> Result<(Vec<u8>, Vec<Progress>)> {
            unreachable!()
        }
        fn schema(&self) -> Result<Vec<u8>> {
            unreachable!()
        }
        fn apply(&self, _: Vec<Vec<u8>>, _: Vec<Record>) -> Result<bool> {
            unreachable!()
        }
        fn observe(&self, _: u64, _: u32) -> Result<()> {
            unreachable!()
        }
        fn accept_schema(&self, _: Vec<u8>) -> Result<bool> {
            unreachable!()
        }
    }
    const NOW: u64 = 100_000;
    fn config() -> Config {
        let mut c = Config::new(
            "127.0.0.1:0".parse().unwrap(),
            Credentials {
                chain: vec![],
                private_key: vec![].into(),
                roots: vec![],
            },
            [1; 32],
        );
        c.minimum_retained = 2;
        c.retention = Duration::from_millis(1000);
        c.maximum_offline = Duration::from_millis(5000);
        c
    }
    fn logs(b: &TestBackend, count: u64, age: u64) {
        let origin = [3; 16];
        b.commit(vec![Record::put(
            retained_key(origin),
            encode(&Progress {
                origin,
                observed: count,
                applied: count,
                retained_from: 1,
                retained_through: count,
            })
            .unwrap(),
        )])
        .unwrap();
        for seq in 1..=count {
            b.commit(vec![
                Record::put(log_key(origin, seq), vec![0xff]),
                Record::put(time_key(origin, seq), encode(&(NOW - age)).unwrap()),
            ])
            .unwrap();
        }
    }
    fn put_peer(b: &TestBackend, last_seen: u64, ack: u64, retired: bool) {
        let peer = PeerRetention {
            node: [4; 16],
            last_seen_millis: last_seen,
            retired,
            acknowledged: BTreeMap::from([([3; 16], ack)]),
        };
        b.commit(vec![Record::put(
            peer_key(peer.node),
            encode(&peer).unwrap(),
        )])
        .unwrap();
    }
    #[test]
    fn age_and_count_are_independent_retention_requirements() {
        for (count, age, expected) in [(2, 2000, 0), (6, 999, 0), (6, 1000, 4)] {
            let b = TestBackend::default();
            logs(&b, count, age);
            assert_eq!(collect_at(&b, &config(), NOW).unwrap(), expected);
            let events = b.events.lock().unwrap();
            if expected > 0 {
                assert_eq!(&events[..], ["snapshot", "prune:3:4", "reclaim"]);
            } else {
                assert_eq!(
                    &events[..],
                    ["reclaim"],
                    "quiet pass must skip snapshot/prune but still reclaim"
                );
            }
        }
    }
    #[test]
    fn offline_window_acknowledgements_and_retirement_bound_pruning() {
        for (last_seen, ack, retired, expected) in [
            (NOW - 5000, 0, false, 0),
            (NOW - 5000, 2, false, 2),
            (NOW - 5001, 0, false, 4),
            (NOW, 0, true, 4),
        ] {
            let b = TestBackend::default();
            logs(&b, 6, 2000);
            put_peer(&b, last_seen, ack, retired);
            assert_eq!(collect_at(&b, &config(), NOW).unwrap(), expected);
        }
    }
    #[test]
    fn pending_bridge_exports_protect_transaction_history() {
        for (exported, expected) in [(None, 0), (Some(2u64), 2), (Some(6), 4)] {
            let b = TestBackend::default();
            logs(&b, 6, 2000);
            let mut c = config();
            c.bridge = Some(bridge::Config::exporter(
                [1; 16],
                std::path::PathBuf::new(),
                "low".into(),
                [5; 32],
                [6; 32],
            ));
            if let Some(seq) = exported {
                b.commit(vec![Record::put(
                    format!("09/bridge/exported/low/{}", hex(&[3; 16])).into_bytes(),
                    encode(&seq).unwrap(),
                )])
                .unwrap();
            }
            assert_eq!(collect_at(&b, &c, NOW).unwrap(), expected);
        }
    }
    #[test]
    fn failed_snapshot_never_prunes_and_retry_snapshots_first() {
        let b = TestBackend::default();
        logs(&b, 6, 2000);
        b.checkpoint_fails.store(true, Ordering::SeqCst);
        assert!(collect_at(&b, &config(), NOW).is_err());
        assert_eq!(&*b.events.lock().unwrap(), &["snapshot"]);
        assert_eq!(b.keys(b"07/replication/log/").unwrap().len(), 6);
        b.checkpoint_fails.store(false, Ordering::SeqCst);
        assert_eq!(collect_at(&b, &config(), NOW).unwrap(), 4);
        assert_eq!(
            &*b.events.lock().unwrap(),
            &["snapshot", "snapshot", "prune:3:4", "reclaim"]
        );
    }
    #[test]
    fn legacy_missing_time_blocks_prefix_until_full_minimum_age() {
        let b = TestBackend::default();
        logs(&b, 6, 2000);
        b.commit(vec![Record::delete(time_key([3; 16], 1))])
            .unwrap();
        assert_eq!(collect_at(&b, &config(), NOW).unwrap(), 0);
        assert_eq!(
            decode::<u64>(&b.get(&time_key([3; 16], 1)).unwrap().unwrap()).unwrap(),
            NOW
        );
        assert_eq!(collect_at(&b, &config(), NOW + 999).unwrap(), 0);
        assert_eq!(collect_at(&b, &config(), NOW + 1000).unwrap(), 4);
    }
    #[test]
    fn retirement_is_atomic_and_cannot_be_undone_by_peer_touch() {
        let b = TestBackend::default();
        put_peer(&b, NOW, 2, false);
        retire(&b, [4; 16]).unwrap();
        assert!(peer(&b, [4; 16]).unwrap().unwrap().retired);
        assert!(
            b.get(format!("07/replication/excluded/{}", hex(&[4; 16])).as_bytes())
                .unwrap()
                .is_some()
        );
        assert!(matches!(touch(&b, [4; 16], &[]), Err(Error::Auth(_))));
    }
    #[test]
    fn legacy_peer_migration_preserves_max_ack_and_retirement() {
        let b = TestBackend::default();
        put_peer(&b, NOW, 7, true);
        let key = format!("07/replication/peer/{}/{}", hex(&[4; 16]), hex(&[3; 16])).into_bytes();
        b.commit(vec![Record::put(key, encode(&(2u64, NOW - 1)).unwrap())])
            .unwrap();
        migrate_peers(&b).unwrap();
        migrate_peers(&b).unwrap();
        let p = peer(&b, [4; 16]).unwrap().unwrap();
        assert!(p.retired);
        assert_eq!(p.acknowledged[&[3; 16]], 7);
        assert_eq!(p.last_seen_millis, NOW);
        assert!(b.keys(b"07/replication/peer/").unwrap().is_empty());
    }
    #[test]
    fn pruning_is_bounded_even_when_history_is_large() {
        let b = TestBackend::default();
        logs(&b, 600, 2000);
        assert_eq!(collect_at(&b, &config(), NOW).unwrap(), 256);
        assert!(b.events.lock().unwrap().contains(&"prune:3:256".into()));
    }
}
