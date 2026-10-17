//! Reclamation: compaction, tombstone collection, dead-file removal
//! (port of `spool_compact_test.go` and `spool_tomb_test.go`).

#[path = "common/mod.rs"]
mod common;

use std::path::Path;

use common::{
    compact_options, incompressible, load_all, load_all_seq, segment_count, test_options,
};
use spool::{open, Store};

fn dead_heavy_store(dir: &Path) -> Store {
    let st = open(&compact_options(dir)).unwrap();
    for i in 0..100 {
        st.put(
            format!("k{i:03}").as_bytes(),
            &incompressible(300, i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    for i in 0..90 {
        st.put(format!("k{i:03}").as_bytes(), b"new").unwrap();
    }
    st.flush().unwrap();
    st
}

fn drive_compaction(st: &Store) {
    for _ in 0..200 {
        if st.stats().compactions > 0 {
            break;
        }
        st.reclaim().unwrap();
    }
    assert!(
        st.stats().compactions > 0,
        "no compaction; stats: {:?}",
        st.stats()
    );
}

#[test]
fn compaction_rewrites_live() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&compact_options(dir)).unwrap();
    for i in 0..100 {
        st.put(
            format!("k{i:03}").as_bytes(),
            &incompressible(300, i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    assert!(segment_count(dir) > 0);
    for i in 0..90 {
        st.put(format!("k{i:03}").as_bytes(), b"new").unwrap();
    }
    st.flush().unwrap();
    drive_compaction(&st);
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.len(), 100);
    for i in 0..90 {
        let k = format!("k{i:03}");
        assert_eq!(
            got.get(k.as_bytes()).map(|b| b.as_slice()),
            Some(&b"new"[..]),
            "{k}"
        );
    }
    for i in 90..100 {
        let k = format!("k{i:03}");
        assert_eq!(
            got.get(k.as_bytes()).map(|v| v.len()),
            Some(300),
            "{k} survivor damaged"
        );
    }
}

#[test]
fn compaction_writer_id_collision() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = dead_heavy_store(dir);
    drive_compaction(&st);
    // Write more oversized generations, forcing writer rotations
    // past the replacement id range.
    for gen in 0..5 {
        for i in 0..100 {
            st.put(
                format!("g{gen}/k{i:03}").as_bytes(),
                &incompressible(300, (gen * 100 + i) as u64),
            )
            .unwrap();
        }
        st.flush().unwrap();
        st.reclaim().unwrap();
    }
    let got = load_all(dir);
    assert_eq!(got.len(), 600);
    for i in 0..90 {
        let k = format!("k{i:03}");
        assert_eq!(
            got.get(k.as_bytes()).map(|b| b.as_slice()),
            Some(&b"new"[..]),
            "{k}"
        );
    }
    st.close().unwrap();
    // A clean reopen proves the file set is coherent.
    let st2 = open(&compact_options(dir)).unwrap();
    assert_eq!(load_all(dir).len(), 600);
    st2.close().unwrap();
}

#[test]
fn compaction_pauses_under_pressure() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = compact_options(dir);
    // 64KiB budget: the ~38KiB setup stays admittable while the
    // ~38KiB backlog trips the half-full pressure line.
    o.max_pending_bytes = 64 << 10;
    let st = open(&o).unwrap();
    for i in 0..100 {
        st.put(
            format!("k{i:03}").as_bytes(),
            &incompressible(300, i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    for i in 0..90 {
        st.put(format!("k{i:03}").as_bytes(), b"new").unwrap();
    }
    st.flush().unwrap();
    // Backlog over half the budget, unflushed.
    let pairs: Vec<spool::KV> = (0..100)
        .map(|i| {
            spool::KV::new(
                format!("p{i:03}").into_bytes(),
                incompressible(300, 1000 + i),
            )
        })
        .collect();
    st.put_batch(&pairs).unwrap();
    st.reclaim().unwrap();
    assert_eq!(st.stats().compactions, 0, "compacted under pressure");
    // The fence committed the backlog, so the next pass compacts.
    drive_compaction(&st);
    st.close().unwrap();
}

#[test]
fn compaction_min_free_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = compact_options(dir);
    o.compaction_min_free_bytes = 1 << 62;
    let st = open(&o).unwrap();
    for i in 0..100 {
        st.put(
            format!("k{i:03}").as_bytes(),
            &incompressible(300, i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    for i in 0..90 {
        st.put(format!("k{i:03}").as_bytes(), b"new").unwrap();
    }
    st.flush().unwrap();
    for _ in 0..5 {
        st.reclaim().unwrap();
    }
    assert_eq!(st.stats().compactions, 0, "compacted under reserve");
    st.close().unwrap();

    let mut o2 = compact_options(dir);
    o2.compaction_min_free_bytes = 1;
    let st2 = open(&o2).unwrap();
    drive_compaction(&st2);
    st2.close().unwrap();
}

#[test]
fn compaction_preserves_sequences() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&compact_options(dir)).unwrap();
    for i in 0..100 {
        st.put(
            format!("k{i:03}").as_bytes(),
            &incompressible(300, i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    for i in 0..90 {
        st.put(format!("k{i:03}").as_bytes(), b"new").unwrap();
    }
    st.flush().unwrap();
    let before = load_all_seq(dir);
    assert_eq!(before.len(), 100);
    drive_compaction(&st);
    let after = load_all_seq(dir);
    assert_eq!(after.len(), 100);
    for (k, seq) in &before {
        assert_eq!(
            after.get(k),
            Some(seq),
            "key {k:?} sequence moved across compaction"
        );
    }
    st.close().unwrap();
    let reopened = load_all_seq(dir);
    for (k, seq) in &before {
        assert_eq!(
            reopened.get(k),
            Some(seq),
            "key {k:?} sequence moved across reopen"
        );
    }
}

#[test]
fn tomb_proof_blocks_and_converges() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.max_segment_size = 4096;
    o.target_block_bytes = 1024;
    o.max_block_bytes = 2048;
    o.max_records_per_block = 100;
    o.max_key_size = 64;
    o.max_value_size = 1024;
    o.tomb_proof_threshold = 4;
    let st = open(&o).unwrap();
    // Generation 1: keepers plus holders, big values force rotation.
    for i in 0..10 {
        st.put(
            format!("keeper{i:02}").as_bytes(),
            &incompressible(900, i as u64),
        )
        .unwrap();
        st.put(
            format!("holder{i:02}").as_bytes(),
            &incompressible(900, 100 + i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    // Generation 2: tombstone the keepers.
    for i in 0..10 {
        st.delete(format!("keeper{i:02}").as_bytes()).unwrap();
    }
    st.flush().unwrap();
    for _ in 0..10 {
        st.reclaim().unwrap();
    }
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.len(), 10, "want 10 holders");
    for i in 0..10 {
        assert!(
            !got.contains_key(format!("keeper{i:02}").as_bytes()),
            "keeper{i:02} resurrected"
        );
    }
    let st = open(&test_options(dir)).unwrap();
    // Delete the holders: their files drain, then the proofs
    // complete and every tomb file is reclaimed.
    for i in 0..10 {
        st.delete(format!("holder{i:02}").as_bytes()).unwrap();
    }
    st.flush().unwrap();
    for _ in 0..200 {
        if segment_count(dir) <= 1 {
            break;
        }
        st.reclaim().unwrap();
    }
    assert_eq!(segment_count(dir), 1, "want 1 (active) after convergence");
    st.close().unwrap();
    assert_eq!(load_all(dir).len(), 0);
}

#[test]
fn compaction_uses_live_bytes_not_tomb_count() {
    // A sealed file whose live content is tiny by bytes but heavy by
    // tombstone count must still compact: tombstones cost ~100 bytes
    // to rewrite and must not veto reclamation. Under a count ratio a
    // few hundred stranded markers pin a 99%+ garbage file forever
    // (resource-exhaustion gridlock: reclaim a permanent no-op).
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&compact_options(dir)).unwrap();
    // Generation 0: small values, deleted while file 1 is active so
    // their tombstones strand inside the sealed file.
    for i in 0..20 {
        st.put(
            format!("g{i:02}").as_bytes(),
            &incompressible(100, i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    for i in 0..20 {
        st.delete(format!("g{i:02}").as_bytes()).unwrap();
    }
    st.flush().unwrap();
    // Live stragglers pin file 1 off the deletion path.
    for i in 0..3 {
        st.put(
            format!("s{i}").as_bytes(),
            &incompressible(100, 1000 + i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    // Bulk sealers, then deleted so dead bytes dominate file 1.
    for i in 0..8 {
        st.put(
            format!("b{i}").as_bytes(),
            &incompressible(2000, 2000 + i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    for i in 0..8 {
        st.delete(format!("b{i}").as_bytes()).unwrap();
    }
    st.flush().unwrap();
    // Precondition: a sealed file heavy on tombstones with tiny live
    // content (count ratio far over the threshold, byte ratio tiny).
    // Tomb proofs stay parked (default 1024 markers) to isolate the
    // compaction gate.
    let files = st.file_stats();
    assert!(
        files.iter().any(|f| {
            f.state == spool::SegmentState::Sealed
                && f.live_tombs >= 15
                && (1..=10).contains(&f.live_records)
        }),
        "setup did not strand tombs in a sealed file: {files:?}"
    );
    assert!(segment_count(dir) >= 2);
    for _ in 0..20 {
        st.reclaim().unwrap();
    }
    assert!(
        st.stats().compactions > 0,
        "tomb-heavy file never compacted; stats: {:?}",
        st.stats()
    );
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.len(), 3);
    for i in 0..3 {
        assert!(got.contains_key(format!("s{i}").as_bytes()), "s{i} lost");
    }
}

#[test]
fn tomb_cascade_drains_across_reclaims() {
    // Cross-file delete chain (resource-exhaustion shape): a sealed
    // tomb file whose values live in an older straggler file cannot
    // drain until the older file compacts away, so the segment count
    // drops only across successive reclaims. With the count-ratio gate
    // the straggler file is never eligible and the count never drops.
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = compact_options(dir);
    o.tomb_proof_threshold = 4; // proofs run at test scale
    let st = open(&o).unwrap();
    // File 1: live stragglers plus a deleted generation whose
    // tombstones strand in file 1.
    for i in 0..3 {
        st.put(
            format!("s{i}").as_bytes(),
            &incompressible(100, i as u64),
        )
        .unwrap();
    }
    for i in 0..20 {
        st.put(
            format!("a{i:02}").as_bytes(),
            &incompressible(100, 100 + i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    for i in 0..20 {
        st.delete(format!("a{i:02}").as_bytes()).unwrap();
    }
    st.flush().unwrap();
    // Bulk sealers for file 1, then deleted (tombs land in file 2).
    for i in 0..8 {
        st.put(
            format!("b1-{i}").as_bytes(),
            &incompressible(2000, 200 + i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    for i in 0..8 {
        st.delete(format!("b1-{i}").as_bytes()).unwrap();
    }
    st.flush().unwrap();
    // Bulk sealers for file 2, then deleted (tombs land in the active file).
    for i in 0..8 {
        st.put(
            format!("b2-{i}").as_bytes(),
            &incompressible(2000, 300 + i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    for i in 0..8 {
        st.delete(format!("b2-{i}").as_bytes()).unwrap();
    }
    st.flush().unwrap();
    let before = segment_count(dir);
    assert!(before >= 3, "want >= 3 segments, got {before}");
    let files = st.file_stats();
    assert!(
        files.iter().any(|f| {
            f.state == spool::SegmentState::Sealed
                && (1..=10).contains(&f.live_records)
                && f.live_tombs >= 15
        }),
        "no sealed straggler+tomb file: {files:?}"
    );
    assert!(
        files.iter().any(|f| {
            f.state == spool::SegmentState::Sealed
                && f.live_records == 0
                && f.live_tombs >= 5
        }),
        "no sealed drained tomb file: {files:?}"
    );
    for _ in 0..20 {
        st.reclaim().unwrap();
        if segment_count(dir) < before {
            break;
        }
    }
    assert!(
        segment_count(dir) < before,
        "segments stuck at {before}; stats: {:?}",
        st.stats()
    );
    assert!(st.stats().compactions > 0);
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.len(), 3);
}

#[test]
fn dead_file_removal() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&compact_options(dir)).unwrap();
    for i in 0..50 {
        st.put(format!("a{i}").as_bytes(), &incompressible(300, i as u64))
            .unwrap();
    }
    st.flush().unwrap();
    let before = segment_count(dir);
    assert!(
        before >= 2,
        "first flush spans {before} segment(s), want >= 2"
    );
    for i in 0..50 {
        st.put(format!("b{i}").as_bytes(), b"new").unwrap();
    }
    // Delete the whole first generation so its files go fully dead.
    for i in 0..50 {
        st.delete(format!("a{i}").as_bytes()).unwrap();
    }
    st.flush().unwrap();
    let mut removed = false;
    for _ in 0..200 {
        st.reclaim().unwrap();
        if segment_count(dir) < before {
            removed = true;
            break;
        }
    }
    assert!(removed, "no segment removed (still {})", segment_count(dir));
    st.close().unwrap();
    assert_eq!(load_all(dir).len(), 50);
}

#[test]
fn delete_and_reload() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    for i in 0..50 {
        st.put(format!("k{i}").as_bytes(), b"v").unwrap();
    }
    st.flush().unwrap();
    for i in (0..50).step_by(2) {
        st.delete(format!("k{i}").as_bytes()).unwrap();
    }
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.len(), 25);
    for i in (1..50).step_by(2) {
        assert_eq!(
            got.get(format!("k{i}").as_bytes()).map(|b| b.as_slice()),
            Some(&b"v"[..])
        );
    }
}

#[test]
fn tombstone_no_resurrection() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.max_segment_size = 4096; // force many small segments
    o.target_block_bytes = 1024;
    o.max_block_bytes = 2048;
    o.max_records_per_block = 100;
    o.max_key_size = 64;
    o.max_value_size = 1024;
    let st = open(&o).unwrap();
    // File 1: the victim key plus padding that will also die.
    st.put(b"victim", b"v1").unwrap();
    for i in 0..20 {
        st.put(
            format!("pad{i}").as_bytes(),
            &incompressible(1024, i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    // Delete everything from file 1; tombs land in newer files.
    st.delete(b"victim").unwrap();
    for i in 0..20 {
        st.delete(format!("pad{i}").as_bytes()).unwrap();
    }
    st.flush().unwrap();
    // Survivor data in still newer files.
    for i in 0..30 {
        st.put(format!("live{i}").as_bytes(), b"y").unwrap();
    }
    st.flush().unwrap();

    let victim_seg = dir.join("segments").join("000000000001.spool");
    assert!(victim_seg.exists(), "expected file 1 to exist");
    let mut reclaimed = false;
    for _ in 0..200 {
        st.reclaim().unwrap();
        if !victim_seg.exists() {
            reclaimed = true;
            break;
        }
    }
    assert!(reclaimed, "file 1 never reclaimed; stats: {:?}", st.stats());
    // Plenty more passes so tomb generations drain too.
    for _ in 0..50 {
        st.reclaim().unwrap();
    }
    st.close().unwrap();
    let got = load_all(dir);
    assert!(!got.contains_key(&b"victim"[..]), "victim resurrected");
    assert_eq!(got.len(), 30);
}

#[test]
fn put_after_delete_wins() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.put(b"k", b"v1").unwrap();
    st.delete(b"k").unwrap();
    st.close().unwrap();
    // New epoch.
    let st2 = open(&test_options(dir)).unwrap();
    st2.put(b"k", b"v2").unwrap();
    st2.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.get(&b"k"[..]).map(|b| b.as_slice()), Some(&b"v2"[..]));
}
