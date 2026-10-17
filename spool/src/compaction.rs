//! Reclamation: tombstone drops, dead-file deletion, compaction.
//!
//! Rust port of `spool/compaction.go`.

use std::fs;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::block::BLOCK_HEADER_LEN;
use crate::error::{err, ErrorKind, Result};
use crate::filestats::SegmentState;
use crate::index::Location;
use crate::loader::Loader;
use crate::manifest::{dir_sync, SEGMENTS_DIR_NAME};
use crate::record::{PendingRecord, RECORD_OVERHEAD};
use crate::segment::{segment_file_name, StagedFile};
use crate::store::{split_blocks, Store};

impl Store {
    /// Tick `reclaim` on the configured interval.
    pub(crate) fn reclaim_loop(&self) {
        let interval: Duration = self
            .inner
            .opts
            .reclaim_interval
            .unwrap_or(Duration::from_secs(30));
        loop {
            std::thread::sleep(interval);
            if self.inner.stop.load(Ordering::SeqCst) {
                return;
            }
            if self.inner.maint_active.load(Ordering::SeqCst) {
                continue;
            }
            if let Err(e) = self.reclaim() {
                if self.inner.stop.load(Ordering::SeqCst) {
                    return;
                }
                self.inner.reclaim_errors.fetch_add(1, Ordering::Relaxed);
                self.record_reclaim_err(e);
            }
        }
    }

    /// Run one pass: fence all in-flight batches, drop collectible
    /// tombstones, delete fully dead files and compact one eligible
    /// file. Concurrent calls serialize.
    ///
    /// The rewrite pass is skipped while writers keep the buffer over
    /// half full and while free disk space is below
    /// `compaction_min_free_bytes`. Tombstone drops and dead-file
    /// deletion always run.
    pub fn reclaim(&self) -> Result<()> {
        let _r = self.inner.reclaim_lock.lock().unwrap();
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        self.check_maint()?;
        let start = std::time::SystemTime::now();
        let result = self.reclaim_inner();
        self.inner.reclaims.fetch_add(1, Ordering::Relaxed);
        self.inner.reclaim_nanos.fetch_add(
            start.elapsed().map(|d| d.as_nanos() as u64).unwrap_or(0),
            Ordering::Relaxed,
        );
        result
    }

    fn reclaim_inner(&self) -> Result<()> {
        // Snapshot write pressure before the fence drains the queue.
        let pressured =
            self.inner.pending_bytes.load(Ordering::SeqCst) > self.inner.opts.max_pending_bytes / 2;
        // Fence: admit everything pending, then wait for every group
        // admitted up to and including ours.
        let id = self.flush_internal(self.inner.opts.durability, false)?;
        self.wait_for_group(id, false)?;
        self.drop_tombs()?;
        self.delete_dead_files()?;
        if pressured {
            return Ok(());
        }
        if self.inner.opts.compaction_min_free_bytes > 0 {
            let free = free_bytes(&self.inner.dir.join(SEGMENTS_DIR_NAME))?;
            if free < self.inner.opts.compaction_min_free_bytes as u64 {
                return Ok(());
            }
        }
        self.compact_one()
    }

    /// Discard deletion markers only after proving no older value for
    /// the key remains in any surviving authoritative segment.
    fn drop_tombs(&self) -> Result<()> {
        use std::collections::HashMap;
        // Tombs only drop from drained (zero-live-record) files; when every
        // file still holds live data the full-spool validation scan below
        // cannot drop anything, so skip it (it costs seconds on large
        // stores under load).
        let any_drained = self.inner.fstats.ids().iter().any(|id| {
            self.inner
                .fstats
                .snapshot(*id)
                .is_some_and(|s| s.stats.live_records == 0)
        });
        if !any_drained {
            return Ok(());
        }
        let mut tombs: HashMap<Vec<u8>, crate::index::TombMark> = HashMap::new();
        self.inner.idx.scan_tombs(|key, tm| {
            tombs.insert(key.to_vec(), tm);
        });
        if tombs.len() < self.inner.opts.tomb_proof_threshold {
            return Ok(());
        }
        #[derive(Default)]
        struct SeenFiles {
            first: u64,
            multi: bool,
            seen: bool,
        }
        let mut locs: HashMap<Vec<u8>, SeenFiles> = HashMap::with_capacity(tombs.len());
        let man = self.inner.manifest.lock().unwrap().clone();
        let ctx = *self.inner.ctx.read().unwrap();
        let store_id = man.store_id;
        let members = man.members.clone();
        let mut loader = Loader {
            dir: self.inner.dir.clone(),
            man,
            ring: self.inner.ring.clone(),
            comp: self.inner.comp.clone(),
            ctx,
            replay: None,
            torn_tail_id: 0,
            store_id,
        };
        for id in &members {
            loader.visit_validated_file(*id, |r, plain, _| {
                if r.deleted {
                    return Ok(());
                }
                let key = &plain[r.key_range.0..r.key_range.1];
                if !tombs.contains_key(key) {
                    return Ok(());
                }
                let e = locs.entry(key.to_vec()).or_default();
                if !e.seen {
                    e.first = *id;
                    e.seen = true;
                } else if e.first != *id {
                    e.multi = true;
                }
                Ok(())
            })?;
        }
        // A file drains when it holds no live records and every
        // marker in it is co-contained.
        let mut by_file: HashMap<u64, Vec<Vec<u8>>> = HashMap::new();
        for (key, tm) in &tombs {
            by_file.entry(tm.file).or_default().push(key.clone());
        }
        for (file, keys) in &by_file {
            let snap = match self.inner.fstats.snapshot(*file) {
                Some(s) => s,
                None => continue,
            };
            if snap.stats.live_records != 0 {
                continue;
            }
            let mut drain = true;
            for key in keys {
                if let Some(e) = locs.get(key) {
                    if e.multi || e.first != *file {
                        drain = false;
                        break;
                    }
                }
            }
            if !drain {
                continue;
            }
            for key in keys {
                let tm = tombs[key];
                if self.inner.idx.drop_tomb(key, tm.seq, tm.file).is_some() {
                    self.inner.fstats.tomb_dropped(tm.file);
                }
            }
        }
        Ok(())
    }

    /// Unlink sealed files with no live records and no live tombstones.
    fn delete_dead_files(&self) -> Result<()> {
        let active = self.inner.seg.active_id();
        let seg_dir = self.inner.dir.join(SEGMENTS_DIR_NAME);
        let mut removed = false;
        for id in self.inner.fstats.ids() {
            if id == active {
                continue;
            }
            let snap = match self.inner.fstats.snapshot(id) {
                Some(s) => s,
                None => continue,
            };
            if snap.state == SegmentState::Compacting {
                continue;
            }
            if snap.stats.live_records != 0 || snap.live_tombs != 0 {
                continue;
            }
            self.inner.fstats.set_state(id, SegmentState::Obsolete);
            if let Err(e) = self.publish_member_removal(id) {
                self.inner.fstats.set_state(id, SegmentState::Sealed);
                self.set_terminal(e.clone());
                return Err(err(
                    e.kind(),
                    format!("publish removal of segment {id}: {e}"),
                ));
            }
            if let Some(f) = &self.inner.faults {
                f.trip("delete")
                    .map_err(|e| err(ErrorKind::Io, format!("remove segment {id}: {e}")))?;
            }
            match fs::remove_file(seg_dir.join(segment_file_name(id))) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(err(ErrorKind::Io, format!("remove segment {id}: {e}"))),
            }
            self.inner.fstats.remove(id);
            removed = true;
        }
        if removed {
            if let Some(f) = &self.inner.faults {
                f.trip("dirsync")
                    .map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))?;
            }
            dir_sync(&seg_dir)
                .map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))?;
        }
        Ok(())
    }

    /// Rewrite the oldest eligible sealed segment into a replacement,
    /// publish the swap, then unlink the victim. Nil when nothing is
    /// eligible.
    fn compact_one(&self) -> Result<()> {
        let id = match self.pick_compaction() {
            Some(id) => id,
            None => return Ok(()),
        };
        self.inner.fstats.set_state(id, SegmentState::Compacting);
        let result = (|| -> Result<()> {
            let live = self.select_live(id)?;
            if live.is_empty() {
                return self.delete_one_file(id);
            }
            self.rewrite_live(id, live)
        })();
        if let Some(snap) = self.inner.fstats.snapshot(id) {
            if snap.state == SegmentState::Compacting {
                self.inner.fstats.set_state(id, SegmentState::Sealed);
            }
        }
        result
    }

    /// Select the oldest sealed segment whose live-value-byte ratio is
    /// at or below the threshold.
    ///
    /// Tombstones are excluded from the ratio: a tombstone costs ~100
    /// bytes to rewrite but counts as a full live record under a count
    /// ratio, so a few hundred markers (tens of KiB) can veto
    /// compacting a file that is 99%+ garbage by bytes. Those stranded
    /// markers pin their file from deletion (which needs zero live
    /// tombstones) while their file's survival pins younger tomb files
    /// from draining (tomb drops need co-contained values), gridlocking
    /// reclamation behind cross-file delete chains (retention prunes,
    /// snapshot chunk deletes, blob-piece cleanup) with reclaim a
    /// permanent no-op. Files with no live value records stay on the
    /// deletion path and are never rewritten.
    fn pick_compaction(&self) -> Option<u64> {
        let active = self.inner.seg.active_id();
        let mut best: Option<u64> = None;
        for id in self.inner.fstats.ids() {
            if id == active {
                continue;
            }
            let snap = self.inner.fstats.snapshot(id)?;
            if snap.state != SegmentState::Sealed {
                continue;
            }
            if snap.stats.live_records == 0 {
                continue;
            }
            let total = snap.stats.total_bytes;
            if total == 0 {
                continue;
            }
            if snap.stats.live_bytes as f64 / total as f64 > self.inner.opts.compaction_threshold {
                continue;
            }
            if best.map(|b| id < b).unwrap_or(true) {
                best = Some(id);
            }
        }
        best
    }

    /// Scan a segment's validated groups and keep records the index
    /// still points at exactly.
    fn select_live(&self, id: u64) -> Result<Vec<RewriteCandidate>> {
        let man = self.inner.manifest.lock().unwrap().clone();
        let ctx = *self.inner.ctx.read().unwrap();
        let store_id = man.store_id;
        let mut loader = Loader {
            dir: self.inner.dir.clone(),
            man,
            ring: self.inner.ring.clone(),
            comp: self.inner.comp.clone(),
            ctx,
            replay: None,
            torn_tail_id: 0,
            store_id,
        };
        let mut out = Vec::new();
        loader.visit_validated_file(id, |r, plain, loc| {
            let key = &plain[r.key_range.0..r.key_range.1];
            let val = &plain[r.val_range.0..r.val_range.1];
            if r.deleted {
                if self.inner.idx.tomb_live(key, r.seq, id) {
                    out.push(RewriteCandidate {
                        rec: PendingRecord {
                            key: key.to_vec(),
                            value: Vec::new(),
                            seq: r.seq,
                            tomb: true,
                            group: 0,
                            size: key.len() + RECORD_OVERHEAD,
                        },
                        loc: Location {
                            file_id: id,
                            block_id: 0,
                            block_offset: 0,
                            record_index: 0,
                            sequence: r.seq,
                            size: 0,
                        },
                    });
                }
                return Ok(());
            }
            if self.inner.idx.points_at(key, loc) {
                out.push(RewriteCandidate {
                    rec: PendingRecord {
                        key: key.to_vec(),
                        value: val.to_vec(),
                        seq: r.seq,
                        tomb: false,
                        group: 0,
                        size: key.len() + val.len() + RECORD_OVERHEAD,
                    },
                    loc,
                });
            }
            Ok(())
        })?;
        Ok(out)
    }

    /// Copy live records into a staged replacement file preserving
    /// logical sequences, publish the swap, install survivors with
    /// exact-identity rechecks, and unlink the victim.
    ///
    /// Staging failures are ordinary errors; publication failures are
    /// terminal.
    fn rewrite_live(&self, victim_id: u64, live: Vec<RewriteCandidate>) -> Result<()> {
        self.ensure_fresh_key()?;
        let _quiesce = self.inner.seal_quiesce.read().unwrap();
        let (key_id, key) = self
            .seal_data_key()
            .ok_or_else(|| err(ErrorKind::NoCurrentKey, "no current data key"))?;
        let start = std::time::SystemTime::now();
        let result = self.rewrite_live_sealed(victim_id, live, key_id, &key);
        self.release_seal_key(key_id);
        self.inner.compaction_nanos.fetch_add(
            start.elapsed().map(|d| d.as_nanos() as u64).unwrap_or(0),
            Ordering::Relaxed,
        );
        result
    }

    fn rewrite_live_sealed(
        &self,
        victim_id: u64,
        live: Vec<RewriteCandidate>,
        key_id: u32,
        key: &[u8; 32],
    ) -> Result<()> {
        // Reserve the replacement id up front from the shared allocator.
        let new_id = self.allocate_file_id();
        let max_body = (self.inner.opts.max_block_bytes as i64
            - BLOCK_HEADER_LEN as i64
            - self.inner.crypt.overhead() as i64
            - 1024)
            .max(1) as usize;
        let seg_dir = self.inner.dir.join(SEGMENTS_DIR_NAME);
        let mut staged = StagedFile::create(&seg_dir, &format!(".cmp-{victim_id}-"))?;
        // Chunk candidates into bound-fitting replacement groups.
        struct Chunk {
            recs: Vec<PendingRecord>,
            expected: Vec<Location>,
        }
        let mut chunks: Vec<Chunk> = Vec::new();
        let mut cur = Chunk {
            recs: Vec::new(),
            expected: Vec::new(),
        };
        let mut cur_size: i64 = 0;
        for c in live {
            let rs = c.rec.size as i64 + 32;
            if !cur.recs.is_empty() && cur_size + rs > self.inner.opts.max_atomic_batch_bytes {
                chunks.push(cur);
                cur = Chunk {
                    recs: Vec::new(),
                    expected: Vec::new(),
                };
                cur_size = 0;
            }
            cur.recs.push(c.rec);
            cur.expected.push(c.loc);
            cur_size += rs;
        }
        if !cur.recs.is_empty() {
            chunks.push(cur);
        }
        if let Some(f) = &self.inner.faults {
            f.trip("compact")
                .map_err(|e| err(ErrorKind::Io, format!("stage replacement: {e}")))?;
        }
        struct InstalledRec {
            rec: PendingRecord,
            expected: Location,
            block_id: u64,
            off: i64,
            index: u32,
        }
        let mut installed: Vec<InstalledRec> = Vec::new();
        let mut staged_off: i64 = 0;
        let mut max_compacted_group: u64 = 0;
        let nchunks = chunks.len();
        let ctx = *self.inner.ctx.read().unwrap();
        for mut ch in chunks {
            let gid = {
                let _a = self.inner.admission.lock().unwrap();
                self.inner.group_seq.fetch_add(1, Ordering::SeqCst) + 1
            };
            // Rewrite groups carry the records' existing sequences;
            // tag them with the new group id for tracing.
            for r in ch.recs.iter_mut() {
                r.group = gid;
            }
            let groups = split_blocks(
                &ch.recs,
                self.inner.opts.target_block_bytes,
                max_body,
                self.inner.opts.max_records_per_block,
            );
            let n_data = groups.len();
            let first_block = self
                .inner
                .next_block_id
                .fetch_add((n_data + 1) as u64, Ordering::SeqCst)
                + 1;
            let (frames, body_lens) =
                self.build_frames(gid, &groups, key_id, key, first_block, ctx)?;
            let total_records: u32 = groups.iter().map(|b| b.len() as u32).sum();
            let (comp_frame, _) = self.build_completion_block(
                gid,
                &frames,
                &body_lens,
                total_records,
                key_id,
                key,
                first_block + n_data as u64,
                ctx,
            )?;
            for (i, recs) in groups.iter().enumerate() {
                for (j, r) in recs.iter().enumerate() {
                    installed.push(InstalledRec {
                        rec: r.clone(),
                        expected: ch.expected.remove(0),
                        block_id: first_block + i as u64,
                        off: staged_off,
                        index: j as u32,
                    });
                }
                staged.write_frame(&frames[i])?;
                staged_off += frames[i].len() as i64;
            }
            staged.write_frame(&comp_frame)?;
            staged_off += comp_frame.len() as i64;
            self.inner.groups_accepted.fetch_add(1, Ordering::Relaxed);
            max_compacted_group = max_compacted_group.max(gid);
        }
        if let Some(f) = &self.inner.faults {
            f.trip("sync")
                .map_err(|e| err(ErrorKind::Io, format!("sync replacement: {e}")))?;
        }
        let staged_path = staged.sync_and_close()?;
        let staged_written = fs::metadata(&staged_path).map(|m| m.len()).unwrap_or(0);
        let final_path = seg_dir.join(segment_file_name(new_id));
        if let Some(f) = &self.inner.faults {
            f.trip("rename")
                .map_err(|e| err(ErrorKind::Io, format!("publish replacement file: {e}")))?;
        }
        fs::rename(&staged_path, &final_path)
            .map_err(|e| err(ErrorKind::Io, format!("publish replacement file: {e}")))?;
        if let Some(f) = &self.inner.faults {
            f.trip("dirsync")
                .map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))?;
        }
        dir_sync(&seg_dir).map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))?;
        if let Err(e) = self.publish_replacement(new_id, victim_id) {
            self.set_terminal(e.clone());
            return Err(err(e.kind(), format!("publish replacement: {e}")));
        }
        self.inner
            .fstats
            .register(new_id, std::time::SystemTime::now());
        self.inner.fstats.set_state(new_id, SegmentState::Sealed);
        // Install survivors with exact-identity rechecks.
        let mut n = 0;
        for ins in &installed {
            let size = (ins.rec.key.len() + ins.rec.value.len() + 17) as u32;
            let loc = Location {
                file_id: new_id,
                block_id: ins.block_id,
                block_offset: ins.off,
                record_index: ins.index,
                sequence: ins.rec.seq,
                size,
            };
            if self.inner.idx.commit_rewrite(
                &self.inner.fstats,
                &ins.rec.key,
                ins.expected,
                ins.rec.tomb,
                loc,
                size,
            ) {
                n += 1;
            }
        }
        if n == 0 {
            // Everything lost its race: unpublish the all-dead file.
            if let Err(e) = self.publish_member_removal(new_id) {
                self.set_terminal(e.clone());
                return Err(e);
            }
            let _ = fs::remove_file(&final_path);
            self.inner.fstats.remove(new_id);
            let _ = dir_sync(&seg_dir);
        } else {
            self.inner
                .groups_written
                .fetch_add(nchunks as u64, Ordering::Relaxed);
            self.inner
                .groups_synced
                .fetch_add(nchunks as u64, Ordering::Relaxed);
            self.note_appended(max_compacted_group);
        }
        self.inner.compactions.fetch_add(1, Ordering::Relaxed);
        self.inner
            .compaction_bytes
            .fetch_add(staged_written, Ordering::Relaxed);
        // The victim is already unpublished by the swap; unlink it.
        if let Some(f) = &self.inner.faults {
            f.trip("delete")
                .map_err(|e| err(ErrorKind::Io, format!("remove segment {victim_id}: {e}")))?;
        }
        match fs::remove_file(seg_dir.join(segment_file_name(victim_id))) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(err(
                    ErrorKind::Io,
                    format!("remove segment {victim_id}: {e}"),
                ))
            }
        }
        self.inner.fstats.remove(victim_id);
        if let Some(f) = &self.inner.faults {
            f.trip("dirsync")
                .map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))?;
        }
        dir_sync(&seg_dir).map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))
    }

    /// Unlink a fully dead segment after compaction.
    fn delete_one_file(&self, id: u64) -> Result<()> {
        if id == self.inner.seg.active_id() {
            return Ok(());
        }
        let snap = match self.inner.fstats.snapshot(id) {
            Some(s) => s,
            None => return Ok(()),
        };
        if snap.stats.live_records != 0 || snap.live_tombs != 0 {
            return Ok(());
        }
        self.inner.fstats.set_state(id, SegmentState::Obsolete);
        let seg_dir = self.inner.dir.join(SEGMENTS_DIR_NAME);
        if let Err(e) = self.publish_member_removal(id) {
            self.inner.fstats.set_state(id, SegmentState::Sealed);
            self.set_terminal(e.clone());
            return Err(err(
                e.kind(),
                format!("publish removal of segment {id}: {e}"),
            ));
        }
        if let Some(f) = &self.inner.faults {
            f.trip("delete")
                .map_err(|e| err(ErrorKind::Io, format!("remove segment {id}: {e}")))?;
        }
        match fs::remove_file(seg_dir.join(segment_file_name(id))) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.inner.fstats.set_state(id, SegmentState::Sealed);
                return Ok(());
            }
            Err(e) => {
                self.inner.fstats.set_state(id, SegmentState::Sealed);
                return Err(err(ErrorKind::Io, format!("remove segment {id}: {e}")));
            }
        }
        self.inner.fstats.remove(id);
        if let Some(f) = &self.inner.faults {
            f.trip("dirsync")
                .map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))?;
        }
        dir_sync(&seg_dir).map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))
    }
}

/// A record selected for rewriting with its expected current location.
struct RewriteCandidate {
    rec: PendingRecord,
    /// Expected winner for values; FileID+Sequence for tombs.
    loc: Location,
}

/// Bytes available to the caller on `dir`'s filesystem.
#[cfg(unix)]
pub(crate) fn free_bytes(dir: &std::path::Path) -> Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c = CString::new(dir.as_os_str().as_bytes())
        .map_err(|e| err(ErrorKind::Io, format!("statfs {}: {e}", dir.display())))?;
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: statfs writes a struct we own; path is a valid C string.
    let rc = unsafe { libc::statfs(c.as_ptr(), &mut st) };
    if rc != 0 {
        return Err(err(
            ErrorKind::Io,
            format!(
                "statfs {}: {}",
                dir.display(),
                std::io::Error::last_os_error()
            ),
        ));
    }
    // Mirror Go's signed check (defensive: kernel values are never negative).
    let avail = st.f_bavail as i64;
    let bsize = st.f_bsize as i64;
    if avail < 0 || bsize < 0 {
        return Ok(0);
    }
    Ok(avail as u64 * bsize as u64)
}

/// Non-unix fallback: no cheap free-space probe; treat as sufficient.
#[cfg(not(unix))]
pub(crate) fn free_bytes(_dir: &std::path::Path) -> Result<u64> {
    Ok(u64::MAX)
}
