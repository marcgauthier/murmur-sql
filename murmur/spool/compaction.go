package spool

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// reclaimLoop ticks Reclaim on the configured interval.
func (s *Store) reclaimLoop() {
	defer s.closeWG.Done()
	ticker := time.NewTicker(s.opts.ReclaimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if s.maintActive.Load() {
				continue
			}
			if err := s.Reclaim(); err != nil {
				s.reclaimErrors.Add(1)
				s.recordReclaimErr(err)
			}
		}
	}
}

// recordReclaimErr stashes the latest background reclamation error.
func (s *Store) recordReclaimErr(err error) {
	s.flushErrMu.Lock()
	s.lastReclaimErr = err
	s.flushErrMu.Unlock()
}

// LastReclaimErr reports the most recent background reclamation
// error, if any.
func (s *Store) LastReclaimErr() error {
	s.flushErrMu.Lock()
	defer s.flushErrMu.Unlock()
	return s.lastReclaimErr
}

// Reclaim runs one pass: fence all in-flight batches, drop
// collectible tombstones, delete fully dead files and compact one
// eligible file. Concurrent calls serialize.
//
// The rewrite pass is skipped while writers keep the buffer over
// half full (foreground takes priority) and while free disk space
// is below CompactionMinFreeBytes. Tombstone drops and dead-file
// deletion always run: they are cheap and free space.
func (s *Store) Reclaim() error {
	_, err := s.reclaimPass()
	return err
}

// ReclaimStep runs a single reclamation pass like Reclaim and
// reports whether the pass did any work: dropped tombstones,
// deleted dead files, or rewrote a segment. Offline compaction
// loops it until it reports no work, which is a fixed point: with
// no concurrent writers, a pass that changes nothing leaves the
// store in a state where every later pass also changes nothing, so
// the loop terminates. Under concurrent writers new garbage can
// arrive between passes; cap the loop.
func (s *Store) ReclaimStep() (bool, error) {
	return s.reclaimPass()
}

func (s *Store) reclaimPass() (bool, error) {
	s.reclaimMu.Lock()
	defer s.reclaimMu.Unlock()
	if s.closed.Load() {
		return false, ErrClosed
	}
	if err := s.checkTerminal(); err != nil {
		return false, err
	}
	if err := s.checkMaint(); err != nil {
		return false, err
	}
	start := time.Now()
	defer func() {
		s.reclaims.Add(1)
		s.reclaimNanos.Add(uint64(time.Since(start)))
	}()
	// Snapshot write pressure before the fence drains the queue.
	pressured := s.pendingBytes.Load() > s.opts.MaxPendingBytes/2
	// Fence: admit everything pending as groups, then wait for every
	// group admitted up to and including ours, so no commit can
	// still reference the files evaluated below.
	id, err := s.flushInternal(s.opts.Durability, false)
	if err != nil {
		return false, err
	}
	if err := s.waitForGroup(id, false); err != nil {
		return false, err
	}
	worked := false
	if w, err := s.dropTombs(); err != nil {
		return worked, err
	} else if w {
		worked = true
	}
	if w, err := s.deleteDeadFiles(); err != nil {
		return worked, err
	} else if w {
		worked = true
	}
	if pressured {
		return worked, nil
	}
	if min := s.opts.CompactionMinFreeBytes; min > 0 {
		free, err := freeBytes(filepath.Join(s.dir, segmentsDirName))
		if err != nil {
			return worked, err
		}
		if free < uint64(min) {
			return worked, nil
		}
	}
	if w, err := s.compactOne(); err != nil {
		return worked, err
	} else if w {
		worked = true
	}
	return worked, nil
}

// dropTombs discards deletion markers only after proving no older
// value for the key remains in any surviving authoritative segment.
// Compaction relocates versions across files, so no file-age
// shortcut is sound. Once TombProofThreshold live markers
// accumulate, one store-wide scan collects the files holding values
// for tombstoned keys; a file whose live records are gone and whose
// markers' older values live only inside it drains with the file's
// deletion in the same pass.
func (s *Store) dropTombs() (bool, error) {
	tombs := make(map[string]tombMark)
	s.idx.scanTombs(func(key string, tm tombMark) {
		tombs[key] = tm
	})
	if len(tombs) < s.opts.TombProofThreshold {
		return false, nil
	}
	// Tombs only drop from drained (zero-live-record) files; when
	// every file still holds live data the full-spool validation
	// scan below cannot drop anything, so skip it (it costs seconds
	// on large stores under load).
	anyDrained := false
	for _, id := range s.fstats.ids() {
		if snap, ok := s.fstats.snapshot(id); ok && snap.stats.LiveRecords == 0 {
			anyDrained = true
			break
		}
	}
	if !anyDrained {
		return false, nil
	}

	// seenFiles tracks, per tombstoned key, value locations: the
	// first file seen plus whether a second distinct file appeared.
	type seenFiles struct {
		first uint64
		multi bool
		seen  bool
	}
	locs := make(map[string]seenFiles, len(tombs))
	l := &loader{dir: s.dir, man: s.man, ring: s.ring, comp: s.comp, ctx: s.ctx}
	s.manifestMu.Lock()
	members := append([]uint64(nil), s.man.members...)
	s.manifestMu.Unlock()
	for _, id := range members {
		_, _, _, _, err := l.visitValidatedFile(id, func(r Record, loc Location) error {
			if r.Deleted {
				return nil
			}
			k := string(r.Key)
			if _, ok := tombs[k]; !ok {
				return nil
			}
			e := locs[k]
			if !e.seen {
				e = seenFiles{first: id, seen: true}
			} else if e.first != id {
				e.multi = true
			}
			locs[k] = e
			return nil
		})
		if err != nil {
			return false, err
		}
	}
	// A file drains when it holds no live records and every marker
	// in it is co-contained: older values only inside the same file.
	byFile := make(map[uint64][]string)
	for key, tm := range tombs {
		byFile[tm.file] = append(byFile[tm.file], key)
	}
	dropped := false
	for file, keys := range byFile {
		snap, ok := s.fstats.snapshot(file)
		if !ok || snap.stats.LiveRecords != 0 {
			continue
		}
		drain := true
		for _, key := range keys {
			e := locs[key]
			if e.seen && (e.multi || e.first != file) {
				drain = false
				break
			}
		}
		if !drain {
			continue
		}
		for _, key := range keys {
			tm := tombs[key]
			if _, ok := s.idx.dropTomb(key, tm.seq, tm.file); ok {
				s.fstats.tombDropped(tm.file)
				dropped = true
			}
		}
	}
	return dropped, nil
}

// deleteDeadFiles unlinks sealed files with no live records and no
// live tombstones. The fence taken by Reclaim guarantees no commit
// can reference them afterwards. Membership is published before
// unlinking: a crash between publish and unlink leaves an unlisted
// file that open sweeps.
func (s *Store) deleteDeadFiles() (bool, error) {
	active := s.seg.activeID()
	segDir := filepath.Join(s.dir, segmentsDirName)
	removed := false
	for _, id := range s.fstats.ids() {
		if id == active {
			continue
		}
		snap, ok := s.fstats.snapshot(id)
		if !ok {
			continue
		}
		if snap.state == SegmentCompacting {
			continue
		}
		if snap.stats.LiveRecords != 0 || snap.liveTombs != 0 {
			continue
		}
		s.fstats.setState(id, SegmentObsolete)
		if err := s.publishMemberRemoval(id); err != nil {
			s.fstats.setState(id, SegmentSealed)
			s.setTerminal(err)
			return removed, fmt.Errorf("spool: publish removal of segment %d: %w", id, err)
		}
		if err := s.faults.trip("delete"); err != nil {
			return removed, fmt.Errorf("spool: remove segment %d: %w", id, err)
		}
		if err := os.Remove(filepath.Join(segDir, segmentFileName(id))); err != nil && !os.IsNotExist(err) {
			return removed, fmt.Errorf("spool: remove segment %d: %w", id, err)
		}
		s.fstats.remove(id)
		removed = true
	}
	if removed {
		if err := s.faults.trip("dirsync"); err != nil {
			return removed, fmt.Errorf("spool: sync segments dir: %w", err)
		}
		if err := dirSync(segDir); err != nil {
			return removed, fmt.Errorf("spool: sync segments dir: %w", err)
		}
	}
	return removed, nil
}

// compactOne rewrites the oldest eligible sealed segment into a
// replacement segment preserving logical sequences, publishes the
// swap under one manifest generation, then unlinks the victim. It
// reports whether it rewrote (or deleted) a segment.
func (s *Store) compactOne() (bool, error) {
	id, ok := s.pickCompaction()
	if !ok {
		return false, nil
	}
	s.fstats.setState(id, SegmentCompacting)
	defer func() {
		if snap, ok := s.fstats.snapshot(id); ok && snap.state == SegmentCompacting {
			s.fstats.setState(id, SegmentSealed)
		}
	}()
	live, err := s.selectLive(id)
	if err != nil {
		return false, err
	}
	if len(live) == 0 {
		// Fully dead already; deletion happens below.
		if err := s.deleteOneFile(id); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := s.rewriteLive(id, live); err != nil {
		return false, err
	}
	return true, nil
}

// pickCompaction selects the oldest sealed segment whose
// live-value-byte ratio is at or below the threshold.
func (s *Store) pickCompaction() (uint64, bool) {
	active := s.seg.activeID()
	var best uint64
	found := false
	for _, id := range s.fstats.ids() {
		if id == active {
			continue
		}
		snap, ok := s.fstats.snapshot(id)
		if !ok || snap.state != SegmentSealed {
			continue
		}
		// Files with no live value records stay on the deletion
		// path and are never rewritten.
		if snap.stats.LiveRecords == 0 {
			continue
		}
		// Files with no dead records would rewrite into
		// themselves: every record is still live, so a rewrite
		// reproduces the file byte-equivalent while burning a
		// full rewrite. Skipping them keeps reclamation
		// convergent: without this, a tomb-heavy file below the
		// tomb-proof threshold rewrites on every pass forever,
		// online and offline.
		if snap.stats.DeadRecords == 0 {
			continue
		}
		total := snap.stats.TotalBytes
		if total == 0 {
			continue
		}
		// Tombstones are excluded from the ratio: a tombstone costs
		// ~100 bytes to rewrite but counts as a full live record
		// under a count ratio, so a few hundred markers (tens of
		// KiB) can veto compacting a file that is 99%+ garbage by
		// bytes. Those stranded markers pin their file from
		// deletion (which needs zero live tombstones) while their
		// file's survival pins younger tomb files from draining
		// (tomb drops need co-contained values), gridlocking
		// reclamation behind cross-file delete chains with reclaim
		// a permanent no-op.
		if float64(snap.stats.LiveBytes)/float64(total) > s.opts.CompactionThreshold {
			continue
		}
		if !found || id < best {
			best, found = id, true
		}
	}
	return best, found
}

// rewriteCandidate is a record selected for rewriting with its
// expected current location for the commit recheck.
type rewriteCandidate struct {
	rec pendingRecord
	loc Location // expected winner for values; FileID+Sequence for tombs
}

// selectLive scans a segment's validated groups and keeps records
// the index still points at exactly. It is a filter; commit
// rechecks identity.
func (s *Store) selectLive(id uint64) ([]rewriteCandidate, error) {
	l := &loader{dir: s.dir, man: s.man, ring: s.ring, comp: s.comp, ctx: s.ctx}
	var out []rewriteCandidate
	_, _, _, _, err := l.visitValidatedFile(id, func(r Record, loc Location) error {
		key := string(r.Key)
		if r.Deleted {
			if s.idx.tombLive(key, r.Sequence, id) {
				out = append(out, rewriteCandidate{
					rec: pendingRecord{key: cloneBytes(r.Key), seq: r.Sequence, tomb: true, size: len(r.Key) + recordOverhead},
					loc: Location{FileID: id, Sequence: r.Sequence},
				})
			}
			return nil
		}
		if s.idx.pointsAt(key, loc) {
			out = append(out, rewriteCandidate{
				rec: pendingRecord{key: cloneBytes(r.Key), value: cloneBytes(r.Value), seq: r.Sequence, size: len(r.Key) + len(r.Value) + recordOverhead},
				loc: loc,
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("spool: compact segment %d: %w", id, err)
	}
	return out, nil
}

// rewriteLive copies live records into a staged replacement file
// preserving logical sequences, publishes the victim/replacement
// swap under one manifest generation, installs survivors with
// exact-identity rechecks, and unlinks the victim. Records that
// lost a race rewrite stillborn (counted dead).
//
// Staging failures are ordinary errors (nothing published, victim
// intact, staged files swept). Publication failures are terminal:
// authoritative membership is uncertain.
func (s *Store) rewriteLive(victimID uint64, live []rewriteCandidate) error {
	if err := s.ensureFreshKey(); err != nil {
		return err
	}
	keyID, key, ok := s.sealDataKey()
	if !ok {
		return errNoCurrentKey
	}
	defer s.releaseSealKey(keyID)
	start := time.Now()
	defer func() { s.compactionNanos.Add(uint64(time.Since(start))) }()
	// Reserve the replacement id up front from the shared
	// allocator: a writer rotation interleaving the slow staging
	// phase must never reuse this id.
	newID := s.allocateFileID()
	maxBody := s.opts.MaxBlockBytes - blockHeaderLen - s.crypt.overhead() - 1024
	segDir := filepath.Join(s.dir, segmentsDirName)
	staged, err := os.CreateTemp(segDir, fmt.Sprintf(".cmp-%d-.tmp-", victimID))
	if err != nil {
		return fmt.Errorf("spool: stage replacement: %w", err)
	}
	stagedName := staged.Name()
	defer os.Remove(stagedName) // success path renames away
	// Chunk candidates into bound-fitting replacement groups.
	type chunk struct {
		recs     []pendingRecord
		expected []Location
	}
	var chunks []chunk
	var cur chunk
	var curSize int64
	for _, c := range live {
		rs := int64(c.rec.size + 32)
		if len(cur.recs) > 0 && curSize+rs > s.opts.MaxAtomicBatchBytes {
			chunks = append(chunks, cur)
			cur = chunk{}
			curSize = 0
		}
		cur.recs = append(cur.recs, c.rec)
		cur.expected = append(cur.expected, c.loc)
		curSize += rs
	}
	if len(cur.recs) > 0 {
		chunks = append(chunks, cur)
	}
	if err := s.faults.trip("compact"); err != nil {
		staged.Close()
		return fmt.Errorf("spool: stage replacement: %w", err)
	}
	installed := make([]installedRec, 0, len(live))
	var written, stagedOff int64
	var maxCompactedGroup uint64
	for _, ch := range chunks {
		s.admissionMu.Lock()
		gid := s.groupSeq.Add(1)
		s.admissionMu.Unlock()
		groups := splitBlocks(ch.recs, s.opts.TargetBlockBytes, maxBody, s.opts.MaxRecordsPerBlock)
		nData := len(groups)
		firstBlock := s.nextBlockID.Add(uint64(nData + 1))
		firstBlock -= uint64(nData + 1)
		firstBlock++
		frames, bodyLens, err := s.buildFrames(gid, groups, keyID, key, firstBlock, s.ctx)
		if err != nil {
			staged.Close()
			return err
		}
		var totalRecords uint32
		for _, b := range groups {
			totalRecords += uint32(len(b))
		}
		compFrame, _, err := s.buildCompletionBlock(gid, frames, bodyLens, totalRecords, keyID, key, firstBlock+uint64(nData), s.ctx)
		if err != nil {
			staged.Close()
			return err
		}
		for _, f := range append(frames, compFrame) {
			if _, err := staged.Write(f); err != nil {
				staged.Close()
				return fmt.Errorf("spool: stage replacement: %w", err)
			}
			written += int64(len(f))
		}
		// Record install order with absolute staged-file offsets;
		// the replacement id is assigned below.
		for i, recs := range groups {
			for j := range recs {
				installed = append(installed, installedRec{
					rec:      recs[j],
					expected: ch.expected[0],
					blockID:  firstBlock + uint64(i),
					off:      stagedOff,
					index:    uint32(j),
				})
				ch.expected = ch.expected[1:]
			}
			stagedOff += int64(len(frames[i]))
		}
		stagedOff += int64(len(compFrame))
		s.groupsAccepted.Add(1)
		if gid > maxCompactedGroup {
			maxCompactedGroup = gid
		}
	}
	if err := s.faults.trip("sync"); err != nil {
		staged.Close()
		return fmt.Errorf("spool: sync replacement: %w", err)
	}
	if err := staged.Sync(); err != nil {
		staged.Close()
		return fmt.Errorf("spool: sync replacement: %w", err)
	}
	if err := staged.Close(); err != nil {
		return fmt.Errorf("spool: close replacement: %w", err)
	}
	final := filepath.Join(segDir, segmentFileName(newID))
	if err := s.faults.trip("rename"); err != nil {
		return fmt.Errorf("spool: publish replacement file: %w", err)
	}
	if err := os.Rename(stagedName, final); err != nil {
		return fmt.Errorf("spool: publish replacement file: %w", err)
	}
	if err := s.faults.trip("dirsync"); err != nil {
		return fmt.Errorf("spool: sync segments dir: %w", err)
	}
	if err := dirSync(segDir); err != nil {
		return fmt.Errorf("spool: sync segments dir: %w", err)
	}
	if err := s.publishReplacement(newID, victimID); err != nil {
		s.setTerminal(err)
		return fmt.Errorf("spool: publish replacement: %w", err)
	}
	s.fstats.register(newID, time.Now())
	s.fstats.setState(newID, SegmentSealed)
	// Install survivors with exact-identity rechecks; races
	// rewrite stillborn.
	n := 0
	for _, in := range installed {
		size := uint32(len(in.rec.key) + len(in.rec.value) + 17)
		loc := Location{FileID: newID, BlockID: in.blockID, BlockOffset: in.off, RecordIndex: in.index, Sequence: in.rec.seq, Size: size}
		if s.commitRewrite(string(in.rec.key), in.expected, in.rec.tomb, loc, size) {
			n++
		}
	}
	if n == 0 {
		// Everything lost its race: unpublish the all-dead file
		// instead of littering it for a later pass.
		if err := s.publishMemberRemoval(newID); err != nil {
			s.setTerminal(err)
			return err
		}
		os.Remove(final)
		s.fstats.remove(newID)
		dirSync(segDir)
	} else {
		s.groupsWritten.Add(uint64(len(chunks)))
		s.groupsSynced.Add(uint64(len(chunks)))
		s.noteAppended(maxCompactedGroup)
	}
	s.compactions.Add(1)
	s.compactionBytes.Add(uint64(written))
	// The victim is already unpublished by the swap; unlink it.
	if err := s.faults.trip("delete"); err != nil {
		return fmt.Errorf("spool: remove segment %d: %w", victimID, err)
	}
	if err := os.Remove(filepath.Join(segDir, segmentFileName(victimID))); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("spool: remove segment %d: %w", victimID, err)
	}
	s.fstats.remove(victimID)
	if err := s.faults.trip("dirsync"); err != nil {
		return fmt.Errorf("spool: sync segments dir: %w", err)
	}
	return dirSync(segDir)
}

// installedRec stages one replacement record for rechecked install.
type installedRec struct {
	rec      pendingRecord
	expected Location
	blockID  uint64
	off      int64
	index    uint32
}

// commitRewrite installs a rewritten record only when the index
// still resolves the key to exactly the expected old location.
// Anything else means a concurrent write won; the rewrite is
// stillborn. It reports whether the rewrite installed.
func (s *Store) commitRewrite(key string, expected Location, tomb bool, loc Location, size uint32) bool {
	sh := s.idx.shardFor([]byte(key))
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if tomb {
		tm, ok := sh.tombs[key]
		if !ok || tm.seq != expected.Sequence || tm.file != expected.FileID {
			s.fstats.recordDead(loc.FileID, size)
			return false
		}
		delete(sh.tombs, key)
		s.fstats.tombEvicted(expected.FileID)
		sh.tombs[key] = tombMark{seq: loc.Sequence, file: loc.FileID, loc: loc}
		s.fstats.tombInstalled(loc.FileID, size)
		return true
	}
	got, ok := sh.values[key]
	if !ok || got != expected {
		s.fstats.recordDead(loc.FileID, size)
		return false
	}
	delete(sh.values, key)
	s.fstats.valueEvicted(expected.FileID, got.Size)
	sh.values[key] = loc
	s.fstats.valueInstalled(loc.FileID, size)
	return true
}

// deleteOneFile unlinks a fully dead segment after compaction,
// publishing the removal first like deleteDeadFiles.
func (s *Store) deleteOneFile(id uint64) error {
	if id == s.seg.activeID() {
		return nil
	}
	snap, ok := s.fstats.snapshot(id)
	if !ok {
		return nil
	}
	if snap.stats.LiveRecords != 0 || snap.liveTombs != 0 {
		return nil
	}
	s.fstats.setState(id, SegmentObsolete)
	segDir := filepath.Join(s.dir, segmentsDirName)
	if err := s.publishMemberRemoval(id); err != nil {
		s.fstats.setState(id, SegmentSealed)
		s.setTerminal(err)
		return fmt.Errorf("spool: publish removal of segment %d: %w", id, err)
	}
	if err := s.faults.trip("delete"); err != nil {
		s.fstats.setState(id, SegmentSealed)
		return fmt.Errorf("spool: remove segment %d: %w", id, err)
	}
	if err := os.Remove(filepath.Join(segDir, segmentFileName(id))); err != nil && !os.IsNotExist(err) {
		s.fstats.setState(id, SegmentSealed)
		return fmt.Errorf("spool: remove segment %d: %w", id, err)
	}
	s.fstats.remove(id)
	if err := s.faults.trip("dirsync"); err != nil {
		return fmt.Errorf("spool: sync segments dir: %w", err)
	}
	return dirSync(segDir)
}
