package spool

import "sort"

// Stats reports store counters. All values are cheap snapshots;
// ratios guard against zero denominators.
type Stats struct {
	// Keys is the live (non-deleted) key count.
	Keys uint64
	// Segments is the tracked segment file count.
	Segments uint64

	// LiveRecords counts value records the index points at.
	LiveRecords uint64
	// DeadRecords counts superseded, stale and dropped records.
	DeadRecords uint64

	// DiskBytes sums tracked segment content bytes.
	DiskBytes uint64
	// LiveBytes sums live value record bytes.
	LiveBytes uint64

	// PendingRecords counts buffered plus in-flight records.
	PendingRecords uint64
	// PendingBytes counts buffered plus in-flight bytes.
	PendingBytes uint64

	// BlocksWritten counts committed blocks (flushes and compactions).
	BlocksWritten uint64
	// BytesWritten counts committed frame bytes on disk.
	BytesWritten uint64

	// CompressionRatio is uncompressed over compressed block bytes
	// (1.0 means no compression measured yet).
	CompressionRatio float64

	// Compactions counts completed compaction rewrites.
	Compactions uint64
	// CompactionBytes counts bytes rewritten by compaction.
	CompactionBytes uint64

	// TruncatedTails counts torn tails found during loads.
	TruncatedTails uint64
	// FlushErrors counts failed background flushes (retried).
	FlushErrors uint64
	// ReclaimErrors counts failed background reclamation passes.
	ReclaimErrors uint64

	// GroupsAccepted counts admitted commit groups.
	GroupsAccepted uint64
	// GroupsWritten counts groups appended and committed.
	GroupsWritten uint64
	// GroupsSynced counts groups committed at Sync durability.
	GroupsSynced uint64
	// GroupsDiscarded counts incomplete tail groups discarded in loads.
	GroupsDiscarded uint64
	// PendingGroups counts admitted groups awaiting commit.
	PendingGroups uint64
	// Commits counts Commit calls admitted (async and sync).
	Commits uint64
	// Syncs counts completed Sync/Flush barriers.
	Syncs uint64
	// StorageFailures counts terminal storage failures (0 or 1).
	StorageFailures uint64
	// Rotations counts completed data-key rotations (manual and
	// age-triggered).
	Rotations uint64
	// Checkpoints counts registered backup checkpoints.
	Checkpoints uint64
	// CommitNanos is the cumulative Commit call time (including
	// backpressure and durability waits); divide by Commits for
	// the mean.
	CommitNanos uint64
	// SyncNanos is the cumulative Sync/Flush barrier time; divide
	// by Syncs for the mean.
	SyncNanos uint64
	// Flushes counts flushInternal admissions (background and
	// explicit, including barriers' inner flush).
	Flushes uint64
	// FlushNanos is the cumulative flushInternal time.
	FlushNanos uint64
	// Reclaims counts completed Reclaim passes.
	Reclaims uint64
	// ReclaimNanos is the cumulative Reclaim time.
	ReclaimNanos uint64
	// CompactionNanos is the cumulative file-rewrite time; divide
	// by Compactions for the mean.
	CompactionNanos uint64
	// IndexBytesEstimate approximates index memory: key bytes plus
	// a per-entry allowance for locations and map overhead.
	IndexBytesEstimate uint64
	// WorkerBytesMax bounds worker-held buffers: Workers blocks
	// built at once, each transiently holding plaintext,
	// compressed, and sealed copies up to MaxBlockBytes each.
	WorkerBytesMax uint64
}

// Stats snapshots the store's counters.
func (s *Store) Stats() Stats {
	segments, _, live, dead, totalBytes, liveBytes := s.fstats.aggregate()
	st := Stats{
		Keys:               s.idx.keyCount(),
		Segments:           segments,
		LiveRecords:        live,
		DeadRecords:        dead,
		DiskBytes:          totalBytes,
		LiveBytes:          liveBytes,
		BlocksWritten:      s.blocksWritten.Load(),
		BytesWritten:       s.bytesWritten.Load(),
		Compactions:        s.compactions.Load(),
		CompactionBytes:    s.compactionBytes.Load(),
		TruncatedTails:     s.truncatedTails.Load(),
		FlushErrors:        s.flushErrors.Load(),
		ReclaimErrors:      s.reclaimErrors.Load(),
		GroupsAccepted:     s.groupsAccepted.Load(),
		GroupsWritten:      s.groupsWritten.Load(),
		GroupsSynced:       s.groupsSynced.Load(),
		GroupsDiscarded:    s.groupsDiscarded.Load(),
		Commits:            s.commits.Load(),
		Syncs:              s.syncs.Load(),
		StorageFailures:    s.storageFailures.Load(),
		Rotations:          s.rotations.Load(),
		Checkpoints:        uint64(len(s.ListCheckpoints())),
		CommitNanos:        s.commitNanos.Load(),
		SyncNanos:          s.syncNanos.Load(),
		Flushes:            s.flushes.Load(),
		FlushNanos:         s.flushNanos.Load(),
		Reclaims:           s.reclaims.Load(),
		ReclaimNanos:       s.reclaimNanos.Load(),
		CompactionNanos:    s.compactionNanos.Load(),
		IndexBytesEstimate: s.idx.memoryEstimate(),
		WorkerBytesMax:     uint64(s.workers) * uint64(s.opts.MaxBlockBytes) * 3,
	}
	if n := s.pendingRecords.Load(); n > 0 {
		st.PendingRecords = uint64(n)
	}
	if n := s.pendingBytes.Load(); n > 0 {
		st.PendingBytes = uint64(n)
	}
	if n := s.pendingGroups.Load(); n > 0 {
		st.PendingGroups = uint64(n)
	}
	if comp, uncomp := s.bytesWritten.Load(), s.bytesUncompressed.Load(); comp > 0 {
		st.CompressionRatio = float64(uncomp) / float64(comp)
	} else {
		st.CompressionRatio = 1.0
	}
	return st
}

// StoreInfo reports store identity, effective algorithms, and
// sequence/epoch/membership positions for diagnostics.
type StoreInfo struct {
	// StoreID is the immutable random store identity.
	StoreID [16]byte
	// Encrypted reports whether blocks are sealed.
	Encrypted bool
	// Encryption and Compression are the effective algorithms.
	Encryption  Encryption
	Compression Compression
	// WrappingKeyID is the authenticated wrapping id ("" when plain).
	WrappingKeyID string
	// Generation is the authoritative manifest generation.
	Generation uint64
	// Epoch is the sequence epoch of this open.
	Epoch uint64
	// LastGroupID is the newest admitted group (0 when none).
	LastGroupID uint64
	// NextFileID and ActiveFileID position the segment writer.
	NextFileID   uint64
	ActiveFileID uint64
}

// Info snapshots store identity and positions. It touches memory
// only.
func (s *Store) Info() StoreInfo {
	info := StoreInfo{
		Encrypted:     s.ring != nil,
		Encryption:    s.opts.Encryption,
		Compression:   s.opts.Compression,
		WrappingKeyID: s.wrapID,
		Epoch:         s.seqBase >> epochShift,
		LastGroupID:   s.lastGroup.Load(),
	}
	s.manifestMu.Lock()
	info.StoreID = s.man.storeID
	info.Generation = s.man.generation
	info.NextFileID = s.man.nextFileID
	info.ActiveFileID = s.man.activeFileID
	s.manifestMu.Unlock()
	return info
}

// FileStat is a per-file diagnostic snapshot: live/dead ratios and
// lifecycle state.
type FileStat struct {
	FileID       uint64
	State        SegmentState
	TotalRecords uint64
	LiveRecords  uint64
	DeadRecords  uint64
	TotalBytes   uint64
	LiveBytes    uint64
	LiveTombs    uint64
}

// FileStats snapshots per-file statistics ascending by file id.
func (s *Store) FileStats() []FileStat {
	ids := s.fstats.ids()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]FileStat, 0, len(ids))
	for _, id := range ids {
		snap, ok := s.fstats.snapshot(id)
		if !ok {
			continue
		}
		out = append(out, FileStat{
			FileID:       id,
			State:        snap.state,
			TotalRecords: snap.stats.TotalRecords,
			LiveRecords:  snap.stats.LiveRecords,
			DeadRecords:  snap.stats.DeadRecords,
			TotalBytes:   snap.stats.TotalBytes,
			LiveBytes:    snap.stats.LiveBytes,
			LiveTombs:    snap.liveTombs,
		})
	}
	return out
}
