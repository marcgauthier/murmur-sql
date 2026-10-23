package spool

import (
	"sync"
	"time"
)

// SegmentState tracks a segment file's lifecycle. Only the active
// segment accepts appends; sealed segments are immutable.
type SegmentState uint8

const (
	// SegmentActive accepts new appends.
	SegmentActive SegmentState = iota
	// SegmentSealed is immutable and readable.
	SegmentSealed
	// SegmentCompacting is being rewritten; reclamation must skip it.
	SegmentCompacting
	// SegmentObsolete is fully reclaimed and awaiting unlink.
	SegmentObsolete
)

// String returns a human-readable segment state.
func (s SegmentState) String() string {
	switch s {
	case SegmentActive:
		return "active"
	case SegmentSealed:
		return "sealed"
	case SegmentCompacting:
		return "compacting"
	case SegmentObsolete:
		return "obsolete"
	default:
		return "unknown"
	}
}

// FileStats counts a segment's records and bytes. Live counters
// change as keys are replaced; no disk scan is needed to judge a
// file's reclaimability.
type FileStats struct {
	FileID uint64

	TotalRecords uint64
	LiveRecords  uint64
	DeadRecords  uint64

	TotalBytes uint64
	LiveBytes  uint64

	CreatedAt time.Time
}

// fileInfo extends FileStats with reclamation internals.
type fileInfo struct {
	stats FileStats
	state SegmentState
	// liveTombs counts tombstone records still winning their keys.
	liveTombs uint64
}

// fileStats guards per-segment counters. Lock order: index shard ->
// fileStats (see index). Never acquire an index shard lock while
// holding this mutex.
type fileStats struct {
	mu    sync.RWMutex
	files map[uint64]*fileInfo
}

func newFileStats() *fileStats {
	return &fileStats{files: make(map[uint64]*fileInfo)}
}

// register adds a file id if absent.
func (fs *fileStats) register(id uint64, created time.Time) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, ok := fs.files[id]; !ok {
		fs.files[id] = &fileInfo{stats: FileStats{FileID: id, CreatedAt: created}}
	}
}

// setState updates a file's lifecycle state.
func (fs *fileStats) setState(id uint64, st SegmentState) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if f, ok := fs.files[id]; ok {
		f.state = st
	}
}

// valueInstalled accounts a newly visible value record.
func (fs *fileStats) valueInstalled(id uint64, size uint32) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f := fs.ensureLocked(id)
	f.stats.TotalRecords++
	f.stats.TotalBytes += uint64(size)
	f.stats.LiveRecords++
	f.stats.LiveBytes += uint64(size)
}

// valueEvicted accounts a superseded value record.
func (fs *fileStats) valueEvicted(id uint64, size uint32) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if f, ok := fs.files[id]; ok {
		if f.stats.LiveRecords > 0 {
			f.stats.LiveRecords--
		}
		if f.stats.LiveBytes >= uint64(size) {
			f.stats.LiveBytes -= uint64(size)
		} else {
			f.stats.LiveBytes = 0
		}
		f.stats.DeadRecords++
	}
}

// tombInstalled accounts a newly winning tombstone.
func (fs *fileStats) tombInstalled(id uint64, size uint32) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f := fs.ensureLocked(id)
	f.stats.TotalRecords++
	f.stats.TotalBytes += uint64(size)
	f.liveTombs++
}

// tombEvicted accounts a superseded tombstone.
func (fs *fileStats) tombEvicted(id uint64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if f, ok := fs.files[id]; ok {
		if f.liveTombs > 0 {
			f.liveTombs--
		}
		f.stats.DeadRecords++
	}
}

// tombDropped accounts a reclaimed tombstone: it stops being live but
// still occupies space until its file is unlinked.
func (fs *fileStats) tombDropped(id uint64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if f, ok := fs.files[id]; ok {
		if f.liveTombs > 0 {
			f.liveTombs--
		}
		f.stats.DeadRecords++
	}
}

// recordDead accounts a stillborn record (stale on arrival): space
// used, never visible.
func (fs *fileStats) recordDead(id uint64, size uint32) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f := fs.ensureLocked(id)
	f.stats.TotalRecords++
	f.stats.TotalBytes += uint64(size)
	f.stats.DeadRecords++
}

// ensureLocked returns the file entry, registering a ghost on demand
// for late-arriving callers. Ghosts carry no live data and are
// dropped by the next reclamation pass.
func (fs *fileStats) ensureLocked(id uint64) *fileInfo {
	f, ok := fs.files[id]
	if !ok {
		f = &fileInfo{stats: FileStats{FileID: id}}
		fs.files[id] = f
	}
	return f
}

// snapshot returns a copy of one file's info.
func (fs *fileStats) snapshot(id uint64) (fileInfo, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	f, ok := fs.files[id]
	if !ok {
		return fileInfo{}, false
	}
	return *f, true
}

// ids returns all tracked file ids.
func (fs *fileStats) ids() []uint64 {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	out := make([]uint64, 0, len(fs.files))
	for id := range fs.files {
		out = append(out, id)
	}
	return out
}

// remove forgets a file after unlinking.
func (fs *fileStats) remove(id uint64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.files, id)
}

// aggregate sums counters for Stats.
func (fs *fileStats) aggregate() (segments, total, live, dead, totalBytes, liveBytes uint64) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	segments = uint64(len(fs.files))
	for _, f := range fs.files {
		total += f.stats.TotalRecords
		live += f.stats.LiveRecords
		dead += f.stats.DeadRecords
		totalBytes += f.stats.TotalBytes
		liveBytes += f.stats.LiveBytes
	}
	return segments, total, live, dead, totalBytes, liveBytes
}
