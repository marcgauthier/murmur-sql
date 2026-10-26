package murmur

import (
	"sync"
	"time"
)

// OpenProgressReporter delivers reload/startup progress without locking
// storage or SQLite data paths. Nil-safe when no callback is set.
type OpenProgressReporter struct {
	mu         sync.Mutex
	startedAt  time.Time
	phaseSince time.Time
	lastEmit   time.Time
	progress   OpenProgress
	stages     OpenStageTimings
	callback   func(OpenProgress)
	has        bool
}

// NewOpenProgressReporter builds a reporter around callback.
func NewOpenProgressReporter(callback func(OpenProgress)) *OpenProgressReporter {
	if callback == nil {
		return &OpenProgressReporter{}
	}
	now := time.Now()
	initial := OpenProgress{Phase: PhaseOpening}
	r := &OpenProgressReporter{
		startedAt: now, phaseSince: now, lastEmit: now,
		progress: initial, callback: callback, has: true,
	}
	callback(initial)
	return r
}

func (r *OpenProgressReporter) updateTimings(now time.Time) {
	r.progress.Elapsed = now.Sub(r.startedAt)
	if r.progress.Elapsed < 0 {
		r.progress.Elapsed = 0
	}
	r.progress.PhaseElapsed = now.Sub(r.phaseSince)
	if r.progress.PhaseElapsed < 0 {
		r.progress.PhaseElapsed = 0
	}
}

func (r *OpenProgressReporter) emitLocked(now time.Time) {
	r.updateTimings(now)
	r.lastEmit = now
	snap := r.progress
	r.callback(snap)
}

// Phase moves to a new phase, emitting immediately on change.
func (r *OpenProgressReporter) Phase(phase OpenPhase) {
	if !r.has {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.progress.Phase != phase {
		now := time.Now()
		r.progress.Phase = phase
		r.phaseSince = now
		r.progress.CurrentTable = nil
		r.emitLocked(now)
	}
}

// SetTable names the table under work, emitting immediately.
func (r *OpenProgressReporter) SetTable(table string) {
	if !r.has {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.progress.CurrentTable = &table
	r.emitLocked(now)
}

// AddProcessedItems accumulates items, emitting at most every 200ms.
func (r *OpenProgressReporter) AddProcessedItems(delta uint64) {
	if !r.has {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progress.ProcessedItems += delta
	now := time.Now()
	if now.Sub(r.lastEmit) >= 200*time.Millisecond {
		r.emitLocked(now)
	}
}

// AddRows accumulates row outcomes, emitting at most every 200ms.
func (r *OpenProgressReporter) AddRows(inserted, skipped uint64) {
	if !r.has {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progress.RowsInserted += inserted
	r.progress.RowsSkipped += skipped
	r.progress.ProcessedItems += inserted + skipped
	now := time.Now()
	if now.Sub(r.lastEmit) >= 200*time.Millisecond {
		r.emitLocked(now)
	}
}

// RecordStage accumulates d into the named open stage. Unknown stages
// are ignored. Always recorded, even for silent reporters, so stage
// timings stay available without a progress callback.
func (r *OpenProgressReporter) RecordStage(stage OpenStage, d time.Duration) {
	if d < 0 {
		d = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch stage {
	case StageSpoolOpen:
		r.stages.SpoolOpen += d
	case StageRowInsert:
		r.stages.RowInsert += d
	case StageIndexBuild:
		r.stages.IndexBuild += d
	case StageIntegrityCheck:
		r.stages.IntegrityCheck += d
	case StageSearchInit:
		r.stages.SearchInit += d
	}
}

// StageTimings returns the accumulated per-stage open timings.
func (r *OpenProgressReporter) StageTimings() OpenStageTimings {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stages
}

// Snapshot returns current progress, if reporting.
func (r *OpenProgressReporter) Snapshot() *OpenProgress {
	if !r.has {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updateTimings(time.Now())
	snap := r.progress
	return &snap
}

// Finish emits the terminal Ready/Failed event.
func (r *OpenProgressReporter) Finish(err error) *OpenProgress {
	if !r.has {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if err == nil {
		r.progress.Phase = PhaseReady
		r.progress.Error = nil
	} else {
		r.progress.Phase = PhaseFailed
		msg := err.Error()
		r.progress.Error = &msg
	}
	r.progress.CurrentTable = nil
	r.emitLocked(now)
	snap := r.progress
	return &snap
}
