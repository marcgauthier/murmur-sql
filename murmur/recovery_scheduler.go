package murmur

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/recovery"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// RecoveryBuildStatus is the last recovery-build outcome. Build errors
// are recorded here, never propagated as database errors.
type RecoveryBuildStatus struct {
	Running    bool
	Started    time.Time
	Finished   time.Time
	Generation uint64
	Rows       uint64
	Bytes      uint64
	Seconds    float64
	Err        string
}

// recoveryScheduler runs single-flight background recovery builds:
// one worker checkpoint request plus off-thread conversion. Builds
// are bounded by MaxBuildMemory (spilling to encrypted temp runs)
// and throttled by BuildInterval; every build failure fails closed
// (recorded in status, legacy path unaffected), including out-of-disk
// and unwritable-directory failures mid-build.
type recoveryScheduler struct {
	opts             RecoveryOptions
	worker           chan<- Work
	store            *spool.Store
	recoveryDir      string
	compressionLevel int

	mu        sync.Mutex
	building  bool
	stopped   bool
	lastStart time.Time
	last      RecoveryBuildStatus

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// cleanupStaleBuildDirs removes unpublished .building-* staging
// directories left by crashed builds. Published generations,
// CURRENT, and anything else are never touched. Best effort:
// errors are ignored because a leftover staging dir only wastes
// disk, and the next build uses a fresh random name.
func cleanupStaleBuildDirs(recoveryDir string) {
	entries, err := os.ReadDir(recoveryDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || !strings.HasPrefix(name, recovery.BuildingPrefix) {
			continue
		}
		// Refuse symlinks and anything outside the recovery dir.
		if e.Type()&os.ModeSymlink != 0 {
			continue
		}
		_ = os.RemoveAll(filepath.Join(recoveryDir, name))
	}
}

func newRecoveryScheduler(opts RecoveryOptions, worker chan<- Work, store *spool.Store, recoveryDir string, compressionLevel int) *recoveryScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &recoveryScheduler{
		opts: opts, worker: worker, store: store,
		recoveryDir: recoveryDir, compressionLevel: compressionLevel,
		ctx: ctx, cancel: cancel,
	}
}

// Trigger starts a build unless one is running, the scheduler stopped,
// or the last start is inside the throttle interval.
func (s *recoveryScheduler) Trigger() {
	s.mu.Lock()
	if s.building || s.stopped {
		s.mu.Unlock()
		return
	}
	if interval := s.opts.BuildInterval; interval > 0 && time.Since(s.lastStart) < interval {
		s.mu.Unlock()
		return
	}
	s.building = true
	s.lastStart = time.Now()
	s.last = RecoveryBuildStatus{Running: true, Started: s.lastStart}
	s.wg.Add(1)
	s.mu.Unlock()
	go s.run()
}

func (s *recoveryScheduler) run() {
	defer s.wg.Done()
	outcome, err := s.build(s.ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.building = false
	s.last.Running = false
	s.last.Finished = time.Now()
	if err != nil {
		s.last.Err = err.Error()
		return
	}
	s.last.Err = ""
	s.last.Generation = outcome.Generation
	s.last.Rows = outcome.Rows
	s.last.Bytes = outcome.Bytes
	s.last.Seconds = outcome.Seconds
}

func (s *recoveryScheduler) build(ctx context.Context) (recoveryBuildOutcome, error) {
	res, err := s.capture(ctx)
	if err != nil {
		return recoveryBuildOutcome{}, err
	}
	return runRecoveryBuild(ctx, res, s.store, s.recoveryDir, s.opts, s.compressionLevel)
}

// capture requests a worker checkpoint. The reply channel is buffered
// so the worker never blocks; on cancellation an orphan janitor
// receives and cleans the late result.
func (s *recoveryScheduler) capture(ctx context.Context) (RecoveryBuildResult, error) {
	var zero RecoveryBuildResult
	reply := make(chan RecoveryBuildResult, 1)
	select {
	case s.worker <- Work{Kind: WorkBuildRecovery, Destination: s.recoveryDir, RecoveryBuildReply: reply}:
	case <-ctx.Done():
		return zero, ctx.Err()
	}
	select {
	case res := <-reply:
		return res, nil
	case <-ctx.Done():
		go func() {
			res := <-reply
			res.Cleanup()
		}()
		return zero, ctx.Err()
	}
}

// Status returns the last build outcome.
func (s *recoveryScheduler) Status() RecoveryBuildStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

// Stop cancels any running build and waits for it. Idempotent.
func (s *recoveryScheduler) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()
	s.cancel()
	s.wg.Wait()
}
