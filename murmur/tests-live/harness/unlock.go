// Open-with-key plus on-open-progress watchdog.
//
// Same env knobs (MURMUR_LIVE_UNLOCK_STALL_SECONDS=120,
// MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS=1800) and same advancing-vs-frozen
// semantics as the Rust harness: murmur.Open runs in a worker goroutine
// while the caller watches progress snapshots; a stall (no advancement
// for the stall window) or an overall timeout fails fast.
//
// Deviation: the product has no open cancellation, so a timed-out open
// keeps running detached in the background. Do not reuse that directory
// afterwards (use a fresh node dir or abort the scenario).
package harness

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur"
)

// progressSnapshot is a comparable progress snapshot (clocks and error
// text excluded).
type progressSnapshot struct {
	phase          murmur.OpenPhase
	processedItems uint64
	rowsInserted   uint64
	rowsSkipped    uint64
	currentTable   string
}

func snapshotOf(p murmur.OpenProgress) progressSnapshot {
	s := progressSnapshot{
		phase:          p.Phase,
		processedItems: p.ProcessedItems,
		rowsInserted:   p.RowsInserted,
		rowsSkipped:    p.RowsSkipped,
	}
	if p.CurrentTable != nil {
		s.currentTable = *p.CurrentTable
	}
	return s
}

// OpenWithWatchdog opens a database under the stall/overall watchdog. Any
// caller-provided OnOpenProgress callback is preserved (it runs after the
// recorder).
func OpenWithWatchdog(path string, key []byte, opts murmur.Options) (*murmur.Database, error) {
	stall := EnvSeconds("MURMUR_LIVE_UNLOCK_STALL_SECONDS", 120)
	overall := EnvSeconds("MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS", 1800)

	var mu sync.Mutex
	var seen *progressSnapshot
	userCB := opts.OnOpenProgress
	opts.OnOpenProgress = func(p murmur.OpenProgress) {
		s := snapshotOf(p)
		mu.Lock()
		seen = &s
		mu.Unlock()
		if userCB != nil {
			userCB(p)
		}
	}

	type result struct {
		db  *murmur.Database
		err error
	}
	ch := make(chan result, 1)
	go func() {
		db, err := murmur.Open(path, key, opts)
		ch <- result{db: db, err: err}
	}()

	start := time.Now()
	lastAdvance := start
	var last *progressSnapshot
	lastLog := start
	for {
		select {
		case r := <-ch:
			return r.db, r.err
		case <-time.After(250 * time.Millisecond):
		}
		mu.Lock()
		current := seen
		mu.Unlock()
		if (current == nil) != (last == nil) || (current != nil && last != nil && *current != *last) {
			lastAdvance = time.Now()
			last = current
		}
		elapsed := time.Since(start)
		if time.Since(lastAdvance) >= stall {
			return nil, fmt.Errorf("open timed out after %v (no open-progress advancement)", elapsed)
		}
		if elapsed >= overall {
			return nil, fmt.Errorf("open timed out after %v (overall open timeout)", elapsed)
		}
		if time.Since(lastLog) >= 30*time.Second {
			lastLog = time.Now()
			fmt.Fprintf(os.Stderr, "open still advancing after %v: %+v\n", elapsed, last)
		}
	}
}
