package replication

import (
	"context"
	"time"
)

// waitPollInterval paces WaitForApplied and WaitForLag polls. Backend
// heads refresh at most once a second, so tighter polling only burns CPU.
const waitPollInterval = 100 * time.Millisecond

// LocalProgress returns durable per-origin replication heads: what this
// node applied and observed. A writer's read-your-write cursor is its
// own origin with the applied head read after flushing.
func (h *Handle) LocalProgress() ([]Progress, error) {
	return h.control.backend.Progress()
}

// InvalidateProgress drops cached progress heads after durable writes.
func (h *Handle) InvalidateProgress() {
	h.control.backend.InvalidateProgress()
}

// WaitForApplied blocks until origin's local applied head reaches seq,
// the context ends, or origin is retired (progress will never advance).
// Head reads resolve within about a second; use the context for tighter
// deadlines. Waiting for sequence zero returns immediately.
func (h *Handle) WaitForApplied(ctx context.Context, origin NodeId, seq uint64) error {
	if seq == 0 {
		return nil
	}
	if retired, err := h.originRetired(origin); err == nil && retired {
		return invalidErr("origin %x is retired; progress will not advance", origin)
	}
	for {
		heads, err := h.LocalProgress()
		if err != nil {
			return err
		}
		for _, p := range heads {
			if p.Origin == origin && p.Applied >= seq {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitPollInterval):
		}
	}
}

// WaitForLag blocks until this node's worst deficit behind its peers'
// observed heads is at most bound: bounded-staleness reads. With no
// connected peers there is no known-newer state and any bound holds.
func (h *Handle) WaitForLag(ctx context.Context, bound uint64) error {
	for {
		if h.Status().ObservedDeficit <= bound {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitPollInterval):
		}
	}
}

// originRetired reports whether the retention layer retired origin. A
// failed report is not retirement; the waiter falls through to polling,
// where a stopped engine surfaces through the context.
func (h *Handle) originRetired(origin NodeId) (bool, error) {
	report, err := h.RetentionStatus()
	if err != nil {
		return false, err
	}
	for _, p := range report.Peers {
		if p.Node == origin && p.Retired {
			return true, nil
		}
	}
	return false, nil
}
