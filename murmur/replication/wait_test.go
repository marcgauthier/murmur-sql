package replication

import (
	"context"
	"errors"
	"testing"
	"time"
)

func seedRetainedHead(t *testing.T, b *stubBackend, origin NodeId, applied uint64) {
	t.Helper()
	head := &Progress{Origin: origin, Applied: applied, Observed: applied,
		RetainedFrom: applied, RetainedThrough: applied}
	if err := b.Commit([]Record{PutRecord(RetainedKey(origin), EncodeProgress(head))}); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// TestWaitForAppliedOverStub pins wait semantics without networking:
// satisfied returns, unsatisfied honors the context, and a head
// advancing mid-wait unblocks the waiter.
func TestWaitForAppliedOverStub(t *testing.T) {
	var origin NodeId
	origin[0] = 0xA1
	backend := newStubBackend(origin, []byte("s"))
	h := &Handle{control: &engineControl{backend: backend}}
	seedRetainedHead(t, backend, origin, 7)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.WaitForApplied(ctx, origin, 7); err != nil {
		t.Fatalf("satisfied: %v", err)
	}
	if err := h.WaitForApplied(ctx, origin, 0); err != nil {
		t.Fatalf("zero: %v", err)
	}
	short, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := h.WaitForApplied(short, origin, 8); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.WaitForApplied(cancelled, origin, 8); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want Canceled", err)
	}
	// Satisfied wins over a canceled context: no timeout flake when the
	// head arrived first.
	if err := h.WaitForApplied(cancelled, origin, 7); err != nil {
		t.Fatalf("satisfied+canceled: %v", err)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		seedRetainedHead(t, backend, origin, 8)
	}()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.WaitForApplied(waitCtx, origin, 8); err != nil {
		t.Fatalf("advanced: %v", err)
	}
}

// TestWaitForLagOverStub pins the staleness gate against a fixed status
// snapshot: within bound returns, over bound honors the context.
func TestWaitForLagOverStub(t *testing.T) {
	var origin NodeId
	backend := newStubBackend(origin, []byte("s"))
	h := &Handle{control: &engineControl{backend: backend, status: Status{ObservedDeficit: 5}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.WaitForLag(ctx, 10); err != nil {
		t.Fatalf("within bound: %v", err)
	}
	if err := h.WaitForLag(ctx, 5); err != nil {
		t.Fatalf("at bound: %v", err)
	}
	short, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := h.WaitForLag(short, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.WaitForLag(cancelled, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want Canceled", err)
	}
}
