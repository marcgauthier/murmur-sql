package murmur

import (
	"errors"
	"testing"
)

func TestOpenProgressReporter(t *testing.T) {
	// Nil callback: every method is a safe no-op.
	silent := NewOpenProgressReporter(nil)
	silent.Phase(PhaseReady)
	silent.SetTable("t")
	silent.AddProcessedItems(5)
	silent.AddRows(2, 3)
	if silent.Snapshot() != nil || silent.Finish(nil) != nil {
		t.Fatal("silent reporter returned progress")
	}

	// Recording reporter: initial event plus phase/table transitions.
	var events []OpenProgress
	r := NewOpenProgressReporter(func(p OpenProgress) { events = append(events, p) })
	if len(events) != 1 || events[0].Phase != PhaseOpening {
		t.Fatalf("initial events = %+v", events)
	}
	r.Phase(PhaseOpening)
	if len(events) != 1 {
		t.Fatal("same-phase move emitted")
	}
	r.Phase(PhaseReady)
	if len(events) != 2 || events[1].Phase != PhaseReady {
		t.Fatalf("phase events = %+v", events)
	}
	r.SetTable("users")
	if len(events) != 3 || events[2].CurrentTable == nil || *events[2].CurrentTable != "users" {
		t.Fatalf("table events = %+v", events)
	}
	// Counters accumulate (emission is throttled; Snapshot is exact).
	r.AddProcessedItems(5)
	r.AddRows(2, 3)
	snap := r.Snapshot()
	if snap == nil {
		t.Fatal("no snapshot")
	}
	if snap.ProcessedItems != 10 || snap.RowsInserted != 2 || snap.RowsSkipped != 3 {
		t.Fatalf("snapshot = %+v", snap)
	}
	// Terminal states.
	done := r.Finish(nil)
	if done == nil || done.Phase != PhaseReady || done.Error != nil || done.CurrentTable != nil {
		t.Fatalf("finish ok = %+v", done)
	}
	failed := r.Finish(errors.New("boom"))
	if failed == nil || failed.Phase != PhaseFailed || failed.Error == nil {
		t.Fatalf("finish err = %+v", failed)
	}
}
