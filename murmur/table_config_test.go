package murmur

import (
	"testing"
	"time"
)

// TestMarkTableConfigOwnedWaitsForBusyWorker pins the storage-worker send in
// MarkTableConfigOwned to the blocking pattern every sibling uses. A
// momentarily busy worker must not surface as ErrClosed.
func TestMarkTableConfigOwnedWaitsForBusyWorker(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()

	v, err := db.Call(func(c *Core) (any, error) { return c.worker, nil })
	if err != nil {
		t.Fatalf("worker handle: %v", err)
	}
	worker, ok := v.(chan<- Work)
	if !ok {
		t.Fatalf("worker handle has type %T", v)
	}
	release := make(chan struct{})
	received := make(chan struct{})
	go func() {
		worker <- Work{Kind: WorkNetwork, Network: func(*Opened) {
			close(received)
			<-release
		}}
	}()
	select {
	case <-received:
	case <-time.After(30 * time.Second):
		t.Fatal("storage worker never picked up the occupying job")
	}

	done := make(chan error, 1)
	go func() { done <- db.MarkTableConfigOwned() }()
	select {
	case err := <-done:
		t.Fatalf("MarkTableConfigOwned returned %v while the worker was occupied; want it to wait", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("MarkTableConfigOwned: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("MarkTableConfigOwned did not finish after the worker was released")
	}
	owned, err := db.TableConfigOwned()
	if err != nil {
		t.Fatalf("TableConfigOwned: %v", err)
	}
	if !owned {
		t.Fatal("config-owned marker missing after MarkTableConfigOwned")
	}
}
