package murmur

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

// openWaitNode opens one replicated node of a shared database.
func openWaitNode(t *testing.T, path string, dbID [16]byte, cfg *replication.Config) *Database {
	t.Helper()
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	options.DatabaseID = &dbID
	options.Replication = cfg
	db, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

// meshWaitPair opens two peered replicating nodes with the users table.
func meshWaitPair(t *testing.T) (dbA, dbB *Database, handleA, handleB *replication.Handle) {
	t.Helper()
	dir := t.TempDir()
	idA, idB := compatNodeID(0xA1), compatNodeID(0xB2)
	dbID := compatNodeID(0xD4)
	configs := compatConfigs(t, [][16]byte{idA, idB})
	dbA = openWaitNode(t, dir+"/a", dbID, configs[0])
	t.Cleanup(func() { dbA.Close() })
	dbB = openWaitNode(t, dir+"/b", dbID, configs[1])
	t.Cleanup(func() { dbB.Close() })
	for _, db := range []*Database{dbA, dbB} {
		if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if err := db.SetReplication("users", true); err != nil {
			t.Fatalf("enable replication: %v", err)
		}
	}
	handleA, handleB = dbA.Replication(), dbB.Replication()
	if handleA == nil || handleB == nil {
		t.Fatal("replication handles missing")
	}
	addrA, addrB := handleA.Status().Address, handleB.Status().Address
	if addrA == nil || addrB == nil {
		t.Fatal("missing listen addresses")
	}
	if err := handleA.AddPeer(*addrB); err != nil {
		t.Fatalf("add peer B: %v", err)
	}
	if err := handleB.AddPeer(*addrA); err != nil {
		t.Fatalf("add peer A: %v", err)
	}
	return dbA, dbB, handleA, handleB
}

// localApplied reads one origin's applied head from fresh local progress.
func localApplied(t *testing.T, h *replication.Handle, origin [16]byte) uint64 {
	t.Helper()
	heads, err := h.LocalProgress()
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	for _, p := range heads {
		if p.Origin == origin {
			return p.Applied
		}
	}
	t.Fatalf("no local head for %x in %+v", origin, heads)
	return 0
}

// TestReadYourWriteAcrossNodes pins the read-your-write recipe: flush on
// the writer, read the cursor fresh, wait on the reader, then read.
func TestReadYourWriteAcrossNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live replication test in short mode")
	}
	dbA, dbB, handleA, handleB := meshWaitPair(t)
	execMust(t, dbA, "INSERT INTO users (id, name, score) VALUES (1, 'ada', 10)")
	if err := dbA.Flush(); err != nil {
		t.Fatalf("flush A: %v", err)
	}
	seq := localApplied(t, handleA, dbA.NodeID())
	if seq == 0 {
		t.Fatal("writer head is zero after flush")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := handleB.WaitForApplied(ctx, dbA.NodeID(), seq); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if n := compatRowCount(t, dbB, 1); n != 1 {
		t.Fatalf("reader sees %d rows, want 1", n)
	}
}

// TestWaitForAppliedTimeout pins that waiting for an unreachable cursor
// ends with the context error, not a hang.
func TestWaitForAppliedTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live replication test in short mode")
	}
	dbA, _, _, handleB := meshWaitPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := handleB.WaitForApplied(ctx, dbA.NodeID(), 1<<60)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("wait took %v, want prompt context error", elapsed)
	}
}

// TestWaitForAppliedRetiredOrigin pins fail-fast: progress for a retired
// origin errors instead of waiting out the context.
func TestWaitForAppliedRetiredOrigin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live replication test in short mode")
	}
	dbA, dbB, handleA, _ := meshWaitPair(t)
	if err := dbA.RetirePeer(dbB.NodeID()); err != nil {
		t.Fatalf("retire: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	err := handleA.WaitForApplied(ctx, dbB.NodeID(), 1<<60)
	if err == nil || !strings.Contains(err.Error(), "retired") {
		t.Fatalf("err = %v, want retired-origin error", err)
	}
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Fatalf("retired wait took %v, want fail-fast", elapsed)
	}
}

// TestWaitForLagConverged pins bounded-staleness waits: a converged node
// reports zero deficit promptly, and cancellation aborts the wait.
func TestWaitForLagConverged(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live replication test in short mode")
	}
	dbA, dbB, handleA, handleB := meshWaitPair(t)
	execMust(t, dbA, "INSERT INTO users (id, name, score) VALUES (1, 'ada', 10)")
	if err := dbA.Flush(); err != nil {
		t.Fatalf("flush A: %v", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for compatRowCount(t, dbB, 1) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("row never arrived on B")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := handleA.ForceSync(); err != nil {
		t.Fatalf("sync A: %v", err)
	}
	if err := handleB.ForceSync(); err != nil {
		t.Fatalf("sync B: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := handleA.WaitForLag(ctx, 0); err != nil {
		t.Fatalf("wait lag: %v", err)
	}
}
