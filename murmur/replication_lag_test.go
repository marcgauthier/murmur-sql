package murmur

import (
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

// TestPeerLagConvergesToZero pins the Phase 1 diagnostics: after two nodes
// converge, each side reports its peer connected, caught up (lag zero),
// with a fresh progress report and an alive membership state.
func TestPeerLagConvergesToZero(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live replication test in short mode")
	}
	dir := t.TempDir()
	idA, idB := compatNodeID(0xA1), compatNodeID(0xB2)
	dbID := compatNodeID(0xD4)
	configs := compatConfigs(t, [][16]byte{idA, idB})

	openNode := func(t *testing.T, path string, cfg *replication.Config) *Database {
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
	dbA := openNode(t, dir+"/a", configs[0])
	defer dbA.Close()
	dbB := openNode(t, dir+"/b", configs[1])
	defer dbB.Close()

	for _, db := range []*Database{dbA, dbB} {
		if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if err := db.SetReplication("users", true); err != nil {
			t.Fatalf("enable replication: %v", err)
		}
	}
	execMust(t, dbA, "INSERT INTO users (id, name, score) VALUES (1, 'ada', 10)")
	if err := dbA.Flush(); err != nil {
		t.Fatalf("flush A: %v", err)
	}

	handleA, handleB := dbA.Replication(), dbB.Replication()
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

	deadline := time.Now().Add(60 * time.Second)
	for compatRowCount(t, dbB, 1) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("row never arrived on B")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Force a progress exchange, then poll until both sides report a
	// caught-up peer.
	if err := handleA.ForceSync(); err != nil {
		t.Fatalf("sync A: %v", err)
	}
	if err := handleB.ForceSync(); err != nil {
		t.Fatalf("sync B: %v", err)
	}
	deadline = time.Now().Add(60 * time.Second)
	for {
		caughtUp := true
		for _, h := range []*replication.Handle{handleA, handleB} {
			diag := h.Status().PeersDiagnostics
			if len(diag) != 1 || !diag[0].Connected {
				caughtUp = false
				break
			}
			d := diag[0]
			if d.LagTxns != 0 || d.OriginsBehind != 0 || d.MemberState != "alive" ||
				d.LastProgressMillisAgo == nil || d.QueueMessages < 0 || d.QueueAvailableBytes <= 0 {
				caughtUp = false
				break
			}
		}
		if caughtUp {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("peers never reported caught-up: A=%+v B=%+v",
				handleA.Status().PeersDiagnostics, handleB.Status().PeersDiagnostics)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
