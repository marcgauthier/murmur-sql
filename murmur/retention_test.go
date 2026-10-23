package murmur

import (
	"testing"
)

func TestRetentionRequiresReplication(t *testing.T) {
	db := openTestDB(t, t.TempDir()+"/db")
	defer db.Close()
	var zero [16]byte
	if err := db.RetirePeer(zero); err == nil {
		t.Fatal("retire without replication accepted")
	}
	if _, err := db.RetentionStatus(); err == nil {
		t.Fatal("retention status without replication accepted")
	}
	if _, err := db.CollectRetention(); err == nil {
		t.Fatal("retention collect without replication accepted")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.RetirePeer(zero); err != ErrClosed {
		t.Fatalf("retire after close = %v", err)
	}
	if _, err := db.RetentionStatus(); err != ErrClosed {
		t.Fatalf("status after close = %v", err)
	}
	if _, err := db.CollectRetention(); err != ErrClosed {
		t.Fatalf("collect after close = %v", err)
	}
}

func TestRetentionAdminRoundtrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live replication test in short mode")
	}
	dir := t.TempDir()
	idA := compatNodeID(0xA1)
	dbID := compatNodeID(0xD4)
	configs := compatConfigs(t, [][16]byte{idA})
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	options.DatabaseID = &dbID
	options.Replication = configs[0]
	db, err := Open(dir+"/db", testKey(), options)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	status, err := db.RetentionStatus()
	if err != nil || status == nil {
		t.Fatalf("status = %+v, %v", status, err)
	}
	pruned, err := db.CollectRetention()
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if pruned != 0 {
		t.Fatalf("pruned = %d on a fresh node", pruned)
	}
	other := compatNodeID(0xB2)
	if err := db.RetirePeer(other); err != nil {
		t.Fatalf("retire: %v", err)
	}
}
