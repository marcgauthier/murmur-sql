package murmur

import (
	"errors"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
)

const counterTestDDL = "CREATE TABLE items(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)"

func counterValue(t *testing.T, db *Database) int64 {
	t.Helper()
	v, err := db.Read(func(sql *Sql) (any, error) {
		return sql.QueryRow("SELECT n FROM items WHERE id = 1")
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return v.([]sqlite.Value)[0].Int
}

func counterAdd(t *testing.T, db *Database, delta int64) {
	t.Helper()
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		return nil, sql.CounterAdd("items", []Value{IntValue(1)}, "n", delta)
	}); err != nil {
		t.Fatalf("counter_add: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestCounterOperationsConverge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live replication test in short mode")
	}
	dir := t.TempDir()
	idA, idB := compatNodeID(0xA1), compatNodeID(0xB2)
	dbID := compatNodeID(0xC7)
	configs := compatConfigs(t, [][16]byte{idA, idB})
	for _, c := range configs {
		if c.MergePolicies == nil {
			c.MergePolicies = make(map[replication.PolicyColumn]replication.Policy)
		}
		c.MergePolicies[replication.PolicyColumn{Table: "items", Column: "n"}] = replication.PolicyCounter
	}
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
		if err := db.Migrate(NewMigration(counterTestDDL)); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if err := db.SetReplication("items", true); err != nil {
			t.Fatalf("enable replication: %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	execMust(t, dbA, "INSERT INTO items VALUES (1, 0)")
	if err := dbA.Flush(); err != nil {
		t.Fatalf("flush A: %v", err)
	}

	dbA.inner.replicationMu.Lock()
	handleA := dbA.inner.replication
	dbA.inner.replicationMu.Unlock()
	dbB.inner.replicationMu.Lock()
	handleB := dbB.inner.replication
	dbB.inner.replicationMu.Unlock()
	addrB := handleB.Status().Address
	if err := handleA.AddPeer(*addrB); err != nil {
		t.Fatalf("add peer: %v", err)
	}
	itemsPresent := func(db *Database) bool {
		v, err := db.Read(func(sql *Sql) (any, error) {
			return sql.Query("SELECT id FROM items WHERE id = 1")
		})
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return len(v.([][]sqlite.Value)) > 0
	}
	deadline := time.Now().Add(30 * time.Second)
	for !itemsPresent(dbB) {
		if time.Now().After(deadline) {
			t.Fatal("base row never arrived on B")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Offline writes on both sides, then reconnect.
	if err := handleA.RemovePeer(idB); err != nil {
		t.Fatalf("remove peer: %v", err)
	}
	counterAdd(t, dbA, 5)
	counterAdd(t, dbB, 7)
	if err := handleA.AddPeer(*handleB.Status().Address); err != nil {
		t.Fatalf("re-add peer: %v", err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for counterValue(t, dbA) != 12 || counterValue(t, dbB) != 12 {
		if time.Now().After(deadline) {
			t.Fatalf("counter convergence: a=%d b=%d", counterValue(t, dbA), counterValue(t, dbB))
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Direct SQL writes to a policy column must fail.
	if _, err := dbA.Transaction(func(sql *Sql) (any, error) {
		_, err := sql.Execute("UPDATE items SET n = 100")
		return nil, err
	}); err == nil {
		t.Fatal("expected direct policy-column write to fail")
	}
}

// TestPolicyOperationMissingRowFailsNotFound pins the missing-row contract
// all clients rely on: a policy op against a nonexistent row reports
// ErrNotFound (SQLSTATE 02000), not a generic SQLite error.
func TestPolicyOperationMissingRowFailsNotFound(t *testing.T) {
	dir := t.TempDir()
	idA := compatNodeID(0xA1)
	dbID := compatNodeID(0xC7)
	configs := compatConfigs(t, [][16]byte{idA})
	configs[0].MergePolicies = map[replication.PolicyColumn]replication.Policy{
		{Table: "items", Column: "n"}: replication.PolicyCounter,
	}
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	options.DatabaseID = &dbID
	options.Replication = configs[0]
	db, err := Open(dir+"/db", testKey(), options)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(NewMigration(counterTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, "INSERT INTO items VALUES (1, 0)")
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		return nil, sql.CounterAdd("items", []Value{IntValue(1)}, "n", 1)
	}); err != nil {
		t.Fatalf("counter_add on present row: %v", err)
	}
	_, err = db.Transaction(func(sql *Sql) (any, error) {
		return nil, sql.CounterAdd("items", []Value{IntValue(999)}, "n", 1)
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("counter_add on missing row = %v, want ErrNotFound", err)
	}
}
