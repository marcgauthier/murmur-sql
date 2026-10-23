package murmur

import (
	"sync/atomic"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

// TestExecuteBatchRunsAllStatements pins that ExecuteBatch runs every
// statement in order inside a transaction.
func TestExecuteBatchRunsAllStatements(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(
		`CREATE TABLE kv(k TEXT NOT NULL PRIMARY KEY, v TEXT NOT NULL)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	batch := "INSERT INTO kv(k, v) VALUES('a', '1');" +
		"INSERT INTO kv(k, v) VALUES('b', '2');" +
		"UPDATE kv SET v = '3' WHERE k = 'a';"
	if _, err := db.Transaction(func(s *Sql) (any, error) {
		return nil, s.ExecuteBatch(batch)
	}); err != nil {
		t.Fatalf("batch: %v", err)
	}
	v, err := db.Read(func(s *Sql) (any, error) {
		return s.Query("SELECT k, v FROM kv ORDER BY k")
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) != 2 || rows[0][1].Str != "3" || rows[1][1].Str != "2" {
		t.Fatalf("rows = %v, want a=3 b=2", rows)
	}
}

// TestLastInsertRowIDReturnsRowID pins the last-insert-rowid accessor after
// an insert into a rowid table.
func TestLastInsertRowIDReturnsRowID(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(`CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	v, err := db.Transaction(func(s *Sql) (any, error) {
		if _, err := s.Execute("INSERT INTO t(v) VALUES(?)", "x"); err != nil {
			return nil, err
		}
		return s.LastInsertRowID(), nil
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if v.(int64) != 1 {
		t.Fatalf("last insert rowid = %v, want 1", v)
	}
}

// TestSetHelpersExerciseCRDTSet pins the set CRDT helpers through the thin
// Sql wrappers: adds union, removes delete, and the survivor set reads
// back through the value codec.
func TestSetHelpersExerciseCRDTSet(t *testing.T) {
	dir := t.TempDir()
	idA := compatNodeID(0xA1)
	dbID := compatNodeID(0xC7)
	configs := compatConfigs(t, [][16]byte{idA})
	configs[0].MergePolicies = map[replication.PolicyColumn]replication.Policy{
		{Table: "s", Column: "tags"}: replication.PolicySet,
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
	if err := db.Migrate(NewMigration(`CREATE TABLE s(k TEXT NOT NULL PRIMARY KEY, tags BLOB)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, "INSERT INTO s(k) VALUES ('k1')")
	v, err := db.Transaction(func(s *Sql) (any, error) {
		key := []Value{TextValue("k1")}
		if err := s.SetAdd("s", key, "tags", TextValue("red")); err != nil {
			return nil, err
		}
		if err := s.SetAdd("s", key, "tags", TextValue("blue")); err != nil {
			return nil, err
		}
		if err := s.SetAdd("s", key, "tags", TextValue("red")); err != nil {
			return nil, err
		}
		if err := s.SetRemove("s", key, "tags", TextValue("red")); err != nil {
			return nil, err
		}
		return s.QueryRow("SELECT tags FROM s WHERE k = 'k1'")
	})
	if err != nil {
		t.Fatalf("set ops: %v", err)
	}
	decoded, err := DecodeValues(v.([]sqlite.Value)[0].Blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded) != 1 || decoded[0].Tag != ValueText || decoded[0].S != "blue" {
		t.Fatalf("survivor set = %+v, want [blue]", decoded)
	}
}

// TestCallCancelPreCancelled pins that CallCancel honors a pre-cancelled
// handle without invoking the callback.
func TestCallCancelPreCancelled(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	var cancelled atomic.Bool
	cancelled.Store(true)
	called := false
	_, err := db.CallCancel(func(c *Core) (any, error) {
		called = true
		return nil, nil
	}, &cancelled)
	if err == nil {
		t.Fatalf("pre-cancelled call returned nil error")
	}
	if called {
		t.Fatalf("pre-cancelled call invoked the callback")
	}
}

// TestCallCancelRunsCallback pins the non-cancelled CallCancel path.
func TestCallCancelRunsCallback(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(`CREATE TABLE t(id INTEGER PRIMARY KEY)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	var cancelled atomic.Bool
	v, err := db.CallCancel(func(c *Core) (any, error) {
		return c.transaction(func(s *Sql) (any, error) {
			return s.Query("SELECT COUNT(*) FROM t")
		})
	}, &cancelled)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := v.([][]sqlite.Value)[0][0].Int; got != 0 {
		t.Fatalf("count = %d, want 0", got)
	}
}

// TestCounterHelpersExerciseCRDTCounters pins the counter/min/max CRDT
// helpers through the thin Sql wrappers. Each helper requires its column
// to carry the matching merge policy.
func TestCounterHelpersExerciseCRDTCounters(t *testing.T) {
	dir := t.TempDir()
	idA := compatNodeID(0xA1)
	dbID := compatNodeID(0xC7)
	configs := compatConfigs(t, [][16]byte{idA})
	configs[0].MergePolicies = map[replication.PolicyColumn]replication.Policy{
		{Table: "t", Column: "n"}:  replication.PolicyCounter,
		{Table: "t", Column: "lo"}: replication.PolicyMin,
		{Table: "t", Column: "hi"}: replication.PolicyMax,
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
	if err := db.Migrate(NewMigration(`CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER NOT NULL, lo INTEGER NOT NULL, hi INTEGER NOT NULL)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, "INSERT INTO t VALUES (1, 0, 100, 0)")
	v, err := db.Transaction(func(s *Sql) (any, error) {
		key := []Value{IntValue(1)}
		if err := s.CounterAdd("t", key, "n", 5); err != nil {
			return nil, err
		}
		if err := s.Max("t", key, "hi", IntValue(10)); err != nil {
			return nil, err
		}
		if err := s.Min("t", key, "lo", IntValue(7)); err != nil {
			return nil, err
		}
		return s.QueryRow("SELECT n, lo, hi FROM t WHERE id = 1")
	})
	if err != nil {
		t.Fatalf("counters: %v", err)
	}
	row := v.([]sqlite.Value)
	if row[0].Int != 5 || row[1].Int != 7 || row[2].Int != 10 {
		t.Fatalf("n/lo/hi = %d/%d/%d, want 5/7/10", row[0].Int, row[1].Int, row[2].Int)
	}
}
