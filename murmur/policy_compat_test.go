package murmur

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
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

// TestPolicyOperationCompositeKey pins multi-column key support: ops
// address rows by the full application key, and wrong-width keys fail.
func TestPolicyOperationCompositeKey(t *testing.T) {
	dir := t.TempDir()
	idA := compatNodeID(0xA1)
	dbID := compatNodeID(0xC7)
	configs := compatConfigs(t, [][16]byte{idA})
	configs[0].MergePolicies = map[replication.PolicyColumn]replication.Policy{
		{Table: "pairs", Column: "n"}: replication.PolicyCounter,
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
	if err := db.Migrate(NewMigration("CREATE TABLE pairs(a INTEGER NOT NULL, b TEXT NOT NULL, n INTEGER NOT NULL, PRIMARY KEY (a, b))")); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, "INSERT INTO pairs VALUES (1, 'x', 10)")
	execMust(t, db, "INSERT INTO pairs VALUES (1, 'y', 20)")
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		return nil, sql.CounterAdd("pairs", []Value{IntValue(1), TextValue("x")}, "n", 5)
	}); err != nil {
		t.Fatalf("composite counter_add: %v", err)
	}
	v, err := db.Read(func(sql *Sql) (any, error) {
		return sql.Query("SELECT b, n FROM pairs ORDER BY b")
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) != 2 || rows[0][1].Int != 15 || rows[1][1].Int != 20 {
		t.Fatalf("rows = %+v, want x/15 y/20", rows)
	}
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		return nil, sql.CounterAdd("pairs", []Value{IntValue(1)}, "n", 1)
	}); err == nil {
		t.Fatal("short key accepted, want arity error")
	}
}

// TestConfigurePolicyDefersMissingTables pins that merge policies for
// tables not yet in the schema do not fail migration; the policy
// attaches when the table is created. Otherwise no store could ever
// carry policies on two tables.
func TestConfigurePolicyDefersMissingTables(t *testing.T) {
	dir := t.TempDir()
	idA := compatNodeID(0xA1)
	dbID := compatNodeID(0xC7)
	configs := compatConfigs(t, [][16]byte{idA})
	configs[0].MergePolicies = map[replication.PolicyColumn]replication.Policy{
		{Table: "pairs", Column: "n"}: replication.PolicyCounter,
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
	if err := db.Migrate(NewMigration("CREATE TABLE stats(k TEXT NOT NULL PRIMARY KEY, v INTEGER NOT NULL)")); err != nil {
		t.Fatalf("unrelated migrate with future policy table: %v", err)
	}
	if err := db.Migrate(NewMigration("CREATE TABLE pairs(a INTEGER NOT NULL, b TEXT NOT NULL, n INTEGER NOT NULL, PRIMARY KEY (a, b))")); err != nil {
		t.Fatalf("migrate pairs: %v", err)
	}
	execMust(t, db, "INSERT INTO pairs VALUES (1, 'x', 0)")
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		return nil, sql.CounterAdd("pairs", []Value{IntValue(1), TextValue("x")}, "n", 3)
	}); err != nil {
		t.Fatalf("counter_add after deferred attach: %v", err)
	}
}

// TestJsonOperations pins local JSON policy ops: set and remove render
// canonical object text, and mistyped operands fail loudly.
func TestJsonOperations(t *testing.T) {
	dir := t.TempDir()
	idA := compatNodeID(0xA1)
	dbID := compatNodeID(0xC7)
	configs := compatConfigs(t, [][16]byte{idA})
	configs[0].MergePolicies = map[replication.PolicyColumn]replication.Policy{
		{Table: "docs", Column: "doc"}: replication.PolicyJson,
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
	if err := db.Migrate(NewMigration("CREATE TABLE docs(k TEXT NOT NULL PRIMARY KEY, doc TEXT)")); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, "INSERT INTO docs VALUES ('d1', NULL)")
	apply := func(op PolicyOperation) error {
		_, err := db.Transaction(func(sql *Sql) (any, error) {
			switch op.Tag {
			case PolicyOpJsonSet:
				return nil, sql.JsonSet("docs", []Value{TextValue("d1")}, "doc", op.Path, op.V)
			default:
				return nil, sql.JsonRemove("docs", []Value{TextValue("d1")}, "doc", op.Path)
			}
		})
		return err
	}
	read := func() string {
		t.Helper()
		v, err := db.Read(func(sql *Sql) (any, error) {
			return sql.QueryRow("SELECT doc FROM docs WHERE k = 'd1'")
		})
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return v.([]sqlite.Value)[0].Str
	}
	if err := apply(PolicyOperation{Tag: PolicyOpJsonSet, Path: "b", V: TextValue("x")}); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if err := apply(PolicyOperation{Tag: PolicyOpJsonSet, Path: "a", V: IntValue(1)}); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if got := read(); got != `{"a":1,"b":"x"}` {
		t.Fatalf("doc = %s, want canonical object", got)
	}
	if err := apply(PolicyOperation{Tag: PolicyOpJsonSet, Path: "a", V: IntValue(2)}); err != nil {
		t.Fatalf("overwrite a: %v", err)
	}
	if got := read(); got != `{"a":2,"b":"x"}` {
		t.Fatalf("doc = %s", got)
	}
	if err := apply(PolicyOperation{Tag: PolicyOpJsonRemove, Path: "b"}); err != nil {
		t.Fatalf("remove b: %v", err)
	}
	if got := read(); got != `{"a":2}` {
		t.Fatalf("doc = %s", got)
	}
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		_, err := sql.Execute("UPDATE docs SET doc = '{\"a\":9}'")
		return nil, err
	}); err == nil {
		t.Fatal("direct json write accepted")
	}
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		return nil, sql.JsonSet("docs", []Value{TextValue("d1")}, "doc", "", TextValue("x"))
	}); err == nil {
		t.Fatal("empty path accepted")
	}
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		return nil, sql.JsonSet("docs", []Value{TextValue("d1")}, "doc", "b", BlobValue([]byte{1}))
	}); err == nil {
		t.Fatal("blob value accepted")
	}
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		return nil, sql.CounterAdd("docs", []Value{TextValue("d1")}, "doc", 1)
	}); err == nil {
		t.Fatal("counter op on json column accepted")
	}
	// Reopen proves the causal projection agrees with the op-time
	// image: removed paths stay removed after restart.
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	options.Mode = OpenExisting
	db2, err := Open(dir+"/db", testKey(), options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	v, err := db2.Read(func(sql *Sql) (any, error) {
		return sql.QueryRow("SELECT doc FROM docs WHERE k = 'd1'")
	})
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if got := v.([]sqlite.Value)[0].Str; got != `{"a":2}` {
		t.Fatalf("doc after reopen = %s, want projection agreement", got)
	}
}

// TestJsonOperationsConverge pins cross-node JSON convergence: disjoint
// paths union, and same-path conflicts resolve identically on both
// nodes (canonical text makes byte equality the convergence check).
func TestJsonOperationsConverge(t *testing.T) {
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
		c.MergePolicies[replication.PolicyColumn{Table: "docs", Column: "doc"}] = replication.PolicyJson
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
	ddl := "CREATE TABLE docs(k TEXT NOT NULL PRIMARY KEY, doc TEXT)"
	for _, db := range []*Database{dbA, dbB} {
		if err := db.Migrate(NewMigration(ddl)); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if err := db.SetReplication("docs", true); err != nil {
			t.Fatalf("enable replication: %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	execMust(t, dbA, "INSERT INTO docs VALUES ('d1', NULL)")
	if err := dbA.Flush(); err != nil {
		t.Fatalf("flush A: %v", err)
	}
	dbA.inner.replicationMu.Lock()
	handleA := dbA.inner.replication
	dbA.inner.replicationMu.Unlock()
	dbB.inner.replicationMu.Lock()
	handleB := dbB.inner.replication
	dbB.inner.replicationMu.Unlock()
	if err := handleA.AddPeer(*handleB.Status().Address); err != nil {
		t.Fatalf("add peer: %v", err)
	}
	docOf := func(db *Database) string {
		v, err := db.Read(func(sql *Sql) (any, error) {
			return sql.Query("SELECT doc FROM docs WHERE k = 'd1'")
		})
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		rows := v.([][]sqlite.Value)
		if len(rows) != 1 {
			return ""
		}
		if rows[0][0].Type == sqlite.Null {
			return "NULL"
		}
		return rows[0][0].Str
	}
	deadline := time.Now().Add(30 * time.Second)
	for docOf(dbB) == "" {
		if time.Now().After(deadline) {
			t.Fatal("base row never arrived on B")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := handleA.RemovePeer(idB); err != nil {
		t.Fatalf("remove peer: %v", err)
	}
	set := func(db *Database, path string, v Value) {
		t.Helper()
		if _, err := db.Transaction(func(sql *Sql) (any, error) {
			return nil, sql.JsonSet("docs", []Value{TextValue("d1")}, "doc", path, v)
		}); err != nil {
			t.Fatalf("json set %s: %v", path, err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	set(dbA, "a", IntValue(1))
	set(dbB, "b", IntValue(2))
	set(dbA, "shared", IntValue(1))
	set(dbB, "shared", IntValue(2))
	if err := handleA.AddPeer(*handleB.Status().Address); err != nil {
		t.Fatalf("re-add peer: %v", err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		a, b := docOf(dbA), docOf(dbB)
		if a != "" && a == b {
			doc, err := ParseJsonDoc(TextValue(a))
			if err != nil {
				t.Fatalf("converged doc unparsable: %v", err)
			}
			if !ValuesEqual(doc["a"], IntValue(1)) || !ValuesEqual(doc["b"], IntValue(2)) {
				t.Fatalf("converged doc = %s, want disjoint paths unioned", a)
			}
			if s, ok := doc["shared"]; !ok || (s.Tag != ValueInteger || (s.I != 1 && s.I != 2)) {
				t.Fatalf("converged doc = %s, want one shared winner", a)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("json convergence: a=%s b=%s", a, b)
		}
		time.Sleep(30 * time.Millisecond)
	}
}

// TestListOperations pins local list policy ops: appends and removes
// render canonical array text, duplicates render twice, and mistyped
// operands fail loudly.
func TestListOperations(t *testing.T) {
	dir := t.TempDir()
	idA := compatNodeID(0xA1)
	dbID := compatNodeID(0xC7)
	configs := compatConfigs(t, [][16]byte{idA})
	configs[0].MergePolicies = map[replication.PolicyColumn]replication.Policy{
		{Table: "nums", Column: "list"}: replication.PolicyList,
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
	if err := db.Migrate(NewMigration("CREATE TABLE nums(k TEXT NOT NULL PRIMARY KEY, list TEXT)")); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, "INSERT INTO nums VALUES ('n1', NULL)")
	apply := func(tag PolicyOpTag, v Value) error {
		_, err := db.Transaction(func(sql *Sql) (any, error) {
			if tag == PolicyOpListAppend {
				return nil, sql.ListAppend("nums", []Value{TextValue("n1")}, "list", v)
			}
			return nil, sql.ListRemove("nums", []Value{TextValue("n1")}, "list", v)
		})
		return err
	}
	read := func() string {
		t.Helper()
		v, err := db.Read(func(sql *Sql) (any, error) {
			return sql.QueryRow("SELECT list FROM nums WHERE k = 'n1'")
		})
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return v.([]sqlite.Value)[0].Str
	}
	if err := apply(PolicyOpListAppend, IntValue(2)); err != nil {
		t.Fatalf("append 2: %v", err)
	}
	if err := apply(PolicyOpListAppend, IntValue(1)); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if err := apply(PolicyOpListAppend, IntValue(2)); err != nil {
		t.Fatalf("append dup: %v", err)
	}
	// Single-node appends render in tag order, which is random: pin
	// the multiset, not the sequence.
	if got := listMultiset(t, read()); !reflect.DeepEqual(got, []string{"1", "2", "2"}) {
		t.Fatalf("multiset = %v", got)
	}
	if err := apply(PolicyOpListRemove, IntValue(2)); err != nil {
		t.Fatalf("remove 2: %v", err)
	}
	if got := read(); got != `[1]` {
		t.Fatalf("list = %s", got)
	}
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		return nil, sql.ListAppend("nums", []Value{TextValue("n1")}, "list", BlobValue([]byte{1}))
	}); err == nil {
		t.Fatal("blob element accepted")
	}
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		return nil, sql.SetAdd("nums", []Value{TextValue("n1")}, "list", TextValue("x"))
	}); err == nil {
		t.Fatal("set op on list column accepted")
	}
	// Reopen proves the causal projection agrees with the op-time
	// image across restart.
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	options.Mode = OpenExisting
	db2, err := Open(dir+"/db", testKey(), options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	v, err := db2.Read(func(sql *Sql) (any, error) {
		return sql.QueryRow("SELECT list FROM nums WHERE k = 'n1'")
	})
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if got := v.([]sqlite.Value)[0].Str; got != `[1]` {
		t.Fatalf("list after reopen = %s", got)
	}
}

func listMultiset(t *testing.T, raw string) []string {
	t.Helper()
	var list []any
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&list); err != nil {
		t.Fatalf("unparsable list %s: %v", raw, err)
	}
	out := make([]string, 0, len(list))
	for _, e := range list {
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		out = append(out, string(b))
	}
	sort.Strings(out)
	return out
}

// TestListOperationsConverge pins cross-node list convergence:
// concurrent appends union (compared as multisets; tag order is
// random but identical on both nodes), and same-node removes hold.
func TestListOperationsConverge(t *testing.T) {
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
		c.MergePolicies[replication.PolicyColumn{Table: "nums", Column: "list"}] = replication.PolicyList
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
	ddl := "CREATE TABLE nums(k TEXT NOT NULL PRIMARY KEY, list TEXT)"
	for _, db := range []*Database{dbA, dbB} {
		if err := db.Migrate(NewMigration(ddl)); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if err := db.SetReplication("nums", true); err != nil {
			t.Fatalf("enable replication: %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	execMust(t, dbA, "INSERT INTO nums VALUES ('n1', NULL)")
	if err := dbA.Flush(); err != nil {
		t.Fatalf("flush A: %v", err)
	}
	dbA.inner.replicationMu.Lock()
	handleA := dbA.inner.replication
	dbA.inner.replicationMu.Unlock()
	dbB.inner.replicationMu.Lock()
	handleB := dbB.inner.replication
	dbB.inner.replicationMu.Unlock()
	if err := handleA.AddPeer(*handleB.Status().Address); err != nil {
		t.Fatalf("add peer: %v", err)
	}
	listOf := func(db *Database) string {
		v, err := db.Read(func(sql *Sql) (any, error) {
			return sql.Query("SELECT list FROM nums WHERE k = 'n1'")
		})
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		rows := v.([][]sqlite.Value)
		if len(rows) != 1 {
			return ""
		}
		if rows[0][0].Type == sqlite.Null {
			return "NULL"
		}
		return rows[0][0].Str
	}
	deadline := time.Now().Add(30 * time.Second)
	for listOf(dbB) == "" {
		if time.Now().After(deadline) {
			t.Fatal("base row never arrived on B")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := handleA.RemovePeer(idB); err != nil {
		t.Fatalf("remove peer: %v", err)
	}
	appendOp := func(db *Database, v Value) {
		t.Helper()
		if _, err := db.Transaction(func(sql *Sql) (any, error) {
			return nil, sql.ListAppend("nums", []Value{TextValue("n1")}, "list", v)
		}); err != nil {
			t.Fatalf("list append: %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	appendOp(dbA, IntValue(1))
	appendOp(dbA, IntValue(2))
	appendOp(dbB, IntValue(2))
	appendOp(dbB, IntValue(3))
	appendOp(dbA, IntValue(9))
	if _, err := dbA.Transaction(func(sql *Sql) (any, error) {
		return nil, sql.ListRemove("nums", []Value{TextValue("n1")}, "list", IntValue(9))
	}); err != nil {
		t.Fatalf("list remove: %v", err)
	}
	if err := dbA.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := handleA.AddPeer(*handleB.Status().Address); err != nil {
		t.Fatalf("re-add peer: %v", err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		a, b := listOf(dbA), listOf(dbB)
		if a != "" && a != "NULL" && a == b {
			if got := listMultiset(t, a); !reflect.DeepEqual(got, []string{"1", "2", "2", "3"}) {
				t.Fatalf("converged multiset = %v", got)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("list convergence: a=%s b=%s", a, b)
		}
		time.Sleep(30 * time.Millisecond)
	}
}

// TestCoerceKeyValue pins wire-string to key-component coercion by the
// key column's declared type: integers parse, reals parse finite,
// text passes through, anything else fails loudly.
func TestCoerceKeyValue(t *testing.T) {
	col := func(declared string) *Column { return &Column{Name: "k", DeclaredType: declared} }
	for in, want := range map[string]Value{
		"7": IntValue(7), "-3": IntValue(-3),
	} {
		got, err := CoerceKeyValue(col("INTEGER"), in)
		if err != nil || !ValuesEqual(got, want) {
			t.Fatalf("INTEGER %q = %+v, %v; want %+v", in, got, err, want)
		}
	}
	got, err := CoerceKeyValue(col("REAL"), "2.5")
	if err != nil || !ValuesEqual(got, RealValue(2.5)) {
		t.Fatalf("REAL 2.5 = %+v, %v", got, err)
	}
	got, err = CoerceKeyValue(col("TEXT"), "123")
	if err != nil || !ValuesEqual(got, TextValue("123")) {
		t.Fatalf("TEXT 123 = %+v, %v; want verbatim text", got, err)
	}
	for _, tc := range []struct {
		declared, in string
	}{
		{"INTEGER", "abc"}, {"INTEGER", "1.5"}, {"INTEGER", ""},
		{"REAL", "abc"}, {"REAL", "NaN"}, {"REAL", "Inf"},
		{"BLOB", "e301"},
	} {
		if _, err := CoerceKeyValue(col(tc.declared), tc.in); err == nil {
			t.Fatalf("%s %q accepted, want error", tc.declared, tc.in)
		}
	}
}
