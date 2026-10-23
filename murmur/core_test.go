package murmur

import (
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
)

func testKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

func openTestDB(t *testing.T, path string) *Database {
	t.Helper()
	options := DefaultOptions()
	options.Mode = OpenCreateNew
	db, err := Open(path, testKey(), options)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

func reopenTestDB(t *testing.T, path string) *Database {
	t.Helper()
	db, err := Open(path, testKey(), DefaultOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return db
}

const coreTestDDL = `CREATE TABLE users (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	score INTEGER NOT NULL DEFAULT 0
);`

func execMust(t *testing.T, db *Database, stmt string) {
	t.Helper()
	if _, err := db.Transaction(func(sql *Sql) (any, error) {
		if _, err := sql.Execute(stmt); err != nil {
			return nil, err
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

func TestOpenMigrateWriteReadReopen(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, "INSERT INTO users (id, name, score) VALUES (1, 'ada', 10)")
	readName := func(db *Database) string {
		v, err := db.Read(func(sql *Sql) (any, error) {
			return sql.QueryRow("SELECT name FROM users WHERE id = 1")
		})
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return v.([]sqlite.Value)[0].Str
	}
	if got := readName(db); got != "ada" {
		t.Fatalf("name = %q", got)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2 := reopenTestDB(t, dir+"/db")
	defer db2.Close()
	if got := readName(db2); got != "ada" {
		t.Fatalf("after reopen name = %q", got)
	}
	status := db2.Status()
	if status.Committed == 0 || status.Durable == 0 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestOpenWrongKey(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	wrong := make([]byte, 32)
	for i := range wrong {
		wrong[i] = byte(0xff - i)
	}
	if _, err := Open(dir+"/db", wrong, DefaultOptions()); err == nil {
		t.Fatal("expected wrong-key error")
	}
}

type testAbort struct{}

func (testAbort) Error() string { return "abort" }

var errTestAbort error = testAbort{}

func TestTransactionRollbackOnError(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_, err := db.Transaction(func(sql *Sql) (any, error) {
		if _, err := sql.Execute("INSERT INTO users (id, name) VALUES (1, 'ada')"); err != nil {
			return nil, err
		}
		return nil, errTestAbort
	})
	if err != errTestAbort {
		t.Fatalf("err = %v", err)
	}
	v, err := db.Read(func(sql *Sql) (any, error) {
		return sql.Query("SELECT id FROM users")
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(v.([][]sqlite.Value)) != 0 {
		t.Fatal("aborted transaction left rows behind")
	}
}

type userRow struct {
	ID    int64
	Name  string
	Score int64
}

func TestSubscriptionReceivesUpdates(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	query := func(sql *Sql) ([]userRow, error) {
		rows, err := sql.Query("SELECT id, name, score FROM users ORDER BY id")
		if err != nil {
			return nil, err
		}
		out := make([]userRow, len(rows))
		for i, r := range rows {
			out[i] = userRow{ID: r[0].Int, Name: r[1].Str, Score: r[2].Int}
		}
		return out, nil
	}
	sub, err := StartSubscription(db, query, func(u *userRow) []byte {
		return []byte(u.Name)
	}, SubscriptionOptions{Tables: []string{"users"}})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()
	initial, err := sub.RecvTimeout(5 * time.Second)
	if err != nil || initial.Kind != SubInitial || len(initial.Rows) != 0 {
		t.Fatalf("initial = %+v, %v", initial, err)
	}
	execMust(t, db, "INSERT INTO users (id, name, score) VALUES (1, 'ada', 3)")
	update, err := sub.RecvTimeout(5 * time.Second)
	if err != nil || update.Kind != SubUpdate || len(update.Rows) != 1 || update.Rows[0].Name != "ada" {
		t.Fatalf("update = %+v, %v", update, err)
	}
	if len(update.Changes) != 1 || update.Changes[0].Type != ChangeAdded {
		t.Fatalf("changes = %+v", update.Changes)
	}
}

func TestFtsSearchRoundtrip(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	defs := []SearchIndex{Fts5SearchIndex(NewFts5Index("user_search", "users", []string{"name"}))}
	if err := db.SetSearchIndexes(defs); err != nil {
		t.Fatalf("set search indexes: %v", err)
	}
	execMust(t, db, "INSERT INTO users (id, name) VALUES (1, 'ada lovelace')")
	execMust(t, db, "INSERT INTO users (id, name) VALUES (2, 'grace hopper')")
	if err := db.RefreshIndexes(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	v, err := db.Read(func(sql *Sql) (any, error) {
		return sql.Query(`SELECT name FROM temp."user_search" WHERE "user_search" MATCH 'lovelace'`)
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) != 1 || rows[0][0].Str != "ada lovelace" {
		t.Fatalf("search rows = %+v", rows)
	}
}

func TestSetReplicationToggle(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	enabled, err := db.ReplicationEnabled("users")
	if err != nil || enabled {
		t.Fatalf("enabled = %v, %v (want explicit opt-in default off)", enabled, err)
	}
	if err := db.SetReplication("users", true); err != nil {
		t.Fatalf("enable: %v", err)
	}
	enabled, err = db.ReplicationEnabled("users")
	if err != nil || !enabled {
		t.Fatalf("enabled = %v, %v", enabled, err)
	}
	if err := db.SetReplication("users", false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	enabled, err = db.ReplicationEnabled("users")
	if err != nil || enabled {
		t.Fatalf("enabled = %v, %v", enabled, err)
	}
}

func TestBackupRestoreRoundtrip(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, "INSERT INTO users (id, name) VALUES (7, 'backup')")
	if err := db.Backup(dir + "/backup"); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := VerifyBackupDir(dir+"/backup", testKey()); err != nil {
		t.Fatalf("verify: %v", err)
	}
	restoreOpts := DefaultRestoreOptions()
	if err := RestoreBackup(dir+"/backup", dir+"/restored", testKey(), &restoreOpts); err != nil {
		t.Fatalf("restore: %v", err)
	}
	db2 := reopenTestDB(t, dir+"/restored")
	defer db2.Close()
	v, err := db2.Read(func(sql *Sql) (any, error) {
		return sql.QueryRow("SELECT name FROM users WHERE id = 7")
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if v.([]sqlite.Value)[0].Str != "backup" {
		t.Fatalf("restored row = %+v", v)
	}
}

func TestExportMergeState(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	execMust(t, db, "INSERT INTO users (id, name) VALUES (1, 'ada')")
	if err := db.SetReplication("users", true); err != nil {
		t.Fatalf("enable replication: %v", err)
	}
	state, err := db.ExportState()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(state.Rows) == 0 {
		t.Fatal("exported state has no rows")
	}
	if err := db.MergeState(state); err != nil {
		t.Fatalf("merge: %v", err)
	}
}

func TestAuthorizerBlocksInternalTables(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(coreTestDDL)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_, err := db.Transaction(func(sql *Sql) (any, error) {
		_, err := sql.Execute("CREATE TABLE _crdt_evil (id INTEGER)")
		return nil, err
	})
	if err == nil || !strings.Contains(err.Error(), "authoriz") {
		t.Fatalf("expected authorizer denial, got %v", err)
	}
}
