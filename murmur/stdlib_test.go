package murmur_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur"
)

// openStdlibDB opens an embedded database and its database/sql handle.
// The sql.DB closes before the embedded database.
func openStdlibDB(t *testing.T) (*murmur.Database, *sql.DB) {
	t.Helper()
	dir := t.TempDir()
	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenCreateNew
	db, err := murmur.Open(dir+"/store", exampleKey(), options)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sqldb := db.OpenDB()
	t.Cleanup(func() { sqldb.Close() })
	return db, sqldb
}

// TestStdlibExecQueryRoundTrip pins the database/sql adapter: DDL through
// Exec, writes with mixed argument types, and reads through Query/QueryRow.
func TestStdlibExecQueryRoundTrip(t *testing.T) {
	_, sqldb := openStdlibDB(t)
	ctx := context.Background()
	if _, err := sqldb.ExecContext(ctx,
		`CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, data BLOB, at TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	when := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	res, err := sqldb.ExecContext(ctx,
		"INSERT INTO users(id, name, score, data, at) VALUES(?, ?, ?, ?, ?)",
		1, "ada", 9.5, []byte{1, 2, 3}, when)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("rows affected = %d, %v; want 1", n, err)
	}
	if _, err := sqldb.ExecContext(ctx,
		"INSERT INTO users(id, name) VALUES(?, ?)", 2, "grace"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var name, at string
	var score float64
	var data []byte
	if err := sqldb.QueryRowContext(ctx,
		"SELECT name, score, data, at FROM users WHERE id = ?", 1).Scan(&name, &score, &data, &at); err != nil {
		t.Fatalf("query row: %v", err)
	}
	if name != "ada" || score != 9.5 || string(data) != string([]byte{1, 2, 3}) {
		t.Fatalf("row = %q %v %x", name, score, data)
	}
	if at != "2024-05-06 07:08:09.000000" {
		t.Fatalf("at = %q, want server timestamp text", at)
	}
	rows, err := sqldb.QueryContext(ctx, "SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "name" {
		t.Fatalf("columns = %v", cols)
	}
	var ids []int64
	var names []string
	for rows.Next() {
		var id int64
		var n string
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if len(ids) != 2 || ids[0] != 1 || names[1] != "grace" {
		t.Fatalf("rows = %v %v", ids, names)
	}
	if err := sqldb.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

// TestStdlibQueryMatchesNative pins that driver reads return the same rows
// as the embedded Read/Query path.
func TestStdlibQueryMatchesNative(t *testing.T) {
	db, sqldb := openStdlibDB(t)
	ctx := context.Background()
	if err := db.Migrate(murmur.NewMigration(
		`CREATE TABLE kv(k TEXT NOT NULL PRIMARY KEY, v INTEGER NOT NULL)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		for i := 0; i < 25; i++ {
			if _, err := s.Execute("INSERT INTO kv(k, v) VALUES(?, ?)", fmt.Sprintf("k%02d", i), i); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rows, err := sqldb.QueryContext(ctx, "SELECT k, v FROM kv ORDER BY k")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var k string
		var v int64
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if k != fmt.Sprintf("k%02d", count) || v != int64(count) {
			t.Fatalf("row %d = %q %d", count, k, v)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count != 25 {
		t.Fatalf("rows = %d, want 25", count)
	}
}

// TestStdlibTxCommit pins atomic commit: both writes land together.
func TestStdlibTxCommit(t *testing.T) {
	_, sqldb := openStdlibDB(t)
	ctx := context.Background()
	if _, err := sqldb.ExecContext(ctx,
		`CREATE TABLE accounts(id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := sqldb.ExecContext(ctx,
		"INSERT INTO accounts(id, balance) VALUES(1, 100), (2, 100)"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	tx, err := sqldb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - 30 WHERE id = 1"); err != nil {
		t.Fatalf("debit: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + 30 WHERE id = 2"); err != nil {
		t.Fatalf("credit: %v", err)
	}
	var mid int64
	if err := tx.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE id = 2").Scan(&mid); err != nil {
		t.Fatalf("tx read: %v", err)
	}
	if mid != 130 {
		t.Fatalf("tx read = %d, want 130", mid)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	var a, b int64
	if err := sqldb.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE id = 1").Scan(&a); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := sqldb.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE id = 2").Scan(&b); err != nil {
		t.Fatalf("read: %v", err)
	}
	if a != 70 || b != 130 {
		t.Fatalf("balances = %d/%d, want 70/130", a, b)
	}
}

// TestStdlibTxRollback pins atomic rollback: a failed transaction applies
// nothing.
func TestStdlibTxRollback(t *testing.T) {
	_, sqldb := openStdlibDB(t)
	ctx := context.Background()
	if _, err := sqldb.ExecContext(ctx,
		`CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	tx, err := sqldb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t(id, v) VALUES(1, 'a')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t(id, v) VALUES(1, 'dup')"); err == nil {
		t.Fatalf("duplicate insert succeeded, want error")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	var count int64
	if err := sqldb.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 after rollback", count)
	}
}

// TestStdlibRejectsUnsupported pins clear errors for unsupported shapes:
// multi-statement queries, DDL in Query or inside Tx, named parameters,
// and non-default transaction options.
func TestStdlibRejectsUnsupported(t *testing.T) {
	_, sqldb := openStdlibDB(t)
	ctx := context.Background()
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t(id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := sqldb.QueryContext(ctx, "SELECT 1; SELECT 2"); err == nil {
		t.Fatalf("multi-statement query succeeded, want error")
	}
	if _, err := sqldb.QueryContext(ctx, "CREATE TABLE q(id INTEGER PRIMARY KEY)"); err == nil {
		t.Fatalf("DDL query succeeded, want error")
	}
	if _, err := sqldb.ExecContext(ctx, ""); err == nil {
		t.Fatalf("empty exec succeeded, want error")
	}
	if _, err := sqldb.ExecContext(ctx, "SELECT ?", sql.Named("x", 1)); err == nil {
		t.Fatalf("named parameter succeeded, want error")
	}
	if _, err := sqldb.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}); err == nil {
		t.Fatalf("serializable begin succeeded, want error")
	}
	if _, err := sqldb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true}); err == nil {
		t.Fatalf("read-only begin succeeded, want error")
	}
	tx, err := sqldb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "CREATE TABLE txddl(id INTEGER PRIMARY KEY)"); err == nil {
		t.Fatalf("DDL inside tx succeeded, want error")
	}
}

// TestStdlibExecBatchAndTxnNoops pins multi-statement Exec and the server's
// transaction no-op statements.
func TestStdlibExecBatchAndTxnNoops(t *testing.T) {
	_, sqldb := openStdlibDB(t)
	ctx := context.Background()
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
		INSERT INTO t(id, v) VALUES(1, 'a');
		INSERT INTO t(id, v) VALUES(2, 'b');`); err != nil {
		t.Fatalf("batch: %v", err)
	}
	for _, noop := range []string{"BEGIN", "COMMIT", "ROLLBACK", "START TRANSACTION", "END"} {
		if _, err := sqldb.ExecContext(ctx, noop); err != nil {
			t.Fatalf("noop %q: %v", noop, err)
		}
	}
	var count int64
	if err := sqldb.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
}

// TestStdlibConcurrentUse smokes concurrent database/sql handles: all
// operations funnel through the single writer thread.
func TestStdlibConcurrentUse(t *testing.T) {
	_, sqldb := openStdlibDB(t)
	ctx := context.Background()
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	var id atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				n := id.Add(1)
				if _, err := sqldb.ExecContext(ctx, "INSERT INTO t(id, v) VALUES(?, ?)", n, "x"); err != nil {
					errs <- err
					return
				}
				var c int64
				if err := sqldb.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&c); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent op: %v", err)
	}
	var count int64
	if err := sqldb.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 80 {
		t.Fatalf("count = %d, want 80", count)
	}
}

// TestStdlibClosedPins errors after Close.
func TestStdlibClosedPins(t *testing.T) {
	_, sqldb := openStdlibDB(t)
	if _, err := sqldb.ExecContext(context.Background(), `CREATE TABLE t(id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := sqldb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := sqldb.ExecContext(context.Background(), "INSERT INTO t(id) VALUES(1)"); err == nil {
		t.Fatalf("exec after close succeeded, want error")
	}
}
