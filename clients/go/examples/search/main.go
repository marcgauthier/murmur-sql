// Example search enables column FTS and verifies parameterized MATCH queries.
package main

import (
	"context"
	"fmt"
	"strings"

	murmur "github.com/marcgauthier/murmur-sql/clients/go"
	"github.com/marcgauthier/murmur-sql/clients/go/examples/internal/run"
	"github.com/marcgauthier/murmur-sql/clients/go/internal/servertest"
)

func main() {
	run.Main(func(t servertest.TB, dir string) {
		ctx := context.Background()
		server := servertest.Start(t, dir, servertest.Options{})
		db, err := murmur.Open(ctx, murmur.Config{Host: "127.0.0.1", Port: server.Port, User: "admin", Password: "test-password", Key: "example-key"})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()
		for _, sql := range []string{
			"CREATE TABLE documents(id INTEGER PRIMARY KEY,title TEXT,body TEXT)",
			"INSERT INTO documents VALUES(1,'SQLite','fast search'),(2,'Notes','other text')",
		} {
			if _, err := db.ExecContext(ctx, sql); err != nil {
				t.Fatalf("setup: %v", err)
			}
		}
		for _, col := range []string{"title", "body"} {
			if _, err := db.ExecContext(ctx, "MURMUR SET COLUMN 'documents' "+"'"+strings.ReplaceAll(col, "'", "''")+"'"+" FTS true"); err != nil {
				t.Fatalf("enable: %v", err)
			}
		}
		if _, err := db.ExecContext(ctx, "MURMUR REFRESH INDEXES"); err != nil {
			t.Fatalf("refresh: %v", err)
		}
		rows, err := db.QueryContext(ctx, "MURMUR SHOW COLUMNS FTS")
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		var name string
		for rows.Next() {
			var table, column string
			var enabled bool
			var indexName, indexError *string
			var ready, rebuilding *bool
			var pending, queued *int64
			if err := rows.Scan(&table, &column, &enabled, &indexName, &ready, &rebuilding, &pending, &queued, &indexError); err != nil {
				t.Fatalf("scan: %v", err)
			}
			if enabled && indexName != nil && ready != nil && *ready {
				name = *indexName
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
		rows.Close()
		if name == "" {
			t.Fatalf("no ready index")
		}
		ident := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		query := "SELECT d.id FROM documents d JOIN temp." + ident + " f ON f.rowid=d.id WHERE " + ident + " MATCH $1"
		var id int64
		if err := db.QueryRowContext(ctx, query, "SQLite AND search").Scan(&id); err != nil || id != 1 {
			t.Fatalf("match: id=%d err=%v", id, err)
		}
		if _, err := db.ExecContext(ctx, "UPDATE documents SET body='updated' WHERE id=1"); err != nil {
			t.Fatalf("update: %v", err)
		}
		if _, err := db.ExecContext(ctx, "MURMUR REFRESH INDEXES"); err != nil {
			t.Fatalf("refresh update: %v", err)
		}
		if err := db.QueryRowContext(ctx, query, "updated").Scan(&id); err != nil || id != 1 {
			t.Fatalf("updated match: id=%d err=%v", id, err)
		}
		if _, err := db.ExecContext(ctx, "MURMUR SET COLUMN 'documents' 'body' FTS false"); err != nil {
			t.Fatalf("disable: %v", err)
		}
		var count int64
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM temp."+ident+" WHERE "+ident+" MATCH $1", "updated").Scan(&count); err != nil || count != 0 {
			t.Fatalf("disabled column: count=%d err=%v", count, err)
		}
		if _, err := db.ExecContext(ctx, "MURMUR FLUSH"); err != nil {
			t.Fatalf("flush: %v", err)
		}
		fmt.Println("search: verified column FTS, MATCH, refresh and disable")
	})
}
