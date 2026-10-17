// Example search enables column FTS and verifies parameterized MATCH queries.
package main

import (
	"context"
	"fmt"
	"strings"

	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/run"
	"github.com/marcgauthier/murmur/clients/go/internal/servertest"
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
			if err := db.SetColumnFTS(ctx, "documents", col, true); err != nil {
				t.Fatalf("enable: %v", err)
			}
		}
		if err := db.RefreshIndexes(ctx); err != nil {
			t.Fatalf("refresh: %v", err)
		}
		columns, err := db.ColumnFTS(ctx)
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		var name string
		for _, c := range columns {
			if c.Enabled && c.IndexName != nil && c.Ready != nil && *c.Ready {
				name = *c.IndexName
			}
		}
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
		if err := db.RefreshIndexes(ctx); err != nil {
			t.Fatalf("refresh update: %v", err)
		}
		if err := db.QueryRowContext(ctx, query, "updated").Scan(&id); err != nil || id != 1 {
			t.Fatalf("updated match: id=%d err=%v", id, err)
		}
		if err := db.SetColumnFTS(ctx, "documents", "body", false); err != nil {
			t.Fatalf("disable: %v", err)
		}
		var count int64
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM temp."+ident+" WHERE "+ident+" MATCH $1", "updated").Scan(&count); err != nil || count != 0 {
			t.Fatalf("disabled column: count=%d err=%v", count, err)
		}
		if err := db.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		fmt.Println("search: verified column FTS, MATCH, refresh and disable")
	})
}
