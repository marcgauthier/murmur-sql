// Example metrics queries live in-memory SQLite and Spool persistence metrics.
package main

import (
	"context"
	"fmt"

	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/run"
	"github.com/marcgauthier/murmur/clients/go/internal/servertest"
)

func main() {
	run.Main(func(t servertest.TB, dir string) {
		ctx := context.Background()
		server := servertest.Start(t, dir, servertest.Options{})
		db, err := murmur.Open(ctx, murmur.Config{
			Host: "127.0.0.1", Port: server.Port,
			User: "admin", Password: "test-password", Key: "example-key",
		})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()

		_, err = db.ExecContext(ctx, "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
		if err != nil {
			t.Fatalf("create table: %v", err)
		}
		_, err = db.ExecContext(ctx, "INSERT INTO items VALUES (1, 'widget')")
		if err != nil {
			t.Fatalf("insert item: %v", err)
		}
		if err := db.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}

		metrics, err := db.Metrics(ctx)
		if err != nil {
			t.Fatalf("metrics: %v", err)
		}
		foundSqlite := false
		foundSpool := false
		for _, m := range metrics {
			if m.Subsystem == "sqlite" {
				foundSqlite = true
			}
			if m.Subsystem == "spool" {
				foundSpool = true
			}
		}
		if !foundSqlite || !foundSpool {
			t.Fatalf("expected both sqlite and spool metrics, got %d total", len(metrics))
		}

		sqliteOnly, err := db.Metrics(ctx, "sqlite")
		if err != nil {
			t.Fatalf("sqlite metrics: %v", err)
		}
		for _, m := range sqliteOnly {
			if m.Subsystem != "sqlite" {
				t.Fatalf("expected subsystem sqlite, got %s", m.Subsystem)
			}
		}

		fmt.Printf("metrics: verified %d metrics\n", len(metrics))
	})
}
