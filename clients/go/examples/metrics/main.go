// Example metrics queries live in-memory SQLite and Spool persistence metrics.
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
		if _, err := db.ExecContext(ctx, "MURMUR FLUSH"); err != nil {
			t.Fatalf("flush: %v", err)
		}

		count := 0
		for _, scope := range []string{"", "SQLITE", "SPOOL"} {
			// Scope comes from this fixed list, not external input.
			rows, err := db.QueryContext(ctx, "MURMUR SHOW METRICS "+scope)
			if err != nil {
				t.Fatalf("metrics: %v", err)
			}
			found := map[string]bool{}
			for rows.Next() {
				var subsystem, metric, value string
				if err := rows.Scan(&subsystem, &metric, &value); err != nil {
					t.Fatalf("scan: %v", err)
				}
				found[subsystem] = true
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows: %v", err)
			}
			rows.Close()
			if scope == "" && (!found["sqlite"] || !found["spool"]) {
				t.Fatalf("missing subsystems: %v", found)
			}
			if scope != "" && (len(found) != 1 || !found[strings.ToLower(scope)]) {
				t.Fatalf("wrong scope: %v", found)
			}
		}
		fmt.Printf("metrics: verified %d metric rows\n", count)
	})
}
