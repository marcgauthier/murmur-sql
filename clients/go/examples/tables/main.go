// Example tables manages per-table replication flags and a durability flush.
package main

import (
	"context"
	"fmt"

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

		for _, table := range []string{"orders", "scratch"} {
			_, err = db.ExecContext(ctx,
				fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, v TEXT NOT NULL)", table))
			if err != nil {
				t.Fatalf("create %s: %v", table, err)
			}
		}
		show := func() map[string]bool {
			rows, err := db.QueryContext(ctx, "MURMUR SHOW TABLES REPLICATION")
			if err != nil {
				t.Fatalf("flags: %v", err)
			}
			defer rows.Close()
			byName := map[string]bool{}
			for rows.Next() {
				var table string
				var replicated bool
				if err := rows.Scan(&table, &replicated); err != nil {
					t.Fatalf("flags scan: %v", err)
				}
				byName[table] = replicated
				fmt.Printf("table %s replicated=%v\n", table, replicated)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("flags rows: %v", err)
			}
			return byName
		}
		if got := show(); got["orders"] || got["scratch"] {
			t.Fatalf("defaults: %v", got)
		}
		if _, err := db.ExecContext(ctx, "MURMUR SET TABLE 'orders' REPLICATED true"); err != nil {
			t.Fatalf("set: %v", err)
		}
		if got := show(); got["scratch"] || !got["orders"] {
			t.Fatalf("after set: %v", got)
		}
		var first string
		if err := db.QueryRowContext(ctx,
			"INSERT INTO orders(id,v) VALUES($1,$2) RETURNING id_murmur_primary_key",
			1, "replicated").Scan(&first); err != nil || len(first) != 36 || first[14] != '5' {
			t.Fatalf("generated UUIDv5: %q, %v", first, err)
		}
		if _, err := db.ExecContext(ctx,
			"UPDATE orders SET id_murmur_primary_key='forged' WHERE id=1"); err == nil {
			t.Fatalf("managed column unexpectedly writable")
		}
		if _, err := db.ExecContext(ctx, "MURMUR SET TABLE 'orders' REPLICATED false"); err != nil {
			t.Fatalf("disable populated table: %v", err)
		}
		if _, err := db.ExecContext(ctx, "MURMUR SET TABLE 'orders' REPLICATED true"); err != nil {
			t.Fatalf("enable populated table: %v", err)
		}
		var again string
		if err := db.QueryRowContext(ctx,
			"SELECT id_murmur_primary_key FROM orders WHERE id=$1", 1).Scan(&again); err != nil || first != again {
			t.Fatalf("stable UUID: %q != %q, %v", first, again, err)
		}
		if _, err := db.ExecContext(ctx, "MURMUR FLUSH"); err != nil {
			t.Fatalf("flush: %v", err)
		}
		fmt.Println("tables: verified flags, UUIDv5 keys, populated conversions and flush")
	})
}
