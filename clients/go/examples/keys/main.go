// Example keys rotates the encryption key and reconnects with data intact.
package main

import (
	"context"
	"errors"
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
		cfg := murmur.Config{Host: "127.0.0.1", Port: server.Port, User: "admin", Password: "test-password", Key: "old-key"}
		db, err := murmur.Open(ctx, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := db.ExecContext(ctx, "INSERT INTO records VALUES($1,$2)", 1, "kept"); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if _, err := db.ExecContext(ctx, "MURMUR ROTATE KEY "+"'"+strings.ReplaceAll(cfg.Key, "'", "''")+"'"+" 'new-key'"); err != nil {
			t.Fatalf("rotate: %v", err)
		}
		db.Close()
		stale, err := murmur.Open(ctx, cfg)
		if err == nil {
			stale.Close()
			t.Fatalf("old key still works")
		}
		var serverErr *murmur.Error
		if !errors.As(err, &serverErr) || serverErr.Code != "28000" {
			t.Fatalf("old key: %v", err)
		}
		cfg.Key = "new-key"
		fresh, err := murmur.Open(ctx, cfg)
		if err != nil {
			t.Fatalf("reconnect: %v", err)
		}
		defer fresh.Close()
		var value string
		if err := fresh.QueryRowContext(ctx, "SELECT value FROM records WHERE id=$1", 1).Scan(&value); err != nil || value != "kept" {
			t.Fatalf("read after rotation: %q, %v", value, err)
		}
		fmt.Println("keys: verified rotation, old-key rejection and preserved data")
	})
}
