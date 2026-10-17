// Example tls connects over verified TLS with a minted test PKI.
package main

import (
	"context"
	"fmt"
	"path/filepath"

	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/run"
	"github.com/marcgauthier/murmur/clients/go/internal/servertest"
)

func main() {
	run.Main(func(t servertest.TB, dir string) {
		ctx := context.Background()
		pki := servertest.IssuePKI(t)
		server := servertest.Start(t, dir, servertest.Options{
			TLSFiles: pki.WriteServerFiles(t, dir),
		})
		db, err := murmur.Open(ctx, murmur.Config{
			Host: "127.0.0.1", Port: server.Port,
			User: "admin", Password: "test-password", Key: "example-key",
			TLS: murmur.TLSConfig{
				Mode:   murmur.TLSVerifyFull,
				CAFile: filepath.Join(dir, "ca.crt"),
			},
		})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()

		if _, err := db.ExecContext(ctx,
			`CREATE TABLE secrets(id INTEGER PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO secrets VALUES ($1, $2)", 1, "over-tls"); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var value string
		if err := db.QueryRowContext(ctx,
			"SELECT v FROM secrets WHERE id = 1").Scan(&value); err != nil {
			t.Fatalf("select: %v", err)
		}
		if value != "over-tls" {
			t.Fatalf("value = %q", value)
		}
		fmt.Println("tls: verified round-trip over verified TLS")
	})
}
