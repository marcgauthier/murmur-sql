// Example bridge releases High ownership of a bridge-shielded field on a
// single high-receiver scratch server.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	murmur "github.com/marcgauthier/murmur-sql/clients/go"
	"github.com/marcgauthier/murmur-sql/clients/go/examples/internal/run"
	"github.com/marcgauthier/murmur-sql/clients/go/internal/servertest"
)

func main() {
	run.Main(func(t servertest.TB, dir string) {
		ctx := context.Background()
		node := filepath.Join(dir, "high")
		if err := os.MkdirAll(node, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		opts := servertest.MeshOptions(t, []string{node}, "")
		// Scratch high-receiver: no bundles are exchanged, so the Low domain
		// and recipient/secret are arbitrary distinct test values. Domain
		// must equal the node's own database id.
		o := opts[0]
		o.ExtraYAML += fmt.Sprintf("  bridge:\n    role: \"high-receiver\"\n"+
			"    domain: %q\n    trusted_domains:\n      - %q\n    directory: %q\n    stream: \"example-stream\"\n"+
			"    recipient: %q\n    receiver_secret_hex: %q\n",
			o.DatabaseID, strings.Repeat("11", 16), filepath.Join(node, "inbox"),
			strings.Repeat("ee", 32), strings.Repeat("dd", 32))
		server := servertest.Start(t, node, o)
		db, err := murmur.Open(ctx, murmur.Config{
			Host: "127.0.0.1", Port: server.Port,
			User: "admin", Password: "test-password", Key: "example-key",
		})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()

		if _, err := db.ExecContext(ctx, `CREATE TABLE notes(name TEXT NOT NULL PRIMARY KEY,
			title TEXT NOT NULL)`); err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := db.ExecContext(ctx, "INSERT INTO notes VALUES ($1, $2)", "r1", "v1"); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if _, err := db.ExecContext(ctx, "MURMUR FLUSH"); err != nil {
			t.Fatalf("flush: %v", err)
		}
		// The High write above claimed ownership of title; releasing it
		// reports true, and a second release finds nothing owned.
		var released bool
		err = db.QueryRowContext(ctx, "MURMUR BRIDGE RELEASE notes 'r1' COLUMN title").Scan(&released)
		if err != nil || !released {
			t.Fatalf("release=%v: %v", released, err)
		}
		for _, query := range []string{
			"MURMUR BRIDGE RELEASE notes 'r1' COLUMN title",
			"MURMUR BRIDGE RELEASE notes 'r1'",
			"MURMUR BRIDGE RELEASE missing 'r1' COLUMN title",
		} {
			_, err := db.ExecContext(ctx, query)
			var serverErr *murmur.Error
			if !errors.As(err, &serverErr) || serverErr.Code != "22023" {
				t.Fatalf("%s: %v", query, err)
			}
		}
		fmt.Println("bridge: verified field release, re-release, row form, and unknown table")
	})
}
