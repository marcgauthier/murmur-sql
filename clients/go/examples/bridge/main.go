// Example bridge releases High ownership of a bridge-shielded field on a
// single high-receiver scratch server.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/run"
	"github.com/marcgauthier/murmur/clients/go/internal/servertest"
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
		o.ExtraTOML += fmt.Sprintf("[replication.bridge]\nrole = \"high-receiver\"\n"+
			"domain = %q\ntrusted_domains = [%q]\ndirectory = %q\nstream = \"example-stream\"\n"+
			"recipient = %q\nreceiver_secret_hex = %q\n",
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
		if err := db.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		// The High write above claimed ownership of title; releasing it
		// reports true, and a second release finds nothing owned.
		released, err := db.BridgeRelease(ctx, "notes", "r1", "title")
		if err != nil || !released {
			t.Fatalf("release: released=%v, err=%v", released, err)
		}
		if _, err := db.BridgeRelease(ctx, "notes", "r1", "title"); murmur.CodeOf(err) != "22023" {
			t.Fatalf("re-release: code = %q, want 22023 (%v)", murmur.CodeOf(err), err)
		}
		// Row release without COLUMN addresses whole-row ownership, which a
		// plain insert never creates; unknown tables are rejected too.
		if _, err := db.BridgeRelease(ctx, "notes", "r1"); murmur.CodeOf(err) != "22023" {
			t.Fatalf("row release: code = %q, want 22023 (%v)", murmur.CodeOf(err), err)
		}
		if _, err := db.BridgeRelease(ctx, "missing", "r1", "title"); murmur.CodeOf(err) != "22023" {
			t.Fatalf("unknown table: code = %q, want 22023 (%v)", murmur.CodeOf(err), err)
		}
		fmt.Println("bridge: verified field release, re-release, row form, and unknown table")
	})
}
