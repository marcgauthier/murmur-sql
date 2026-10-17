// Example migrations applies schema versions through server-side DDL and
// verifies each step.
package main

import (
	"context"
	"fmt"

	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/run"
	"github.com/marcgauthier/murmur/clients/go/internal/servertest"
)

var migrations = []string{
	`CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`,
	`ALTER TABLE users ADD COLUMN email TEXT NOT NULL DEFAULT ''`,
	`CREATE INDEX users_name ON users(name)`,
}

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

		for i, migration := range migrations {
			if _, err := db.ExecContext(ctx, migration); err != nil {
				t.Fatalf("migration %d: %v", i+1, err)
			}
			fmt.Printf("applied migration %d\n", i+1)
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO users VALUES ($1, $2, $3)", 1, "ada", "ada@example.com"); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var email string
		if err := db.QueryRowContext(ctx,
			"SELECT email FROM users WHERE name = $1", "ada").Scan(&email); err != nil {
			t.Fatalf("select: %v", err)
		}
		if email != "ada@example.com" {
			t.Fatalf("email = %q", email)
		}
		fmt.Println("migrations: verified 3 migrations")
	})
}
