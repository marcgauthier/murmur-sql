// Example basic opens a scratch server, unlocks it, and runs CRUD with
// parameters over database/sql.
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

		must := func(err error) {
			if err != nil {
				t.Fatalf("%v", err)
			}
		}
		_, err = db.ExecContext(ctx, `CREATE TABLE greetings(
			id INTEGER PRIMARY KEY, lang TEXT NOT NULL, text TEXT NOT NULL)`)
		must(err)
		for id, greeting := range map[int][2]string{
			1: {"en", "hello"}, 2: {"fr", "bonjour"}, 3: {"es", "hola"},
		} {
			_, err = db.ExecContext(ctx,
				"INSERT INTO greetings VALUES ($1, $2, $3)", id, greeting[0], greeting[1])
			must(err)
		}
		rows, err := db.QueryContext(ctx,
			"SELECT lang, text FROM greetings WHERE id >= $1 ORDER BY id", 1)
		must(err)
		defer rows.Close()
		count := 0
		for rows.Next() {
			var lang, text string
			must(rows.Scan(&lang, &text))
			fmt.Printf("%s: %s\n", lang, text)
			count++
		}
		must(rows.Err())
		if count != 3 {
			t.Fatalf("got %d rows, want 3", count)
		}
		fmt.Println("basic: verified 3 rows")
	})
}
