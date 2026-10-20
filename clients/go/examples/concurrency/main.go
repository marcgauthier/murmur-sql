// Example concurrency inserts from parallel workers through one pool.
package main

import (
	"context"
	"fmt"
	"sync"

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
			MaxOpenConns: 8,
		})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()

		if _, err := db.ExecContext(ctx,
			`CREATE TABLE events(id INTEGER PRIMARY KEY, worker INTEGER NOT NULL)`); err != nil {
			t.Fatalf("create: %v", err)
		}
		const workers, perWorker = 4, 50
		var wg sync.WaitGroup
		errs := make(chan error, workers*perWorker)
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for i := 0; i < perWorker; i++ {
					id := worker*perWorker + i + 1
					if _, err := db.ExecContext(ctx,
						"INSERT INTO events VALUES ($1, $2)", id, worker); err != nil {
						errs <- err
						return
					}
				}
			}(w)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("insert: %v", err)
		}
		var count int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM events").Scan(&count); err != nil {
			t.Fatalf("count: %v", err)
		}
		if count != workers*perWorker {
			t.Fatalf("count = %d, want %d", count, workers*perWorker)
		}
		fmt.Printf("concurrency: verified %d rows from %d workers\n", count, workers)
	})
}
