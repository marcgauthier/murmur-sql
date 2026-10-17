// Example backup saves a datetime-named archive, lists it, downloads and
// re-uploads it, then restores from it.
package main

import (
	"bytes"
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

		_, err = db.ExecContext(ctx,
			`CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT NOT NULL)`)
		must(err)
		_, err = db.ExecContext(ctx, "INSERT INTO docs VALUES (1, 'v1')")
		must(err)

		saved, err := db.BackupSave(ctx)
		must(err)
		fmt.Printf("saved %s (%d bytes)\n", saved.Name, saved.Size)

		files, err := db.BackupList(ctx)
		must(err)
		if len(files) != 1 || files[0].Name != saved.Name {
			t.Fatalf("list = %+v", files)
		}

		var downloaded bytes.Buffer
		must(db.BackupDownload(ctx, saved.Name, &downloaded))
		if int64(downloaded.Len()) != saved.Size {
			t.Fatalf("downloaded %d bytes, want %d", downloaded.Len(), saved.Size)
		}
		copyName := "copy-" + saved.Name
		must(db.BackupUpload(ctx, copyName, bytes.NewReader(downloaded.Bytes())))
		files, err = db.BackupList(ctx)
		must(err)
		if len(files) != 2 {
			t.Fatalf("list after upload = %+v", files)
		}

		_, err = db.ExecContext(ctx, "INSERT INTO docs VALUES (2, 'v2')")
		must(err)
		must(db.Restore(ctx, saved.Name, "example-key"))
		var count int
		must(db.QueryRowContext(ctx, "SELECT count(*) FROM docs").Scan(&count))
		if count != 1 {
			t.Fatalf("count after restore = %d, want 1", count)
		}
		fmt.Println("backup: verified save, list, download, upload, restore")
	})
}
