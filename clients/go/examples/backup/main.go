// Example backup saves a datetime-named archive, lists it, downloads and
// re-uploads it, then restores from it.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

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

		var saved string
		var savedSize int64
		must(db.QueryRowContext(ctx, "MURMUR BACKUP SAVE").Scan(&saved, &savedSize))
		if savedSize <= 0 {
			t.Fatalf("empty archive")
		}
		rows, err := db.QueryContext(ctx, "MURMUR BACKUP LIST")
		must(err)
		count := 0
		for rows.Next() {
			var name, modified string
			var size int64
			must(rows.Scan(&name, &size, &modified))
			if name != saved || size != savedSize {
				t.Fatalf("backup list mismatch")
			}
			count++
		}
		must(rows.Err())
		rows.Close()
		if count != 1 {
			t.Fatalf("expected one backup")
		}

		// Backup DOWNLOAD returns byte ranges; its names/offsets are SQL literals.
		var downloaded bytes.Buffer
		for offset := int64(0); ; {
			var actual, total int64
			var chunk []byte
			query := fmt.Sprintf("MURMUR BACKUP DOWNLOAD '%s' OFFSET %d LIMIT 1048576", strings.ReplaceAll(saved, "'", "''"), offset)
			must(db.QueryRowContext(ctx, query).Scan(&actual, &total, &chunk))
			if actual != offset {
				t.Fatalf("unexpected offset")
			}
			downloaded.Write(chunk)
			offset += int64(len(chunk))
			if offset >= total {
				break
			}
			if len(chunk) == 0 {
				t.Fatalf("empty nonfinal chunk")
			}
		}
		if int64(downloaded.Len()) != savedSize {
			t.Fatalf("download size mismatch")
		}
		copyName := "copy-" + saved
		quotedCopy := "'" + strings.ReplaceAll(copyName, "'", "''") + "'"
		_, err = db.ExecContext(ctx, "MURMUR BACKUP UPLOAD BEGIN "+quotedCopy)
		must(err)
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			db.ExecContext(cleanupCtx, "MURMUR BACKUP UPLOAD ABORT "+quotedCopy)
		}()
		payload := downloaded.Bytes()
		for offset := 0; offset < len(payload); offset += 256 << 10 {
			end := min(offset+(256<<10), len(payload))
			query := "MURMUR BACKUP UPLOAD APPEND " + quotedCopy + " '" + base64.StdEncoding.EncodeToString(payload[offset:end]) + "'"
			_, err = db.ExecContext(ctx, query)
			must(err)
		}
		var uploaded string
		var uploadedSize int64
		must(db.QueryRowContext(ctx, "MURMUR BACKUP UPLOAD FINISH "+quotedCopy).Scan(&uploaded, &uploadedSize))
		if uploaded != copyName || uploadedSize != savedSize {
			t.Fatalf("uploaded archive mismatch")
		}
		rows, err = db.QueryContext(ctx, "MURMUR BACKUP LIST")
		must(err)
		count = 0
		for rows.Next() {
			var name, modified string
			var size int64
			must(rows.Scan(&name, &size, &modified))
			count++
		}
		must(rows.Err())
		rows.Close()
		if count != 2 {
			t.Fatalf("expected two backups")
		}
		_, err = db.ExecContext(ctx, "INSERT INTO docs VALUES (2,'v2')")
		must(err)
		_, err = db.ExecContext(ctx, "MURMUR RESTORE FROM '"+strings.ReplaceAll(saved, "'", "''")+"' KEY 'example-key'")
		must(err)
		must(db.QueryRowContext(ctx, "SELECT count(*) FROM docs").Scan(&count))
		if count != 1 {
			t.Fatalf("count after restore=%d, want 1", count)
		}
		fmt.Println("backup: verified save, list, download, upload, restore")
	})
}
