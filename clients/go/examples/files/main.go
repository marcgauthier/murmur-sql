// Example files transfers arbitrary binary content through PostgreSQL BYTEA.
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
		server := servertest.Start(t, dir, servertest.FileOptions(t, dir))
		db, err := murmur.Open(ctx, murmur.Config{Port: server.Port, User: "admin", Password: "test-password", Key: "files-example"})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()
		payload := bytes.Repeat([]byte{0, 1, 255, 128}, 40000)
		info, err := db.UploadFile(ctx, "reports/sample.bin", bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("upload: %v", err)
		}
		cursor, err := db.SearchFiles(ctx, "reports/%.bin")
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		defer cursor.Close()
		count := 0
		for cursor.Next(ctx) {
			f := cursor.File()
			if f.Name != info.Name || f.Size != int64(len(payload)) || !f.Local {
				t.Fatalf("metadata: %+v", f)
			}
			count++
		}
		if cursor.Err() != nil || count != 1 {
			t.Fatalf("cursor: %d %v", count, cursor.Err())
		}
		var out bytes.Buffer
		if err = db.DownloadFile(ctx, info.Name, &out); err != nil {
			t.Fatalf("download: %v", err)
		}
		if !bytes.Equal(out.Bytes(), payload) {
			t.Fatalf("binary mismatch")
		}
		fmt.Printf("Verified %s: %d bytes, SHA256 %s\n", info.Name, info.Size, info.SHA256)
	})
}
