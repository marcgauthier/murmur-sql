// Example files transfers arbitrary binary content through PostgreSQL BYTEA.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	murmur "github.com/marcgauthier/murmur-sql/clients/go"
	"github.com/marcgauthier/murmur-sql/clients/go/examples/internal/run"
	"github.com/marcgauthier/murmur-sql/clients/go/internal/servertest"
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
		// Handles belong to a physical connection, so pin it for the operation.
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("connection: %v", err)
		}
		defer conn.Close()
		payload := bytes.Repeat([]byte{0, 1, 255, 128}, 40000)
		var upload string
		if err := conn.QueryRowContext(ctx, "MURMUR FILE UPLOAD BEGIN $1", "reports/sample.bin").Scan(&upload); err != nil {
			t.Fatalf("begin: %v", err)
		}
		// ABORT only has an effect while the upload is unfinished.
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			conn.ExecContext(cleanupCtx, "MURMUR FILE UPLOAD ABORT $1", upload)
		}()
		for offset := 0; offset < len(payload); offset += 65536 {
			end := min(offset+65536, len(payload))
			if _, err := conn.ExecContext(ctx, "MURMUR FILE UPLOAD APPEND $1 $2", upload, payload[offset:end]); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		var name, hash string
		var size, modified int64
		var local bool
		if err := conn.QueryRowContext(ctx, "MURMUR FILE UPLOAD FINISH $1", upload).Scan(&name, &size, &modified, &hash, &local); err != nil {
			t.Fatalf("finish: %v", err)
		}
		if size != int64(len(payload)) || !local || hash != fmt.Sprintf("%x", sha256.Sum256(payload)) {
			t.Fatalf("bad upload metadata")
		}

		var cursor string
		if err := conn.QueryRowContext(ctx, "MURMUR FILE SEARCH $1", "reports/%.bin").Scan(&cursor); err != nil {
			t.Fatalf("search: %v", err)
		}
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			conn.ExecContext(cleanupCtx, "MURMUR FILE CLOSE $1", cursor)
		}()
		count := 0
		for {
			rows, err := conn.QueryContext(ctx, "MURMUR FILE NEXT $1 LIMIT 100", cursor)
			if err != nil {
				t.Fatalf("page: %v", err)
			}
			pageCount := 0
			for rows.Next() {
				var gotName, gotHash string
				var gotSize, gotModified int64
				var gotLocal bool
				if err := rows.Scan(&gotName, &gotSize, &gotModified, &gotHash, &gotLocal); err != nil {
					t.Fatalf("scan: %v", err)
				}
				if gotName != name || gotSize != size || gotHash != hash || !gotLocal {
					t.Fatalf("metadata mismatch")
				}
				pageCount++
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows: %v", err)
			}
			rows.Close()
			if pageCount == 0 {
				break
			}
		}
		if count != 1 {
			t.Fatalf("got %d files, want 1", count)
		}

		var download, downloadName string
		var downloadSize int64
		if err := conn.QueryRowContext(ctx, "MURMUR FILE DOWNLOAD $1", name).Scan(&download, &downloadName, &downloadSize); err != nil {
			t.Fatalf("download: %v", err)
		}
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			conn.ExecContext(cleanupCtx, "MURMUR FILE CLOSE $1", download)
		}()
		if downloadName != name || downloadSize != size {
			t.Fatalf("download metadata mismatch")
		}
		var out bytes.Buffer
		for {
			var offset, total int64
			var chunk []byte
			var eof bool
			if err := conn.QueryRowContext(ctx, "MURMUR FILE DOWNLOAD NEXT $1 LIMIT 65536", download).Scan(&offset, &total, &chunk, &eof); err != nil {
				t.Fatalf("chunk: %v", err)
			}
			if offset != int64(out.Len()) || total != size {
				t.Fatalf("invalid chunk bounds")
			}
			out.Write(chunk)
			if eof {
				break
			}
			if len(chunk) == 0 {
				t.Fatalf("empty nonfinal chunk")
			}
		}
		if !bytes.Equal(out.Bytes(), payload) {
			t.Fatalf("binary mismatch")
		}
		fmt.Printf("Verified %s: %d bytes, SHA256 %s\n", name, size, hash)
	})
}
