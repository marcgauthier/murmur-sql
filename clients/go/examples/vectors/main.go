// Example vectors configures a column and checks local nearest-neighbor results.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	murmur "github.com/marcgauthier/murmur-sql/clients/go"
	"github.com/marcgauthier/murmur-sql/clients/go/examples/internal/run"
	"github.com/marcgauthier/murmur-sql/clients/go/internal/servertest"
)

func main() {
	run.Main(func(t servertest.TB, dir string) {
		ctx := context.Background()
		server := servertest.Start(t, dir, servertest.Options{})
		db, err := murmur.Open(ctx, murmur.Config{Host: "127.0.0.1", Port: server.Port, User: "admin", Password: "test-password", Key: "example-key"})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, "CREATE TABLE documents(id INTEGER PRIMARY KEY,embedding BLOB)"); err != nil {
			t.Fatalf("create: %v", err)
		}
		for i, vector := range [][]float32{{1, 0}, {0, 1}} {
			bytes := make([]byte, 4*len(vector))
			for j, v := range vector {
				binary.LittleEndian.PutUint32(bytes[4*j:], math.Float32bits(v))
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO documents VALUES($1,$2)", i+1, bytes); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		if _, err := db.ExecContext(ctx, "MURMUR SET COLUMN 'documents' 'embedding' VECTOR true DIMENSIONS 2 DISTANCE l2"); err != nil {
			t.Fatalf("enable: %v", err)
		}
		if _, err := db.ExecContext(ctx, "MURMUR REFRESH INDEXES"); err != nil {
			t.Fatalf("refresh: %v", err)
		}
		rows, err := db.QueryContext(ctx, "MURMUR SHOW COLUMNS VECTOR")
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		var name string
		for rows.Next() {
			var table, column string
			var enabled bool
			var indexName, indexError *string
			var ready, rebuilding *bool
			var pending, queued *int64
			var dimensions *int64
			var distance *string
			if err := rows.Scan(&table, &column, &enabled, &dimensions, &distance, &indexName, &ready, &rebuilding, &pending, &queued, &indexError); err != nil {
				t.Fatalf("scan: %v", err)
			}
			if enabled && indexName != nil && ready != nil && *ready {
				name = *indexName
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
		rows.Close()
		if name == "" {
			t.Fatalf("no ready vector index")
		}
		ident := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		query := "SELECT rowid,distance FROM temp." + ident + " WHERE embedding MATCH $1 AND k=$2 ORDER BY distance"
		rows, err = db.QueryContext(ctx, query, "[1,0]", 2)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		count := 0
		for rows.Next() {
			var id int64
			var distance float64
			if err := rows.Scan(&id, &distance); err != nil {
				t.Fatalf("scan: %v", err)
			}
			want := 0.
			if count == 1 {
				want = math.Sqrt(2)
			}
			if id != int64(count+1) || math.Abs(distance-want) > 1e-5 {
				t.Fatalf("neighbor: %d %f", id, distance)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
		rows.Close()
		if count != 2 {
			t.Fatalf("expected 2 neighbors, got %d", count)
		}
		if _, err := db.ExecContext(ctx, "MURMUR SET COLUMN 'documents' 'embedding' VECTOR false"); err != nil {
			t.Fatalf("disable: %v", err)
		}
		rows, err = db.QueryContext(ctx, "MURMUR SHOW COLUMNS VECTOR")
		if err != nil {
			t.Fatalf("disabled status: %v", err)
		}
		for rows.Next() {
			var table, column string
			var enabled bool
			var indexName, indexError *string
			var ready, rebuilding *bool
			var pending, queued *int64
			var dimensions *int64
			var distance *string
			if err := rows.Scan(&table, &column, &enabled, &dimensions, &distance, &indexName, &ready, &rebuilding, &pending, &queued, &indexError); err != nil {
				t.Fatalf("scan: %v", err)
			}
			if enabled || indexName != nil {
				t.Fatalf("column still enabled")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
		rows.Close()
		if _, err := db.ExecContext(ctx, "MURMUR FLUSH"); err != nil {
			t.Fatalf("flush: %v", err)
		}
		fmt.Println("vectors: verified declarations, nearest neighbors, distances and disable")
	})
}
