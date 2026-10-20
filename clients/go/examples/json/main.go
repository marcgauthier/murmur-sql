// Example json verifies SQLite JSON queries, updates, arrays and JSONB over SQL.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

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

		for _, query := range []string{
			"CREATE TABLE json_documents(id INTEGER PRIMARY KEY, data TEXT NOT NULL)",
			`INSERT INTO json_documents(id,data) VALUES(1,'{"name":"Alice","age":30,"tags":["sqlite","search"]}')`,
		} {
			if _, err := db.ExecContext(ctx, query); err != nil {
				t.Fatalf("setup: %v", err)
			}
		}
		var name string
		var age int64
		if err := db.QueryRowContext(ctx, `SELECT data ->> '$.name', json_extract(data,'$.age') FROM json_documents WHERE id=1`).Scan(&name, &age); err != nil || name != "Alice" || age != 30 {
			t.Fatalf("extract: name=%q age=%d err=%v", name, age, err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE json_documents SET data=json_set(data,'$.age',31) WHERE id=1`); err != nil {
			t.Fatalf("update: %v", err)
		}
		var valid int64
		if err := db.QueryRowContext(ctx, `SELECT json_valid(data),json_extract(data,'$.age') FROM json_documents WHERE id=1`).Scan(&valid, &age); err != nil || valid != 1 || age != 31 {
			t.Fatalf("updated JSON: valid=%d age=%d err=%v", valid, age, err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE json_documents SET data=json_set(data,'$.tags',json('["sqlite","json"]')) WHERE id=1`); err != nil {
			t.Fatalf("replace array: %v", err)
		}
		rows, err := db.QueryContext(ctx, `SELECT CAST(t.value AS TEXT) FROM json_documents AS d,json_each(d.data,'$.tags') AS t WHERE d.id=1 ORDER BY t.key`)
		if err != nil {
			t.Fatalf("array query: %v", err)
		}
		var tags []string
		for rows.Next() {
			var tag string
			if err := rows.Scan(&tag); err != nil {
				t.Fatalf("array scan: %v", err)
			}
			tags = append(tags, tag)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("array rows: %v", err)
		}
		rows.Close()
		if !reflect.DeepEqual(tags, []string{"sqlite", "json"}) {
			t.Fatalf("tags: %v", tags)
		}

		var object string
		if err := db.QueryRowContext(ctx, `SELECT json_object('name','Alice','active',json('true'))`).Scan(&object); err != nil || object != `{"name":"Alice","active":true}` {
			t.Fatalf("JSON object: %q err=%v", object, err)
		}
		var nodes int64
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM json_documents AS d,json_tree(d.data) WHERE d.id=1`).Scan(&nodes); err != nil || nodes != 6 {
			t.Fatalf("JSON tree: nodes=%d err=%v", nodes, err)
		}

		// TEXT parameters carry JSON strings. Validate before writing: declaring
		// TEXT does not enforce JSON, and CHECK constraints are unsupported.
		payload := `{"name":"Bob","age":25,"tags":[]}`
		if !json.Valid([]byte(payload)) {
			t.Fatalf("invalid application JSON")
		}
		if _, err := db.ExecContext(ctx, "INSERT INTO json_documents(id,data) VALUES($1,$2)", 2, payload); err != nil {
			t.Fatalf("bound insert: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT CAST(data ->> '$.name' AS TEXT) FROM json_documents WHERE id=$1`, 2).Scan(&name); err != nil || name != "Bob" {
			t.Fatalf("bound JSON: name=%q err=%v", name, err)
		}
		if err := db.QueryRowContext(ctx, `SELECT json_valid('not JSON')`).Scan(&valid); err != nil || valid != 0 {
			t.Fatalf("invalid JSON check: %d %v", valid, err)
		}

		for _, query := range []string{
			"CREATE TABLE json_binary(id INTEGER PRIMARY KEY, data BLOB NOT NULL)",
			"INSERT INTO json_binary(id,data) SELECT id,jsonb(data) FROM json_documents",
		} {
			if _, err := db.ExecContext(ctx, query); err != nil {
				t.Fatalf("JSONB setup: %v", err)
			}
		}
		var storage, text string
		if err := db.QueryRowContext(ctx, `SELECT typeof(data),json_valid(data,8),json(data) FROM json_binary WHERE id=1`).Scan(&storage, &valid, &text); err != nil || storage != "blob" || valid != 1 || !json.Valid([]byte(text)) {
			t.Fatalf("JSONB: storage=%q valid=%d text=%q err=%v", storage, valid, text, err)
		}
		var decoded struct {
			Name string
			Age  int
			Tags []string
		}
		if err := json.Unmarshal([]byte(text), &decoded); err != nil || decoded.Name != "Alice" || decoded.Age != 31 || !reflect.DeepEqual(decoded.Tags, []string{"sqlite", "json"}) {
			t.Fatalf("JSONB round trip: %+v err=%v", decoded, err)
		}
		// Confirm the documented migration restrictions without changing code.
		if _, err := db.ExecContext(ctx, "CREATE INDEX json_name ON json_documents(json_extract(data,'$.name'))"); err == nil {
			t.Fatalf("expected JSON expression index to be rejected by managed migrations")
		}
		if _, err := db.ExecContext(ctx, "MURMUR FLUSH"); err != nil {
			t.Fatalf("flush: %v", err)
		}
		fmt.Println("json: verified extraction, updates, arrays, parameters, validation and JSONB")
	})
}
