package murmur_test

import (
	"context"
	"strings"
	"testing"

	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/internal/servertest"
)

func TestColumnFTS(t *testing.T) {
	server := servertest.Start(t, t.TempDir(), servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE "doc list"(id INTEGER PRIMARY KEY, "full text" TEXT, body TEXT, n INTEGER)`,
		`INSERT INTO "doc list" VALUES(1,'hello','world',2)`,
	} {
		if _, err := db.ExecContext(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.SetColumnFTS(ctx, "doc list", "full text", true); err != nil {
		t.Fatal(err)
	}
	columns, err := db.ColumnFTS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var index string
	for _, c := range columns {
		if c.Column == "full text" {
			if !c.Enabled || c.IndexName == nil || c.Ready == nil || !*c.Ready {
				t.Fatalf("bad status: %+v", c)
			}
			index = *c.IndexName
		} else if c.Enabled || c.IndexName != nil || c.Ready != nil || c.PendingKeys != nil {
			t.Fatalf("disabled status: %+v", c)
		}
	}
	if index == "" {
		t.Fatal("missing index")
	}
	ident := `"` + strings.ReplaceAll(index, `"`, `""`) + `"`
	query := `SELECT d.id FROM "doc list" d JOIN temp.` + ident + ` f ON f.rowid=d.id WHERE ` + ident + ` MATCH $1`
	statement, err := db.PrepareContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	var id int64
	if err := statement.QueryRowContext(ctx, "hello").Scan(&id); err != nil || id != 1 {
		t.Fatalf("match: %d %v", id, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE "doc list" SET "full text"='changed'`); err != nil {
		t.Fatal(err)
	}
	if err := db.RefreshIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	if err := statement.QueryRowContext(ctx, "changed").Scan(&id); err != nil || id != 1 {
		t.Fatalf("updated match: %d %v", id, err)
	}
	for _, column := range []string{"missing", "n"} {
		if err := db.SetColumnFTS(ctx, "doc list", column, true); murmur.CodeOf(err) != "22023" {
			t.Fatalf("invalid %s: %v", column, err)
		}
	}
	if _, err := db.ExecContext(ctx, "MURMUR SET COLUMN"); murmur.CodeOf(err) != "42601" {
		t.Fatalf("syntax: %v", err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM temp."+ident); err == nil {
		t.Fatal("index writable")
	}
	if err := db.SetColumnFTS(ctx, "doc list", "full text", false); err != nil {
		t.Fatal(err)
	}
	columns, err = db.ColumnFTS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range columns {
		if c.Enabled || c.IndexName != nil || c.Ready != nil {
			t.Fatalf("disable: %+v", c)
		}
	}
}
