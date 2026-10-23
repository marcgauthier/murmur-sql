package native

import (
	"context"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/config"
	"github.com/marcgauthier/murmur-sql-go/engine"
)

// TestDescribeDDL pins that describing DDL succeeds without result columns.
// Preparing DDL would run the SQLite authorizer, which only admits DDL
// under migration access; extended-protocol clients describe before
// executing, so describe must succeed and let execute route to Migrate.
func TestDescribeDDL(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir + "/data"
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	ctx := context.Background()
	db, _, err := Factory{}.Open(ctx, engine.OpenRequest{Config: cfg, Key: key})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close(ctx)
	for _, sql := range []string{
		"CREATE TABLE t(id INTEGER PRIMARY KEY)",
		"ALTER TABLE t ADD COLUMN v TEXT",
		"DROP TABLE t",
	} {
		d, err := db.Describe(ctx, sql, nil)
		if err != nil {
			t.Fatalf("describe %q: %v", sql, err)
		}
		if len(d.Parameters) != 0 || len(d.Columns) != 0 {
			t.Fatalf("describe %q = %+v, want no parameters or columns", sql, d)
		}
	}
	if _, err := db.Migrate(ctx, "CREATE TABLE t(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	d, err := db.Describe(ctx, "SELECT id FROM t WHERE id = ?1", []uint32{0})
	if err != nil {
		t.Fatalf("describe select: %v", err)
	}
	if len(d.Parameters) != 1 || len(d.Columns) != 1 {
		t.Fatalf("describe select = %+v, want one parameter and column", d)
	}
}
