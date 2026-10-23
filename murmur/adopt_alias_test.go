package murmur

import (
	"errors"
	"testing"
)

// TestAdoptSchemaFailureLeavesLiveSchemaUntouched pins that adoptSchema
// merges into a private copy. The merge loop once ran on a shallow copy
// aliasing the live schema, so a flag-only change flipped c.schema before
// the equality check: adoption short-circuited to (true, nil), skipping
// the index build and the storage event while still recording acceptance
// (phantom adoption with memory/storage divergence).
func TestAdoptSchemaFailureLeavesLiveSchemaUntouched(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(
		`CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Opt-in assigns the application key adoption requires.
	if err := db.SetReplication("docs", true); err != nil {
		t.Fatalf("opt-in: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Incoming schema: FTS flag flipped on body (merges first), plus a
	// second table reusing the docs name (rejected after the flag merge).
	incoming, err := db.Schema()
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	for i := range incoming.Tables {
		if incoming.Tables[i].Name != "docs" {
			continue
		}
		for j := range incoming.Tables[i].Columns {
			if incoming.Tables[i].Columns[j].Name == "body" {
				incoming.Tables[i].Columns[j].Fts = true
			}
		}
		dup := CloneTable(&incoming.Tables[i])
		dup.ID = "duplicate-table-id"
		incoming.Tables = append(incoming.Tables, dup)
	}
	v, err := db.Call(func(c *Core) (any, error) { return c.adoptSchema(*incoming) })
	if err == nil || !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("adopt = (%v, %v), want (false, ErrSchemaMismatch)", v, err)
	}
	schema, err := db.Schema()
	if err != nil {
		t.Fatalf("schema after failed adopt: %v", err)
	}
	if len(schema.Tables) != 1 {
		t.Fatalf("failed adoption changed the live table set: %d tables", len(schema.Tables))
	}
	for _, table := range schema.Tables {
		if table.Name != "docs" {
			continue
		}
		for _, col := range table.Columns {
			if col.Name == "body" && col.Fts {
				t.Fatal("failed adoption flipped the live Fts flag through the shared backing")
			}
		}
	}
	_, statuses, err := db.SchemaWithSearchStatus()
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(statuses) != 0 {
		t.Fatalf("failed adoption built indexes: %+v", statuses)
	}
}
