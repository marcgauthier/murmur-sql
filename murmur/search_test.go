package murmur

import (
	"testing"
)

// TestSetColumnSearchBuildsIndexStatus pins that enabling column search
// actually builds the index. setColumnSearch once mutated the live schema
// through a shallow copy, so the no-change short-circuit fired and SHOW
// COLUMNS found an enabled column with no index status.
func TestSetColumnSearchBuildsIndexStatus(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir+"/db")
	defer db.Close()
	if err := db.Migrate(NewMigration(
		`CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT, embedding BLOB)`)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := db.SetColumnFts("docs", "body", true); err != nil {
		t.Fatalf("set fts: %v", err)
	}
	if err := db.SetColumnVector("docs", "embedding",
		&VectorColumnConfig{Dimensions: 2, Distance: DistL2}); err != nil {
		t.Fatalf("set vector: %v", err)
	}
	schema, statuses, err := db.SchemaWithSearchStatus()
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	byName := map[string]SearchIndexStatus{}
	for _, s := range statuses {
		byName[s.Name] = s
	}
	for _, table := range schema.Tables {
		if table.Name != "docs" {
			continue
		}
		var ftsCol, vecCol *Column
		for i := range table.Columns {
			switch table.Columns[i].Name {
			case "body":
				ftsCol = &table.Columns[i]
			case "embedding":
				vecCol = &table.Columns[i]
			}
		}
		if ftsCol == nil || !ftsCol.Fts {
			t.Fatalf("fts flag not persisted: %+v", table.Columns)
		}
		if vecCol == nil || vecCol.Vector == nil {
			t.Fatalf("vector config not persisted: %+v", table.Columns)
		}
		for _, name := range []string{SchemaIndexName(&table), VectorIndexName(&table, vecCol)} {
			s, ok := byName[name]
			if !ok {
				t.Fatalf("index %q has no status", name)
			}
			if !s.Ready {
				t.Fatalf("index %q not ready: %+v", name, s)
			}
		}
	}
}
