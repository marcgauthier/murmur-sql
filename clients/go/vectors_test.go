package murmur_test

import (
	"context"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/clients/go/internal/servertest"
)

func vectorBytes(values ...float32) []byte {
	bytes := make([]byte, 4*len(values))
	for i, v := range values {
		binary.LittleEndian.PutUint32(bytes[i*4:], math.Float32bits(v))
	}
	return bytes
}
func TestColumnVectors(t *testing.T) {
	server := servertest.Start(t, t.TempDir(), servertest.Options{})
	db := open(t, testConfig(server))
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE "doc list"(id INTEGER PRIMARY KEY,title TEXT,embedding BLOB)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR SET COLUMN 'doc list' 'embedding' VECTOR true DIMENSIONS 2"); err != nil {
		t.Fatal(err)
	}
	columns, err := sqlRows(ctx, db, "MURMUR SHOW COLUMNS VECTOR")
	if err != nil {
		t.Fatal(err)
	}
	var name string
	for _, c := range columns {
		if c["enabled"] == "true" {
			if c["index_name"] == nil || c["dimensions"] == nil || c["dimensions"].(int64) != 2 || c["distance"] == nil || c["distance"].(string) != "l2" || c["ready"] == nil || c["ready"] != "true" {
				t.Fatalf("bad status: %+v", c)
			}
			name = c["index_name"].(string)
		} else if c["index_name"] != nil || c["dimensions"] != nil || c["ready"] != nil {
			t.Fatalf("disabled status: %+v", c)
		}
	}
	if name == "" {
		t.Fatal("missing index")
	}
	ident := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	query := `SELECT d.id, v.distance AS score FROM temp.` + ident + ` v JOIN "doc list" d ON d.id=v.rowid WHERE v.embedding MATCH $1 AND v.k=$2 ORDER BY v.distance`
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	// Prepare against an empty index: result types must remain numeric later.
	rows, err := stmt.QueryContext(ctx, "[1,0]", 2)
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if len(types) != 2 || types[1].DatabaseTypeName() != "FLOAT8" {
		t.Fatalf("distance type: %+v", types)
	}
	if rows.Next() {
		t.Fatal("expected empty index")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	for id, v := range [][]byte{vectorBytes(1, 0), vectorBytes(0, 1)} {
		if _, err := db.ExecContext(ctx, `INSERT INTO "doc list" VALUES($1,$2,$3)`, id+1, "hello", v); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, "MURMUR SET COLUMN 'doc list' 'title' FTS true"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR REFRESH INDEXES"); err != nil {
		t.Fatal(err)
	}
	var id int64
	var distance float64
	if err := stmt.QueryRowContext(ctx, "[1,0]", 1).Scan(&id, &distance); err != nil || id != 1 || distance != 0 {
		t.Fatalf("match: %d %f %v", id, distance, err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR SET COLUMN 'doc list' 'embedding' VECTOR true DIMENSIONS 3"); err == nil {
		t.Fatal("expected invalid populated reconfiguration")
	}
	if err := stmt.QueryRowContext(ctx, "[1,0]", 1).Scan(&id, &distance); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	for metric, want := range map[string]float64{"l2": math.Sqrt(2), "l1": 2, "cosine": 1} {
		if _, err := db.ExecContext(ctx, "MURMUR SET COLUMN 'doc list' 'embedding' VECTOR true DIMENSIONS 2 DISTANCE "+metric); err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(ctx, query, "[1,0]", 2)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			if err := rows.Scan(&id, &distance); err != nil {
				t.Fatal(err)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if count != 2 || id != 2 || math.Abs(distance-want) > 1e-5 {
			t.Fatalf("metric %s: count=%d id=%d distance=%f", metric, count, id, distance)
		}
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM temp."+ident); err == nil {
		t.Fatal("index writable")
	}
	if _, err := db.ExecContext(ctx, `UPDATE "doc list" SET embedding=$1 WHERE id=1`, []byte{0}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR REFRESH INDEXES"); err == nil {
		t.Fatal("invalid embedding should fail refresh")
	}
	columns, err = sqlRows(ctx, db, "MURMUR SHOW COLUMNS VECTOR")
	if err != nil {
		t.Fatal(err)
	}
	foundError := false
	for _, c := range columns {
		if c["enabled"] == "true" && c["error"] != nil && c["ready"] != nil && c["ready"] != "true" {
			foundError = true
		}
	}
	if !foundError {
		t.Fatal("missing maintenance error")
	}
	if _, err := db.ExecContext(ctx, `UPDATE "doc list" SET embedding=$1 WHERE id=1`, vectorBytes(1, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR REFRESH INDEXES"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "MURMUR SET COLUMN 'doc list' 'embedding' VECTOR false"); err != nil {
		t.Fatal(err)
	}
	columns, err = sqlRows(ctx, db, "MURMUR SHOW COLUMNS VECTOR")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range columns {
		if c["enabled"] == "true" || c["index_name"] != nil {
			t.Fatalf("disable: %+v", c)
		}
	}
	for _, sql := range []string{
		`MURMUR SET COLUMN 'doc list' embedding VECTOR true DIMENSIONS 0`,
		`MURMUR SET COLUMN 'doc list' embedding VECTOR true DIMENSIONS 8193`,
		`MURMUR SET COLUMN 'doc list' title VECTOR true DIMENSIONS 2`,
		`MURMUR SET COLUMN 'doc list' embedding VECTOR true DIMENSIONS 2 DISTANCE unknown`,
	} {
		if _, err := db.ExecContext(ctx, sql); codeOf(err) != "22023" {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for _, sql := range []string{`MURMUR SET COLUMN 'doc list' embedding VECTOR true`, `MURMUR SET COLUMN 'doc list' embedding VECTOR false DIMENSIONS 2`} {
		if _, err := db.ExecContext(ctx, sql); codeOf(err) != "42601" {
			t.Fatalf("%s: %v", sql, err)
		}
	}
}
