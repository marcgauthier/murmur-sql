package sqlite

import (
	"encoding/binary"
	"math"
	"testing"
)

func vecBytes(values ...float32) []byte {
	out := make([]byte, 4*len(values))
	for i, v := range values {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(v))
	}
	return out
}

func TestVec0KNN(t *testing.T) {
	c := openTest(t)
	if err := c.ExecBatch(`CREATE VIRTUAL TABLE temp.v USING vec0(embedding float[2] distance_metric=l2)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.Exec(`INSERT INTO temp.v(rowid, embedding) VALUES(?,?)`, 1, vecBytes(1, 0)); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if err := c.Exec(`INSERT INTO temp.v(rowid, embedding) VALUES(?,?)`, 2, vecBytes(0, 1)); err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	if err := c.Exec(`INSERT INTO temp.v(rowid, embedding) VALUES(?,?)`, 3, []byte{0}); err == nil {
		t.Fatal("short embedding accepted")
	}
	rows, err := c.Query(`SELECT rowid, distance FROM temp.v WHERE embedding MATCH ? AND k=? ORDER BY distance`, "[1,0]", 2)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 || rows[0][0].Int != 1 || rows[0][1].Float != 0 || rows[1][0].Int != 2 {
		t.Fatalf("knn = %v", rows)
	}
	if math.Abs(rows[1][1].Float-math.Sqrt(2)) > 1e-9 {
		t.Fatalf("l2 distance = %v", rows[1][1].Float)
	}
	if rows[0][1].Type != Float {
		t.Fatalf("distance type = %v, want Float", rows[0][1].Type)
	}
	rows, err = c.Query(`SELECT rowid FROM temp.v WHERE embedding MATCH ? AND k=?`, "[1,0]", 1)
	if err != nil || len(rows) != 1 || rows[0][0].Int != 1 {
		t.Fatalf("top-1 = %v, %v", rows, err)
	}
	if err := c.Exec(`DELETE FROM temp.v WHERE rowid=?`, 1); err != nil {
		t.Fatalf("delete: %v", err)
	}
	rows, err = c.Query(`SELECT rowid FROM temp.v WHERE embedding MATCH '[1,0]' AND k=5`)
	if err != nil || len(rows) != 1 || rows[0][0].Int != 2 {
		t.Fatalf("after delete = %v, %v", rows, err)
	}
	if err := c.ExecBatch(`DROP TABLE temp.v`); err != nil {
		t.Fatalf("drop: %v", err)
	}
	left, err := c.Query(`SELECT name FROM temp.sqlite_master WHERE name LIKE '%v%'`)
	if err != nil {
		t.Fatalf("master: %v", err)
	}
	if len(left) != 0 {
		t.Fatalf("shadow leftovers: %v", left)
	}
}

func TestVec0Metrics(t *testing.T) {
	for metric, want := range map[string]float64{"l2": math.Sqrt(2), "l1": 2, "cosine": 1} {
		c := openTest(t)
		if err := c.ExecBatch(`CREATE VIRTUAL TABLE temp.v USING vec0(embedding float[2] distance_metric=` + metric + `)`); err != nil {
			t.Fatalf("%s create: %v", metric, err)
		}
		if err := c.Exec(`INSERT INTO temp.v(rowid, embedding) VALUES(?,?)`, 1, vecBytes(1, 0)); err != nil {
			t.Fatal(err)
		}
		if err := c.Exec(`INSERT INTO temp.v(rowid, embedding) VALUES(?,?)`, 2, vecBytes(0, 1)); err != nil {
			t.Fatal(err)
		}
		rows, err := c.Query(`SELECT distance FROM temp.v WHERE embedding MATCH '[1,0]' AND k=2 ORDER BY distance`)
		if err != nil || len(rows) != 2 {
			t.Fatalf("%s query = %v, %v", metric, rows, err)
		}
		if math.Abs(rows[1][0].Float-want) > 1e-9 {
			t.Fatalf("%s distance = %v, want %v", metric, rows[1][0].Float, want)
		}
	}
}

func TestVec0BlobQuery(t *testing.T) {
	c := openTest(t)
	if err := c.ExecBatch(`CREATE VIRTUAL TABLE temp.v USING vec0(embedding float[2])`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.Exec(`INSERT INTO temp.v(rowid, embedding) VALUES(?,?)`, 7, vecBytes(1, 0)); err != nil {
		t.Fatal(err)
	}
	rows, err := c.Query(`SELECT rowid FROM temp.v WHERE embedding MATCH ? AND k=1`, vecBytes(1, 0))
	if err != nil || len(rows) != 1 || rows[0][0].Int != 7 {
		t.Fatalf("blob query = %v, %v", rows, err)
	}
}
