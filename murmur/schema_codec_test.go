package murmur

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
)

func TestSchemaNestedRoundtrip(t *testing.T) {
	var history [32]byte
	for i := range history {
		history[i] = 3
	}
	legacy := Schema{
		Tables: []Table{{
			ID:   "t",
			Name: "docs",
			SQL:  "CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT)",
			Columns: []Column{{
				Policy: PolicyLww, ID: "c", Name: "body",
				DeclaredType: "TEXT",
			}},
		}},
		History: history,
	}
	pack := func(s Schema) []byte {
		e := postcard.NewEncoder(64)
		e.U32(12)
		encodeSchema(e, s)
		e.U64(99)
		return append([]byte(nil), e.Bytes()...)
	}
	unpack := func(b []byte) (uint32, Schema, uint64) {
		d := postcard.NewDecoder(b)
		head := d.U32()
		s := decodeSchema(d)
		tail := d.U64()
		if err := d.End(); err != nil {
			t.Fatalf("trailing bytes: %v", err)
		}
		return head, s, tail
	}
	raw := pack(legacy)
	_, schema, tail := unpack(raw)
	if tail != 99 {
		t.Fatalf("tail = %d", tail)
	}
	if schema.Tables[0].Columns[0].Fts || schema.Tables[0].Columns[0].Vector != nil {
		t.Fatal("legacy schema gained extensions")
	}
	if again := pack(schema); !bytes.Equal(again, raw) {
		t.Fatal("legacy roundtrip changed bytes")
	}
	schema.Tables[0].Columns[0].Vector = &VectorColumnConfig{Dimensions: 3, Distance: DistCosine}
	schema.Tables[0].Columns[0].Fts = true
	encoded := pack(schema)
	_, decoded, tail := unpack(encoded)
	if tail != 99 {
		t.Fatalf("tail = %d", tail)
	}
	if !SchemasEqual(&decoded, &schema) {
		t.Fatalf("extended roundtrip mismatch: %+v", decoded)
	}
	if len(decoded.Indexes) != 0 {
		t.Fatalf("extensions leaked into indexes: %q", decoded.Indexes)
	}
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	var back Schema
	if err := json.Unmarshal(js, &back); err != nil {
		t.Fatal(err)
	}
	if !SchemasEqual(&back, &schema) {
		t.Fatalf("JSON roundtrip mismatch: %s", js)
	}
	// Unknown extensions are rejected, plain indexes preserved.
	bad := legacy
	bad.Indexes = []string{"\x00murmur/unknown/v1/x"}
	d := postcard.NewDecoder(EncodeSchema(bad))
	_ = decodeSchema(d)
	if d.Err() == nil {
		t.Fatal("unknown extension accepted")
	}
}
