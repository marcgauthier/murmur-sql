package server

import (
	"github.com/marcgauthier/murmur-sql/engine"
	"reflect"
	"testing"
)

func TestSQLLexerBoundaries(t *testing.T) {
	sql := "-- $99;\nSELECT '$1;''x', \"$2;\", `$3;`, [$4;], $1 /* $8; */; SELECT $2"
	parts, e := SplitStatements(sql)
	if e != nil || len(parts) != 2 {
		t.Fatal(parts, e)
	}
	got, n, e := RewriteParameters(parts[0])
	want := "-- $99;\nSELECT '$1;''x', \"$2;\", `$3;`, [$4;], ?1 /* $8; */"
	if e != nil || n != 1 || got != want {
		t.Fatal(got, n, e)
	}
	for _, s := range []string{"SELECT $0", "SELECT $65536", "SELECT 'unterminated", "SELECT /* unterminated"} {
		if _, _, e := RewriteParameters(s); e == nil {
			t.Fatal(s)
		}
	}
}
func TestAdministrationGrammar(t *testing.T) {
	for _, sql := range []string{"MURMUR FLUSH", "MURMUR SHOW PEERS", "MURMUR SHOW COLUMNS VECTOR", "MURMUR SET TABLE t REPLICATED yes", "MURMUR SET COLUMN t c FTS on", "MURMUR SET COLUMN t c VECTOR true DIMENSIONS 3 DISTANCE cosine", "MURMUR SET COLUMN t c VECTOR false", "MURMUR ROTATE KEY 'old', 'new'", "MURMUR PEER RETIRE '00112233445566778899aabbccddeeff'", "MURMUR POLICY COUNTER_ADD t c 'key' -3", "MURMUR BRIDGE RELEASE t 'key' COLUMN c", "MURMUR FILE UPLOAD APPEND $1 $2", "MURMUR FILE DOWNLOAD NEXT 'handle' LIMIT 16", "MURMUR BACKUP DOWNLOAD 'backup' LIMIT 7 OFFSET 0"} {
		if _, e := parseCommand(sql); e != nil {
			t.Fatal(sql, e)
		}
	}
	for _, sql := range []string{"MURMUR UNKNOWN", "MURMUR FLUSH extra", "MURMUR SET TABLE t REPLICATED", "MURMUR SET COLUMN t c VECTOR true DIMENSIONS 0", "MURMUR SET COLUMN t c VECTOR true DIMENSIONS 3 DISTANCE bad", "MURMUR FILE UPLOAD APPEND $1 $1", "MURMUR FILE SEARCH $2", "MURMUR BACKUP DOWNLOAD 'x' LIMIT 1 LIMIT 2"} {
		if _, e := parseCommand(sql); e == nil {
			t.Fatal(sql)
		}
	}
	spec, e := parseCommand("MURMUR FILE UPLOAD APPEND $1 $2")
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(spec.parameters, []uint32{engine.Text, engine.Bytea}) {
		t.Fatal(spec.parameters)
	}
	values, e := spec.values([]engine.Value{"handle", []byte{0, 1}})
	if e != nil || !reflect.DeepEqual(values, []engine.Value{"handle", []byte{0, 1}}) {
		t.Fatal(values, e)
	}
}
func TestBinaryValuesAndNull(t *testing.T) {
	for _, v := range []struct {
		value any
		oid   uint32
	}{{int64(-32768), engine.Int2}, {int64(-2147483648), engine.Int4}, {int64(-9223372036854775808), engine.Int8}, {float64(1.25), engine.Float4}, {float64(1.25), engine.Float8}, {true, engine.Bool}, {"héllo", engine.Text}, {[]byte{0, 255}, engine.Bytea}, {[]byte{}, engine.Bytea}} {
		raw, e := encodeValue(v.value, v.oid, 1)
		if e != nil {
			t.Fatal(e)
		}
		got, e := decodeValue(raw, v.oid, 1)
		if e != nil || !reflect.DeepEqual(got, v.value) {
			t.Fatalf("%v: %v %v", v, got, e)
		}
	}
	if v, e := decodeValue(nil, engine.Int8, 1); e != nil || v != nil {
		t.Fatal(v, e)
	}
	if _, e := encodeValue(int64(32768), engine.Int2, 1); e == nil {
		t.Fatal("integer overflow accepted")
	}
	if _, e := decodeValue([]byte{1}, engine.Int8, 1); e == nil {
		t.Fatal("bad binary size accepted")
	}
}
func FuzzSQLLexer(f *testing.F) {
	for _, s := range []string{"SELECT $1; SELECT '$2'", "/*comment*/ SELECT 1", "SELECT [x;y]", "MURMUR UNLOCK KEY 'secret'"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) { SplitStatements(s); RewriteParameters(s); tokenize(s); parseCommand(s) })
}

func TestPostgresTypedParameterCompatibility(t *testing.T) {
	for _, tc := range []struct {
		raw  []byte
		oid  uint32
		want any
	}{{[]byte{0, 0, 0, 0}, engine.Date, "2000-01-01"}, {[]byte{255, 255, 255, 255, 255, 255, 255, 255}, engine.Timestamp, "1999-12-31 23:59:59.999999"}, {[]byte{0, 0, 0, 0, 0, 0, 0, 0}, engine.TimestampTZ, "2000-01-01 00:00:00.000000"}, {make([]byte, 16), engine.UUID, "00000000-0000-0000-0000-000000000000"}} {
		got, e := decodeValue(tc.raw, tc.oid, 1)
		if e != nil || got != tc.want {
			t.Fatal(got, e, tc.want)
		}
	}
	if _, e := decodeValue([]byte{0}, engine.Numeric, 1); engine.SQLState(e) != "42846" {
		t.Fatal(e)
	}
	for _, s := range []string{"t", "f"} {
		if _, e := decodeValue([]byte(s), engine.Bool, 0); e != nil {
			t.Fatal(e)
		}
	}
	if _, ok := builtin("BEGIN INVALID"); ok {
		t.Fatal("invalid transaction command accepted")
	}
	parts, e := SplitStatements("'not a valid SQL statement'")
	if e != nil || len(parts) != 1 {
		t.Fatal(parts, e)
	}
}
