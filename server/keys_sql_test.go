package server

import (
	"testing"

	"github.com/marcgauthier/murmur-sql/engine"
)

func TestKeysCommandsGrammar(t *testing.T) {
	for sql, op := range map[string]string{
		"MURMUR SHOW KEYS":       "SHOW KEYS",
		"murmur show keys":       "SHOW KEYS",
		"MURMUR ROTATE DATA KEY": "ROTATE DATA KEY",
		"MURMUR PRUNE KEYS":      "PRUNE KEYS",
	} {
		spec, e := parseCommand(sql)
		if e != nil {
			t.Fatal(sql, e)
		}
		if spec.operation != op {
			t.Fatalf("%s parsed as %q, want %q", sql, spec.operation, op)
		}
		if len(spec.args) != 0 {
			t.Fatalf("%s took %d args, want none", sql, len(spec.args))
		}
	}
	for _, sql := range []string{
		"MURMUR SHOW KEYS extra", "MURMUR ROTATE DATA", "MURMUR ROTATE DATA KEY extra",
		"MURMUR PRUNE", "MURMUR PRUNE KEYS extra", "MURMUR ROTATE KEYS",
	} {
		if _, e := parseCommand(sql); e == nil {
			t.Fatal(sql)
		}
	}
	if _, e := parseCommand("MURMUR ROTATE KEY 'old' 'new'"); e != nil {
		t.Fatalf("master rotation broke: %v", e)
	}
	columns := cols(commandColumns["SHOW KEYS"])
	if len(columns) != 6 || columns[0].OID != engine.Int8 || columns[5].OID != engine.Bool {
		t.Fatalf("SHOW KEYS columns = %+v", columns)
	}
	columns = cols(commandColumns["PRUNE KEYS"])
	if len(columns) != 1 || columns[0].OID != engine.Int8 {
		t.Fatalf("PRUNE KEYS columns = %+v", columns)
	}
}
