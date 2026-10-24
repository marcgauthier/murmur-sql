package server

import (
	"reflect"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql/config"
	"github.com/marcgauthier/murmur-sql/engine"
)

// TestPolicyCompositeGrammar pins the composite-key POLICY form:
// MURMUR POLICY <op> <table> <column> KEY <k1> [<k2>...] VALUE <value>.
// The legacy single-key form keeps parsing exactly as before.
func TestPolicyCompositeGrammar(t *testing.T) {
	legacy, e := parseCommand("MURMUR POLICY COUNTER_ADD 't' 'c' 'k' 1")
	if e != nil {
		t.Fatalf("legacy form broke: %v", e)
	}
	if legacy.operation != "POLICY COUNTER_ADD" || len(legacy.args) != 4 {
		t.Fatalf("legacy = %q %d args", legacy.operation, len(legacy.args))
	}
	// A bare KEY in the legacy key slot still parses as the key.
	bare, e := parseCommand("MURMUR POLICY MIN t c KEY 5")
	if e != nil {
		t.Fatalf("bare KEY legacy form broke: %v", e)
	}
	if bare.args[2].value != "KEY" {
		t.Fatalf("legacy key = %q, want KEY", bare.args[2].value)
	}
	for sql, wantArgs := range map[string]int{
		"MURMUR POLICY COUNTER_ADD 't' 'c' KEY 'k1' 'k2' VALUE 5": 5,
		"murmur policy max t c key 1 'a' value 9":                 5,
		"MURMUR POLICY MIN 't' 'c' KEY 'k1' VALUE 7":              4,
	} {
		spec, e := parseCommand(sql)
		if e != nil {
			t.Fatal(sql, e)
		}
		if len(spec.args) != wantArgs {
			t.Fatalf("%s took %d args, want %d", sql, len(spec.args), wantArgs)
		}
	}
	spec, e := parseCommand("MURMUR POLICY COUNTER_ADD 't' 'c' KEY 'k1' 'k2' VALUE 5")
	if e != nil {
		t.Fatal(e)
	}
	if spec.types[len(spec.types)-1] != engine.Int8 {
		t.Fatalf("composite counter value type = %d, want Int8", spec.types[len(spec.types)-1])
	}
	for _, sql := range []string{
		"MURMUR POLICY MIN t c KEY VALUE 5",
		"MURMUR POLICY MIN t c KEY 'k1' 5",
		"MURMUR POLICY MIN t c 'k1' 'k2' 5",
		"MURMUR POLICY MIN t c KEY 'k1' VALUE",
		"MURMUR POLICY FROBNICATE t c KEY 'k' VALUE 1",
		"MURMUR POLICY MIN t c",
	} {
		if _, e := parseCommand(sql); e == nil {
			t.Fatalf("%s parsed, want syntax error", sql)
		}
	}
}

// TestPolicyJSONGrammar pins the JSON op shapes: JSON_SET takes
// KEY ... PATH ... VALUE, JSON_REMOVE takes KEY ... PATH, and neither
// has a legacy unmarked form.
func TestPolicyJSONGrammar(t *testing.T) {
	set, e := parseCommand("MURMUR POLICY JSON_SET 't' 'c' KEY 1 'x' PATH 'name' VALUE 'bob'")
	if e != nil {
		t.Fatalf("json set: %v", e)
	}
	if set.operation != "POLICY JSON_SET" {
		t.Fatalf("op = %q", set.operation)
	}
	want := []string{"t", "c", "1", "x", "name", "bob"}
	if len(set.args) != len(want) {
		t.Fatalf("args = %d, want %d", len(set.args), len(want))
	}
	for i, w := range want {
		if set.args[i].value != w {
			t.Fatalf("arg %d = %q, want %q", i, set.args[i].value, w)
		}
	}
	rem, e := parseCommand("murmur policy json_remove t c key 'k' path 'a'")
	if e != nil {
		t.Fatalf("json remove: %v", e)
	}
	if rem.operation != "POLICY JSON_REMOVE" || len(rem.args) != 4 {
		t.Fatalf("remove = %q %d args", rem.operation, len(rem.args))
	}
	for _, sql := range []string{
		"MURMUR POLICY JSON_SET 't' 'c' 'k' 'p' 1",
		"MURMUR POLICY JSON_REMOVE 't' 'c' 'k' 'p'",
		"MURMUR POLICY JSON_SET 't' 'c' KEY 'k' VALUE 1",
		"MURMUR POLICY JSON_SET 't' 'c' KEY PATH 'p' VALUE 1",
		"MURMUR POLICY JSON_REMOVE 't' 'c' KEY 'k' VALUE 'p'",
		"MURMUR POLICY JSON_REMOVE 't' 'c' KEY PATH",
	} {
		if _, e := parseCommand(sql); e == nil {
			t.Fatalf("%s parsed, want syntax error", sql)
		}
	}
}

// TestPolicyListGrammar pins the list op shapes: the single-value ops
// take both the legacy single-key form and the marked composite form.
func TestPolicyListGrammar(t *testing.T) {
	legacy, e := parseCommand("MURMUR POLICY LIST_APPEND 't' 'c' 'k' 1")
	if e != nil {
		t.Fatalf("legacy list append: %v", e)
	}
	if legacy.operation != "POLICY LIST_APPEND" || len(legacy.args) != 4 {
		t.Fatalf("legacy = %q %d args", legacy.operation, len(legacy.args))
	}
	marked, e := parseCommand("MURMUR POLICY LIST_REMOVE 't' 'c' KEY 1 'x' VALUE 'a'")
	if e != nil {
		t.Fatalf("marked list remove: %v", e)
	}
	if marked.operation != "POLICY LIST_REMOVE" || len(marked.args) != 5 {
		t.Fatalf("marked = %q %d args", marked.operation, len(marked.args))
	}
	for _, sql := range []string{
		"MURMUR POLICY LIST_APPEND 't' 'c' 'k'",
		"MURMUR POLICY LIST_APPEND 't' 'c' KEY 'k' 'v'",
		"MURMUR POLICY LIST_APPEND 't' 'c' KEY 'k' VALUE",
		"MURMUR POLICY LIST_PUSH 't' 'c' 'k' 1",
	} {
		if _, e := parseCommand(sql); e == nil {
			t.Fatalf("%s parsed, want syntax error", sql)
		}
	}
}

// TestPolicyCompositeWire pins that composite-key args reach the engine
// over the wire: keys stay text (the engine coerces by schema), the
// counter delta arrives typed, and MIN/MAX value coercion applies to
// the last argument whatever the arity.
func TestPolicyCompositeWire(t *testing.T) {
	f := &fixtureEngine{}
	_, addr, _ := startFixture(t, config.DefaultConfig(), f)
	w := mustConnect(t, addr)
	if ms := w.query(t, "MURMUR UNLOCK KEY 'old-secret'"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	if ms := w.query(t, "MURMUR POLICY COUNTER_ADD 't' 'c' KEY 7 'k' VALUE 5"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	if ms := w.query(t, "MURMUR POLICY MIN 't' 'c' KEY 'a' 9 VALUE 2.5"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	if ms := w.query(t, "MURMUR POLICY JSON_SET 't' 'c' KEY 'k' PATH 'a' VALUE '123'"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	if ms := w.query(t, "MURMUR POLICY JSON_SET 't' 'c' KEY 'k' PATH 'b' VALUE 123"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	if ms := w.query(t, "MURMUR POLICY JSON_REMOVE 't' 'c' KEY 'k' PATH 'a'"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	if ms := w.query(t, "MURMUR POLICY JSON_SET 't' 'c' KEY 'k' PATH 'z' VALUE null"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	if ms := w.query(t, "MURMUR POLICY LIST_APPEND 't' 'c' KEY 'k' VALUE 7"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	if ms := w.query(t, "MURMUR POLICY LIST_REMOVE 't' 'c' KEY 'k' VALUE 7"); findCode(ms) != "" {
		t.Fatal(ms)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var got []engine.Command
	for _, c := range f.commands {
		if strings.HasPrefix(c.Operation, "POLICY") {
			got = append(got, c)
		}
	}
	want := [][]engine.Value{
		{"t", "c", "7", "k", int64(5)},
		{"t", "c", "a", "9", 2.5},
		{"t", "c", "k", "a", "123"},
		{"t", "c", "k", "b", int64(123)},
		{"t", "c", "k", "a"},
		{"t", "c", "k", "z", nil},
		{"t", "c", "k", int64(7)},
		{"t", "c", "k", int64(7)},
	}
	if len(got) != len(want) {
		t.Fatalf("policy commands = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual([]engine.Value(got[i].Arguments), want[i]) {
			t.Fatalf("command %d args = %#v, want %#v", i, got[i].Arguments, want[i])
		}
	}
}
