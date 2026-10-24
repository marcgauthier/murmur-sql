package murmur

import (
	"math"
	"testing"
)

func TestJsonDocRoundtrip(t *testing.T) {
	doc, err := ParseJsonDoc(TextValue(`{"b":"x","a":1,"c":2.5,"n":null,"t":true,"f":false}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := map[string]Value{
		"a": IntValue(1), "b": TextValue("x"), "c": RealValue(2.5),
		"n": NullValue(), "t": IntValue(1), "f": IntValue(0),
	}
	for path, v := range want {
		if !ValuesEqual(doc[path], v) {
			t.Fatalf("%s = %+v, want %+v", path, doc[path], v)
		}
	}
	rendered, err := RenderJsonDoc(doc)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if rendered.S != `{"a":1,"b":"x","c":2.5,"f":0,"n":null,"t":1}` {
		t.Fatalf("rendered = %q", rendered.S)
	}
	empty, err := ParseJsonDoc(NullValue())
	if err != nil || len(empty) != 0 {
		t.Fatalf("null = %+v, %v", empty, err)
	}
	rendered, err = RenderJsonDoc(empty)
	if err != nil || rendered.S != "{}" {
		t.Fatalf("empty rendered = %+v, %v", rendered, err)
	}
	for _, bad := range []Value{IntValue(1), TextValue(`[1]`), TextValue(`{"a":{"b":1}}`), TextValue(`{"a":`)} {
		if _, err := ParseJsonDoc(bad); err == nil {
			t.Fatalf("%+v parsed, want error", bad)
		}
	}
	if err := CheckPolicyScalar(BlobValue([]byte{1})); err == nil {
		t.Fatal("blob scalar accepted")
	}
	if err := CheckPolicyScalar(RealValue(math.Inf(1))); err == nil {
		t.Fatal("infinite scalar accepted")
	}
}
