package murmur

import (
	"testing"
)

func TestListDocRoundtrip(t *testing.T) {
	list, err := ParseListDoc(TextValue(`[1,"x",2.5,null,true]`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []Value{IntValue(1), TextValue("x"), RealValue(2.5), NullValue(), IntValue(1)}
	if len(list) != len(want) {
		t.Fatalf("parsed = %+v", list)
	}
	for i := range want {
		if !ValuesEqual(list[i], want[i]) {
			t.Fatalf("element %d = %+v, want %+v", i, list[i], want[i])
		}
	}
	rendered, err := RenderListDoc(list)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if rendered.S != `[1,"x",2.5,null,1]` {
		t.Fatalf("rendered = %q", rendered.S)
	}
	empty, err := ParseListDoc(NullValue())
	if err != nil || len(empty) != 0 {
		t.Fatalf("null = %+v, %v", empty, err)
	}
	rendered, err = RenderListDoc(empty)
	if err != nil || rendered.S != "[]" {
		t.Fatalf("empty rendered = %+v, %v", rendered, err)
	}
	for _, bad := range []Value{IntValue(1), TextValue(`{"a":1}`), TextValue(`[[1]]`), TextValue(`[1,`)} {
		if _, err := ParseListDoc(bad); err == nil {
			t.Fatalf("%+v parsed, want error", bad)
		}
	}
}
