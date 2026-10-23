package murmur

import (
	"testing"
)

func TestHighLowOwnerCodec(t *testing.T) {
	owners := []HighLowOwner{
		{Table: "t", Key: []Value{IntValue(1)}, Column: "c"},
		{
			Table: "t", Key: []Value{IntValue(1), TextValue("k")}, Column: "c",
			Epoch: testVersion(), ReleaseEpoch: testVersion(), Owned: true,
			Cell: &Cell{Column: "c", Value: IntValue(9), Version: testVersion()},
		},
	}
	for i, o := range owners {
		back, err := DecodeHighLowOwner(EncodeHighLowOwner(&o))
		if err != nil {
			t.Fatalf("owner %d: %v", i, err)
		}
		if string(EncodeHighLowOwner(&back)) != string(EncodeHighLowOwner(&o)) {
			t.Fatalf("owner %d mismatch", i)
		}
	}
	if _, err := DecodeHighLowOwner(nil); err == nil {
		t.Fatal("empty owner accepted")
	}
	enc := EncodeHighLowOwner(&owners[1])
	if _, err := DecodeHighLowOwner(enc[:len(enc)-1]); err == nil {
		t.Fatal("truncated owner accepted")
	}
}

func TestOwnerPrecedes(t *testing.T) {
	ver := func(millis uint64) Version {
		return Version{Millis: millis, Logical: 1, Node: [16]byte{9}}
	}
	cellAt := func(millis uint64) *Cell {
		return &Cell{Column: "c", Value: IntValue(1), Version: ver(millis)}
	}
	cases := []struct {
		name     string
		previous HighLowOwner
		incoming HighLowOwner
		want     bool
	}{
		{"newer epoch precedes", HighLowOwner{Epoch: ver(20)}, HighLowOwner{Epoch: ver(10)}, true},
		{"older epoch yields", HighLowOwner{Epoch: ver(10)}, HighLowOwner{Epoch: ver(20)}, false},
		{"newer cell wins", HighLowOwner{Epoch: ver(10), Cell: cellAt(30)}, HighLowOwner{Epoch: ver(10), Cell: cellAt(20)}, true},
		{"older cell yields", HighLowOwner{Epoch: ver(10), Cell: cellAt(20)}, HighLowOwner{Epoch: ver(10), Cell: cellAt(30)}, false},
		{"cell beats no cell", HighLowOwner{Epoch: ver(10), Cell: cellAt(1)}, HighLowOwner{Epoch: ver(10)}, true},
		{"no cell yields", HighLowOwner{Epoch: ver(10)}, HighLowOwner{Epoch: ver(10), Cell: cellAt(1)}, false},
		{"neither cell yields", HighLowOwner{Epoch: ver(10)}, HighLowOwner{Epoch: ver(10)}, false},
	}
	for _, c := range cases {
		if got := ownerPrecedes(&c.previous, &c.incoming); got != c.want {
			t.Fatalf("%s = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestHighLowOwnerKey(t *testing.T) {
	key := []Value{IntValue(1)}
	a := HighLowOwnerKey("t", key, "c")
	if string(a) == "" {
		t.Fatal("empty owner key")
	}
	if string(HighLowOwnerKey("t", key, "c")) != string(a) {
		t.Fatal("owner key unstable")
	}
	if string(HighLowOwnerKey("t", key, "d")) == string(a) {
		t.Fatal("column ignored in owner key")
	}
	if string(HighLowOwnerKey("u", key, "c")) == string(a) {
		t.Fatal("table ignored in owner key")
	}
}
