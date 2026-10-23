package replication

import (
	"testing"
)

func TestCounterAddJoinValue(t *testing.T) {
	db, a, b := idOf(1), idOf(2), idOf(3)
	c := &Counter{}
	if err := c.Add(db, a, 5); err != nil {
		t.Fatal(err)
	}
	if err := c.Add(db, a, -2); err != nil {
		t.Fatal(err)
	}
	if err := c.Add(db, b, 10); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Value(); err != nil || v != 13 {
		t.Fatalf("value = %d, %v", v, err)
	}
	// Join converges both ways.
	d := &Counter{}
	if err := d.Add(db, a, 7); err != nil {
		t.Fatal(err)
	}
	if err := d.Add(db, b, 1); err != nil {
		t.Fatal(err)
	}
	c.Join(d)
	d.Join(c)
	vc, err := c.Value()
	if err != nil {
		t.Fatal(err)
	}
	vd, err := d.Value()
	if err != nil {
		t.Fatal(err)
	}
	if vc != vd || vc != 15 {
		t.Fatalf("joined = %d/%d", vc, vd)
	}
	// Component overflow is refused.
	full := &Counter{Positive: map[[2]NodeId]uint64{{db, a}: ^uint64(0)}}
	if err := full.Add(db, a, 1); err == nil {
		t.Fatal("component overflow accepted")
	}
	// Totals outside the SQLite integer range are refused.
	wide := &Counter{Positive: map[[2]NodeId]uint64{{db, a}: uint64(1) << 63}}
	if _, err := wide.Value(); err == nil {
		t.Fatal("wide total accepted")
	}
	// MaxInt64 stays representable.
	top := &Counter{Positive: map[[2]NodeId]uint64{{db, a}: uint64(1)<<63 - 1}}
	if v, err := top.Value(); err != nil || v != 1<<63-1 {
		t.Fatalf("max total = %d, %v", v, err)
	}
	edge := &Counter{Negative: map[[2]NodeId]uint64{{db, a}: uint64(1) << 63}}
	if v, err := edge.Value(); err != nil || v != -1<<63 {
		t.Fatalf("min total = %d, %v", v, err)
	}
}

func TestSetAddRemoveJoinValues(t *testing.T) {
	tag := func(op uint32) Tag {
		return Tag{Database: idOf(1), Node: idOf(2), Transaction: idOf(3), Operation: op}
	}
	s := &Set{}
	s.Add([]byte("b"), tag(1))
	s.Add([]byte("a"), tag(2))
	s.Remove([]byte("b"))
	values := s.Values()
	if len(values) != 1 || string(values[0]) != "a" {
		t.Fatalf("values = %q", values)
	}
	// Re-adding under a fresh tag revives the value.
	s.Add([]byte("b"), tag(3))
	values = s.Values()
	if len(values) != 2 || string(values[0]) != "a" || string(values[1]) != "b" {
		t.Fatalf("revived = %q", values)
	}
	// Join unions disjoint additions both ways.
	o := &Set{}
	o.Add([]byte("c"), tag(4))
	s.Join(o)
	o.Join(s)
	for i, set := range []*Set{s, o} {
		values := set.Values()
		if len(values) != 3 {
			t.Fatalf("set %d values = %q", i, values)
		}
	}
}

func TestCausalJoin(t *testing.T) {
	db, a := idOf(1), idOf(2)
	x := &Causal{Counter: &Counter{}}
	y := &Causal{Counter: &Counter{}}
	if err := x.Counter.Add(db, a, 1); err != nil {
		t.Fatal(err)
	}
	if err := x.Join(y); err != nil {
		t.Fatal(err)
	}
	if v, err := x.Counter.Value(); err != nil || v != 1 {
		t.Fatalf("counter join = %d, %v", v, err)
	}
	xs := &Causal{Set: &Set{}}
	ys := &Causal{Set: &Set{}}
	xs.Set.Add([]byte("v"), Tag{Database: db})
	if err := xs.Join(ys); err != nil {
		t.Fatal(err)
	}
	if len(xs.Set.Values()) != 1 {
		t.Fatal("set join lost value")
	}
	if err := x.Join(xs); err == nil {
		t.Fatal("mixed join accepted")
	}
	if err := (&Causal{}).Join(&Causal{}); err == nil {
		t.Fatal("empty join accepted")
	}
}

func TestPolicyCodecRoundtrips(t *testing.T) {
	db, a := idOf(1), idOf(2)
	counter := &Counter{}
	if err := counter.Add(db, a, 3); err != nil {
		t.Fatal(err)
	}
	backCounter, err := DecodeCounter(EncodeCounter(counter))
	if err != nil {
		t.Fatal(err)
	}
	if v, err := backCounter.Value(); err != nil || v != 3 {
		t.Fatalf("counter roundtrip = %d, %v", v, err)
	}
	set := &Set{}
	set.Add([]byte("v"), Tag{Database: db, Node: a, Transaction: idOf(3), Operation: 7})
	backSet, err := DecodeSet(EncodeSet(set))
	if err != nil {
		t.Fatal(err)
	}
	if values := backSet.Values(); len(values) != 1 || string(values[0]) != "v" {
		t.Fatalf("set roundtrip = %q", values)
	}
	causal := &Causal{Counter: counter}
	backCausal, err := DecodeCausal(EncodeCausal(causal))
	if err != nil {
		t.Fatal(err)
	}
	if backCausal.Counter == nil {
		t.Fatal("causal kind lost")
	}
	if v, err := backCausal.Counter.Value(); err != nil || v != 3 {
		t.Fatalf("causal roundtrip = %d, %v", v, err)
	}
	tag := Tag{Database: db, Node: a, Transaction: idOf(3), Operation: 9}
	backTag, err := DecodeTag(EncodeTag(tag))
	if err != nil {
		t.Fatal(err)
	}
	if backTag != tag {
		t.Fatalf("tag roundtrip = %+v", backTag)
	}
	if !tagLess(Tag{Operation: 1}, Tag{Operation: 2}) || tagLess(Tag{Operation: 2}, Tag{Operation: 1}) {
		t.Fatal("tag ordering broken")
	}
	for name, raw := range map[string][]byte{
		"counter": EncodeCounter(counter),
		"set":     EncodeSet(set),
		"causal":  EncodeCausal(causal),
		"tag":     EncodeTag(tag),
	} {
		var err error
		switch name {
		case "counter":
			_, err = DecodeCounter(raw[:len(raw)-1])
		case "set":
			_, err = DecodeSet(raw[:len(raw)-1])
		case "causal":
			_, err = DecodeCausal(raw[:len(raw)-1])
		case "tag":
			_, err = DecodeTag(raw[:len(raw)-1])
		}
		if err == nil {
			t.Fatalf("%s: truncated accepted", name)
		}
	}
}
