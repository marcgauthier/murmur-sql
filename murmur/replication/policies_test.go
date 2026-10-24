package replication

import (
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/internal/postcard"
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

func TestJsonMapSetRemoveJoin(t *testing.T) {
	tag := func(op uint32) Tag {
		return Tag{Database: idOf(1), Node: idOf(2), Transaction: idOf(3), Operation: op}
	}
	m := &JsonMap{}
	// Later sets overwrite regardless of tag order (intra-batch recency).
	m.Set("a", []byte("1"), tag(2))
	m.Set("a", []byte("0"), tag(1))
	m.Set("b", []byte("x"), tag(1))
	if got := m.Live(); len(got) != 2 || string(got["a"]) != "0" {
		t.Fatalf("live = %+v, want a/0 b/x", got)
	}
	// Removal retires the observed entry whatever the tag order; a
	// later set revives the path.
	m.Remove("a")
	if got := m.Live(); len(got) != 1 {
		t.Fatalf("after remove live = %+v", got)
	}
	m.Set("a", []byte("2"), tag(4))
	if got := m.Live(); len(got) != 2 || string(got["a"]) != "2" {
		t.Fatalf("revived = %+v", got)
	}
	// Join takes the per-path highest tag both ways.
	o := &JsonMap{}
	o.Set("a", []byte("9"), tag(9))
	o.Set("c", []byte("z"), tag(1))
	m.Join(o)
	o.Join(m)
	for i, jm := range []*JsonMap{m, o} {
		got := jm.Live()
		if len(got) != 3 || string(got["a"]) != "9" || string(got["c"]) != "z" {
			t.Fatalf("map %d live = %+v", i, got)
		}
	}
	// An observed remove propagates through joins; an unobserved
	// concurrent set survives it.
	o.Remove("a")
	m.Join(o)
	o.Join(m)
	for i, jm := range []*JsonMap{m, o} {
		if got := jm.Live(); len(got) != 2 {
			t.Fatalf("map %d live = %+v, want b/c", i, got)
		}
	}
	r := &JsonMap{}
	r.Set("a", []byte("late"), tag(7))
	m.Join(r)
	r.Join(m)
	for i, jm := range []*JsonMap{m, r} {
		got := jm.Live()
		if len(got) != 3 || string(got["a"]) != "late" {
			t.Fatalf("map %d live = %+v, want concurrent set to survive", i, got)
		}
	}
}

func TestCausalJsonJoin(t *testing.T) {
	x := &Causal{Json: &JsonMap{}}
	y := &Causal{Json: &JsonMap{}}
	x.Json.Set("a", []byte("1"), Tag{Database: idOf(1), Node: idOf(2)})
	if err := x.Join(y); err != nil {
		t.Fatal(err)
	}
	if err := y.Join(x); err != nil {
		t.Fatal(err)
	}
	if len(y.Json.Live()) != 1 {
		t.Fatal("json join lost path")
	}
	if err := x.Join(&Causal{Counter: &Counter{}}); err == nil {
		t.Fatal("mixed json/counter join accepted")
	}
}

func TestJsonCodecRoundtrips(t *testing.T) {
	m := &JsonMap{}
	m.Set("b", []byte("x"), Tag{Database: idOf(1), Node: idOf(2), Transaction: idOf(3), Operation: 7})
	m.Set("a", []byte("y"), Tag{Database: idOf(1), Node: idOf(2), Transaction: idOf(4)})
	m.Remove("b")
	back, err := DecodeJsonMap(EncodeJsonMap(m))
	if err != nil {
		t.Fatal(err)
	}
	got := back.Live()
	if len(got) != 1 || string(got["a"]) != "y" {
		t.Fatalf("roundtrip live = %+v", got)
	}
	causal, err := DecodeCausal(EncodeCausal(&Causal{Json: m}))
	if err != nil || causal.Json == nil {
		t.Fatalf("causal roundtrip = %+v, %v", causal, err)
	}
	if _, err = DecodeJsonMap(EncodeJsonMap(m)[:len(EncodeJsonMap(m))-1]); err == nil {
		t.Fatal("truncated json accepted")
	}
	backPolicy, err := decodeOne(encode(func(e *postcard.Encoder) { encodePolicy(e, PolicyJson) }), decodePolicy)
	if err != nil || backPolicy != PolicyJson {
		t.Fatalf("policy roundtrip = %v, %v", backPolicy, err)
	}
}

func TestJsonMapSequentialRemoveHides(t *testing.T) {
	// A remove that observes a set must hide the path whatever the
	// tag order: sequential set-then-remove always reads back removed.
	hi := Tag{Database: idOf(1), Node: idOf(2), Transaction: idOf(3), Operation: 9}
	lo := Tag{Database: idOf(1), Node: idOf(2), Transaction: idOf(3), Operation: 1}
	m := &JsonMap{}
	m.Set("a", []byte("v"), hi)
	m.Remove("a")
	if got := m.Live(); len(got) != 0 {
		t.Fatalf("live after observed remove = %+v, want empty", got)
	}
	_ = lo
}

func TestListSetAppendRemoveJoin(t *testing.T) {
	tag := func(op uint32) Tag {
		return Tag{Database: idOf(1), Node: idOf(2), Transaction: idOf(3), Operation: op}
	}
	l := &ListSet{}
	// Duplicates render twice, in tag order.
	l.Append([]byte("b"), tag(2))
	l.Append([]byte("a"), tag(1))
	l.Append([]byte("b"), tag(3))
	if got := stringifyList(l.Values()); len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "b" {
		t.Fatalf("values = %v", got)
	}
	// Remove retires every observed tag with the value.
	l.Remove([]byte("b"))
	if got := stringifyList(l.Values()); len(got) != 1 || got[0] != "a" {
		t.Fatalf("after remove = %v", got)
	}
	// A later append revives the value.
	l.Append([]byte("b"), tag(4))
	if got := stringifyList(l.Values()); len(got) != 2 {
		t.Fatalf("revived = %v", got)
	}
	// Join unions appends and tombstones both ways.
	o := &ListSet{}
	o.Append([]byte("c"), tag(5))
	l.Join(o)
	o.Join(l)
	for i, ls := range []*ListSet{l, o} {
		if got := stringifyList(ls.Values()); len(got) != 3 {
			t.Fatalf("list %d = %v", i, got)
		}
	}
}

func stringifyList(in [][]byte) []string {
	out := make([]string, 0, len(in))
	for _, b := range in {
		out = append(out, string(b))
	}
	return out
}

func TestCausalListJoin(t *testing.T) {
	x := &Causal{List: &ListSet{}}
	y := &Causal{List: &ListSet{}}
	x.List.Append([]byte("v"), Tag{Database: idOf(1), Node: idOf(2)})
	if err := x.Join(y); err != nil {
		t.Fatal(err)
	}
	if err := y.Join(x); err != nil {
		t.Fatal(err)
	}
	if len(y.List.Values()) != 1 {
		t.Fatal("list join lost element")
	}
	if err := x.Join(&Causal{Set: &Set{}}); err == nil {
		t.Fatal("mixed list/set join accepted")
	}
}

func TestListCodecRoundtrips(t *testing.T) {
	l := &ListSet{}
	l.Append([]byte("b"), Tag{Database: idOf(1), Node: idOf(2), Transaction: idOf(3), Operation: 7})
	l.Append([]byte("a"), Tag{Database: idOf(1), Node: idOf(2), Transaction: idOf(4)})
	l.Remove([]byte("b"))
	back, err := DecodeListSet(EncodeListSet(l))
	if err != nil {
		t.Fatal(err)
	}
	if got := stringifyList(back.Values()); len(got) != 1 || got[0] != "a" {
		t.Fatalf("roundtrip = %v", got)
	}
	causal, err := DecodeCausal(EncodeCausal(&Causal{List: l}))
	if err != nil || causal.List == nil {
		t.Fatalf("causal roundtrip = %+v, %v", causal, err)
	}
	if _, err = DecodeListSet(EncodeListSet(l)[:len(EncodeListSet(l))-1]); err == nil {
		t.Fatal("truncated list accepted")
	}
	backPolicy, err := decodeOne(encode(func(e *postcard.Encoder) { encodePolicy(e, PolicyList) }), decodePolicy)
	if err != nil || backPolicy != PolicyList {
		t.Fatalf("policy roundtrip = %v, %v", backPolicy, err)
	}
}
