package replication

import (
	"bytes"
	"math/bits"
	"sort"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
)

// Policy selects the merge behavior of one column.
type Policy int

const (
	PolicyLww Policy = iota
	PolicyCounter
	PolicySet
	PolicyMin
	PolicyMax
)

// Counter is a grow-only per-actor counter pair.
type Counter struct {
	Positive map[[2]NodeId]uint64
	Negative map[[2]NodeId]uint64
}

// Add attributes delta to one actor.
func (c *Counter) Add(database, node NodeId, delta int64) error {
	if c.Positive == nil {
		c.Positive = make(map[[2]NodeId]uint64)
	}
	if c.Negative == nil {
		c.Negative = make(map[[2]NodeId]uint64)
	}
	abs := uint64(delta)
	if delta < 0 {
		abs = uint64(-(delta + 1)) + 1
	}
	component := c.Positive
	if delta < 0 {
		component = c.Negative
	}
	v, carry := bits.Add64(component[[2]NodeId{database, node}], abs, 0)
	if carry != 0 {
		return protocolErr("counter component overflow")
	}
	component[[2]NodeId{database, node}] = v
	return nil
}

// Join takes the per-actor maximum of both counters.
func (c *Counter) Join(other *Counter) {
	if c.Positive == nil {
		c.Positive = make(map[[2]NodeId]uint64)
	}
	if c.Negative == nil {
		c.Negative = make(map[[2]NodeId]uint64)
	}
	for _, pair := range [][2]map[[2]NodeId]uint64{{c.Positive, other.Positive}, {c.Negative, other.Negative}} {
		target, source := pair[0], pair[1]
		for actor, n := range source {
			if target[actor] < n {
				target[actor] = n
			}
		}
	}
}

func sum128(values map[[2]NodeId]uint64) (hi, lo uint64, err error) {
	for _, n := range values {
		var carry uint64
		lo, carry = bits.Add64(lo, n, 0)
		hi, carry = bits.Add64(hi, 0, carry)
		if carry != 0 {
			return 0, 0, protocolErr("counter overflow")
		}
	}
	return hi, lo, nil
}

// Value projects the signed counter total.
func (c *Counter) Value() (int64, error) {
	phi, plo, err := sum128(c.Positive)
	if err != nil {
		return 0, err
	}
	nhi, nlo, err := sum128(c.Negative)
	if err != nil {
		return 0, err
	}
	// diff = positive - negative in signed 128-bit, then range-check.
	lo, borrow := bits.Sub64(plo, nlo, 0)
	hi, _ := bits.Sub64(phi, nhi, borrow)
	neg := hi>>63 != 0
	var magHi, magLo uint64
	if neg {
		var carry uint64
		magLo, carry = bits.Sub64(0, lo, 0)
		magHi, _ = bits.Sub64(0, hi, carry)
	} else {
		magHi, magLo = hi, lo
	}
	if magHi != 0 || magLo > uint64(1)<<63 {
		return 0, protocolErr("counter exceeds SQLite integer range")
	}
	if neg {
		if magLo == uint64(1)<<63 {
			return -1 << 63, nil
		}
		return -int64(magLo), nil
	}
	return int64(magLo), nil
}

// Tag identifies one set addition.
type Tag struct {
	Database    NodeId
	Node        NodeId
	Transaction NodeId
	Operation   uint32
}

func tagLess(a, b Tag) bool {
	if a.Database != b.Database {
		return bytes.Compare(a.Database[:], b.Database[:]) < 0
	}
	if a.Node != b.Node {
		return bytes.Compare(a.Node[:], b.Node[:]) < 0
	}
	if a.Transaction != b.Transaction {
		return bytes.Compare(a.Transaction[:], b.Transaction[:]) < 0
	}
	return a.Operation < b.Operation
}

// Set is an observed-remove set keyed by encoded value.
type Set struct {
	Additions map[string]map[Tag]bool
	Removals  map[Tag]bool
}

// Add records one tagged addition.
func (s *Set) Add(value []byte, tag Tag) {
	if s.Additions == nil {
		s.Additions = make(map[string]map[Tag]bool)
	}
	tags := s.Additions[string(value)]
	if tags == nil {
		tags = make(map[Tag]bool)
		s.Additions[string(value)] = tags
	}
	tags[tag] = true
}

// Remove retires every tag added for value.
func (s *Set) Remove(value []byte) {
	if s.Removals == nil {
		s.Removals = make(map[Tag]bool)
	}
	for tag := range s.Additions[string(value)] {
		s.Removals[tag] = true
	}
}

// Join unions additions and removals.
func (s *Set) Join(other *Set) {
	if s.Additions == nil {
		s.Additions = make(map[string]map[Tag]bool)
	}
	if s.Removals == nil {
		s.Removals = make(map[Tag]bool)
	}
	for value, tags := range other.Additions {
		mine := s.Additions[value]
		if mine == nil {
			mine = make(map[Tag]bool)
			s.Additions[value] = mine
		}
		for tag := range tags {
			mine[tag] = true
		}
	}
	for tag := range other.Removals {
		s.Removals[tag] = true
	}
}

// Values returns live values in key order.
func (s *Set) Values() [][]byte {
	keys := make([]string, 0, len(s.Additions))
	for k := range s.Additions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out [][]byte
	for _, k := range keys {
		live := false
		for tag := range s.Additions[k] {
			if !s.Removals[tag] {
				live = true
				break
			}
		}
		if live {
			out = append(out, []byte(k))
		}
	}
	return out
}

// Causal is one cell's causal merge state.
type Causal struct {
	Counter *Counter
	Set     *Set
}

// Join merges same-kind causal state.
func (c *Causal) Join(other *Causal) error {
	switch {
	case c.Counter != nil && other.Counter != nil:
		c.Counter.Join(other.Counter)
		return nil
	case c.Set != nil && other.Set != nil:
		c.Set.Join(other.Set)
		return nil
	}
	return protocolErr("merge policy changed")
}

func encodePolicy(e *postcard.Encoder, p Policy) { e.Discriminant(uint32(p)) }

func decodePolicy(d *postcard.Decoder) Policy {
	switch d.Discriminant() {
	case 0:
		return PolicyLww
	case 1:
		return PolicyCounter
	case 2:
		return PolicySet
	case 3:
		return PolicyMin
	case 4:
		return PolicyMax
	}
	d.Fail(protocolErr("bad merge policy"))
	return PolicyLww
}

func encodeCounterMap(e *postcard.Encoder, m map[[2]NodeId]uint64) {
	keys := make([][2]NodeId, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return bytes.Compare(keys[i][0][:], keys[j][0][:]) < 0
		}
		return bytes.Compare(keys[i][1][:], keys[j][1][:]) < 0
	})
	e.Len(len(keys))
	for _, k := range keys {
		e.Raw(k[0][:])
		e.Raw(k[1][:])
		e.U64(m[k])
	}
}

func decodeCounterMap(d *postcard.Decoder) map[[2]NodeId]uint64 {
	out := make(map[[2]NodeId]uint64)
	for n := d.Len(); n > 0; n-- {
		var k [2]NodeId
		copy(k[0][:], d.Raw(16))
		copy(k[1][:], d.Raw(16))
		out[k] = d.U64()
	}
	return out
}

func encodeCounter(e *postcard.Encoder, c *Counter) {
	encodeCounterMap(e, c.Positive)
	encodeCounterMap(e, c.Negative)
}

func decodeCounter(d *postcard.Decoder) *Counter {
	return &Counter{Positive: decodeCounterMap(d), Negative: decodeCounterMap(d)}
}

// EncodeCounter serializes a counter.
func EncodeCounter(c *Counter) []byte {
	return encode(func(e *postcard.Encoder) { encodeCounter(e, c) })
}

// DecodeCounter parses a counter.
func DecodeCounter(b []byte) (*Counter, error) { return decodeOne(b, decodeCounter) }

func encodeTag(e *postcard.Encoder, t Tag) {
	e.Raw(t.Database[:])
	e.Raw(t.Node[:])
	e.Raw(t.Transaction[:])
	e.U32(t.Operation)
}

func decodeTag(d *postcard.Decoder) Tag {
	var t Tag
	copy(t.Database[:], d.Raw(16))
	copy(t.Node[:], d.Raw(16))
	copy(t.Transaction[:], d.Raw(16))
	t.Operation = d.U32()
	return t
}

// EncodeTag serializes a tag.
func EncodeTag(t Tag) []byte { return encode(func(e *postcard.Encoder) { encodeTag(e, t) }) }

// DecodeTag parses a tag.
func DecodeTag(b []byte) (Tag, error) { return decodeOne(b, decodeTag) }

func encodeTagSet(e *postcard.Encoder, s map[Tag]bool) {
	tags := make([]Tag, 0, len(s))
	for t := range s {
		tags = append(tags, t)
	}
	sort.Slice(tags, func(i, j int) bool { return tagLess(tags[i], tags[j]) })
	e.Len(len(tags))
	for _, t := range tags {
		encodeTag(e, t)
	}
}

func decodeTagSet(d *postcard.Decoder) map[Tag]bool {
	out := make(map[Tag]bool)
	for n := d.Len(); n > 0; n-- {
		out[decodeTag(d)] = true
	}
	return out
}

func encodeSet(e *postcard.Encoder, s *Set) {
	keys := make([]string, 0, len(s.Additions))
	for k := range s.Additions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	e.Len(len(keys))
	for _, k := range keys {
		e.ByteString([]byte(k))
		encodeTagSet(e, s.Additions[k])
	}
	encodeTagSet(e, s.Removals)
}

func decodeSet(d *postcard.Decoder) *Set {
	s := &Set{Additions: make(map[string]map[Tag]bool)}
	for n := d.Len(); n > 0; n-- {
		s.Additions[string(d.Bytes())] = decodeTagSet(d)
	}
	s.Removals = decodeTagSet(d)
	return s
}

// EncodeSet serializes a set.
func EncodeSet(s *Set) []byte { return encode(func(e *postcard.Encoder) { encodeSet(e, s) }) }

// DecodeSet parses a set.
func DecodeSet(b []byte) (*Set, error) { return decodeOne(b, decodeSet) }

func encodeCausal(e *postcard.Encoder, c *Causal) {
	if c.Counter != nil {
		e.Discriminant(0)
		encodeCounter(e, c.Counter)
	} else {
		e.Discriminant(1)
		encodeSet(e, c.Set)
	}
}

func decodeCausal(d *postcard.Decoder) *Causal {
	switch d.Discriminant() {
	case 0:
		return &Causal{Counter: decodeCounter(d)}
	case 1:
		return &Causal{Set: decodeSet(d)}
	}
	d.Fail(protocolErr("bad causal state"))
	return nil
}

// EncodeCausal serializes causal state.
func EncodeCausal(c *Causal) []byte { return encode(func(e *postcard.Encoder) { encodeCausal(e, c) }) }

// DecodeCausal parses causal state.
func DecodeCausal(b []byte) (*Causal, error) { return decodeOne(b, decodeCausal) }
