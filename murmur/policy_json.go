package murmur

import (
	"bytes"
	"encoding/json"
	"math"
	"sort"
	"strconv"
)

// ParseJsonDoc decodes a JSON policy cell into path values: TEXT must
// hold an object, NULL decodes to the empty document, anything else
// fails loudly.
func ParseJsonDoc(v Value) (map[string]Value, error) {
	doc := make(map[string]Value)
	switch v.Tag {
	case ValueNull:
		return doc, nil
	case ValueText:
	default:
		return nil, invalidErr("json projection must be TEXT or NULL")
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(v.S)))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil, invalidErr("json projection must be an object: %v", err)
	}
	for path, n := range raw {
		v, err := policyScalarFromJSON(n)
		if err != nil {
			return nil, err
		}
		doc[path] = v
	}
	return doc, nil
}

// policyScalarFromJSON renders one decoded JSON scalar as a murmur
// value. Booleans map to 1/0 (SQLite has no bool either); nested
// values have no scalar form.
func policyScalarFromJSON(n any) (Value, error) {
	switch n := n.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return IntValue(i), nil
		}
		if f, err := strconv.ParseFloat(n.String(), 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			return RealValue(f), nil
		}
		return Value{}, invalidErr("json number out of range")
	case string:
		return TextValue(n), nil
	case bool:
		if n {
			return IntValue(1), nil
		}
		return IntValue(0), nil
	case nil:
		return NullValue(), nil
	default:
		return Value{}, invalidErr("json projection must hold scalars")
	}
}

// RenderJsonDoc renders path values as canonical JSON text: sorted
// keys, integers bare, finite reals shortest-form. Operation apply and
// causal projection share this renderer so both sides agree byte for
// byte.
func RenderJsonDoc(doc map[string]Value) (Value, error) {
	paths := make([]string, 0, len(doc))
	for path := range doc {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var out bytes.Buffer
	out.WriteByte('{')
	for i, path := range paths {
		if i > 0 {
			out.WriteByte(',')
		}
		key, err := json.Marshal(path)
		if err != nil {
			return Value{}, corruptErr("json path: %v", err)
		}
		out.Write(key)
		out.WriteByte(':')
		if err := appendPolicyScalarJSON(&out, doc[path]); err != nil {
			return Value{}, err
		}
	}
	out.WriteByte('}')
	return TextValue(out.String()), nil
}

// appendPolicyScalarJSON renders one scalar value as JSON.
func appendPolicyScalarJSON(out *bytes.Buffer, v Value) error {
	switch v.Tag {
	case ValueInteger:
		out.WriteString(strconv.FormatInt(v.I, 10))
	case ValueReal:
		if math.IsNaN(v.F) || math.IsInf(v.F, 0) {
			return invalidErr("json real must be finite")
		}
		out.WriteString(strconv.FormatFloat(v.F, 'g', -1, 64))
	case ValueText:
		s, err := json.Marshal(v.S)
		if err != nil {
			return corruptErr("json text: %v", err)
		}
		out.Write(s)
	case ValueNull:
		out.WriteString("null")
	default:
		return invalidErr("json value must be a scalar")
	}
	return nil
}

// CheckPolicyScalar rejects values with no JSON scalar form.
func CheckPolicyScalar(v Value) error {
	switch v.Tag {
	case ValueInteger, ValueText, ValueNull:
		return nil
	case ValueReal:
		if math.IsNaN(v.F) || math.IsInf(v.F, 0) {
			return invalidErr("json real must be finite")
		}
		return nil
	default:
		return invalidErr("json value must be a scalar")
	}
}
