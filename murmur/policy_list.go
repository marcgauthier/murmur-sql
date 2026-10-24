package murmur

import (
	"bytes"
	"encoding/json"
)

// ParseListDoc decodes a list policy cell into elements: TEXT must
// hold an array of scalars, NULL decodes to the empty list, anything
// else fails loudly.
func ParseListDoc(v Value) ([]Value, error) {
	switch v.Tag {
	case ValueNull:
		return nil, nil
	case ValueText:
	default:
		return nil, invalidErr("list projection must be TEXT or NULL")
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(v.S)))
	dec.UseNumber()
	var raw []any
	if err := dec.Decode(&raw); err != nil {
		return nil, invalidErr("list projection must be an array: %v", err)
	}
	out := make([]Value, 0, len(raw))
	for _, n := range raw {
		v, err := policyScalarFromJSON(n)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// RenderListDoc renders elements as canonical JSON array text.
// Operation apply and causal projection share this renderer so both
// sides agree byte for byte.
func RenderListDoc(list []Value) (Value, error) {
	var out bytes.Buffer
	out.WriteByte('[')
	for i, v := range list {
		if i > 0 {
			out.WriteByte(',')
		}
		if err := appendPolicyScalarJSON(&out, v); err != nil {
			return Value{}, err
		}
	}
	out.WriteByte(']')
	return TextValue(out.String()), nil
}
