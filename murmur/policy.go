package murmur

import (
	"bytes"
	"math"
)

// Explicit operation intents; durable causal state stays in Spool.
//
// This file holds the dependency-free policy value ordering. The full
// operation/materialize/join port follows with storage and replication.

// PolicyCompare orders policy values: NULL < number < text < blob.
// NaN is rejected; integers and reals compare exactly.
func PolicyCompare(a, b Value) (int, error) {
	rank := func(v Value) int {
		switch v.Tag {
		case ValueNull:
			return 0
		case ValueInteger, ValueReal:
			return 1
		case ValueText:
			return 2
		case ValueBlob:
			return 3
		}
		return 4
	}
	if ra, rb := rank(a), rank(b); ra != rb {
		if ra < rb {
			return -1, nil
		}
		return 1, nil
	}
	switch a.Tag {
	case ValueNull:
		return 0, nil
	case ValueInteger:
		if b.Tag == ValueInteger {
			switch {
			case a.I < b.I:
				return -1, nil
			case a.I > b.I:
				return 1, nil
			}
			return 0, nil
		}
		return integerReal(a.I, b.F)
	case ValueReal:
		if b.Tag == ValueReal {
			if math.IsNaN(a.F) || math.IsNaN(b.F) {
				return 0, invalidErr("NaN policy value")
			}
			switch {
			case a.F < b.F:
				return -1, nil
			case a.F > b.F:
				return 1, nil
			}
			return 0, nil
		}
		ord, err := integerReal(b.I, a.F)
		return -ord, err
	case ValueText:
		switch {
		case a.S < b.S:
			return -1, nil
		case a.S > b.S:
			return 1, nil
		}
		return 0, nil
	case ValueBlob:
		return bytes.Compare(a.B, b.B), nil
	}
	return 0, invalidErr("policy value types")
}

func integerReal(a int64, b float64) (int, error) {
	if math.IsNaN(b) {
		return 0, invalidErr("NaN policy value")
	}
	if b >= 9223372036854775808.0 {
		return -1, nil
	}
	if b < -9223372036854775808.0 {
		return 1, nil
	}
	integer := int64(b)
	switch {
	case a < integer:
		return -1, nil
	case a > integer:
		return 1, nil
	}
	f := b - math.Trunc(b)
	switch {
	case f > 0:
		return -1, nil
	case f < 0:
		return 1, nil
	}
	return 0, nil
}
