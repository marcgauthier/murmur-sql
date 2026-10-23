package server

import (
	"math"
	"strconv"
	"strings"

	"github.com/marcgauthier/murmur-sql/engine"
)

// coerceValue preserves the Rust shell's SQLite-affinity conversions for both
// text and binary PostgreSQL rows, including values whose storage class varies.
func coerceValue(value engine.Value, oid uint32) (engine.Value, error) {
	if value == nil {
		return nil, nil
	}
	failure := func() (engine.Value, error) {
		return nil, sqlError("42846", "cannot encode SQLite value as the described PostgreSQL type")
	}
	switch oid {
	case engine.Bool:
		switch v := value.(type) {
		case bool:
			return v, nil
		case int64:
			return v != 0, nil
		case float64:
			return v != 0, nil
		case string:
			s := strings.ToLower(strings.TrimSpace(v))
			if s == "t" {
				return true, nil
			}
			if s == "f" {
				return false, nil
			}
			b, e := boolean(s)
			if e != nil {
				return failure()
			}
			return b, nil
		}
		return failure()
	case engine.Int8, engine.Int4, engine.Int2:
		switch v := value.(type) {
		case int64:
			return v, nil
		case float64:
			if math.IsNaN(v) {
				return int64(0), nil
			}
			if v >= float64(math.MaxInt64) {
				return int64(math.MaxInt64), nil
			}
			if v <= float64(math.MinInt64) {
				return int64(math.MinInt64), nil
			}
			return int64(v), nil
		case string:
			return sqliteInteger(v), nil
		}
		return failure()
	case engine.Float8, engine.Float4:
		switch v := value.(type) {
		case float64:
			return v, nil
		case int64:
			return float64(v), nil
		case string:
			n, e := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if e != nil {
				return float64(0), nil
			}
			return n, nil
		}
		return failure()
	case engine.Bytea:
		switch v := value.(type) {
		case []byte:
			return v, nil
		case string:
			return []byte(v), nil
		}
		return failure()
	}
	return value, nil
}
func sqliteInteger(value string) int64 {
	s := strings.TrimLeft(value, " \t\r\n\v\f")
	sign := int64(1)
	if strings.HasPrefix(s, "-") {
		sign = -1
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	var n int64
	for _, r := range s {
		if r < '0' || r > '9' {
			break
		}
		digit := int64(r - '0')
		if n > (math.MaxInt64-digit)/10 {
			n = math.MaxInt64
		} else {
			n = n*10 + digit
		}
	}
	return n * sign
}
