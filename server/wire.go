package server

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/marcgauthier/murmur-sql-go/engine"
)

const maxMessage = 32 << 20

type packet struct {
	b   []byte
	pos int
	err error
}

func (p *packet) read(n int) []byte {
	if p.err != nil {
		return nil
	}
	if n < 0 || n > len(p.b)-p.pos {
		p.err = sqlError("08P01", "truncated protocol message")
		return nil
	}
	b := p.b[p.pos : p.pos+n]
	p.pos += n
	return b
}
func (p *packet) u16() uint16 {
	b := p.read(2)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}
func (p *packet) u32() uint32 {
	b := p.read(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
func (p *packet) cstr() string {
	if p.err != nil {
		return ""
	}
	b := p.b[p.pos:]
	i := bytes.IndexByte(b, 0)
	if i < 0 {
		p.err = sqlError("08P01", "unterminated protocol string")
		return ""
	}
	p.pos += i + 1
	if !utf8.Valid(b[:i]) {
		p.err = sqlError("22021", "protocol strings must be UTF-8")
		return ""
	}
	return string(b[:i])
}
func (p *packet) end() error {
	if p.err != nil {
		return p.err
	}
	if p.pos != len(p.b) {
		return sqlError("08P01", "trailing bytes in protocol message")
	}
	return nil
}
func put16(b []byte, n uint16) []byte { return binary.BigEndian.AppendUint16(b, n) }
func put32(b []byte, n uint32) []byte { return binary.BigEndian.AppendUint32(b, n) }
func cstr(b []byte, s string) []byte  { return append(append(b, s...), 0) }
func readLength(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, e := io.ReadFull(r, header[:]); e != nil {
		return nil, e
	}
	n := int64(binary.BigEndian.Uint32(header[:]))
	if n < 4 || n > maxMessage {
		return nil, sqlError("08P01", "invalid protocol message length")
	}
	b := make([]byte, int(n-4))
	_, e := io.ReadFull(r, b)
	return b, e
}
func (s *session) send(kind byte, b []byte) error {
	if len(b) > maxMessage-4 {
		return sqlError("54000", "response exceeds protocol message limit")
	}
	buf := make([]byte, 0, len(b)+5)
	buf = append(buf, kind)
	buf = put32(buf, uint32(len(b)+4))
	buf = append(buf, b...)
	_, e := s.writer.Write(buf)
	return e
}
func (s *session) sendError(e error) error {
	code := engine.SQLState(e)
	b := []byte{'S'}
	b = cstr(b, "ERROR")
	b = append(b, 'V')
	b = cstr(b, "ERROR")
	b = append(b, 'C')
	b = cstr(b, code)
	b = append(b, 'M')
	b = cstr(b, strings.ReplaceAll(e.Error(), "\x00", ""))
	return s.send('E', append(b, 0))
}
func (s *session) ready() error { return s.send('Z', []byte{'I'}) }
func formatFor(f []uint16, i int) uint16 {
	if len(f) == 0 {
		return 0
	}
	if len(f) == 1 {
		return f[0]
	}
	return f[i]
}
func validateFormats(f []uint16, n int) error {
	if len(f) > 1 && len(f) != n {
		return sqlError("08P01", "format count does not match field count")
	}
	for _, v := range f {
		if v > 1 {
			return sqlError("08P01", "invalid field format")
		}
	}
	return nil
}
func (s *session) rowDescription(columns []engine.Column, formats []uint16) error {
	if len(columns) > 32767 {
		return sqlError("54000", "too many columns")
	}
	if e := validateFormats(formats, len(columns)); e != nil {
		return e
	}
	b := put16(nil, uint16(len(columns)))
	for i, col := range columns {
		if strings.ContainsRune(col.Name, 0) {
			return fmt.Errorf("invalid column name")
		}
		b = cstr(b, col.Name)
		b = put32(b, 0)
		b = put16(b, 0)
		b = put32(b, col.OID)
		size := uint16(65535)
		switch col.OID {
		case engine.Bool:
			size = 1
		case engine.Int2:
			size = 2
		case engine.Int4, engine.Float4:
			size = 4
		case engine.Int8, engine.Float8:
			size = 8
		}
		b = put16(b, size)
		b = put32(b, math.MaxUint32)
		b = put16(b, formatFor(formats, i))
	}
	return s.send('T', b)
}
func (s *session) dataRow(columns []engine.Column, row []engine.Value, formats []uint16) error {
	if len(row) != len(columns) {
		return fmt.Errorf("engine returned incorrect row width")
	}
	b := put16(nil, uint16(len(row)))
	for i, v := range row {
		if v == nil {
			b = put32(b, math.MaxUint32)
			continue
		}
		raw, e := encodeValue(v, columns[i].OID, formatFor(formats, i))
		if e != nil {
			return e
		}
		if len(raw) > maxMessage-len(b)-4 {
			return sqlError("54000", "row exceeds protocol message limit")
		}
		b = put32(b, uint32(len(raw)))
		b = append(b, raw...)
	}
	return s.send('D', b)
}
func encodeValue(v engine.Value, oid uint32, format uint16) ([]byte, error) {
	var conversion error
	v, conversion = coerceValue(v, oid)
	if conversion != nil {
		return nil, conversion
	}
	if e := engine.RequireOID(oid); e != nil {
		return nil, e
	}
	fail := func() ([]byte, error) {
		return nil, sqlError("22003", fmt.Sprintf("cannot encode %T as PostgreSQL OID %d", v, oid))
	}
	if format == 0 {
		switch oid {
		case engine.Bool:
			if b, ok := v.(bool); ok {
				if b {
					return []byte("t"), nil
				}
				return []byte("f"), nil
			}
			if n, ok := v.(int64); ok {
				if n != 0 {
					return []byte("t"), nil
				}
				return []byte("f"), nil
			}
		case engine.Bytea:
			if b, ok := v.([]byte); ok {
				return []byte("\\x" + hex.EncodeToString(b)), nil
			}
		case engine.Int2, engine.Int4, engine.Int8:
			if n, ok := v.(int64); ok {
				if oid == engine.Int2 && (n < math.MinInt16 || n > math.MaxInt16) || oid == engine.Int4 && (n < math.MinInt32 || n > math.MaxInt32) {
					return fail()
				}
				return []byte(strconv.FormatInt(n, 10)), nil
			}
		case engine.Float4, engine.Float8:
			var f float64
			switch n := v.(type) {
			case float64:
				f = n
			case int64:
				f = float64(n)
			default:
				return fail()
			}
			return []byte(strconv.FormatFloat(f, 'g', -1, 64)), nil
		default:
			switch x := v.(type) {
			case string:
				if !utf8.ValidString(x) || strings.ContainsRune(x, 0) {
					return nil, sqlError("22021", "text result contains invalid UTF-8 or NUL")
				}
				return []byte(x), nil
			case int64:
				return []byte(strconv.FormatInt(x, 10)), nil
			case float64:
				return []byte(strconv.FormatFloat(x, 'g', -1, 64)), nil
			case bool:
				return []byte(strconv.FormatBool(x)), nil
			case []byte:
				return []byte("\\x" + hex.EncodeToString(x)), nil
			}
		}
		return fail()
	}
	switch oid {
	case engine.Bytea:
		if b, ok := v.([]byte); ok {
			return b, nil
		}
	case engine.Text, engine.Varchar, engine.Unknown:
		return encodeValue(v, oid, 0)
	case engine.Bool:
		if b, ok := v.(bool); ok {
			if b {
				return []byte{1}, nil
			}
			return []byte{0}, nil
		}
		if n, ok := v.(int64); ok {
			if n != 0 {
				return []byte{1}, nil
			}
			return []byte{0}, nil
		}
	case engine.Int2, engine.Int4, engine.Int8:
		if n, ok := v.(int64); ok {
			switch oid {
			case engine.Int2:
				if n < math.MinInt16 || n > math.MaxInt16 {
					return fail()
				}
				return put16(nil, uint16(n)), nil
			case engine.Int4:
				if n < math.MinInt32 || n > math.MaxInt32 {
					return fail()
				}
				return put32(nil, uint32(n)), nil
			default:
				return binary.BigEndian.AppendUint64(nil, uint64(n)), nil
			}
		}
	case engine.Float4, engine.Float8:
		var f float64
		switch n := v.(type) {
		case float64:
			f = n
		case int64:
			f = float64(n)
		default:
			return fail()
		}
		if oid == engine.Float4 {
			return put32(nil, math.Float32bits(float32(f))), nil
		}
		return binary.BigEndian.AppendUint64(nil, math.Float64bits(f)), nil
	}
	return fail()
}
func decodeValue(raw []byte, oid uint32, format uint16) (engine.Value, error) {
	if raw == nil {
		return nil, nil
	}
	if e := engine.RequireOID(oid); e != nil {
		return nil, e
	}
	fail := func() (engine.Value, error) {
		return nil, sqlError("22P02", fmt.Sprintf("invalid value for PostgreSQL OID %d", oid))
	}
	if format == 0 {
		s := string(raw)
		switch oid {
		case engine.Bool:
			if s == "t" {
				return true, nil
			}
			if s == "f" {
				return false, nil
			}
			b, e := boolean(s)
			return b, e
		case engine.Int2, engine.Int4, engine.Int8:
			bits := 64
			if oid == engine.Int2 {
				bits = 16
			} else if oid == engine.Int4 {
				bits = 32
			}
			n, e := strconv.ParseInt(s, 10, bits)
			if e != nil {
				return fail()
			}
			return n, nil
		case engine.Float4, engine.Float8:
			f, e := strconv.ParseFloat(s, 64)
			if e != nil {
				return fail()
			}
			return f, nil
		case engine.Bytea:
			if strings.HasPrefix(s, "\\x") || strings.HasPrefix(s, "\\X") {
				b, e := hex.DecodeString(s[2:])
				if e != nil {
					return fail()
				}
				if b == nil {
					b = []byte{}
				}
				return b, nil
			}
			return []byte(raw), nil
		default:
			if !utf8.Valid(raw) || bytes.IndexByte(raw, 0) >= 0 {
				return nil, sqlError("22021", "text parameter contains invalid UTF-8 or NUL")
			}
			return s, nil
		}
	}
	switch oid {
	case engine.Date:
		if len(raw) != 4 {
			return fail()
		}
		days := int64(int32(binary.BigEndian.Uint32(raw)))
		return time.Unix((10957+days)*86400, 0).UTC().Format("2006-01-02"), nil
	case engine.Timestamp, engine.TimestampTZ:
		if len(raw) != 8 {
			return fail()
		}
		micros := int64(binary.BigEndian.Uint64(raw))
		sec, rem := micros/1e6, micros%1e6
		return time.Unix(946684800+sec, rem*1000).UTC().Format("2006-01-02 15:04:05.000000"), nil
	case engine.UUID:
		if len(raw) != 16 {
			return fail()
		}
		h := hex.EncodeToString(raw)
		return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], nil
	case engine.Numeric:
		return nil, sqlError("42846", "binary numeric parameters are not supported; send text")
	case engine.JSON, engine.JSONB, engine.BPChar, engine.Name:
		return decodeValue(raw, engine.Text, 0)
	}

	switch oid {
	case engine.Bool:
		if len(raw) != 1 || raw[0] > 1 {
			return fail()
		}
		return raw[0] == 1, nil
	case engine.Int2:
		if len(raw) == 2 {
			return int64(int16(binary.BigEndian.Uint16(raw))), nil
		}
	case engine.Int4:
		if len(raw) == 4 {
			return int64(int32(binary.BigEndian.Uint32(raw))), nil
		}
	case engine.Int8:
		if len(raw) == 8 {
			return int64(binary.BigEndian.Uint64(raw)), nil
		}
	case engine.Float4:
		if len(raw) == 4 {
			return float64(math.Float32frombits(binary.BigEndian.Uint32(raw))), nil
		}
	case engine.Float8:
		if len(raw) == 8 {
			return math.Float64frombits(binary.BigEndian.Uint64(raw)), nil
		}
	case engine.Bytea:
		return append([]byte{}, raw...), nil
	case 0, engine.Text, engine.Varchar, engine.Unknown:
		return decodeValue(raw, engine.Text, 0)
	}
	return fail()
}
