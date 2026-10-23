package murmur

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
)

// Managed row identities. All SQL rewriting stays at the local SQL
// boundary; conflict versions and transition baselines live only in Spool.

// ManagedKeyColumn is the managed replication primary-key column.
const ManagedKeyColumn = "id_murmur_primary_key"

// sqliteConstraintNotNull is SQLITE_CONSTRAINT_NOTNULL.
const sqliteConstraintNotNull = 19 | (5 << 8)

func literal(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// Token is one SQL token with offsets, so rewriting preserves
// expressions and placeholder spelling.
type Token struct {
	Text   string
	Start  int
	End    int
	Quoted bool
}

func (t Token) is(s string) bool {
	return !t.Quoted && strings.EqualFold(t.Text, s)
}

// Tokens lexes SQL into tokens.
func Tokens(sql string) ([]Token, error) {
	b := []byte(sql)
	var out []Token
	i := 0
	for i < len(b) {
		if b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r' || b[i] == '\x0b' || b[i] == '\x0c' {
			i++
			continue
		}
		if i+1 < len(b) && b[i] == '-' && b[i+1] == '-' {
			for i < len(b) && b[i] != '\n' {
				i++
			}
			continue
		}
		if i+1 < len(b) && b[i] == '/' && b[i+1] == '*' {
			i += 2
			for i+1 < len(b) && !(b[i] == '*' && b[i+1] == '/') {
				i++
			}
			if i+1 >= len(b) {
				return nil, invalidErr("unterminated SQL comment")
			}
			i += 2
			continue
		}
		start := i
		if b[i] == '\'' || b[i] == '"' || b[i] == '`' || b[i] == '[' {
			opener := b[i]
			closer := opener
			if opener == '[' {
				closer = ']'
			}
			i++
			var text []byte
			closed := false
			for i < len(b) {
				if b[i] == closer {
					if closer != ']' && i+1 < len(b) && b[i+1] == closer {
						text = append(text, closer)
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				text = append(text, b[i])
				i++
			}
			if !closed {
				return nil, invalidErr("unterminated SQL quote")
			}
			if !utf8.Valid(text) {
				return nil, invalidErr("invalid SQL UTF-8")
			}
			out = append(out, Token{Text: string(text), Start: start, End: i, Quoted: true})
		} else {
			if isWordByte(b[i]) {
				i++
				for i < len(b) && isWordByte(b[i]) {
					i++
				}
			} else {
				i++
			}
			out = append(out, Token{Text: sql[start:i], Start: start, End: i})
		}
	}
	return out, nil
}

func isWordByte(c byte) bool {
	return c == '_' || c >= 128 || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isWordStart(c byte) bool { return isWordByte(c) }

// topIndices returns token indexes at parenthesis depth zero.
func topIndices(t []Token) []int {
	depth := 0
	var out []int
	for i := range t {
		if t[i].is(")") {
			depth--
		}
		if depth == 0 {
			out = append(out, i)
		}
		if t[i].is("(") {
			depth++
		}
	}
	return out
}

// Statements splits SQL on top-level semicolons.
func Statements(sql string) ([]string, error) {
	t, err := Tokens(sql)
	if err != nil {
		return nil, err
	}
	start := 0
	var out []string
	for _, i := range topIndices(t) {
		if t[i].is(";") {
			rest, err := Tokens(sql[start:t[i].Start])
			if err != nil {
				return nil, err
			}
			if len(rest) > 0 {
				out = append(out, sql[start:t[i].Start])
			}
			start = t[i].End
		}
	}
	rest, err := Tokens(sql[start:])
	if err != nil {
		return nil, err
	}
	if len(rest) > 0 {
		out = append(out, sql[start:])
	}
	return out, nil
}

// ApplicationColumns returns the application-key columns, or the
// primary-key columns when the table has no application key.
func ApplicationColumns(t *Table) []*Column {
	if k := t.ApplicationKey; k != nil {
		var out []*Column
		for _, n := range k.Columns {
			for ci := range t.Columns {
				if t.Columns[ci].Name == n {
					out = append(out, &t.Columns[ci])
				}
			}
		}
		return out
	}
	return PkColumns(t)
}

// InspectKey derives the application-key metadata for a table.
func InspectKey(c *sqlite.Conn, t *Table) (ApplicationKey, error) {
	var columns []string
	for _, col := range PkColumns(t) {
		columns = append(columns, col.Name)
	}
	key := ApplicationKey{
		Columns: columns,
		Strict:  false,
	}
	for range columns {
		key.Collations = append(key.Collations, "BINARY")
		key.Descending = append(key.Descending, false)
	}
	ts, err := Tokens(t.SQL)
	if err != nil {
		return key, err
	}
	for _, i := range topIndices(ts) {
		if ts[i].is(")") {
			for _, o := range ts[i+1:] {
				if o.is("STRICT") {
					key.Strict = true
				}
			}
			for w := i + 1; w+1 < len(ts); w++ {
				if ts[w].is("WITHOUT") && ts[w+1].is("ROWID") {
					key.WithoutRow = true
				}
			}
			break
		}
	}
	for p := 0; p+1 < len(ts); p++ {
		if !ts[p].is("PRIMARY") || !ts[p+1].is("KEY") {
			continue
		}
		end := p + 2
		if end < len(ts) && ts[end].is("(") {
			depth := 0
			for end < len(ts) {
				if ts[end].is("(") {
					depth++
				}
				if ts[end].is(")") {
					depth--
				}
				end++
				if depth == 0 {
					break
				}
			}
		} else if end < len(ts) && (ts[end].is("ASC") || ts[end].is("DESC")) {
			end++
		}
		if end+1 < len(ts) && ts[end].is("ON") && ts[end+1].is("CONFLICT") && end+2 < len(ts) {
			upper := strings.ToUpper(ts[end+2].Text)
			key.Conflict = &upper
		}
		break
	}
	rows, err := c.Query(fmt.Sprintf("PRAGMA index_list(%s)", quote(t.Name)))
	if err != nil {
		return key, sqlErr(err)
	}
	for _, r := range rows {
		name, origin := r[1].Str, r[3].Str
		if origin != "pk" {
			continue
		}
		entries, err := c.Query(fmt.Sprintf("PRAGMA index_xinfo(%s)", quote(name)))
		if err != nil {
			return key, sqlErr(err)
		}
		for _, e := range entries {
			if e[5].Int == 0 { // not a key column
				continue
			}
			colName := e[2].Str
			desc := e[3].Int != 0
			coll := e[4].Str
			for i, cn := range key.Columns {
				if cn == colName {
					key.Collations[i] = coll
					key.Descending[i] = desc
				}
			}
		}
	}
	return key, nil
}

var uuidNamespaceURL = [16]byte{0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}

func uuidV5(namespace [16]byte, name []byte) [16]byte {
	h := sha1.New()
	h.Write(namespace[:])
	h.Write(name)
	sum := h.Sum(nil)
	var out [16]byte
	copy(out[:], sum)
	out[6] = out[6]&0x0f | 0x50
	out[8] = out[8]&0x3f | 0x80
	return out
}

func uuidString(u [16]byte) string {
	hex := hexEncode(u[:])
	return hex[0:8] + "-" + hex[8:12] + "-" + hex[12:16] + "-" + hex[16:20] + "-" + hex[20:32]
}

func isIntegralReal(f float64) bool {
	return !math.IsNaN(f) && !math.IsInf(f, 0) &&
		f >= -9223372036854775808.0 && f < 9223372036854775808.0 &&
		float64(int64(f)) == f
}

// ManagedKeyUUID derives the stable managed identity for an
// application key: length-prefixed encoding with SQLite equality
// for numerics and normalized built-in text collations.
func ManagedKeyUUID(table string, collations []string, values []Value) (string, error) {
	if len(values) != len(collations) {
		return "", invalidErr("application key width mismatch")
	}
	bytes := []byte("murmur/replication-primary-key/v1\x00")
	part := func(value []byte) {
		var ln [8]byte
		binary.BigEndian.PutUint64(ln[:], uint64(len(value)))
		bytes = append(bytes, ln[:]...)
		bytes = append(bytes, value...)
	}
	part([]byte(strings.ToLower(table)))
	var ln [8]byte
	binary.BigEndian.PutUint64(ln[:], uint64(len(values)))
	bytes = append(bytes, ln[:]...)
	for i, value := range values {
		collation := collations[i]
		switch value.Tag {
		case ValueNull:
			return "", invalidErr("application primary key must be supplied and non-null")
		case ValueInteger:
			bytes = append(bytes, 1)
			var n [8]byte
			binary.BigEndian.PutUint64(n[:], uint64(value.I))
			bytes = append(bytes, n[:]...)
		case ValueReal:
			if isIntegralReal(value.F) {
				bytes = append(bytes, 1)
				var n [8]byte
				binary.BigEndian.PutUint64(n[:], uint64(int64(value.F)))
				bytes = append(bytes, n[:]...)
			} else {
				bytes = append(bytes, 2)
				var n [8]byte
				binary.BigEndian.PutUint64(n[:], math.Float64bits(value.F))
				bytes = append(bytes, n[:]...)
			}
		case ValueText:
			bytes = append(bytes, 3)
			var s string
			switch {
			case strings.EqualFold(collation, "NOCASE"):
				lower := make([]byte, len(value.S))
				for j := 0; j < len(value.S); j++ {
					c := value.S[j]
					if c >= 'A' && c <= 'Z' {
						c += 'a' - 'A'
					}
					lower[j] = c
				}
				for j, c := range lower {
					if c == 0 {
						for k := j; k < len(lower); k++ {
							lower[k] = 0
						}
						break
					}
				}
				if !utf8.Valid(lower) {
					return "", invalidErr("invalid NOCASE key")
				}
				s = string(lower)
			case strings.EqualFold(collation, "RTRIM"):
				s = strings.TrimRight(value.S, " ")
			case strings.EqualFold(collation, "BINARY"):
				s = value.S
			default:
				return "", invalidErr("unsupported application key collation")
			}
			part([]byte(s))
		case ValueBlob:
			bytes = append(bytes, 4)
			part(value.B)
		}
	}
	return uuidString(uuidV5(uuidNamespaceURL, bytes)), nil
}

// ValidateRow checks a row's managed key against its application key.
func ValidateRow(t *Table, row *RowState) error {
	key := t.ApplicationKey
	if key == nil {
		return nil
	}
	values := make([]Value, 0, len(key.Columns))
	for _, name := range key.Columns {
		var col *Column
		for ci := range t.Columns {
			if t.Columns[ci].Name == name {
				col = &t.Columns[ci]
				break
			}
		}
		if col == nil {
			return ErrSchemaMismatch
		}
		found := false
		for _, c := range row.Cells {
			if c.Column == col.ID {
				values = append(values, c.Value)
				found = true
				break
			}
		}
		if !found {
			return ErrSchemaMismatch
		}
	}
	uuidText, err := ManagedKeyUUID(t.Name, key.Collations, values)
	if err != nil {
		return err
	}
	var col *Column
	for ci := range t.Columns {
		if t.Columns[ci].Name == ManagedKeyColumn {
			col = &t.Columns[ci]
			break
		}
	}
	if col == nil {
		return ErrSchemaMismatch
	}
	if len(row.Key) != 1 || row.Key[0].Tag != ValueText || row.Key[0].S != uuidText {
		return invalidErr("invalid managed replication primary key")
	}
	for _, c := range row.Cells {
		if c.Column == col.ID {
			if c.Value.Tag != ValueText || c.Value.S != uuidText {
				return invalidErr("invalid managed replication primary key")
			}
			return nil
		}
	}
	return invalidErr("invalid managed replication primary key")
}

// valueFromSQLite converts a SQLite value, validating text encoding.
func valueFromSQLite(v sqlite.Value) (Value, error) {
	switch v.Type {
	case sqlite.Null:
		return NullValue(), nil
	case sqlite.Integer:
		return IntValue(v.Int), nil
	case sqlite.Float:
		return RealValue(v.Float), nil
	case sqlite.Text:
		if !utf8.ValidString(v.Str) {
			return Value{}, &Error{Kind: KindSql, Err: fmt.Errorf("invalid UTF-8 text")}
		}
		return TextValue(v.Str), nil
	case sqlite.Blob:
		return BlobValue(v.Blob), nil
	}
	return Value{}, invalidErr("bad SQLite value")
}

// RegisterKeyFunctions installs the managed-key SQL functions.
func RegisterKeyFunctions(c *sqlite.Conn) error {
	if err := c.CreateScalarFunction("_murmur_uuid", -1, true, uuidFunc); err != nil {
		return sqlErr(err)
	}
	if err := c.CreateRawScalarFunction("_murmur_affinity", 2, true, affinityFunc); err != nil {
		return invalidErr("cannot register key affinity function")
	}
	return nil
}

func uuidFunc(ctx *sqlite.FuncContext, args []sqlite.Value) {
	if len(args) < 3 || args[0].Type != sqlite.Text || args[1].Type != sqlite.Text {
		ctx.ResultError("invalid managed key arguments")
		return
	}
	var collations []string
	if err := json.Unmarshal([]byte(args[1].Str), &collations); err != nil {
		ctx.ResultError("invalid key collations")
		return
	}
	values := make([]Value, 0, len(args)-2)
	for _, a := range args[2:] {
		if a.Type == sqlite.Null {
			ctx.ResultErrorCode(sqliteConstraintNotNull,
				"every application primary-key component must be supplied and non-null")
			return
		}
		mv, err := valueFromSQLite(a)
		if err != nil {
			ctx.ResultError(err.Error())
			return
		}
		values = append(values, mv)
	}
	u, err := ManagedKeyUUID(args[0].Str, collations, values)
	if err != nil {
		ctx.ResultError(err.Error())
		return
	}
	ctx.ResultText(u)
}

func affinityFunc(ctx *sqlite.FuncContext, args []sqlite.RawValue) {
	if len(args) != 2 {
		ctx.ResultError("invalid affinity arguments")
		return
	}
	kind := args[0].Int64()
	v := args[1]
	if v.Type() == sqlite.TypeNull {
		ctx.ResultErrorCode(sqliteConstraintNotNull,
			"every application primary-key component must be supplied and non-null")
		return
	}
	if kind == 1 && (v.Type() == sqlite.TypeInteger || v.Type() == sqlite.TypeFloat) {
		ctx.ResultText(string(v.Text()))
		return
	}
	if kind >= 2 {
		numeric := v.NumericType()
		if numeric == sqlite.TypeInteger || numeric == sqlite.TypeFloat {
			if kind == 3 {
				ctx.ResultFloat(v.Float64())
				return
			}
			if numeric == sqlite.TypeFloat {
				if x := v.Float64(); isIntegralReal(x) {
					ctx.ResultInt(int64(x))
					return
				}
			}
		}
	}
	ctx.ResultRawValue(v)
}

func affinityOf(c *Column) int {
	s := strings.ToUpper(c.DeclaredType)
	switch {
	case strings.Contains(s, "INT"):
		return 2
	case strings.Contains(s, "CHAR") || strings.Contains(s, "CLOB") || strings.Contains(s, "TEXT"):
		return 1
	case s == "" || strings.Contains(s, "BLOB"):
		return 0
	case strings.Contains(s, "REAL") || strings.Contains(s, "FLOA") || strings.Contains(s, "DOUB"):
		return 3
	default:
		return 2
	}
}

// authError reports a managed-column access violation. It carries the real
// SQLITE_AUTH code so error mapping matches the authorizer path (42501).
func authError() *Error {
	return &Error{Kind: KindSql,
		Err: &sqlite.Error{Code: 23, Msg: "id_murmur_primary_key is managed and read-only"}}
}

// Rewrite transforms INSERT sources once, after binding, with
// materialized CTEs.
func Rewrite(sql string, schema *Schema) (string, error) {
	hasKey := false
	for i := range schema.Tables {
		if schema.Tables[i].ApplicationKey != nil {
			hasKey = true
			break
		}
	}
	if !hasKey {
		return sql, nil
	}
	trimmed := strings.TrimLeft(sql, " \t\n\r\v\f")
	word := trimmed
	if i := strings.IndexAny(trimmed, " \t\n\r\v\f"); i >= 0 {
		word = trimmed[:i]
	}
	for _, w := range []string{"SELECT", "UPDATE", "DELETE", "PRAGMA", "EXPLAIN"} {
		if strings.EqualFold(word, w) {
			return sql, nil
		}
	}
	t, err := Tokens(sql)
	if err != nil {
		return "", err
	}
	tops := topIndices(t)
	insert := -1
	for _, i := range tops {
		if t[i].is("INSERT") || t[i].is("REPLACE") {
			insert = i
			break
		}
	}
	if insert < 0 {
		return sql, nil
	}
	// Only the outer statement can be an INSERT.
	if insert != 0 && !(len(t) > 0 && t[0].is("WITH")) {
		return sql, nil
	}
	i := insert + 1
	if i < len(t) && t[i].is("OR") {
		i += 2
	}
	if i < len(t) && t[i].is("INTO") {
		i++
	}
	if i >= len(t) {
		return sql, nil
	}
	name := &t[i]
	i++
	if i < len(t) && t[i].is(".") {
		i++
		if i >= len(t) {
			return "", invalidErr("missing table name")
		}
		name = &t[i]
		i++
	}
	var table *Table
	for ti := range schema.Tables {
		if strings.EqualFold(schema.Tables[ti].Name, name.Text) && schema.Tables[ti].ApplicationKey != nil {
			table = &schema.Tables[ti]
			break
		}
	}
	if table == nil {
		return sql, nil
	}
	key := table.ApplicationKey
	if i < len(t) && t[i].is("AS") {
		i += 2
	}
	var columns []string
	if i < len(t) && t[i].is("(") {
		i++
		for i < len(t) && !t[i].is(")") {
			columns = append(columns, t[i].Text)
			i++
			if i < len(t) && t[i].is(",") {
				i++
			} else if !(i < len(t) && t[i].is(")")) {
				return "", invalidErr("invalid INSERT column list")
			}
		}
		i++
	} else {
		for ci := range table.Columns {
			if table.Columns[ci].Name != ManagedKeyColumn {
				columns = append(columns, table.Columns[ci].Name)
			}
		}
	}
	for _, n := range columns {
		if strings.EqualFold(n, ManagedKeyColumn) {
			return "", authError()
		}
	}
	for _, n := range key.Columns {
		found := false
		for _, c := range columns {
			if strings.EqualFold(c, n) {
				found = true
				break
			}
		}
		if !found {
			return "", &Error{Kind: KindSql,
				Err: fmt.Errorf("every application primary-key component must be supplied and non-null")}
		}
	}
	if i >= len(t) {
		return "", invalidErr("missing INSERT source")
	}
	source := t[i].Start
	if t[i].is("DEFAULT") {
		return "", &Error{Kind: KindSql,
			Err: fmt.Errorf("every application primary-key component must be supplied and non-null")}
	}
	suffix := len(sql)
	for _, j := range tops {
		if j < i {
			continue
		}
		if t[j].is("RETURNING") || t[j].is(";") ||
			(t[j].is("ON") && j+1 < len(t) && t[j+1].is("CONFLICT")) {
			suffix = t[j].Start
			break
		}
	}
	ordinal := 0
	var cte, normalized string
	for {
		a := fmt.Sprintf("_murmur_insert_input_%d", ordinal)
		b := fmt.Sprintf("_murmur_insert_values_%d", ordinal)
		clash := false
		for _, x := range t {
			if strings.EqualFold(x.Text, a) || strings.EqualFold(x.Text, b) {
				clash = true
				break
			}
		}
		if !clash {
			cte, normalized = a, b
			break
		}
		ordinal++
	}
	names := make([]string, len(columns))
	for j := range columns {
		names[j] = fmt.Sprintf("v%d", j)
	}
	projection := make([]string, len(columns))
	for j, n := range columns {
		var c *Column
		for ci := range table.Columns {
			if strings.EqualFold(table.Columns[ci].Name, n) {
				c = &table.Columns[ci]
				break
			}
		}
		if c == nil {
			return "", invalidErr("unknown INSERT column")
		}
		inkey := false
		for _, k := range key.Columns {
			if k == c.Name {
				inkey = true
				break
			}
		}
		if inkey {
			projection[j] = fmt.Sprintf("_murmur_affinity(%d,v%d) AS v%d", affinityOf(c), j, j)
		} else {
			projection[j] = fmt.Sprintf("v%d", j)
		}
	}
	args := make([]string, len(key.Columns))
	for j, n := range key.Columns {
		for k, c := range columns {
			if strings.EqualFold(c, n) {
				args[j] = fmt.Sprintf("v%d", k)
				break
			}
		}
	}
	var prefix string
	if insert == 0 {
		prefix = "WITH "
	} else {
		prefix = strings.TrimRight(sql[:t[insert].Start], " \t\n\r\v\f") + ","
	}
	head := sql[t[insert].Start:name.End]
	aliasPos := -1
	for x, x2 := range t {
		if x2.Start == name.Start {
			aliasPos = x + 1
			break
		}
	}
	alias := ""
	if aliasPos >= 0 && aliasPos < len(t) && t[aliasPos].is("AS") && aliasPos+1 < len(t) {
		alias = fmt.Sprintf(" AS %s", quote(t[aliasPos+1].Text))
	}
	collJSON, _ := json.Marshal(key.Collations)
	return fmt.Sprintf("%s%s(%s) AS MATERIALIZED (%s), %s AS MATERIALIZED (SELECT %s FROM %s) %s%s (%s,%s) SELECT %s,_murmur_uuid(%s,%s,%s) FROM %s WHERE true %s",
		prefix, cte, strings.Join(names, ","), sql[source:suffix],
		normalized, strings.Join(projection, ","), cte,
		head, alias, strings.Join(columns, ","), quote(ManagedKeyColumn),
		strings.Join(names, ","), literal(table.Name), literal(string(collJSON)), strings.Join(args, ","),
		normalized, sql[suffix:]), nil
}

// Definition rebuilds CREATE TABLE without losing defaults,
// collations, or non-key columns.
func Definition(t *Table, key *ApplicationKey, enable bool) (string, error) {
	tokens, err := Tokens(t.SQL)
	if err != nil {
		return "", err
	}
	open := -1
	for i, x := range tokens {
		if x.is("(") {
			open = i
			break
		}
	}
	if open < 0 {
		return "", ErrSchemaMismatch
	}
	depth := 1
	start := open + 1
	var definitions []string
	closeIdx := -1
	for i := open + 1; i < len(tokens); i++ {
		if tokens[i].is("(") {
			depth++
		}
		if tokens[i].is(")") {
			depth--
		}
		if depth == 0 || (depth == 1 && tokens[i].is(",")) {
			part := tokens[start:i]
			if len(part) > 0 {
				tablePK := part[0].is("PRIMARY") || part[0].is("CONSTRAINT")
				managedUnique := !enable && part[0].is("UNIQUE")
				if !tablePK && !managedUnique && !strings.EqualFold(part[0].Text, ManagedKeyColumn) {
					text := t.SQL[part[0].Start:part[len(part)-1].End]
					pk := -1
					for w := 0; w+1 < len(part); w++ {
						if part[w].is("PRIMARY") && part[w+1].is("KEY") {
							pk = w
							break
						}
					}
					if pk >= 0 {
						end := pk + 2
						if end < len(part) && (part[end].is("ASC") || part[end].is("DESC")) {
							end++
						}
						if end < len(part) && part[end].is("ON") {
							end += 3
						}
						begin := pk
						if pk >= 2 && part[pk-2].is("CONSTRAINT") {
							begin = pk - 2
						}
						cutEnd := part[len(part)-1].End
						if end < len(part) {
							cutEnd = part[end].Start
						}
						rel0 := part[begin].Start - part[0].Start
						rel1 := cutEnd - part[0].Start
						text = text[:rel0] + text[rel1:]
					}
					if enable {
						inKey := false
						for _, n := range key.Columns {
							if strings.EqualFold(n, part[0].Text) {
								inKey = true
								break
							}
						}
						if inKey {
							kws := Keywords(text)
							hasNotNull := false
							for w := 0; w+1 < len(kws); w++ {
								if kws[w] == "NOT" && kws[w+1] == "NULL" {
									hasNotNull = true
									break
								}
							}
							if !hasNotNull {
								text += " NOT NULL"
							}
						}
					}
					definitions = append(definitions, text)
				}
			}
			start = i + 1
			if depth == 0 {
				closeIdx = i
				break
			}
		}
	}
	if closeIdx < 0 {
		return "", ErrSchemaMismatch
	}
	cols := make([]string, len(key.Columns))
	for i, n := range key.Columns {
		op := "ASC"
		if key.Descending[i] {
			op = "DESC"
		}
		cols[i] = fmt.Sprintf("%s COLLATE %s %s", quote(n), quote(key.Collations[i]), op)
	}
	if enable {
		definitions = append(definitions, fmt.Sprintf("%s TEXT NOT NULL PRIMARY KEY", quote(ManagedKeyColumn)))
		definitions = append(definitions, fmt.Sprintf("UNIQUE (%s)%s", strings.Join(cols, ","), Conflict(key)))
	} else {
		definitions = append(definitions, fmt.Sprintf("PRIMARY KEY (%s)%s", strings.Join(cols, ","), Conflict(key)))
	}
	tail := strings.TrimRight(t.SQL[tokens[closeIdx].End:], ";")
	return fmt.Sprintf("CREATE TABLE %s (%s)%s", quote(t.Name), strings.Join(definitions, ","), tail), nil
}

// ColumnDefinitions extracts column definitions from SQLite's own
// schema SQL, retaining defaults and collations during canonical
// schema exchange. Table constraints are rebuilt from validated key
// metadata.
func ColumnDefinitions(sql string) (map[string]string, error) {
	ts, err := Tokens(sql)
	if err != nil {
		return nil, err
	}
	open := -1
	for i, x := range ts {
		if x.is("(") {
			open = i
			break
		}
	}
	if open < 0 {
		return nil, ErrSchemaMismatch
	}
	depth := 1
	start := open + 1
	out := make(map[string]string)
	for i := open + 1; i < len(ts); i++ {
		if ts[i].is("(") {
			depth++
		}
		if ts[i].is(")") {
			depth--
		}
		if depth == 0 || (depth == 1 && ts[i].is(",")) {
			part := ts[start:i]
			if len(part) > 0 {
				name := part[0]
				constraint := false
				for _, s := range []string{"PRIMARY", "UNIQUE", "CONSTRAINT", "CHECK", "FOREIGN"} {
					if name.is(s) {
						constraint = true
						break
					}
				}
				if !constraint {
					for _, x := range part {
						if x.is(";") {
							return nil, ErrSchemaMismatch
						}
					}
					text := sql[name.Start:part[len(part)-1].End]
					pk := -1
					for w := 0; w+1 < len(part); w++ {
						if part[w].is("PRIMARY") && part[w+1].is("KEY") {
							pk = w
							break
						}
					}
					if pk >= 0 {
						end := pk + 2
						if end < len(part) && (part[end].is("ASC") || part[end].is("DESC")) {
							end++
						}
						if end < len(part) && part[end].is("ON") {
							end += 3
						}
						begin := pk
						if pk >= 2 && part[pk-2].is("CONSTRAINT") {
							begin = pk - 2
						}
						cutEnd := part[len(part)-1].End
						if end < len(part) {
							cutEnd = part[end].Start
						}
						rel0 := part[begin].Start - name.Start
						rel1 := cutEnd - name.Start
						text = text[:rel0] + text[rel1:]
					}
					if _, dup := out[name.Text]; dup {
						return nil, ErrSchemaMismatch
					}
					out[name.Text] = strings.TrimSpace(text)
				}
			}
			start = i + 1
			if depth == 0 {
				return out, nil
			}
		}
	}
	return nil, ErrSchemaMismatch
}

// ValidateTable checks application-key metadata against the table.
func ValidateTable(t *Table) error {
	key := t.ApplicationKey
	if key == nil {
		return nil
	}
	if len(key.Columns) == 0 || len(key.Columns) != len(key.Collations) ||
		len(key.Columns) != len(key.Descending) {
		return ErrSchemaMismatch
	}
	seen := make(map[string]bool, len(key.Columns))
	for _, n := range key.Columns {
		seen[n] = true
	}
	if len(seen) != len(key.Columns) {
		return ErrSchemaMismatch
	}
	for _, n := range key.Columns {
		if strings.EqualFold(n, ManagedKeyColumn) {
			return ErrSchemaMismatch
		}
		found := false
		for ci := range t.Columns {
			if t.Columns[ci].Name == n && t.Columns[ci].NotNull {
				found = true
				break
			}
		}
		if !found {
			return ErrSchemaMismatch
		}
	}
	if key.Conflict != nil {
		switch *key.Conflict {
		case "ROLLBACK", "ABORT", "FAIL", "IGNORE", "REPLACE":
		default:
			return ErrSchemaMismatch
		}
	}
	for _, c := range key.Collations {
		if !strings.EqualFold(c, "BINARY") && !strings.EqualFold(c, "NOCASE") && !strings.EqualFold(c, "RTRIM") {
			return ErrSchemaMismatch
		}
	}
	pk := PkColumns(t)
	if len(pk) != 1 || pk[0].Name != ManagedKeyColumn ||
		!strings.EqualFold(pk[0].DeclaredType, "TEXT") || !pk[0].NotNull {
		return ErrSchemaMismatch
	}
	return nil
}

// Conflict renders the ON CONFLICT clause suffix.
func Conflict(key *ApplicationKey) string {
	if key.Conflict == nil {
		return ""
	}
	return " ON CONFLICT " + *key.Conflict
}

// ResolveKey computes the managed identity for application key values.
func ResolveKey(c *sqlite.Conn, t *Table, values []Value) ([]Value, error) {
	columns := ApplicationColumns(t)
	if len(values) != len(columns) {
		return nil, invalidErr("application primary-key width or value mismatch")
	}
	for _, v := range values {
		if v.IsNull() {
			return nil, invalidErr("application primary-key width or value mismatch")
		}
	}
	key := t.ApplicationKey
	if key == nil {
		return values, nil
	}
	args := make([]string, len(columns))
	for i, col := range columns {
		args[i] = fmt.Sprintf("_murmur_affinity(%d,?%d)", affinityOf(col), i+1)
	}
	collJSON, _ := json.Marshal(key.Collations)
	query := fmt.Sprintf("SELECT _murmur_uuid(%s,%s,%s)", literal(t.Name), literal(string(collJSON)), strings.Join(args, ","))
	bind := make([]any, len(values))
	for i, v := range values {
		bind[i] = sqliteBind(v)
	}
	row, err := c.QueryRow(query, bind...)
	if err != nil {
		return nil, sqlErr(err)
	}
	if row[0].Type != sqlite.Text {
		return nil, invalidErr("managed key resolution failed")
	}
	return []Value{TextValue(row[0].Str)}, nil
}

// sqliteBind converts a model value to a SQLite binding.
func sqliteBind(v Value) any {
	switch v.Tag {
	case ValueNull:
		return nil
	case ValueInteger:
		return v.I
	case ValueReal:
		return v.F
	case ValueText:
		return v.S
	case ValueBlob:
		return v.B
	}
	return nil
}

// ReferencesManagedColumn reports whether SQL names the managed key column.
func ReferencesManagedColumn(sql string) (bool, error) {
	t, err := Tokens(sql)
	if err != nil {
		return false, err
	}
	b := []byte(sql)
	for _, x := range t {
		if strings.EqualFold(x.Text, ManagedKeyColumn) && b[x.Start] != '\'' {
			return true, nil
		}
	}
	return false, nil
}

// ApplicationPredicate renders the key equality predicate.
func ApplicationPredicate(t *Table) string {
	columns := ApplicationColumns(t)
	parts := make([]string, len(columns))
	for i, c := range columns {
		collation := ""
		if t.ApplicationKey != nil {
			collation = fmt.Sprintf(" COLLATE %s", quote(t.ApplicationKey.Collations[i]))
		}
		parts[i] = fmt.Sprintf("%s%s IS ?", quote(c.Name), collation)
	}
	return strings.Join(parts, " AND ")
}
