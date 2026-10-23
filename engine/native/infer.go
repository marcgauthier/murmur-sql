package native

import (
	"strconv"
	"strings"

	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/engine"
)

type sqlToken struct {
	text            string
	param           int
	literal, quoted bool
}

func upper(s string) string { return strings.ToUpper(s) }
func lex(sql string) []sqlToken {
	raw, e := murmur.Tokens(sql)
	if e != nil {
		return nil
	}
	var out []sqlToken
	for i := 0; i < len(raw); i++ {
		r := raw[i]
		if !r.Quoted && (r.Text == "?" || r.Text == "$") && i+1 < len(raw) && raw[i+1].Start == r.End {
			if n, e := strconv.Atoi(raw[i+1].Text); e == nil && n > 0 {
				out = append(out, sqlToken{param: n})
				i++
				continue
			}
		}
		out = append(out, sqlToken{text: r.Text, quoted: r.Quoted, literal: r.Quoted && r.Start < len(sql) && sql[r.Start] == '\''})
	}
	return out
}
func word(t sqlToken, s string) bool {
	return !t.quoted && !t.literal && t.param == 0 && strings.EqualFold(t.text, s)
}
func ident(t sqlToken) bool {
	if t.literal || t.param != 0 || t.text == "" {
		return false
	}
	if t.quoted {
		return true
	}
	c := t.text[0]
	return c == '_' || c > 127 || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}
func declaredType(s string, parameter bool) uint32 {
	s = upper(s)
	if parameter && (s == "BOOL" || s == "BOOLEAN") || s == "MURMUR_BOOL" {
		return engine.Bool
	}
	switch {
	case strings.Contains(s, "INT"):
		return engine.Int8
	case strings.Contains(s, "CHAR"), strings.Contains(s, "CLOB"), strings.Contains(s, "TEXT"):
		return engine.Text
	case strings.Contains(s, "BLOB"):
		return engine.Bytea
	case strings.Contains(s, "REAL"), strings.Contains(s, "FLOA"), strings.Contains(s, "DOUB"):
		return engine.Float8
	}
	return engine.Text
}

type scopedTable struct {
	table murmur.Table
	names []string
}

func factorStop(s string) bool {
	return strings.Contains(" WHERE GROUP ORDER LIMIT OFFSET SET VALUES ON USING JOIN INNER LEFT RIGHT FULL OUTER CROSS NATURAL UNION EXCEPT INTERSECT RETURNING WINDOW HAVING FETCH SELECT INSERT UPDATE DELETE TABLE AS AND OR NOT NULL IS IN LIKE MATCH BETWEEN EXISTS CASE WHEN THEN ELSE END BY ASC DESC ", " "+upper(s)+" ")
}
func scope(tokens []sqlToken, schema *murmur.Schema) []scopedTable {
	var out []scopedTable
	for i := 0; i < len(tokens); i++ {
		if !(word(tokens[i], "FROM") || word(tokens[i], "JOIN") || word(tokens[i], "UPDATE") || word(tokens[i], "INTO") || i == 0 && word(tokens[i], "TABLE")) {
			continue
		}
		cursor := i + 1
		for cursor < len(tokens) {
			if !ident(tokens[cursor]) {
				break
			}
			name := tokens[cursor].text
			cursor++
			if cursor+1 < len(tokens) && tokens[cursor].text == "." && ident(tokens[cursor+1]) {
				name = tokens[cursor+1].text
				cursor += 2
			}
			names := []string{name}
			if cursor < len(tokens) && word(tokens[cursor], "AS") {
				cursor++
			}
			if cursor < len(tokens) && ident(tokens[cursor]) && !factorStop(tokens[cursor].text) {
				names = append(names, tokens[cursor].text)
				cursor++
			}
			for _, table := range schema.Tables {
				if strings.EqualFold(table.Name, name) {
					out = append(out, scopedTable{table: table, names: names})
					break
				}
			}
			if cursor < len(tokens) && tokens[cursor].text == "," {
				cursor++
				continue
			}
			break
		}
	}
	return out
}
func columnRef(tokens []sqlToken, i int) (qualifier, name string, next int) {
	if i < 0 || i >= len(tokens) || !ident(tokens[i]) {
		return "", "", i
	}
	name = tokens[i].text
	next = i + 1
	if next+1 < len(tokens) && tokens[next].text == "." && ident(tokens[next+1]) {
		qualifier = name
		name = tokens[next+1].text
		next += 2
	}
	return
}
func resolve(tables []scopedTable, qualifier, name string) *murmur.Column {
	var found *murmur.Column
	for _, table := range tables {
		if qualifier != "" {
			match := false
			for _, alias := range table.names {
				match = match || strings.EqualFold(alias, qualifier)
			}
			if !match {
				continue
			}
		}
		for i := range table.table.Columns {
			c := &table.table.Columns[i]
			if strings.EqualFold(c.Name, name) {
				if found != nil && qualifier == "" {
					return nil
				}
				found = c
			}
		}
	}
	return found
}
func comparison(tokens []sqlToken, i int) int {
	if i >= len(tokens) {
		return i
	}
	switch tokens[i].text {
	case "=":
		return i + 1
	case "<", ">", "!":
		if i+1 < len(tokens) && (tokens[i+1].text == "=" || tokens[i].text == "<" && tokens[i+1].text == ">") {
			return i + 2
		}
		if tokens[i].text != "!" {
			return i + 1
		}
	}
	return i
}
func inferParameters(schema *murmur.Schema, sql string, hints []uint32) []uint32 {
	tokens := lex(sql)
	tables := scope(tokens, schema)
	out := append([]uint32{}, hints...)
	assign := func(n int, oid uint32) {
		if n > 0 && n <= len(out) && (out[n-1] == 0 || out[n-1] == engine.Unknown) {
			out[n-1] = oid
		}
	}
	for i, t := range tokens {
		q, name, next := columnRef(tokens, i)
		if name != "" {
			col := resolve(tables, q, name)
			after := comparison(tokens, next)
			if col != nil && after > next && after < len(tokens) {
				assign(tokens[after].param, declaredType(col.DeclaredType, true))
			}
			if next < len(tokens) && (word(tokens[next], "LIKE") || word(tokens[next], "MATCH")) && next+1 < len(tokens) {
				assign(tokens[next+1].param, engine.Text)
			}
			if col != nil && next < len(tokens) {
				oid := declaredType(col.DeclaredType, true)
				if word(tokens[next], "BETWEEN") && next+3 < len(tokens) && word(tokens[next+2], "AND") {
					assign(tokens[next+1].param, oid)
					assign(tokens[next+3].param, oid)
				}
				if word(tokens[next], "IN") && next+1 < len(tokens) && tokens[next+1].text == "(" {
					depth := 0
					for j := next + 2; j < len(tokens); j++ {
						if tokens[j].text == ")" {
							if depth == 0 {
								break
							}
							depth--
						} else if tokens[j].text == "(" {
							depth++
						} else if depth == 0 {
							assign(tokens[j].param, oid)
						}
					}
				}
			}
		}
		if t.param > 0 {
			after := comparison(tokens, i+1)
			if after > i+1 {
				q, name, _ := columnRef(tokens, after)
				if col := resolve(tables, q, name); col != nil {
					assign(t.param, declaredType(col.DeclaredType, true))
				}
			}
		}
		if word(t, "LIMIT") || word(t, "OFFSET") {
			for j := i + 1; j < len(tokens); j++ {
				if tokens[j].param > 0 {
					assign(tokens[j].param, engine.Int8)
				} else if tokens[j].text != "," {
					break
				}
			}
		}
	}
	// Positional INSERT VALUES, including explicit column lists and multiple tuples.
	for i := 0; i+2 < len(tokens); i++ {
		if !word(tokens[i], "INSERT") {
			continue
		}
		j := i + 1
		for j < len(tokens) && !word(tokens[j], "INTO") {
			j++
		}
		if j+1 >= len(tokens) {
			continue
		}
		_, tableName, j := columnRef(tokens, j+1)
		var table *murmur.Table
		for k := range schema.Tables {
			if strings.EqualFold(schema.Tables[k].Name, tableName) {
				table = &schema.Tables[k]
				break
			}
		}
		if table == nil {
			continue
		}
		var columns []murmur.Column
		if j < len(tokens) && tokens[j].text == "(" {
			j++
			for j < len(tokens) && tokens[j].text != ")" {
				if ident(tokens[j]) {
					for _, c := range table.Columns {
						if strings.EqualFold(c.Name, tokens[j].text) {
							columns = append(columns, c)
							break
						}
					}
				}
				j++
			}
			j++
		} else {
			columns = table.Columns
		}
		for j < len(tokens) && !word(tokens[j], "VALUES") {
			j++
		}
		j++
		for j < len(tokens) && tokens[j].text == "(" {
			j++
			position, depth, start := 0, 0, j
			for j < len(tokens) {
				t := tokens[j]
				if t.text == "(" {
					depth++
				}
				if t.text == ")" && depth > 0 {
					depth--
					j++
					continue
				}
				boundary := depth == 0 && (t.text == "," || t.text == ")")
				if boundary {
					if j == start+1 && position < len(columns) {
						assign(tokens[start].param, declaredType(columns[position].DeclaredType, true))
					}
					position++
					start = j + 1
					if t.text == ")" {
						j++
						break
					}
				}
				j++
			}
			if j < len(tokens) && tokens[j].text == "," {
				j++
			}
		}
	}
	for i, oid := range out {
		if oid == 0 || oid == engine.Unknown {
			out[i] = engine.Text
		}
	}
	return out
}
func querySchema(schema *murmur.Schema) *murmur.Schema {
	out := *schema
	out.Tables = append([]murmur.Table{}, schema.Tables...)
	for _, t := range schema.Tables {
		var fts []murmur.Column
		for _, c := range t.Columns {
			if c.Vector != nil {
				out.Tables = append(out.Tables, murmur.Table{Name: murmur.VectorIndexName(&t, &c), Columns: []murmur.Column{{Name: c.Name, DeclaredType: "BLOB"}, {Name: "rowid", DeclaredType: "INTEGER"}, {Name: "k", DeclaredType: "INTEGER"}, {Name: "distance", DeclaredType: "REAL"}}})
			}
			if c.Fts {
				fts = append(fts, murmur.Column{Name: c.Name, DeclaredType: "TEXT"})
			}
		}
		if len(fts) > 0 {
			fts = append(fts, murmur.Column{Name: "rowid", DeclaredType: "INTEGER"})
			out.Tables = append(out.Tables, murmur.Table{Name: murmur.SchemaIndexName(&t), Columns: fts})
		}
	}
	return &out
}
func inferColumns(schema *murmur.Schema, sql string, names []string, params []uint32) []engine.Column {
	tokens := lex(sql)
	tables := scope(tokens, schema)
	var expressions [][]sqlToken
	start, depth := -1, 0
	for i, t := range tokens {
		if start < 0 {
			if word(t, "SELECT") || word(t, "RETURNING") {
				start = i + 1
			}
			continue
		}
		if t.text == "(" {
			depth++
		}
		if t.text == ")" {
			depth--
		}
		if depth == 0 && (t.text == "," || word(t, "FROM") || word(t, "WHERE") || word(t, "ORDER") || word(t, "LIMIT")) {
			if i > start {
				expressions = append(expressions, tokens[start:i])
			}
			start = i + 1
			if t.text != "," {
				start = -2
				break
			}
		}
	}
	if start >= 0 && start < len(tokens) {
		expressions = append(expressions, tokens[start:])
	}
	var expanded []uint32
	for _, expr := range expressions {
		if len(expr) == 0 {
			continue
		}
		if word(expr[0], "DISTINCT") {
			expr = expr[1:]
			if len(expr) == 0 {
				continue
			}
		}
		if expr[0].text == "*" || len(expr) >= 3 && expr[1].text == "." && expr[2].text == "*" {
			for _, t := range tables {
				match := expr[0].text == "*"
				for _, n := range t.names {
					match = match || strings.EqualFold(n, expr[0].text)
				}
				if match {
					for _, c := range t.table.Columns {
						expanded = append(expanded, declaredType(c.DeclaredType, false))
					}
				}
			}
			continue
		}
		oid := uint32(0)
		q, name, next := columnRef(expr, 0)
		if name != "" && (next == len(expr) || next < len(expr) && word(expr[next], "AS") || next == len(expr)-1 && ident(expr[next])) {
			if c := resolve(tables, q, name); c != nil {
				oid = declaredType(c.DeclaredType, false)
			}
		}
		if expr[0].param > 0 && expr[0].param <= len(params) {
			oid = storageType(params[expr[0].param-1])
		}
		if expr[0].literal {
			oid = engine.Text
		}
		if word(expr[0], "COUNT") || word(expr[0], "LENGTH") || word(expr[0], "CHANGES") || word(expr[0], "LAST_INSERT_ROWID") {
			oid = engine.Int8
		}
		expanded = append(expanded, oid)
	}
	out := make([]engine.Column, len(names))
	for i, name := range names {
		oid := uint32(0)
		if len(expanded) == len(names) {
			oid = expanded[i]
		}
		if oid == 0 {
			if c := resolve(tables, "", name); c != nil {
				oid = declaredType(c.DeclaredType, false)
			}
		}
		out[i] = engine.Column{Name: name, OID: oid}
	}
	return out
}

func storageType(oid uint32) uint32 {
	switch oid {
	case engine.Bool, engine.Int2, engine.Int4, engine.Int8:
		return engine.Int8
	case engine.Float4, engine.Float8, engine.Numeric:
		return engine.Float8
	case engine.Bytea:
		return engine.Bytea
	default:
		return engine.Text
	}
}
