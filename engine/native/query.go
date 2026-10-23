package native

import (
	"context"
	"fmt"
	"strings"

	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/engine"
)

func first(sql string) string {
	tokens := lex(sql)
	if len(tokens) == 0 {
		return ""
	}
	return upper(tokens[0].text)
}
func dummy(oid uint32) any {
	switch oid {
	case engine.Bool:
		return false
	case engine.Int8, engine.Int4, engine.Int2:
		return int64(0)
	case engine.Float8, engine.Float4, engine.Numeric:
		return float64(0)
	case engine.Bytea:
		return []byte{}
	}
	return ""
}
func probeSQL(sql string) string {
	switch first(sql) {
	case "SELECT", "WITH", "VALUES":
		return "SELECT * FROM (" + strings.TrimSuffix(strings.TrimSpace(sql), ";") + ") LIMIT 1"
	case "PRAGMA":
		tokens := lex(sql)
		if len(tokens) > 1 {
			switch upper(tokens[1].text) {
			case "TABLE_INFO", "TABLE_XINFO", "INDEX_LIST", "INDEX_INFO":
				return sql
			}
		}
	}
	return ""
}
func (d *database) Describe(ctx context.Context, sql string, hints []uint32) (engine.Description, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if e := d.ready(ctx); e != nil {
		return engine.Description{}, e
	}
	return d.describe(ctx, sql, hints)
}
func (d *database) describe(ctx context.Context, sql string, hints []uint32) (engine.Description, error) {
	schema, e := d.db.Schema()
	if e != nil {
		return engine.Description{}, translate(e)
	}
	schema = querySchema(schema)
	// Views and TEMP search tables are query metadata, not managed schema.
	tokens := lex(sql)
	for i, t := range tokens {
		if !(word(t, "FROM") || word(t, "JOIN")) || i+1 >= len(tokens) {
			continue
		}
		_, name, _ := columnRef(tokens, i+1)
		exists := false
		for _, table := range schema.Tables {
			exists = exists || strings.EqualFold(table.Name, name)
		}
		if exists || name == "" {
			continue
		}
		var table murmur.Table
		table.Name = name
		_, _ = d.db.Read(func(s *murmur.Sql) (any, error) {
			rows, e := s.Query("PRAGMA table_xinfo(" + quote(name) + ")")
			if e != nil {
				return nil, e
			}
			for _, row := range rows {
				if len(row) >= 3 {
					table.Columns = append(table.Columns, murmur.Column{Name: row[1].Str, DeclaredType: row[2].Str})
				}
			}
			return nil, nil
		})
		if len(table.Columns) > 0 {
			schema.Tables = append(schema.Tables, table)
		}
	}
	params := inferParameters(schema, sql, hints)
	// DDL has no result columns and must not be prepared here: preparing
	// runs the authorizer, which only admits DDL under migration access.
	// Extended-protocol clients describe before executing, so describe
	// must succeed and let execute route to Migrate.
	switch first(sql) {
	case "CREATE", "ALTER", "DROP", "REINDEX":
		return engine.Description{Parameters: params}, nil
	}
	bindings := make([]any, len(params))
	for i, oid := range params {
		bindings[i] = dummy(oid)
	}
	var columns []engine.Column
	prepare := func(s *murmur.Sql) (any, error) {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		stmt, e := s.Prepare(sql)
		if e != nil {
			return nil, e
		}
		defer stmt.Close()
		names := make([]string, stmt.ColumnCount())
		for i := range names {
			names[i] = stmt.ColumnName(i)
		}
		columns = inferColumns(schema, sql, names, params)
		if probe := probeSQL(sql); probe != "" {
			sample, e := s.Prepare(probe)
			if e == nil {
				defer sample.Close()
				if e = sample.Bind(bindings...); e == nil {
					if row, e := sample.Step(); e == nil && row {
						for i, v := range sample.Columns() {
							if i >= len(columns) || columns[i].OID != 0 {
								continue
							}
							switch int(v.Type) {
							case 1:
								columns[i].OID = engine.Int8
							case 2:
								columns[i].OID = engine.Float8
							case 3:
								columns[i].OID = engine.Text
							case 4:
								columns[i].OID = engine.Bytea
							}
						}
					}
				}
			}
		}
		for i := range columns {
			if columns[i].OID == 0 {
				columns[i].OID = engine.Text
			}
		}
		return nil, nil
	}
	if first(sql) == "SELECT" {
		_, e = d.db.Read(prepare)
	} else {
		_, e = d.db.Transaction(prepare)
	}
	return engine.Description{Parameters: params, Columns: columns}, translate(e)
}
func (d *database) Execute(ctx context.Context, sql string, params []engine.Value) (engine.Result, error) {
	return d.execute(ctx, sql, params, nil)
}
func (d *database) ExecuteDescribed(ctx context.Context, sql string, params []engine.Value, description engine.Description) (engine.Result, error) {
	return d.execute(ctx, sql, params, &description)
}
func (d *database) execute(ctx context.Context, sql string, params []engine.Value, expected *engine.Description) (engine.Result, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if e := d.ready(ctx); e != nil {
		return engine.Result{}, e
	}
	var desc engine.Description
	var e error
	if expected != nil {
		desc = *expected
	} else {
		hints := make([]uint32, len(params))
		desc, e = d.describe(ctx, sql, hints)
		if e != nil {
			return engine.Result{}, e
		}
	}
	out := engine.Result{Columns: desc.Columns}
	bindings := make([]any, len(params))
	copy(bindings, params)
	run := func(s *murmur.Sql) (any, error) {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		stmt, e := s.Prepare(sql)
		if e != nil {
			return nil, e
		}
		defer stmt.Close()
		if e = stmt.Bind(bindings...); e != nil {
			return nil, e
		}
		if stmt.ColumnCount() != len(out.Columns) {
			return nil, userError("0A000", "schema changed while preparing query")
		}
		size := 0
		for {
			if e = ctx.Err(); e != nil {
				return nil, e
			}
			row, e := stmt.Step()
			if e != nil {
				return nil, e
			}
			if !row {
				break
			}
			values := make([]engine.Value, stmt.ColumnCount())
			size += len(values)*16 + 24
			for i, v := range stmt.Columns() {
				switch int(v.Type) {
				case 0:
					values[i] = nil
				case 1:
					values[i] = v.Int
				case 2:
					values[i] = v.Float
				case 3:
					values[i] = v.Str
					size += len(v.Str)
				case 4:
					values[i] = append([]byte{}, v.Blob...)
					size += len(v.Blob)
				}
			}
			if size > 64<<20 {
				return nil, userError("54000", "query result exceeds the 64 MiB shell buffer budget")
			}
			out.Rows = append(out.Rows, values)
		}
		if len(out.Columns) > 0 {
			out.Tag = fmt.Sprintf("SELECT %d", len(out.Rows))
		} else {
			changes, e := s.QueryRow("SELECT changes()")
			if e != nil {
				return nil, e
			}
			n := int64(0)
			if len(changes) > 0 {
				n = changes[0].Int
			}
			switch first(sql) {
			case "INSERT", "REPLACE":
				out.Tag = fmt.Sprintf("INSERT 0 %d", n)
			case "UPDATE":
				out.Tag = fmt.Sprintf("UPDATE %d", n)
			case "DELETE":
				out.Tag = fmt.Sprintf("DELETE %d", n)
			default:
				out.Tag = first(sql)
			}
		}
		return nil, nil
	}
	if first(sql) == "SELECT" {
		_, e = d.db.Read(run)
	} else {
		_, e = d.db.Transaction(run)
	}
	return out, translate(e)
}
func quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
