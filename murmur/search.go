package murmur

import (
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
)

// Search index accessors.

// Name returns the index name.
func (s *SearchIndex) Name() string {
	if s.Tag == SearchVector {
		return s.Vector.Name
	}
	return s.Fts5.Name
}

// SourceTable returns the indexed source table.
func (s *SearchIndex) SourceTable() string {
	if s.Tag == SearchVector {
		return s.Vector.SourceTable
	}
	return s.Fts5.SourceTable
}

func (s *SearchIndex) columns() []string {
	if s.Tag == SearchVector {
		return []string{s.Vector.Column}
	}
	return append([]string(nil), s.Fts5.Columns...)
}

func (s *SearchIndex) createSQL() string {
	var module string
	if s.Tag == SearchVector {
		module = "vec0(" + s.Vector.Column + " float[" + itoa(s.Vector.Dimensions) + "] distance_metric=" + s.Vector.Distance.String() + ")"
	} else {
		quoted := make([]string, len(s.Fts5.Columns))
		for i, c := range s.Fts5.Columns {
			quoted[i] = quote(c)
		}
		module = "fts5(" + strings.Join(quoted, ",") + ", tokenize='" + s.Fts5.Tokenizer + "')"
	}
	return "CREATE VIRTUAL TABLE temp." + quote(s.Name()) + " USING " + module
}

// SchemaIndexName renders the managed FTS index name for a table.
func SchemaIndexName(table *Table) string {
	return "murmur_fts_" + table.ID
}

// VectorIndexName renders the managed vector index name.
func VectorIndexName(table *Table, column *Column) string {
	return "murmur_vec_" + table.ID + "_" + column.ID
}

// SchemaSearchDefinitions merges explicit and schema-derived definitions.
func SchemaSearchDefinitions(explicit []SearchIndex, schema *Schema) ([]SearchIndex, error) {
	definitions := append([]SearchIndex(nil), explicit...)
	for _, table := range schema.Tables {
		var columns []string
		for _, c := range table.Columns {
			if c.Fts {
				columns = append(columns, c.Name)
			}
		}
		if len(columns) > 0 {
			definitions = append(definitions, Fts5SearchIndex(NewFts5Index(SchemaIndexName(&table), table.Name, columns)))
		}
		for i := range table.Columns {
			column := &table.Columns[i]
			if column.Vector != nil {
				if asciiEqualFold(column.Name, ManagedKeyColumn) {
					return nil, invalidErr("vector declarations require an application column")
				}
				definitions = append(definitions, VectorSearchIndex(VectorIndex{
					Name: VectorIndexName(&table, column), SourceTable: table.Name,
					Column: column.Name, Dimensions: column.Vector.Dimensions, Distance: column.Vector.Distance,
				}))
			}
		}
	}
	options := DefaultOptions()
	options.SearchIndexes = definitions
	if err := ValidateSearchOptions(&options); err != nil {
		return nil, err
	}
	return options.SearchIndexes, nil
}

// SearchGuard maps owned index names to readiness.
type SearchGuard struct {
	Mu    sync.RWMutex
	Ready map[string]bool
}

// ValidateSearchOptions checks search maintenance limits and declarations.
func ValidateSearchOptions(o *Options) error {
	if o.SearchQueueBytes == 0 || o.SearchQueueBytes > 128<<20 ||
		o.SearchBatchRows == 0 || o.SearchBatchRows > 65536 ||
		o.SearchInterval <= 0 {
		return invalidErr("invalid search maintenance limits")
	}
	names := make(map[string]bool)
	for i := range o.SearchIndexes {
		index := &o.SearchIndexes[i]
		lower := strings.ToLower(index.Name())
		if index.Name() == "" || strings.Contains(index.Name(), "\x00") ||
			index.SourceTable() == "" || strings.Contains(index.SourceTable(), "\x00") ||
			strings.HasPrefix(lower, "sqlite_") || strings.HasPrefix(lower, "_crdt_") ||
			names[lower] {
			return invalidErr("invalid or duplicate search index name")
		}
		names[lower] = true
		columns := index.columns()
		lowered := make(map[string]bool)
		valid := len(columns) > 0
		for _, c := range columns {
			if c == "" || strings.Contains(c, "\x00") ||
				asciiEqualFold(c, "rowid") || asciiEqualFold(c, index.Name()) {
				valid = false
				break
			}
			lowered[strings.ToLower(c)] = true
		}
		if !valid || len(lowered) != len(columns) {
			return invalidErr("invalid search index columns")
		}
		switch index.Tag {
		case SearchFts5:
			switch index.Fts5.Tokenizer {
			case "unicode61", "ascii", "porter", "trigram":
			default:
				return invalidErr("unsupported FTS5 tokenizer")
			}
		case SearchVector:
			column := index.Vector.Column
			if len(column) == 0 || !isASCIILetter(column[0]) {
				return invalidErr("sqlite-vec column names must be ASCII SQL identifiers")
			}
			for i := 0; i < len(column); i++ {
				if !isASCIIAlnum(column[i]) && column[i] != '_' {
					return invalidErr("sqlite-vec column names must be ASCII SQL identifiers")
				}
			}
			if index.Vector.Dimensions < 1 || index.Vector.Dimensions > 8192 {
				return invalidErr("vector dimensions must be from 1 to 8192")
			}
			switch index.Vector.Distance {
			case DistL1, DistL2, DistCosine:
			default:
				return invalidErr("invalid vector distance")
			}
		}
	}
	return nil
}

func isASCIILetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isASCIIAlnum(c byte) bool {
	return isASCIILetter(c) || (c >= '0' && c <= '9')
}

type searchIndexState struct {
	definition SearchIndex
	owned      map[string]bool
	installed  bool
	ready      bool
	rebuild    bool
	started    bool
	last       *int64
	keys       map[int64]bool
	err        *string
}

func (s *searchIndexState) invalidate() {
	s.rebuild = true
	s.started = false
	s.last = nil
	s.keys = make(map[int64]bool)
	s.ready = false
}

// SearchManager owns disposable search indexes.
type SearchManager struct {
	explicit      []SearchIndex
	indexes       []*searchIndexState
	queued        [][]byte
	queuedBytes   int
	queuedSources map[string]bool
	budget        int
	batch         int
	next          int
	publications  map[string]bool
	Guard         *SearchGuard
}

// NewSearchManager builds a manager from options.
func NewSearchManager(o *Options) *SearchManager {
	m := &SearchManager{
		explicit:      append([]SearchIndex(nil), o.SearchIndexes...),
		queuedSources: make(map[string]bool),
		budget:        o.SearchQueueBytes, batch: o.SearchBatchRows,
		publications: make(map[string]bool),
		Guard:        &SearchGuard{Ready: make(map[string]bool)},
	}
	for _, definition := range o.SearchIndexes {
		m.indexes = append(m.indexes, &searchIndexState{
			definition: definition,
			owned:      map[string]bool{strings.ToLower(definition.Name()): true},
			keys:       make(map[int64]bool),
		})
	}
	return m
}

// CloneSearchManager deep-copies a manager.
func CloneSearchManager(m *SearchManager) *SearchManager {
	out := NewSearchManager(&Options{SearchQueueBytes: m.budget, SearchBatchRows: m.batch})
	out.explicit = append([]SearchIndex(nil), m.explicit...)
	out.indexes = nil
	for _, index := range m.indexes {
		cp := *index
		cp.owned = copyStringBool(index.owned)
		cp.keys = copyInt64Bool(index.keys)
		if index.last != nil {
			last := *index.last
			cp.last = &last
		}
		if index.err != nil {
			msg := *index.err
			cp.err = &msg
		}
		out.indexes = append(out.indexes, &cp)
	}
	out.queued = append([][]byte(nil), m.queued...)
	out.queuedBytes = m.queuedBytes
	out.queuedSources = copyStringBool(m.queuedSources)
	out.next = m.next
	out.publications = copyStringBool(m.publications)
	out.Guard = m.Guard
	return out
}

func copyStringBool(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyInt64Bool(m map[int64]bool) map[int64]bool {
	out := make(map[int64]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// RegisterSearchIndexes pins TEMP pages to memory. The vec0 module is
// registered on every connection, so vector definitions install directly.
func RegisterSearchIndexes(c *sqlite.Conn, definitions []SearchIndex) error {
	if len(definitions) > 0 {
		row, err := c.QueryRow("PRAGMA temp_store")
		if err != nil {
			return sqlErr(err)
		}
		if len(row) == 0 || row[0].Int != 2 {
			if err := c.ExecBatch("PRAGMA temp_store=MEMORY"); err != nil {
				return sqlErr(err)
			}
		}
	}
	return nil
}

// ValidateSearchSchema checks index sources against the schema.
func (m *SearchManager) ValidateSearchSchema(s *Schema, allowMissing bool) error {
	for _, index := range m.indexes {
		d := &index.definition
		for _, t := range s.Tables {
			if index.owned[strings.ToLower(t.Name)] {
				return invalidErr("source schema collides with search index %s", d.Name())
			}
		}
		var t *Table
		for i := range s.Tables {
			if s.Tables[i].Name == d.SourceTable() {
				t = &s.Tables[i]
				break
			}
		}
		if t == nil {
			if allowMissing && !index.installed {
				continue
			}
			return invalidErr("missing source table %s", d.SourceTable())
		}
		pk := ApplicationColumns(t)
		if len(pk) != 1 || !asciiEqualFold(pk[0].DeclaredType, "INTEGER") {
			return invalidErr("%s requires one INTEGER PRIMARY KEY for search indexing", t.Name)
		}
		for _, name := range d.columns() {
			col := findColumnByName(t, name)
			if col == nil {
				return invalidErr("missing search column %s.%s", t.Name, name)
			}
			expected := "TEXT"
			if d.Tag == SearchVector {
				expected = "BLOB"
			}
			if !asciiEqualFold(col.DeclaredType, expected) {
				return invalidErr("%s.%s must be declared %s", t.Name, col.Name, expected)
			}
		}
	}
	return nil
}

// SyncSearchSchema reconciles definitions with the schema.
func (m *SearchManager) SyncSearchSchema(c *sqlite.Conn, s *Schema, allowMissing bool) error {
	definitions, err := SchemaSearchDefinitions(m.explicit, s)
	if err != nil {
		return err
	}
	current := make([]SearchIndex, len(m.indexes))
	for i, index := range m.indexes {
		current[i] = index.definition
	}
	if !searchIndexSlicesEqual(definitions, current) {
		options := DefaultOptions()
		options.SearchIndexes = definitions
		if err := ValidateSearchOptions(&options); err != nil {
			return err
		}
		candidate := NewSearchManager(&options)
		if err := candidate.ValidateSearchSchema(s, allowMissing); err != nil {
			return err
		}
		if err := RegisterSearchIndexes(c, options.SearchIndexes); err != nil {
			return err
		}
		if err := m.DropSearchIndexes(c); err != nil {
			return err
		}
		m.indexes = candidate.indexes
		m.queued = nil
		m.queuedSources = make(map[string]bool)
		m.queuedBytes = 0
		m.next = 0
	}
	if err := m.ValidateSearchSchema(s, allowMissing); err != nil {
		return err
	}
	for _, index := range m.indexes {
		found := false
		for _, t := range s.Tables {
			if t.Name == index.definition.SourceTable() {
				found = true
				break
			}
		}
		if !found {
			continue
		}
		if !index.installed {
			before, err := tempSearchTables(c)
			if err != nil {
				return err
			}
			if err := c.ExecBatch(index.definition.createSQL()); err != nil {
				return sqlErr(err)
			}
			after, err := tempSearchTables(c)
			if err != nil {
				return err
			}
			index.owned = make(map[string]bool)
			for name := range after {
				if !before[name] {
					index.owned[name] = true
				}
			}
			index.installed = true
		}
		index.invalidate()
	}
	if err := m.ValidateSearchSchema(s, allowMissing); err != nil {
		return err
	}
	m.PublishSearchGuard()
	return nil
}

func searchIndexSlicesEqual(a, b []SearchIndex) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !searchIndexEqual(&a[i], &b[i]) {
			return false
		}
	}
	return true
}

func searchIndexEqual(a, b *SearchIndex) bool {
	if a.Tag != b.Tag {
		return false
	}
	if a.Tag == SearchVector {
		return a.Vector == b.Vector
	}
	if a.Fts5.Name != b.Fts5.Name || a.Fts5.SourceTable != b.Fts5.SourceTable || a.Fts5.Tokenizer != b.Fts5.Tokenizer || len(a.Fts5.Columns) != len(b.Fts5.Columns) {
		return false
	}
	for i := range a.Fts5.Columns {
		if a.Fts5.Columns[i] != b.Fts5.Columns[i] {
			return false
		}
	}
	return true
}

// DropSearchIndexes drops installed index tables.
func (m *SearchManager) DropSearchIndexes(c *sqlite.Conn) error {
	for _, index := range m.indexes {
		if index.installed {
			if err := c.ExecBatch("DROP TABLE temp." + quote(index.definition.Name())); err != nil {
				return sqlErr(err)
			}
		}
	}
	return nil
}

// PublishSearchGuard publishes owned names and readiness.
func (m *SearchManager) PublishSearchGuard() {
	guard := make(map[string]bool)
	for _, index := range m.indexes {
		for name := range index.owned {
			guard[name] = index.ready
		}
	}
	m.Guard.Mu.Lock()
	m.Guard.Ready = guard
	m.Guard.Mu.Unlock()
}

// SearchStatus reports per-index build state.
func (m *SearchManager) SearchStatus() []SearchIndexStatus {
	var out []SearchIndexStatus
	for _, index := range m.indexes {
		out = append(out, SearchIndexStatus{
			Name: index.definition.Name(), Ready: index.ready, Rebuilding: index.rebuild,
			PendingKeys: len(index.keys), QueuedBytes: m.queuedBytes, Error: index.err,
		})
	}
	return out
}

// TakeSearchPublications drains published index names.
func (m *SearchManager) TakeSearchPublications() []string {
	out := make([]string, 0, len(m.publications))
	for name := range m.publications {
		out = append(out, name)
	}
	sort.Strings(out)
	m.publications = make(map[string]bool)
	return out
}

func (m *SearchManager) memory() int {
	total := m.queuedBytes
	for _, index := range m.indexes {
		total += len(index.keys) * 32
	}
	return total
}

func (m *SearchManager) overflow() {
	m.queued = nil
	m.queuedBytes = 0
	for _, index := range m.indexes {
		if index.installed && (len(index.keys) > 0 || m.queuedSources[index.definition.SourceTable()]) {
			index.invalidate()
		}
	}
	m.queuedSources = make(map[string]bool)
	m.PublishSearchGuard()
}

// EnqueueSearch buffers a changeset affecting indexed tables.
func (m *SearchManager) EnqueueSearch(changes []byte, affected []string) {
	hit := false
	for _, index := range m.indexes {
		for _, t := range affected {
			if t == index.definition.SourceTable() {
				hit = true
				break
			}
		}
	}
	if !hit {
		return
	}
	for _, t := range affected {
		for _, index := range m.indexes {
			if index.definition.SourceTable() == t {
				m.queuedSources[t] = true
				break
			}
		}
	}
	if m.memory()+len(changes) > m.budget {
		m.overflow()
		return
	}
	m.queued = append(m.queued, append([]byte(nil), changes...))
	m.queuedBytes += len(changes)
}

// SearchRows notes changed rows for indexed tables.
func (m *SearchManager) SearchRows(s *Schema, rows []RowState) {
	for _, row := range rows {
		t := findSchemaTable(s, row.Table)
		if t == nil {
			continue
		}
		if t.ApplicationKey != nil {
			for _, index := range m.indexes {
				if index.definition.SourceTable() == t.Name {
					index.invalidate()
				}
			}
		} else if len(row.Key) > 0 && row.Key[0].Tag == ValueInteger {
			m.searchKey(t.Name, row.Key[0].I)
		}
	}
}

func (m *SearchManager) searchKey(table string, key int64) {
	for _, index := range m.indexes {
		if index.definition.SourceTable() == table && index.installed {
			index.keys[key] = true
		}
	}
	if m.memory() > m.budget {
		m.overflow()
	}
}

func (m *SearchManager) decodePending() error {
	for len(m.queued) > 0 {
		changes := m.queued[0]
		m.queued = m.queued[1:]
		m.queuedBytes -= len(changes)
		err := sqlite.IterateChangeset(changes, func(change sqlite.Change) error {
			hit := false
			for _, index := range m.indexes {
				if index.definition.SourceTable() == change.Table {
					hit = true
					break
				}
			}
			if !hit {
				return nil
			}
			pk := -1
			for i, flag := range change.PK {
				if flag {
					pk = i
					break
				}
			}
			if pk < 0 {
				return ErrSchemaMismatch
			}
			var value sqlite.Value
			if change.Op == sqlite.OpInsert {
				value = change.New[pk]
			} else {
				value = change.Old[pk]
			}
			if value.Type != sqlite.Integer {
				for _, index := range m.indexes {
					if index.definition.SourceTable() == change.Table {
						index.invalidate()
					}
				}
				return nil
			}
			m.searchKey(change.Table, value.Int)
			return nil
		})
		if err != nil {
			return sqlErr(err)
		}
	}
	m.queuedSources = make(map[string]bool)
	return nil
}

// SearchPending reports queued or rebuilding work.
func (m *SearchManager) SearchPending() bool {
	if len(m.queued) > 0 {
		return true
	}
	for _, index := range m.indexes {
		if index.rebuild || len(index.keys) > 0 {
			return true
		}
	}
	return false
}

// SearchTick runs one bounded maintenance turn.
func (m *SearchManager) SearchTick(c *sqlite.Conn, s *Schema) ([]string, error) {
	if err := m.decodePending(); err != nil {
		return nil, err
	}
	var published []string
	remaining := m.batch
	var firstError error
	start := m.next
	length := len(m.indexes)
	for offset := 0; offset < length; offset++ {
		if remaining == 0 {
			break
		}
		position := 0
		if length > 0 {
			position = (start + offset) % length
		}
		index := m.indexes[position]
		if !index.installed || (!index.rebuild && len(index.keys) == 0) {
			continue
		}
		if length > 0 {
			m.next = (position + 1) % length
		}
		savedRebuild, savedStarted, savedLast := index.rebuild, index.started, index.last
		var completed []int64
		count, err := func() (int, error) {
			if err := c.ExecBatch("SAVEPOINT murmur_search_work"); err != nil {
				return 0, sqlErr(err)
			}
			t := findTableByName(s, index.definition.SourceTable())
			if t == nil {
				return 0, ErrSchemaMismatch
			}
			pk := ApplicationColumns(t)[0].Name
			total := 0
			if index.rebuild {
				if !index.started {
					if err := c.Exec("DELETE FROM temp." + quote(index.definition.Name())); err != nil {
						return 0, sqlErr(err)
					}
					index.started = true
				}
				columns := index.definition.columns()
				quoted := make([]string, len(columns))
				for i, col := range columns {
					quoted[i] = quote(col)
				}
				sql := "SELECT " + quote(pk) + "," + strings.Join(quoted, ",") + " FROM main." + quote(t.Name) + " "
				var rows [][]sqlite.Value
				var err error
				if index.last != nil {
					sql += "WHERE " + quote(pk) + " > ? ORDER BY " + quote(pk) + " LIMIT ?"
					rows, err = c.Query(sql, *index.last, int64(remaining))
				} else {
					sql += "ORDER BY " + quote(pk) + " LIMIT ?"
					rows, err = c.Query(sql, int64(remaining))
				}
				if err != nil {
					return 0, sqlErr(err)
				}
				for _, row := range rows {
					key := row[0].Int
					values := make([]Value, 0, len(columns))
					for i := 1; i <= len(columns); i++ {
						v, err := sqliteValue(row[i])
						if err != nil {
							return 0, err
						}
						values = append(values, v)
					}
					if err := insertSearchRow(c, &index.definition, key, values); err != nil {
						return 0, err
					}
					last := key
					index.last = &last
					total++
				}
				if total < remaining {
					index.rebuild = false
				}
			} else {
				keys := make([]int64, 0, len(index.keys))
				for k := range index.keys {
					keys = append(keys, k)
				}
				sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
				if len(keys) > remaining {
					keys = keys[:remaining]
				}
				for _, key := range keys {
					if err := c.Exec("DELETE FROM temp."+quote(index.definition.Name())+" WHERE rowid=?", key); err != nil {
						return 0, sqlErr(err)
					}
					columns := index.definition.columns()
					quoted := make([]string, len(columns))
					for i, col := range columns {
						quoted[i] = quote(col)
					}
					rows, err := c.Query("SELECT "+strings.Join(quoted, ",")+" FROM main."+quote(t.Name)+" WHERE "+quote(pk)+"=?", key)
					if err != nil {
						return 0, sqlErr(err)
					}
					if len(rows) > 0 {
						values := make([]Value, 0, len(columns))
						for i := 0; i < len(columns); i++ {
							v, err := sqliteValue(rows[0][i])
							if err != nil {
								return 0, err
							}
							values = append(values, v)
						}
						if err := insertSearchRow(c, &index.definition, key, values); err != nil {
							return 0, err
						}
					}
				}
				total = len(keys)
				completed = keys
			}
			if err := c.ExecBatch("RELEASE murmur_search_work"); err != nil {
				return 0, sqlErr(err)
			}
			return total, nil
		}()
		if err == nil {
			for _, key := range completed {
				delete(index.keys, key)
			}
			if count < 1 {
				count = 1
			}
			remaining -= count
			if remaining < 0 {
				remaining = 0
			}
			index.err = nil
			if !index.rebuild && len(index.keys) == 0 {
				index.ready = true
			}
			if index.ready {
				published = append(published, index.definition.Name())
			}
		} else {
			_ = c.ExecBatch("ROLLBACK TO murmur_search_work; RELEASE murmur_search_work")
			index.rebuild, index.started, index.last = savedRebuild, savedStarted, savedLast
			index.ready = false
			msg := err.Error()
			index.err = &msg
			if firstError == nil {
				firstError = err
			}
		}
	}
	m.PublishSearchGuard()
	for _, name := range published {
		m.publications[name] = true
	}
	if firstError != nil {
		return nil, firstError
	}
	return published, nil
}

// SearchRefresh drains all pending work.
func (m *SearchManager) SearchRefresh(c *sqlite.Conn, s *Schema) ([]string, error) {
	published := make(map[string]bool)
	for m.SearchPending() {
		names, err := m.SearchTick(c, s)
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			published[name] = true
		}
	}
	out := make([]string, 0, len(published))
	for name := range published {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

func tempSearchTables(c *sqlite.Conn) (map[string]bool, error) {
	rows, err := c.Query("SELECT name FROM sqlite_temp_schema WHERE type IN ('table','index')")
	if err != nil {
		return nil, sqlErr(err)
	}
	out := make(map[string]bool)
	for _, row := range rows {
		if len(row) > 0 {
			out[strings.ToLower(row[0].Str)] = true
		}
	}
	return out, nil
}

func insertSearchRow(c *sqlite.Conn, d *SearchIndex, key int64, values []Value) error {
	if d.Tag == SearchVector {
		if values[0].Tag == ValueNull {
			return nil
		}
		if values[0].Tag != ValueBlob {
			return invalidErr("embedding must be a float32 BLOB")
		}
		raw := values[0].B
		if len(raw) != d.Vector.Dimensions*4 || !finiteFloats(raw) {
			return invalidErr("invalid embedding for %s row %d", d.Vector.Name, key)
		}
	}
	columns := d.columns()
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = quote(col)
	}
	placeholders := make([]string, len(values)+1)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	args := []any{key}
	for _, v := range values {
		args = append(args, sqlArg(v))
	}
	if err := c.Exec("INSERT INTO temp."+quote(d.Name())+"(rowid,"+strings.Join(quoted, ",")+") VALUES("+strings.Join(placeholders, ",")+")", args...); err != nil {
		return sqlErr(err)
	}
	return nil
}

func finiteFloats(raw []byte) bool {
	if len(raw)%4 != 0 {
		return false
	}
	for i := 0; i < len(raw); i += 4 {
		bits := uint32(raw[i]) | uint32(raw[i+1])<<8 | uint32(raw[i+2])<<16 | uint32(raw[i+3])<<24
		f := float64(math.Float32frombits(bits))
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return false
		}
	}
	return true
}

func sqliteValue(v sqlite.Value) (Value, error) {
	switch v.Type {
	case sqlite.Null:
		return NullValue(), nil
	case sqlite.Integer:
		return IntValue(v.Int), nil
	case sqlite.Float:
		return RealValue(v.Float), nil
	case sqlite.Text:
		return TextValue(v.Str), nil
	case sqlite.Blob:
		return BlobValue(append([]byte(nil), v.Blob...)), nil
	}
	return Value{}, corruptErr("unknown column type")
}
