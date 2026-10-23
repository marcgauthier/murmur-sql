package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/marcgauthier/murmur-sql-go/auth"
	"github.com/marcgauthier/murmur-sql-go/engine"
)

const maxStatements = 1024
const maxPortals = 1024
const maxSessionBytes = 64 << 20

type statement struct {
	sql         string
	description engine.Description
	generation  uint64
}
type portal struct {
	stmt     statement
	params   []engine.Value
	formats  []uint16
	result   *engine.Result
	position int
	complete bool
}
type session struct {
	server        *Server
	conn          net.Conn
	reader        *bufio.Reader
	writer        *bufio.Writer
	id            uint64
	user          string
	unlocked      bool
	pid, secret   uint32
	statements    map[string]statement
	portals       map[string]*portal
	extendedError bool
	ctx           context.Context
}

func newSession(s *Server, c net.Conn, id uint64) *session {
	return &session{server: s, conn: c, reader: bufio.NewReader(c), writer: bufio.NewWriter(c), id: id, statements: map[string]statement{}, portals: map[string]*portal{}}
}
func (s *session) cleanup() {
	s.server.mu.Lock()
	delete(s.server.cancels, s.pid)
	s.server.mu.Unlock()
	if db, e := s.server.database(); e == nil {
		db.Disconnect(s.id)
	}
}
func (s *session) flush() error {
	s.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	return s.writer.Flush()
}
func (s *session) readMessage() (byte, []byte, error) {
	kind, e := s.reader.ReadByte()
	if e != nil {
		return 0, nil, e
	}
	b, e := readLength(s.reader)
	return kind, b, e
}
func (s *session) startup(ctx context.Context) error {
	s.conn.SetDeadline(time.Now().Add(15 * time.Second))
	defer s.conn.SetDeadline(time.Time{})
	secure := false
	for requests := 0; requests < 4; requests++ {
		b, e := readLength(s.reader)
		if e != nil {
			return e
		}
		p := packet{b: b}
		version := p.u32()
		if p.err != nil {
			return p.err
		}
		switch version {
		case 80877103:
			if e = p.end(); e != nil {
				return e
			}
			if secure {
				return sqlError("08P01", "repeated SSLRequest")
			}
			if s.server.tls == nil {
				if _, e = s.conn.Write([]byte{'N'}); e != nil {
					return e
				}
				continue
			}
			if _, e = s.conn.Write([]byte{'S'}); e != nil {
				return e
			}
			tc := tls.Server(&bufferedConn{Conn: s.conn, reader: s.reader}, s.server.tls)
			if e = tc.HandshakeContext(ctx); e != nil {
				return e
			}
			s.conn = tc
			s.reader = bufio.NewReader(tc)
			s.writer = bufio.NewWriter(tc)
			secure = true
			continue
		case 80877104:
			if e = p.end(); e != nil {
				return e
			}
			if _, e = s.conn.Write([]byte{'N'}); e != nil {
				return e
			}
			continue
		case 80877102:
			pid, secret := p.u32(), p.u32()
			if e = p.end(); e != nil {
				return e
			}
			s.server.mu.Lock()
			entry, ok := s.server.cancels[pid]
			if ok && entry.secret == secret && entry.cancel != nil {
				entry.cancel()
			}
			s.server.mu.Unlock()
			return io.EOF
		case 196608:
			if s.server.Config().Server.Production && !secure {
				return sqlError("28000", "production mode requires a TLS connection")
			}
			params := map[string]string{}
			for p.err == nil {
				key := p.cstr()
				if key == "" {
					break
				}
				params[key] = p.cstr()
			}
			if e = p.end(); e != nil {
				return e
			}
			s.user = params["user"]
			if e = s.send('R', put32(nil, 3)); e != nil {
				return e
			}
			if e = s.flush(); e != nil {
				return e
			}
			kind, password, e := s.readMessage()
			if e != nil {
				return e
			}
			if kind != 'p' || len(password) == 0 || len(password) > 64<<10 || password[len(password)-1] != 0 {
				return sqlError("08P01", "expected password message")
			}
			candidate := password[:len(password)-1]
			expected := auth.DummyHash
			found := false
			for _, u := range s.server.Config().Auth.Users {
				if u.Name == s.user {
					expected = u.PasswordHash
					found = true
					break
				}
			}
			verified := auth.VerifyPassword(expected, candidate)
			clear(password)
			if !verified || !found {
				return sqlError("28P01", "password authentication failed")
			}
			s.pid = uint32(s.id)
			s.secret = random32()
			s.server.mu.Lock()
			s.server.cancels[s.pid] = cancelEntry{secret: s.secret}
			s.server.mu.Unlock()
			if e = s.send('R', put32(nil, 0)); e != nil {
				return e
			}
			for _, kv := range [][2]string{{"server_version", PGVersion}, {"server_encoding", "UTF8"}, {"client_encoding", "UTF8"}, {"DateStyle", "ISO, MDY"}, {"integer_datetimes", "on"}, {"standard_conforming_strings", "on"}, {"TimeZone", "UTC"}} {
				if e = s.send('S', cstr(cstr(nil, kv[0]), kv[1])); e != nil {
					return e
				}
			}
			if e = s.send('K', put32(put32(nil, s.pid), s.secret)); e != nil {
				return e
			}
			return s.ready()
		default:
			return sqlError("0A000", "unsupported PostgreSQL protocol version")
		}
	}
	return sqlError("08P01", "too many startup negotiation requests")
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) { return c.reader.Read(b) }
func (s *session) run(ctx context.Context) error {
	s.ctx = ctx
	if e := s.startup(ctx); e != nil {
		if !errors.Is(e, io.EOF) {
			s.sendError(e)
			s.flush()
		}
		return e
	}
	if e := s.flush(); e != nil {
		return e
	}
	for {
		kind, b, e := s.readMessage()
		if e != nil {
			return e
		}
		if kind == 'X' {
			if len(b) != 0 {
				return sqlError("08P01", "invalid terminate message")
			}
			return nil
		}
		if s.extendedError && kind != 'S' {
			continue
		}
		e = s.handle(kind, b)
		if e != nil {
			if se := s.sendError(e); se != nil {
				return se
			}
			if kind == 'Q' {
				if e = s.ready(); e != nil {
					return e
				}
			} else {
				s.extendedError = true
			}
		}
		if kind == 'Q' || kind == 'S' || kind == 'H' || e != nil {
			if e = s.flush(); e != nil {
				return e
			}
		}
	}
}
func (s *session) requestContext() (context.Context, func()) {
	ctx, cancel := context.WithCancel(s.ctx)
	s.server.mu.Lock()
	entry := s.server.cancels[s.pid]
	entry.cancel = cancel
	s.server.cancels[s.pid] = entry
	s.server.mu.Unlock()
	return ctx, func() {
		s.server.mu.Lock()
		entry := s.server.cancels[s.pid]
		entry.cancel = nil
		s.server.cancels[s.pid] = entry
		s.server.mu.Unlock()
		cancel()
	}
}
func (s *session) handle(kind byte, b []byte) error {
	p := packet{b: b}
	switch kind {
	case 'Q':
		query := p.cstr()
		if e := p.end(); e != nil {
			return e
		}
		delete(s.statements, "")
		delete(s.portals, "")
		stmts, e := SplitStatements(query)
		if e != nil {
			return e
		}
		if len(stmts) == 0 {
			if e = s.send('I', nil); e != nil {
				return e
			}
			return s.ready()
		}
		ctx, done := s.requestContext()
		defer done()
		for _, sql := range stmts {
			r, e := s.execute(ctx, sql, nil)
			if e != nil {
				return e
			}
			if len(r.Columns) > 0 {
				if e = s.rowDescription(r.Columns, nil); e != nil {
					return e
				}
			}
			for _, row := range r.Rows {
				if e = s.dataRow(r.Columns, row, nil); e != nil {
					return e
				}
			}
			if e = s.complete(r); e != nil {
				return e
			}
		}
		return s.ready()
	case 'P':
		name, sql := p.cstr(), p.cstr()
		n := int(p.u16())
		oids := make([]uint32, n)
		for i := range oids {
			oids[i] = p.u32()
		}
		if e := p.end(); e != nil {
			return e
		}
		if name != "" {
			if _, ok := s.statements[name]; ok {
				return sqlError("42P05", "prepared statement already exists")
			}
		}
		if len(s.statements) >= maxStatements {
			return sqlError("54000", "too many prepared statements")
		}
		if s.memory()+len(sql)+len(name) > maxSessionBytes {
			return sqlError("54000", "session memory limit exceeded")
		}
		parts, e := SplitStatements(sql)
		if e != nil {
			return e
		}
		if len(parts) > 1 {
			return syntax("extended query accepts only one statement")
		}
		if len(parts) == 1 {
			sql = parts[0]
		} else {
			sql = ""
		}
		ctx, done := s.requestContext()
		defer done()
		d, e := s.describe(ctx, sql, oids)
		if e != nil {
			return e
		}
		s.statements[name] = statement{sql: sql, description: d, generation: s.server.generation.Load()}
		return s.send('1', nil)
	case 'B':
		name, stmtName := p.cstr(), p.cstr()
		nf := int(p.u16())
		formats := make([]uint16, nf)
		for i := range formats {
			formats[i] = p.u16()
		}
		n := int(p.u16())
		raw := make([][]byte, n)
		for i := range raw {
			size := int32(p.u32())
			if size < -1 {
				return sqlError("08P01", "invalid parameter length")
			}
			if size >= 0 {
				v := p.read(int(size))
				if v != nil {
					raw[i] = append([]byte{}, v...)
				}
			}
		}
		nr := int(p.u16())
		results := make([]uint16, nr)
		for i := range results {
			results[i] = p.u16()
		}
		if e := p.end(); e != nil {
			return e
		}
		st, ok := s.statements[stmtName]
		if !ok {
			return sqlError("26000", "unknown prepared statement")
		}
		if n != len(st.description.Parameters) {
			return sqlError("08P01", "parameter count mismatch")
		}
		if e := validateFormats(formats, n); e != nil {
			return e
		}
		if e := validateFormats(results, len(st.description.Columns)); e != nil {
			return e
		}
		if name != "" {
			if _, ok := s.portals[name]; ok {
				return sqlError("42P03", "portal already exists")
			}
		}
		if len(s.portals) >= maxPortals || s.memory()+len(b) > maxSessionBytes {
			return sqlError("54000", "portal/session limit exceeded")
		}
		params := make([]engine.Value, n)
		for i := range raw {
			v, e := decodeValue(raw[i], st.description.Parameters[i], formatFor(formats, i))
			if e != nil {
				return e
			}
			params[i] = v
		}
		s.portals[name] = &portal{stmt: st, params: params, formats: results}
		return s.send('2', nil)
	case 'D':
		target := p.read(1)
		name := p.cstr()
		if e := p.end(); e != nil {
			return e
		}
		switch target[0] {
		case 'S':
			st, ok := s.statements[name]
			if !ok {
				return sqlError("26000", "unknown prepared statement")
			}
			if e := s.refresh(&st); e != nil {
				return e
			}
			s.statements[name] = st
			b := put16(nil, uint16(len(st.description.Parameters)))
			for _, oid := range st.description.Parameters {
				b = put32(b, oid)
			}
			if e := s.send('t', b); e != nil {
				return e
			}
			if len(st.description.Columns) == 0 {
				return s.send('n', nil)
			}
			return s.rowDescription(st.description.Columns, nil)
		case 'P':
			portal, ok := s.portals[name]
			if !ok {
				return sqlError("34000", "unknown portal")
			}
			if e := s.refresh(&portal.stmt); e != nil {
				return e
			}
			if len(portal.stmt.description.Columns) == 0 {
				return s.send('n', nil)
			}
			return s.rowDescription(portal.stmt.description.Columns, portal.formats)
		default:
			return sqlError("08P01", "invalid describe target")
		}
	case 'E':
		name := p.cstr()
		limit := int32(p.u32())
		if e := p.end(); e != nil {
			return e
		}
		if limit < 0 {
			return sqlError("08P01", "negative row limit")
		}
		po, ok := s.portals[name]
		if !ok {
			return sqlError("34000", "unknown portal")
		}
		if po.result == nil {
			if e := s.refresh(&po.stmt); e != nil {
				return e
			}
			ctx, done := s.requestContext()
			defer done()
			r, e := s.execute(ctx, po.stmt.sql, po.params, po.stmt.description)
			if e != nil {
				return e
			}
			if !sameColumns(r.Columns, po.stmt.description.Columns) {
				return sqlError("0A000", "cached plan result type changed; prepare the statement again")
			}
			if s.memory()+resultBytes(r) > maxSessionBytes {
				return sqlError("54000", "portal result exceeds session memory limit")
			}
			po.result = &r
		}
		if po.stmt.sql == "" {
			return s.send('I', nil)
		}
		r := po.result
		end := len(r.Rows)
		if limit > 0 && int(limit) < end-po.position {
			end = po.position + int(limit)
		}
		for po.position < end {
			if e := s.dataRow(r.Columns, r.Rows[po.position], po.formats); e != nil {
				return e
			}
			po.position++
		}
		if po.position < len(r.Rows) {
			return s.send('s', nil)
		}
		po.complete = true
		return s.complete(*r)
	case 'C':
		target := p.read(1)
		name := p.cstr()
		if e := p.end(); e != nil {
			return e
		}
		switch target[0] {
		case 'S':
			delete(s.statements, name)
		case 'P':
			delete(s.portals, name)
		default:
			return sqlError("08P01", "invalid close target")
		}
		return s.send('3', nil)
	case 'S':
		if e := p.end(); e != nil {
			return e
		}
		s.extendedError = false
		return s.ready()
	case 'H':
		return p.end()
	default:
		return sqlError("0A000", fmt.Sprintf("unsupported frontend message %q", kind))
	}
}
func (s *session) refresh(st *statement) error {
	gen := s.server.generation.Load()
	if gen == st.generation {
		return nil
	}
	ctx, done := s.requestContext()
	defer done()
	d, e := s.describe(ctx, st.sql, st.description.Parameters)
	if e != nil {
		return e
	}
	if !sameColumns(d.Columns, st.description.Columns) {
		return sqlError("0A000", "cached plan result type changed; prepare the statement again")
	}
	st.generation = gen
	return nil
}
func (s *session) memory() int {
	n := 0
	for name, st := range s.statements {
		n += len(name) + len(st.sql)
	}
	for name, p := range s.portals {
		n += len(name) + len(p.stmt.sql)
		for _, v := range p.params {
			n += valueBytes(v)
		}
		if p.result != nil {
			n += resultBytes(*p.result)
		}
	}
	return n
}
func valueBytes(v any) int {
	switch x := v.(type) {
	case string:
		return len(x)
	case []byte:
		return len(x)
	default:
		return 16
	}
}
func resultBytes(r engine.Result) int {
	n := 0
	for _, row := range r.Rows {
		for _, v := range row {
			n += valueBytes(v)
		}
	}
	return n
}
func (s *session) complete(r engine.Result) error {
	tag := r.Tag
	if tag == "" {
		tag = fmt.Sprintf("SELECT %d", len(r.Rows))
	}
	return s.send('C', cstr(nil, tag))
}
func (s *session) describe(ctx context.Context, sql string, hints []uint32) (engine.Description, error) {
	if sql == "" {
		return engine.Description{}, nil
	}
	if firstWord(sql) == "MURMUR" {
		if !s.unlocked && !isUnlock(sql) {
			return engine.Description{}, locked()
		}
		c, e := parseCommand(sql)
		if e != nil {
			return engine.Description{}, e
		}
		if len(hints) > len(c.parameters) {
			return engine.Description{}, sqlError("08P01", "too many parameter type hints")
		}
		for i, oid := range hints {
			if oid != 0 && oid != engine.Unknown && oid != c.parameters[i] {
				return engine.Description{}, sqlError("42P08", "inconsistent command parameter type")
			}
		}
		return engine.Description{Parameters: c.parameters, Columns: c.columns}, nil
	}
	if !s.unlocked {
		return engine.Description{}, locked()
	}
	if r, ok := builtin(sql); ok {
		return engine.Description{Columns: r.Columns}, nil
	}
	rewritten, n, e := RewriteParameters(sql)
	if e != nil {
		return engine.Description{}, e
	}
	if len(hints) > n {
		return engine.Description{}, sqlError("08P01", "too many parameter type hints")
	}
	types := make([]uint32, n)
	copy(types, hints)
	for _, oid := range types {
		if e := engine.RequireOID(oid); e != nil {
			return engine.Description{}, e
		}
	}
	db, e := s.server.database()
	if e != nil {
		return engine.Description{}, e
	}
	d, e := db.Describe(ctx, rewritten, types)
	if e != nil {
		return d, e
	}
	if len(d.Parameters) != n {
		return d, fmt.Errorf("engine description returned incorrect parameter count")
	}
	for i, oid := range d.Parameters {
		if oid == 0 || oid == engine.Unknown {
			d.Parameters[i] = engine.Text
		}
		if e := engine.RequireOID(d.Parameters[i]); e != nil {
			return d, e
		}
	}
	return d, nil
}
func (s *session) execute(ctx context.Context, sql string, params []engine.Value, expected ...engine.Description) (engine.Result, error) {
	if sql == "" {
		return engine.Result{}, nil
	}
	if firstWord(sql) == "MURMUR" {
		if !s.unlocked && !isUnlock(sql) {
			return engine.Result{}, locked()
		}
		c, e := parseCommand(sql)
		if e != nil {
			return engine.Result{}, e
		}
		return c.execute(ctx, s, params)
	}
	if !s.unlocked {
		return engine.Result{}, locked()
	}
	if r, ok := builtin(sql); ok {
		if len(params) > 0 {
			return r, sqlError("08P01", "unexpected parameters")
		}
		return r, nil
	}
	db, e := s.server.database()
	if e != nil {
		return engine.Result{}, e
	}
	rewritten, n, e := RewriteParameters(sql)
	if e != nil {
		return engine.Result{}, e
	}
	if len(params) != n {
		return engine.Result{}, sqlError("08P01", "parameter count mismatch")
	}
	switch firstWord(sql) {
	case "CREATE", "ALTER", "DROP", "REINDEX":
		if n != 0 {
			return engine.Result{}, sqlError("0A000", "DDL parameters are not supported")
		}
		r, e := db.Migrate(ctx, sql)
		if e == nil {
			s.server.generation.Add(1)
		}
		return r, e
	}
	if len(expected) > 0 {
		if typed, ok := db.(engine.DescribedDatabase); ok {
			return typed.ExecuteDescribed(ctx, rewritten, params, expected[0])
		}
	}
	return db.Execute(ctx, rewritten, params)
}
func builtin(sql string) (engine.Result, bool) {
	normalized := strings.ToUpper(strings.Join(strings.Fields(stripTrivia(sql)), " "))
	compact := strings.ReplaceAll(normalized, " ", "")
	if compact == "SELECTVERSION()" || normalized == "SHOW SERVER_VERSION" {
		return engine.Result{Columns: cols("version"), Rows: [][]engine.Value{{"PostgreSQL " + PGVersion}}, Tag: "SELECT 1"}, true
	}
	switch normalized {
	case "BEGIN", "BEGIN TRANSACTION", "BEGIN WORK", "START TRANSACTION":
		return engine.Result{Tag: "BEGIN"}, true
	case "COMMIT", "COMMIT TRANSACTION", "COMMIT WORK", "END", "END TRANSACTION", "END WORK":
		return engine.Result{Tag: "COMMIT"}, true
	case "ROLLBACK", "ROLLBACK TRANSACTION", "ROLLBACK WORK":
		return engine.Result{Tag: "ROLLBACK"}, true
	}
	return engine.Result{}, false
}

func isUnlock(sql string) bool {
	words := strings.Fields(stripTrivia(sql))
	return len(words) > 1 && strings.EqualFold(words[0], "MURMUR") && strings.EqualFold(words[1], "UNLOCK")
}
