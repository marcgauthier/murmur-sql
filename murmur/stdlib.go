package murmur

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// This file adapts an embedded Database to database/sql. Only the standard
// library is used; there are no new dependencies.
//
//   - OpenDB returns a *sql.DB over an open database. sql.Open is not
//     supported: there is no DSN form for raw encryption keys.
//   - Each statement autocommits, like engine execution. Multi-statement
//     Exec batches run their DML in one transaction; DDL routes to
//     migrations, mirroring the server.
//   - Begin maps to one engine Transaction served over a channel bridge,
//     so Tx.Exec/Tx.Query are atomic. Only default options are supported.
//   - Queries buffer their rows, like Sql.Query. Contexts are honored
//     before admission, but in-flight work cannot be interrupted: the
//     engine exposes no statement interrupt.
//
// Handles are safe for concurrent use through database/sql's pool.

var (
	_ driver.Conn               = (*stdConn)(nil)
	_ driver.ConnPrepareContext = (*stdConn)(nil)
	_ driver.ExecerContext      = (*stdConn)(nil)
	_ driver.QueryerContext     = (*stdConn)(nil)
	_ driver.ConnBeginTx        = (*stdConn)(nil)
	_ driver.Pinger             = (*stdConn)(nil)
	_ driver.NamedValueChecker  = (*stdConn)(nil)
	_ driver.Stmt               = (*stdStmt)(nil)
	_ driver.StmtExecContext    = (*stdStmt)(nil)
	_ driver.StmtQueryContext   = (*stdStmt)(nil)
	_ driver.Tx                 = (*stdTx)(nil)
	_ driver.Rows               = (*stdRows)(nil)
	_ driver.Result             = stdResult{}
	_ driver.Connector          = (*stdConnector)(nil)
)

// stdTimeFormat matches the server's timestamp text (server/wire.go).
const stdTimeFormat = "2006-01-02 15:04:05.000000"

// Connector returns a database/sql connector over this database.
func (db *Database) Connector() driver.Connector {
	return &stdConnector{db: db}
}

// OpenDB returns a *sql.DB over this database. Close the *sql.DB before
// closing the database.
func (db *Database) OpenDB() *sql.DB {
	return sql.OpenDB(db.Connector())
}

type stdDriver struct{ db *Database }

func (d *stdDriver) Open(name string) (driver.Conn, error) {
	return nil, invalidErr("murmur: sql.Open is not supported; use (*Database).OpenDB")
}

type stdConnector struct{ db *Database }

func (c *stdConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &stdConn{db: c.db}, nil
}

func (c *stdConnector) Driver() driver.Driver {
	return &stdDriver{db: c.db}
}

// firstKeyword returns the uppercased first SQL keyword, skipping trivia.
func firstKeyword(query string) string {
	tokens, err := Tokens(query)
	if err != nil || len(tokens) == 0 || tokens[0].Quoted {
		return ""
	}
	return strings.ToUpper(tokens[0].Text)
}

// isDDLStatement mirrors the server's DDL routing (server/session.go):
// these statements run through migrations, not SQL execution.
func isDDLStatement(query string) bool {
	switch firstKeyword(query) {
	case "CREATE", "ALTER", "DROP", "REINDEX":
		return true
	}
	return false
}

// isTxnNoop mirrors the server's transaction compatibility no-ops
// (server/session.go builtin): bare transaction verbs succeed and do
// nothing, since the engine autocommits statements.
func isTxnNoop(query string) bool {
	tokens, err := Tokens(query)
	if err != nil {
		return false
	}
	var words []string
	for _, t := range tokens {
		if t.Quoted {
			return false
		}
		if len(t.Text) == 0 || !isWordByte(t.Text[0]) {
			continue
		}
		words = append(words, t.Text)
	}
	switch strings.ToUpper(strings.Join(words, " ")) {
	case "BEGIN", "BEGIN TRANSACTION", "BEGIN WORK", "START TRANSACTION",
		"COMMIT", "COMMIT TRANSACTION", "COMMIT WORK",
		"ROLLBACK", "ROLLBACK TRANSACTION", "ROLLBACK WORK",
		"END", "END TRANSACTION", "END WORK":
		return true
	}
	return false
}

// rejectTxnVerbs guards execution entry points against bare transaction
// verbs, which would commit, roll back, or nest outside the engine's
// BEGIN IMMEDIATE framing on the shared writer connection.
func rejectTxnVerbs(query string) error {
	stmts, err := Statements(query)
	if err != nil {
		return nil // the execution path reports the syntax error
	}
	for _, st := range stmts {
		if isTxnNoop(st) {
			return invalidErr("murmur: bare transaction statements are not supported; use Database.Transaction")
		}
	}
	return nil
}

// checkStdArg validates one driver argument in place. Time values format
// as the server's timestamp text; named parameters are rejected.
func checkStdArg(nv *driver.NamedValue) error {
	if nv.Name != "" {
		return invalidErr("murmur: named parameters are not supported; use ? placeholders")
	}
	v, err := convertStdArg(nv.Value)
	if err != nil {
		return err
	}
	nv.Value = v
	return nil
}

func convertStdArg(v driver.Value) (driver.Value, error) {
	switch t := v.(type) {
	case nil, bool, int64, float64, string, []byte:
		return v, nil
	case int:
		return int64(t), nil
	case int8:
		return int64(t), nil
	case int16:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case uint:
		return int64(t), nil
	case uint8:
		return int64(t), nil
	case uint16:
		return int64(t), nil
	case uint32:
		return int64(t), nil
	case uint64:
		return int64(t), nil
	case float32:
		return float64(t), nil
	case time.Time:
		return t.UTC().Format(stdTimeFormat), nil
	default:
		return nil, invalidErr("murmur: cannot bind %T", v)
	}
}

// stdValues validates driver arguments into positional bindings.
func stdValues(args []driver.NamedValue) ([]any, error) {
	vals := make([]any, len(args))
	for i := range args {
		nv := args[i]
		if err := checkStdArg(&nv); err != nil {
			return nil, err
		}
		vals[i] = nv.Value
	}
	return vals, nil
}

type stdConn struct {
	db     *Database
	mu     sync.Mutex
	closed bool
	tx     *stdTx
}

func (c *stdConn) checkOpen() error {
	if c.closed {
		return driver.ErrBadConn
	}
	return nil
}

func (c *stdConn) activeTx() (*stdTx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	return c.tx, nil
}

func (c *stdConn) clearTx(tx *stdTx) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tx == tx {
		c.tx = nil
	}
}

func (c *stdConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *stdConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	return &stdStmt{conn: c, query: query}, nil
}

func (c *stdConn) Close() error {
	c.mu.Lock()
	tx := c.tx
	c.tx = nil
	c.closed = true
	c.mu.Unlock()
	if tx != nil {
		_ = tx.Rollback()
	}
	return nil
}

func (c *stdConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *stdConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.Isolation != driver.IsolationLevel(0) {
		return nil, invalidErr("murmur: isolation levels are not supported")
	}
	if opts.ReadOnly {
		return nil, invalidErr("murmur: read-only transactions are not supported")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if c.tx != nil {
		return nil, invalidErr("murmur: connection already has an active transaction")
	}
	tx, err := beginStdTx(c)
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return tx, nil
}

func (c *stdConn) CheckNamedValue(nv *driver.NamedValue) error {
	return checkStdArg(nv)
}

func (c *stdConn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx, err := c.activeTx()
	if err != nil {
		return err
	}
	if tx != nil {
		_, err := tx.query(ctx, "SELECT 1", nil)
		return err
	}
	_, err = c.db.Read(func(s *Sql) (any, error) {
		return s.Row("SELECT 1")
	})
	return err
}

func (c *stdConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	vals, err := stdValues(args)
	if err != nil {
		return nil, err
	}
	tx, err := c.activeTx()
	if err != nil {
		return nil, err
	}
	if tx != nil {
		return tx.exec(ctx, query, vals)
	}
	return execDirect(ctx, c.db, query, vals)
}

func (c *stdConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	vals, err := stdValues(args)
	if err != nil {
		return nil, err
	}
	tx, err := c.activeTx()
	if err != nil {
		return nil, err
	}
	if tx != nil {
		return tx.query(ctx, query, vals)
	}
	return queryDirect(ctx, c.db, query, vals)
}

// execDirect runs Exec outside a transaction: DDL statements migrate in
// order, and each DML run shares one transaction.
func execDirect(ctx context.Context, db *Database, query string, vals []any) (driver.Result, error) {
	if isTxnNoop(query) {
		return stdResult{}, nil
	}
	stmts, err := Statements(query)
	if err != nil {
		return nil, err
	}
	if len(stmts) == 0 {
		return nil, invalidErr("murmur: empty statement")
	}
	if len(stmts) > 1 && len(vals) > 0 {
		return nil, invalidErr("murmur: parameters with multiple statements are not supported")
	}
	var out stdResult
	var pending []string
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		batch := pending
		pending = nil
		v, err := db.Transaction(func(s *Sql) (any, error) {
			var affected, lastID int64
			for _, st := range batch {
				var n int64
				if len(batch) == 1 {
					n, err = s.Execute(st, vals...)
				} else {
					n, err = s.Execute(st)
				}
				if err != nil {
					return nil, err
				}
				affected += n
				lastID = s.LastInsertRowID()
			}
			return []int64{affected, lastID}, nil
		})
		if err != nil {
			return err
		}
		parts := v.([]int64)
		out.affected += parts[0]
		out.lastID = parts[1]
		return nil
	}
	for _, st := range stmts {
		if !isDDLStatement(st) {
			pending = append(pending, st)
			continue
		}
		if err := flush(); err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(vals) > 0 {
			return nil, invalidErr("murmur: DDL parameters are not supported")
		}
		if err := db.Migrate(NewMigration(st)); err != nil {
			return nil, err
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return out, nil
}

// queryDirect runs a single non-DDL query and buffers its rows.
func queryDirect(ctx context.Context, db *Database, query string, vals []any) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmts, err := Statements(query)
	if err != nil {
		return nil, err
	}
	if len(stmts) == 0 {
		return nil, invalidErr("murmur: empty statement")
	}
	if len(stmts) > 1 {
		return nil, invalidErr("murmur: Query supports a single statement; use Exec for batches")
	}
	if isDDLStatement(stmts[0]) {
		return nil, invalidErr("murmur: DDL statements must run with Exec, not Query")
	}
	v, err := db.Read(func(s *Sql) (any, error) {
		return collectStdRows(s, stmts[0], vals)
	})
	if err != nil {
		return nil, err
	}
	return v.(*stdRows), nil
}

// collectStdRows drains a query into buffered driver rows. Natural values
// are already valid driver values: nil, int64, float64, string, []byte.
func collectStdRows(s *Sql, query string, vals []any) (*stdRows, error) {
	rows, err := s.Rows(query, vals...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := &stdRows{columns: rows.Columns()}
	for rows.Next() {
		rv, err := rows.Values()
		if err != nil {
			return nil, err
		}
		dv := make([]driver.Value, len(rv))
		for i, v := range rv {
			dv[i] = v
		}
		out.rows = append(out.rows, dv)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// errTxRollback unwinds the bridge transaction; the waiter maps it to nil.
var errTxRollback = errors.New("murmur: rollback")

type stdTxRequest struct {
	exec  bool
	query string
	args  []any
	resp  chan stdTxResponse
}

type stdTxResponse struct {
	result stdResult
	rows   *stdRows
	err    error
}

// stdTx serves Tx statements inside one engine Transaction. database/sql
// routes Tx statements through the driver Conn, so the connection holds
// the active bridge and forwards while it lives.
type stdTx struct {
	conn   *stdConn
	reqCh  chan stdTxRequest
	resCh  chan error
	mu     sync.Mutex
	done   bool
	commit atomic.Bool
}

func beginStdTx(conn *stdConn) (*stdTx, error) {
	tx := &stdTx{conn: conn, reqCh: make(chan stdTxRequest), resCh: make(chan error, 1)}
	ready := make(chan struct{})
	go func() {
		_, err := conn.db.Transaction(func(s *Sql) (any, error) {
			close(ready)
			for req := range tx.reqCh {
				var resp stdTxResponse
				if req.exec {
					resp.result, resp.err = execTx(s, req.query, req.args)
				} else {
					resp.rows, resp.err = queryTx(s, req.query, req.args)
				}
				req.resp <- resp
			}
			if tx.commit.Load() {
				return nil, nil
			}
			return nil, errTxRollback
		})
		if err == errTxRollback {
			err = nil
		}
		tx.resCh <- err
	}()
	select {
	case <-ready:
		runtime.SetFinalizer(tx, func(t *stdTx) { go t.Rollback() })
		return tx, nil
	case err := <-tx.resCh:
		return nil, err
	}
}

func (tx *stdTx) request(ctx context.Context, exec bool, query string, args []any) stdTxResponse {
	failed := func(err error) stdTxResponse { return stdTxResponse{err: err} }
	if err := ctx.Err(); err != nil {
		return failed(err)
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return failed(invalidErr("murmur: transaction is finished"))
	}
	respCh := make(chan stdTxResponse, 1)
	select {
	case tx.reqCh <- stdTxRequest{exec: exec, query: query, args: args, resp: respCh}:
	case <-ctx.Done():
		return failed(ctx.Err())
	}
	select {
	case resp := <-respCh:
		return resp
	case <-ctx.Done():
		return failed(ctx.Err())
	}
}

func (tx *stdTx) exec(ctx context.Context, query string, args []any) (driver.Result, error) {
	if isTxnNoop(query) {
		return stdResult{}, nil
	}
	resp := tx.request(ctx, true, query, args)
	return resp.result, resp.err
}

func (tx *stdTx) query(ctx context.Context, query string, args []any) (driver.Rows, error) {
	resp := tx.request(ctx, false, query, args)
	if resp.err != nil {
		return nil, resp.err
	}
	return resp.rows, nil
}

func (tx *stdTx) Commit() error {
	return tx.finish(true)
}

func (tx *stdTx) Rollback() error {
	return tx.finish(false)
}

func (tx *stdTx) finish(commit bool) error {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return invalidErr("murmur: transaction is finished")
	}
	tx.done = true
	tx.mu.Unlock()
	if commit {
		tx.commit.Store(true)
	}
	tx.conn.clearTx(tx)
	close(tx.reqCh)
	err := <-tx.resCh
	runtime.SetFinalizer(tx, nil)
	return err
}

// execTx runs Exec statements on the bridge transaction. DDL needs
// migration access, which a write transaction cannot grant.
func execTx(s *Sql, query string, args []any) (stdResult, error) {
	stmts, err := Statements(query)
	if err != nil {
		return stdResult{}, err
	}
	if len(stmts) == 0 {
		return stdResult{}, invalidErr("murmur: empty statement")
	}
	if len(stmts) > 1 && len(args) > 0 {
		return stdResult{}, invalidErr("murmur: parameters with multiple statements are not supported")
	}
	var out stdResult
	for _, st := range stmts {
		if isDDLStatement(st) {
			return stdResult{}, invalidErr("murmur: DDL is not allowed inside a transaction; execute it outside Tx")
		}
		var n int64
		if len(stmts) == 1 {
			n, err = s.Execute(st, args...)
		} else {
			n, err = s.Execute(st)
		}
		if err != nil {
			return stdResult{}, err
		}
		out.affected += n
		out.lastID = s.LastInsertRowID()
	}
	return out, nil
}

func queryTx(s *Sql, query string, args []any) (*stdRows, error) {
	stmts, err := Statements(query)
	if err != nil {
		return nil, err
	}
	if len(stmts) == 0 {
		return nil, invalidErr("murmur: empty statement")
	}
	if len(stmts) > 1 {
		return nil, invalidErr("murmur: Query supports a single statement; use Exec for batches")
	}
	if isDDLStatement(stmts[0]) {
		return nil, invalidErr("murmur: DDL statements must run with Exec, not Query")
	}
	return collectStdRows(s, stmts[0], args)
}

type stdStmt struct {
	conn   *stdConn
	query  string
	mu     sync.Mutex
	closed bool
}

func (st *stdStmt) Close() error {
	st.mu.Lock()
	st.closed = true
	st.mu.Unlock()
	return nil
}

func (st *stdStmt) NumInput() int { return -1 }

func (st *stdStmt) CheckNamedValue(nv *driver.NamedValue) error {
	return checkStdArg(nv)
}

func (st *stdStmt) checkOpen() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return invalidErr("murmur: statement is closed")
	}
	return nil
}

func namedValues(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, v := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}

func (st *stdStmt) Exec(args []driver.Value) (driver.Result, error) {
	return st.ExecContext(context.Background(), namedValues(args))
}

func (st *stdStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := st.checkOpen(); err != nil {
		return nil, err
	}
	return st.conn.ExecContext(ctx, st.query, args)
}

func (st *stdStmt) Query(args []driver.Value) (driver.Rows, error) {
	return st.QueryContext(context.Background(), namedValues(args))
}

func (st *stdStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := st.checkOpen(); err != nil {
		return nil, err
	}
	return st.conn.QueryContext(ctx, st.query, args)
}

type stdRows struct {
	columns []string
	rows    [][]driver.Value
	pos     int
	closed  bool
}

func (r *stdRows) Columns() []string {
	out := make([]string, len(r.columns))
	copy(out, r.columns)
	return out
}

func (r *stdRows) Close() error {
	r.closed = true
	r.rows = nil
	return nil
}

func (r *stdRows) Next(dest []driver.Value) error {
	if r.closed || r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}

type stdResult struct {
	lastID   int64
	affected int64
}

func (r stdResult) LastInsertId() (int64, error) { return r.lastID, nil }
func (r stdResult) RowsAffected() (int64, error) { return r.affected, nil }
