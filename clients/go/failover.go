package murmur

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lib/pq"
)

// HostPort names one Murmur server. Empty Host defaults to 127.0.0.1;
// Port 0 defaults to 5432.
type HostPort struct {
	Host string
	Port int
}

// String renders host:port with defaults applied.
func (h HostPort) String() string {
	h = h.withDefaults()
	return h.Host + ":" + strconv.Itoa(h.Port)
}

func (h HostPort) withDefaults() HostPort {
	if h.Host == "" {
		h.Host = "127.0.0.1"
	}
	if h.Port == 0 {
		h.Port = 5432
	}
	return h
}

// failoverAction classifies a driver error for failover.
type failoverAction int

const (
	// failTerminal returns the error as-is: auth, constraints, app
	// errors, and caller cancellation never move hosts.
	failTerminal failoverAction = iota
	// failRetry converts to driver.ErrBadConn so database/sql retries
	// (usually on the same healthy host) without penalizing it.
	failRetry
	// failDown converts to driver.ErrBadConn and blacks the host out.
	failDown
)

// classifyFailover maps driver errors to failover actions.
func classifyFailover(err error) failoverAction {
	if err == nil {
		return failTerminal
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return failTerminal
	}
	if code, ok := failoverCode(err); ok && code == "40001" {
		return failRetry
	}
	if isTransportError(err) {
		return failDown
	}
	return failTerminal
}

// failoverCode extracts a SQLSTATE from client or driver errors.
func failoverCode(err error) (string, bool) {
	var own *Error
	if errors.As(err, &own) {
		return own.Code, true
	}
	var source *pq.Error
	if errors.As(err, &source) {
		return string(source.Code), true
	}
	return "", false
}

// isTransportCode reports connection-class SQLSTATEs: class 08 and the
// 57P0x shutdowns.
func isTransportCode(code string) bool {
	return strings.HasPrefix(code, "08") || code == "57P01" || code == "57P02" || code == "57P03"
}

// isTransportError reports unambiguously transport-level failures:
// connection-class SQLSTATEs, network errors, EOF, and refused/reset
// syscalls.
func isTransportError(err error) bool {
	if err == nil {
		return false
	}
	// Caller cancellation first: context errors implement net.Error.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if code, ok := failoverCode(err); ok {
		return isTransportCode(code)
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.EPIPE,
			syscall.ENETUNREACH, syscall.EHOSTUNREACH, syscall.ETIMEDOUT:
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"unexpected eof",
		"connection refused",
		"connection reset",
		"broken pipe",
		"use of closed network connection",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// failoverBackoff paces host blackout: 200ms doubling per consecutive
// failure, capped at 5s.
func failoverBackoff(failures int) time.Duration {
	d := 200 * time.Millisecond
	for i := 0; i < failures && d < 5*time.Second; i++ {
		d *= 2
		if d > 5*time.Second {
			return 5 * time.Second
		}
	}
	return d
}

// failoverHost is one dialable server with health state.
type failoverHost struct {
	addr          HostPort
	connector     driver.Connector
	failures      int
	blackoutUntil time.Time
}

// failoverConnector dials across primary and standbys in config order,
// skipping blacked-out hosts. Strict order keeps reads and writes on
// the primary while it is healthy, which also keeps single-writer
// workloads read-your-write without waits.
type failoverConnector struct {
	hosts []failoverHost
	mu    sync.Mutex
}

func (c *failoverConnector) Driver() driver.Driver { return failoverDriver{c} }

// order returns host indices: healthy in config order, then blacked-out
// in config order as fallback.
func (c *failoverConnector) order(now time.Time) []int {
	var healthy, blacked []int
	for i := range c.hosts {
		if now.Before(c.hosts[i].blackoutUntil) {
			blacked = append(blacked, i)
		} else {
			healthy = append(healthy, i)
		}
	}
	return append(healthy, blacked...)
}

// Connect dials the first reachable host. Auth failures return at once:
// a wrong key fails everywhere, so failover would only add latency.
func (c *failoverConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.mu.Lock()
	order := c.order(time.Now())
	c.mu.Unlock()
	var firstErr error
	for _, i := range order {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := c.hostConnector(i).Connect(ctx)
		if err == nil {
			c.succeeded(i)
			inner, ok := conn.(connection)
			if !ok {
				conn.Close()
				return nil, errors.New("murmur: driver connection has an unexpected shape")
			}
			return &failoverConn{connection: inner, f: c, idx: i}, nil
		}
		var own *Error
		if errors.As(err, &own) && own.Code == "28000" {
			return nil, err
		}
		var source *pq.Error
		if errors.As(err, &source) && source.Code == "28000" {
			return nil, err
		}
		c.failed(i)
		if firstErr == nil {
			firstErr = &hostError{host: c.hostAddr(i).String(), err: err}
		}
	}
	if firstErr == nil {
		firstErr = errors.New("murmur: no hosts configured")
	}
	return nil, firstErr
}

func (c *failoverConnector) hostConnector(i int) driver.Connector {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hosts[i].connector
}

func (c *failoverConnector) hostAddr(i int) HostPort {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hosts[i].addr
}

// succeeded resets a host's health on any successful use.
func (c *failoverConnector) succeeded(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hosts[i].failures = 0
	c.hosts[i].blackoutUntil = time.Time{}
}

// failed records a transport failure and blacks the host out.
func (c *failoverConnector) failed(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hosts[i].blackoutUntil = time.Now().Add(failoverBackoff(c.hosts[i].failures))
	c.hosts[i].failures++
}

// translate maps a statement error to its failover action, recording
// host health. Retryable errors become driver.ErrBadConn so
// database/sql transparently retries on a (usually different)
// connection; the cause stays reachable via errors.As.
func (c *failoverConnector) translate(idx int, err error) error {
	if err == nil {
		c.succeeded(idx)
		return nil
	}
	switch classifyFailover(err) {
	case failDown:
		c.failed(idx)
		return badConnError{host: c.hostAddr(idx).String(), err: err}
	case failRetry:
		return badConnError{host: c.hostAddr(idx).String(), err: err}
	default:
		return err
	}
}

// hostError names the failing host while preserving the cause chain.
type hostError struct {
	host string
	err  error
}

func (e *hostError) Error() string { return "murmur: host " + e.host + ": " + e.err.Error() }
func (e *hostError) Unwrap() error { return e.err }

// badConnError is driver.ErrBadConn with the host and cause attached.
type badConnError struct {
	host string
	err  error
}

func (e badConnError) Error() string { return "murmur: host " + e.host + ": " + e.err.Error() }
func (e badConnError) Unwrap() error { return e.err }
func (e badConnError) Is(target error) bool {
	return target == driver.ErrBadConn
}

type failoverDriver struct{ c *failoverConnector }

func (d failoverDriver) Open(name string) (driver.Conn, error) {
	d.c.mu.Lock()
	order := d.c.order(time.Now())
	d.c.mu.Unlock()
	var firstErr error
	for _, i := range order {
		conn, err := d.c.hostConnector(i).Driver().Open(name)
		if err == nil {
			d.c.succeeded(i)
			inner, ok := conn.(connection)
			if !ok {
				conn.Close()
				return nil, errors.New("murmur: driver connection has an unexpected shape")
			}
			return &failoverConn{connection: inner, f: d.c, idx: i}, nil
		}
		d.c.failed(i)
		if firstErr == nil {
			firstErr = &hostError{host: d.c.hostAddr(i).String(), err: err}
		}
	}
	if firstErr == nil {
		firstErr = errors.New("murmur: no hosts configured")
	}
	return nil, firstErr
}

// failoverConn translates statement errors for failover. It satisfies
// the same driver surface as the wrapped connection.
type failoverConn struct {
	connection
	f   *failoverConnector
	idx int
}

func (c *failoverConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.connection.PrepareContext(ctx, query)
	if err != nil {
		return nil, c.f.translate(c.idx, err)
	}
	inner, ok := stmt.(statement)
	if !ok {
		stmt.Close()
		return nil, errors.New("murmur: driver statement has an unexpected shape")
	}
	return &failoverStmt{statement: inner, f: c.f, idx: c.idx}, nil
}

func (c *failoverConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.connection.BeginTx(ctx, opts)
	if err != nil {
		return nil, c.f.translate(c.idx, err)
	}
	return &failoverTx{Tx: tx, f: c.f, idx: c.idx}, nil
}

func (c *failoverConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.connection.ExecContext(ctx, query, args)
	return result, c.f.translate(c.idx, err)
}

func (c *failoverConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.connection.QueryContext(ctx, query, args)
	if err != nil {
		return nil, c.f.translate(c.idx, err)
	}
	inner, ok := rows.(resultRows)
	if !ok {
		rows.Close()
		return nil, errors.New("murmur: driver rows have an unexpected shape")
	}
	c.f.succeeded(c.idx)
	return &failoverRows{resultRows: inner, f: c.f, idx: c.idx}, nil
}

func (c *failoverConn) Ping(ctx context.Context) error {
	return c.f.translate(c.idx, c.connection.Ping(ctx))
}

func (c *failoverConn) ResetSession(ctx context.Context) error {
	return c.f.translate(c.idx, c.connection.ResetSession(ctx))
}

type failoverStmt struct {
	statement
	f   *failoverConnector
	idx int
}

func (s *failoverStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	result, err := s.statement.ExecContext(ctx, args)
	return result, s.f.translate(s.idx, err)
}

func (s *failoverStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.statement.QueryContext(ctx, args)
	if err != nil {
		return nil, s.f.translate(s.idx, err)
	}
	inner, ok := rows.(resultRows)
	if !ok {
		rows.Close()
		return nil, errors.New("murmur: driver rows have an unexpected shape")
	}
	s.f.succeeded(s.idx)
	return &failoverRows{resultRows: inner, f: s.f, idx: s.idx}, nil
}

type failoverTx struct {
	driver.Tx
	f   *failoverConnector
	idx int
}

func (t *failoverTx) Commit() error   { return t.f.translate(t.idx, t.Tx.Commit()) }
func (t *failoverTx) Rollback() error { return t.f.translate(t.idx, t.Tx.Rollback()) }

type failoverRows struct {
	resultRows
	f   *failoverConnector
	idx int
}

func (r *failoverRows) Next(dest []driver.Value) error {
	err := r.resultRows.Next(dest)
	if err == nil {
		return nil
	}
	// io.EOF is the healthy end of rows, not a failure.
	if errors.Is(err, io.EOF) {
		return err
	}
	return r.f.translate(r.idx, err)
}

func (r *failoverRows) Close() error {
	return r.f.translate(r.idx, r.resultRows.Close())
}
