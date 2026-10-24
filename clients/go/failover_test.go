package murmur

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
)

type fakeFailoverConnector struct {
	conn  driver.Conn
	err   error
	calls int
}

func (c *fakeFailoverConnector) Connect(context.Context) (driver.Conn, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	return c.conn, nil
}

func (c *fakeFailoverConnector) Driver() driver.Driver { return nil }

func testFailoverHosts(primary, standby *fakeFailoverConnector) *failoverConnector {
	return &failoverConnector{hosts: []failoverHost{
		{addr: HostPort{Host: "primary", Port: 5432}, connector: primary},
		{addr: HostPort{Host: "standby", Port: 5433}, connector: standby},
	}}
}

// TestFailoverPickerPrefersPrimary pins strict config order: the first
// healthy host wins, and failures black the host out.
func TestFailoverPickerPrefersPrimary(t *testing.T) {
	primary := &fakeFailoverConnector{conn: &fakeConnection{}}
	standby := &fakeFailoverConnector{conn: &fakeConnection{}}
	fc := testFailoverHosts(primary, standby)
	conn, err := fc.Connect(context.Background())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if conn.(*failoverConn).idx != 0 || primary.calls != 1 || standby.calls != 0 {
		t.Fatalf("calls = %d/%d, want 1/0", primary.calls, standby.calls)
	}
	fc.failed(0)
	conn, err = fc.Connect(context.Background())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if conn.(*failoverConn).idx != 1 || standby.calls != 1 {
		t.Fatalf("standby calls = %d, want 1", standby.calls)
	}
	// Blackout expiry re-admits the primary.
	fc.hosts[0].blackoutUntil = time.Now().Add(-time.Second)
	if _, err := fc.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if primary.calls != 2 {
		t.Fatalf("primary calls = %d, want 2", primary.calls)
	}
}

// TestFailoverConnectFallsThrough pins dial failover: a dead primary
// fails to the standby, and total outage reports the primary error.
func TestFailoverConnectFallsThrough(t *testing.T) {
	dialErr := &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	primary := &fakeFailoverConnector{err: dialErr}
	standby := &fakeFailoverConnector{conn: &fakeConnection{}}
	fc := testFailoverHosts(primary, standby)
	conn, err := fc.Connect(context.Background())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if conn.(*failoverConn).idx != 1 {
		t.Fatalf("landed on host %d, want 1", conn.(*failoverConn).idx)
	}
	if fc.hosts[0].failures == 0 || !time.Now().Before(fc.hosts[0].blackoutUntil) {
		t.Fatalf("primary not blacked out: %+v", fc.hosts[0])
	}
	down := testFailoverHosts(
		&fakeFailoverConnector{err: dialErr},
		&fakeFailoverConnector{err: dialErr},
	)
	if _, err := down.Connect(context.Background()); err == nil {
		t.Fatal("all-down connect succeeded, want error")
	} else if !strings.Contains(err.Error(), "primary") {
		t.Fatalf("err = %v, want primary host named", err)
	}
}

// TestFailoverConnectAuthStops pins no-failover on auth errors: a wrong
// key fails everywhere, so the standby is never tried.
func TestFailoverConnectAuthStops(t *testing.T) {
	authErr := &Error{Code: "28000", Message: "incorrect key"}
	primary := &fakeFailoverConnector{err: authErr}
	standby := &fakeFailoverConnector{conn: &fakeConnection{}}
	fc := testFailoverHosts(primary, standby)
	_, err := fc.Connect(context.Background())
	var target *Error
	if !errors.As(err, &target) || target.Code != "28000" {
		t.Fatalf("err = %v, want 28000", err)
	}
	if standby.calls != 0 {
		t.Fatalf("standby calls = %d, want 0", standby.calls)
	}
	if errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("auth error maps to BadConn, want terminal failure")
	}
}

// TestIsTransportError pins the retryable classifier.
func TestIsTransportError(t *testing.T) {
	pqErr := func(code string) error { return &pq.Error{Code: pq.ErrorCode(code)} }
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"conn refused", &net.OpError{Op: "dial", Err: errors.New("refused")}, true},
		{"reset", &net.OpError{Op: "read", Err: errors.New("reset by peer")}, true},
		{"eof", io.EOF, true},
		{"unexpected eof", io.ErrUnexpectedEOF, true},
		{"pq eof text", errors.New("pq: unexpected EOF"), true},
		{"08006", pqErr("08006"), true},
		{"08003", pqErr("08003"), true},
		{"57P01", pqErr("57P01"), true},
		{"40001", pqErr("40001"), false},
		{"28000", pqErr("28000"), false},
		{"23505", pqErr("23505"), false},
		{"22023", pqErr("22023"), false},
		{"57014", pqErr("57014"), false},
		{"canceled", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, false},
		{"generic", errors.New("boom"), false},
	}
	for _, c := range cases {
		if got := isTransportError(c.err); got != c.want {
			t.Fatalf("%s: transport = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestClassifyFailover pins the tri-state: terminal errors return as-is,
// busy retries without penalizing the host, transport failures black
// the host out.
func TestClassifyFailover(t *testing.T) {
	pqErr := func(code string) error { return &pq.Error{Code: pq.ErrorCode(code)} }
	cases := []struct {
		name string
		err  error
		want failoverAction
	}{
		{"nil", nil, failTerminal},
		{"busy", pqErr("40001"), failRetry},
		{"conn failure", pqErr("08006"), failDown},
		{"shutdown", pqErr("57P01"), failDown},
		{"net", &net.OpError{Op: "read", Err: errors.New("reset")}, failDown},
		{"eof", io.EOF, failDown},
		{"auth", pqErr("28000"), failTerminal},
		{"constraint", pqErr("23505"), failTerminal},
		{"canceled", context.Canceled, failTerminal},
		{"deadline", context.DeadlineExceeded, failTerminal},
	}
	for _, c := range cases {
		if got := classifyFailover(c.err); got != c.want {
			t.Fatalf("%s: action = %d, want %d", c.name, got, c.want)
		}
	}
}

// TestFailoverConnTranslates pins statement-error handling: transport
// failures become ErrBadConn (so database/sql retries elsewhere) with
// the cause preserved; app errors pass through untouched.
func TestFailoverConnTranslates(t *testing.T) {
	transport := &net.OpError{Op: "read", Err: errors.New("reset by peer")}
	primary := &fakeFailoverConnector{conn: &fakeConnection{err: transport}}
	standby := &fakeFailoverConnector{conn: &fakeConnection{}}
	fc := testFailoverHosts(primary, standby)
	conn, err := fc.Connect(context.Background())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	_, err = conn.(driver.ExecerContext).ExecContext(context.Background(), "SELECT 1", nil)
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v, want ErrBadConn", err)
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("cause lost: %v", err)
	}
	if fc.hosts[0].failures == 0 {
		t.Fatal("host failure not recorded")
	}

	appErr := &pq.Error{Code: "23505", Message: "duplicate"}
	primary2 := &fakeFailoverConnector{conn: &fakeConnection{err: appErr}}
	fc2 := testFailoverHosts(primary2, standby)
	conn2, err := fc2.Connect(context.Background())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	_, err = conn2.(driver.ExecerContext).ExecContext(context.Background(), "SELECT 1", nil)
	if err != appErr {
		t.Fatalf("err = %v, want passthrough", err)
	}
	if fc2.hosts[0].failures != 0 {
		t.Fatal("app error penalized the host")
	}

	busy := &pq.Error{Code: "40001", Message: "busy"}
	primary3 := &fakeFailoverConnector{conn: &fakeConnection{err: busy}}
	fc3 := testFailoverHosts(primary3, standby)
	conn3, err := fc3.Connect(context.Background())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	_, err = conn3.(driver.ExecerContext).ExecContext(context.Background(), "SELECT 1", nil)
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v, want ErrBadConn", err)
	}
	if fc3.hosts[0].failures != 0 {
		t.Fatal("busy error penalized the host")
	}
}

// TestFailoverBackoffGrows pins exponential blackout with a cap.
func TestFailoverBackoffGrows(t *testing.T) {
	if got := failoverBackoff(0); got != 200*time.Millisecond {
		t.Fatalf("backoff(0) = %v", got)
	}
	if got := failoverBackoff(1); got != 400*time.Millisecond {
		t.Fatalf("backoff(1) = %v", got)
	}
	if got := failoverBackoff(100); got != 5*time.Second {
		t.Fatalf("backoff(100) = %v", got)
	}
}
