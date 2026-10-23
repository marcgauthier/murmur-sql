package murmur_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/auth"
	"github.com/marcgauthier/murmur-sql-go/config"
	"github.com/marcgauthier/murmur-sql-go/engine"
	"github.com/marcgauthier/murmur-sql-go/server"
	murmur "github.com/marcgauthier/murmur-sql/clients/go"
)

// This fixture returns fixed protocol results and never emulates storage.
type protocolFixture struct{ key [32]byte }

func (f *protocolFixture) Open(ctx context.Context, r engine.OpenRequest) (engine.Database, engine.OpenOutcome, error) {
	f.key = r.Key
	return f, engine.OpenedNew, nil
}
func (f *protocolFixture) Admin(context.Context, engine.AdminRequest) (any, error) {
	return nil, engine.ErrUnavailable
}
func (f *protocolFixture) Unlock(ctx context.Context, key [32]byte) error {
	if key != f.key {
		return &engine.Error{Code: "28000", Message: "incorrect key"}
	}
	return nil
}
func (f *protocolFixture) Describe(ctx context.Context, sql string, hints []uint32) (engine.Description, error) {
	if sql == "SELECT ?1, ?2, ?3" {
		return engine.Description{Parameters: []uint32{engine.Int8, engine.Text, engine.Bytea}, Columns: []engine.Column{{Name: "number", OID: engine.Int8}, {Name: "text", OID: engine.Text}, {Name: "data", OID: engine.Bytea}}}, nil
	}
	return engine.Description{}, &engine.Error{Code: "42P01", Message: "missing fixture table"}
}
func (f *protocolFixture) Execute(ctx context.Context, sql string, args []engine.Value) (engine.Result, error) {
	if sql == "SELECT ?1, ?2, ?3" {
		d, _ := f.Describe(ctx, sql, nil)
		return engine.Result{Columns: d.Columns, Rows: [][]engine.Value{{args[0], args[1], args[2]}}, Tag: "SELECT 1"}, nil
	}
	return engine.Result{}, &engine.Error{Code: "42P01", Message: "missing fixture table"}
}
func (f *protocolFixture) Migrate(context.Context, string) (engine.Result, error) {
	return engine.Result{}, engine.ErrUnavailable
}
func (f *protocolFixture) Command(ctx context.Context, id uint64, c engine.Command) (engine.Result, error) {
	if c.Operation == "FLUSH" {
		return engine.Result{Tag: "FLUSH"}, nil
	}
	return engine.Result{}, engine.ErrUnavailable
}
func (f *protocolFixture) Disconnect(uint64)           {}
func (f *protocolFixture) Close(context.Context) error { return nil }
func shellServer(t *testing.T, f engine.Factory) murmur.Config {
	t.Helper()
	hash, e := auth.HashPassword([]byte("test-password"))
	if e != nil {
		t.Fatal(e)
	}
	c := config.DefaultConfig()
	c.Auth.Users = []config.UserConfig{{Name: "admin", PasswordHash: hash}}
	s, e := server.New("", c, f, server.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if e != nil {
		t.Fatal(e)
	}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, l) }()
	t.Cleanup(func() {
		cancel()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(5 * time.Second):
			t.Error("server shutdown timed out")
		}
	})
	host, p, _ := net.SplitHostPort(l.Addr().String())
	port, _ := strconv.Atoi(p)
	return murmur.Config{Host: host, Port: port, User: "admin", Password: "test-password", Key: "fixture-key", TLS: murmur.TLSConfig{Mode: murmur.TLSDisable}, MaxOpenConns: 2}
}
func TestGoShellExistingDriver(t *testing.T) {
	cfg := shellServer(t, &protocolFixture{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, e := murmur.Open(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	stmt, e := db.PrepareContext(ctx, "SELECT $1, $2, $3")
	if e != nil {
		t.Fatal(e)
	}
	defer stmt.Close()
	var n int64
	var text string
	var blob []byte
	payload := []byte{0, 1, 255}
	if e = stmt.QueryRowContext(ctx, int64(42), "hello'; DROP TABLE t;--", payload).Scan(&n, &text, &blob); e != nil {
		t.Fatal(e)
	}
	if n != 42 || text != "hello'; DROP TABLE t;--" || string(blob) != string(payload) {
		t.Fatal(n, text, blob)
	}
	var nullable any
	if e = stmt.QueryRowContext(ctx, int64(7), nil, []byte{}).Scan(&n, &nullable, &blob); e != nil {
		t.Fatal(e)
	}
	if nullable != nil || blob == nil || len(blob) != 0 {
		t.Fatal(nullable, blob)
	}
	if _, e = db.ExecContext(ctx, "MURMUR FLUSH"); e != nil {
		t.Fatal(e)
	}
	if _, e = db.QueryContext(ctx, "SELECT absent"); e == nil {
		t.Fatal("missing table unexpectedly succeeded")
	} else {
		var state interface{ SQLState() string }
		if !errors.As(e, &state) || state.SQLState() != "42P01" {
			t.Fatal(e)
		}
	}
	if e = stmt.QueryRowContext(ctx, int64(9), "after error", payload).Scan(&n, &text, &blob); e != nil {
		t.Fatal(e)
	}
}
func TestGoShellUnavailableEngine(t *testing.T) {
	cfg := shellServer(t, nil)
	_, e := murmur.Open(context.Background(), cfg)
	if e == nil {
		t.Fatal("production shell pretended to open a database")
	}
	var state interface{ SQLState() string }
	if !errors.As(e, &state) || state.SQLState() != "0A000" {
		t.Fatal(e)
	}
}
