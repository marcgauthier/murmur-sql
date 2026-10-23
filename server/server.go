// Package server implements the PostgreSQL-wire shell. Application SQL uses
// SQLite semantics and is executed only through the engine contract.
package server

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/murmur-sql-go/config"
	"github.com/marcgauthier/murmur-sql-go/engine"
)

const Version = "1.0.0-go"
const PGVersion = "16.6-murmur-" + Version

type Server struct {
	cfgMu       sync.RWMutex
	cfg         config.Config
	path        string
	factory     engine.Factory
	tls         *tls.Config
	log         *slog.Logger
	backupMu    sync.Mutex
	slot        sync.Mutex
	db          engine.Database
	generation  atomic.Uint64
	nextID      atomic.Uint64
	mu          sync.Mutex
	connections map[net.Conn]struct{}
	cancels     map[uint32]cancelEntry
	serving     bool
}
type cancelEntry struct {
	secret uint32
	cancel context.CancelFunc
}
type Option func(*Server)

func WithLogger(l *slog.Logger) Option {
	return func(s *Server) {
		if l != nil {
			s.log = l
		}
	}
}
func New(path string, c config.Config, f engine.Factory, opts ...Option) (*Server, error) {
	if err := c.ValidateCredentials(); err != nil {
		return nil, err
	}
	t, e := TLSConfig(c)
	if e != nil {
		return nil, e
	}
	if f == nil {
		f = engine.Unavailable{}
	}
	s := &Server{cfg: c.Snapshot(), path: path, factory: f, tls: t, log: slog.Default(), connections: map[net.Conn]struct{}{}, cancels: map[uint32]cancelEntry{}}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}
func (s *Server) Config() config.Config {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg.Snapshot()
}
func (s *Server) saveFlags(flags map[string]bool) error {
	if err := config.SaveTableFlags(s.path, flags); err != nil {
		return err
	}
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.Replication.Tables = map[string]bool{}
	for k, v := range flags {
		s.cfg.Replication.Tables[k] = v
	}
	return nil
}
func (s *Server) unlock(ctx context.Context, key [32]byte) error {
	s.slot.Lock()
	defer s.slot.Unlock()
	if s.db != nil {
		return s.db.Unlock(ctx, key)
	}
	db, _, e := s.factory.Open(ctx, engine.OpenRequest{Config: s.Config(), Key: key, TableFlagsSink: s.saveFlags})
	if e != nil {
		return e
	}
	if db == nil {
		return errors.New("engine returned a nil database")
	}
	s.db = db
	return nil
}
func (s *Server) database() (engine.Database, error) {
	s.slot.Lock()
	defer s.slot.Unlock()
	if s.db == nil {
		return nil, sqlError("28000", "connection is locked; send MURMUR UNLOCK KEY '<key>' first")
	}
	return s.db, nil
}
func (s *Server) ListenAndServe(ctx context.Context) error {
	l, e := net.Listen("tcp", s.Config().Server.Bind)
	if e != nil {
		return &BindError{e}
	}
	return s.Serve(ctx, l)
}

type BindError struct{ error }

// Serve owns the listener and connections. Cancellation stops admission,
// cancels active requests, waits for sessions, and closes the engine last.
func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	s.mu.Lock()
	if s.serving {
		s.mu.Unlock()
		return errors.New("server may only be served once")
	}
	s.serving = true
	s.mu.Unlock()
	defer l.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			l.Close()
			s.mu.Lock()
			for c := range s.connections {
				c.Close()
			}
			s.mu.Unlock()
		case <-done:
		}
	}()
	var wg sync.WaitGroup
	slots := make(chan struct{}, s.Config().Server.MaxConnections)
	s.log.Info("server listening", "address", l.Addr().String())
	var serveErr error
	for {
		c, e := l.Accept()
		if e != nil {
			if ctx.Err() == nil {
				serveErr = e
			}
			break
		}
		select {
		case slots <- struct{}{}:
		default:
			c.Close()
			continue
		}
		s.mu.Lock()
		s.connections[c] = struct{}{}
		s.mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots; s.mu.Lock(); delete(s.connections, c); s.mu.Unlock(); c.Close() }()
			id := s.nextID.Add(1)
			stopConnection := context.AfterFunc(ctx, func() { c.Close() })
			defer stopConnection()
			session := newSession(s, c, id)
			defer session.cleanup()
			if e := session.run(ctx); e != nil && ctx.Err() == nil {
				s.log.Debug("connection ended", "session", id, "error", e)
			}
		}()
	}
	cancel()
	wg.Wait()
	s.slot.Lock()
	db := s.db
	s.db = nil
	s.slot.Unlock()
	if db != nil {
		closeCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		serveErr = errors.Join(serveErr, db.Close(closeCtx))
	}
	return serveErr
}
func random32() uint32 {
	var b [4]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return binary.BigEndian.Uint32(b[:])
}
