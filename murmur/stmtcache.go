package murmur

import (
	"container/list"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
)

// cachedStmt is one pooled prepared statement. A Sql handle runs only on
// the writer thread, so entries need no mutex; inUse keeps an entry a
// live Rows cursor still steps from being shared or evicted.
type cachedStmt struct {
	stmt  *sqlite.Stmt
	inUse bool
	elem  *list.Element
}

// stmtCache is a bounded LRU of prepared statements for one Sql handle,
// keyed by the original SQL text. The schema cannot change during a
// callback, so rewritten prepares stay valid for the handle's life.
type stmtCache struct {
	cap     int
	entries map[string]*cachedStmt
	order   *list.List
}

func newStmtCache(size int) *stmtCache {
	if size <= 0 {
		return nil
	}
	return &stmtCache{cap: size, entries: make(map[string]*cachedStmt), order: list.New()}
}

// checkOpen reports whether the handle's callback is still running. Handles
// escape only by misuse; failing closed keeps them off the writer
// connection from the wrong goroutine.
func (s *Sql) checkOpen() error {
	if s == nil || s.dead {
		return invalidErr("sql handle used after its callback returned")
	}
	return nil
}

// checkout prepares a statement for key, reusing an idle cache entry when
// one exists. The caller must call release exactly once. A statement
// already checked out elsewhere (for example by an open Rows cursor)
// prepares fresh instead of sharing.
func (s *Sql) checkout(key, rewritten string) (*sqlite.Stmt, func(), error) {
	fresh := func() (*sqlite.Stmt, func(), error) {
		stmt, err := s.connection.Prepare(rewritten)
		if err != nil {
			return nil, nil, sqlErr(err)
		}
		return stmt, func() { _ = stmt.Close() }, nil
	}
	if s.stmts == nil {
		return fresh()
	}
	c := s.stmts
	if e, ok := c.entries[key]; ok {
		if e.inUse {
			return fresh()
		}
		if err := e.stmt.Reset(); err != nil {
			_ = e.stmt.Close()
			delete(c.entries, key)
			c.order.Remove(e.elem)
		} else {
			e.inUse = true
			c.order.MoveToFront(e.elem)
			return e.stmt, func() { e.inUse = false }, nil
		}
	}
	stmt, err := s.connection.Prepare(rewritten)
	if err != nil {
		return nil, nil, sqlErr(err)
	}
	e := &cachedStmt{stmt: stmt, inUse: true}
	e.elem = c.order.PushFront(key)
	c.entries[key] = e
	for c.order.Len() > c.cap {
		if !c.evictIdle() {
			// Every entry is live; keep the new statement
			// uncached rather than evict a live one.
			delete(c.entries, key)
			c.order.Remove(e.elem)
			return stmt, func() { _ = stmt.Close() }, nil
		}
	}
	return stmt, func() { e.inUse = false }, nil
}

// evictIdle finalizes the least recently used idle entry, reporting
// whether one existed.
func (c *stmtCache) evictIdle() bool {
	for el := c.order.Back(); el != nil; el = el.Prev() {
		name, ok := el.Value.(string)
		if !ok {
			continue
		}
		victim := c.entries[name]
		if victim == nil || victim.inUse {
			continue
		}
		_ = victim.stmt.Close()
		delete(c.entries, name)
		c.order.Remove(el)
		return true
	}
	return false
}

// closeStmts finalizes pooled statements and retires the handle. It runs
// when the handle's callback returns, even on panic.
func (s *Sql) closeStmts() {
	s.dead = true
	if s.stmts == nil {
		return
	}
	for key, e := range s.stmts.entries {
		_ = e.stmt.Close()
		delete(s.stmts.entries, key)
	}
	s.stmts.order.Init()
}
