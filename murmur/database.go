package murmur

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// TableFlagsSink receives the full table-replication flag map after
// engine-side changes. A sink error fails the setting change.
type TableFlagsSink func(map[string]bool) error

// Access modes for the SQL authorizer.
const (
	AccessNone      uint32 = 0
	AccessRead      uint32 = 1
	AccessWrite     uint32 = 2
	AccessMigration uint32 = 3
	AccessPolicy    uint32 = 4
)

// Sql is scoped SQL access without hooks or transaction controls.
type Sql struct {
	connection *sqlite.Conn
	schema     *Schema
	intents    *[]PolicyIntent
	access     *atomic.Uint32
}

// Execute runs one statement, returning modified rows.
func (s *Sql) Execute(sql string, params ...any) (int64, error) {
	rewritten, err := Rewrite(sql, s.schema)
	if err != nil {
		return 0, err
	}
	stmt, err := s.connection.Prepare(rewritten)
	if err != nil {
		return 0, sqlErr(err)
	}
	defer stmt.Close()
	if err := stmt.Bind(params...); err != nil {
		return 0, sqlErr(err)
	}
	if _, err := stmt.Step(); err != nil {
		return 0, sqlErr(err)
	}
	return s.connection.Changes(), nil
}

// ExecuteBatch runs rewritten statements.
func (s *Sql) ExecuteBatch(sql string) error {
	statements, err := Statements(sql)
	if err != nil {
		return err
	}
	for _, statement := range statements {
		rewritten, err := Rewrite(statement, s.schema)
		if err != nil {
			return err
		}
		if err := s.connection.ExecBatch(rewritten); err != nil {
			return sqlErr(err)
		}
	}
	return nil
}

// Prepare prepares a rewritten statement.
func (s *Sql) Prepare(sql string) (*sqlite.Stmt, error) {
	rewritten, err := Rewrite(sql, s.schema)
	if err != nil {
		return nil, err
	}
	stmt, err := s.connection.Prepare(rewritten)
	if err != nil {
		return nil, sqlErr(err)
	}
	return stmt, nil
}

// QueryRow runs a query returning one row.
func (s *Sql) QueryRow(sql string, params ...any) ([]sqlite.Value, error) {
	rewritten, err := Rewrite(sql, s.schema)
	if err != nil {
		return nil, err
	}
	row, err := s.connection.QueryRow(rewritten, params...)
	if err != nil {
		return nil, sqlErr(err)
	}
	return row, nil
}

// Query runs a query returning all rows.
func (s *Sql) Query(sql string, params ...any) ([][]sqlite.Value, error) {
	rewritten, err := Rewrite(sql, s.schema)
	if err != nil {
		return nil, err
	}
	rows, err := s.connection.Query(rewritten, params...)
	if err != nil {
		return nil, sqlErr(err)
	}
	return rows, nil
}

// LastInsertRowID returns the latest inserted rowid.
func (s *Sql) LastInsertRowID() int64 {
	return s.connection.LastInsertRowid()
}

// CounterAdd adds a delta to a counter cell.
func (s *Sql) CounterAdd(table string, key []Value, column string, delta int64) error {
	return s.operation(table, key, column, PolicyOperation{Tag: PolicyOpCounter, Delta: delta})
}

// SetAdd adds a value to a set cell.
func (s *Sql) SetAdd(table string, key []Value, column string, value Value) error {
	return s.operation(table, key, column, PolicyOperation{Tag: PolicyOpAdd, V: value})
}

// SetRemove removes a value from a set cell.
func (s *Sql) SetRemove(table string, key []Value, column string, value Value) error {
	return s.operation(table, key, column, PolicyOperation{Tag: PolicyOpRemove, V: value})
}

// Min lowers a min cell.
func (s *Sql) Min(table string, key []Value, column string, value Value) error {
	return s.operation(table, key, column, PolicyOperation{Tag: PolicyOpMin, V: value})
}

// Max raises a max cell.
func (s *Sql) Max(table string, key []Value, column string, value Value) error {
	return s.operation(table, key, column, PolicyOperation{Tag: PolicyOpMax, V: value})
}

func (s *Sql) operation(table string, key []Value, column string, operation PolicyOperation) error {
	if s.intents == nil {
		return invalidErr("policy operations require a transaction")
	}
	t := findTableByName(s.schema, table)
	if t == nil {
		return ErrSchemaMismatch
	}
	var c *Column
	for i := range t.Columns {
		if t.Columns[i].Name == column {
			c = &t.Columns[i]
			break
		}
	}
	if c == nil {
		return ErrSchemaMismatch
	}
	var required MergePolicy
	switch operation.Tag {
	case PolicyOpCounter:
		required = MergePolicyCounter
	case PolicyOpAdd, PolicyOpRemove:
		required = MergePolicySet
	case PolicyOpMin:
		required = MergePolicyMin
	case PolicyOpMax:
		required = MergePolicyMax
	}
	if c.Policy != required || len(key) != len(ApplicationColumns(t)) {
		return invalidErr("operation does not match column policy or primary key")
	}
	for _, k := range key {
		if k.Tag == ValueNull {
			return invalidErr("operation does not match column policy or primary key")
		}
	}
	if len(*s.intents) >= 10000 {
		return limitErr("policy operation count")
	}
	predicate := ApplicationPredicate(t)
	row, err := s.connection.QueryRow("SELECT "+quote(column)+" FROM "+quote(table)+" WHERE "+predicate, valuesToArgs(key)...)
	if err != nil {
		if errors.Is(err, sqlite.ErrNoRows) {
			return notFoundErr("policy target row does not exist")
		}
		return sqlErr(err)
	}
	before, err := sqliteValue(row[0])
	if err != nil {
		return err
	}
	var after Value
	switch operation.Tag {
	case PolicyOpCounter:
		if before.Tag != ValueInteger {
			return invalidErr("counter projection must be INTEGER")
		}
		sum := before.I + operation.Delta
		if (operation.Delta > 0 && sum < before.I) || (operation.Delta < 0 && sum > before.I) {
			return limitErr("counter overflow")
		}
		after = IntValue(sum)
	case PolicyOpAdd, PolicyOpRemove:
		var values []Value
		switch before.Tag {
		case ValueBlob:
			values, err = DecodeValues(before.B)
			if err != nil {
				return err
			}
		case ValueNull:
		default:
			return invalidErr("set projection must be BLOB or NULL")
		}
		if operation.Tag == PolicyOpAdd {
			found := false
			for _, v := range values {
				if ValuesEqual(v, operation.V) {
					found = true
					break
				}
			}
			if !found {
				values = append(values, operation.V)
			}
		} else {
			kept := values[:0]
			for _, v := range values {
				if !ValuesEqual(v, operation.V) {
					kept = append(kept, v)
				}
			}
			values = kept
		}
		sort.Slice(values, func(i, j int) bool {
			return bytes.Compare(EncodeValue(values[i]), EncodeValue(values[j])) < 0
		})
		after = BlobValue(EncodeValues(values))
	case PolicyOpMin:
		if cmp, err := PolicyCompare(operation.V, before); err != nil {
			return err
		} else if cmp < 0 {
			after = operation.V
		} else {
			after = before
		}
	case PolicyOpMax:
		if cmp, err := PolicyCompare(operation.V, before); err != nil {
			return err
		} else if cmp > 0 {
			after = operation.V
		} else {
			after = before
		}
	}
	// Only this fixed statement bypasses policy assignment restrictions.
	s.access.Store(AccessPolicy)
	args := append([]any{sqlArg(after)}, valuesToArgs(key)...)
	execErr := s.connection.Exec("UPDATE "+quote(table)+" SET "+quote(column)+"=? WHERE "+predicate, args...)
	s.access.Store(AccessWrite)
	if execErr != nil {
		return sqlErr(execErr)
	}
	if s.connection.Changes() != 1 {
		return invalidErr("policy row missing")
	}
	resolved := key
	if t.ApplicationKey != nil {
		managed, err := s.connection.QueryRow("SELECT "+quote(ManagedKeyColumn)+" FROM "+quote(table)+" WHERE "+predicate, valuesToArgs(key)...)
		if err != nil {
			return sqlErr(err)
		}
		v, err := sqliteValue(managed[0])
		if err != nil {
			return err
		}
		resolved = []Value{v}
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return ioErr(err)
	}
	*s.intents = append(*s.intents, PolicyIntent{Table: table, Column: column, Key: resolved, Before: before, Operation: operation, ID: id})
	return nil
}

func valuesToArgs(values []Value) []any {
	args := make([]any, len(values))
	for i, v := range values {
		args[i] = sqlArg(v)
	}
	return args
}

type dbMessage struct {
	job  func(*Core)
	stop chan error
}

// Inner owns the writer thread and shared handles.
type Inner struct {
	tx            chan dbMessage
	worker        chan<- Work
	store         *spool.Store
	replicationMu sync.Mutex
	replication   *replication.Handle
	threadMu      sync.Mutex
	threadDone    chan struct{}
	status        *StatusState
	identity      Identity
	subMgr        *SubManager
	scheduler     *WriterScheduler
	active        atomic.Bool
	writerGID     atomic.Int64
}

// Database is a cloneable handle to an open database.
type Database struct {
	inner *Inner
}

// Open opens or creates a database.
func Open(path string, key []byte, options Options) (*Database, error) {
	return openInner(path, key, options, nil)
}

// OpenWithFlags opens with explicit table replication flags.
func OpenWithFlags(path string, key []byte, options Options, flags map[string]bool) (*Database, error) {
	return openInner(path, key, options, flags)
}

func openInner(path string, key []byte, options Options, flags map[string]bool) (*Database, error) {
	if options.Replication != nil {
		config := options.Replication
		if len(config.Credentials.Chain) == 0 {
			return nil, invalidErr("missing certificate")
		}
		id, err := replication.NodeID(config.Credentials.Chain[0])
		if err != nil {
			return nil, invalidErr("%v", err)
		}
		if options.NodeID == nil {
			options.NodeID = &id
		}
	}
	networkConfig := options.Replication
	if err := options.Validate(); err != nil {
		return nil, err
	}
	schedulerConfig := options.Scheduler
	keyCopy := append([]byte(nil), key...)
	tx := make(chan dbMessage)
	type startResult struct {
		identity  Identity
		worker    chan<- Work
		store     *spool.Store
		subMgr    *SubManager
		writerGID int64
		err       error
	}
	startCh := make(chan startResult, 1)
	status := &StatusState{}
	progress := NewOpenProgressReporter(options.OnOpenProgress)
	threadDone := make(chan struct{})
	go func() {
		defer close(threadDone)
		core, err := func() (*Core, error) {
			opened, err := OpenStorage(path, keyCopy, &options, progress)
			if err != nil {
				return nil, err
			}
			for i := range keyCopy {
				keyCopy[i] = 0
			}
			connection, err := RecoverCurrent(opened, progress)
			if err != nil {
				opened.Close()
				return nil, err
			}
			opened.History = nil
			identity := opened.Identity
			// The worker keeps opened.Schema; the writer needs private backing.
			schema := CloneSchema(opened.Schema)
			cursor := opened.Cursor
			collectionClock := opened.Clock
			search := NewSearchManager(&options)
			definitions, err := SchemaSearchDefinitions(options.SearchIndexes, &schema)
			if err != nil {
				opened.Close()
				connection.Close()
				return nil, err
			}
			if err := RegisterSearchIndexes(connection, definitions); err != nil {
				opened.Close()
				connection.Close()
				return nil, err
			}
			if err := search.SyncSearchSchema(connection, &schema, options.Mode == OpenCreateNew); err != nil {
				opened.Close()
				connection.Close()
				return nil, err
			}
			if _, err := search.SearchRefresh(connection, &schema); err != nil {
				opened.Close()
				connection.Close()
				return nil, err
			}
			search.TakeSearchPublications()
			status.Mu.Lock()
			status.Status.SearchIndexes = search.SearchStatus()
			status.Mu.Unlock()
			progress.Phase(PhaseFinalizing)
			capture, err := CaptureSchema(connection, &schema)
			if err != nil {
				opened.Close()
				connection.Close()
				return nil, err
			}
			access := &atomic.Uint32{}
			if cerr := configureAuthorizer(connection, &schema, access, search.Guard); cerr != nil {
				opened.Close()
				connection.Close()
				return nil, cerr
			}
			physical, worker, workerDone := StartStorage(opened, &options, status)
			_ = physical
			_ = workerDone
			finalProgress := progress.Finish(nil)
			status.Mu.Lock()
			status.Status.Committed = cursor
			status.Status.Durable = cursor
			status.Status.OpenProgress = finalProgress
			status.Mu.Unlock()
			subMgr := NewSubManager(identity.Database, 1024)
			core := &Core{
				capture: capture, connection: connection, access: access,
				schema: schema, identity: identity, options: options,
				store: physical, worker: worker, status: status,
				collectionClock: collectionClock, capturedCursor: cursor,
				lastCapture: time.Now(), search: search,
				lastSearch: time.Now(), subMgr: subMgr,
			}
			status.Mu.Lock()
			metrics := core.sqliteMetrics()
			status.Status.Sqlite = &metrics
			status.Mu.Unlock()
			return core, nil
		}()
		if err != nil {
			progress.Finish(err)
			startCh <- startResult{err: err}
			return
		}
		startCh <- startResult{identity: core.identity, worker: core.worker, store: core.store, subMgr: core.subMgr, writerGID: curGoroutineID()}
		for {
			delay := core.captureInterval()
			if elapsed := time.Since(core.lastCapture); elapsed < delay {
				delay -= elapsed
			} else {
				delay = 0
			}
			if core.search.SearchPending() {
				searchDelay := core.options.SearchInterval
				if elapsed := time.Since(core.lastSearch); elapsed < searchDelay {
					searchDelay -= elapsed
				} else {
					searchDelay = 0
				}
				if searchDelay < delay {
					delay = searchDelay
				}
			}
			timer := time.NewTimer(delay)
			var msg dbMessage
			var ok bool
			select {
			case msg, ok = <-tx:
				timer.Stop()
				if !ok {
					core.close()
					return
				}
			case <-timer.C:
			}
			if msg.job != nil {
				msg.job(core)
			} else if msg.stop != nil {
				msg.stop <- core.close()
				return
			}
			status.Mu.Lock()
			closed := status.Status.Closed
			status.Mu.Unlock()
			if closed {
				return
			}
			if time.Since(core.lastCapture) >= core.captureInterval() {
				_ = core.checkpoint()
				core.lastCapture = time.Now()
			}
			if core.search.SearchPending() && time.Since(core.lastSearch) >= core.options.SearchInterval {
				_ = core.maintainSearch(false)
				core.lastSearch = time.Now()
			}
		}
	}()
	started := <-startCh
	if started.err != nil {
		<-threadDone
		return nil, started.err
	}
	database := &Database{inner: &Inner{
		tx: tx, worker: started.worker, store: started.store,
		scheduler:  NewWriterScheduler(schedulerConfig),
		threadDone: threadDone, status: status,
		identity: started.identity, subMgr: started.subMgr,
	}}
	database.inner.writerGID.Store(started.writerGID)
	if flags != nil {
		if len(flags) > 0 {
			if owned, _ := database.TableConfigOwned(); owned || len(flags) > 0 {
				if err := database.SetTableReplicationMemory(flags); err != nil {
					database.Close()
					return nil, err
				}
			}
		} else if owned, _ := database.TableConfigOwned(); owned {
			if err := database.SetTableReplicationMemory(flags); err != nil {
				database.Close()
				return nil, err
			}
		}
	}
	if networkConfig != nil {
		handle, err := Attach(database, networkConfig)
		if err != nil {
			database.Close()
			return nil, err
		}
		database.inner.replicationMu.Lock()
		database.inner.replication = handle
		database.inner.replicationMu.Unlock()
	}
	return database, nil
}

// onWriterThread reports whether the caller is the writer goroutine.
// A shared busy flag is wrong here: engine background applies run writer
// jobs while user goroutines legitimately queue their own calls. Only a
// call issued FROM the writer goroutine itself would deadlock, mirroring
// the Rust thread-identity check.
func (db *Database) onWriterThread() bool {
	if os.Getenv("MURMUR_REENTRY") == "legacy" {
		return db.inner.active.Load()
	}
	return curGoroutineID() == db.inner.writerGID.Load()
}

// curGoroutineID parses the current goroutine id from runtime.Stack.
// Goroutine ids are process-unique and never reused, so this is a stable
// identity for reentrancy detection.
func curGoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := buf[:n]
	const prefix = "goroutine "
	if len(s) < len(prefix) || string(s[:len(prefix)]) != prefix {
		return -1
	}
	s = s[len(prefix):]
	var id int64
	for _, c := range s {
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + int64(c-'0')
	}
	return id
}

// Call runs f on the writer thread.
func (db *Database) Call(f func(*Core) (any, error)) (any, error) {
	if db.onWriterThread() {
		return nil, invalidErr("database callbacks cannot reenter the database")
	}
	db.inner.status.Mu.Lock()
	closed := db.inner.status.Status.Closed
	db.inner.status.Mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	reply := make(chan callResult, 1)
	select {
	case db.inner.tx <- dbMessage{job: func(c *Core) {
		db.inner.active.Store(true)
		// Clear before replying: the reply channel is buffered, so
		// the caller may issue its next Call before a deferred
		// store runs.
		defer func() {
			db.inner.active.Store(false)
			if p := recover(); p != nil {
				panic(p)
			}
		}()
		v, err := f(c)
		db.inner.active.Store(false)
		reply <- callResult{v: v, err: err}
	}}:
	case <-db.inner.threadDone:
		return nil, ErrClosed
	}
	select {
	case r := <-reply:
		return r.v, r.err
	case <-db.inner.threadDone:
		return nil, ErrClosed
	}
}

type callResult struct {
	v   any
	err error
}

// CallCancel runs f on the writer thread, aborting on cancel.
func (db *Database) CallCancel(f func(*Core) (any, error), cancelled *atomic.Bool) (any, error) {
	if cancelled.Load() {
		return nil, ErrClosed
	}
	reply := make(chan callResult, 1)
	select {
	case db.inner.tx <- dbMessage{job: func(c *Core) {
		defer func() {
			db.inner.active.Store(false)
			if p := recover(); p != nil {
				panic(p)
			}
		}()
		var v any
		var err error
		if cancelled.Load() {
			err = ErrClosed
		} else {
			db.inner.active.Store(true)
			v, err = f(c)
			db.inner.active.Store(false)
		}
		reply <- callResult{v: v, err: err}
	}}:
	case <-db.inner.threadDone:
		return nil, ErrClosed
	}
	for {
		select {
		case r := <-reply:
			return r.v, r.err
		case <-db.inner.threadDone:
			return nil, ErrClosed
		case <-time.After(20 * time.Millisecond):
			if cancelled.Load() {
				return nil, ErrClosed
			}
		}
	}
}

// Transaction runs f in a local write transaction.
func (db *Database) Transaction(f func(*Sql) (any, error)) (any, error) {
	ticket, err := db.inner.scheduler.Admit(WriterLocal)
	if err != nil {
		return nil, err
	}
	defer ticket.Release()
	return db.Call(func(c *Core) (any, error) { return c.transaction(f) })
}

// Read runs f with read access.
func (db *Database) Read(f func(*Sql) (any, error)) (any, error) {
	return db.Call(func(c *Core) (any, error) {
		c.access.Store(AccessRead)
		v, err := func() (v any, err error) {
			defer func() {
				if recover() != nil {
					v, err = nil, ErrCallbackPanicked
				}
			}()
			return f(&Sql{connection: c.connection, schema: &c.schema, access: c.access})
		}()
		c.access.Store(AccessNone)
		return v, err
	})
}

// Migrate applies a schema migration.
func (db *Database) Migrate(m Migration) error {
	ticket, err := db.inner.scheduler.Admit(WriterMaintenance)
	if err != nil {
		return err
	}
	defer ticket.Release()
	_, err = db.Call(func(c *Core) (any, error) { return nil, c.migrate(m) })
	return err
}

// RefreshIndexes collects writes and waits for search to catch up.
func (db *Database) RefreshIndexes() error {
	_, err := db.Call(func(c *Core) (any, error) {
		if err := c.checkpoint(); err != nil {
			return nil, err
		}
		return nil, c.maintainSearch(true)
	})
	return err
}

// SetSearchIndexes replaces explicit index definitions and rebuilds them.
func (db *Database) SetSearchIndexes(definitions []SearchIndex) error {
	_, err := db.Call(func(c *Core) (any, error) {
		if err := c.writable(); err != nil {
			return nil, err
		}
		if err := c.checkpoint(); err != nil {
			return nil, err
		}
		options := c.options
		options.SearchIndexes = definitions
		if err := options.Validate(); err != nil {
			return nil, err
		}
		search := NewSearchManager(&options)
		if err := RegisterSearchIndexes(c.connection, options.SearchIndexes); err != nil {
			return nil, err
		}
		if err := c.connection.ExecBatch("BEGIN IMMEDIATE"); err != nil {
			return nil, sqlErr(err)
		}
		if err := func() error {
			if err := c.search.DropSearchIndexes(c.connection); err != nil {
				return err
			}
			if err := search.SyncSearchSchema(c.connection, &c.schema, false); err != nil {
				return err
			}
			if _, err := search.SearchRefresh(c.connection, &c.schema); err != nil {
				return err
			}
			if err := c.connection.ExecBatch("COMMIT"); err != nil {
				return sqlErr(err)
			}
			return nil
		}(); err != nil {
			_ = c.connection.ExecBatch("ROLLBACK")
			return nil, err
		}
		c.options = options
		c.search = search
		if err := configureAuthorizer(c.connection, &c.schema, c.access, c.search.Guard); err != nil {
			return nil, err
		}
		c.status.Mu.Lock()
		c.status.Status.SearchIndexes = c.search.SearchStatus()
		c.status.Mu.Unlock()
		c.subMgr.NotifyReset("search indexes reconfigured")
		return nil, nil
	})
	return err
}

// SetColumnFts persists a column FTS declaration.
func (db *Database) SetColumnFts(table, column string, enabled bool) error {
	ticket, err := db.inner.scheduler.Admit(WriterMaintenance)
	if err != nil {
		return err
	}
	defer ticket.Release()
	_, err = db.Call(func(c *Core) (any, error) {
		return nil, c.setColumnSearch(table, column, func(col *Column) { col.Fts = enabled })
	})
	return err
}

// SetColumnVector persists or removes a vector declaration.
func (db *Database) SetColumnVector(table, column string, config *VectorColumnConfig) error {
	ticket, err := db.inner.scheduler.Admit(WriterMaintenance)
	if err != nil {
		return err
	}
	defer ticket.Release()
	_, err = db.Call(func(c *Core) (any, error) {
		return nil, c.setColumnSearch(table, column, func(col *Column) { col.Vector = config })
	})
	return err
}

// SetReplication persists a table's local replication setting.
func (db *Database) SetReplication(table string, enabled bool) error {
	ticket, err := db.inner.scheduler.Admit(WriterMaintenance)
	if err != nil {
		return err
	}
	defer ticket.Release()
	_, err = db.Call(func(c *Core) (any, error) { return nil, c.setTableReplication(table, enabled) })
	return err
}

// SetReplicationSink sets replication with a flags sink.
func (db *Database) SetReplicationSink(name string, enabled bool, sink TableFlagsSink) error {
	ticket, err := db.inner.scheduler.Admit(WriterMaintenance)
	if err != nil {
		return err
	}
	defer ticket.Release()
	_, err = db.Call(func(c *Core) (any, error) { return nil, c.setTableMode(name, enabled, sink) })
	return err
}

// SetTableReplicationMemory reconciles table modes from a config map.
func (db *Database) SetTableReplicationMemory(flags map[string]bool) error {
	_, err := db.Call(func(c *Core) (any, error) {
		type nameID struct{ name, id string }
		var tables []nameID
		for _, t := range c.schema.Tables {
			tables = append(tables, nameID{t.Name, t.ID})
		}
		for _, t := range tables {
			if err := c.setTableReplication(t.name, flags[t.id]); err != nil {
				return nil, err
			}
		}
		c.schema.TableReplication = flags
		return nil, nil
	})
	return err
}

// BindTableFlagsSink installs the flags sink.
func (db *Database) BindTableFlagsSink(sink TableFlagsSink) error {
	_, err := db.Call(func(c *Core) (any, error) { c.tableFlagsSink = sink; return nil, nil })
	return err
}

// TableConfigOwned reports whether the config-owned marker exists.
func (db *Database) TableConfigOwned() (bool, error) {
	v, err := db.inner.store.Get([]byte("00/schema/config-owned"))
	if err != nil {
		return false, storageErr("%v", err)
	}
	return v != nil, nil
}

// MarkTableConfigOwned persists the config-owned marker.
func (db *Database) MarkTableConfigOwned() error {
	_, err := db.Call(func(c *Core) (any, error) {
		reply := make(chan error, 1)
		select {
		case c.worker <- Work{Kind: WorkNetwork, Network: func(o *Opened) {
			err := o.Store.Commit([]spool.Mutation{{Key: []byte("00/schema/config-owned"), Value: []byte{1}}}, spool.DurabilitySync)
			reply <- err
		}}:
		case <-time.After(5 * time.Minute):
			return nil, ErrClosed
		}
		return nil, <-reply
	})
	return err
}

// ReplicationEnabled reads a table's local replication setting.
func (db *Database) ReplicationEnabled(table string) (bool, error) {
	v, err := db.Call(func(c *Core) (any, error) {
		for _, t := range c.schema.Tables {
			if asciiEqualFold(t.Name, table) {
				return ReplicationEnabled(&c.schema, t.ID), nil
			}
		}
		return false, invalidErr("unknown table: %s", table)
	})
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}

// Flush checkpoints and waits for durability.
func (db *Database) Flush() error {
	_, err := db.Call(func(c *Core) (any, error) { return nil, c.flush() })
	return err
}

// Backup captures an online backup into a new directory.
func (db *Database) Backup(destination string) error {
	_, err := db.Call(func(c *Core) (any, error) {
		if err := c.flush(); err != nil {
			return nil, err
		}
		reply := make(chan error, 1)
		select {
		case c.worker <- Work{Kind: WorkBackup, Destination: destination, BackupReply: reply}:
		case <-time.After(5 * time.Minute):
			return nil, ErrClosed
		}
		return nil, <-reply
	})
	return err
}

// Restore restores a backup directory, preserving identity.
func Restore(backup, destination string, key []byte) error {
	return RestoreWithOptions(backup, destination, key, DefaultRestoreOptions())
}

// RestoreWithOptions restores with optional fresh identity.
func RestoreWithOptions(backup, destination string, key []byte, options RestoreOptions) error {
	return RestoreBackup(backup, destination, key, &options)
}

// VerifyBackup verifies a backup directory opens and rebuilds.
func VerifyBackup(backup string, key []byte) error {
	return VerifyBackupDir(backup, key)
}

// RotateDataKey rotates the data encryption key.
func (db *Database) RotateDataKey() error {
	_, err := db.Call(func(c *Core) (any, error) {
		if err := c.flush(); err != nil {
			return nil, err
		}
		reply := make(chan error, 1)
		select {
		case c.worker <- Work{Kind: WorkRotate, RotateReply: reply}:
		case <-time.After(5 * time.Minute):
			return nil, ErrClosed
		}
		return nil, <-reply
	})
	return err
}

// RotateMasterKey replaces the 32-byte application key.
func (db *Database) RotateMasterKey(newKey [32]byte) error {
	ticket, err := db.inner.scheduler.Admit(WriterMaintenance)
	if err != nil {
		return err
	}
	defer ticket.Release()
	_, err = db.Call(func(c *Core) (any, error) {
		if err := c.flush(); err != nil {
			return nil, err
		}
		reply := make(chan error, 1)
		select {
		case c.worker <- Work{Kind: WorkRotateMasterKey, MasterKey: newKey[:], RotateReply: reply}:
		case <-time.After(5 * time.Minute):
			return nil, ErrClosed
		}
		return nil, <-reply
	})
	return err
}

// RetryStorage retries retained checkpoints.
func (db *Database) RetryStorage() error {
	_, err := db.Call(func(c *Core) (any, error) {
		if c.transitionFailed {
			return nil, storageErr("table transition interrupted; close and reopen the database")
		}
		reply := make(chan error, 1)
		select {
		case c.worker <- Work{Kind: WorkRetry, RetryReply: reply}:
		case <-time.After(5 * time.Minute):
			return nil, ErrClosed
		}
		if err := <-reply; err != nil {
			return nil, err
		}
		return nil, c.flush()
	})
	return err
}

// ExportState exports the replicated state.
func (db *Database) ExportState() (*ReplicaState, error) {
	v, err := db.Call(func(c *Core) (any, error) { return c.export(true) })
	if err != nil {
		return nil, err
	}
	return v.(*ReplicaState), nil
}

// MergeState merges a foreign state.
func (db *Database) MergeState(state *ReplicaState) error {
	for i := 0; i < 8; i++ {
		v, err := db.Call(func(c *Core) (any, error) {
			baseline, err := c.export(false)
			if err != nil {
				return nil, err
			}
			c.status.Mu.Lock()
			committed := c.status.Status.Committed
			c.status.Mu.Unlock()
			return &mergeBaseline{worker: c.worker, baseline: baseline, cursor: committed}, nil
		})
		if err != nil {
			return err
		}
		base := v.(*mergeBaseline)
		stateCopy := *state
		reply := make(chan MergeResult, 1)
		select {
		case base.worker <- Work{Kind: WorkMerge, Baseline: base.baseline, Cursor: base.cursor, State: &stateCopy, MergeReply: reply}:
		case <-time.After(5 * time.Minute):
			return ErrClosed
		}
		result := <-reply
		if result.Err == ErrBusy {
			continue
		}
		if result.Err != nil {
			return result.Err
		}
		patch := result.Patch
		_, err = db.Call(func(c *Core) (any, error) { return nil, c.applyPatch(patch) })
		if err == ErrBusy {
			continue
		}
		return err
	}
	return ErrBusy
}

type mergeBaseline struct {
	worker   chan<- Work
	baseline *ReplicaState
	cursor   uint64
}

// OpenProgress returns the recorded open progress.
func (db *Database) OpenProgress() *OpenProgress {
	db.inner.status.Mu.Lock()
	defer db.inner.status.Mu.Unlock()
	return db.inner.status.Status.OpenProgress
}

// SqliteMetrics samples SQLite metrics.
func (db *Database) SqliteMetrics() (*SqliteMetrics, error) {
	v, err := db.Call(func(c *Core) (any, error) { m := c.sqliteMetrics(); return &m, nil })
	if err != nil {
		return nil, err
	}
	return v.(*SqliteMetrics), nil
}

// SpoolMetrics samples spool metrics.
func (db *Database) SpoolMetrics() *SpoolMetrics {
	m := spoolMetricsFrom(db.inner.store.Stats())
	return &m
}

// Status snapshots database status.
func (db *Database) Status() Status {
	db.inner.status.Mu.Lock()
	s := db.inner.status.Status
	db.inner.status.Mu.Unlock()
	spoolMetrics := spoolMetricsFrom(db.inner.store.Stats())
	s.Spool = &spoolMetrics
	schedulerSnapshot := db.inner.scheduler.Snapshot()
	s.Scheduler = &schedulerSnapshot
	db.inner.replicationMu.Lock()
	if db.inner.replication != nil {
		replicationStatus := db.inner.replication.Status()
		s.Replication = &replicationStatus
	}
	db.inner.replicationMu.Unlock()
	return s
}

// Scheduler returns the writer scheduler.
func (db *Database) Scheduler() *WriterScheduler {
	return db.inner.scheduler
}

// DatabaseID returns the database id.
func (db *Database) DatabaseID() [16]byte {
	return db.inner.identity.Database
}

// NodeID returns the node id.
func (db *Database) NodeID() [16]byte {
	return db.inner.identity.Node
}

// KeyInventory reports key inventory.
func (db *Database) KeyInventory() spool.KeyInventory {
	return db.inner.store.KeyInventory()
}

// Schema returns the current schema.
func (db *Database) Schema() (*Schema, error) {
	v, err := db.Call(func(c *Core) (any, error) { s := CloneSchema(c.schema); return &s, nil })
	if err != nil {
		return nil, err
	}
	return v.(*Schema), nil
}

// SchemaWithSearchStatus reads schema plus search status atomically.
func (db *Database) SchemaWithSearchStatus() (*Schema, []SearchIndexStatus, error) {
	v, err := db.Call(func(c *Core) (any, error) {
		s := CloneSchema(c.schema)
		return &schemaWithSearch{schema: &s, status: c.search.SearchStatus()}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	out := v.(*schemaWithSearch)
	return out.schema, out.status, nil
}

type schemaWithSearch struct {
	schema *Schema
	status []SearchIndexStatus
}

// Subscribe creates a reactive query subscription.
func SubscribeQuery[T any](db *Database, query func(*Sql) ([]T, error), keyFn func(*T) []byte, options SubscriptionOptions) (*Subscription[T], error) {
	return StartSubscription(db, query, keyFn, options)
}

// Replication returns the replication handle, if enabled.
func (db *Database) Replication() *replication.Handle {
	db.inner.replicationMu.Lock()
	defer db.inner.replicationMu.Unlock()
	return db.inner.replication
}

// Close closes the database.
func (db *Database) Close() error {
	if db.onWriterThread() {
		return invalidErr("database callbacks cannot close the database")
	}
	db.inner.status.Mu.Lock()
	if db.inner.status.Status.Closed {
		db.inner.status.Mu.Unlock()
		return nil
	}
	db.inner.status.Mu.Unlock()
	db.inner.subMgr.Close()
	db.inner.scheduler.Close()
	db.inner.replicationMu.Lock()
	handle := db.inner.replication
	db.inner.replication = nil
	db.inner.replicationMu.Unlock()
	if handle != nil {
		if err := handle.Close(); err != nil {
			return storageErr("%v", err)
		}
	}
	reply := make(chan error, 1)
	select {
	case db.inner.tx <- dbMessage{stop: reply}:
	case <-db.inner.threadDone:
		return ErrClosed
	}
	select {
	case err := <-reply:
		<-db.inner.threadDone
		return err
	case <-db.inner.threadDone:
		return ErrClosed
	}
}

// Core is the writer-thread state.
type Core struct {
	capture          *Capture
	pendingDelta     []byte
	pendingRows      uint64
	connection       *sqlite.Conn
	access           *atomic.Uint32
	schema           Schema
	identity         Identity
	options          Options
	store            *spool.Store
	worker           chan<- Work
	status           *StatusState
	collectionClock  Version
	capturedCursor   uint64
	lastCapture      time.Time
	pendingIntents   []PolicyIntent
	tableFlagsSink   TableFlagsSink
	transitionFailed bool
	search           *SearchManager
	lastSearch       time.Time
	subMgr           *SubManager
}

func (c *Core) maintainSearch(all bool) error {
	var err error
	if all {
		_, err = c.search.SearchRefresh(c.connection, &c.schema)
	} else {
		_, err = c.search.SearchTick(c.connection, &c.schema)
	}
	c.status.Mu.Lock()
	c.status.Status.SearchIndexes = c.search.SearchStatus()
	c.status.Mu.Unlock()
	published := c.search.TakeSearchPublications()
	if len(published) > 0 {
		c.status.Mu.Lock()
		cursor := c.status.Status.Committed
		c.status.Mu.Unlock()
		c.subMgr.NotifyCommit(cursor, published)
	}
	return err
}

func (c *Core) sqliteMetrics() SqliteMetrics {
	memoryUsed := sqlite.MemoryUsed()
	memoryHighwater := sqlite.MemoryHighwater()
	pageSize := pragmaInt(c.connection, "PRAGMA page_size")
	pageCount := pragmaInt(c.connection, "PRAGMA page_count")
	freelist := pragmaInt(c.connection, "PRAGMA freelist_count")
	schemaVersion := pragmaInt(c.connection, "PRAGMA schema_version")
	var totalRows uint64
	for _, table := range c.schema.Tables {
		row, err := c.connection.QueryRow("SELECT COUNT(*) FROM " + quote(table.Name))
		if err == nil && len(row) > 0 && row[0].Int > 0 {
			totalRows += uint64(row[0].Int)
		}
	}
	return SqliteMetrics{
		MemoryUsedBytes: memoryUsed, MemoryHighwaterBytes: memoryHighwater,
		PageSize: pageSize, PageCount: pageCount,
		DatabaseSizeBytes: pageSize * pageCount,
		FreelistPages:     freelist, SchemaVersion: schemaVersion,
		TableCount: len(c.schema.Tables), IndexCount: len(c.schema.Indexes),
		TotalRows:             totalRows,
		SessionMemoryBytes:    int(c.uncheckpointedMemory()),
		SessionChangesetBytes: int(c.uncheckpointedSize()),
	}
}

func pragmaInt(c *sqlite.Conn, pragma string) int64 {
	row, err := c.QueryRow(pragma)
	if err != nil || len(row) == 0 {
		return 0
	}
	return row[0].Int
}

func (c *Core) writable() error {
	if c.transitionFailed {
		return storageErr("table transition interrupted; close and reopen the database")
	}
	c.status.Mu.Lock()
	defer c.status.Mu.Unlock()
	if c.status.Status.Closed {
		return ErrClosed
	}
	if c.status.Status.StorageError != nil {
		return storageErr("%s", *c.status.Status.StorageError)
	}
	if c.status.Status.PendingBytes >= c.options.QueueBytes {
		return ErrBusy
	}
	return nil
}

// Admit checks queue admission for bytes.
func (c *Core) Admit(size int) error {
	c.status.Mu.Lock()
	defer c.status.Mu.Unlock()
	if c.status.Status.PendingBytes == 0 && size > c.options.QueueBytes &&
		c.options.Replication != nil && size <= c.options.Replication.MaxTransactionBytes*2+4<<20 {
		return nil
	}
	if c.status.Status.PendingBytes+size < c.status.Status.PendingBytes ||
		c.status.Status.PendingBytes+size > c.options.QueueBytes {
		return ErrBusy
	}
	return nil
}

func (c *Core) bump() (uint64, error) {
	c.status.Mu.Lock()
	defer c.status.Mu.Unlock()
	if c.status.Status.Committed == ^uint64(0) {
		return 0, invalidErr("commit cursor exhausted")
	}
	c.status.Status.Committed++
	return c.status.Status.Committed, nil
}

func (c *Core) send(event *Event, cursor uint64, size int) error {
	if err := c.Admit(size); err != nil {
		return err
	}
	c.status.Mu.Lock()
	c.status.Status.PendingBytes += size
	c.status.Mu.Unlock()
	select {
	case c.worker <- Work{Kind: WorkCheckpoint, Event: event, Cursor: cursor, Bytes: size}:
		return nil
	case <-time.After(5 * time.Minute):
		c.status.Mu.Lock()
		msg := "checkpoint worker stopped"
		c.status.Status.StorageError = &msg
		c.status.Mu.Unlock()
		return ErrClosed
	}
}

func (c *Core) captureInterval() time.Duration {
	if c.options.Replication != nil && c.options.Replication.AdaptiveSync {
		c.status.Mu.Lock()
		pending := c.status.Status.PendingBytes
		c.status.Mu.Unlock()
		fast := 100 * time.Millisecond
		if pending < c.options.QueueBytes/2 {
			fast = time.Second
		}
		if fast < c.options.CaptureInterval {
			return fast
		}
	}
	return c.options.CaptureInterval
}

func (c *Core) uncheckpointedSize() int {
	total := len(c.pendingDelta)
	if c.capture != nil {
		total += c.capture.Size()
	}
	return total
}

func (c *Core) uncheckpointedMemory() int {
	total := len(c.pendingDelta)
	if c.capture != nil {
		total += c.capture.Memory()
	}
	return total
}

func (c *Core) uncheckpointedBytes() ([]byte, error) {
	out := append([]byte(nil), c.pendingDelta...)
	if c.capture != nil {
		changes, err := c.capture.Bytes()
		if err != nil {
			return nil, err
		}
		out = append(out, changes...)
	}
	return out, nil
}

func (c *Core) resetCapture() {
	fresh, err := CaptureSchema(c.connection, &c.schema)
	if err != nil {
		c.status.Mu.Lock()
		msg := err.Error()
		c.status.Status.StorageError = &msg
		c.status.Mu.Unlock()
		return
	}
	if c.capture != nil {
		c.capture.Close()
	}
	c.capture = fresh
}

// AdvanceDisjointPatch advances a patch cursor across unrelated captures.
func (c *Core) AdvanceDisjointPatch(patch *Patch, keys map[RowID]bool) (bool, error) {
	c.status.Mu.Lock()
	if c.status.Status.StorageError != nil {
		c.status.Mu.Unlock()
		return false, nil
	}
	committed := c.status.Status.Committed
	c.status.Mu.Unlock()
	if committed == patch.Cursor {
		return true, nil
	}
	if c.capturedCursor != patch.Cursor {
		return false, nil
	}
	if len(c.pendingIntents) > 0 {
		return false, nil
	}
	changes, err := c.uncheckpointedBytes()
	if err != nil {
		return false, err
	}
	overlap, err := ChangesetOverlaps(&c.schema, changes, keys)
	if err != nil {
		return false, err
	}
	if overlap {
		return false, nil
	}
	if err := c.checkpoint(); err != nil {
		return false, err
	}
	patch.Cursor = committed
	return true, nil
}

func (c *Core) checkpoint() error {
	if err := c.writable(); err != nil {
		return err
	}
	start := time.Now()
	if c.capture == nil {
		return ErrClosed
	}
	estimate := c.uncheckpointedSize()
	c.status.Mu.Lock()
	committed := c.status.Status.Committed
	c.status.Mu.Unlock()
	if estimate == 0 && committed == c.capturedCursor {
		c.lastCapture = time.Now()
		return nil
	}
	if err := c.Admit(estimate); err != nil {
		return err
	}
	changes, err := c.uncheckpointedBytes()
	if err != nil {
		return err
	}
	version, err := c.collectionClock.Next(c.identity.Node)
	if err != nil {
		return err
	}
	length := len(changes) + len(EncodePolicyIntents(c.pendingIntents))
	if err := c.Admit(length); err != nil {
		return err
	}
	c.status.Mu.Lock()
	cursor := c.status.Status.Committed
	c.status.Mu.Unlock()
	intents := c.pendingIntents
	c.pendingIntents = nil
	affected := TablesInChangeset(changes)
	c.search.EnqueueSearch(changes, affected)
	c.status.Mu.Lock()
	c.status.Status.SearchIndexes = c.search.SearchStatus()
	c.status.Mu.Unlock()
	if err := c.send(&Event{Tag: EventDelta, Bytes: changes, Version: version, Intents: intents}, cursor, length); err != nil {
		return err
	}
	c.pendingDelta = nil
	c.pendingRows = 0
	fresh, err := CaptureSchema(c.connection, &c.schema)
	if err != nil {
		return err
	}
	if c.capture != nil {
		c.capture.Close()
	}
	c.capture = fresh
	c.collectionClock = version
	c.capturedCursor = cursor
	c.subMgr.NotifyCommit(cursor, affected)
	metrics := c.sqliteMetrics()
	c.status.Mu.Lock()
	c.status.Status.CheckpointPause = time.Since(start)
	c.status.Status.Sqlite = &metrics
	c.status.Mu.Unlock()
	c.lastCapture = time.Now()
	return nil
}

func (c *Core) flush() error {
	if err := c.checkpoint(); err == ErrBusy {
		if err := c.barrier(); err != nil {
			return err
		}
		if err := c.checkpoint(); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return c.barrier()
}

func (c *Core) barrier() error {
	reply := make(chan error, 1)
	select {
	case c.worker <- Work{Kind: WorkBarrier, BarrierReply: reply}:
	case <-time.After(30 * time.Second):
		return ErrClosed
	}
	return <-reply
}

func (c *Core) export(replicatedOnly bool) (*ReplicaState, error) {
	if err := c.flush(); err != nil {
		return nil, err
	}
	rows, err := ReadRows(c.connection, &c.schema)
	if err != nil {
		return nil, err
	}
	reply := make(chan ExportResult, 1)
	select {
	case c.worker <- Work{Kind: WorkExport, Rows: rows, ReplicatedOnly: replicatedOnly, ExportReply: reply}:
	case <-time.After(30 * time.Second):
		return nil, ErrClosed
	}
	result := <-reply
	return result.State, result.Err
}

func (c *Core) finishCommit() error {
	if c.uncheckpointedMemory() > c.options.TransactionBytes ||
		c.uncheckpointedSize() > c.options.TransactionBytes {
		return limitErr("transaction/session capture budget exceeded")
	}
	return c.Admit(c.uncheckpointedSize())
}

func (c *Core) transaction(f func(*Sql) (any, error)) (any, error) {
	if err := c.writable(); err != nil {
		return nil, err
	}
	var intents []PolicyIntent
	beforeSize := len(c.pendingDelta)
	beforeChanges := c.connection.TotalChanges()
	if err := c.connection.ExecBatch("BEGIN IMMEDIATE"); err != nil {
		return nil, sqlErr(err)
	}
	value, err := func() (v any, err error) {
		c.access.Store(AccessWrite)
		func() {
			defer func() {
				if recover() != nil {
					v, err = nil, ErrCallbackPanicked
				}
			}()
			v, err = f(&Sql{connection: c.connection, schema: &c.schema, intents: &intents, access: c.access})
		}()
		c.access.Store(AccessNone)
		if err != nil {
			return nil, err
		}
		dirty := c.connection.TotalChanges() != beforeChanges
		if ferr := c.finishCommit(); ferr != nil {
			if _, ok := ferr.(*Error); ok && ferr.(*Error).Kind == KindLimit &&
				beforeSize > 0 && c.uncheckpointedSize()-beforeSize <= c.options.TransactionBytes {
				return nil, ErrBusy
			}
			return nil, ferr
		}
		intentBytes := len(EncodePolicyIntents(intents)) + len(EncodePolicyIntents(c.pendingIntents))
		captured := c.uncheckpointedSize()
		if captured+intentBytes < captured || captured+intentBytes > c.options.TransactionBytes {
			return nil, limitErr("policy intent/session budget exceeded")
		}
		if err := c.Admit(captured + intentBytes); err != nil {
			return nil, err
		}
		if err := c.connection.ExecBatch("COMMIT"); err != nil {
			return nil, sqlErr(err)
		}
		c.pendingIntents = append(c.pendingIntents, intents...)
		touched := uint64(0)
		if delta := c.connection.TotalChanges() - beforeChanges; delta > 0 {
			touched = uint64(delta)
		}
		if c.capture != nil {
			committed, err := c.capture.Bytes()
			if err != nil {
				return nil, err
			}
			if len(committed) > 0 {
				c.pendingDelta = append(c.pendingDelta, committed...)
				c.pendingRows += touched
				c.resetCapture()
			}
		}
		if dirty {
			if _, err := c.bump(); err != nil {
				return nil, err
			}
		}
		budget := c.options.TransactionBytes
		if budget < 1024 {
			budget = 1024
		}
		rowCap := c.options.TransactionBytes
		if rowCap < 8192 {
			rowCap = 8192
		}
		if len(c.pendingDelta) > budget/8 || c.pendingRows > uint64(rowCap)/512 {
			_ = c.checkpoint()
		}
		return v, nil
	}()
	if err != nil {
		c.access.Store(AccessNone)
		_ = c.connection.ExecBatch("ROLLBACK")
		if c.capture != nil {
			c.resetCapture()
		}
	}
	return value, err
}

func (c *Core) setColumnSearch(table, column string, change func(*Column)) error {
	if err := c.writable(); err != nil {
		return err
	}
	if asciiEqualFold(column, ManagedKeyColumn) {
		return invalidErr("column search declarations require an application column")
	}
	target := c.schema
	target.Tables = make([]Table, len(c.schema.Tables))
	for i := range c.schema.Tables {
		target.Tables[i] = c.schema.Tables[i]
		target.Tables[i].Columns = append([]Column(nil), c.schema.Tables[i].Columns...)
	}
	foundTable := false
	for i := range target.Tables {
		if asciiEqualFold(target.Tables[i].Name, table) {
			foundTable = true
			foundColumn := false
			for j := range target.Tables[i].Columns {
				if asciiEqualFold(target.Tables[i].Columns[j].Name, column) {
					change(&target.Tables[i].Columns[j])
					foundColumn = true
					break
				}
			}
			if !foundColumn {
				return invalidErr("unknown column: %s.%s", target.Tables[i].Name, column)
			}
			break
		}
	}
	if !foundTable {
		return invalidErr("unknown table: %s", table)
	}
	if schemasEqual(&target, &c.schema) {
		return nil
	}
	validation := c.options
	definitions, err := SchemaSearchDefinitions(validation.SearchIndexes, &target)
	if err != nil {
		return err
	}
	validation.SearchIndexes = definitions
	if err := validation.Validate(); err != nil {
		return err
	}
	if err := NewSearchManager(&validation).ValidateSearchSchema(&target, false); err != nil {
		return err
	}
	if err := c.checkpoint(); err != nil {
		return err
	}
	if err := RegisterSearchIndexes(c.connection, validation.SearchIndexes); err != nil {
		return err
	}
	target.History = hash(append(append([]byte(nil), c.schema.History[:]...), EncodeSchema(target)...))
	version, err := c.collectionClock.Next(c.identity.Node)
	if err != nil {
		return err
	}
	size := len(EncodeSchema(target))
	if err := c.Admit(size); err != nil {
		return err
	}
	previous := CloneSearchManager(c.search)
	if err := c.connection.ExecBatch("BEGIN IMMEDIATE"); err != nil {
		return sqlErr(err)
	}
	if err := func() error {
		if err := c.search.SyncSearchSchema(c.connection, &target, false); err != nil {
			return err
		}
		if _, err := c.search.SearchRefresh(c.connection, &target); err != nil {
			return err
		}
		if err := c.connection.ExecBatch("COMMIT"); err != nil {
			return sqlErr(err)
		}
		return nil
	}(); err != nil {
		_ = c.connection.ExecBatch("ROLLBACK")
		c.search = previous
		c.search.PublishSearchGuard()
		return err
	}
	c.schema = target
	c.collectionClock = version
	if err := configureAuthorizer(c.connection, &c.schema, c.access, c.search.Guard); err != nil {
		return err
	}
	c.status.Mu.Lock()
	c.status.Status.SearchIndexes = c.search.SearchStatus()
	c.status.Mu.Unlock()
	cursor, err := c.bump()
	if err != nil {
		return err
	}
	// The event crosses to the storage worker; it must own its backing.
	schemaCopy := CloneSchema(c.schema)
	if err := c.send(&Event{Tag: EventSchemaCaptured, Schema: schemaCopy, Version: version}, cursor, size); err != nil {
		return err
	}
	c.capturedCursor = cursor
	c.subMgr.NotifyReset("column search setting changed")
	return nil
}

func (c *Core) setTableReplication(name string, enabled bool) error {
	return c.setTableMode(name, enabled, c.tableFlagsSink)
}

func (c *Core) setTableMode(name string, enabled bool, sink TableFlagsSink) error {
	if err := c.writable(); err != nil {
		return err
	}
	var old *Table
	for i := range c.schema.Tables {
		if asciiEqualFold(c.schema.Tables[i].Name, name) {
			oldCopy := c.schema.Tables[i]
			old = &oldCopy
			break
		}
	}
	if old == nil {
		return invalidErr("unknown table: %s", name)
	}
	if (old.ApplicationKey != nil) == enabled {
		if c.schema.TableReplication[old.ID] != enabled {
			if err := c.flush(); err != nil {
				return err
			}
			schemaCopy := CloneSchema(c.schema)
			if schemaCopy.TableReplication == nil {
				schemaCopy.TableReplication = make(map[string]bool)
			}
			schemaCopy.TableReplication[old.ID] = enabled
			version, err := c.collectionClock.Next(c.identity.Node)
			if err != nil {
				return err
			}
			size := len(EncodeSchema(schemaCopy))
			if err := c.Admit(size); err != nil {
				return err
			}
			cursor, err := c.bump()
			if err != nil {
				return err
			}
			if err := c.send(&Event{Tag: EventSchemaCaptured, Schema: schemaCopy, Version: version}, cursor, size); err != nil {
				return err
			}
			c.capturedCursor = cursor
			c.schema = schemaCopy
			c.collectionClock = version
		}
		if err := c.barrier(); err != nil {
			return err
		}
		if sink != nil {
			return sink(c.schema.TableReplication)
		}
		return nil
	}
	if err := c.flush(); err != nil {
		return err
	}
	var appKey ApplicationKey
	if old.ApplicationKey != nil {
		appKey = *old.ApplicationKey
	} else {
		inspected, err := InspectKey(c.connection, old)
		if err != nil {
			return err
		}
		appKey = inspected
	}
	for _, collation := range appKey.Collations {
		if !asciiEqualFold(collation, "BINARY") && !asciiEqualFold(collation, "NOCASE") && !asciiEqualFold(collation, "RTRIM") {
			return invalidErr("unsupported application primary-key collation")
		}
	}
	before, err := ReadTableRows(c.connection, old)
	if err != nil {
		return err
	}
	replacement := *old
	sql, err := Definition(old, &appKey, enabled)
	if err != nil {
		return err
	}
	replacement.SQL = sql
	if enabled {
		replacement.ApplicationKey = &appKey
	} else {
		replacement.ApplicationKey = nil
	}
	for i := range replacement.Columns {
		column := &replacement.Columns[i]
		if enabled {
			column.PrimaryKey = 0
		} else {
			column.PrimaryKey = 0
			for j, n := range appKey.Columns {
				if n == column.Name {
					column.PrimaryKey = j + 1
					break
				}
			}
		}
		if enabled {
			for _, n := range appKey.Columns {
				if n == column.Name {
					column.NotNull = true
				}
			}
		}
	}
	if enabled {
		replacement.Columns = append(replacement.Columns, Column{
			ID:   id(append(append([]byte(nil), old.ID...), ManagedKeyColumn...)),
			Name: ManagedKeyColumn, PrimaryKey: 1, DeclaredType: "TEXT", NotNull: true, Policy: MergePolicyLww,
		})
	} else {
		kept := replacement.Columns[:0]
		for _, col := range replacement.Columns {
			if col.Name != ManagedKeyColumn {
				kept = append(kept, col)
			}
		}
		replacement.Columns = kept
	}
	intended := c.schema
	for i := range intended.Tables {
		if intended.Tables[i].ID == old.ID {
			intended.Tables[i] = replacement
			break
		}
	}
	if intended.TableReplication == nil {
		intended.TableReplication = make(map[string]bool)
	}
	intended.TableReplication[old.ID] = enabled
	after := make([]RowState, 0, len(before))
	for _, row := range before {
		rowCopy := row
		var appValues []Value
		for _, n := range appKey.Columns {
			var column *Column
			for i := range old.Columns {
				if old.Columns[i].Name == n {
					column = &old.Columns[i]
					break
				}
			}
			if column == nil {
				return ErrSchemaMismatch
			}
			found := false
			for _, cell := range row.Cells {
				if cell.Column == column.ID {
					appValues = append(appValues, cell.Value)
					found = true
					break
				}
			}
			if !found {
				return ErrSchemaMismatch
			}
		}
		if enabled {
			uuid, err := ManagedKeyUUID(old.Name, appKey.Collations, appValues)
			if err != nil {
				return err
			}
			value := TextValue(uuid)
			rowCopy.Key = []Value{value}
			rowCopy.Cells = append(rowCopy.Cells, Cell{Column: replacement.Columns[len(replacement.Columns)-1].ID, Value: value})
		} else {
			rowCopy.Key = appValues
			var managed *Column
			for i := range old.Columns {
				if old.Columns[i].Name == ManagedKeyColumn {
					managed = &old.Columns[i]
					break
				}
			}
			if managed == nil {
				return ErrSchemaMismatch
			}
			kept := rowCopy.Cells[:0]
			for _, cell := range rowCopy.Cells {
				if cell.Column != managed.ID {
					kept = append(kept, cell)
				}
			}
			rowCopy.Cells = kept
		}
		after = append(after, rowCopy)
	}
	previousSearch := CloneSearchManager(c.search)
	if c.capture != nil {
		c.capture.Close()
		c.capture = nil
	}
	if err := c.connection.ExecBatch("BEGIN IMMEDIATE"); err != nil {
		return sqlErr(err)
	}
	published := false
	result := func() error {
		if err := c.connection.ExecBatch("DROP TABLE " + quote(old.Name)); err != nil {
			return sqlErr(err)
		}
		if err := c.connection.ExecBatch(replacement.SQL); err != nil {
			return sqlErr(err)
		}
		for i := range after {
			if err := PutUserRow(c.connection, &replacement, &after[i]); err != nil {
				return err
			}
		}
		for _, sql := range c.schema.Indexes {
			tableName, ok := IndexTable(sql)
			if !ok || !asciiEqualFold(tableName, old.Name) {
				continue
			}
			if !enabled {
				references, err := ReferencesManagedColumn(sql)
				if err != nil {
					return err
				}
				if references {
					continue
				}
			}
			if err := c.connection.ExecBatch(sql); err != nil {
				return sqlErr(err)
			}
		}
		emptyMigration := NewMigration("")
		inspected, err := Inspect(c.connection, &intended, &emptyMigration)
		if err != nil {
			return err
		}
		intended = *inspected
		intended.History = c.schema.History
		if err := c.search.SyncSearchSchema(c.connection, &intended, true); err != nil {
			return err
		}
		if _, err := c.search.SearchRefresh(c.connection, &intended); err != nil {
			return err
		}
		version, err := c.collectionClock.Next(c.identity.Node)
		if err != nil {
			return err
		}
		size := len(EncodeRowStates(before)) + len(EncodeRowStates(after)) + len(EncodeSchema(intended))
		if err := c.Admit(size); err != nil {
			return err
		}
		cursor, err := c.bump()
		if err != nil {
			return err
		}
		if sink != nil {
			if err := sink(intended.TableReplication); err != nil {
				return err
			}
		}
		if err := c.send(&Event{Tag: EventTableReplication, Table: old.ID, Enabled: enabled, Schema: CloneSchema(intended), Before: before, After: after, Version: version}, cursor, size); err != nil {
			return err
		}
		c.capturedCursor = cursor
		published = true
		if err := c.barrier(); err != nil {
			return err
		}
		if err := c.connection.ExecBatch("COMMIT"); err != nil {
			return sqlErr(err)
		}
		c.schema = intended
		c.collectionClock = version
		return nil
	}()
	if result != nil {
		_ = c.connection.ExecBatch("ROLLBACK")
		c.search = previousSearch
		c.search.PublishSearchGuard()
		if published {
			c.transitionFailed = true
			c.status.Mu.Lock()
			msg := "table transition interrupted; close and reopen the database"
			c.status.Status.StorageError = &msg
			c.status.Mu.Unlock()
		}
	}
	capture, err := CaptureSchema(c.connection, &c.schema)
	if err != nil {
		return err
	}
	c.capture = capture
	if err := configureAuthorizer(c.connection, &c.schema, c.access, c.search.Guard); err != nil {
		return err
	}
	c.status.Mu.Lock()
	c.status.Status.SearchIndexes = c.search.SearchStatus()
	c.status.Mu.Unlock()
	if result == nil {
		c.subMgr.NotifyReset("table replication mode changed")
	}
	return result
}

func (c *Core) migrate(m Migration) error {
	if err := c.writable(); err != nil {
		return err
	}
	if err := c.checkpoint(); err != nil {
		return err
	}
	previousSearch := CloneSearchManager(c.search)
	if c.capture != nil {
		c.capture.Close()
		c.capture = nil
	}
	keywords := Keywords(m.SQL)
	indexOnly := len(keywords) > 0 && len(m.Mappings) == 0
	if indexOnly {
		allowed := map[string]bool{"CREATE": true, "UNIQUE": true, "INDEX": true, "IF": true, "NOT": true,
			"EXISTS": true, "ON": true, "DROP": true, "ASC": true, "DESC": true, "WHERE": true, "COLLATE": true}
		for _, k := range keywords {
			if !allowed[k] {
				indexOnly = false
				break
			}
		}
	}
	var candidateTables []Table
	if !indexOnly {
		sqlLower := strings.ToLower(m.SQL)
		for _, t := range c.schema.Tables {
			nameLower := strings.ToLower(t.Name)
			inSQL := strings.Contains(sqlLower, nameLower)
			inMap := false
			for _, mapping := range m.Mappings {
				if asciiEqualFold(mapping.OldName, t.Name) || asciiEqualFold(mapping.NewName, t.Name) {
					inMap = true
					break
				}
			}
			if inSQL || inMap {
				candidateTables = append(candidateTables, t)
			}
		}
	}
	var beforeCandidateRows []RowState
	for i := range candidateTables {
		rows, err := ReadTableRows(c.connection, &candidateTables[i])
		if err != nil {
			return err
		}
		beforeCandidateRows = append(beforeCandidateRows, rows...)
	}
	totalBefore := c.connection.TotalChanges()
	if err := c.connection.ExecBatch("BEGIN IMMEDIATE"); err != nil {
		return sqlErr(err)
	}
	type migrateOut struct {
		schema  Schema
		chunks  []MigrationChunk
		version Version
	}
	out, err := func() (*migrateOut, error) {
		c.access.Store(AccessMigration)
		statements, serr := Statements(m.SQL)
		if serr == nil {
			for _, statement := range statements {
				rewritten, rerr := Rewrite(statement, &c.schema)
				if rerr != nil {
					serr = rerr
					break
				}
				if serr = c.connection.ExecBatch(rewritten); serr != nil {
					serr = sqlErr(serr)
					break
				}
			}
		}
		c.access.Store(AccessNone)
		if serr != nil {
			return nil, serr
		}
		inspected, err := Inspect(c.connection, &c.schema, &m)
		if err != nil {
			return nil, err
		}
		newSchema := *inspected
		if err := ConfigurePolicy(&newSchema, c.options.Replication); err != nil {
			return nil, err
		}
		if err := c.search.SyncSearchSchema(c.connection, &newSchema, true); err != nil {
			return nil, err
		}
		if _, err := c.search.SearchRefresh(c.connection, &newSchema); err != nil {
			return nil, err
		}
		dmlOccurred := totalBefore != c.connection.TotalChanges()
		var affectedBefore, affectedAfter []RowState
		if !indexOnly || dmlOccurred {
			for _, oldT := range c.schema.Tables {
				var newT *Table
				for i := range newSchema.Tables {
					if newSchema.Tables[i].ID == oldT.ID {
						newT = &newSchema.Tables[i]
						break
					}
				}
				if newT == nil {
					for _, r := range beforeCandidateRows {
						if r.Table == oldT.ID {
							affectedBefore = append(affectedBefore, r)
						}
					}
					continue
				}
				schemaChanged := oldT.Name != newT.Name || !columnsEqual(oldT.Columns, newT.Columns)
				inCandidates := false
				for _, t := range candidateTables {
					if t.ID == oldT.ID {
						inCandidates = true
						break
					}
				}
				if schemaChanged || (dmlOccurred && inCandidates) {
					for _, r := range beforeCandidateRows {
						if r.Table == oldT.ID {
							affectedBefore = append(affectedBefore, r)
						}
					}
					rows, err := ReadTableRows(c.connection, newT)
					if err != nil {
						return nil, err
					}
					affectedAfter = append(affectedAfter, rows...)
				}
			}
			for i := range newSchema.Tables {
				found := false
				for _, t := range c.schema.Tables {
					if t.ID == newSchema.Tables[i].ID {
						found = true
						break
					}
				}
				if !found {
					rows, err := ReadTableRows(c.connection, &newSchema.Tables[i])
					if err != nil {
						return nil, err
					}
					affectedAfter = append(affectedAfter, rows...)
				}
			}
		}
		maxChunk := c.options.TransactionBytes / 2
		if maxChunk > 2<<20 {
			maxChunk = 2 << 20
		}
		chunks := ChunkMigrationRows(affectedBefore, affectedAfter, 2000, maxChunk)
		version, err := c.collectionClock.Next(c.identity.Node)
		if err != nil {
			return nil, err
		}
		if err := c.connection.ExecBatch("COMMIT"); err != nil {
			return nil, sqlErr(err)
		}
		return &migrateOut{schema: newSchema, chunks: chunks, version: version}, nil
	}()
	if err != nil {
		c.access.Store(AccessNone)
		_ = c.connection.ExecBatch("ROLLBACK")
		c.search = previousSearch
		c.search.PublishSearchGuard()
		c.status.Mu.Lock()
		c.status.Status.SearchIndexes = c.search.SearchStatus()
		c.status.Mu.Unlock()
		capture, cerr := CaptureSchema(c.connection, &c.schema)
		if cerr != nil {
			return cerr
		}
		c.capture = capture
		return err
	}
	c.collectionClock = out.version
	c.schema = out.schema
	if err := configureAuthorizer(c.connection, &c.schema, c.access, c.search.Guard); err != nil {
		return err
	}
	c.status.Mu.Lock()
	c.status.Status.SearchIndexes = c.search.SearchStatus()
	c.status.Mu.Unlock()
	capture, err := CaptureSchema(c.connection, &c.schema)
	if err != nil {
		return err
	}
	c.capture = capture
	if len(out.chunks) == 0 {
		size := len(m.SQL) + len(EncodeSchema(out.schema))
		if size > c.options.TransactionBytes {
			return limitErr("migration capture budget exceeded")
		}
		cursor, err := c.bump()
		if err != nil {
			return err
		}
		if err := c.send(&Event{Tag: EventSchemaCaptured, Migration: m.SQL, Schema: CloneSchema(out.schema), Version: out.version}, cursor, size); err != nil {
			return err
		}
		c.capturedCursor = cursor
	} else {
		for i, chunk := range out.chunks {
			chunkSize := len(EncodeRowStates(chunk.Before)) + len(EncodeRowStates(chunk.After))
			if i == 0 {
				chunkSize += len(m.SQL) + len(EncodeSchema(out.schema))
			}
			if chunkSize > c.options.TransactionBytes {
				return limitErr("migration capture budget exceeded")
			}
			cursor, err := c.bump()
			if err != nil {
				return err
			}
			if i == 0 {
				sqlCopy := m.SQL
				schemaCopy := CloneSchema(out.schema)
				if err := c.send(&Event{Tag: EventSchemaCaptured, Migration: sqlCopy, Schema: schemaCopy, Before: chunk.Before, After: chunk.After, Version: out.version}, cursor, chunkSize); err != nil {
					return err
				}
			} else {
				if err := c.send(&Event{Tag: EventMigrationChunk, Before: chunk.Before, After: chunk.After, Version: out.version}, cursor, chunkSize); err != nil {
					return err
				}
			}
			c.capturedCursor = cursor
			if err := c.barrier(); err != nil {
				return err
			}
		}
	}
	c.subMgr.NotifyReset("schema migration applied")
	return nil
}

func (c *Core) applyPatch(p *Patch) error {
	if err := c.writable(); err != nil {
		return err
	}
	c.status.Mu.Lock()
	committed := c.status.Status.Committed
	c.status.Mu.Unlock()
	if committed != p.Cursor {
		return ErrBusy
	}
	if len(p.Rows) == 0 && len(p.Deleted) == 0 && len(p.Tombstones) == 0 && p.Clock.Compare(c.collectionClock) <= 0 {
		return nil
	}
	if err := c.checkpoint(); err != nil {
		return err
	}
	if p.State == nil {
		return corruptErr("full import state missing")
	}
	size := len(EncodeReplicaState(*p.State))
	if err := c.Admit(size); err != nil {
		return err
	}
	if err := c.connection.ExecBatch("BEGIN IMMEDIATE"); err != nil {
		return sqlErr(err)
	}
	result := func() error {
		for i := range p.Deleted {
			row := &p.Deleted[i]
			t := findSchemaTable(&c.schema, row.Table)
			if t == nil {
				return ErrSchemaMismatch
			}
			conditions := make([]string, 0, len(PkColumns(t)))
			for _, col := range PkColumns(t) {
				conditions = append(conditions, quote(col.Name)+" IS ?")
			}
			if err := c.connection.Exec("DELETE FROM "+quote(t.Name)+" WHERE "+strings.Join(conditions, " AND "), valuesToArgs(row.Key)...); err != nil {
				return sqlErr(err)
			}
		}
		for i := range p.Rows {
			t := findSchemaTable(&c.schema, p.Rows[i].Table)
			if t == nil {
				return ErrSchemaMismatch
			}
			if err := PutUserRow(c.connection, t, &p.Rows[i]); err != nil {
				return err
			}
		}
		if err := c.finishCommit(); err != nil {
			return err
		}
		if err := c.connection.ExecBatch("COMMIT"); err != nil {
			return sqlErr(err)
		}
		c.search.SearchRows(&c.schema, p.Rows)
		c.search.SearchRows(&c.schema, p.Deleted)
		c.status.Mu.Lock()
		c.status.Status.SearchIndexes = c.search.SearchStatus()
		c.status.Mu.Unlock()
		capture, err := CaptureSchema(c.connection, &c.schema)
		if err != nil {
			return err
		}
		if c.capture != nil {
			c.capture.Close()
		}
		c.capture = capture
		cursor, err := c.bump()
		if err != nil {
			return err
		}
		c.collectionClock = c.collectionClock.Max(p.Clock)
		stateCopy := *p.State
		if err := c.send(&Event{Tag: EventReplica, State: &stateCopy}, cursor, size); err != nil {
			return err
		}
		c.capturedCursor = cursor
		affected := make([]string, 0, len(c.schema.Tables))
		for _, t := range c.schema.Tables {
			affected = append(affected, t.Name)
		}
		c.subMgr.NotifyCommit(cursor, affected)
		return nil
	}()
	if result != nil {
		_ = c.connection.ExecBatch("ROLLBACK")
	}
	return result
}

func (c *Core) close() error {
	flushErr := c.flush()
	reply := make(chan error, 1)
	var stopErr error
	select {
	case c.worker <- Work{Kind: WorkStop, StopReply: reply}:
		stopErr = <-reply
	case <-time.After(30 * time.Second):
		stopErr = ErrClosed
	}
	c.status.Mu.Lock()
	c.status.Status.Closed = true
	c.status.Mu.Unlock()
	if flushErr != nil {
		return flushErr
	}
	return stopErr
}

func spoolMetricsFrom(stats spool.Stats) SpoolMetrics {
	return SpoolMetrics{
		Keys: stats.Keys, Segments: stats.Segments,
		LiveRecords: stats.LiveRecords, DeadRecords: stats.DeadRecords,
		DiskBytes: stats.DiskBytes, LiveBytes: stats.LiveBytes,
		PendingRecords: stats.PendingRecords, PendingBytes: stats.PendingBytes,
		BlocksWritten: stats.BlocksWritten, BytesWritten: stats.BytesWritten,
		CompressionRatio: stats.CompressionRatio,
		Compactions:      stats.Compactions, CompactionBytes: stats.CompactionBytes,
	}
}

func configureAuthorizer(c *sqlite.Conn, s *Schema, mode *atomic.Uint32, guard *SearchGuard) error {
	type namePair struct{ table, column string }
	var pk []namePair
	for i := range s.Tables {
		t := &s.Tables[i]
		if t.ApplicationKey == nil {
			continue
		}
		appCols := ApplicationColumns(t)
		for j := range t.Columns {
			col := &t.Columns[j]
			protected := col.PrimaryKey > 0
			if !protected {
				for _, key := range appCols {
					if key.ID == col.ID {
						protected = true
						break
					}
				}
			}
			if protected {
				pk = append(pk, namePair{t.Name, col.Name})
			}
		}
	}
	var policies []namePair
	for i := range s.Tables {
		for j := range s.Tables[i].Columns {
			if s.Tables[i].Columns[j].Policy != MergePolicyLww {
				policies = append(policies, namePair{s.Tables[i].Name, s.Tables[i].Columns[j].Name})
			}
		}
	}
	guard.Mu.RLock()
	hasSearch := len(guard.Ready) > 0
	guard.Mu.RUnlock()
	fn := func(action sqlite.AuthAction, a1, a2, a3, a4 string) sqlite.AuthResult {
		_ = a4
		access := mode.Load()
		if access != AccessRead && access != AccessWrite && access != AccessMigration {
			return sqlite.AuthOK
		}
		internal := func(name string) bool {
			return strings.HasPrefix(strings.ToLower(name), "_crdt_")
		}
		dbOf := func(db string) string { return db }
		_ = dbOf
		if hasSearch {
			db := ""
			switch action {
			case sqlite.AuthRead:
				db = a3
			case sqlite.AuthInsert, sqlite.AuthDelete:
				db = a2
			case sqlite.AuthCreateTable, sqlite.AuthDropTable:
				db = a2
			case sqlite.AuthCreateIndex, sqlite.AuthDropIndex:
				db = a3
			case sqlite.AuthPragma:
				db = a3
			}
			if db != "main" {
				guard.Mu.RLock()
				ready, ok := map[string]bool(nil), false
				_ = ready
				_ = ok
				check := func(name string) (bool, bool) {
					v, found := guard.Ready[strings.ToLower(name)]
					return v, found
				}
				var deny bool
				switch action {
				case sqlite.AuthRead:
					if v, found := check(a1); found && !v {
						deny = true
					}
				case sqlite.AuthInsert, sqlite.AuthDelete, sqlite.AuthUpdate,
					sqlite.AuthDropTable, sqlite.AuthAlterTable, sqlite.AuthCreateTable:
					name := a1
					if action == sqlite.AuthAlterTable {
						name = a2
					}
					if _, found := check(name); found {
						deny = true
					}
				case sqlite.AuthCreateIndex, sqlite.AuthDropIndex:
					if _, found := check(a2); found {
						deny = true
					}
				case sqlite.AuthReindex:
					if _, found := check(a1); found {
						deny = true
					}
				}
				guard.Mu.RUnlock()
				if deny {
					return sqlite.AuthDeny
				}
			}
		}
		allowed := false
		switch action {
		case sqlite.AuthSelect, sqlite.AuthRecursive:
			allowed = true
		case sqlite.AuthRead:
			allowed = !internal(a1)
		case sqlite.AuthInsert, sqlite.AuthDelete:
			allowed = access != AccessRead && !internal(a1)
		case sqlite.AuthUpdate:
			allowed = access != AccessRead && !internal(a1) && !asciiEqualFold(a2, ManagedKeyColumn)
			if allowed {
				for _, p := range pk {
					if asciiEqualFold(p.table, a1) && asciiEqualFold(p.column, a2) {
						allowed = false
						break
					}
				}
			}
			if allowed && access != AccessMigration {
				for _, p := range policies {
					if asciiEqualFold(p.table, a1) && asciiEqualFold(p.column, a2) {
						allowed = false
						break
					}
				}
			}
		case sqlite.AuthPragma:
			info := a1 == "table_xinfo" || a1 == "table_info" || a1 == "index_list" || a1 == "index_info"
			if info && a2 != "" && !internal(a2) {
				allowed = true
			}
			if a1 == "data_version" && a2 == "" && a3 == "temp" {
				allowed = true
			}
		case sqlite.AuthReindex:
			allowed = access == AccessMigration && !internal(a1)
		case sqlite.AuthFunction:
			name := strings.ToLower(a2)
			if !strings.HasPrefix(name, "_crdt_") && name != "load_extension" && name != "writefile" && name != "readfile" {
				if access != AccessMigration {
					allowed = true
				} else {
					switch name {
					case "_murmur_uuid", "_murmur_affinity", "abs", "coalesce", "ifnull",
						"nullif", "lower", "upper", "length", "substr", "substring",
						"replace", "trim", "ltrim", "rtrim", "round", "min", "max",
						"typeof", "hex", "unhex", "quote", "count", "sum", "total",
						"avg", "printf", "sqlite_rename_table", "sqlite_rename_column",
						"sqlite_rename_test", "sqlite_drop_column", "sqlite_rename_quotefix",
						"like", "glob":
						allowed = true
					}
				}
			}
		case sqlite.AuthCreateTable, sqlite.AuthDropTable:
			allowed = access == AccessMigration && !internal(a1)
		case sqlite.AuthAlterTable:
			allowed = access == AccessMigration && !internal(a2)
		case sqlite.AuthCreateIndex, sqlite.AuthDropIndex:
			allowed = access == AccessMigration && !internal(a2) && !internal(a1)
		}
		if allowed {
			return sqlite.AuthOK
		}
		return sqlite.AuthDeny
	}
	if err := c.SetAuthorizer(fn); err != nil {
		return sqlErr(err)
	}
	return nil
}

func schemasEqual(a, b *Schema) bool {
	return bytes.Equal(EncodeSchema(*a), EncodeSchema(*b))
}

func columnsEqual(a, b []Column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if fmt.Sprintf("%#v", a[i]) != fmt.Sprintf("%#v", b[i]) {
			return false
		}
	}
	return true
}
