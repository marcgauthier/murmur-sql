package murmur

import (
	"bytes"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// SubscriptionCursor identifies a subscription position.
type SubscriptionCursor struct {
	DatabaseID [16]byte
	Epoch      uint64
	Sequence   uint64
}

// ChangeType classifies a row change.
type ChangeType int

const (
	ChangeAdded ChangeType = iota
	ChangeUpdated
	ChangeRemoved
)

// RowChange describes one changed row.
type RowChange[T any] struct {
	Type ChangeType
	Key  []byte
	Row  *T
}

// SubEventKind classifies a subscription event.
type SubEventKind int

const (
	SubInitial SubEventKind = iota
	SubUpdate
	SubReset
)

// SubEvent is one subscription delivery.
type SubEvent[T any] struct {
	Kind    SubEventKind
	Rows    []T
	Changes []RowChange[T]
	Cursor  SubscriptionCursor
	Reason  string
}

// SubscriptionOptions configures a subscription.
type SubscriptionOptions struct {
	// Capacity of the event buffer for this subscriber.
	BufferSize int
	// Emit an event even when rows haven't changed.
	EmitUnchanged bool
	// Attempt to resume after this cursor.
	ResumeFrom *SubscriptionCursor
	// Limit notifications to writes touching these tables (empty = all).
	Tables []string
}

// DefaultSubscriptionOptions returns default subscription options.
func DefaultSubscriptionOptions() SubscriptionOptions {
	return SubscriptionOptions{BufferSize: 32}
}

type subEventQueue[T any] struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []SubEvent[T]
	closed atomic.Bool
}

func newSubEventQueue[T any]() *subEventQueue[T] {
	q := &subEventQueue[T]{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// push enqueues an event. On buffer overflow it discards the backlog,
// enqueues Reset, and returns false (slow consumer).
func (q *subEventQueue[T]) push(event SubEvent[T], bufferSize int, cursor SubscriptionCursor) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed.Load() {
		return false
	}
	if len(q.queue) >= bufferSize {
		q.queue = []SubEvent[T]{{Kind: SubReset, Cursor: cursor, Reason: "buffer overflow: consumer fell behind"}}
		q.closed.Store(true)
		q.cond.Broadcast()
		return false
	}
	q.queue = append(q.queue, event)
	q.cond.Signal()
	return true
}

func (q *subEventQueue[T]) pushReset(cursor SubscriptionCursor, reason string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed.Load() {
		return
	}
	q.queue = []SubEvent[T]{{Kind: SubReset, Cursor: cursor, Reason: reason}}
	q.closed.Store(true)
	q.cond.Broadcast()
}

func (q *subEventQueue[T]) recv() (SubEvent[T], error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if len(q.queue) > 0 {
			event := q.queue[0]
			q.queue = q.queue[1:]
			return event, nil
		}
		if q.closed.Load() {
			return SubEvent[T]{}, ErrClosed
		}
		q.cond.Wait()
	}
}

func (q *subEventQueue[T]) recvTimeout(timeout time.Duration) (SubEvent[T], error) {
	deadline := time.Now().Add(timeout)
	// Wake the condvar at the deadline so a quiet queue still notices.
	timer := time.AfterFunc(timeout, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer timer.Stop()
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if len(q.queue) > 0 {
			event := q.queue[0]
			q.queue = q.queue[1:]
			return event, nil
		}
		if q.closed.Load() {
			return SubEvent[T]{}, ErrClosed
		}
		if !time.Now().Before(deadline) {
			return SubEvent[T]{}, ErrBusy
		}
		q.cond.Wait()
		if len(q.queue) == 0 && !time.Now().Before(deadline) {
			if q.closed.Load() {
				return SubEvent[T]{}, ErrClosed
			}
			return SubEvent[T]{}, ErrBusy
		}
	}
}

func (q *subEventQueue[T]) tryRecv() (SubEvent[T], error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) > 0 {
		event := q.queue[0]
		q.queue = q.queue[1:]
		return event, nil
	}
	if q.closed.Load() {
		return SubEvent[T]{}, ErrClosed
	}
	return SubEvent[T]{}, ErrBusy
}

func (q *subEventQueue[T]) close() {
	q.closed.Store(true)
	q.mu.Lock()
	q.cond.Broadcast()
	q.mu.Unlock()
}

// Subscription is an active subscription stream.
type Subscription[T any] struct {
	queue  *subEventQueue[T]
	closed atomic.Bool
	done   chan struct{}
	once   sync.Once
}

// Recv receives the next event, blocking until available or closed.
func (s *Subscription[T]) Recv() (SubEvent[T], error) {
	return s.queue.recv()
}

// RecvTimeout receives the next event with a timeout.
func (s *Subscription[T]) RecvTimeout(timeout time.Duration) (SubEvent[T], error) {
	return s.queue.recvTimeout(timeout)
}

// TryRecv receives the next event without blocking.
func (s *Subscription[T]) TryRecv() (SubEvent[T], error) {
	return s.queue.tryRecv()
}

// Close terminates the subscription and waits for its worker.
func (s *Subscription[T]) Close() {
	s.once.Do(func() {
		s.closed.Store(true)
		s.queue.close()
		<-s.done
	})
}

type subListener struct {
	tables map[string]bool
	tx     chan<- uint64
}

// SubManager fans committed sequences out to listeners.
type SubManager struct {
	databaseID [16]byte
	epoch      uint64
	nextID     atomic.Uint64
	mu         sync.Mutex
	listeners  map[uint64]*subListener
	historyMu  sync.RWMutex
	history    []uint64
	maxHistory int
	closed     atomic.Bool
}

// NewSubManager builds a manager with a bounded commit history.
func NewSubManager(databaseID [16]byte, maxHistory int) *SubManager {
	epoch := uint64(time.Now().UnixNano())
	if epoch == 0 {
		epoch = 1
	}
	m := &SubManager{databaseID: databaseID, epoch: epoch,
		listeners: make(map[uint64]*subListener), maxHistory: maxHistory}
	m.nextID.Store(1)
	return m
}

// SubCursor renders a cursor for a sequence.
func (m *SubManager) SubCursor(sequence uint64) SubscriptionCursor {
	return SubscriptionCursor{DatabaseID: m.databaseID, Epoch: m.epoch, Sequence: sequence}
}

// NotifyCommit records a commit and wakes interested listeners.
func (m *SubManager) NotifyCommit(sequence uint64, affected []string) {
	if m.closed.Load() {
		return
	}
	m.historyMu.Lock()
	m.history = append(m.history, sequence)
	if len(m.history) > m.maxHistory {
		m.history = append([]uint64(nil), m.history[len(m.history)-m.maxHistory:]...)
	}
	m.historyMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, listener := range m.listeners {
		if len(listener.tables) == 0 {
			select {
			case listener.tx <- sequence:
			default:
			}
			continue
		}
		for _, t := range affected {
			if listener.tables[t] {
				select {
				case listener.tx <- sequence:
				default:
				}
				break
			}
		}
	}
}

// NotifyReset wakes all listeners with the reset signal.
func (m *SubManager) NotifyReset(reason string) {
	_ = reason
	if m.closed.Load() {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, listener := range m.listeners {
		select {
		case listener.tx <- ^uint64(0):
		default:
		}
	}
}

// ValidateResume checks a resume cursor.
func (m *SubManager) ValidateResume(cursor SubscriptionCursor, current uint64) error {
	if cursor.DatabaseID != m.databaseID || cursor.Epoch != m.epoch {
		return invalidErr("subscription cursor expired: epoch mismatch")
	}
	if cursor.Sequence > current {
		return invalidErr("subscription cursor expired: future sequence")
	}
	m.historyMu.RLock()
	defer m.historyMu.RUnlock()
	if len(m.history) > 0 && cursor.Sequence < m.history[0] {
		return invalidErr("subscription cursor expired: sequence purged")
	}
	return nil
}

// Register adds a listener; the channel is never closed by the manager.
func (m *SubManager) Register(tables []string) (uint64, <-chan uint64) {
	id := m.nextID.Add(1)
	ch := make(chan uint64, 1024)
	set := make(map[string]bool)
	for _, t := range tables {
		set[t] = true
	}
	m.mu.Lock()
	m.listeners[id] = &subListener{tables: set, tx: ch}
	m.mu.Unlock()
	return id, ch
}

// Unregister removes a listener.
func (m *SubManager) Unregister(id uint64) {
	m.mu.Lock()
	delete(m.listeners, id)
	m.mu.Unlock()
}

// Close wakes all listeners with reset and drops them.
func (m *SubManager) Close() {
	m.closed.Store(true)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, listener := range m.listeners {
		select {
		case listener.tx <- ^uint64(0):
		default:
		}
	}
	m.listeners = make(map[uint64]*subListener)
}

// StartSubscription creates a reactive query subscription. Rows compare
// with reflect.DeepEqual.
func StartSubscription[T any](db *Database, query func(*Sql) ([]T, error), keyFn func(*T) []byte, options SubscriptionOptions) (*Subscription[T], error) {
	subMgr := db.inner.subMgr
	current := db.Status().Committed
	if options.ResumeFrom != nil {
		if err := subMgr.ValidateResume(*options.ResumeFrom, current); err != nil {
			return nil, err
		}
	}
	initialAny, err := db.Read(func(sql *Sql) (any, error) { return query(sql) })
	if err != nil {
		return nil, err
	}
	initialRows := initialAny.([]T)
	cursor := subMgr.SubCursor(current)
	queue := newSubEventQueue[T]()
	closed := atomic.Bool{}
	buffer := options.BufferSize
	if buffer < 1 {
		buffer = 1
	}
	if options.ResumeFrom != nil {
		queue.push(SubEvent[T]{Kind: SubUpdate, Rows: initialRows, Cursor: cursor}, buffer, cursor)
	} else {
		queue.push(SubEvent[T]{Kind: SubInitial, Rows: initialRows, Cursor: cursor}, buffer, cursor)
	}
	listenerID, wake := subMgr.Register(options.Tables)
	sub := &Subscription[T]{queue: queue, done: make(chan struct{})}
	sub.closed.Store(closed.Load())
	go func() {
		defer close(sub.done)
		defer subMgr.Unregister(listenerID)
		lastRows := initialRows
		lastSeq := current
		for !sub.closed.Load() {
			var seq uint64
			var ok bool
			select {
			case seq, ok = <-wake:
				if !ok {
					return
				}
			case <-time.After(250 * time.Millisecond):
				continue
			}
			if seq == ^uint64(0) {
				queue.pushReset(subMgr.SubCursor(lastSeq), "schema or storage reset")
				return
			}
			// Derived search indexes can publish after the source
			// commit using the same cursor: re-evaluate equals.
			if seq < lastSeq {
				continue
			}
			newAny, err := db.Read(func(sql *Sql) (any, error) { return query(sql) })
			if err != nil {
				queue.pushReset(subMgr.SubCursor(lastSeq), "query error: "+err.Error())
				return
			}
			newRows := newAny.([]T)
			changes := diffSubRows(lastRows, newRows, keyFn)
			lastSeq = seq
			cursor := subMgr.SubCursor(lastSeq)
			if len(changes) > 0 || options.EmitUnchanged {
				event := SubEvent[T]{Kind: SubUpdate, Rows: newRows, Changes: changes, Cursor: cursor}
				if !queue.push(event, buffer, cursor) {
					sub.closed.Store(true)
					return
				}
			}
			lastRows = newRows
		}
	}()
	return sub, nil
}

func diffSubRows[T any](oldRows, newRows []T, keyFn func(*T) []byte) []RowChange[T] {
	oldByKey := make(map[string]*T, len(oldRows))
	for i := range oldRows {
		oldByKey[string(keyFn(&oldRows[i]))] = &oldRows[i]
	}
	newByKey := make(map[string]*T, len(newRows))
	for i := range newRows {
		newByKey[string(keyFn(&newRows[i]))] = &newRows[i]
	}
	var changes []RowChange[T]
	for key, row := range newByKey {
		old, ok := oldByKey[key]
		switch {
		case !ok:
			rowCopy := *row
			changes = append(changes, RowChange[T]{Type: ChangeAdded, Key: []byte(key), Row: &rowCopy})
		case !reflect.DeepEqual(*old, *row):
			rowCopy := *row
			changes = append(changes, RowChange[T]{Type: ChangeUpdated, Key: []byte(key), Row: &rowCopy})
		}
	}
	for key := range oldByKey {
		if _, ok := newByKey[key]; !ok {
			changes = append(changes, RowChange[T]{Type: ChangeRemoved, Key: []byte(key)})
		}
	}
	sort.Slice(changes, func(i, j int) bool { return bytes.Compare(changes[i].Key, changes[j].Key) < 0 })
	return changes
}
