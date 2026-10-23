package replication

import (
	"context"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// Byte leases with a reserved control lane. A session drains
// independently of producers.

var nextQueueID atomic.Int64

// queued is one leased message. Release returns its byte leases after
// the message has been written; it is idempotent.
type queued struct {
	Message  Message
	Prepared *Prepared
	release  func()
	done     atomic.Bool
}

// Release returns the message's byte leases.
func (q *queued) Release() {
	if q.release != nil && q.done.CompareAndSwap(false, true) {
		q.release()
	}
}

type byteLease struct {
	sem   *semaphore.Weighted
	avail atomic.Int64
}

func newByteLease(n int64) *byteLease {
	b := &byteLease{sem: semaphore.NewWeighted(n)}
	b.avail.Store(n)
	return b
}

func (b *byteLease) tryAcquire(n int64) bool {
	if !b.sem.TryAcquire(n) {
		return false
	}
	b.avail.Add(-n)
	return true
}

func (b *byteLease) acquire(ctx context.Context, n int64) error {
	if err := b.sem.Acquire(ctx, n); err != nil {
		return err
	}
	b.avail.Add(-n)
	return nil
}

func (b *byteLease) release(n int64) {
	b.sem.Release(n)
	b.avail.Add(n)
}

func (b *byteLease) available() int64 { return b.avail.Load() }

// queueSender enqueues leased messages on data and control lanes.
type queueSender struct {
	data         chan *queued
	control      chan *queued
	global       *byteLease
	controlGroup *byteLease
	peer         *byteLease
	controlPeer  *byteLease
	maximum      int
	id           int64
	ctx          context.Context
}

// queueReceiver drains one session's queues, control first.
type queueReceiver struct {
	data    chan *queued
	control chan *queued
	done    <-chan struct{}
	cancel  context.CancelFunc
}

// newQueue builds a sender/receiver pair sharing one global lease.
func newQueue(global *byteLease, maximum int) (*queueSender, *queueReceiver) {
	return withControlQueue(global, newByteLease(64<<10), maximum)
}

// withControlQueue builds a pair with an explicit control lease.
func withControlQueue(global, controlGlobal *byteLease, maximum int) (*queueSender, *queueReceiver) {
	data := make(chan *queued, 128)
	control := make(chan *queued, 128)
	ctx, cancel := context.WithCancel(context.Background())
	return &queueSender{data: data, control: control, global: global, controlGroup: controlGlobal,
			peer: newByteLease(int64(maximum - (64 << 10))), controlPeer: newByteLease(64 << 10),
			maximum: maximum, id: nextQueueID.Add(1), ctx: ctx},
		&queueReceiver{data: data, control: control, done: ctx.Done(), cancel: cancel}
}

// Available reports remaining peer data-lease bytes.
func (s *queueSender) Available() int64 { return s.peer.available() }

// ID reports the sender identity.
func (s *queueSender) ID() int64 { return s.id }

// Close releases blocked senders. It is idempotent.
func (r *queueReceiver) Close() { r.cancel() }

func isControl(m *Message) bool {
	switch m.Kind {
	case MsgBatches, MsgPlumtreeData, MsgTransactionChunk, MsgSnapshotChunk:
		return false
	}
	return true
}

func (s *queueSender) size(m *Message) (int64, error) {
	n := int64(len(EncodeMessage(m)) + 12)
	if n > int64(s.maximum) || n > int64(^uint32(0)) {
		return 0, ErrOverloaded
	}
	return n, nil
}

func (s *queueSender) lanes(m *Message, size int64) (global, peer *byteLease, out chan *queued) {
	if isControl(m) && size <= 64<<10 {
		return s.controlGroup, s.controlPeer, s.control
	}
	return s.global, s.peer, s.data
}

// TrySend enqueues one message without blocking.
func (s *queueSender) TrySend(m Message) error {
	if s.ctx.Err() != nil {
		return ErrOverloaded
	}
	size, err := s.size(&m)
	if err != nil {
		return err
	}
	global, peer, out := s.lanes(&m, size)
	if !global.tryAcquire(size) {
		return ErrOverloaded
	}
	if !peer.tryAcquire(size) {
		global.release(size)
		return ErrOverloaded
	}
	select {
	case out <- &queued{Message: m, release: func() { global.release(size); peer.release(size) }}:
		return nil
	default:
		global.release(size)
		peer.release(size)
		return ErrOverloaded
	}
}

// TrySendPrepared enqueues one message with a prebuilt frame.
func (s *queueSender) TrySendPrepared(m Message, prepared *Prepared) error {
	if s.ctx.Err() != nil {
		return ErrOverloaded
	}
	if prepared.Charge > s.maximum || prepared.Charge > int(^uint32(0)) {
		return ErrOverloaded
	}
	size := int64(prepared.Charge)
	global, peer, out := s.lanes(&m, size)
	if !global.tryAcquire(size) {
		return ErrOverloaded
	}
	if !peer.tryAcquire(size) {
		global.release(size)
		return ErrOverloaded
	}
	select {
	case out <- &queued{Message: m, Prepared: prepared, release: func() { global.release(size); peer.release(size) }}:
		return nil
	default:
		global.release(size)
		peer.release(size)
		return ErrOverloaded
	}
}

// Send enqueues one message, waiting up to five seconds for leases.
func (s *queueSender) Send(m Message) error {
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	size, err := s.size(&m)
	if err != nil {
		return err
	}
	global, peer, out := s.lanes(&m, size)
	if err := global.acquire(ctx, size); err != nil {
		if s.ctx.Err() != nil {
			return unavailableErr("queue closed")
		}
		return unavailableErr("queue deadline")
	}
	if err := peer.acquire(ctx, size); err != nil {
		global.release(size)
		if s.ctx.Err() != nil {
			return unavailableErr("queue closed")
		}
		return unavailableErr("queue deadline")
	}
	select {
	case out <- &queued{Message: m, release: func() { global.release(size); peer.release(size) }}:
		return nil
	case <-ctx.Done():
		global.release(size)
		peer.release(size)
		if s.ctx.Err() != nil {
			return unavailableErr("queue closed")
		}
		return unavailableErr("queue deadline")
	}
}

// Recv dequeues one message, control first. It reports false after Close
// once buffered messages are drained.
func (r *queueReceiver) Recv() (*queued, bool) {
	select {
	case q := <-r.control:
		return q, true
	default:
	}
	select {
	case q := <-r.data:
		return q, true
	default:
	}
	select {
	case q := <-r.control:
		return q, true
	case q := <-r.data:
		return q, true
	case <-r.done:
		select {
		case q := <-r.control:
			return q, true
		default:
		}
		select {
		case q := <-r.data:
			return q, true
		default:
		}
		return nil, false
	}
}

// TryRecv dequeues one message without blocking.
func (r *queueReceiver) TryRecv() (*queued, bool) {
	select {
	case q := <-r.control:
		return q, true
	default:
	}
	select {
	case q := <-r.data:
		return q, true
	default:
		return nil, false
	}
}
