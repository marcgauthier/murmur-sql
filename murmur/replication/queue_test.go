package replication

import (
	"errors"
	"testing"
	"time"
)

func TestQueueControlLane(t *testing.T) {
	send, recv := newQueue(newByteLease(1<<20), 1<<20)
	data := Message{Kind: MsgSnapshotChunk, Transfer: idOf(1), Data: bytesRepeat(1, 64<<10)}
	for send.TrySend(data) == nil {
	}
	if err := send.TrySend(Message{Kind: MsgPing, Nonce: 1}); err != nil {
		t.Fatalf("control send: %v", err)
	}
	q, ok := recv.Recv()
	if !ok || q.Message.Kind != MsgPing {
		t.Fatalf("control recv = %+v, %v", q, ok)
	}
	q.Release()
}

func TestQueueDisconnectReleasesWaiters(t *testing.T) {
	send, recv := newQueue(newByteLease(1<<20), 1<<20)
	data := Message{Kind: MsgSnapshotChunk, Transfer: idOf(1), Data: bytesRepeat(1, 64<<10)}
	for send.TrySend(data) == nil {
	}
	done := make(chan error, 1)
	go func() { done <- send.Send(data) }()
	time.Sleep(50 * time.Millisecond)
	recv.Close()
	select {
	case err := <-done:
		var rerr *Error
		if !errors.As(err, &rerr) || rerr.Kind != KindUnavailable {
			t.Fatalf("waiter err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter stuck")
	}
	if _, ok := recv.Recv(); ok {
		// Drained buffer may still hold data messages; keep draining.
		for {
			q, ok := recv.TryRecv()
			if !ok {
				break
			}
			q.Release()
		}
		if _, ok := recv.Recv(); ok {
			t.Fatal("recv after close and drain")
		}
	}
}
