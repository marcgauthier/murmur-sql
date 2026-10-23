package spool

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

func TestDrainQueueReleasesQueuedGroups(t *testing.T) {
	cause := errors.New("terminal write failure")
	st := &Store{}
	st.commitCond = sync.NewCond(&st.queueMu)

	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	first := &commitGroup{bytes: 7, recs: make([]pendingRecord, 2), done: firstDone}
	second := &commitGroup{bytes: 11, recs: make([]pendingRecord, 1), done: secondDone}
	st.queue = []*commitGroup{first, second}
	st.pendingBytes.Store(18)
	st.pendingRecords.Store(3)
	st.pendingGroups.Store(2)

	st.drainQueue(cause)

	if len(st.queue) != 0 {
		t.Fatalf("queue still has %d groups", len(st.queue))
	}
	if got := st.pendingBytes.Load(); got != 0 {
		t.Errorf("pending bytes = %d, want 0", got)
	}
	if got := st.pendingRecords.Load(); got != 0 {
		t.Errorf("pending records = %d, want 0", got)
	}
	if got := st.pendingGroups.Load(); got != 0 {
		t.Errorf("pending groups = %d, want 0", got)
	}
	for name, ch := range map[string]<-chan error{"first": firstDone, "second": secondDone} {
		select {
		case got := <-ch:
			if got != cause {
				t.Errorf("%s result = %v, want original cause", name, got)
			}
		default:
			t.Errorf("%s waiter was not released", name)
		}
	}
}

func TestSealBlockRoundTrip(t *testing.T) {
	st, err := Open(shutdownOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	_, key, ok := st.currentDataKey()
	if !ok {
		t.Fatal("no current data key")
	}
	body := []byte("sealed block body")
	frame, plainLen, err := st.sealBlock(body, false, 1, st.ring.current, key, 9)
	if err != nil {
		t.Fatalf("sealBlock: %v", err)
	}
	if plainLen != len(body) {
		t.Fatalf("plain length = %d, want %d", plainLen, len(body))
	}
	hdr, err := parseBlockHeader(frame)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	if hdr.blockSeq != 9 || hdr.recordCount != 1 || hdr.completion {
		t.Fatalf("unexpected header: seq=%d records=%d completion=%t", hdr.blockSeq, hdr.recordCount, hdr.completion)
	}
	l := &loader{man: st.man, ring: st.ring, comp: st.comp, ctx: st.ctx}
	got, err := l.openPayload(hdr, frame[blockHeaderLen:])
	if err != nil {
		t.Fatalf("open payload: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("opened body = %q, want %q", got, body)
	}
}

func TestBackgroundFlushSkipsDuringMaintenance(t *testing.T) {
	st, err := Open(shutdownOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	st.maintActive.Store(true)
	st.backgroundFlush()
	if got := st.flushes.Load(); got != 0 {
		t.Errorf("flush count = %d, want 0 while maintenance is active", got)
	}
	if got := st.flushErrors.Load(); got != 0 {
		t.Errorf("flush errors = %d, want 0", got)
	}
}

func TestBackgroundFlushRecordsRetryableError(t *testing.T) {
	st, err := Open(shutdownOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if err := st.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Force sealLocked to fail once. Sequence exhaustion is retryable and
	// leaves the staged write available for a later flush.
	st.seqNext.Store(seqCounterMax - 1)
	st.backgroundFlush()
	if got := st.flushErrors.Load(); got != 1 {
		t.Errorf("flush errors = %d, want 1", got)
	}
	st.flushErrMu.Lock()
	lastErr := st.lastFlushErr
	st.flushErrMu.Unlock()
	if !errors.Is(lastErr, ErrSeqExhausted) {
		t.Errorf("last flush error = %v, want ErrSeqExhausted", lastErr)
	}
	// Restore sequence space so deferred Close can flush the staged put.
	st.seqNext.Store(0)
}

func TestWaitForGroupOutcomes(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		st := &Store{completedThrough: 4}
		st.commitCond = sync.NewCond(&st.queueMu)
		if err := st.waitForGroup(4, false); err != nil {
			t.Fatalf("waitForGroup: %v", err)
		}
	})
	t.Run("closed", func(t *testing.T) {
		st := &Store{}
		st.commitCond = sync.NewCond(&st.queueMu)
		st.closed.Store(true)
		if err := st.waitForGroup(1, false); !errors.Is(err, ErrClosed) {
			t.Fatalf("waitForGroup error = %v, want ErrClosed", err)
		}
	})
	t.Run("terminal", func(t *testing.T) {
		st := &Store{}
		st.commitCond = sync.NewCond(&st.queueMu)
		st.storageErr = errors.New("disk failed")
		st.terminal.Store(true)
		if err := st.waitForGroup(1, true); !errors.Is(err, ErrStorageFailed) {
			t.Fatalf("waitForGroup error = %v, want ErrStorageFailed", err)
		}
	})
	t.Run("broadcast completion", func(t *testing.T) {
		st := &Store{}
		st.commitCond = sync.NewCond(&st.queueMu)
		result := make(chan error, 1)
		go func() { result <- st.waitForGroup(1, true) }()
		st.queueMu.Lock()
		st.completedThrough = 1
		st.commitCond.Broadcast()
		st.queueMu.Unlock()
		if err := <-result; err != nil {
			t.Fatalf("waitForGroup: %v", err)
		}
	})
}
