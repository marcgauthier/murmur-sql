package murmur

import (
	"math/bits"
	"testing"
	"time"
)

func TestSchedulerConfigValidate(t *testing.T) {
	good := DefaultSchedulerConfig()
	if err := good.Validate(); err != nil {
		t.Fatalf("default config rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*SchedulerConfig)
	}{
		{"zero local", func(c *SchedulerConfig) { c.LocalShare = 0 }},
		{"zero remote", func(c *SchedulerConfig) { c.RemoteShare = 0 }},
		{"sum 90", func(c *SchedulerConfig) { c.LocalShare = 80 }},
		{"sum 110", func(c *SchedulerConfig) { c.RemoteShare = 20 }},
		{"zero debt", func(c *SchedulerConfig) { c.MaxDebt = 0 }},
		{"negative debt", func(c *SchedulerConfig) { c.MaxDebt = -time.Second }},
		{"zero reserve", func(c *SchedulerConfig) { c.MaintenanceReserveEvery = 0 }},
	}
	for _, tc := range cases {
		cfg := DefaultSchedulerConfig()
		tc.mutate(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Fatalf("%s accepted", tc.name)
		}
	}
}

func TestWriterClassString(t *testing.T) {
	if WriterLocal.String() != "local" || WriterRemote.String() != "remote" ||
		WriterMaintenance.String() != "maintenance" || WriterClass(99).String() != "unknown" {
		t.Fatal("class names wrong")
	}
}

func TestSatAdd(t *testing.T) {
	if satAdd(1, 2) != 3 || satAdd(0, 0) != 0 {
		t.Fatal("small sums wrong")
	}
	if satAdd(^uint64(0), 1) != ^uint64(0) {
		t.Fatal("overflow did not saturate")
	}
	if satAdd(^uint64(0), ^uint64(0)) != ^uint64(0) {
		t.Fatal("double-max did not saturate")
	}
}

func TestSatMul(t *testing.T) {
	if satMul(6, 7) != 42 || satMul(0, ^uint64(0)) != 0 || satMul(^uint64(0), 0) != 0 {
		t.Fatal("small products wrong")
	}
	if satMul(^uint64(0), 2) != ^uint64(0) {
		t.Fatal("overflow did not saturate")
	}
	// Exactly 2^64 overflows and saturates.
	if got := satMul(1<<32, 1<<32); got != ^uint64(0) {
		t.Fatalf("2^32*2^32 = %d, want saturation", got)
	}
	if got := satMul((1<<32)-1, (1<<32)-1); got != ((1<<32)-1)*((1<<32)-1) {
		t.Fatal("largest non-overflowing square wrong")
	}
}

// TestBitsMul64 cross-checks the hand-rolled 128-bit multiply against
// math/bits over edges and pseudorandom values.
func TestBitsMul64(t *testing.T) {
	edges := []uint64{0, 1, 2, (1 << 32) - 1, 1 << 32, (1 << 32) + 1, (1 << 63) - 1, 1 << 63, ^uint64(0)}
	for _, a := range edges {
		for _, b := range edges {
			hi, lo := bitsMul64(a, b)
			wantHi, wantLo := bits.Mul64(a, b)
			if hi != wantHi || lo != wantLo {
				t.Fatalf("bitsMul64(%d,%d) = (%d,%d), want (%d,%d)", a, b, hi, lo, wantHi, wantLo)
			}
		}
	}
	// Deterministic xorshift64* stream (no test-time randomness).
	x := uint64(0x9E3779B97F4A7C15)
	next := func() uint64 {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		return x * 0x2545F4914F6CDD1D
	}
	for i := 0; i < 20000; i++ {
		a, b := next(), next()
		hi, lo := bitsMul64(a, b)
		wantHi, wantLo := bits.Mul64(a, b)
		if hi != wantHi || lo != wantLo {
			t.Fatalf("bitsMul64(%d,%d) = (%d,%d), want (%d,%d)", a, b, hi, lo, wantHi, wantLo)
		}
	}
}

func testScheduler() *WriterScheduler {
	return NewWriterScheduler(DefaultSchedulerConfig())
}

func TestSchedulerIdleGrantAndRelease(t *testing.T) {
	s := testScheduler()
	if s.Active() {
		t.Fatal("active before any admission")
	}
	tk, err := s.Admit(WriterLocal)
	if err != nil {
		t.Fatal(err)
	}
	if tk.Class() != WriterLocal || tk.Dual() {
		t.Fatal("idle grant misattributed")
	}
	if !s.Active() {
		t.Fatal("not active while ticket held")
	}
	tk.Release()
	if s.Active() {
		t.Fatal("still active after release")
	}
	tk.Release() // double release is harmless
	snap := s.Snapshot()
	if snap.Local.Acquisitions != 1 || snap.Remote.Acquisitions != 0 {
		t.Fatalf("acquisitions = %+v", snap)
	}
	if snap.Shares != [2]uint32{90, 10} {
		t.Fatalf("shares = %v", snap.Shares)
	}
}

// admitAsync queues one admission and reports the ticket class (or a
// nil ticket on error) on the returned channel.
func admitAsync(s *WriterScheduler, class WriterClass) <-chan *Ticket {
	out := make(chan *Ticket, 1)
	go func() {
		tk, err := s.Admit(class)
		if err != nil {
			out <- nil
			return
		}
		out <- tk
	}()
	return out
}

func waitTicket(t *testing.T, ch <-chan *Ticket) *Ticket {
	t.Helper()
	select {
	case tk := <-ch:
		if tk == nil {
			t.Fatal("admission failed")
		}
		return tk
	case <-time.After(5 * time.Second):
		t.Fatal("admission never granted")
		return nil
	}
}

func waitWaiters(t *testing.T, s *WriterScheduler, class WriterClass, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var got int
		snap := s.Snapshot()
		switch class {
		case WriterLocal:
			got = snap.Local.Waiters
		case WriterRemote:
			got = snap.Remote.Waiters
		case WriterMaintenance:
			got = snap.Maintenance.Waiters
		}
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiters stuck at %d, want %d", got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSchedulerFIFOWithinClass(t *testing.T) {
	s := testScheduler()
	hold, err := s.Admit(WriterLocal)
	if err != nil {
		t.Fatal(err)
	}
	first := admitAsync(s, WriterLocal)
	waitWaiters(t, s, WriterLocal, 1)
	second := admitAsync(s, WriterLocal)
	waitWaiters(t, s, WriterLocal, 2)
	hold.Release()
	t1 := waitTicket(t, first)
	select {
	case <-second:
		t.Fatal("second waiter granted before first")
	default:
	}
	t1.Release()
	t2 := waitTicket(t, second)
	t2.Release()
	if s.Active() {
		t.Fatal("active after all released")
	}
}

// TestSchedulerServiceFlipsGrant pins weighted fair queueing without
// timing assertions: after the local class accrues real service while
// remote accrued none, the next dual grant must go remote.
func TestSchedulerServiceFlipsGrant(t *testing.T) {
	cfg := SchedulerConfig{LocalShare: 50, RemoteShare: 50, MaxDebt: time.Second, MaintenanceReserveEvery: 100}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	s := NewWriterScheduler(cfg)
	hold, err := s.Admit(WriterLocal)
	if err != nil {
		t.Fatal(err)
	}
	localCh := admitAsync(s, WriterLocal)
	remoteCh := admitAsync(s, WriterRemote)
	waitWaiters(t, s, WriterLocal, 1)
	waitWaiters(t, s, WriterRemote, 1)
	time.Sleep(5 * time.Millisecond) // accrue local service against exact-zero remote
	hold.Release()
	winner := waitTicket(t, remoteCh)
	if winner.Class() != WriterRemote || !winner.Dual() {
		t.Fatal("dual grant did not go remote")
	}
	select {
	case <-localCh:
		t.Fatal("local granted despite higher normalized service")
	default:
	}
	winner.Release()
	loser := waitTicket(t, localCh)
	if loser.Class() != WriterLocal {
		t.Fatal("local never granted second")
	}
	loser.Release()
}

// TestSchedulerTieBreaksLocal: with zero accrued service on both
// classes, a dual grant goes local (<= tie-break).
func TestSchedulerTieBreaksLocal(t *testing.T) {
	cfg := SchedulerConfig{LocalShare: 50, RemoteShare: 50, MaxDebt: time.Second, MaintenanceReserveEvery: 100}
	s := NewWriterScheduler(cfg)
	hold, err := s.Admit(WriterMaintenance)
	if err != nil {
		t.Fatal(err)
	}
	localCh := admitAsync(s, WriterLocal)
	remoteCh := admitAsync(s, WriterRemote)
	waitWaiters(t, s, WriterLocal, 1)
	waitWaiters(t, s, WriterRemote, 1)
	hold.Release() // maintenance accrues no [2]-bucket service: tie at zero
	winner := waitTicket(t, localCh)
	if winner.Class() != WriterLocal {
		t.Fatal("tie did not break local")
	}
	winner.Release()
	runnerUp := waitTicket(t, remoteCh)
	runnerUp.Release()
}

// TestSchedulerMaintenanceReserve: with ReserveEvery=2 and maintenance
// queued, the third grant goes to maintenance.
func TestSchedulerMaintenanceReserve(t *testing.T) {
	cfg := SchedulerConfig{LocalShare: 50, RemoteShare: 50, MaxDebt: time.Second, MaintenanceReserveEvery: 2}
	s := NewWriterScheduler(cfg)
	hold, err := s.Admit(WriterLocal) // sinceMaintenance = 1
	if err != nil {
		t.Fatal(err)
	}
	localCh := admitAsync(s, WriterLocal)
	maintCh := admitAsync(s, WriterMaintenance)
	waitWaiters(t, s, WriterLocal, 1)
	waitWaiters(t, s, WriterMaintenance, 1)
	hold.Release() // since=1 < 2: local wins, since=2
	next := waitTicket(t, localCh)
	if next.Class() != WriterLocal {
		t.Fatal("expected local before reserve tripped")
	}
	select {
	case <-maintCh:
		t.Fatal("maintenance granted before reserve tripped")
	default:
	}
	next.Release() // since=2 >= 2 with maintenance queued: reserve wins
	reserved := waitTicket(t, maintCh)
	if reserved.Class() != WriterMaintenance {
		t.Fatal("reserve did not grant maintenance")
	}
	reserved.Release()
}

func TestSchedulerAdmitTimeout(t *testing.T) {
	s := testScheduler()
	hold, err := s.Admit(WriterLocal)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := s.AdmitTimeout(WriterRemote, 20*time.Millisecond); err != ErrBusy {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("timeout took too long")
	}
	if got := s.Snapshot().Remote.Cancels; got != 1 {
		t.Fatalf("cancels = %d, want 1", got)
	}
	hold.Release()
	tk, err := s.AdmitTimeout(WriterRemote, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tk.Release()
}

func TestSchedulerClose(t *testing.T) {
	s := testScheduler()
	hold, err := s.Admit(WriterLocal)
	if err != nil {
		t.Fatal(err)
	}
	pending := admitAsync(s, WriterRemote)
	waitWaiters(t, s, WriterRemote, 1)
	s.Close()
	select {
	case tk := <-pending:
		if tk != nil {
			t.Fatal("waiter granted after close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never woken by close")
	}
	if _, err := s.Admit(WriterLocal); err != ErrClosed {
		t.Fatalf("admit after close err = %v, want ErrClosed", err)
	}
	hold.Release() // releasing into a closed scheduler is harmless
}

// TestSchedulerClampDebt drives the debt clamp directly: a class more
// than MaxDebt ahead (normalized) is pulled back to exactly the bound,
// and saturation never wraps.
func TestSchedulerClampDebt(t *testing.T) {
	inner := &schedulerInner{localW: 90, remoteW: 10, maxDebtNanos: 1_000_000_000}
	inner.service[0] = 90 * 5_000_000_000 // v0 = 5s
	inner.service[1] = 10 * 500_000_000   // v1 = 0.5s
	inner.clampDebtLocked()
	if inner.service[0] != 90*1_500_000_000 { // (v1 + 1s) * 90
		t.Fatalf("service[0] = %d, want bound", inner.service[0])
	}
	// Within the bound: untouched.
	inner.service[0] = 90 * 1_500_000_000 // v0 = 1.5s
	inner.service[1] = 10 * 1_000_000_000 // v1 = 1s
	inner.clampDebtLocked()
	if inner.service[0] != 90*1_500_000_000 || inner.service[1] != 10*1_000_000_000 {
		t.Fatal("in-bound service moved")
	}
	// Saturation: absurd service plus huge bound must not wrap.
	big := &schedulerInner{localW: 1, remoteW: 1, maxDebtNanos: ^uint64(0)}
	big.service[0] = ^uint64(0)
	big.service[1] = 0
	big.clampDebtLocked()
	if big.service[0] != ^uint64(0) {
		t.Fatalf("saturated clamp wrapped to %d", big.service[0])
	}
	// Zero weights (unvalidated config): no division by zero.
	zero := &schedulerInner{}
	zero.service[0] = 100
	zero.clampDebtLocked()
	if zero.service[0] != 100 {
		t.Fatal("zero-weight clamp moved service")
	}
}
