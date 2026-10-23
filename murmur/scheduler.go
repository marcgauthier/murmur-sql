package murmur

import (
	"sync"
	"time"
)

// WriterClass identifies a writer scheduling class.
type WriterClass int

const (
	// WriterLocal covers local user transactions.
	WriterLocal WriterClass = iota
	// WriterRemote covers mesh/High-Low remote apply and schema adoption.
	WriterRemote
	// WriterMaintenance covers migrations, rewrites, and retention/GC units.
	WriterMaintenance
)

// writerClassCount is the number of scheduling classes.
const writerClassCount = 3

// String renders the class name.
func (c WriterClass) String() string {
	switch c {
	case WriterLocal:
		return "local"
	case WriterRemote:
		return "remote"
	case WriterMaintenance:
		return "maintenance"
	}
	return "unknown"
}

// SchedulerConfig configures writer-time shares and fairness bounds.
type SchedulerConfig struct {
	// Local writer share percentage (e.g. 90).
	LocalShare uint32
	// Remote writer share percentage (e.g. 10).
	RemoteShare uint32
	// Bound on normalized service debt (default 1s).
	MaxDebt time.Duration
	// Reserves 1 grant in every N for waiting maintenance work (default 100).
	MaintenanceReserveEvery uint64
}

// DefaultSchedulerConfig returns the default scheduler configuration.
func DefaultSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		LocalShare: 90, RemoteShare: 10,
		MaxDebt: time.Second, MaintenanceReserveEvery: 100,
	}
}

// Validate checks the configuration.
func (c *SchedulerConfig) Validate() error {
	if c.LocalShare == 0 || c.RemoteShare == 0 {
		return invalidErr("writer shares must be positive (%d/%d)", c.LocalShare, c.RemoteShare)
	}
	if c.LocalShare+c.RemoteShare != 100 {
		return invalidErr("writer shares must total 100 (%d/%d)", c.LocalShare, c.RemoteShare)
	}
	if c.MaxDebt <= 0 {
		return invalidErr("writer max debt must be positive")
	}
	if c.MaintenanceReserveEvery == 0 {
		return invalidErr("maintenance reserve must be positive")
	}
	return nil
}

// SchedulerClassStats carries diagnostics for one writer class.
type SchedulerClassStats struct {
	Acquisitions     uint64
	Cancels          uint64
	WaitNanos        uint64
	ServiceNanos     uint64
	DualServiceNanos uint64
	// DualGrants counts grants made while the other interactive class
	// also had waiters.
	DualGrants      uint64
	Waiters         int
	OldestWaitNanos uint64
}

// SchedulerSnapshot carries coordinator diagnostics.
type SchedulerSnapshot struct {
	Local       SchedulerClassStats
	Remote      SchedulerClassStats
	Maintenance SchedulerClassStats
	DebtNanos   uint64
	Shares      [2]uint32
}

type schedulerWaiter struct {
	id      uint64
	enqueue time.Time
	reply   chan schedulerGrant
}

type schedulerGrant struct {
	dual bool
	err  error
}

type schedulerInner struct {
	localW, remoteW         uint64
	maxDebtNanos            uint64
	maintenanceReserveEvery uint64

	closed           bool
	active           bool
	activeClass      WriterClass
	nextWaiterID     uint64
	queues           [writerClassCount][]*schedulerWaiter
	sinceMaintenance uint64

	acquisitions     [writerClassCount]uint64
	cancels          [writerClassCount]uint64
	waitNanos        [writerClassCount]uint64
	serviceNanos     [writerClassCount]uint64
	dualServiceNanos [writerClassCount]uint64
	dualGrants       [writerClassCount]uint64
	service          [2]uint64
}

func (s *schedulerInner) queuesEmpty() bool {
	return len(s.queues[0]) == 0 && len(s.queues[1]) == 0 && len(s.queues[2]) == 0
}

func satAdd(a, b uint64) uint64 {
	if c := a + b; c >= a {
		return c
	}
	return ^uint64(0)
}

func satMul(a, b uint64) uint64 {
	if a == 0 || b == 0 {
		return 0
	}
	if c := a * b; c/a == b {
		return c
	}
	return ^uint64(0)
}

func (s *schedulerInner) clampDebtLocked() {
	if s.localW == 0 || s.remoteW == 0 {
		return
	}
	v0 := s.service[0] / s.localW
	v1 := s.service[1] / s.remoteW
	if v0 > satAdd(v1, s.maxDebtNanos) {
		s.service[0] = satMul(satAdd(v1, s.maxDebtNanos), s.localW)
	} else if v1 > satAdd(v0, s.maxDebtNanos) {
		s.service[1] = satMul(satAdd(v0, s.maxDebtNanos), s.remoteW)
	}
}

func (s *schedulerInner) grantNextLocked() {
	if s.closed || s.active {
		return
	}
	localQ := len(s.queues[WriterLocal]) > 0
	remoteQ := len(s.queues[WriterRemote]) > 0
	maintenanceQ := len(s.queues[WriterMaintenance]) > 0
	if !localQ && !remoteQ && !maintenanceQ {
		return
	}
	var class WriterClass
	switch {
	case maintenanceQ && s.sinceMaintenance >= s.maintenanceReserveEvery:
		class = WriterMaintenance
	case localQ && remoteQ:
		// Compare service/weight cross-multiplied in 128-bit.
		sLocalHi, sLocalLo := bitsMul64(s.service[0], s.remoteW)
		sRemoteHi, sRemoteLo := bitsMul64(s.service[1], s.localW)
		if sLocalHi < sRemoteHi || (sLocalHi == sRemoteHi && sLocalLo <= sRemoteLo) {
			class = WriterLocal
		} else {
			class = WriterRemote
		}
	case localQ:
		class = WriterLocal
	case remoteQ:
		class = WriterRemote
	default:
		class = WriterMaintenance
	}
	if class == WriterMaintenance {
		s.sinceMaintenance = 0
	} else {
		s.sinceMaintenance++
	}
	dual := (class == WriterLocal && remoteQ) || (class == WriterRemote && localQ)
	q := s.queues[class]
	waiter := q[0]
	s.queues[class] = q[1:]
	s.active = true
	s.activeClass = class
	s.acquisitions[class]++
	if dual {
		s.dualGrants[class]++
		wait := time.Since(waiter.enqueue)
		if wait < 0 {
			wait = 0
		}
		s.waitNanos[class] += uint64(wait.Nanoseconds())
	}
	select {
	case waiter.reply <- schedulerGrant{dual: dual}:
	default:
	}
}

func bitsMul64(a, b uint64) (hi, lo uint64) {
	const mask = uint64(0xffffffff)
	a0, a1 := a&mask, a>>32
	b0, b1 := b&mask, b>>32
	p0 := a0 * b0
	p1 := a0 * b1
	p2 := a1 * b0
	p3 := a1 * b1
	mid := (p0 >> 32) + (p1 & mask) + (p2 & mask)
	lo = (p0 & mask) | (mid << 32)
	hi = p3 + (p1 >> 32) + (p2 >> 32) + (mid >> 32)
	return hi, lo
}

// WriterScheduler is a fair exclusive writer-admission coordinator.
type WriterScheduler struct {
	mu    sync.Mutex
	inner *schedulerInner
}

// NewWriterScheduler builds a scheduler from config.
func NewWriterScheduler(config SchedulerConfig) *WriterScheduler {
	return &WriterScheduler{inner: &schedulerInner{
		localW: uint64(config.LocalShare), remoteW: uint64(config.RemoteShare),
		maxDebtNanos:            uint64(config.MaxDebt.Nanoseconds()),
		maintenanceReserveEvery: config.MaintenanceReserveEvery,
		nextWaiterID:            1,
	}}
}

func (s *WriterScheduler) idleGrant(class WriterClass) (*Ticket, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.inner
	if in.closed || in.active || !in.queuesEmpty() {
		return nil, false
	}
	in.active = true
	in.activeClass = class
	if class == WriterMaintenance {
		in.sinceMaintenance = 0
	} else {
		in.sinceMaintenance++
	}
	in.acquisitions[class]++
	return &Ticket{class: class, grantedAt: time.Now(), sched: s}, true
}

// Admit queues for writer admission, granting in class-share order.
func (s *WriterScheduler) Admit(class WriterClass) (*Ticket, error) {
	if ticket, ok := s.idleGrant(class); ok {
		return ticket, nil
	}
	reply := make(chan schedulerGrant, 1)
	s.mu.Lock()
	in := s.inner
	if in.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	in.nextWaiterID++
	in.queues[class] = append(in.queues[class], &schedulerWaiter{id: in.nextWaiterID, enqueue: time.Now(), reply: reply})
	in.grantNextLocked()
	s.mu.Unlock()
	grant, ok := <-reply
	if !ok {
		return nil, ErrClosed
	}
	if grant.err != nil {
		return nil, grant.err
	}
	return &Ticket{class: class, grantedAt: time.Now(), dual: grant.dual, sched: s}, nil
}

// AdmitTimeout queues for writer admission with a timeout.
func (s *WriterScheduler) AdmitTimeout(class WriterClass, timeout time.Duration) (*Ticket, error) {
	if ticket, ok := s.idleGrant(class); ok {
		return ticket, nil
	}
	reply := make(chan schedulerGrant, 1)
	s.mu.Lock()
	in := s.inner
	if in.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	in.nextWaiterID++
	id := in.nextWaiterID
	in.queues[class] = append(in.queues[class], &schedulerWaiter{id: id, enqueue: time.Now(), reply: reply})
	in.grantNextLocked()
	s.mu.Unlock()
	select {
	case grant, ok := <-reply:
		if !ok {
			return nil, ErrClosed
		}
		if grant.err != nil {
			return nil, grant.err
		}
		return &Ticket{class: class, grantedAt: time.Now(), dual: grant.dual, sched: s}, nil
	case <-time.After(timeout):
		s.mu.Lock()
		q := in.queues[class]
		for i, w := range q {
			if w.id == id {
				in.queues[class] = append(q[:i], q[i+1:]...)
				in.cancels[class]++
				s.mu.Unlock()
				return nil, ErrBusy
			}
		}
		s.mu.Unlock()
		// Already granted concurrently: grant wins.
		grant, ok := <-reply
		if !ok {
			return nil, ErrClosed
		}
		if grant.err != nil {
			return nil, grant.err
		}
		return &Ticket{class: class, grantedAt: time.Now(), dual: grant.dual, sched: s}, nil
	}
}

// Active reports whether a ticket is currently active.
func (s *WriterScheduler) Active() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.active
}

// Close wakes all waiters with ErrClosed.
func (s *WriterScheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.inner
	if in.closed {
		return
	}
	in.closed = true
	for i := 0; i < writerClassCount; i++ {
		for _, w := range in.queues[i] {
			select {
			case w.reply <- schedulerGrant{err: ErrClosed}:
			default:
			}
		}
		in.queues[i] = nil
	}
}

// Snapshot returns coordinator diagnostics.
func (s *WriterScheduler) Snapshot() SchedulerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.inner
	now := time.Now()
	var stats [writerClassCount]SchedulerClassStats
	for i := 0; i < writerClassCount; i++ {
		var oldest uint64
		for _, w := range in.queues[i] {
			age := now.Sub(w.enqueue)
			if age < 0 {
				age = 0
			}
			if uint64(age.Nanoseconds()) > oldest {
				oldest = uint64(age.Nanoseconds())
			}
		}
		stats[i] = SchedulerClassStats{
			Acquisitions: in.acquisitions[i], Cancels: in.cancels[i],
			WaitNanos: in.waitNanos[i], ServiceNanos: in.serviceNanos[i],
			DualServiceNanos: in.dualServiceNanos[i], DualGrants: in.dualGrants[i],
			Waiters: len(in.queues[i]), OldestWaitNanos: oldest,
		}
	}
	var v0, v1 uint64
	if in.localW > 0 {
		v0 = in.service[0] / in.localW
	}
	if in.remoteW > 0 {
		v1 = in.service[1] / in.remoteW
	}
	var debt uint64
	if v0 > v1 {
		debt = v0 - v1
	} else {
		debt = v1 - v0
	}
	return SchedulerSnapshot{
		Local: stats[0], Remote: stats[1], Maintenance: stats[2],
		DebtNanos: debt, Shares: [2]uint32{uint32(in.localW), uint32(in.remoteW)},
	}
}

// Ticket is one granted writer admission. Call Release when done;
// tickets do not self-release (no finalizers).
type Ticket struct {
	class     WriterClass
	grantedAt time.Time
	dual      bool
	sched     *WriterScheduler
	released  bool
	mu        sync.Mutex
}

// Class returns the ticket's writer class.
func (t *Ticket) Class() WriterClass { return t.class }

// Dual reports whether the other interactive class also had waiters.
func (t *Ticket) Dual() bool { return t.dual }

// GrantedAt returns the grant time.
func (t *Ticket) GrantedAt() time.Time { return t.grantedAt }

// Release returns the ticket, accounting service time.
func (t *Ticket) Release() {
	t.mu.Lock()
	if t.released {
		t.mu.Unlock()
		return
	}
	t.released = true
	t.mu.Unlock()
	service := time.Since(t.grantedAt)
	if service < 0 {
		service = 0
	}
	nanos := uint64(service.Nanoseconds())
	s := t.sched
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.inner
	in.serviceNanos[t.class] += nanos
	if t.dual {
		in.dualServiceNanos[t.class] += nanos
	}
	if t.class == WriterLocal {
		in.service[0] += nanos
	} else if t.class == WriterRemote {
		in.service[1] += nanos
	}
	in.clampDebtLocked()
	in.active = false
	in.grantNextLocked()
}
