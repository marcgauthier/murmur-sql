// Live scenario: long-duration combined-fault endurance with chaos.
//
// Go port of the Rust `endurance-chaos` scenario
// (/home/marc/MURMUR/tests-live/endurance-chaos/tests/endurance_chaos.rs).
// A live multi-node mesh absorbs continuous writes while rolling restarts,
// peer flaps, disk pressure, online key rotation, snapshot resyncs, and
// origin-log GC overlap for a configured chaos window. The verdict is exact
// cross-node convergence (equal row counts plus identical PK-ordered row
// contents) with counter-proven GC progress on every node, at least one
// proven snapshot resync, successful key rotations, a rejoined full mesh,
// and a post-chaos write. Plain SQL replaces the typed-contention HTTP API;
// tc/netem impairment degrades to logical peer flaps and libfaketime clock
// skew is skipped (see README.md for the deviations).
package endurancechaos

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// minNodes/maxNodes is the mesh-size clamp (min(max(nodes, 3), 16)).
	minNodes = 3
	maxNodes = 16
	// maxWriters is the writer-thread cap (min(max(nodes, 2), 8)).
	maxWriters = 8
	// minMaxRows is the bounded working-set floor (maxRows < 100 fatal).
	minMaxRows = 100
	// minSnapshotOff is the snapshot offline-window floor (snapshotOff <
	// 60 fatal: retention expiry + GC tick).
	minSnapshotOff = 60 * time.Second
	// baselineTimeout is the baseline replication deadline (2 minutes).
	baselineTimeout = 120 * time.Second
	// directorTick is the director tick (1 s ticker).
	directorTick = time.Second
	// impairWarmup is the first impairment delay (15 s stagger after
	// writers warm up).
	impairWarmup = 15 * time.Second
	// diskWarmup is the disk-loop stagger (30 s).
	diskWarmup = 30 * time.Second
	// verdictPoll is the convergence poll interval (2 s).
	verdictPoll = 2 * time.Second
	// meshTimeout is the full-mesh wait bound.
	meshTimeout = 60 * time.Second
	// snapProofTimeout is the snapshot catch-up observation bound: the
	// victim's own restart plus a donor build plus the chunk transfer can
	// take ~45 s under chaos, and the victim can lose the donor race to a
	// stale donor, forcing a second snapshot cycle. The event-driven proof
	// polls up to 480 s so two full cycles fit; the bound covers -race
	// runs where snapshot builds/installs slow past 260 s, and the proof
	// returns as soon as the install lands so fast runs pay nothing.
	snapProofTimeout = 480 * time.Second
	// snapProofPoll is the snapshot proof poll interval.
	snapProofPoll = 2 * time.Second
	// errSampleCap caps distinct error-message samples (first 8).
	errSampleCap = 8
	// errTruncChars truncates stored error messages (200 chars).
	errTruncChars = 200
	// diskHeadroomBytes is the headroom the disk filler must leave free
	// (1 GiB).
	diskHeadroomBytes = uint64(1) << 30
	// diskMinMB is the minimum filler size before the disk leg degrades
	// away (16 MiB).
	diskMinMB = uint64(16)
)

// envUsize is a usize env knob with fallback.
func envUsize(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

// envU64 is a u64 env knob with fallback.
func envU64(name string, fallback uint64) uint64 {
	if v, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil {
		return v
	}
	return fallback
}

// envMillis is a millisecond-duration env knob with fallback.
func envMillis(name string, fallbackMs uint64) time.Duration {
	if v, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil && v > 0 {
		return time.Duration(v) * time.Millisecond
	}
	return time.Duration(fallbackMs) * time.Millisecond
}

// fmtDur renders durations Go-style (90s -> 1m30s) for progress lines.
func fmtDur(d time.Duration) string {
	s := int64(d.Seconds())
	switch {
	case s >= 3600:
		return fmt.Sprintf("%dh%dm%ds", s/3600, s%3600/60, s%60)
	case s >= 60:
		return fmt.Sprintf("%dm%ds", s/60, s%60)
	case s >= 1:
		return fmt.Sprintf("%ds", s)
	default:
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
}

// rng is a deterministic splitmix64.
type rng struct{ state uint64 }

func newRng(seed uint64) *rng { return &rng{state: seed} }

func (r *rng) next() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *rng) below(n int) int {
	if n <= 0 {
		panic("rng range must be non-empty")
	}
	return int(r.next() % uint64(n))
}

// float returns a uniform float in [0, 1) (53-bit precision).
func (r *rng) float() float64 {
	return float64(r.next()>>11) / float64(uint64(1)<<53)
}

// config is the chaos configuration (same env names/defaults as the
// original).
type config struct {
	nodes           int
	duration        time.Duration
	settle          time.Duration
	writeInterval   time.Duration
	maxRows         int
	seed            uint64
	statusEvery     time.Duration
	restartEvery    time.Duration
	restartDown     time.Duration
	rotateEvery     time.Duration
	snapshotEvery   time.Duration
	snapshotOff     time.Duration
	impairOn        time.Duration
	impairOff       time.Duration
	impairLatencyMs uint64
	impairLossPct   uint64
	diskEvery       time.Duration
	diskMB          uint64
	diskHold        time.Duration
	skewDur         time.Duration
}

func loadConfig(t *testing.T) config {
	t.Helper()
	cfg := config{
		nodes:           min(max(envUsize("MURMUR_ENDURANCE_NODES", 5), minNodes), maxNodes),
		duration:        harness.EnvSeconds("MURMUR_ENDURANCE_DURATION_SECONDS", 600),
		settle:          harness.EnvSeconds("MURMUR_ENDURANCE_SETTLE_SECONDS", 300),
		writeInterval:   envMillis("MURMUR_ENDURANCE_WRITE_INTERVAL_MS", 25),
		maxRows:         envUsize("MURMUR_ENDURANCE_MAX_ROWS", 10_000),
		seed:            envU64("MURMUR_ENDURANCE_SEED", 1),
		statusEvery:     harness.EnvSeconds("MURMUR_ENDURANCE_STATUS_SECONDS", 30),
		restartEvery:    harness.EnvSeconds("MURMUR_ENDURANCE_RESTART_SECONDS", 90),
		restartDown:     harness.EnvSeconds("MURMUR_ENDURANCE_RESTART_DOWN_SECONDS", 8),
		rotateEvery:     harness.EnvSeconds("MURMUR_ENDURANCE_ROTATE_SECONDS", 180),
		snapshotEvery:   harness.EnvSeconds("MURMUR_ENDURANCE_SNAPSHOT_SECONDS", 300),
		snapshotOff:     harness.EnvSeconds("MURMUR_ENDURANCE_SNAPSHOT_OFFLINE_SECONDS", 75),
		impairOn:        harness.EnvSeconds("MURMUR_ENDURANCE_IMPAIR_ON_SECONDS", 60),
		impairOff:       harness.EnvSeconds("MURMUR_ENDURANCE_IMPAIR_OFF_SECONDS", 20),
		impairLatencyMs: envU64("MURMUR_ENDURANCE_IMPAIR_LATENCY_MS", 150),
		impairLossPct:   envU64("MURMUR_ENDURANCE_IMPAIR_LOSS_PCT", 3),
		diskEvery:       harness.EnvSeconds("MURMUR_ENDURANCE_DISK_SECONDS", 180),
		diskMB:          envU64("MURMUR_ENDURANCE_DISK_MB", 128),
		diskHold:        harness.EnvSeconds("MURMUR_ENDURANCE_DISK_HOLD_SECONDS", 60),
		skewDur:         harness.EnvSeconds("MURMUR_ENDURANCE_SKEW_SECONDS", 120),
	}
	if cfg.maxRows < minMaxRows {
		t.Fatal("MURMUR_ENDURANCE_MAX_ROWS must be >= 100")
	}
	if cfg.snapshotOff < minSnapshotOff {
		t.Fatal("MURMUR_ENDURANCE_SNAPSHOT_OFFLINE_SECONDS must be >= 60 (retention expiry + GC tick)")
	}
	return cfg
}

// faultCounts tallies executed faults.
type faultCounts struct {
	restarts     uint64
	rotations    uint64
	impairCycles uint64
	diskCycles   uint64
	snapshots    uint64
	skews        uint64
}

// reporter is the live progress reporter: timestamped phase/log lines plus
// the same events for the final markdown report. All methods are safe for
// concurrent use.
type reporter struct {
	start     time.Time
	mu        sync.Mutex
	events    []string
	attempted atomic.Uint64
	succeeded atomic.Uint64
	failed    atomic.Uint64
	faults    faultCounts
	rotSeq    atomic.Uint64
	errSeen   map[string]int
	errSample []string
}

func newReporter() *reporter {
	return &reporter{start: time.Now(), errSeen: make(map[string]int)}
}

func (r *reporter) stamp() string {
	return "[T+" + fmtDur(time.Since(r.start)) + "]"
}

func (r *reporter) emit(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	r.mu.Lock()
	r.events = append(r.events, msg)
	r.mu.Unlock()
}

// phase announces a phase transition.
func (r *reporter) phase(name, detail string) {
	r.emit(fmt.Sprintf("%s === PHASE: %s === %s", r.stamp(), name, detail))
}

// log records a timestamped progress line.
func (r *reporter) log(msg string) {
	r.emit(r.stamp() + " " + msg)
}

func (r *reporter) countAttempt(ok bool) {
	r.attempted.Add(1)
	if ok {
		r.succeeded.Add(1)
	} else {
		r.failed.Add(1)
	}
}

func (r *reporter) countFault(kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch kind {
	case "restart":
		r.faults.restarts++
	case "rotate":
		r.faults.rotations++
	case "impair":
		r.faults.impairCycles++
	case "disk":
		r.faults.diskCycles++
	case "snapshot":
		r.faults.snapshots++
	case "skew":
		r.faults.skews++
	}
}

func (r *reporter) nextRotation() uint64 {
	return r.rotSeq.Add(1)
}

func (r *reporter) faultSnapshot() faultCounts {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.faults
}

// sampleErr records distinct write-error messages (first 8) with counts.
func (r *reporter) sampleErr(err string) {
	msg := truncateRunes(err, errTruncChars)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errSeen[msg]++
	if len(r.errSample) < errSampleCap && !slices.Contains(r.errSample, msg) {
		r.errSample = append(r.errSample, msg)
	}
}

// verdict logs the final PASS/FAIL line and writes the markdown report.
// Failure artifact retention itself is handled by Cluster.Finish.
func (r *reporter) verdict(pass bool, reportPath, summary string) {
	status := "FAIL"
	if pass {
		status = "PASS"
	}
	r.emit(fmt.Sprintf("%s === VERDICT: %s === %s", r.stamp(), status, summary))
	r.mu.Lock()
	events := slices.Clone(r.events)
	errSample := slices.Clone(r.errSample)
	errSeen := make(map[string]int, len(r.errSeen))
	for k, v := range r.errSeen {
		errSeen[k] = v
	}
	f := r.faults
	r.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "# Endurance-chaos run report — %s\n\n", status)
	fmt.Fprintf(&b, "- finished (unix): %d\n- wall time: %s\n- writes attempted: %d\n- writes acked: %d\n- writes failed (transient): %d\n- faults: restarts=%d rotations=%d impairCycles=%d diskCycles=%d snapshots=%d skews=%d\n- verdict: %s\n\n",
		time.Now().Unix(), fmtDur(time.Since(r.start)), r.attempted.Load(), r.succeeded.Load(), r.failed.Load(),
		f.restarts, f.rotations, f.impairCycles, f.diskCycles, f.snapshots, f.skews, summary)
	if len(errSample) > 0 {
		b.WriteString("## Distinct write errors\n\n")
		for _, s := range errSample {
			fmt.Fprintf(&b, "- (%dx) %s\n", errSeen[s], s)
		}
		b.WriteString("\n")
	}
	b.WriteString("## Timeline\n\n")
	for _, e := range events {
		fmt.Fprintf(&b, "- %s\n", e)
	}
	if err := os.WriteFile(reportPath, []byte(b.String()), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "report write failed: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "report written to %s\n", reportPath)
	}
}

func truncateRunes(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n])
}

// idRegistry is the bounded working-set registry: every appended id plus
// the live subset; deletes remove from live only.
type idRegistry struct {
	mu   sync.Mutex
	ids  []string
	live map[string]bool
}

func newIDRegistry() *idRegistry {
	return &idRegistry{live: make(map[string]bool)}
}

func (r *idRegistry) add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
	r.live[id] = true
}

func (r *idRegistry) remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.live, id)
}

func (r *idRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.live)
}

// pick returns a random live id, tolerating entries deleted since append.
func (r *idRegistry) pick(rng *rng) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.live) == 0 || len(r.ids) == 0 {
		return "", false
	}
	for range 8 {
		id := r.ids[rng.below(len(r.ids))]
		if r.live[id] {
			return id, true
		}
	}
	return "", false
}

// gcHistory is the restart-proof per-node GC history: in-memory
// retention_runs resets on every restart, so the sampler records
// high-water observations and the verdict asserts on those.
type gcHistory struct {
	mu       sync.Mutex
	ever     []bool
	collEver []bool
	gcErr    []string
	hasErr   []bool
}

func newGCHistory(nodes int) *gcHistory {
	return &gcHistory{
		ever:     make([]bool, nodes),
		collEver: make([]bool, nodes),
		gcErr:    make([]string, nodes),
		hasErr:   make([]bool, nodes),
	}
}

func (g *gcHistory) observe(idx int, runs, collected uint64, lastError *string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if runs > 0 {
		g.ever[idx] = true
	}
	if collected > 0 {
		g.collEver[idx] = true
	}
	if lastError != nil && !g.hasErr[idx] && isGCError(*lastError) {
		g.gcErr[idx] = truncateRunes(*lastError, errTruncChars)
		g.hasErr[idx] = true
	}
}

func (g *gcHistory) snapshot(idx int) (ever, collEver bool, gcErr string, hasErr bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ever[idx], g.collEver[idx], g.gcErr[idx], g.hasErr[idx]
}

// isGCError reports whether an engine error string plausibly comes from
// log-GC collection. The engine multiplexes every subsystem error into one
// last_error latch (peer disconnects during restarts/flaps latch here too),
// so only GC-substring matches fail the verdict.
func isGCError(msg string) bool {
	lower := strings.ToLower(msg)
	return strings.Contains(lower, "retention") ||
		strings.Contains(lower, "collect") ||
		strings.Contains(lower, "gc_") ||
		strings.Contains(lower, "prune")
}

// gcSample returns the GC counters for one node: collection passes,
// collected transactions, latest engine error. ok is false when the node is
// unreachable (stopped or mid-restart).
func gcSample(db *murmur.Database) (runs, collected uint64, lastError *string, ok bool) {
	h := db.Replication()
	if h == nil {
		return 0, 0, nil, false
	}
	status := h.Status()
	rs, err := db.RetentionStatus()
	if err != nil {
		return 0, 0, nil, false
	}
	return status.Metrics.RetentionRuns, rs.CollectedTransactions, status.LastError, true
}

// appliedCount is the log-path applied-transaction counter for one node
// (snapshot application bypasses it, so it underpins the snapshot proof).
func appliedCount(db *murmur.Database) uint64 {
	if h := db.Replication(); h != nil {
		return h.Status().Metrics.AppliedTransactions
	}
	return 0
}

// snapshotsApplied is the durably applied snapshot counter for one node.
func snapshotsApplied(db *murmur.Database) uint64 {
	if h := db.Replication(); h != nil {
		return h.Status().Metrics.SnapshotsApplied
	}
	return 0
}

// rowCount returns the row count on one node, or -1 when the read fails.
func rowCount(db *murmur.Database) int {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM contention")
	})
	if err != nil {
		return -1
	}
	return int(v.([][]sqlite.Value)[0][0].Int)
}

// canonicalRows returns the PK-ordered encoded rows on one node
// (id:name:phone:score sorted by id), or nil when the read fails.
func canonicalRows(db *murmur.Database) []string {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name, phone, score FROM contention ORDER BY id")
	})
	if err != nil {
		return nil
	}
	rows := v.([][]sqlite.Value)
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, fmt.Sprintf("%s:%s:%s:%d", row[0].Str, row[1].Str, row[2].Str, row[3].Int))
	}
	return out
}

// digestHex is a deterministic log-only digest over canonical rows
// (comparisons use byte-identical vectors, which imply identical digests).
// FNV-1a keeps this dependency-free.
func digestHex(rows []string) string {
	h := uint64(0xcbf29ce484222325)
	for _, row := range rows {
		for i := 0; i < len(row); i++ {
			h ^= uint64(row[i])
			h *= 0x100000001b3
		}
		h ^= uint64('\n')
		h *= 0x100000001b3
	}
	return fmt.Sprintf("%016x", h)
}

// rowDiff is the size of the symmetric difference between two canonical row
// sets.
func rowDiff(a, b []string) int {
	inB := make(map[string]bool, len(b))
	for _, s := range b {
		inB[s] = true
	}
	inA := make(map[string]bool, len(a))
	diff := 0
	for _, s := range a {
		inA[s] = true
		if !inB[s] {
			diff++
		}
	}
	for _, s := range b {
		if !inA[s] {
			diff++
		}
	}
	return diff
}

// dbSlot is one entry of the shared handle table: the entry for a restarted
// node is swapped for the reopened database, and an alive-set steers
// targeting away from stopped nodes.
type dbSlot struct {
	mu sync.Mutex
	db *murmur.Database
}

func (s *dbSlot) get() *murmur.Database {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db
}

func (s *dbSlot) set(db *murmur.Database) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db = db
}

// pickAlive picks a random live node; ok is false when all are down.
func pickAlive(alive []atomic.Bool, rng *rng) (int, bool) {
	var up []int
	for i := range alive {
		if alive[i].Load() {
			up = append(up, i)
		}
	}
	if len(up) == 0 {
		return 0, false
	}
	return up[rng.below(len(up))], true
}

// writerLoop is the bounded insert/update/delete writer: inserts until
// maxRows live ids, then updates/deletes recycle the set so a long run
// keeps churning replication and GC without unbounded growth. Transient
// errors are counted, never fatal.
func writerLoop(id int, handles []*dbSlot, alive []atomic.Bool, registry *idRegistry, rep *reporter, seed uint64, writeInterval time.Duration, maxRows int, stop *atomic.Bool) {
	rng := newRng(seed*1000 + uint64(id))
	var seq uint64
	for !stop.Load() {
		target, ok := pickAlive(alive, rng)
		if !ok {
			time.Sleep(writeInterval)
			continue
		}
		roll := rng.float()
		db := handles[target].get()
		switch {
		case registry.len() < maxRows && roll < 0.7:
			seq++
			insertID := fmt.Sprintf("%08x%024x", id, seq)
			name := fmt.Sprintf("writer-%d", id)
			phone := fmt.Sprintf("v-%d-%d", id, seq)
			_, txErr := db.Transaction(func(s *murmur.Sql) (any, error) {
				_, err := s.Execute(
					"INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
					insertID, name, phone, int64(0),
				)
				return nil, err
			})
			var outcome error
			if txErr != nil {
				outcome = txErr
			} else {
				outcome = db.Flush()
			}
			if outcome == nil {
				registry.add(insertID)
				rep.countAttempt(true)
			} else {
				rep.countAttempt(false)
				rep.sampleErr(outcome.Error())
			}
		case roll < 0.85:
			rowID, ok := registry.pick(rng)
			if !ok {
				time.Sleep(writeInterval)
				continue
			}
			seq++
			name := fmt.Sprintf("writer-%d", id)
			phone := fmt.Sprintf("u-%d-%d", id, seq)
			_, txErr := db.Transaction(func(s *murmur.Sql) (any, error) {
				changed, err := s.Execute(
					"UPDATE contention SET name = ?, phone = ? WHERE id = ?",
					name, phone, rowID,
				)
				if err != nil {
					return nil, err
				}
				if changed != 1 {
					return nil, fmt.Errorf("update hit %d rows", changed)
				}
				return nil, nil
			})
			var outcome error
			if txErr != nil {
				outcome = txErr
			} else {
				outcome = db.Flush()
			}
			if outcome == nil {
				rep.countAttempt(true)
			} else {
				rep.countAttempt(false)
				rep.sampleErr(outcome.Error())
			}
		default:
			rowID, ok := registry.pick(rng)
			if !ok {
				time.Sleep(writeInterval)
				continue
			}
			_, txErr := db.Transaction(func(s *murmur.Sql) (any, error) {
				_, err := s.Execute("DELETE FROM contention WHERE id = ?", rowID)
				return nil, err
			})
			var outcome error
			if txErr != nil {
				outcome = txErr
			} else {
				outcome = db.Flush()
			}
			if outcome == nil {
				registry.remove(rowID)
				rep.countAttempt(true)
			} else {
				rep.countAttempt(false)
				rep.sampleErr(outcome.Error())
			}
		}
		if stop.Load() {
			return
		}
		time.Sleep(writeInterval)
	}
}

// statusLoop is the heartbeat sampler: row spread, fault tallies, GC
// high-water observations, spool/disk pressure. It reads through the shared
// handle table, never through the cluster (the director owns it).
func statusLoop(handles []*dbSlot, nodeDirs []string, rep *reporter, gcHist *gcHistory, nodes int, statusEvery time.Duration, stop *atomic.Bool) {
	for sleepStop(statusEvery, stop) {
		minC, maxC := int64(-1), int64(-1)
		down := 0
		for _, h := range handles {
			c := rowCount(h.get())
			if c < 0 {
				down++
				continue
			}
			if minC < 0 || int64(c) < minC {
				minC = int64(c)
			}
			if int64(c) > maxC {
				maxC = int64(c)
			}
		}
		var spoolMax uint64
		var gcRuns uint64
		for i, h := range handles {
			db := h.get()
			spoolMax = max(spoolMax, db.SpoolMetrics().DiskBytes)
			if runs, collected, lastErr, ok := gcSample(db); ok {
				gcRuns += runs
				gcHist.observe(i, runs, collected, lastErr)
			}
		}
		var diskMin *uint64
		for _, dir := range nodeDirs {
			if free, ok := dirFreeBytes(dir); ok {
				if diskMin == nil || free < *diskMin {
					v := free
					diskMin = &v
				}
			}
		}
		var diskMinMB uint64
		if diskMin != nil {
			diskMinMB = *diskMin >> 20
		}
		f := rep.faultSnapshot()
		rep.log(fmt.Sprintf(
			"status: writes ok=%d err=%d | rows min=%d max=%d down=%d/%d | restart=%d rotate=%d impair=%d disk=%d snap=%d skew=%d | gcRuns=%d spoolMax=%dMiB diskFreeMin=%dMiB",
			rep.succeeded.Load(), rep.failed.Load(), minC, maxC, down, nodes,
			f.restarts, f.rotations, f.impairCycles, f.diskCycles, f.snapshots, f.skews,
			gcRuns, spoolMax>>20, diskMinMB,
		))
	}
}

// diskLoop is the disk-pressure cycler: holds a filler file in each node
// dir in turn, then releases. Filesystem paths only, no cluster use.
func diskLoop(nodeDirs []string, rep *reporter, diskEvery, diskHold time.Duration, fillerMB uint64, stop *atomic.Bool) {
	if !sleepStop(diskWarmup, stop) {
		return
	}
	idx := 0
	for {
		dir := nodeDirs[idx%len(nodeDirs)]
		idx++
		label := filepath.Base(dir)
		path := filepath.Join(dir, "endurance-filler.bin")
		freeBefore, _ := dirFreeBytes(dir)
		if err := writeFiller(path, fillerMB); err != nil {
			rep.log(fmt.Sprintf("disk: %s filler FAILED: %v", label, err))
			removeFiller(path)
		} else {
			freeAfter, _ := dirFreeBytes(dir)
			rep.log(fmt.Sprintf("disk: %s holding %d MiB (free %d -> %d MiB) for %s",
				label, fillerMB, freeBefore>>20, freeAfter>>20, fmtDur(diskHold)))
			interrupted := !sleepStop(diskHold, stop)
			removeFiller(path)
			rep.countFault("disk")
			if interrupted {
				return
			}
		}
		// Wait out the remainder of the period after the hold.
		if diskEvery > diskHold && !sleepStop(diskEvery-diskHold, stop) {
			return
		}
	}
}

// sleepStop sleeps d, waking early when stop is set. It reports false when
// interrupted.
func sleepStop(d time.Duration, stop *atomic.Bool) bool {
	deadline := time.Now().Add(d)
	for {
		if stop.Load() {
			return false
		}
		now := time.Now()
		if !now.Before(deadline) {
			return true
		}
		rest := deadline.Sub(now)
		if rest > 100*time.Millisecond {
			rest = 100 * time.Millisecond
		}
		time.Sleep(rest)
	}
}

// writeFiller consumes mb megabytes at path with incompressible-ish bytes.
func writeFiller(path string, mb uint64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 1<<20)
	x := uint64(0x9e3779b97f4a7c15)
	for range mb {
		for off := 0; off < len(buf); off += 8 {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			n := 8
			if off+n > len(buf) {
				n = len(buf) - off
			}
			for i := 0; i < n; i++ {
				buf[off+i] = byte(x >> (8 * i))
			}
		}
		if _, err := f.Write(buf); err != nil {
			return err
		}
	}
	return f.Sync()
}

func removeFiller(path string) {
	_ = os.Remove(path)
}

// dirFreeBytes returns the free bytes on the filesystem holding path.
func dirFreeBytes(path string) (uint64, bool) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, false
	}
	bsize := st.Frsize
	if bsize <= 0 {
		bsize = st.Bsize
	}
	if bsize <= 0 {
		return 0, false
	}
	return st.Bavail * uint64(bsize), true
}

// randomKey returns a fresh 32-byte storage key.
func randomKey(t *testing.T) [32]byte {
	t.Helper()
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("key entropy: %v", err)
	}
	return key
}

// snapObservation is the per-window snapshot-path observation.
type snapObservation struct {
	victim       int
	outageAcked  uint64
	appliedDelta uint64
	diffRows     int
	rows         int
	proven       bool
}

// observeSnapshotProof decides whether a rejoined snapshot victim caught up
// via the snapshot path. Every writer op is one transaction, and the victim
// missed every acked op from its outage, so the window counts as proven
// when a snapshot applies and the post-apply sample shows the bulk arrived
// via the snapshot path (row drift below the outage's acked count while the
// log-path applied delta since the apply stays below it too — a pure log
// catch-up would advance applied_transactions by at least the outage's
// count). Writers keep running throughout; the proof polls up to
// snapProofTimeout instead of sampling once at a fixed delay.
func observeSnapshotProof(cluster *harness.Cluster, victim int, rep *reporter, ackedAtStop uint64) snapObservation {
	ackedAtRejoin := rep.succeeded.Load()
	outageAcked := ackedAtRejoin - min(ackedAtRejoin, ackedAtStop)
	snapsAtRejoin := snapshotsApplied(cluster.Node(victim))
	start := time.Now()
	survivor := (victim + 1) % cluster.Len()
	obs := snapObservation{
		victim:      victim,
		outageAcked: outageAcked,
		diffRows: rowDiff(
			nonNilRows(canonicalRows(cluster.Node(victim))),
			nonNilRows(canonicalRows(cluster.Node(survivor))),
		),
	}
	var appliedAtApply *uint64
	for time.Since(start) < snapProofTimeout {
		time.Sleep(snapProofPoll)
		if snapshotsApplied(cluster.Node(victim)) <= snapsAtRejoin {
			continue
		}
		appliedNow := appliedCount(cluster.Node(victim))
		if appliedAtApply == nil {
			v := appliedNow
			appliedAtApply = &v
		}
		victimRows := nonNilRows(canonicalRows(cluster.Node(victim)))
		survivorRows := nonNilRows(canonicalRows(cluster.Node(survivor)))
		obs.diffRows = rowDiff(victimRows, survivorRows)
		obs.rows = len(survivorRows)
		obs.appliedDelta = appliedNow - *appliedAtApply
		// The snapshot covered the bulk of the outage when neither the
		// remaining drift nor the post-apply log tail reaches the
		// outage's acked count.
		obs.proven = outageAcked > 100 &&
			uint64(obs.diffRows) < outageAcked &&
			obs.appliedDelta < outageAcked
		if obs.proven {
			break
		}
	}
	rep.log(fmt.Sprintf("snapshot: node%d proof outageAcked=%d appliedDelta=%d diffRows=%d/%d proven=%v",
		obs.victim+1, obs.outageAcked, obs.appliedDelta, obs.diffRows, obs.rows, obs.proven))
	return obs
}

func nonNilRows(rows []string) []string {
	if rows == nil {
		return []string{}
	}
	return rows
}

// directorReport tells the verdict which scheduled legs actually fit in the
// run, so short dev passes skip legs loudly instead of failing on coverage
// they could never have produced.
type directorReport struct {
	snapshotsPossible bool
	skewScheduled     bool
}

// runDirector is the chaos director: all node lifecycle runs here, on the
// test thread. It owns restarts, rotations, snapshot windows, and the
// impairment flaps (integrated here rather than on a background thread
// because peer add/remove borrows the cluster).
func runDirector(t *testing.T, cluster *harness.Cluster, handles []*dbSlot, alive []atomic.Bool, rep *reporter, cfg config, curKeys [][32]byte, snapProofs *[]snapObservation, snapMu *sync.Mutex) directorReport {
	t.Helper()
	rng := newRng(cfg.seed*7919 + 17)
	start := time.Now()
	deadline := start.Add(cfg.duration)

	nextRestart := start.Add(cfg.restartEvery / 2)
	nextRotate := start.Add(cfg.rotateEvery / 2)
	rotateIdx := 0

	// Snapshot windows need a full offline window plus reconvergence room
	// before the verdict; short runs skip the leg loudly.
	snapOK := true
	nextSnapStop := start.Add(cfg.duration / 4)
	if nextSnapStop.Add(cfg.snapshotOff).Add(60 * time.Second).After(deadline) {
		nextSnapStop = start.Add(30 * time.Second)
		if nextSnapStop.Add(cfg.snapshotOff).Add(60 * time.Second).After(deadline) {
			snapOK = false
			rep.log("snapshot: run too short for an offline window, leg skipped")
		}
	}
	snapVictim := -1
	snapRestartAt := time.Now()
	var snapAckedAtStop uint64

	// Clock skew is degraded away: in-process nodes share the test driver
	// clock, so no node can run under libfaketime.
	skewScheduled := false

	// Impairment phases cycle latency -> loss -> both, all implemented as
	// logical edge flaps (no tc privilege model in-process).
	phases := []string{"latency", "loss", "latency+loss"}
	phase := 0
	nextImpair := start.Add(impairWarmup)
	impairActive := false
	var impairA, impairB int
	var impairRestoreAt time.Time

	for {
		if !time.Now().Before(deadline) {
			break
		}
		// Sleep to the next tick, then re-sample time (1 s ticker).
		time.Sleep(directorTick)
		now := time.Now()
		if !now.Before(deadline) {
			break
		}

		// Restore an expired impairment flap.
		if impairActive && !now.Before(impairRestoreAt) {
			_ = cluster.AddPeer(impairA, impairB)
			_ = cluster.AddPeer(impairB, impairA)
			impairActive = false
			rep.countFault("impair")
			rep.log(fmt.Sprintf("impair: edge node%d <-> node%d restored", impairA+1, impairB+1))
			nextImpair = now.Add(cfg.impairOff)
		}
		// Open the next impairment flap.
		if !impairActive && !now.Before(nextImpair) {
			a := rng.below(cfg.nodes)
			b := (a + 1 + rng.below(cfg.nodes-1)) % cfg.nodes
			_ = cluster.RemovePeer(a, b)
			_ = cluster.RemovePeer(b, a)
			rep.log(fmt.Sprintf("impair: flap %s (logical) node%d <-> node%d for %s (no tc privilege model in-process)",
				phases[phase%len(phases)], a+1, b+1, fmtDur(cfg.impairOn)))
			phase++
			impairActive = true
			impairA, impairB = a, b
			impairRestoreAt = now.Add(cfg.impairOn)
		}

		if !now.Before(nextRestart) {
			// Never restart the offline snapshot victim out from under
			// its window.
			idx := rng.below(cfg.nodes)
			for range 16 {
				if idx != snapVictim {
					break
				}
				idx = rng.below(cfg.nodes)
			}
			// In-process close()+reopen for every restart: true SIGKILL
			// vs graceful cannot target one in-process node without
			// freezing the test driver.
			rep.log(fmt.Sprintf("restart: node%d going down (%s downtime)", idx+1, fmtDur(cfg.restartDown)))
			alive[idx].Store(false)
			cluster.StopNode(idx)
			time.Sleep(cfg.restartDown)
			if err := cluster.StartNode(idx); err != nil {
				t.Fatalf("node%d restart failed: %v", idx+1, err)
			}
			if err := cluster.Mesh(); err != nil {
				t.Fatalf("re-mesh after restart: %v", err)
			}
			handles[idx].set(cluster.Node(idx))
			alive[idx].Store(true)
			rep.countFault("restart")
			rep.log(fmt.Sprintf("restart: node%d back", idx+1))
			halfMs := max(int64(cfg.restartEvery.Milliseconds()/2), 1)
			jitter := time.Duration(rng.below(int(halfMs))) * time.Millisecond
			nextRestart = time.Now().Add(cfg.restartEvery).Add(jitter)
		}

		if !now.Before(nextRotate) {
			idx := -1
			for k := 0; k < cfg.nodes; k++ {
				cand := (rotateIdx + k) % cfg.nodes
				if alive[cand].Load() && cand != snapVictim {
					idx = cand
					break
				}
			}
			rotateIdx = (rotateIdx + 1) % cfg.nodes
			if idx < 0 {
				rep.log("rotate: skipped, no node available")
			} else if err := rotateNode(t, cluster, curKeys, idx, rep); err != nil {
				rep.log(fmt.Sprintf("rotate: node%d FAILED: %v (counted, chaos continues)", idx+1, err))
			}
			nextRotate = time.Now().Add(cfg.rotateEvery)
		}

		if snapOK && snapVictim < 0 && !now.Before(nextSnapStop) {
			v := rng.below(cfg.nodes)
			rep.log(fmt.Sprintf("snapshot: node%d going offline for %s (ranges must expire past retention)",
				v+1, fmtDur(cfg.snapshotOff)))
			alive[v].Store(false)
			cluster.StopNode(v)
			snapVictim = v
			snapAckedAtStop = rep.succeeded.Load()
			snapRestartAt = time.Now().Add(cfg.snapshotOff)
		}
		if snapVictim >= 0 && !now.Before(snapRestartAt) {
			v := snapVictim
			if err := cluster.StartNode(v); err != nil {
				t.Fatalf("node%d snapshot rejoin failed: %v", v+1, err)
			}
			if err := cluster.Mesh(); err != nil {
				t.Fatalf("re-mesh after snapshot rejoin: %v", err)
			}
			handles[v].set(cluster.Node(v))
			alive[v].Store(true)
			rep.countFault("snapshot")
			rep.log(fmt.Sprintf("snapshot: node%d rejoined, must resync via snapshot", v+1))
			obs := observeSnapshotProof(cluster, v, rep, snapAckedAtStop)
			snapMu.Lock()
			*snapProofs = append(*snapProofs, obs)
			snapMu.Unlock()
			snapVictim = -1
			nextSnapStop = time.Now().Add(cfg.snapshotEvery)
		}
	}

	// Restore any open impairment flap without counting it (an
	// interrupted cycle returns before the count).
	if impairActive {
		_ = cluster.AddPeer(impairA, impairB)
		_ = cluster.AddPeer(impairB, impairA)
		rep.log(fmt.Sprintf("impair: edge node%d <-> node%d restored at chaos end (uncounted)", impairA+1, impairB+1))
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("final re-mesh: %v", err)
	}

	// Rejoin anyone still down when the chaos window closes.
	if snapVictim >= 0 {
		v := snapVictim
		if err := cluster.StartNode(v); err != nil {
			t.Fatalf("node%d end-of-chaos rejoin failed: %v", v+1, err)
		}
		if err := cluster.Mesh(); err != nil {
			t.Fatalf("re-mesh after end-of-chaos rejoin: %v", err)
		}
		handles[v].set(cluster.Node(v))
		alive[v].Store(true)
		rep.countFault("snapshot")
		rep.log(fmt.Sprintf("snapshot: node%d rejoined at chaos end", v+1))
		obs := observeSnapshotProof(cluster, v, rep, snapAckedAtStop)
		snapMu.Lock()
		*snapProofs = append(*snapProofs, obs)
		snapMu.Unlock()
	}
	return directorReport{snapshotsPossible: snapOK, skewScheduled: skewScheduled}
}

// rotateNode is the online storage-key rotation on one node: rotate to a
// fresh random key without downtime, then record it for later restarts.
func rotateNode(t *testing.T, cluster *harness.Cluster, curKeys [][32]byte, idx int, rep *reporter) error {
	t.Helper()
	newKey := randomKey(t)
	n := rep.nextRotation()
	if err := cluster.Node(idx).RotateMasterKey(newKey); err != nil {
		return err
	}
	// No key IDs exist in the product; staying online plus later restarts
	// on the new key carry that proof instead.
	rows := rowCount(cluster.Node(idx))
	if rows < 0 {
		return fmt.Errorf("node unreadable after rotation")
	}
	curKeys[idx] = newKey
	if err := cluster.SetNodeKey(idx, newKey); err != nil {
		return err
	}
	rep.countFault("rotate")
	rep.log(fmt.Sprintf("rotate: node%d now on endurance-k%d (%d rows live)", idx+1, n, rows))
	return nil
}

// ensureAllUp restarts any node the director left down so the verdict
// window starts with a full mesh.
func ensureAllUp(t *testing.T, cluster *harness.Cluster, handles []*dbSlot, alive []atomic.Bool, rep *reporter) {
	t.Helper()
	rejoined := false
	for i := 0; i < cluster.Len(); i++ {
		if alive[i].Load() {
			continue
		}
		rep.log(fmt.Sprintf("cleanup: node%d still down at chaos end, restarting", i+1))
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("node%d cleanup restart failed: %v", i+1, err)
		}
		rejoined = true
	}
	if rejoined {
		if err := cluster.Mesh(); err != nil {
			t.Fatalf("re-mesh after cleanup restarts: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if !alive[i].Load() {
			handles[i].set(cluster.Node(i))
			alive[i].Store(true)
		}
	}
}

// waitAgreement polls until every node holds want rows with identical
// contents.
func waitAgreement(cluster *harness.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		var first []string
		for i := 0; i < cluster.Len(); i++ {
			rows := canonicalRows(cluster.Node(i))
			if rows == nil || len(rows) != want {
				ok = false
				break
			}
			if first == nil {
				first = rows
			} else if !slices.Equal(first, rows) {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("row agreement on %d rows not reached within %v", want, timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitConverged polls until every node is reachable with equal row counts
// and identical digests. Counts are compared to each other, not to a fixed
// target, because deletes move the total during chaos.
func waitConverged(cluster *harness.Cluster, timeout time.Duration) (counts []int64, digests []string, err error) {
	nodes := cluster.Len()
	deadline := time.Now().Add(timeout)
	start := time.Now()
	lastSample := time.Now().Add(-time.Hour)
	for {
		counts = make([]int64, nodes)
		digests = make([]string, nodes)
		var first []string
		agree := 0
		ok := true
		for i := 0; i < nodes; i++ {
			rows := canonicalRows(cluster.Node(i))
			if rows == nil {
				ok = false
				break
			}
			counts[i] = int64(len(rows))
			digests[i] = digestHex(rows)
			if first == nil {
				agree = 1
				first = rows
			} else if !slices.Equal(first, rows) {
				ok = false
			} else {
				agree++
			}
		}
		if ok {
			return counts, digests, nil
		}
		if !time.Now().Before(deadline) {
			break
		}
		if time.Since(lastSample) >= 15*time.Second {
			lastSample = time.Now()
			lo, hi := counts[0], counts[0]
			for _, c := range counts[1:] {
				lo = min(lo, c)
				hi = max(hi, c)
			}
			fmt.Fprintf(os.Stderr, "[verdict-sample T+%ds] counts=%v spread=%d agree=%d/%d\n",
				int(time.Since(start).Seconds()), counts, hi-lo, agree, nodes)
		}
		time.Sleep(verdictPoll)
	}
	// Final best-effort re-sample for the failure report.
	counts = make([]int64, nodes)
	digests = make([]string, nodes)
	snapshots := make([][]string, nodes)
	for i := 0; i < nodes; i++ {
		rows := canonicalRows(cluster.Node(i))
		if rows == nil {
			counts[i] = -1
			digests[i] = "unreachable"
			snapshots[i] = []string{}
			continue
		}
		counts[i] = int64(len(rows))
		digests[i] = digestHex(rows)
		snapshots[i] = rows
	}
	// Diagnostic sample: first few rows where node0 and the first
	// disagreeing node differ.
	for j := 1; j < nodes; j++ {
		if slices.Equal(snapshots[j], snapshots[0]) {
			continue
		}
		inA := make(map[string]bool)
		for _, s := range snapshots[0] {
			inA[s] = true
		}
		inB := make(map[string]bool)
		for _, s := range snapshots[j] {
			inB[s] = true
		}
		var onlyA, onlyB []string
		for _, s := range snapshots[0] {
			if !inB[s] && len(onlyA) < 5 {
				onlyA = append(onlyA, s)
			}
		}
		for _, s := range snapshots[j] {
			if !inA[s] && len(onlyB) < 5 {
				onlyB = append(onlyB, s)
			}
		}
		onlyACount, onlyBCount := 0, 0
		for _, s := range snapshots[0] {
			if !inB[s] {
				onlyACount++
			}
		}
		for _, s := range snapshots[j] {
			if !inA[s] {
				onlyBCount++
			}
		}
		fmt.Fprintf(os.Stderr, "[verdict-diff] node1-only(%d) sample=%q\n", onlyACount, onlyA)
		fmt.Fprintf(os.Stderr, "[verdict-diff] node%d-only(%d) sample=%q\n", j+1, onlyBCount, onlyB)
		break
	}
	return counts, digests, fmt.Errorf("nodes did not converge within %v", timeout)
}

// meshConnected reports whether every node has a live replication session
// to every other node.
func meshConnected(cluster *harness.Cluster) bool {
	for i := 0; i < cluster.Len(); i++ {
		h := cluster.Node(i).Replication()
		if h == nil {
			return false
		}
		status := h.Status()
		for j := 0; j < cluster.Len(); j++ {
			if i == j {
				continue
			}
			want := cluster.NodeID(j)
			connected := false
			for _, d := range status.PeersDiagnostics {
				if d.NodeID == want && d.Connected {
					connected = true
					break
				}
			}
			if !connected {
				return false
			}
		}
	}
	return true
}

func TestEnduranceChaos(t *testing.T) {
	cfg := loadConfig(t)
	// Deterministic distinct node ids so snapshot trust can be wired up
	// front (the engine only requests snapshots from explicitly trusted
	// sources).
	nodeIDs := make([][16]byte, cfg.nodes)
	for i := range nodeIDs {
		for j := range nodeIDs[i] {
			nodeIDs[i][j] = 0xC3
		}
		nodeIDs[i][0] = byte(i)
		nodeIDs[i][1] = byte(i)*131 + 7
	}
	trustedSnapshotSources := make(map[int]map[[16]byte]bool)
	for i := 0; i < cfg.nodes; i++ {
		set := make(map[[16]byte]bool)
		for _, id := range nodeIDs {
			set[id] = true
		}
		trustedSnapshotSources[i] = set
	}
	initialKey := [32]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	retention := time.Second
	retentionInterval := time.Second
	minimumRetained := uint64(10)
	maximumOffline := 20 * time.Second
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "endurance-chaos",
		NumNodes: cfg.nodes,
		NodeIDs:  nodeIDs,
		Key:      initialKey,
		// Aggressive retention keeps log GC running under the sustained
		// workload and lets bounded offline windows force real snapshot
		// resyncs instead of log catch-up.
		Replication: &harness.ReplicationOptions{
			Retention:         &retention,
			RetentionInterval: &retentionInterval,
			MinimumRetained:   &minimumRetained,
			MaximumOffline:    &maximumOffline,
		},
		TrustedSnapshotSources: trustedSnapshotSources,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("endurance-verdict")
		}
		cluster.Finish(failed)
	}()

	rep := newReporter()
	rep.log(fmt.Sprintf("config: nodes=%d duration=%s settle=%s writeInterval=%s maxRows=%d seed=%d",
		cfg.nodes, fmtDur(cfg.duration), fmtDur(cfg.settle), fmtDur(cfg.writeInterval), cfg.maxRows, cfg.seed))
	rep.log(fmt.Sprintf("fault cadence: restart=%s rotate=%s snapshot=%s/%s impair=%s/%s disk=%s/%s skew=%s (degraded)",
		fmtDur(cfg.restartEvery), fmtDur(cfg.rotateEvery), fmtDur(cfg.snapshotEvery), fmtDur(cfg.snapshotOff),
		fmtDur(cfg.impairOn), fmtDur(cfg.impairOff), fmtDur(cfg.diskEvery), fmtDur(cfg.diskHold), fmtDur(cfg.skewDur)))

	// Same schema on every node.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)"); err != nil {
			t.Fatalf("migrate contention table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("contention", true); err != nil {
			t.Fatalf("opt-in contention: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Baseline: one row everywhere before chaos starts.
	if _, err := cluster.Node(0).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
			fmt.Sprintf("%032x", 0), "baseline", "baseline", int64(0),
		)
		return nil, err
	}); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	if err := waitAgreement(cluster, 1, baselineTimeout); err != nil {
		t.Fatalf("baseline replication: %v", err)
	}
	rep.log(fmt.Sprintf("baseline row converged on all %d nodes", cfg.nodes))

	// Preflights: detect degraded modes before the long run.
	rep.log(fmt.Sprintf("network impairment: logical peer flaps (tc/netem latency=%dms loss=%d%% has no in-process QUIC equivalent)",
		cfg.impairLatencyMs, cfg.impairLossPct))
	rep.log("clock skew: DEGRADED away (in-process nodes share the test driver clock; no daemon for libfaketime)")

	fillerMB := cfg.diskMB
	if free, ok := dirFreeBytes(cluster.RuntimeDir()); ok {
		// Never risk the host filesystem: filler must leave 1 GiB free.
		if free < diskHeadroomBytes || free-diskHeadroomBytes < fillerMB<<20 {
			headroom := uint64(0)
			if free > diskHeadroomBytes {
				headroom = (free - diskHeadroomBytes) >> 20
			}
			if headroom < diskMinMB {
				fillerMB = 0
				rep.log(fmt.Sprintf("disk pressure: DEGRADED away (only %d MiB free)", free>>20))
			} else {
				fillerMB = headroom
				rep.log(fmt.Sprintf("disk pressure: filler reduced to %d MiB (%d MiB free)", fillerMB, free>>20))
			}
		} else {
			rep.log(fmt.Sprintf("disk pressure: %d MiB filler (%d MiB free)", fillerMB, free>>20))
		}
	} else {
		rep.log(fmt.Sprintf("disk pressure: %d MiB filler (free-space probe unavailable)", fillerMB))
	}

	// Shared chaos state.
	curKeys := make([][32]byte, cfg.nodes)
	for i := range curKeys {
		curKeys[i] = initialKey
	}
	registry := newIDRegistry()
	gcHist := newGCHistory(cfg.nodes)
	var snapProofs []snapObservation
	var snapMu sync.Mutex
	var stop atomic.Bool

	// Writer threads share a handle table with the director: the entry for
	// a restarted node is swapped for the reopened database, and an
	// alive-set steers targeting away from stopped nodes.
	handles := make([]*dbSlot, cfg.nodes)
	for i := range handles {
		handles[i] = &dbSlot{db: cluster.Node(i)}
	}
	alive := make([]atomic.Bool, cfg.nodes)
	for i := range alive {
		alive[i].Store(true)
	}
	nodeDirs := make([]string, cfg.nodes)
	for i := range nodeDirs {
		nodeDirs[i] = cluster.NodeInfo(i).Dir
	}

	numWriters := min(max(cfg.nodes, 2), maxWriters)
	var wg sync.WaitGroup
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			writerLoop(w, handles, alive, registry, rep, cfg.seed, cfg.writeInterval, cfg.maxRows, &stop)
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		statusLoop(handles, nodeDirs, rep, gcHist, cfg.nodes, cfg.statusEvery, &stop)
	}()
	diskRunning := fillerMB > 0
	if diskRunning {
		wg.Add(1)
		go func() {
			defer wg.Done()
			diskLoop(nodeDirs, rep, cfg.diskEvery, cfg.diskHold, fillerMB, &stop)
		}()
	}

	// Chaos director (test goroutine: owns all node lifecycle).
	rep.phase("chaos", "all fault injectors live for "+fmtDur(cfg.duration))
	directive := runDirector(t, cluster, handles, alive, rep, cfg, curKeys, &snapProofs, &snapMu)

	// Faults stop; every node must be up before the verdict window.
	stop.Store(true)
	wg.Wait()
	ensureAllUp(t, cluster, handles, alive, rep)
	rep.log(fmt.Sprintf("writers stopping: attempted=%d acked=%d transient-errors=%d live-ids=%d",
		rep.attempted.Load(), rep.succeeded.Load(), rep.failed.Load(), registry.len()))

	reportPath := filepath.Join(cluster.RuntimeDir(), fmt.Sprintf("endurance-report-%d.md", time.Now().Unix()))
	if rep.succeeded.Load() == 0 {
		rep.verdict(false, reportPath, "no writes acked during the entire run")
		t.Fatalf("endurance verdict: 0 of %d writes acked; see report for distinct errors", rep.attempted.Load())
	}

	rep.phase("verdict", "waiting up to "+fmtDur(cfg.settle)+" for exact convergence")
	counts, digests, convErr := waitConverged(cluster, cfg.settle)
	if convErr != nil {
		rep.verdict(false, reportPath, convErr.Error())
		t.Fatalf("endurance verdict: %v\ncounts=%v\ndigests=%v", convErr, counts, digests)
	}
	rep.log(fmt.Sprintf("converged: all %d nodes hold %d rows with digest %s", cfg.nodes, counts[0], digests[0]))

	// Counter-proven post-checks: every claimed fault must have fired.
	// GC history (not base/after snapshots): in-memory runs reset on every
	// restart, so a node revived seconds before the verdict would falsely
	// fail a snapshot comparison. The sampler observed throughout the run.
	for i := 0; i < cfg.nodes; i++ {
		ever, collEver, gcErr, hasErr := gcHist.snapshot(i)
		if !ever {
			rep.verdict(false, reportPath, fmt.Sprintf("node%d GC never ran", i+1))
			t.Fatalf("node%d GC never observed running during chaos", i+1)
		}
		if !collEver {
			rep.verdict(false, reportPath, fmt.Sprintf("node%d GC collected nothing", i+1))
			t.Fatalf("node%d GC never observed collecting during chaos", i+1)
		}
		if hasErr {
			rep.verdict(false, reportPath, fmt.Sprintf("node%d GC failures observed", i+1))
			t.Fatalf("node%d GC failure observed during chaos: %s", i+1, gcErr)
		}
	}
	rep.log(fmt.Sprintf("GC proven on all %d nodes (runs/collect observed, GC failures flat)", cfg.nodes))

	faults := rep.faultSnapshot()
	if faults.restarts == 0 {
		rep.verdict(false, reportPath, "zero restarts executed")
		t.Fatal("chaos ran zero restarts; restart injector never fired")
	}
	if faults.rotations == 0 {
		rep.verdict(false, reportPath, "zero key rotations executed")
		t.Fatal("chaos ran zero key rotations; rotation injector never fired")
	}
	if faults.impairCycles == 0 {
		rep.verdict(false, reportPath, "zero network-impairment cycles executed")
		t.Fatal("chaos ran zero network-impairment cycles")
	}
	if fillerMB > 0 && faults.diskCycles == 0 {
		rep.verdict(false, reportPath, "zero disk-pressure cycles executed")
		t.Fatal("chaos ran zero disk-pressure cycles")
	}
	if directive.snapshotsPossible {
		if faults.snapshots == 0 {
			rep.verdict(false, reportPath, "zero snapshot offline windows executed")
			t.Fatal("chaos ran zero snapshot offline windows")
		}
		proven := 0
		for _, o := range snapProofs {
			if o.proven {
				proven++
			}
		}
		if proven < 1 {
			rep.verdict(false, reportPath, "no snapshot resync observed")
			t.Fatal("offline windows ran but no resync proved the snapshot path (log catch-up hid the path?)")
		}
		rep.log(fmt.Sprintf("snapshots proven: %d resyncs over %d offline windows", proven, len(snapProofs)))
	} else {
		rep.log("snapshots: leg skipped (run too short for an offline window)")
	}
	if directive.skewScheduled && faults.skews == 0 {
		rep.verdict(false, reportPath, "skew scheduled but never executed")
		t.Fatal("clock skew was scheduled but the skew window never ran")
	}

	// Full mesh must be back: every node sees every peer.
	meshed := false
	meshDeadline := time.Now().Add(meshTimeout)
	for {
		if meshConnected(cluster) {
			meshed = true
			break
		}
		if !time.Now().Before(meshDeadline) {
			break
		}
		time.Sleep(verdictPoll)
	}
	if !meshed {
		var detail []string
		for i := 0; i < cfg.nodes; i++ {
			n := 0
			if h := cluster.Node(i).Replication(); h != nil {
				for _, d := range h.Status().PeersDiagnostics {
					if d.Connected {
						n++
					}
				}
			}
			detail = append(detail, fmt.Sprintf("node%d:%d", i+1, n))
		}
		rep.verdict(false, reportPath, "mesh not fully reconnected")
		t.Fatalf("mesh not fully reconnected (connected peers per node: %s), want %d",
			strings.Join(detail, " "), cfg.nodes-1)
	}

	// A post-chaos write must traverse the healed mesh.
	if _, err := cluster.Node(0).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
			fmt.Sprintf("%032x", 999_999_999), "post-chaos", "post-chaos", int64(0),
		)
		return nil, err
	}); err != nil {
		rep.verdict(false, reportPath, "post-chaos write rejected")
		t.Fatalf("post-chaos write: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush post-chaos write: %v", err)
	}
	if err := waitAgreement(cluster, int(counts[0])+1, cfg.settle); err != nil {
		rep.verdict(false, reportPath, "post-chaos write did not converge")
		t.Fatalf("post-chaos convergence: %v", err)
	}

	rep.verdict(true, reportPath, fmt.Sprintf(
		"all %d nodes hold %d rows digest %s after %s of combined chaos (restarts=%d rotations=%d impair=flap/%d disk=%d snapshots=%d skews=%d)",
		cfg.nodes, counts[0]+1, digests[0], fmtDur(cfg.duration),
		faults.restarts, faults.rotations, faults.impairCycles, faults.diskCycles, faults.snapshots, faults.skews))
	failed = false
}
