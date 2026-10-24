// Live scenario: a live multi-node mesh absorbs continuous writes while
// rolling restarts, peer flaps, disk pressure, online key rotation,
// snapshot resyncs, and origin-log GC overlap for a configured chaos
// window.
//
// Go port of tests/endurance_chaos.rs using tests-live/goharness.
// Identical behavior: the verdict is exact cross-node convergence (equal
// row counts plus identical PK-ordered row contents) with counter-proven
// GC progress on every node, at least one proven snapshot resync,
// successful key rotations, a rejoined full mesh, and a post-chaos
// write. Plain SQL replaces the typed-contention API; tc/netem
// impairment degrades to logical peer flaps and libfaketime clock skew
// is skipped. Tokio tasks become goroutines sharing the cluster through
// one mutex; every cluster touch locks, so lifecycle (stop/start) and
// reads never race.
//
// Deviation: the Rust run_body returns early on a director error without
// joining its tasks (the teardown then fails to unwrap the cluster and
// the test passes vacuously). This port always stops and joins every
// task before returning, so a director error fails loudly with
// forensics and retained artifacts.
package endurancechaos

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// shared is the shared cluster handle: every task locks for each
// cluster touch (Rust: Arc<tokio::sync::Mutex<Cluster>>).
type shared struct {
	mu sync.Mutex
	c  *goharness.Cluster
}

const (
	// minNodes/maxNodes is the mesh-size clamp.
	minNodes = 3
	maxNodes = 16
	// maxWriters is the writer-task cap.
	maxWriters = 8
	// minMaxRows is the bounded working-set floor.
	minMaxRows = 100
	// minSnapshotOff is the snapshot offline-window floor (60 s:
	// retention expiry + GC tick).
	minSnapshotOff = 60 * time.Second
	// baselineTimeout is the baseline replication deadline (2 minutes).
	baselineTimeout = 120 * time.Second
	// directorTick is the director tick (1 s).
	directorTick = 1 * time.Second
	// impairWarmup is the first impairment delay (15 s).
	impairWarmup = 15 * time.Second
	// diskWarmup is the disk-loop stagger (30 s).
	diskWarmup = 30 * time.Second
	// verdictPoll is the convergence poll interval (2 s).
	verdictPoll = 2 * time.Second
	// meshTimeout is the full-mesh wait bound (60 s).
	meshTimeout = 60 * time.Second
	// snapProofTimeout is the snapshot catch-up observation bound
	// (240 s: two full snapshot cycles fit instead of sampling once).
	snapProofTimeout = 240 * time.Second
	// snapProofPoll is the snapshot proof poll interval (2 s).
	snapProofPoll = 2 * time.Second
	// errSampleCap caps distinct error-message samples (first 8).
	errSampleCap = 8
	// errTruncChars truncates stored error messages (200 chars).
	errTruncChars = 200
	// diskHeadroomBytes is the headroom the disk filler must leave free
	// (1 GiB).
	diskHeadroomBytes uint64 = 1 << 30
	// diskMinMB is the minimum filler size before the disk leg degrades
	// away (16 MiB).
	diskMinMB uint64 = 16
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

// fmtDur renders a Go-style duration (90s -> 1m30s) for progress lines.
func fmtDur(d time.Duration) string {
	s := int64(d / time.Second)
	if s >= 3600 {
		return fmt.Sprintf("%dh%dm%ds", s/3600, s%3600/60, s%60)
	} else if s >= 60 {
		return fmt.Sprintf("%dm%ds", s/60, s%60)
	} else if s >= 1 {
		return fmt.Sprintf("%ds", s)
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}

// unixNow returns the current unix time in seconds (report stamp).
func unixNow() uint64 {
	if s := time.Now().Unix(); s > 0 {
		return uint64(s)
	}
	return 0
}

// rng is a deterministic splitmix64.
type rng struct {
	state uint64
}

func newRng(seed uint64) *rng {
	return &rng{state: seed}
}

func (r *rng) next() uint64 {
	r.state = r.state + 0x9E3779B97F4A7C15
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

// config is the chaos configuration (same env names/defaults as Rust).
type config struct {
	nodes         int
	duration      time.Duration
	settle        time.Duration
	writeInterval time.Duration
	maxRows       int
	seed          uint64
	statusEvery   time.Duration
	restartEvery  time.Duration
	restartDown   time.Duration
	rotateEvery   time.Duration
	snapshotEvery time.Duration
	snapshotOff   time.Duration
	impairOn      time.Duration
	impairOff     time.Duration
	impairLatency uint64
	impairLossPct uint64
	diskEvery     time.Duration
	diskMB        uint64
	diskHold      time.Duration
	skewDur       time.Duration
}

func loadConfig(t *testing.T) *config {
	t.Helper()
	cfg := &config{
		nodes:         min(max(envUsize("MURMUR_ENDURANCE_NODES", 5), minNodes), maxNodes),
		duration:      goharness.EnvSeconds("MURMUR_ENDURANCE_DURATION_SECONDS", 600),
		settle:        goharness.EnvSeconds("MURMUR_ENDURANCE_SETTLE_SECONDS", 300),
		writeInterval: envMillis("MURMUR_ENDURANCE_WRITE_INTERVAL_MS", 25),
		maxRows:       envUsize("MURMUR_ENDURANCE_MAX_ROWS", 10_000),
		seed:          envU64("MURMUR_ENDURANCE_SEED", 1),
		statusEvery:   goharness.EnvSeconds("MURMUR_ENDURANCE_STATUS_SECONDS", 30),
		restartEvery:  goharness.EnvSeconds("MURMUR_ENDURANCE_RESTART_SECONDS", 90),
		restartDown:   goharness.EnvSeconds("MURMUR_ENDURANCE_RESTART_DOWN_SECONDS", 8),
		rotateEvery:   goharness.EnvSeconds("MURMUR_ENDURANCE_ROTATE_SECONDS", 180),
		snapshotEvery: goharness.EnvSeconds("MURMUR_ENDURANCE_SNAPSHOT_SECONDS", 300),
		snapshotOff:   goharness.EnvSeconds("MURMUR_ENDURANCE_SNAPSHOT_OFFLINE_SECONDS", 75),
		impairOn:      goharness.EnvSeconds("MURMUR_ENDURANCE_IMPAIR_ON_SECONDS", 60),
		impairOff:     goharness.EnvSeconds("MURMUR_ENDURANCE_IMPAIR_OFF_SECONDS", 20),
		impairLatency: envU64("MURMUR_ENDURANCE_IMPAIR_LATENCY_MS", 150),
		impairLossPct: envU64("MURMUR_ENDURANCE_IMPAIR_LOSS_PCT", 3),
		diskEvery:     goharness.EnvSeconds("MURMUR_ENDURANCE_DISK_SECONDS", 180),
		diskMB:        envU64("MURMUR_ENDURANCE_DISK_MB", 128),
		diskHold:      goharness.EnvSeconds("MURMUR_ENDURANCE_DISK_HOLD_SECONDS", 60),
		skewDur:       goharness.EnvSeconds("MURMUR_ENDURANCE_SKEW_SECONDS", 120),
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

// reporter is the live progress reporter: timestamped phase/log lines
// to stderr plus the same events for the final markdown report. All
// methods are safe for concurrent use.
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
	return &reporter{start: time.Now(), errSeen: map[string]int{}}
}

func (r *reporter) stamp() string {
	return fmt.Sprintf("[T+%s]", fmtDur(time.Since(r.start)))
}

func (r *reporter) emit(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, msg)
}

// phase announces a phase transition.
func (r *reporter) phase(name, detail string) {
	r.emit(fmt.Sprintf("%s === PHASE: %s === %s", r.stamp(), name, detail))
}

// log records a timestamped progress line.
func (r *reporter) log(msg string) {
	r.emit(fmt.Sprintf("%s %s", r.stamp(), msg))
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
	msg := string([]rune(err)[:min(len([]rune(err)), errTruncChars)])
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errSeen[msg]++
	found := false
	for _, s := range r.errSample {
		if s == msg {
			found = true
			break
		}
	}
	if len(r.errSample) < errSampleCap && !found {
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
	events := append([]string(nil), r.events...)
	errSample := append([]string(nil), r.errSample...)
	errSeen := make(map[string]int, len(r.errSeen))
	for k, v := range r.errSeen {
		errSeen[k] = v
	}
	f := r.faults
	r.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "# Endurance-chaos run report — %s\n\n", status)
	fmt.Fprintf(&b, "- finished (unix): %d\n- wall time: %s\n- writes attempted: %d\n- writes acked: %d\n- writes failed (transient): %d\n- faults: restarts=%d rotations=%d impairCycles=%d diskCycles=%d snapshots=%d skews=%d\n- verdict: %s\n\n",
		unixNow(), fmtDur(time.Since(r.start)), r.attempted.Load(), r.succeeded.Load(), r.failed.Load(),
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

// idRegistry is the bounded working-set registry: every appended id
// plus the live subset; deletes remove from live only.
type idRegistry struct {
	mu   sync.Mutex
	ids  []string
	live map[string]bool
}

func newIDRegistry() *idRegistry {
	return &idRegistry{live: map[string]bool{}}
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

// pick returns a random live id, tolerating entries deleted since
// append.
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

// gcHist is restart-proof per-node GC history.
type gcHist struct {
	first *uint64
	max   uint64
	gcErr *string
}

// gcHistory records high-water GC observations; the verdict asserts on
// those. No GC-runs counter exists over the wire, so a run is proven by
// collected advancing across samples (durable, restart-proof).
type gcHistory struct {
	mu    sync.Mutex
	hists []gcHist
}

func newGCHistory(nodes int) *gcHistory {
	return &gcHistory{hists: make([]gcHist, nodes)}
}

func (g *gcHistory) observe(idx int, collected uint64, lastError *string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	h := &g.hists[idx]
	if h.first == nil {
		c := collected
		h.first = &c
	}
	if collected > h.max {
		h.max = collected
	}
	if lastError != nil && h.gcErr == nil && isGCError(*lastError) {
		runes := []rune(*lastError)
		e := string(runes[:min(len(runes), errTruncChars)])
		h.gcErr = &e
	}
}

// snapshot returns (runs observed, collected observed, first GC-shaped
// error).
func (g *gcHistory) snapshot(idx int) (bool, bool, *string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	h := &g.hists[idx]
	ever := h.first != nil && h.max > *h.first
	return ever, h.max > 0, h.gcErr
}

// isGCError reports whether an engine error string plausibly comes from
// log-GC collection. The engine multiplexes every subsystem error into
// one last_error latch (peer disconnects during restarts/flaps latch
// here too), so only GC-substring matches fail the verdict.
func isGCError(msg string) bool {
	lower := strings.ToLower(msg)
	return strings.Contains(lower, "retention") || strings.Contains(lower, "collect") ||
		strings.Contains(lower, "gc_") || strings.Contains(lower, "prune")
}

// gcSample returns GC counters for one node (collected transactions
// plus the latest engine error), or false when the node is unreachable
// or the mesh has no retention row yet.
func gcSample(cluster *shared, idx int) (uint64, *string, bool) {
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	if _, ok := cluster.c.NodePID(idx); !ok {
		return 0, nil, false
	}
	repl, err := cluster.c.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil || len(repl) == 0 {
		return 0, nil, false
	}
	if len(repl[0]) < 8 {
		return 0, nil, false
	}
	var lastError *string
	if repl[0][6] != "" {
		e := repl[0][6]
		lastError = &e
	}
	rows, err := cluster.c.Node(idx).Simple("MURMUR SHOW RETENTION")
	if err != nil || len(rows) == 0 {
		return 0, nil, false
	}
	if len(rows[0]) < 6 {
		return 0, nil, false
	}
	collected, err := strconv.ParseUint(rows[0][5], 10, 64)
	if err != nil {
		return 0, nil, false
	}
	return collected, lastError, true
}

// appliedCount returns the log-path applied-transaction counter for one
// node (snapshot application bypasses it, so it underpins the snapshot
// proof).
func appliedCount(cluster *shared, idx int) (uint64, bool) {
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	if _, ok := cluster.c.NodePID(idx); !ok {
		return 0, false
	}
	rows, err := cluster.c.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil || len(rows) == 0 || len(rows[0]) <= 5 {
		return 0, false
	}
	n, err := strconv.ParseUint(rows[0][5], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// spoolDiskBytes returns spool on-disk bytes for one node.
func spoolDiskBytes(cluster *shared, idx int) (uint64, bool) {
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	if _, ok := cluster.c.NodePID(idx); !ok {
		return 0, false
	}
	rows, err := cluster.c.Node(idx).Simple("MURMUR SHOW METRICS SPOOL")
	if err != nil {
		return 0, false
	}
	for _, r := range rows {
		if len(r) == 3 && r[0] == "spool" && r[1] == "disk_bytes" {
			n, err := strconv.ParseUint(r[2], 10, 64)
			if err != nil {
				return 0, false
			}
			return n, true
		}
	}
	return 0, false
}

// rowCount returns the row count on one node, or false when unreadable.
func rowCount(cluster *shared, idx int) (int, bool) {
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	if _, ok := cluster.c.NodePID(idx); !ok {
		return 0, false
	}
	rows, err := cluster.c.Node(idx).Query("SELECT COUNT(*) FROM contention")
	if err != nil || len(rows) == 0 {
		return 0, false
	}
	n, err := rows[0].Int64(0)
	if err != nil {
		return 0, false
	}
	return int(n), true
}

// canonicalRows returns canonical PK-ordered encoded rows on one node
// (id:name:phone:score sorted by id), or false when unreadable.
func canonicalRows(cluster *shared, idx int) ([]string, bool) {
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	if _, ok := cluster.c.NodePID(idx); !ok {
		return nil, false
	}
	rows, err := cluster.c.Node(idx).Query("SELECT id, name, phone, score FROM contention ORDER BY id")
	if err != nil {
		return nil, false
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		id, err := row.String(0)
		if err != nil {
			return nil, false
		}
		name, err := row.String(1)
		if err != nil {
			return nil, false
		}
		phone, err := row.String(2)
		if err != nil {
			return nil, false
		}
		score, err := row.Int64(3)
		if err != nil {
			return nil, false
		}
		out = append(out, fmt.Sprintf("%s:%s:%s:%d", id, name, phone, score))
	}
	return out, true
}

// connectedPeers returns the connected-peer count on one node, or false
// when the node is down or unreadable.
func connectedPeers(cluster *shared, idx int) (int, bool) {
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	if _, ok := cluster.c.NodePID(idx); !ok {
		return 0, false
	}
	rows, err := cluster.c.Node(idx).Simple("MURMUR SHOW PEERS")
	if err != nil {
		return 0, false
	}
	n := 0
	for _, r := range rows {
		if len(r) >= 3 && r[2] == "true" {
			n++
		}
	}
	return n, true
}

// digestHex is a deterministic log-only digest over canonical rows
// (comparisons use byte-identical vectors, which imply identical
// digests). FNV-1a keeps this dependency-free.
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

// rowDiff returns the size of the symmetric difference between two
// canonical row sets.
func rowDiff(a, b []string) int {
	sa := make(map[string]int, len(a))
	for _, s := range a {
		sa[s]++
	}
	diff := 0
	for _, s := range b {
		if sa[s] > 0 {
			sa[s]--
		} else {
			diff++
		}
	}
	for _, n := range sa {
		diff += n
	}
	return diff
}

// pickAlive picks a random live node, or false when all are down.
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

// writeOne runs one bounded insert/update/delete write on a live node:
// inserts until maxRows live ids, then updates/deletes recycle the set
// so a long run keeps churning replication and GC without unbounded
// growth. Transient errors are counted, never fatal. The alive re-check
// and the write run under one cluster lock, so a node the director
// stops mid-op is never touched.
func writeOne(id int, cluster *shared, alive []atomic.Bool, registry *idRegistry, rep *reporter, rng *rng, seq *uint64, maxRows, target int) {
	roll := rng.float()
	if registry.len() < maxRows && roll < 0.7 {
		*seq++
		rowID := fmt.Sprintf("%08x%024x", id, *seq)
		name := fmt.Sprintf("writer-%d", id)
		phone := fmt.Sprintf("v-%d-%d", id, *seq)
		insertID := rowID
		cluster.mu.Lock()
		if !alive[target].Load() {
			cluster.mu.Unlock()
			return
		}
		_, err := cluster.c.Node(target).Execute(
			"INSERT INTO contention(id, name, phone, score) VALUES($1, $2, $3, $4)",
			rowID, name, phone, int64(0))
		if err == nil {
			_, err = cluster.c.Node(target).Simple("MURMUR FLUSH")
		}
		cluster.mu.Unlock()
		if err != nil {
			rep.countAttempt(false)
			rep.sampleErr(err.Error())
		} else {
			registry.add(insertID)
			rep.countAttempt(true)
		}
	} else if roll < 0.85 {
		rowID, ok := registry.pick(rng)
		if !ok {
			return
		}
		*seq++
		name := fmt.Sprintf("writer-%d", id)
		phone := fmt.Sprintf("u-%d-%d", id, *seq)
		cluster.mu.Lock()
		if !alive[target].Load() {
			cluster.mu.Unlock()
			return
		}
		changed, err := cluster.c.Node(target).Execute(
			"UPDATE contention SET name = $1, phone = $2 WHERE id = $3", name, phone, rowID)
		if err == nil {
			if changed != 1 {
				err = fmt.Errorf("update hit %d rows", changed)
			} else {
				_, err = cluster.c.Node(target).Simple("MURMUR FLUSH")
			}
		}
		cluster.mu.Unlock()
		if err != nil {
			rep.countAttempt(false)
			rep.sampleErr(err.Error())
		} else {
			rep.countAttempt(true)
		}
	} else {
		rowID, ok := registry.pick(rng)
		if !ok {
			return
		}
		delID := rowID
		cluster.mu.Lock()
		if !alive[target].Load() {
			cluster.mu.Unlock()
			return
		}
		_, err := cluster.c.Node(target).Execute("DELETE FROM contention WHERE id = $1", rowID)
		if err == nil {
			_, err = cluster.c.Node(target).Simple("MURMUR FLUSH")
		}
		cluster.mu.Unlock()
		if err != nil {
			rep.countAttempt(false)
			rep.sampleErr(err.Error())
		} else {
			registry.remove(delID)
			rep.countAttempt(true)
		}
	}
}

// writerLoop is a bounded writer task.
func writerLoop(id int, cluster *shared, alive []atomic.Bool, registry *idRegistry, rep *reporter, seed uint64, writeInterval time.Duration, maxRows int, stop *atomic.Bool, wg *sync.WaitGroup) {
	defer wg.Done()
	rng := newRng(seed*1000 + uint64(id))
	var seq uint64
	for {
		if stop.Load() {
			return
		}
		target, ok := pickAlive(alive, rng)
		if !ok {
			time.Sleep(writeInterval)
			continue
		}
		writeOne(id, cluster, alive, registry, rep, rng, &seq, maxRows, target)
		if stop.Load() {
			return
		}
		time.Sleep(writeInterval)
	}
}

// statusLoop is the heartbeat sampler: row spread, fault tallies, GC
// high-water observations, spool/disk pressure.
func statusLoop(cluster *shared, nodeDirs []string, rep *reporter, gcHist *gcHistory, nodes int, statusEvery time.Duration, stop *atomic.Bool, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if !sleepStop(statusEvery, stop) {
			return
		}
		minC, maxC := int64(-1), int64(-1)
		down := 0
		for i := 0; i < nodes; i++ {
			c, ok := rowCount(cluster, i)
			if !ok {
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
		var spoolMax, collectedMax uint64
		for i := 0; i < nodes; i++ {
			if b, ok := spoolDiskBytes(cluster, i); ok && b > spoolMax {
				spoolMax = b
			}
			if collected, lastErr, ok := gcSample(cluster, i); ok {
				if collected > collectedMax {
					collectedMax = collected
				}
				gcHist.observe(i, collected, lastErr)
			}
		}
		var diskMin uint64
		hasDisk := false
		for _, dir := range nodeDirs {
			if free, ok := goharness.DirFreeBytes(dir); ok {
				if !hasDisk || free < diskMin {
					diskMin = free
					hasDisk = true
				}
			}
		}
		if !hasDisk {
			diskMin = 0
		}
		f := rep.faultSnapshot()
		rep.log(fmt.Sprintf("status: writes ok=%d err=%d | rows min=%d max=%d down=%d/%d | restart=%d rotate=%d impair=%d disk=%d snap=%d skew=%d | gcCollectedMax=%d spoolMax=%dMiB diskFreeMin=%dMiB",
			rep.succeeded.Load(), rep.failed.Load(), minC, maxC, down, nodes,
			f.restarts, f.rotations, f.impairCycles, f.diskCycles, f.snapshots, f.skews,
			collectedMax, spoolMax>>20, diskMin>>20))
	}
}

// diskLoop is the disk-pressure cycler: holds a filler file in each
// node dir in turn, then releases. Filesystem paths only, no cluster
// use.
func diskLoop(nodeDirs []string, rep *reporter, diskEvery, diskHold time.Duration, fillerMB uint64, stop *atomic.Bool, wg *sync.WaitGroup) {
	defer wg.Done()
	if !sleepStop(diskWarmup, stop) {
		return
	}
	idx := 0
	for {
		dir := nodeDirs[idx%len(nodeDirs)]
		idx++
		label := filepath.Base(dir)
		if label == "" || label == "." || label == string(filepath.Separator) {
			label = "?"
		}
		path := filepath.Join(dir, "endurance-filler.bin")
		freeBefore, _ := goharness.DirFreeBytes(dir)
		if err := writeFiller(path, fillerMB); err != nil {
			rep.log(fmt.Sprintf("disk: %s filler FAILED: %v", label, err))
			removeFiller(path)
		} else {
			freeAfter, _ := goharness.DirFreeBytes(dir)
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

// sleepStop sleeps d, waking early when stop is set. It reports false
// when interrupted.
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

// writeFiller consumes mb megabytes at path with incompressible-ish
// bytes.
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
			b := [8]byte{
				byte(x), byte(x >> 8), byte(x >> 16), byte(x >> 24),
				byte(x >> 32), byte(x >> 40), byte(x >> 48), byte(x >> 56),
			}
			copy(buf[off:off+min(8, len(buf)-off)], b[:min(8, len(buf)-off)])
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

// randomKey returns a fresh 32-byte storage key.
func randomKey() [32]byte {
	var key [32]byte
	if _, err := rand.Read(key[:]); err == nil {
		return key
	}
	// Fallback when urandom is unavailable: splitmix over time + pid.
	var nanos uint64
	if n := time.Now().UnixNano(); n > 0 {
		nanos = uint64(n)
	} else {
		nanos = 0x12345678
	}
	rng := newRng(nanos ^ uint64(os.Getpid())*0x9E3779B97F4A7C15)
	for off := 0; off < len(key); off += 8 {
		v := rng.next()
		for i := 0; i < 8 && off+i < len(key); i++ {
			key[off+i] = byte(v >> (8 * i))
		}
	}
	return key
}

// snapObservation is a per-window snapshot-path observation.
type snapObservation struct {
	victim       int
	outageAcked  uint64
	appliedDelta uint64
	diffRows     int
	rows         int
	proven       bool
}

// observeSnapshotProof decides whether a rejoined snapshot victim
// caught up via the snapshot path. The wire protocol exposes no
// snapshot counter, so the proof is event-driven off the log-path
// applied_transactions counter instead: every writer op is one
// transaction, and the victim missed every acked op from its outage, so
// the window counts as proven when the post-rejoin sample shows the
// bulk arrived off the log path (row drift below the outage's acked
// count while the log-path applied delta since the rejoin stays below
// it too — a pure log catch-up would advance applied_transactions by at
// least the outage's count). Writers keep running throughout; the
// victim's own restart plus a donor build plus the chunk transfer can
// take ~45 s under chaos, and a stale donor winning the race forces a
// second cycle, so the proof polls up to snapProofTimeout instead of
// sampling once at a fixed delay.
func observeSnapshotProof(cluster *shared, victim int, rep *reporter, ackedAtStop uint64, nodes int) snapObservation {
	ackedAtRejoin := rep.succeeded.Load()
	var outageAcked uint64
	if ackedAtRejoin > ackedAtStop {
		outageAcked = ackedAtRejoin - ackedAtStop
	}
	appliedAtRejoin, _ := appliedCount(cluster, victim)
	start := time.Now()
	victimRows, _ := canonicalRows(cluster, victim)
	survivorRows, _ := canonicalRows(cluster, (victim+1)%nodes)
	obs := snapObservation{victim: victim, outageAcked: outageAcked, diffRows: rowDiff(victimRows, survivorRows)}
	for time.Since(start) < snapProofTimeout {
		time.Sleep(snapProofPoll)
		appliedNow, _ := appliedCount(cluster, victim)
		victimRows, _ := canonicalRows(cluster, victim)
		survivor := (victim + 1) % nodes
		survivorRows, _ := canonicalRows(cluster, survivor)
		obs.diffRows = rowDiff(victimRows, survivorRows)
		obs.rows = len(survivorRows)
		obs.appliedDelta = 0
		if appliedNow > appliedAtRejoin {
			obs.appliedDelta = appliedNow - appliedAtRejoin
		}
		// The snapshot covered the bulk of the outage when neither the
		// remaining drift nor the post-rejoin log tail reaches the
		// outage's acked count.
		obs.proven = outageAcked > 100 &&
			uint64(obs.diffRows) < outageAcked &&
			obs.appliedDelta < outageAcked
		if obs.proven {
			break
		}
	}
	rep.log(fmt.Sprintf("snapshot: node%d proof outageAcked=%d appliedDelta=%d diffRows=%d/%d proven=%t",
		obs.victim+1, obs.outageAcked, obs.appliedDelta, obs.diffRows, obs.rows, obs.proven))
	return obs
}

// directorReport is the director outcome (snapshot/skew scheduling
// flags).
type directorReport struct {
	snapshotsPossible bool
	skewScheduled     bool
}

type impairFlap struct {
	a, b      int
	restoreAt time.Time
}

func runDirector(cluster *shared, alive []atomic.Bool, rep *reporter, cfg *config, curKeys [][32]byte, snapProofs *[]snapObservation) (directorReport, error) {
	var zero directorReport
	rng := newRng(cfg.seed*7919 + 17)
	start := time.Now()
	deadline := start.Add(cfg.duration)

	nextRestart := start.Add(cfg.restartEvery / 2)
	nextRotate := start.Add(cfg.rotateEvery / 2)
	rotateIdx := 0

	// Snapshot windows need a full offline window plus reconvergence
	// room before the verdict; short runs skip the leg loudly.
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
	var snapRestartAt time.Time
	var snapAckedAtStop uint64

	// Clock skew is degraded away: server children share the host clock,
	// so no node can run under libfaketime.
	skewScheduled := false

	// Impairment phases cycle latency -> loss -> both, all implemented
	// as logical edge flaps (no tc privilege model for child processes).
	phases := []string{"latency", "loss", "latency+loss"}
	phase := 0
	nextImpair := start.Add(impairWarmup)
	var impairActive *impairFlap

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
		if impairActive != nil && !now.Before(impairActive.restoreAt) {
			a, b := impairActive.a, impairActive.b
			cluster.mu.Lock()
			_ = cluster.c.AddPeer(a, b)
			_ = cluster.c.AddPeer(b, a)
			cluster.mu.Unlock()
			impairActive = nil
			rep.countFault("impair")
			rep.log(fmt.Sprintf("impair: edge node%d <-> node%d restored", a+1, b+1))
			nextImpair = now.Add(cfg.impairOff)
		}
		// Open the next impairment flap.
		if impairActive == nil && !now.Before(nextImpair) {
			a := rng.below(cfg.nodes)
			b := (a + 1 + rng.below(cfg.nodes-1)) % cfg.nodes
			cluster.mu.Lock()
			_ = cluster.c.RemovePeer(a, b)
			_ = cluster.c.RemovePeer(b, a)
			cluster.mu.Unlock()
			rep.log(fmt.Sprintf("impair: flap %s (logical) node%d <-> node%d for %s (no tc privilege model for child processes)",
				phases[phase%len(phases)], a+1, b+1, fmtDur(cfg.impairOn)))
			phase++
			impairActive = &impairFlap{a: a, b: b, restoreAt: now.Add(cfg.impairOn)}
		}

		if !now.Before(nextRestart) {
			// Never restart the offline snapshot victim out from
			// under its window.
			idx := rng.below(cfg.nodes)
			for range 16 {
				if idx != snapVictim {
					break
				}
				idx = rng.below(cfg.nodes)
			}
			// Alternate graceful and crash restarts: the harness
			// kills real child processes either way.
			crash := rng.below(2) == 0
			how := "graceful"
			if crash {
				how = "crash"
			}
			rep.log(fmt.Sprintf("restart: node%d going down (%s downtime, %s)", idx+1, fmtDur(cfg.restartDown), how))
			cluster.mu.Lock()
			alive[idx].Store(false)
			if crash {
				cluster.c.KillNode(idx)
			} else {
				cluster.c.StopNode(idx)
			}
			cluster.mu.Unlock()
			time.Sleep(cfg.restartDown)
			cluster.mu.Lock()
			startErr := cluster.c.StartNode(idx)
			var meshErr error
			if startErr == nil {
				meshErr = cluster.c.Mesh()
			}
			if startErr == nil && meshErr == nil {
				alive[idx].Store(true)
			}
			cluster.mu.Unlock()
			if startErr != nil {
				return zero, fmt.Errorf("node%d restart failed: %w", idx+1, startErr)
			}
			if meshErr != nil {
				return zero, fmt.Errorf("re-mesh after restart: %w", meshErr)
			}
			rep.countFault("restart")
			rep.log(fmt.Sprintf("restart: node%d back", idx+1))
			halfMs := max(1, int(cfg.restartEvery.Milliseconds()/2))
			jitter := time.Duration(rng.below(halfMs)) * time.Millisecond
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
			} else if err := rotateNode(cluster, curKeys, idx, rep); err != nil {
				rep.log(fmt.Sprintf("rotate: node%d FAILED: %v (counted, chaos continues)", idx+1, err))
			}
			nextRotate = time.Now().Add(cfg.rotateEvery)
		}

		if snapOK && snapVictim < 0 && !now.Before(nextSnapStop) {
			v := rng.below(cfg.nodes)
			rep.log(fmt.Sprintf("snapshot: node%d going offline for %s (ranges must expire past retention)",
				v+1, fmtDur(cfg.snapshotOff)))
			cluster.mu.Lock()
			alive[v].Store(false)
			cluster.c.StopNode(v)
			cluster.mu.Unlock()
			snapVictim = v
			snapAckedAtStop = rep.succeeded.Load()
			snapRestartAt = time.Now().Add(cfg.snapshotOff)
		}
		if snapVictim >= 0 && !now.Before(snapRestartAt) {
			v := snapVictim
			cluster.mu.Lock()
			startErr := cluster.c.StartNode(v)
			var meshErr error
			if startErr == nil {
				meshErr = cluster.c.Mesh()
			}
			if startErr == nil && meshErr == nil {
				alive[v].Store(true)
			}
			cluster.mu.Unlock()
			if startErr != nil {
				return zero, fmt.Errorf("node%d snapshot rejoin failed: %w", v+1, startErr)
			}
			if meshErr != nil {
				return zero, fmt.Errorf("re-mesh after snapshot rejoin: %w", meshErr)
			}
			rep.countFault("snapshot")
			rep.log(fmt.Sprintf("snapshot: node%d rejoined, must resync via snapshot", v+1))
			obs := observeSnapshotProof(cluster, v, rep, snapAckedAtStop, cfg.nodes)
			*snapProofs = append(*snapProofs, obs)
			snapVictim = -1
			nextSnapStop = time.Now().Add(cfg.snapshotEvery)
		}
	}

	// Restore any open impairment flap without counting it (an
	// interrupted cycle returns before the count).
	if impairActive != nil {
		a, b := impairActive.a, impairActive.b
		cluster.mu.Lock()
		_ = cluster.c.AddPeer(a, b)
		_ = cluster.c.AddPeer(b, a)
		cluster.mu.Unlock()
		rep.log(fmt.Sprintf("impair: edge node%d <-> node%d restored at chaos end (uncounted)", a+1, b+1))
	}
	cluster.mu.Lock()
	meshErr := cluster.c.Mesh()
	cluster.mu.Unlock()
	if meshErr != nil {
		return zero, fmt.Errorf("final re-mesh: %w", meshErr)
	}

	// Rejoin anyone still down when the chaos window closes.
	if snapVictim >= 0 {
		v := snapVictim
		cluster.mu.Lock()
		startErr := cluster.c.StartNode(v)
		var reMeshErr error
		if startErr == nil {
			reMeshErr = cluster.c.Mesh()
		}
		if startErr == nil && reMeshErr == nil {
			alive[v].Store(true)
		}
		cluster.mu.Unlock()
		if startErr != nil {
			return zero, fmt.Errorf("node%d end-of-chaos rejoin failed: %w", v+1, startErr)
		}
		if reMeshErr != nil {
			return zero, fmt.Errorf("re-mesh after end-of-chaos rejoin: %w", reMeshErr)
		}
		rep.countFault("snapshot")
		rep.log(fmt.Sprintf("snapshot: node%d rejoined at chaos end", v+1))
		obs := observeSnapshotProof(cluster, v, rep, snapAckedAtStop, cfg.nodes)
		*snapProofs = append(*snapProofs, obs)
	}
	return directorReport{snapshotsPossible: snapOK, skewScheduled: skewScheduled}, nil
}

// rotateNode runs online storage-key rotation on one node: rotate to a
// fresh random key without downtime via MURMUR ROTATE, then record it
// for later restarts (the harness stored key).
func rotateNode(cluster *shared, curKeys [][32]byte, idx int, rep *reporter) error {
	newKey := randomKey()
	n := rep.nextRotation()
	cluster.mu.Lock()
	_, err := cluster.c.Node(idx).Simple("MURMUR ROTATE KEY '" + goharness.Hex(curKeys[idx][:]) + "' '" + goharness.Hex(newKey[:]) + "'")
	if err != nil {
		cluster.mu.Unlock()
		return fmt.Errorf("rotate failed: %w", err)
	}
	// No key IDs exist in the product; staying online plus later
	// restarts on the new key carry that proof instead.
	rows, err := cluster.c.Node(idx).Query("SELECT COUNT(*) FROM contention")
	if err != nil {
		cluster.mu.Unlock()
		return fmt.Errorf("node unreadable after rotation")
	}
	if len(rows) == 0 {
		cluster.mu.Unlock()
		return fmt.Errorf("node unreadable after rotation")
	}
	count, err := rows[0].Int64(0)
	if err != nil {
		cluster.mu.Unlock()
		return fmt.Errorf("node unreadable after rotation")
	}
	curKeys[idx] = newKey
	if err := cluster.c.SetNodeKey(idx, newKey); err != nil {
		cluster.mu.Unlock()
		return err
	}
	cluster.mu.Unlock()
	rep.countFault("rotate")
	rep.log(fmt.Sprintf("rotate: node%d now on endurance-k%d (%d rows live)", idx+1, n, count))
	return nil
}

// ensureAllUp restarts any node the director left down so the verdict
// window starts with a full mesh.
func ensureAllUp(cluster *shared, alive []atomic.Bool, rep *reporter) error {
	rejoined := false
	for i := range alive {
		if alive[i].Load() {
			continue
		}
		rep.log(fmt.Sprintf("cleanup: node%d still down at chaos end, restarting", i+1))
		cluster.mu.Lock()
		err := cluster.c.StartNode(i)
		cluster.mu.Unlock()
		if err != nil {
			return fmt.Errorf("node%d cleanup restart failed: %w", i+1, err)
		}
		rejoined = true
	}
	if rejoined {
		cluster.mu.Lock()
		err := cluster.c.Mesh()
		cluster.mu.Unlock()
		if err != nil {
			return fmt.Errorf("re-mesh after cleanup restarts: %w", err)
		}
	}
	for i := range alive {
		alive[i].Store(true)
	}
	return nil
}

// waitAgreement polls until every node holds want rows with identical
// contents.
func waitAgreement(cluster *shared, nodes, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		var first []string
		hasFirst := false
		for i := 0; i < nodes; i++ {
			rows, readable := canonicalRows(cluster, i)
			if !readable || len(rows) != want {
				ok = false
				break
			}
			if !hasFirst {
				first = rows
				hasFirst = true
			} else if !equalStrings(first, rows) {
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

// waitConverged polls until every node is reachable with equal row
// counts and identical digests. Counts are compared to each other, not
// to a fixed target, because deletes move the total during chaos.
func waitConverged(cluster *shared, nodes int, timeout time.Duration) ([]int64, []string, error) {
	deadline := time.Now().Add(timeout)
	start := time.Now()
	lastSample := time.Now().Add(-time.Hour)
	for {
		counts := make([]int64, nodes)
		digests := make([]string, nodes)
		var first []string
		hasFirst := false
		agree := 0
		ok := true
		for i := 0; i < nodes; i++ {
			rows, readable := canonicalRows(cluster, i)
			if !readable {
				ok = false
				break
			}
			counts[i] = int64(len(rows))
			digests[i] = digestHex(rows)
			if !hasFirst {
				agree = 1
				first = rows
				hasFirst = true
			} else if !equalStrings(first, rows) {
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
				if c < lo {
					lo = c
				}
				if c > hi {
					hi = c
				}
			}
			fmt.Fprintf(os.Stderr, "[verdict-sample T+%ds] counts=%v spread=%d agree=%d/%d\n",
				int64(time.Since(start).Seconds()), counts, hi-lo, agree, nodes)
		}
		time.Sleep(verdictPoll)
	}
	// Final best-effort re-sample for the failure report.
	counts := make([]int64, nodes)
	digests := make([]string, nodes)
	snapshots := make([][]string, 0, nodes)
	for i := 0; i < nodes; i++ {
		rows, readable := canonicalRows(cluster, i)
		if !readable {
			counts[i] = -1
			digests[i] = "unreachable"
			snapshots = append(snapshots, nil)
			continue
		}
		counts[i] = int64(len(rows))
		digests[i] = digestHex(rows)
		snapshots = append(snapshots, rows)
	}
	// Diagnostic sample: first few rows where node0 and the first
	// disagreeing node differ (id + full values).
	if len(snapshots) == nodes {
		for j := 1; j < nodes; j++ {
			if equalStrings(snapshots[j], snapshots[0]) {
				continue
			}
			a := make(map[string]bool, len(snapshots[0]))
			for _, s := range snapshots[0] {
				a[s] = true
			}
			b := make(map[string]bool, len(snapshots[j]))
			for _, s := range snapshots[j] {
				b[s] = true
			}
			var onlyA, onlyB []string
			for s := range a {
				if !b[s] {
					if len(onlyA) < 5 {
						onlyA = append(onlyA, s)
					}
				}
			}
			for s := range b {
				if !a[s] {
					if len(onlyB) < 5 {
						onlyB = append(onlyB, s)
					}
				}
			}
			countA, countB := 0, 0
			for s := range a {
				if !b[s] {
					countA++
				}
			}
			for s := range b {
				if !a[s] {
					countB++
				}
			}
			fmt.Fprintf(os.Stderr, "[verdict-diff] node1-only(%d) sample=%q\n", countA, onlyA)
			fmt.Fprintf(os.Stderr, "[verdict-diff] node%d-only(%d) sample=%q\n", j+1, countB, onlyB)
			break
		}
	}
	return counts, digests, fmt.Errorf("nodes did not converge within %v", timeout)
}

// meshConnected reports whether every node sees every peer.
func meshConnected(cluster *shared, nodes int) bool {
	for i := 0; i < nodes; i++ {
		n, ok := connectedPeers(cluster, i)
		if !ok || n < nodes-1 {
			return false
		}
	}
	return true
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestEnduranceChaos(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cfg := loadConfig(t)
	// Deterministic distinct node ids so snapshot trust can be wired up
	// front (the engine only requests snapshots from explicitly trusted
	// sources).
	nodeIDs := make([][16]byte, cfg.nodes)
	for i := range nodeIDs {
		for k := range nodeIDs[i] {
			nodeIDs[i][k] = 0xC3
		}
		nodeIDs[i][0] = byte(i)
		nodeIDs[i][1] = byte(i)*131 + 7
	}
	all := map[[16]byte]bool{}
	for _, id := range nodeIDs {
		all[id] = true
	}
	trusted := map[int]map[[16]byte]bool{}
	for i := 0; i < cfg.nodes; i++ {
		cp := map[[16]byte]bool{}
		for id := range all {
			cp[id] = true
		}
		trusted[i] = cp
	}
	var initialKey [32]byte
	for i := range initialKey {
		initialKey[i] = 9
	}
	retention := time.Second
	retentionInterval := time.Second
	minimumRetained := uint64(10)
	maximumOffline := 20 * time.Second
	opts := goharness.DefaultClusterOptions("endurance-chaos")
	opts.NumNodes = cfg.nodes
	opts.NodeIDs = nodeIDs
	opts.Key = initialKey
	// Aggressive retention keeps log GC running under the sustained
	// workload and lets bounded offline windows force real snapshot
	// resyncs instead of log catch-up.
	opts.Replication = &goharness.ReplicationOptions{
		Retention:         &retention,
		RetentionInterval: &retentionInterval,
		MinimumRetained:   &minimumRetained,
		MaximumOffline:    &maximumOffline,
	}
	opts.TrustedSnapshotSources = trusted
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	runtimeDir := cluster.RuntimeDir()
	sharedCluster := &shared{c: cluster}

	result := runBody(sharedCluster, cfg, runtimeDir, initialKey)
	failed := result != nil
	if result != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", result)
	}
	// All spawned tasks joined inside runBody, so the cluster is
	// unshared here.
	if failed {
		cluster.DumpForensics("endurance-verdict")
	}
	cluster.Finish(failed)
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
}

func runBody(cluster *shared, cfg *config, runtimeDir string, initialKey [32]byte) error {
	rep := newReporter()
	rep.log(fmt.Sprintf("config: nodes=%d duration=%s settle=%s writeInterval=%s maxRows=%d seed=%d",
		cfg.nodes, fmtDur(cfg.duration), fmtDur(cfg.settle), fmtDur(cfg.writeInterval), cfg.maxRows, cfg.seed))
	rep.log(fmt.Sprintf("fault cadence: restart=%s rotate=%s snapshot=%s/%s impair=%s/%s disk=%s/%s skew=%s (degraded)",
		fmtDur(cfg.restartEvery), fmtDur(cfg.rotateEvery), fmtDur(cfg.snapshotEvery), fmtDur(cfg.snapshotOff),
		fmtDur(cfg.impairOn), fmtDur(cfg.impairOff), fmtDur(cfg.diskEvery), fmtDur(cfg.diskHold), fmtDur(cfg.skewDur)))

	// Same schema on every node, then replicate it: convergence needs
	// the per-table flag on each node.
	for i := 0; i < cfg.nodes; i++ {
		cluster.mu.Lock()
		err := cluster.c.EnsureMigrated(i, "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)")
		cluster.mu.Unlock()
		if err != nil {
			return fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < cfg.nodes; i++ {
		cluster.mu.Lock()
		_, setErr := cluster.c.Node(i).Simple("MURMUR SET TABLE contention REPLICATED true")
		var flushErr error
		if setErr == nil {
			_, flushErr = cluster.c.Node(i).Simple("MURMUR FLUSH")
		}
		cluster.mu.Unlock()
		if setErr != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, setErr)
		}
		if flushErr != nil {
			return fmt.Errorf("node%d flush after SET TABLE: %w", i+1, flushErr)
		}
	}

	// Baseline: one row everywhere before chaos starts.
	cluster.mu.Lock()
	_, baseErr := cluster.c.Node(0).Execute(
		"INSERT INTO contention(id, name, phone, score) VALUES($1, $2, $3, $4)",
		fmt.Sprintf("%032x", 0), "baseline", "baseline", int64(0))
	var flushErr error
	if baseErr == nil {
		_, flushErr = cluster.c.Node(0).Simple("MURMUR FLUSH")
	}
	cluster.mu.Unlock()
	if baseErr != nil {
		return fmt.Errorf("baseline insert: %w", baseErr)
	}
	if flushErr != nil {
		return fmt.Errorf("flush baseline: %w", flushErr)
	}
	if err := waitAgreement(cluster, cfg.nodes, 1, baselineTimeout); err != nil {
		return err
	}
	rep.log(fmt.Sprintf("baseline row converged on all %d nodes", cfg.nodes))

	// Preflights: detect degraded modes before the long run.
	rep.log(fmt.Sprintf("network impairment: logical peer flaps (tc/netem latency=%dms loss=%d%% has no child-process QUIC equivalent)",
		cfg.impairLatency, cfg.impairLossPct))
	rep.log("clock skew: DEGRADED away (server children share the host clock; no daemon for libfaketime)")

	fillerMB := cfg.diskMB
	cluster.mu.Lock()
	runtimeProbe := cluster.c.RuntimeDir()
	cluster.mu.Unlock()
	if free, ok := goharness.DirFreeBytes(runtimeProbe); ok {
		// Never risk the host filesystem: filler must leave 1 GiB free.
		var avail uint64
		if free > diskHeadroomBytes {
			avail = free - diskHeadroomBytes
		}
		if avail < fillerMB<<20 {
			headroom := avail >> 20
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
	var stop atomic.Bool

	// An alive-set steers writer targeting away from stopped nodes;
	// every write re-checks under the cluster lock, so lifecycle and
	// reads never race.
	alive := make([]atomic.Bool, cfg.nodes)
	for i := range alive {
		alive[i].Store(true)
	}
	cluster.mu.Lock()
	nodeDirs := make([]string, 0, cfg.nodes)
	for i := 0; i < cfg.nodes; i++ {
		nodeDirs = append(nodeDirs, cluster.c.NodeInfo(i).Dir)
	}
	cluster.mu.Unlock()

	numWriters := max(2, min(cfg.nodes, maxWriters))
	var writersWg, statusWg, diskWg sync.WaitGroup
	for w := 0; w < numWriters; w++ {
		writersWg.Add(1)
		go writerLoop(w, cluster, alive, registry, rep, cfg.seed, cfg.writeInterval, cfg.maxRows, &stop, &writersWg)
	}
	statusWg.Add(1)
	go statusLoop(cluster, nodeDirs, rep, gcHist, cfg.nodes, cfg.statusEvery, &stop, &statusWg)
	diskRunning := fillerMB > 0
	if diskRunning {
		diskWg.Add(1)
		go diskLoop(nodeDirs, rep, cfg.diskEvery, cfg.diskHold, fillerMB, &stop, &diskWg)
	}

	// Chaos director (test task: owns all node lifecycle).
	rep.phase("chaos", fmt.Sprintf("all fault injectors live for %s", fmtDur(cfg.duration)))
	directive, directorErr := runDirector(cluster, alive, rep, cfg, curKeys, &snapProofs)

	// Faults stop; every node must be up before the verdict window.
	// (Unlike Rust, tasks always join here even when the director
	// failed, so teardown never races a live writer.)
	stop.Store(true)
	if diskRunning {
		diskWg.Wait()
	}
	statusWg.Wait()
	if directorErr != nil {
		writersWg.Wait()
		return directorErr
	}
	if err := ensureAllUp(cluster, alive, rep); err != nil {
		writersWg.Wait()
		return err
	}
	rep.log(fmt.Sprintf("writers stopping: attempted=%d acked=%d transient-errors=%d live-ids=%d",
		rep.attempted.Load(), rep.succeeded.Load(), rep.failed.Load(), registry.len()))
	writersWg.Wait()

	reportPath := filepath.Join(runtimeDir, fmt.Sprintf("endurance-report-%d.md", unixNow()))
	if rep.succeeded.Load() == 0 {
		rep.verdict(false, reportPath, "no writes acked during the entire run")
		return fmt.Errorf("endurance verdict: 0 of %d writes acked; see report for distinct errors", rep.attempted.Load())
	}

	rep.phase("verdict", fmt.Sprintf("waiting up to %s for exact convergence", fmtDur(cfg.settle)))
	counts, digests, err := waitConverged(cluster, cfg.nodes, cfg.settle)
	if err != nil {
		rep.verdict(false, reportPath, err.Error())
		return fmt.Errorf("endurance verdict: %w\ncounts=%v\ndigests=%v", err, counts, digests)
	}
	rep.log(fmt.Sprintf("converged: all %d nodes hold %d rows with digest %s", cfg.nodes, counts[0], digests[0]))

	// Counter-proven post-checks: every claimed fault must have fired.
	// GC history (not base/after snapshots): collected is durable, so
	// high-water observations survive restarts; a node revived seconds
	// before the verdict still carries its proven history.
	for i := 0; i < cfg.nodes; i++ {
		ever, collEver, gcErr := gcHist.snapshot(i)
		if !ever {
			rep.verdict(false, reportPath, fmt.Sprintf("node%d GC never ran", i+1))
			return fmt.Errorf("node%d GC never observed running during chaos", i+1)
		}
		if !collEver {
			rep.verdict(false, reportPath, fmt.Sprintf("node%d GC collected nothing", i+1))
			return fmt.Errorf("node%d GC never observed collecting during chaos", i+1)
		}
		if gcErr != nil {
			rep.verdict(false, reportPath, fmt.Sprintf("node%d GC failures observed", i+1))
			return fmt.Errorf("node%d GC failure observed during chaos: %s", i+1, *gcErr)
		}
	}
	rep.log(fmt.Sprintf("GC proven on all %d nodes (collection observed advancing, GC failures flat)", cfg.nodes))

	faults := rep.faultSnapshot()
	if faults.restarts == 0 {
		rep.verdict(false, reportPath, "zero restarts executed")
		return fmt.Errorf("chaos ran zero restarts; restart injector never fired")
	}
	if faults.rotations == 0 {
		rep.verdict(false, reportPath, "zero key rotations executed")
		return fmt.Errorf("chaos ran zero key rotations; rotation injector never fired")
	}
	if faults.impairCycles == 0 {
		rep.verdict(false, reportPath, "zero network-impairment cycles executed")
		return fmt.Errorf("chaos ran zero network-impairment cycles")
	}
	if fillerMB > 0 && faults.diskCycles == 0 {
		rep.verdict(false, reportPath, "zero disk-pressure cycles executed")
		return fmt.Errorf("chaos ran zero disk-pressure cycles")
	}
	if directive.snapshotsPossible {
		if faults.snapshots == 0 {
			rep.verdict(false, reportPath, "zero snapshot offline windows executed")
			return fmt.Errorf("chaos ran zero snapshot offline windows")
		}
		proven := 0
		for _, o := range snapProofs {
			if o.proven {
				proven++
			}
		}
		if proven < 1 {
			rep.verdict(false, reportPath, "no snapshot resync observed")
			return fmt.Errorf("offline windows ran but no resync proved the snapshot path (log catch-up hid the path?)")
		}
		rep.log(fmt.Sprintf("snapshots proven: %d resyncs over %d offline windows", proven, len(snapProofs)))
	} else {
		rep.log("snapshots: leg skipped (run too short for an offline window)")
	}
	if directive.skewScheduled && faults.skews == 0 {
		rep.verdict(false, reportPath, "skew scheduled but never executed")
		return fmt.Errorf("clock skew was scheduled but the skew window never ran")
	}

	// Full mesh must be back: every node sees every peer.
	meshed := false
	meshDeadline := time.Now().Add(meshTimeout)
	for {
		if meshConnected(cluster, cfg.nodes) {
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
			n, _ := connectedPeers(cluster, i)
			detail = append(detail, fmt.Sprintf("node%d:%d", i+1, n))
		}
		rep.verdict(false, reportPath, "mesh not fully reconnected")
		return fmt.Errorf("mesh not fully reconnected (connected peers per node: %s), want %d",
			strings.Join(detail, " "), cfg.nodes-1)
	}

	// A post-chaos write must traverse the healed mesh.
	cluster.mu.Lock()
	_, postErr := cluster.c.Node(0).Execute(
		"INSERT INTO contention(id, name, phone, score) VALUES($1, $2, $3, $4)",
		fmt.Sprintf("%032x", uint64(999_999_999)), "post-chaos", "post-chaos", int64(0))
	var postFlushErr error
	if postErr == nil {
		_, postFlushErr = cluster.c.Node(0).Simple("MURMUR FLUSH")
	}
	cluster.mu.Unlock()
	if postErr != nil {
		rep.verdict(false, reportPath, "post-chaos write rejected")
		return fmt.Errorf("post-chaos write: %w", postErr)
	}
	if postFlushErr != nil {
		return fmt.Errorf("flush post-chaos write: %w", postFlushErr)
	}
	if err := waitAgreement(cluster, cfg.nodes, int(counts[0])+1, cfg.settle); err != nil {
		rep.verdict(false, reportPath, "post-chaos write did not converge")
		return fmt.Errorf("post-chaos convergence: %w", err)
	}

	rep.verdict(true, reportPath, fmt.Sprintf(
		"all %d nodes hold %d rows digest %s after %s of combined chaos (restarts=%d rotations=%d impair=flap/%d disk=%d snapshots=%d skews=%d)",
		cfg.nodes, counts[0]+1, digests[0], fmtDur(cfg.duration),
		faults.restarts, faults.rotations, faults.impairCycles, faults.diskCycles, faults.snapshots, faults.skews))
	return nil
}
