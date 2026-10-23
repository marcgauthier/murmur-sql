// Live scenario: parameterized adversarial validation of a live mesh.
//
// Go port of the Rust `abuse` scenario
// (/home/marc/MURMUR/tests-live/abuse/tests/abuse.rs). A live mesh is
// hammered through a healthy soak, a prolonged half-split partition, an
// extended outage of a quarter of the nodes with reconnect backlogs,
// and a simultaneous kill/restart storm of every node, with continuous
// writers throughout. The verdict is exact cross-node convergence
// (equal row counts plus identical PK-ordered row contents), never a
// sample. Plain SQL replaces the typed-contention HTTP API.
package abuse

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

const (
	// minNodes/maxNodes bound the mesh size (Rust: min(max(nodes, 3),
	// 256)).
	minNodes = 3
	maxNodes = 256
	// maxWriters caps writer threads (Rust: min(nodes, 8)).
	maxWriters = 8
	// linkWorkers is the link cut/heal fan-out (Rust: runLinks workers).
	linkWorkers = 32
	// baselineTimeout is the baseline replication deadline (Rust: 2
	// minutes).
	baselineTimeout = 120 * time.Second
	// baselinePoll is the baseline poll interval (Rust: 500 ms).
	baselinePoll = 500 * time.Millisecond
	// verdictPoll is the verdict convergence poll interval (Rust: 2 s).
	verdictPoll = 2 * time.Second
	// errSampleCap caps distinct error-message samples (Rust: first 8).
	errSampleCap = 8
	// errTruncChars truncates stored error messages (Rust: 200 bytes).
	errTruncChars = 200
)

// envUsize reads a usize env knob with fallback (Rust: harness.EnvInt).
func envUsize(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return fallback
}

// envU64 reads a u64 env knob with fallback (Rust: seed knob).
func envU64(name string, fallback uint64) uint64 {
	if v, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil {
		return v
	}
	return fallback
}

// envMillis reads a millisecond-duration env knob with fallback (Rust:
// MURMUR_ABUSE_WRITE_INTERVAL_MS).
func envMillis(name string, fallbackMs uint64) time.Duration {
	if v, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil && v > 0 {
		return time.Duration(v) * time.Millisecond
	}
	return time.Duration(fallbackMs) * time.Millisecond
}

// fmtDur renders a Go-style duration (90s -> 1m30s) for progress lines.
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

// rng is a deterministic splitmix64: each writer gets an independent
// stream seeded from seed*1000 + writer.
type rng struct {
	state uint64
}

func (r *rng) next() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *rng) below(n int) int {
	return int(r.next() % uint64(n))
}

// reporter is the live progress reporter: timestamped phase/log lines
// plus the same events for the final markdown report. All methods are
// safe for concurrent use.
type reporter struct {
	start     time.Time
	mu        sync.Mutex
	events    []string
	attempted atomic.Uint64
	succeeded atomic.Uint64
	failed    atomic.Uint64
	errSeen   map[string]int
	errSample []string
}

func newReporter() *reporter {
	return &reporter{start: time.Now(), errSeen: make(map[string]int)}
}

func (r *reporter) stamp() string {
	return fmt.Sprintf("[T+%s]", fmtDur(time.Since(r.start)))
}

func (r *reporter) emit(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	r.mu.Lock()
	r.events = append(r.events, msg)
	r.mu.Unlock()
}

// phase announces a phase transition (Rust: reporter.phase).
func (r *reporter) phase(name, detail string) {
	r.emit(fmt.Sprintf("%s === PHASE: %s === %s", r.stamp(), name, detail))
}

// log records a timestamped progress line (Rust: reporter.logf).
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

// sampleErr records distinct write-error messages (first 8) with counts
// (Rust: sampleErr).
func (r *reporter) sampleErr(err string) {
	runes := []rune(err)
	if len(runes) > errTruncChars {
		runes = runes[:errTruncChars]
	}
	msg := string(runes)
	r.mu.Lock()
	r.errSeen[msg]++
	if len(r.errSample) < errSampleCap && !slices.Contains(r.errSample, msg) {
		r.errSample = append(r.errSample, msg)
	}
	r.mu.Unlock()
}

// verdict logs the final PASS/FAIL line and writes the markdown report
// (Rust: verdict). Failure artifact retention itself is handled by
// Cluster.Finish.
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
	r.mu.Unlock()
	b := fmt.Sprintf("# Abuse run report — %s\n\n", status)
	b += fmt.Sprintf("- finished (unix): %d\n- wall time: %s\n- writes attempted: %d\n- writes acked: %d\n- writes failed (transient): %d\n- verdict: %s\n\n",
		time.Now().Unix(), fmtDur(time.Since(r.start)), r.attempted.Load(), r.succeeded.Load(), r.failed.Load(), summary)
	if len(errSample) > 0 {
		b += "## Distinct write errors\n\n"
		for _, s := range errSample {
			b += fmt.Sprintf("- (%dx) %s\n", errSeen[s], s)
		}
		b += "\n"
	}
	b += "## Timeline\n\n"
	for _, e := range events {
		b += fmt.Sprintf("- %s\n", e)
	}
	if err := os.WriteFile(reportPath, []byte(b), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "report write failed: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "report written to %s\n", reportPath)
	}
}

// pickAlive picks a random live node (Rust: pickAlive); ok is false when
// all are down.
func pickAlive(alive []atomic.Bool, r *rng) (int, bool) {
	var up []int
	for i := range alive {
		if alive[i].Load() {
			up = append(up, i)
		}
	}
	if len(up) == 0 {
		return 0, false
	}
	return up[r.below(len(up))], true
}

// rowCount reads the row count on one node, or -1 when the read fails.
func rowCount(db *murmur.Database) int {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.QueryRow("SELECT COUNT(*) FROM contention")
	})
	if err != nil {
		return -1
	}
	return int(v.([]sqlite.Value)[0].Int)
}

// canonicalRows reads the PK-ordered encoded rows on one node (Rust:
// abuseNodeState input: id:name:phone:score sorted by id); ok is false
// when the read fails.
func canonicalRows(db *murmur.Database) ([]string, bool) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT id, name, phone, score FROM contention ORDER BY id")
	})
	if err != nil {
		return nil, false
	}
	var out []string
	for _, r := range v.([][]sqlite.Value) {
		out = append(out, fmt.Sprintf("%s:%s:%s:%d", r[0].Str, r[1].Str, r[2].Str, r[3].Int))
	}
	return out, true
}

// digestHex is a deterministic log-only digest over canonical rows
// (comparisons use byte-identical vectors, which imply identical
// digests). FNV-1a keeps this dependency-free.
func digestHex(rows []string) string {
	var h uint64 = 0xcbf29ce484222325
	for _, row := range rows {
		for _, b := range []byte(row) {
			h ^= uint64(b)
			h *= 0x100000001b3
		}
		h ^= uint64('\n')
		h *= 0x100000001b3
	}
	return fmt.Sprintf("%016x", h)
}

// nodeState reads the row count plus PK-ordered digest and rows on one
// node (Rust: abuseNodeState).
func nodeState(cluster *harness.Cluster, idx int) (int, string, []string, bool) {
	rows, ok := canonicalRows(cluster.Node(idx))
	if !ok {
		return 0, "", nil, false
	}
	return len(rows), digestHex(rows), rows, true
}

// runLinks applies one link operation to every link with a bounded
// worker pool so a large partition heals in seconds (Rust: runLinks).
func runLinks(cluster *harness.Cluster, links [][2]int, add bool) {
	if len(links) == 0 {
		return
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(linkWorkers, len(links)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				k := int(next.Add(1) - 1)
				if k >= len(links) {
					return
				}
				// Non-fatal by design (Rust: runLinks ignores
				// errors); the convergence verdict is the judge.
				from, to := links[k][0], links[k][1]
				if add {
					_ = cluster.AddPeer(from, to)
				} else {
					_ = cluster.RemovePeer(from, to)
				}
			}
		}()
	}
	wg.Wait()
}

// handleTable is the writer handle table shared with the driver: the
// entry for a restarted node is swapped for the reopened database.
type handleTable struct {
	mu sync.Mutex
	db []*murmur.Database
}

func (h *handleTable) get(i int) *murmur.Database {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.db[i]
}

func (h *handleTable) set(i int, db *murmur.Database) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.db[i] = db
}

// restartNodes restarts every listed node and rejoins the mesh (Rust:
// restartBehindBarrier): concurrent in-process reopens behind one
// barrier, then one re-mesh so all catch-up overlaps like a real
// reconnect storm. Writer handles are swapped for the reopened
// databases and the nodes marked alive.
func restartNodes(t *testing.T, cluster *harness.Cluster, handles *handleTable, alive []atomic.Bool, idx []int, rep *reporter) {
	t.Helper()
	begin := time.Now()
	if err := cluster.StartNodesParallel(idx); err != nil {
		t.Fatalf("parallel restart of %v failed: %v", idx, err)
	}
	rep.log(fmt.Sprintf("%d nodes ready %v after storm restart (barrier)", len(idx), time.Since(begin)))
	// Reopened nodes rebind fresh ephemeral ports: re-dial every ordered
	// pair of running nodes (Rust: per-node StartNode re-dials).
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh after restart: %v", err)
	}
	for _, i := range idx {
		handles.set(i, cluster.Node(i))
		alive[i].Store(true)
	}
}

// waitAllCount polls until every node reports want rows or the timeout
// elapses (Rust: waitAllCount).
func waitAllCount(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		for i := 0; i < cluster.Len(); i++ {
			if rowCount(cluster.Node(i)) != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("row count did not reach %d on all %d nodes within %v", want, cluster.Len(), timeout)
		}
		time.Sleep(baselinePoll)
	}
}

// waitConverged polls until every node is reachable and all PK-ordered
// row contents match (Rust: waitConverged: equal counts plus identical
// digests). It returns the last observed per-node counts and digests.
func waitConverged(t *testing.T, cluster *harness.Cluster, nodes int, timeout time.Duration) ([]int64, []string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	start := time.Now()
	lastSample := time.Now().Add(-time.Hour)
	for {
		counts := make([]int64, nodes)
		digests := make([]string, nodes)
		var firstRows []string
		agree := 0
		ok := true
		for i := 0; i < nodes; i++ {
			n, d, rows, good := nodeState(cluster, i)
			if !good {
				ok = false
				break
			}
			counts[i] = int64(n)
			digests[i] = d
			if firstRows == nil {
				agree = 1
				firstRows = rows
			} else if !slices.Equal(firstRows, rows) {
				ok = false
			} else {
				agree++
			}
		}
		if ok {
			return counts, digests
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
			if os.Getenv("MURMUR_VERDICT_METRICS") != "" {
				var m []string
				for i := 0; i < nodes; i++ {
					st := cluster.Node(i).Status()
					if st.Replication == nil {
						m = append(m, fmt.Sprintf("%d:none", i))
						continue
					}
					r := st.Replication
					m = append(m, fmt.Sprintf("%d:p%d|st%d|push%d|rcv%d|app%d|ap%d|ae%d|gos%d",
						i, r.Peers, r.StagedTransactions, r.Metrics.PushedTransactions,
						r.Metrics.ReceivedTransactions, r.Metrics.AppliedTransactions,
						r.AppliedTransactions, r.Metrics.AntiEntropyRuns, r.Metrics.GossipRounds))
				}
				fmt.Fprintf(os.Stderr, "[verdict-metrics] %s\n", joinStrings(m, " "))
			}
		}
		time.Sleep(verdictPoll)
	}
	// Final best-effort re-sample for the failure report.
	counts := make([]int64, nodes)
	digests := make([]string, nodes)
	for i := 0; i < nodes; i++ {
		n, d, _, good := nodeState(cluster, i)
		if good {
			counts[i] = int64(n)
			digests[i] = d
		} else {
			counts[i] = -1
			digests[i] = "unreachable"
		}
	}
	t.Fatalf("nodes did not converge within %v\ncounts=%v\ndigests=%v", timeout, counts, digests)
	return counts, digests
}

func joinStrings(ss []string, sep string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += sep
		}
		out += s
	}
	return out
}

func TestAbuseMesh(t *testing.T) {
	nodes := min(max(envUsize("MURMUR_ABUSE_NODES", 10), minNodes), maxNodes)
	duration := harness.EnvSeconds("MURMUR_ABUSE_DURATION_SECONDS", 600)
	// Quiet convergence window after writers stop. Calibrated with the
	// write interval below: the 10-node colocated mesh repairs ~47 tx/s,
	// so the offered load must stay near that envelope or the chaos
	// backlog grows faster than any reasonable window can drain
	// (parallel trajectories never close). Single-node production
	// deployments (dedicated disk/CPU) converge far faster; these bounds
	// cover the colocated test worst case.
	settle := harness.EnvSeconds("MURMUR_ABUSE_SETTLE_SECONDS", 900)
	// 100ms across 8 writers: keeps multi-writer contention
	// while offering ~80 tx/s (throttled lower) instead of ~160/s, so the
	// outage-gap backlog fits the verdict window at ~47 tx/s repair.
	writeInterval := envMillis("MURMUR_ABUSE_WRITE_INTERVAL_MS", 100)
	seed := envU64("MURMUR_ABUSE_SEED", 1)
	statusInterval := harness.EnvSeconds("MURMUR_ABUSE_STATUS_SECONDS", 15)

	// Fanout 2 (product default 4): ten colocated nodes on a small box
	// saturate CPU on 4-way gossip duplicates; two push targets halve the
	// duplicate stage/verify work while multi-hop diffusion still covers
	// the mesh in seconds, far inside the verdict window.
	// Full-mesh snapshot trust: outage/partition victims face 10k+
	// transaction gaps that grind for many minutes per-tx; bulk
	// snapshot catch-up (receiver-side lag trigger) closes them in about
	// a minute. All members share one CA, so trusting the mesh is sound.
	nodeIDs := make([][16]byte, nodes)
	for i := range nodeIDs {
		for b := range nodeIDs[i] {
			nodeIDs[i][b] = byte(i + 1)
		}
	}
	trust := make(map[int]map[[16]byte]bool, nodes)
	for i := 0; i < nodes; i++ {
		set := make(map[[16]byte]bool)
		for j := 0; j < nodes; j++ {
			if j != i {
				set[nodeIDs[j]] = true
			}
		}
		trust[i] = set
	}
	fanout := 2
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:                   "abuse",
		NumNodes:               nodes,
		NodeIDs:                nodeIDs,
		TrustedSnapshotSources: trust,
		Replication:            &harness.ReplicationOptions{Fanout: &fanout},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("abuse-no-converge")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("Node %s running in directory %s (repl=%s)", info.Label, info.Dir, info.ReplAddr)
	}

	rep := newReporter()
	rep.log(fmt.Sprintf("config: nodes=%d duration=%s settle=%s writeInterval=%s seed=%d statusEvery=%s",
		nodes, fmtDur(duration), fmtDur(settle), fmtDur(writeInterval), seed, fmtDur(statusInterval)))

	// Same schema on every node (Rust: typed-contention fixture table).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)"); err != nil {
			t.Fatalf("setup migrate node%d: %v", i+1, err)
		}
	}
	// Opt in after every node migrated (opting in publishes the schema
	// immediately).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("contention", true); err != nil {
			t.Fatalf("setup opt-in node%d: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Baseline: one row on node 0, present everywhere before abuse starts.
	_, err = cluster.Node(0).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
			fmt.Sprintf("%032x", 0), "baseline", "", int64(0))
		return nil, err
	})
	if err != nil {
		t.Fatalf("baseline insert: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	waitAllCount(t, cluster, 1, baselineTimeout)
	rep.log(fmt.Sprintf("baseline row converged on all %d nodes", nodes))

	// Writer goroutines share a handle table with the driver: the
	// entry for a restarted node is swapped for the reopened database,
	// and an alive-set steers targeting away from stopped nodes. Every
	// attempt mints a fresh id, so a retry after a transient failure
	// can never collide on the primary key. Transient errors are
	// counted, never fatal.
	handles := &handleTable{db: make([]*murmur.Database, nodes)}
	for i := 0; i < nodes; i++ {
		handles.db[i] = cluster.Node(i)
	}
	alive := make([]atomic.Bool, nodes)
	for i := range alive {
		alive[i].Store(true)
	}
	var stopWriters, stopStatus atomic.Bool

	var workers sync.WaitGroup
	for w := 0; w < min(nodes, maxWriters); w++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			r := &rng{state: seed*1000 + uint64(w)}
			var seq uint64
			for {
				if stopWriters.Load() {
					return
				}
				target, ok := pickAlive(alive, r)
				if !ok {
					time.Sleep(writeInterval)
					continue
				}
				seq++
				// 32 lowercase hex chars: unique per
				// writer/attempt (Rust: %08x%024x).
				id := fmt.Sprintf("%08x%024x", w, seq)
				name := fmt.Sprintf("writer-%d", w)
				db := handles.get(target)
				_, txErr := db.Transaction(func(s *murmur.Sql) (any, error) {
					_, err := s.Execute("INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
						id, name, "", int64(0))
					return nil, err
				})
				var outcome error = txErr
				if outcome == nil {
					outcome = db.Flush()
				}
				if outcome == nil {
					rep.countAttempt(true)
				} else {
					rep.countAttempt(false)
					rep.sampleErr(outcome.Error())
				}
				if stopWriters.Load() {
					return
				}
				time.Sleep(writeInterval)
			}
		}()
	}

	// Heartbeat goroutine: elapsed time, write totals, min/max row
	// counts, unreachable nodes.
	statusDone := make(chan struct{})
	go func() {
		defer close(statusDone)
		for {
			time.Sleep(statusInterval)
			if stopStatus.Load() {
				return
			}
			minC, maxC := int64(-1), int64(-1)
			down := 0
			for i := 0; i < nodes; i++ {
				c := rowCount(handles.get(i))
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
			rep.log(fmt.Sprintf("status: writes attempted=%d ok=%d err=%d | rows min=%d max=%d unreachable=%d/%d",
				rep.attempted.Load(), rep.succeeded.Load(), rep.failed.Load(), minC, maxC, down, nodes))
		}
	}()

	quarter := duration / 4

	// Phase 1: healthy-mesh soak. Writers run, nothing breaks.
	rep.phase("mesh-soak", fmt.Sprintf("healthy full mesh for %s", fmtDur(quarter)))
	time.Sleep(quarter)

	// Phase 2: prolonged partition. The mesh splits into halves with no
	// cross-links; both sides keep accepting writes (divergence is the
	// point). Healed at the end of the phase.
	rep.phase("partition", fmt.Sprintf("splitting %d nodes into halves for %s", nodes, fmtDur(quarter)))
	half := nodes / 2
	var links [][2]int
	for a := 0; a < half; a++ {
		for b := half; b < nodes; b++ {
			links = append(links, [2]int{a, b}, [2]int{b, a})
		}
	}
	cut := len(links)
	runLinks(cluster, links, false)
	rep.log(fmt.Sprintf("partition cut: %d cross-links removed (%d nodes isolated)", cut, nodes-half))
	time.Sleep(quarter)
	runLinks(cluster, links, true)
	rep.log(fmt.Sprintf("partition healed: %d cross-links restored", cut))

	// Phase 3: extended outage. A quarter of the nodes go down while
	// writers hammer the survivors, building the reconnect backlog.
	// All victims restart together at the end of the phase.
	// Alternating kill/graceful stops are both close()+reopen
	// in-process (see README.md).
	victims := max(nodes/4, 1)
	rep.phase("outage", fmt.Sprintf("stopping %d of %d nodes for %s", victims, nodes, fmtDur(quarter)))
	victimIdx := make([]int, 0, victims)
	for idx := nodes - victims; idx < nodes; idx++ {
		victimIdx = append(victimIdx, idx)
	}
	for _, idx := range victimIdx {
		alive[idx].Store(false)
		cluster.StopNode(idx)
	}
	rep.log(fmt.Sprintf("outage: %d nodes down, writers continue on %d survivors", victims, nodes-victims))
	time.Sleep(quarter)
	rep.log(fmt.Sprintf("outage over: restarting %d victims behind one barrier", victims))
	restartNodes(t, cluster, handles, alive, victimIdx, rep)

	// Phase 4: reconnect storm. Every node is stopped and restarted
	// while writers keep firing into the churn.
	rep.phase("storm", fmt.Sprintf("killing and restarting all %d nodes, then soaking %s", nodes, fmtDur(quarter)))
	all := make([]int, 0, nodes)
	for idx := 0; idx < nodes; idx++ {
		all = append(all, idx)
	}
	for _, idx := range all {
		alive[idx].Store(false)
	}
	for _, idx := range all {
		cluster.StopNode(idx)
	}
	rep.log(fmt.Sprintf("storm: all %d nodes killed, restarting behind one barrier", nodes))
	restartNodes(t, cluster, handles, alive, all, rep)
	time.Sleep(quarter)

	// Writers stop; the mesh gets `settle` to converge, then verdict.
	stopWriters.Store(true)
	workers.Wait()
	stopStatus.Store(true)
	<-statusDone
	rep.log(fmt.Sprintf("writers stopped: attempted=%d acked=%d transient-errors=%d",
		rep.attempted.Load(), rep.succeeded.Load(), rep.failed.Load()))

	// Anti-vacuous guard: if no write was ever acked (not even during
	// the healthy soak), the abuse proved nothing and the run must fail.
	reportPath := cluster.RuntimeDir() + fmt.Sprintf("/abuse-report-%d.md", time.Now().Unix())
	if rep.succeeded.Load() == 0 {
		rep.verdict(false, reportPath, "no writes acked during the entire run")
		t.Fatalf("abuse verdict: 0 of %d writes acked; see report for distinct errors", rep.attempted.Load())
	}

	rep.phase("verdict", fmt.Sprintf("waiting up to %s for exact convergence", fmtDur(settle)))
	counts, digests := waitConverged(t, cluster, nodes, settle)
	rep.verdict(true, reportPath, fmt.Sprintf("all %d nodes hold %d rows with identical digest %s",
		nodes, counts[0], digests[0]))
	failed = false
}
