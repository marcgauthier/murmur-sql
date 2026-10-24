// Live scenario: one of three meshed servers is SIGKILLed and restarted N
// times under continuous writes from every node, with short uptimes
// between kills.
//
// Go port of tests/crash_loop.rs using tests-live/goharness. Identical
// behavior: every write acknowledged before each kill must survive; the
// looped node must converge to identical contents; it must reach ready
// within a bound after the final restart (no wedge); and a post-loop
// write on it must replicate everywhere. Kill placement is proven, not
// assumed: every non-final round requires a write provably in flight on
// the victim (mid-write), and the final round kills immediately after a
// 30-row burst on a survivor (mid-replication). The Rust tokio tasks
// become goroutines over a shared mutex-held cluster.
package crashloop

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Crash victim: node3.
const victim = 2

// Writer insert cadence.
const writeInterval = 25 * time.Millisecond

// Convergence poll interval.
const poll = 100 * time.Millisecond

// Per-insert wire timeout so a kill can never wedge a writer on the lock.
const insertTimeout = 10 * time.Second

// Positive-control deadlines.
const controlTimeout = 30 * time.Second

// In-flight gate for non-final kills.
const inflightTimeout = 10 * time.Second

// Final-round burst size on a survivor.
const burstRows = int64(30)

// Final-round bound between burst and kill.
const burstWindow = 500 * time.Millisecond

// No-wedge bound for the final restart.
const readyBound = 60 * time.Second

// Post-loop mesh/convergence deadlines.
const healTimeout = 90 * time.Second

// Post-loop write convergence deadline.
const postTimeout = 60 * time.Second

// Row-id stride per writer node: ids are node*idStride + seq, so the
// three continuous writers never collide.
const idStride = int64(10_000_000)

// Baseline ids live above every writer range.
const baselineIDBase = int64(90_000_001)

// Burst ids live above the baseline range.
const burstIDBase = int64(90_001_001)

// Post-loop marker id.
const postLoopID = int64(90_002_001)

// envInt is a positive-integer env knob with fallback.
func envInt(name string, fallback uint64) uint64 {
	if v, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil && v > 0 {
		return v
	}
	return fallback
}

// shared is the shared writer state. Writers and the driver share the
// cluster behind one mutex; the running flags keep writers off a stopped
// node (whose Cluster.Node would panic) without racing the driver's kill
// path: the driver clears the victim flag well before locking for the
// kill, and writers re-check the flag while holding the lock.
type shared struct {
	mu           sync.Mutex
	cluster      *goharness.Cluster
	ackMu        sync.Mutex
	acknowledged map[string]bool
	inFlight     [3]atomic.Int64
	seqs         [3]atomic.Int64
	running      [3]atomic.Bool
	stop         atomic.Bool
}

func newShared(cluster *goharness.Cluster) *shared {
	s := &shared{cluster: cluster, acknowledged: map[string]bool{}}
	for i := range s.running {
		s.running[i].Store(true)
	}
	return s
}

// insert writes one row plus a durability barrier, recording the
// acknowledgement. Only error-free inserts count: refusals during
// kill/restart windows are dropped and the next unique insert continues
// the workload.
func insert(s *shared, node int, id int64, name string) error {
	s.inFlight[node].Add(1)
	defer s.inFlight[node].Add(-1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running[node].Load() {
		return fmt.Errorf("node stopped")
	}
	tagged := fmt.Sprintf("%s@%d", name, id)
	if _, err := s.cluster.Node(node).ExecuteTimeout(
		"INSERT INTO records(id, name) VALUES($1, $2)", insertTimeout, id, tagged); err != nil {
		return fmt.Errorf("node%d insert '%s': %w", node+1, name, err)
	}
	if _, err := s.cluster.Node(node).Inner().SimpleTimeout("MURMUR FLUSH", insertTimeout); err != nil {
		return fmt.Errorf("node%d flush after '%s': %w", node+1, name, err)
	}
	s.ackMu.Lock()
	s.acknowledged[fmt.Sprintf("%s@%d", name, id)] = true
	s.ackMu.Unlock()
	return nil
}

// writer is one continuous writer (one 25 ms ticker goroutine per node).
func writer(s *shared, node int) {
	for !s.stop.Load() {
		time.Sleep(writeInterval)
		if s.stop.Load() {
			break
		}
		if !s.running[node].Load() {
			continue
		}
		seq := s.seqs[node].Add(1)
		name := fmt.Sprintf("n%d-%08d", node+1, seq)
		// Refusals during kill/restart windows are dropped; the next
		// unique insert continues the workload.
		_ = insert(s, node, int64(node)*idStride+seq, name)
	}
}

// names returns the sorted names on one node.
func names(s *shared, idx int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.cluster.Node(idx).Query("SELECT name FROM records")
	if err != nil {
		return nil, fmt.Errorf("node%d SELECT names: %w", idx+1, err)
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		name, err := row.String(0)
		if err != nil {
			return nil, fmt.Errorf("node%d decode name: %w", idx+1, err)
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// peerCounts returns the connected sessions per node, in node order (the
// peers column of MURMUR SHOW REPLICATION).
func peerCounts(s *shared) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, 0, s.cluster.Len())
	for i := 0; i < s.cluster.Len(); i++ {
		rows, err := s.cluster.Node(i).Simple("MURMUR SHOW REPLICATION")
		if err != nil {
			return nil, fmt.Errorf("node%d SHOW REPLICATION: %w", i+1, err)
		}
		peers := ""
		if len(rows) > 0 && len(rows[0]) > 2 {
			peers = rows[0][2]
		}
		n, err := strconv.ParseInt(peers, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("node%d unparsable peers %q", i+1, peers)
		}
		out = append(out, n)
	}
	return out, nil
}

// waitForPeers waits until every node reports its wanted session count.
func waitForPeers(s *shared, want []int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		got, err := peerCounts(s)
		if err != nil {
			return err
		}
		if equalInt64(got, want) {
			return nil
		}
		if !time.Now().Before(deadline) {
			last, err := peerCounts(s)
			if err != nil {
				return err
			}
			return fmt.Errorf("peers did not reach %v within %s (last: %v)", want, timeout, last)
		}
		time.Sleep(poll)
	}
}

// waitForConvergence waits until every node holds equal counts with equal
// sorted-name digests.
func waitForConvergence(s *shared, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var want []string
		first := true
		ok := true
		s.mu.Lock()
		length := s.cluster.Len()
		s.mu.Unlock()
		for i := 0; i < length; i++ {
			nodeNames, err := names(s, i)
			if err != nil {
				return err
			}
			if first {
				want, first = nodeNames, false
			} else if !equalStrings(want, nodeNames) {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		if !time.Now().Before(deadline) {
			s.mu.Lock()
			length := s.cluster.Len()
			s.mu.Unlock()
			for i := 0; i < length; i++ {
				nodeNames, err := names(s, i)
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "node%d at timeout: count=%d\n", i+1, len(nodeNames))
			}
			return fmt.Errorf("nodes did not converge (counts + name digests) within %s", timeout)
		}
		time.Sleep(poll)
	}
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

func equalInt64(a, b []int64) bool {
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

// assertAcknowledgedPresent requires every acknowledged write to be
// present on every node.
func assertAcknowledgedPresent(s *shared) error {
	s.ackMu.Lock()
	acknowledged := make([]string, 0, len(s.acknowledged))
	for name := range s.acknowledged {
		acknowledged = append(acknowledged, name)
	}
	s.ackMu.Unlock()
	s.mu.Lock()
	length := s.cluster.Len()
	s.mu.Unlock()
	for i := 0; i < length; i++ {
		nodeNames, err := names(s, i)
		if err != nil {
			return err
		}
		present := map[string]bool{}
		for _, n := range nodeNames {
			present[n] = true
		}
		for _, name := range acknowledged {
			if !present[name] {
				return fmt.Errorf("node%d lost acknowledged write %q", i+1, name)
			}
		}
	}
	return nil
}

func acknowledgedLen(s *shared) int {
	s.ackMu.Lock()
	defer s.ackMu.Unlock()
	return len(s.acknowledged)
}

func runBody(s *shared, rounds int, uptime time.Duration) error {
	s.mu.Lock()
	for i := 0; i < s.cluster.Len(); i++ {
		info := s.cluster.NodeInfo(i)
		pid, ok := s.cluster.NodePID(i)
		fmt.Fprintf(os.Stderr, "%s pid=%v dir=%s sql=%s repl=%s\n",
			info.Label, pidOrNone(pid, ok), info.Dir, info.SQLAddr, info.ReplAddr)
	}
	s.mu.Unlock()
	// Same schema on every node, then replicate it: row convergence
	// needs the per-table flag on each.
	for i := 0; i < 3; i++ {
		s.mu.Lock()
		err := s.cluster.EnsureMigrated(i, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
		s.mu.Unlock()
		if err != nil {
			return fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < 3; i++ {
		s.mu.Lock()
		_, err := s.cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true")
		s.mu.Unlock()
		if err != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		s.mu.Lock()
		_, err = s.cluster.Node(i).Simple("MURMUR FLUSH")
		s.mu.Unlock()
		if err != nil {
			return fmt.Errorf("node%d flush after SET TABLE: %w", i+1, err)
		}
	}

	// Positive control: honest 3-node convergence before any crash.
	for node := 0; node < 3; node++ {
		if err := insert(s, node, baselineIDBase+int64(node), fmt.Sprintf("base-node%d", node+1)); err != nil {
			return err
		}
	}
	if err := waitForPeers(s, []int64{2, 2, 2}, controlTimeout); err != nil {
		return err
	}
	if err := waitForConvergence(s, controlTimeout); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "positive control: 3-node mesh converged")

	var wg sync.WaitGroup
	for node := 0; node < 3; node++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			writer(s, node)
		}()
	}

	var readyDurations []time.Duration
	driver := func() error {
		for round := 1; round <= rounds; round++ {
			time.Sleep(uptime)
			if round == rounds {
				// Burst on a survivor, then kill at once: prefer a
				// moment with a request in flight (mid-write), but
				// bound the wait so the burst is still unflushed
				// (mid-replication).
				for i := int64(0); i < burstRows; i++ {
					if err := insert(s, 0, burstIDBase+i, fmt.Sprintf("burst-%d", i)); err != nil {
						return err
					}
				}
				deadline := time.Now().Add(burstWindow)
				for s.inFlight[victim].Load() == 0 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
			} else {
				// Every non-final kill requires a request in
				// flight: mid-write proven.
				deadline := time.Now().Add(inflightTimeout)
				for s.inFlight[victim].Load() == 0 {
					if !time.Now().Before(deadline) {
						return fmt.Errorf("node%d had no write request in flight before SIGKILL", victim+1)
					}
					time.Sleep(time.Millisecond)
				}
			}
			fmt.Fprintf(os.Stderr, "round %d/%d: SIGKILL node%d with %d request(s) in flight\n",
				round, rounds, victim+1, s.inFlight[victim].Load())
			// Writers observe the flag (and the re-check under the
			// lock) before the kill lands, so no writer can touch
			// the victim while it is stopped.
			s.running[victim].Store(false)
			time.Sleep(150 * time.Millisecond)
			start := time.Now()
			restartErr := func() error {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.cluster.KillNode(victim)
				if err := s.cluster.StartNode(victim); err != nil {
					return fmt.Errorf("round %d restart node%d: %w", round, victim+1, err)
				}
				if _, err := s.cluster.Node(victim).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
					return fmt.Errorf("round %d re-set replicated: %w", round, err)
				}
				if _, err := s.cluster.Node(victim).Simple("MURMUR FLUSH"); err != nil {
					return fmt.Errorf("round %d flush node%d: %w", round, victim+1, err)
				}
				if err := s.cluster.Mesh(); err != nil {
					return fmt.Errorf("round %d re-mesh: %w", round, err)
				}
				_ = s.cluster.SyncAll()
				return nil
			}()
			if restartErr != nil {
				return restartErr
			}
			s.running[victim].Store(true)
			readyDurations = append(readyDurations, time.Since(start))
			fmt.Fprintf(os.Stderr, "round %d/%d: node%d ready in %s\n",
				round, rounds, victim+1, readyDurations[len(readyDurations)-1])
		}
		return nil
	}()
	s.stop.Store(true)
	wg.Wait()
	if driver != nil {
		return driver
	}

	// No wedge: the final restart reaches ready within the bound, the mesh
	// reforms, and every acked write is present everywhere.
	if len(readyDurations) > 0 {
		if last := readyDurations[len(readyDurations)-1]; last > readyBound {
			return fmt.Errorf("final restart took %s to reach ready, want < %s (wedge)", last, readyBound)
		}
	}
	if err := waitForPeers(s, []int64{2, 2, 2}, healTimeout); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "mesh reformed after the crash loop")
	s.mu.Lock()
	_ = s.cluster.SyncAll()
	s.mu.Unlock()
	if err := waitForConvergence(s, healTimeout); err != nil {
		return err
	}
	if err := assertAcknowledgedPresent(s); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "all %d acknowledged writes survived %d SIGKILLs\n", acknowledgedLen(s), rounds)

	if err := insert(s, victim, postLoopID, "post-loop-from-victim"); err != nil {
		return err
	}
	if err := waitForConvergence(s, postTimeout); err != nil {
		return err
	}
	if err := assertAcknowledgedPresent(s); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "post-loop write on the looped node replicated everywhere")
	return nil
}

func pidOrNone(pid int, ok bool) any {
	if !ok {
		return "none"
	}
	return pid
}

func TestCrashLoopNodeConvergesWithoutLoss(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	rounds := int(envInt("MURMUR_CRASH_LOOP_ROUNDS", 5))
	uptime := time.Duration(envInt("MURMUR_CRASH_LOOP_UPTIME_MS", 1000)) * time.Millisecond

	opts := goharness.DefaultClusterOptions("crash-loop")
	opts.NumNodes = 3
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 3 {
		t.Fatal("this scenario is written for 3 nodes")
	}
	s := newShared(cluster)
	runErr := runBody(s, rounds, uptime)
	failed := runErr != nil
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", runErr)
	}
	if failed {
		cluster.DumpForensics("crash-loop-no-convergence")
	}
	cluster.Finish(failed)
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
}
