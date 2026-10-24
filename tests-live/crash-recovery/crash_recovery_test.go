// Live scenario: three meshed servers take continuous writes (two
// workers per node) while node2 and then node3 are SIGKILLed mid-write
// and restarted under load.
//
// Go port of tests/crash_recovery.rs using tests-live/goharness.
// Identical behavior: every acknowledged application write must survive
// both restarts with identical digests on all nodes, and a post-recovery
// write must replicate everywhere. The Rust tokio tasks become goroutines
// over a shared mutex-held cluster.
package crashrecovery

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

// Writers per node: 2 workers per node.
const workersPerNode = 2

// Writer insert cadence.
const writeInterval = 50 * time.Millisecond

// Convergence poll interval.
const poll = 100 * time.Millisecond

// Per-insert wire timeout so a kill can never wedge a writer on the lock.
const insertTimeout = 10 * time.Second

// Baseline mesh/ack deadlines.
const baselineTimeout = 20 * time.Second

// In-flight gate before each SIGKILL.
const inflightTimeout = 5 * time.Second

// Rejoin deadlines under load.
const rejoinTimeout = 90 * time.Second

// Row-id stride per (node, worker): ids are
// (node*workersPerNode + worker)*idStride + seq, so the six writers never
// collide.
const idStride = int64(10_000_000)

// Baseline ids live above every writer range.
const baselineIDBase = int64(90_000_001)

// Post-recovery marker id.
const postRecoveryID = int64(90_001_001)

// shared is the shared writer state (same shape as the crash-loop port:
// one mutex around the cluster plus per-node running flags so writers
// never touch a stopped node while the driver kills/restarts it).
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

// insert writes one row plus a durability barrier. Only error-free
// inserts join the acknowledged set: refusals during kill/restart windows
// are dropped and the next unique insert continues the workload.
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

// writer is one application writer (two 50 ms ticker goroutines per node).
func writer(s *shared, node, worker int) {
	for !s.stop.Load() {
		time.Sleep(writeInterval)
		if s.stop.Load() {
			break
		}
		if !s.running[node].Load() {
			continue
		}
		seq := s.seqs[node].Add(1)
		name := fmt.Sprintf("node%d-worker%d-write-%08d", node+1, worker, seq)
		id := int64(node*workersPerNode+worker)*idStride + seq
		if insert(s, node, id, name) != nil {
			time.Sleep(5 * time.Millisecond)
		}
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

// digest returns the canonical table digest (FNV-1a/64 over sorted names
// plus newlines, hex; only cross-node equality within one process is
// asserted).
func digest(s *shared, idx int) (string, error) {
	nodeNames, err := names(s, idx)
	if err != nil {
		return "", err
	}
	hash := uint64(0xcbf29ce484222325)
	for _, name := range nodeNames {
		for i := 0; i < len(name); i++ {
			hash ^= uint64(name[i])
			hash *= 0x100000001b3
		}
		hash ^= uint64('\n')
		hash *= 0x100000001b3
	}
	return fmt.Sprintf("%016x", hash), nil
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

// assertConverged requires all nodes to hold equal digests.
func assertConverged(s *shared) error {
	s.mu.Lock()
	length := s.cluster.Len()
	s.mu.Unlock()
	first := ""
	for i := 0; i < length; i++ {
		nodeDigest, err := digest(s, i)
		if err != nil {
			return err
		}
		if i == 0 {
			first = nodeDigest
		} else if nodeDigest != first {
			return fmt.Errorf("node%d digest %s != %s", i+1, nodeDigest, first)
		}
	}
	return nil
}

// waitForConvergence waits until every node holds equal counts with equal
// digests.
func waitForConvergence(s *shared, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		s.mu.Lock()
		length := s.cluster.Len()
		s.mu.Unlock()
		var expectedCount int
		var expectedDigest string
		hasExpected := false
		ok := true
		for i := 0; i < length; i++ {
			nodeNames, err := names(s, i)
			if err != nil {
				return err
			}
			nodeDigest, err := digest(s, i)
			if err != nil {
				return err
			}
			if !hasExpected {
				expectedCount, expectedDigest, hasExpected = len(nodeNames), nodeDigest, true
			} else if expectedCount != len(nodeNames) || expectedDigest != nodeDigest {
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
				nodeDigest, err := digest(s, i)
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "node%d after recovery rows=%d digest=%s\n", i+1, len(nodeNames), nodeDigest)
			}
			return fmt.Errorf("three nodes did not converge after recovery within %s", timeout)
		}
		time.Sleep(poll)
	}
}

// killDuringWrites SIGKILLs one node with a write provably in flight.
func killDuringWrites(s *shared, node int) error {
	deadline := time.Now().Add(inflightTimeout)
	for s.inFlight[node].Load() == 0 {
		if !time.Now().Before(deadline) {
			return fmt.Errorf("node%d had no write in flight before SIGKILL", node+1)
		}
		time.Sleep(time.Millisecond)
	}
	fmt.Fprintf(os.Stderr, "SIGKILL node%d with %d writes in flight\n", node+1, s.inFlight[node].Load())
	s.running[node].Store(false)
	time.Sleep(150 * time.Millisecond)
	s.mu.Lock()
	s.cluster.KillNode(node)
	s.mu.Unlock()
	return nil
}

// restartNode restarts one node and re-meshes it.
func restartNode(s *shared, node int) error {
	s.mu.Lock()
	err := func() error {
		if err := s.cluster.StartNode(node); err != nil {
			return fmt.Errorf("restart node%d: %w", node+1, err)
		}
		if _, err := s.cluster.Node(node).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			return fmt.Errorf("re-set replicated node%d: %w", node+1, err)
		}
		if _, err := s.cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("flush node%d: %w", node+1, err)
		}
		if err := s.cluster.Mesh(); err != nil {
			return fmt.Errorf("re-mesh after node%d restart: %w", node+1, err)
		}
		_ = s.cluster.SyncAll()
		return nil
	}()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	s.running[node].Store(true)
	return nil
}

func acknowledgedLen(s *shared) int {
	s.ackMu.Lock()
	defer s.ackMu.Unlock()
	return len(s.acknowledged)
}

func runBody(s *shared, writeBurst, settle time.Duration) error {
	s.mu.Lock()
	for i := 0; i < s.cluster.Len(); i++ {
		info := s.cluster.NodeInfo(i)
		pid, ok := s.cluster.NodePID(i)
		fmt.Fprintf(os.Stderr, "%s pid=%v dir=%s repl=%s\n",
			info.Label, pidOrNone(pid, ok), info.Dir, info.ReplAddr)
	}
	s.mu.Unlock()
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

	for node := 0; node < 3; node++ {
		if err := insert(s, node, baselineIDBase+int64(node), fmt.Sprintf("baseline-node%d", node+1)); err != nil {
			return err
		}
	}
	if err := waitForPeers(s, []int64{2, 2, 2}, baselineTimeout); err != nil {
		return err
	}
	if err := waitForConvergence(s, baselineTimeout); err != nil {
		return err
	}
	if err := assertAcknowledgedPresent(s); err != nil {
		return err
	}

	var wg sync.WaitGroup
	for node := 0; node < 3; node++ {
		for worker := 0; worker < workersPerNode; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				writer(s, node, worker)
			}()
		}
	}

	driver := func() error {
		time.Sleep(writeBurst)
		if err := killDuringWrites(s, 1); err != nil {
			return err
		}
		time.Sleep(writeBurst)
		if err := restartNode(s, 1); err != nil {
			return err
		}
		if err := waitForPeers(s, []int64{2, 2, 2}, rejoinTimeout); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "node2 rejoined under continuing application load")

		time.Sleep(writeBurst)
		if err := killDuringWrites(s, 2); err != nil {
			return err
		}
		time.Sleep(writeBurst)
		if err := restartNode(s, 2); err != nil {
			return err
		}
		if err := waitForPeers(s, []int64{2, 2, 2}, rejoinTimeout); err != nil {
			return err
		}
		return nil
	}()
	s.stop.Store(true)
	wg.Wait()
	if driver != nil {
		return driver
	}

	if err := waitForConvergence(s, settle); err != nil {
		return err
	}
	if err := assertAcknowledgedPresent(s); err != nil {
		return err
	}
	if err := assertConverged(s); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "all acknowledged application writes survived two SIGKILL restarts (%d acknowledged rows)\n",
		acknowledgedLen(s))

	if err := insert(s, 0, postRecoveryID, "post-recovery-write"); err != nil {
		return err
	}
	if err := waitForConvergence(s, settle); err != nil {
		return err
	}
	if err := assertAcknowledgedPresent(s); err != nil {
		return err
	}
	if err := assertConverged(s); err != nil {
		return err
	}
	return nil
}

func pidOrNone(pid int, ok bool) any {
	if !ok {
		return "none"
	}
	return pid
}

func TestAbruptProcessDeathRecoversCommittedRows(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	writeBurst := goharness.EnvSeconds("MURMUR_CRASH_WRITE_SECONDS", 8)
	settle := goharness.EnvSeconds("MURMUR_CRASH_SETTLE_SECONDS", 90)

	opts := goharness.DefaultClusterOptions("crash-recovery")
	opts.NumNodes = 3
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 3 {
		t.Fatal("this scenario is written for 3 nodes")
	}
	s := newShared(cluster)
	runErr := runBody(s, writeBurst, settle)
	failed := runErr != nil
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", runErr)
	}
	if failed {
		cluster.DumpForensics("crash-recovery-no-convergence")
	}
	cluster.Finish(failed)
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
}
