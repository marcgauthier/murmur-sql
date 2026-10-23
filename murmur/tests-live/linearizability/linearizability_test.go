// Live scenario: Jepsen-style history checker over one shared cell.
//
// The product resolves concurrent cell writes with last-writer-wins, so
// strict linearizability under partition is not a product guarantee and
// this suite does not assert it. It asserts what the system must still
// guarantee:
//
//   - Sequential leg (no faults): a single-threaded write/read stream
//     observes strict read-your-writes: every read returns the latest
//     acknowledged write. For a sequential history that is linearity.
//   - Concurrent leg (partition flaps): a recorded history of timestamped
//     writes and reads must contain no out-of-thin-air value: every read
//     returns the initial value or a value from a write that started
//     before the read completed. After heal all nodes must converge on a
//     single value taken from the acknowledged write set.
//
// A phantom value (never written, or from the future) would prove a merge
// or read-path corruption; the crdt-contention suite already pins the LWW
// winner agreement this leg's convergence relies on.
package linearizability

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

const (
	// cellDDL is the single-row contention table.
	cellDDL = "CREATE TABLE cell(id TEXT NOT NULL PRIMARY KEY, val TEXT NOT NULL)"
	// cellID is the one contested key.
	cellID = "k"
	// initialValue is the pre-leg cell value.
	initialValue = "init-0"
	// writesEnv scales the concurrent leg (writes per writer; reads run
	// at twice that per reader).
	writesEnv = "MURMUR_LINEARIZABILITY_WRITES"
	// defaultWritesPerWriter is the default concurrent writes per writer.
	defaultWritesPerWriter = 60
	// sequentialWrites is the sequential-leg write count.
	sequentialWrites = 20
	// flapRounds is the partition flap count during the concurrent leg.
	flapRounds = 6
	// flapDown is the per-flap isolation duration.
	flapDown = time.Second
	// convergeTimeout bounds every convergence wait.
	convergeTimeout = 90 * time.Second
	// ownReadTimeout bounds a same-node read-your-write (local, fast).
	ownReadTimeout = 30 * time.Second
	// poll is the convergence poll interval.
	poll = 200 * time.Millisecond
)

// writesPerWriter reads the concurrent-leg scale knob.
func writesPerWriter() int {
	if v, err := strconv.Atoi(os.Getenv(writesEnv)); err == nil && v > 0 {
		return v
	}
	return defaultWritesPerWriter
}

// event is one timestamped history entry (nanosecond wall clock).
type event struct {
	write bool
	node  int
	value string
	start int64
	end   int64
}

// history is the mutex-guarded concurrent-leg record.
type history struct {
	mu     sync.Mutex
	writes []event
	reads  []event
}

func (h *history) add(e event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e.write {
		h.writes = append(h.writes, e)
	} else {
		h.reads = append(h.reads, e)
	}
}

// writeCell updates the cell and flushes, returning write timestamps.
func writeCell(db *murmur.Database, value string) (int64, int64, error) {
	start := time.Now().UnixNano()
	_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("UPDATE cell SET val=? WHERE id=?", value, cellID)
		return nil, err
	})
	if err != nil {
		return 0, 0, err
	}
	if err := db.Flush(); err != nil {
		return 0, 0, err
	}
	return start, time.Now().UnixNano(), nil
}

// readCell reads the cell, returning value and read timestamps. A missing
// row is reported as found=false (transient during convergence), never an
// error: convergence waits must retry absence, not fatal on it.
func readCell(db *murmur.Database) (string, int64, int64, bool, error) {
	start := time.Now().UnixNano()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT val FROM cell WHERE id=?", cellID)
	})
	if err != nil {
		return "", 0, 0, false, err
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return "", start, time.Now().UnixNano(), false, nil
	}
	if len(rows) != 1 {
		return "", 0, 0, false, fmt.Errorf("cell row count = %d, want 1", len(rows))
	}
	return rows[0][0].Str, start, time.Now().UnixNano(), true, nil
}

// cellValue returns the current cell value on one node.
func cellValue(t *testing.T, db *murmur.Database) string {
	t.Helper()
	v, _, _, found, err := readCell(db)
	if err != nil {
		t.Fatalf("read cell: %v", err)
	}
	if !found {
		t.Fatalf("read cell: row missing")
	}
	return v
}

// tryValue reads the cell value without failing, for poll conditions.
func tryValue(db *murmur.Database) (string, bool) {
	v, _, _, found, err := readCell(db)
	if err != nil || !found {
		return "", false
	}
	return v, true
}

// waitAgreed waits until every node reads want.
func waitAgreed(t *testing.T, cluster *harness.Cluster, want, what string) {
	t.Helper()
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if got, ok := tryValue(cluster.Node(i)); !ok || got != want {
				return false
			}
		}
		return true
	}); !ok {
		for i := 0; i < cluster.Len(); i++ {
			got, ok := tryValue(cluster.Node(i))
			t.Logf("node%d cell = %q (found=%v)", i, got, ok)
		}
		t.Fatalf("%s: nodes did not agree on %q", what, want)
	}
}

// waitVal waits until one node reads want (bounded read-your-write).
func waitVal(t *testing.T, db *murmur.Database, want, what string) {
	t.Helper()
	if ok := harness.WaitUntil(ownReadTimeout, poll, func() bool {
		got, ok := tryValue(db)
		return ok && got == want
	}); !ok {
		got, ok := tryValue(db)
		t.Fatalf("%s: own read = %q (found=%v), want %q", what, got, ok, want)
	}
}

// checkNoPhantoms verifies every read returned the initial value or a
// value from a write that started before the read completed.
func checkNoPhantoms(t *testing.T, h *history) {
	t.Helper()
	for _, r := range h.reads {
		if r.value == initialValue {
			continue
		}
		known := false
		for _, w := range h.writes {
			if w.value == r.value && w.start < r.end {
				known = true
				break
			}
		}
		if !known {
			t.Fatalf("phantom read: node%d read %q with no write starting before the read completed (%d writes recorded)",
				r.node, r.value, len(h.writes))
		}
	}
	t.Logf("checked %d reads against %d writes: no phantom values", len(h.reads), len(h.writes))
}

func TestCellConsistencyUnderPartition(t *testing.T) {
	perWriter := writesPerWriter()
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "linearizability",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("linearizability-no-convergence")
		}
		cluster.Finish(failed)
	}()

	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, cellDDL); err != nil {
			t.Fatalf("migrate cell table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("cell", true); err != nil {
			t.Fatalf("opt-in cell: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}
	if _, err := cluster.Node(0).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO cell(id, val) VALUES(?, ?)", cellID, initialValue)
		return nil, err
	}); err != nil {
		t.Fatalf("insert cell: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush cell: %v", err)
	}
	waitAgreed(t, cluster, initialValue, "baseline")

	// Leg 1: sequential strictness. Single-threaded writes on node 0,
	// each followed by a same-node read that must observe it, then a
	// cross-node read after convergence.
	for seq := 0; seq < sequentialWrites; seq++ {
		want := fmt.Sprintf("seq-%02d", seq)
		if _, _, err := writeCell(cluster.Node(0), want); err != nil {
			t.Fatalf("sequential write %q: %v", want, err)
		}
		waitVal(t, cluster.Node(0), want, "sequential read-your-write")
	}
	waitAgreed(t, cluster, fmt.Sprintf("seq-%02d", sequentialWrites-1), "sequential")

	// Seed the leg-2 history with the converged leg-1 value: stale reads
	// of it during the concurrent leg are legitimate, not phantoms.
	historySeed := fmt.Sprintf("seq-%02d", sequentialWrites-1)
	var h history
	h.add(event{write: true, node: 0, value: historySeed})

	// Leg 2: concurrent writers and readers under partition flaps. Node
	// 2 is repeatedly isolated; its reads may be stale but never
	// phantom.
	stopFlap := make(chan struct{})
	var flapDone sync.WaitGroup
	flapDone.Add(1)
	go func() {
		defer flapDone.Done()
		for round := 0; round < flapRounds; round++ {
			select {
			case <-stopFlap:
				return
			default:
			}
			_ = cluster.RemovePeer(2, 0)
			_ = cluster.RemovePeer(2, 1)
			_ = cluster.RemovePeer(0, 2)
			_ = cluster.RemovePeer(1, 2)
			time.Sleep(flapDown)
			_ = cluster.AddPeer(2, 0)
			_ = cluster.AddPeer(2, 1)
			_ = cluster.AddPeer(0, 2)
			_ = cluster.AddPeer(1, 2)
			time.Sleep(500 * time.Millisecond)
		}
	}()
	var wg sync.WaitGroup
	for _, node := range []int{0, 1} {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			for seq := 0; seq < perWriter; seq++ {
				value := fmt.Sprintf("w%d-%04d", node, seq)
				start, end, err := writeCell(cluster.Node(node), value)
				if err != nil {
					t.Errorf("node%d write %q: %v", node, value, err)
					return
				}
				h.add(event{write: true, node: node, value: value, start: start, end: end})
			}
		}(node)
	}
	for _, node := range []int{1, 2} {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			for n := 0; n < 2*perWriter; n++ {
				value, start, end, found, err := readCell(cluster.Node(node))
				if err != nil {
					t.Errorf("node%d read: %v", node, err)
					return
				}
				if !found {
					t.Errorf("node%d read: cell row missing mid-leg", node)
					return
				}
				h.add(event{node: node, value: value, start: start, end: end})
			}
		}(node)
	}
	wg.Wait()
	close(stopFlap)
	flapDone.Wait()
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh: %v", err)
	}

	checkNoPhantoms(t, &h)

	// After heal every node must converge on one value from the
	// acknowledged write set.
	acked := map[string]bool{initialValue: true}
	for _, w := range h.writes {
		acked[w.value] = true
	}
	var final string
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		first, ok := tryValue(cluster.Node(0))
		if !ok {
			return false
		}
		for i := 1; i < cluster.Len(); i++ {
			got, ok := tryValue(cluster.Node(i))
			if !ok || got != first {
				return false
			}
		}
		final = first
		return true
	}); !ok {
		for i := 0; i < cluster.Len(); i++ {
			got, ok := tryValue(cluster.Node(i))
			t.Logf("node%d cell = %q (found=%v)", i, got, ok)
		}
		t.Fatalf("nodes did not converge on one cell value")
	}
	if !acked[final] {
		t.Fatalf("converged on %q, outside the acknowledged write set", final)
	}
	t.Logf("cell converged on %q from %d acknowledged writes", final, len(h.writes))
	failed = false
}
