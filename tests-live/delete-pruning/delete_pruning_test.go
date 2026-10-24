// Live scenario: concurrent deletes, resurrections, and updates on a
// shared row set converge under aggressive log retention.
//
// Go port of tests/delete_pruning.rs using tests-live/goharness.
// Identical behavior: a retention pass is driven explicitly while
// tombstone batches are live, and afterwards all nodes agree on contents
// plus exact row counts. Tombstone markers stay in state (only origin-log
// batches are collected), so the suite proves delete/resurrect
// convergence plus log collection of the delete batches. The Rust
// tokio::join! over the three phase workers becomes a sync.WaitGroup over
// goroutines.
package deletepruning

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Log retention.
const retention = 1 * time.Second

// Offline-peer memory.
const maximumOffline = 20 * time.Second

// Newest batches per origin the collectors must keep.
const minimumRetained = uint64(10)

// Convergence poll interval.
const poll = 100 * time.Millisecond

// Convergence deadlines (post-prune 30 s).
const convergeTimeout = 60 * time.Second
const postPruneTimeout = 30 * time.Second

// Collection-pass deadline (the Rust port drives MURMUR RETENTION COLLECT
// explicitly instead of waiting on the GC ticker).
const collectTimeout = 120 * time.Second

// Collection poll interval.
const collectPoll = 2 * time.Second

// Seed flush stride: one replication batch per stride (the product mints
// one log entry per flush batch, not per row).
const seedFlushStride = 25

// envInt is a positive-integer env knob with fallback.
func envInt(name string, fallback uint64) uint64 {
	if v, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil && v > 0 {
		return v
	}
	return fallback
}

// rowID returns the row identity (UUID-shaped hex).
func rowID(n int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", n, n)
}

// insert inserts one row; flush is the caller's choice so seeds can batch
// while phases mint one batch per op.
func insert(cluster *goharness.Cluster, node int, id, name string) error {
	if _, err := cluster.Node(node).Execute("INSERT INTO rows(id, name) VALUES($1, $2)", id, name); err != nil {
		return fmt.Errorf("node%d insert %q: %w", node+1, id, err)
	}
	return nil
}

// update renames one row.
func update(cluster *goharness.Cluster, node int, id, name string) error {
	if _, err := cluster.Node(node).Execute("UPDATE rows SET name = $1 WHERE id = $2", name, id); err != nil {
		return fmt.Errorf("node%d update %q: %w", node+1, id, err)
	}
	return nil
}

// deleteRow deletes one row.
func deleteRow(cluster *goharness.Cluster, node int, id string) error {
	if _, err := cluster.Node(node).Execute("DELETE FROM rows WHERE id = $1", id); err != nil {
		return fmt.Errorf("node%d delete %q: %w", node+1, id, err)
	}
	return nil
}

func flush(cluster *goharness.Cluster, node int) error {
	if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush: %w", node+1, err)
	}
	return nil
}

// idName is one (id, name) pair.
type idName struct {
	id   string
	name string
}

// allRows returns all (id, name) pairs on one node, ordered by id.
func allRows(cluster *goharness.Cluster, idx int) ([]idName, error) {
	rows, err := cluster.Node(idx).Query("SELECT id, name FROM rows ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("node%d SELECT rows: %w", idx+1, err)
	}
	out := make([]idName, 0, len(rows))
	for _, r := range rows {
		id, err := r.String(0)
		if err != nil {
			return nil, fmt.Errorf("node%d decode id: %w", idx+1, err)
		}
		name, err := r.String(1)
		if err != nil {
			return nil, fmt.Errorf("node%d decode name: %w", idx+1, err)
		}
		out = append(out, idName{id: id, name: name})
	}
	return out, nil
}

// digest returns the canonical digest over ordered rows.
func digest(rows []idName) string {
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		parts = append(parts, r.id+"\x00"+r.name)
	}
	return strings.Join(parts, "\n")
}

// waitConverged waits until every node holds exactly want rows with equal
// digests.
func waitConverged(cluster *goharness.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var first *string
		ok := true
		for i := 0; i < cluster.Len(); i++ {
			rows, err := allRows(cluster, i)
			if err != nil {
				return err
			}
			if len(rows) != want {
				ok = false
				break
			}
			nodeDigest := digest(rows)
			if first == nil {
				first = &nodeDigest
			} else if *first != nodeDigest {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		if !time.Now().Before(deadline) {
			for i := 0; i < cluster.Len(); i++ {
				rows, err := allRows(cluster, i)
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "node%d at timeout: count=%d\n", i+1, len(rows))
			}
			return fmt.Errorf("nodes did not converge on %d rows with equal digests within %s", want, timeout)
		}
		time.Sleep(poll)
	}
}

// retentionCounters returns the node-global retention counters on one
// node: (last-collection marker, collected_transactions) from MURMUR SHOW
// RETENTION (columns 6 and 5, in the Rust field order).
func retentionCounters(cluster *goharness.Cluster, idx int) (uint64, uint64, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW RETENTION")
	if err != nil {
		return 0, 0, fmt.Errorf("node%d SHOW RETENTION: %w", idx+1, err)
	}
	if len(rows) == 0 {
		return 0, 0, nil
	}
	first := rows[0]
	collected := ""
	if len(first) > 5 {
		collected = first[5]
	}
	last := ""
	if len(first) > 6 {
		last = first[6]
	}
	lastN, _ := strconv.ParseUint(last, 10, 64)
	collectedN, _ := strconv.ParseUint(collected, 10, 64)
	return lastN, collectedN, nil
}

// readName returns the name of one row, or false when the row is absent.
func readName(cluster *goharness.Cluster, idx int, id string) (string, bool, error) {
	rows, err := cluster.Node(idx).Query("SELECT name FROM rows WHERE id = $1", id)
	if err != nil {
		return "", false, fmt.Errorf("node%d read %q: %w", idx+1, id, err)
	}
	if len(rows) == 0 {
		return "", false, nil
	}
	name, err := rows[0].String(0)
	if err != nil {
		return "", false, fmt.Errorf("node%d decode name: %w", idx+1, err)
	}
	return name, true, nil
}

// phaseDelete deletes a range from one node (one flush per op so every
// delete becomes its own collectable log batch).
func phaseDelete(cluster *goharness.Cluster, node, from, to int) error {
	for i := from; i < to; i++ {
		if err := deleteRow(cluster, node, rowID(i)); err != nil {
			return err
		}
		if err := flush(cluster, node); err != nil {
			return err
		}
	}
	return nil
}

// phaseUpdate renames a range from node 1.
func phaseUpdate(cluster *goharness.Cluster, from, to int) error {
	for i := from; i < to; i++ {
		id := rowID(i)
		if err := update(cluster, 1, id, fmt.Sprintf("upd-%d", i)); err != nil {
			return err
		}
		if err := flush(cluster, 1); err != nil {
			return err
		}
	}
	return nil
}

func runBody(cluster *goharness.Cluster, keys int) error {
	third := keys / 3
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			return fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE rows REPLICATED true"); err != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		if err := flush(cluster, i); err != nil {
			return err
		}
	}

	// Seed the shared row set on node 0 and converge.
	for i := 0; i < keys; i++ {
		id := rowID(i)
		if err := insert(cluster, 0, id, fmt.Sprintf("seed-%d", i)); err != nil {
			return err
		}
		if i%seedFlushStride == seedFlushStride-1 {
			if err := flush(cluster, 0); err != nil {
				return err
			}
		}
	}
	if err := flush(cluster, 0); err != nil {
		return err
	}
	if err := waitConverged(cluster, keys, convergeTimeout); err != nil {
		return err
	}

	runsBefore, collectedBefore, err := retentionCounters(cluster, 0)
	if err != nil {
		return err
	}

	// Phase 1, concurrent on disjoint thirds: node 0 deletes the first
	// third, node 1 updates the middle third, node 2 deletes the last
	// third. Only the middle third survives.
	var wg sync.WaitGroup
	errs := make([]error, 3)
	wg.Add(3)
	go func() {
		defer wg.Done()
		errs[0] = phaseDelete(cluster, 0, 0, third)
	}()
	go func() {
		defer wg.Done()
		errs[1] = phaseUpdate(cluster, third, 2*third)
	}()
	go func() {
		defer wg.Done()
		errs[2] = phaseDelete(cluster, 2, 2*third, keys)
	}()
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	// Anti-vacuity: the deletes must actually remove rows everywhere.
	if err := waitConverged(cluster, third, convergeTimeout); err != nil {
		return err
	}
	spot, spotOK, err := readName(cluster, 0, rowID(third))
	if err != nil {
		return err
	}
	if !spotOK || spot != fmt.Sprintf("upd-%d", third) {
		spotDesc := "<absent>"
		if spotOK {
			spotDesc = fmt.Sprintf("%q", spot)
		}
		return fmt.Errorf("middle-third value = %s; want upd-%d (concurrent update lost)", spotDesc, third)
	}

	// Phase 2: resurrect the first third with new values from node 1.
	for i := 0; i < third; i++ {
		id := rowID(i)
		if err := insert(cluster, 1, id, fmt.Sprintf("res-%d", i)); err != nil {
			return err
		}
		if err := flush(cluster, 1); err != nil {
			return err
		}
	}
	if err := waitConverged(cluster, 2*third, convergeTimeout); err != nil {
		return err
	}

	// Phase 3: delete the middle third from node 0. Survivors: the
	// resurrected first third only.
	for i := third; i < 2*third; i++ {
		if err := deleteRow(cluster, 0, rowID(i)); err != nil {
			return err
		}
		if err := flush(cluster, 0); err != nil {
			return err
		}
	}
	if err := waitConverged(cluster, third, convergeTimeout); err != nil {
		return err
	}

	// Drive collection passes over the tombstone batches until the run
	// counter advances and batches are collected (MURMUR RETENTION
	// COLLECT runs a bounded pass on demand).
	deadline := time.Now().Add(collectTimeout)
	var runsAfter, collectedAfter uint64
	for {
		if _, err := cluster.Node(0).Simple("MURMUR RETENTION COLLECT"); err != nil {
			return fmt.Errorf("node1 RETENTION COLLECT: %w", err)
		}
		runsAfter, collectedAfter, err = retentionCounters(cluster, 0)
		if err != nil {
			return err
		}
		if runsAfter > runsBefore && collectedAfter > collectedBefore {
			break
		}
		if !time.Now().Before(deadline) {
			break
		}
		time.Sleep(collectPoll)
	}
	fmt.Fprintf(os.Stderr, "retention: runs %d -> %d, log batches collected during test: %d\n",
		runsBefore, runsAfter, collectedAfter-collectedBefore)
	if runsAfter <= runsBefore {
		return fmt.Errorf("no retention pass ran (last-collection %d -> %d)", runsBefore, runsAfter)
	}
	if collectedAfter <= collectedBefore {
		return fmt.Errorf("no log batches collected (before=%d after=%d); retention did not prune delete batches",
			collectedBefore, collectedAfter)
	}

	// Identical ordered digests and exact row counts on all nodes.
	for i := 0; i < cluster.Len(); i++ {
		rows, err := allRows(cluster, i)
		if err != nil {
			return err
		}
		if len(rows) != third {
			return fmt.Errorf("node%d count = %d, want %d", i+1, len(rows), third)
		}
	}
	wantRows, err := allRows(cluster, 0)
	if err != nil {
		return err
	}
	wantDigest := digest(wantRows)
	for i := 1; i < cluster.Len(); i++ {
		nodeRows, err := allRows(cluster, i)
		if err != nil {
			return err
		}
		if digest(nodeRows) != wantDigest {
			return fmt.Errorf("node%d digest differs from node1", i+1)
		}
	}
	// Content proof: resurrected rows present with new values, deleted
	// rows absent, on every node.
	for i := 0; i < cluster.Len(); i++ {
		got, ok, err := readName(cluster, i, rowID(0))
		if err != nil {
			return err
		}
		if !ok || got != "res-0" {
			gotDesc := "<absent>"
			if ok {
				gotDesc = fmt.Sprintf("%q", got)
			}
			return fmt.Errorf("node%d resurrected value = %s, want \"res-0\"", i+1, gotDesc)
		}
		absent, ok, err := readName(cluster, i, rowID(third))
		if err != nil {
			return err
		}
		if ok {
			return fmt.Errorf("node%d deleted row still present: %q", i+1, absent)
		}
	}

	// A post-prune write replicates everywhere.
	postID := rowID(keys + 1)
	if err := insert(cluster, 2, postID, "post-prune"); err != nil {
		return err
	}
	if err := flush(cluster, 2); err != nil {
		return err
	}
	if err := waitConverged(cluster, third+1, postPruneTimeout); err != nil {
		return err
	}
	return nil
}

func durationPtr(d time.Duration) *time.Duration { return &d }

func uint64Ptr(v uint64) *uint64 { return &v }

func TestConcurrentDeletesResurrectionsAndUpdatesConverge(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	keys := int(envInt("MURMUR_DELETE_PRUNING_KEYS", 300))
	if keys%3 != 0 {
		t.Fatalf("MURMUR_DELETE_PRUNING_KEYS=%d must be a multiple of 3", keys)
	}

	opts := goharness.DefaultClusterOptions("delete-pruning")
	opts.NumNodes = 3
	opts.Replication = &goharness.ReplicationOptions{
		Retention:       durationPtr(retention),
		MinimumRetained: uint64Ptr(minimumRetained),
		MaximumOffline:  durationPtr(maximumOffline),
	}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 3 {
		t.Fatal("this scenario is written for 3 nodes")
	}
	runErr := runBody(cluster, keys)
	failed := runErr != nil
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", runErr)
	}
	if failed {
		cluster.DumpForensics("delete-pruning-no-convergence")
	}
	cluster.Finish(failed)
	if failed {
		t.Fatal("scenario failed; artifacts retained under tests-live/failures/")
	}
}
