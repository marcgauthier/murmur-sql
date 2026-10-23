// Live scenario: online backups under sustained write load.
//
// Go port of the Rust `backup-under-fire` scenario
// (/home/marc/MURMUR/tests-live/backup-under-fire/tests/backup_under_fire.rs).
// TestBackupUnderFire: a 2-node mesh absorbs sustained writes (25 ms ticker
// per node) for MURMUR_BACKUP_UNDER_FIRE_WRITE_SECONDS (default 30) with zero
// failed writes. A third member ("agent A", cluster node 3, unmeshed at
// boot) joins mid-load, converges, and takes two online backups (b1, b2)
// from its live handle without stopping. A truncated backup remnant must be
// rejected by verify and restore, and the failed restore must publish no
// target dir (the in-process equivalent of the SIGKILL-mid-backup leg; see
// README.md). After quiescence a fresh out-of-band agent ("agent B": new
// data dir, new node identity, own TLS leaf) joins, converges on the
// quiesced total, takes the final online backup (b4), and closes cleanly.
// Every completed backup restores: b1/b2 are non-empty, non-decreasing, and
// subsets of the survivors; b4 matches the survivors exactly (count plus
// canonical ordered names).
//
// Plain SQL replaces the Go typed-records mesh API of the original; the
// backup agent subprocess becomes an in-process third member plus a fresh
// out-of-band murmur.Database open. See README.md.
package backupunderfire

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"net/netip"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// tableDDL is the mesh-leg DDL: plain replicated rows. Each write inserts a
// fresh unique row, so typed merge policies never trigger; a plain table is
// behaviorally identical here.
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)"

const (
	// writeTick is the per-writer cadence (Rust: 25 ms ticker on each of
	// the 2 mesh nodes).
	writeTick = 25 * time.Millisecond
	poll      = 200 * time.Millisecond
	// convergeTimeout is the agent join/converge deadline (Rust: 120 s).
	convergeTimeout = 120 * time.Second
	// quiesceTimeout is the post-quiesce mesh convergence deadline
	// (Rust: 90 s).
	quiesceTimeout = 90 * time.Second
)

// migrateAll migrates every node with ddl (Rust: shared Config.Tables
// definition).
func migrateAll(t *testing.T, cluster *harness.Cluster, ddl string) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, ddl); err != nil {
			t.Fatalf("node%d migrate %q failed: %v", i+1, ddl, err)
		}
	}
	// Opt in after every node migrated: opting in publishes the schema
	// immediately, and an early publication lets a peer adopt the table
	// before its own migrate runs ("table already exists").
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("live_typed_records", true); err != nil {
			t.Fatalf("node%d opt-in failed: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush after opt-in failed: %v", i+1, err)
		}
	}
}

// tryInsert is one writer attempt (Rust: TypedInsert). A failed write does
// not stop the writer.
func tryInsert(db *murmur.Database, name string) error {
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name) VALUES(?)", name)
		return nil, err
	}); err != nil {
		return err
	}
	return db.Flush()
}

// typedNames returns the canonical ordered names on one node (Rust:
// TypedNames).
func typedNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM live_typed_records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read typed names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

func countRows(t *testing.T, db *murmur.Database) int {
	t.Helper()
	return len(typedNames(t, db))
}

// waitConverged waits until every listed node holds the same want names
// (Rust: waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, nodes []int, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		var first []string
		for _, i := range nodes {
			got := typedNames(t, cluster.Node(i))
			if len(got) != want {
				return false
			}
			if first == nil {
				first = got
				continue
			}
			if len(got) != len(first) {
				return false
			}
			for j := range got {
				if got[j] != first[j] {
					return false
				}
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes %v did not converge on %d rows within %v", nodes, want, timeout)
	}
}

// copyDirRecursive copies a directory tree (remnant construction).
func copyDirRecursive(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatalf("create %s failed: %v", dst, err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("read %s failed: %v", src, err)
	}
	for _, entry := range entries {
		target := filepath.Join(dst, entry.Name())
		if entry.IsDir() {
			copyDirRecursive(t, filepath.Join(src, entry.Name()), target)
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, entry.Name()))
		if err != nil {
			t.Fatalf("read %s failed: %v", entry.Name(), err)
		}
		if err := os.WriteFile(target, data, 0o644); err != nil {
			t.Fatalf("copy %s failed: %v", target, err)
		}
	}
}

// openAgent opens a fresh out-of-band agent database: new data dir, fresh
// node identity, freshly issued TLS leaf, cluster CA trust, both survivors
// as bootstrap + trusted snapshot sources. The agent never originates rows,
// so peers need not trust its origin key.
func openAgent(t *testing.T, cluster *harness.Cluster, key [32]byte, dataDir string, fresh [16]byte, seed [32]byte) *murmur.Database {
	t.Helper()
	issued, err := cluster.IssueCert(fresh)
	if err != nil {
		t.Fatalf("issue agent cert: %v", err)
	}
	trusted := make(map[[16]byte][32]byte)
	// All cluster members: agent A (node 2) holds no rows but the agent
	// must still verify anything forwarded under its origin.
	for i := 0; i < 3; i++ {
		trusted[cluster.NodeID(i)] = cluster.NodeOriginPubkey(i)
	}
	trusted[fresh] = replication.PublicKey(&seed)
	config := replication.NewConfig(netip.MustParseAddrPort("127.0.0.1:0"), issued.Credentials, seed)
	config.TrustedOrigins = trusted
	config.TrustedSnapshotSources = map[[16]byte]bool{cluster.NodeID(0): true, cluster.NodeID(1): true}
	config.Bootstrap = []netip.AddrPort{cluster.NodeInfo(0).ReplAddr, cluster.NodeInfo(1).ReplAddr}
	dbid := cluster.DatabaseID()
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.OpenCreateNew
	opts.DatabaseID = &dbid
	nodeID := fresh
	opts.NodeID = &nodeID
	opts.Replication = config
	db, err := harness.OpenWithWatchdog(dataDir, key[:], opts)
	if err != nil {
		t.Fatalf("open agent database: %v", err)
	}
	if err := db.Migrate(murmur.NewMigration(tableDDL)); err != nil {
		t.Fatalf("agent migrate: %v", err)
	}
	if err := db.SetReplication("live_typed_records", true); err != nil {
		t.Fatalf("opt-in live_typed_records: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("agent flush schema: %v", err)
	}
	return db
}

// restoreNames restores backupDir under a fresh writer identity and returns
// the restored canonical names.
func restoreNames(t *testing.T, backupDir, target string, key [32]byte, databaseID [16]byte) []string {
	t.Helper()
	var fresh [16]byte
	if _, err := rand.Read(fresh[:]); err != nil {
		t.Fatalf("fresh node id entropy: %v", err)
	}
	if err := murmur.RestoreWithOptions(backupDir, target, key[:], murmur.RestoreOptions{NewNodeID: &fresh}); err != nil {
		t.Fatalf("restore %s failed: %v", backupDir, err)
	}
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.OpenExisting
	opts.DatabaseID = &databaseID
	nodeID := fresh
	opts.NodeID = &nodeID
	db, err := murmur.Open(target, key[:], opts)
	if err != nil {
		t.Fatalf("offline open of restored backup: %v", err)
	}
	names := typedNames(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close restored handle: %v", err)
	}
	return names
}

func TestBackupUnderFire(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("key entropy: %v", err)
	}
	// Explicit ids so the late-joining agent slot can trust snapshots from
	// the two mesh nodes.
	nodeIDs := make([][16]byte, 3)
	for i := range nodeIDs {
		if _, err := rand.Read(nodeIDs[i][:]); err != nil {
			t.Fatalf("node id entropy: %v", err)
		}
	}
	// Agent B joins later under a fresh identity; the survivors must trust
	// its origin key up front (trust is static config), or they close every
	// session on first verify and the join trickles through reconnects.
	var agentID [16]byte
	var agentSeed [32]byte
	if _, err := rand.Read(agentID[:]); err != nil {
		t.Fatalf("agent id entropy: %v", err)
	}
	if _, err := rand.Read(agentSeed[:]); err != nil {
		t.Fatalf("agent seed entropy: %v", err)
	}
	agentPubkey := replication.PublicKey(&agentSeed)
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        "backup-under-fire",
		NumNodes:    3,
		NodeIDs:     nodeIDs,
		Key:         key,
		ManualPeers: true,
		TrustedSnapshotSources: map[int]map[[16]byte]bool{
			2: {nodeIDs[0]: true, nodeIDs[1]: true},
		},
		ExtraTrustedOrigins: map[int]map[[16]byte][32]byte{
			0: {agentID: agentPubkey},
			1: {agentID: agentPubkey},
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("backup-under-fire")
		}
		cluster.Finish(failed)
	}()

	writeDur := harness.EnvSeconds("MURMUR_BACKUP_UNDER_FIRE_WRITE_SECONDS", 30)
	backups := filepath.Join(cluster.RuntimeDir(), "backups")
	if err := os.MkdirAll(backups, 0o755); err != nil {
		t.Fatalf("backups parent dir: %v", err)
	}

	// Two-node mesh first; node 3 ("agent A") runs unmeshed until it joins
	// mid-load below.
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("mesh 0->1: %v", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatalf("mesh 1->0: %v", err)
	}
	migrateAll(t, cluster, tableDDL)

	var stop atomic.Bool
	var okCount atomic.Int64
	var failedWrites atomic.Int64
	b1 := filepath.Join(backups, "b1")
	b2 := filepath.Join(backups, "b2")
	loadStart := time.Now()
	// Backstop so writers always terminate (even if the main leg fails
	// before setting stop).
	writerDeadline := loadStart.Add(writeDur).Add(120 * time.Second)

	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Sustained writes for the whole load phase.
			db := cluster.Node(w)
			var seq uint64
			for {
				if stop.Load() || !time.Now().Before(writerDeadline) {
					return
				}
				time.Sleep(writeTick)
				if stop.Load() {
					return
				}
				seq++
				if err := tryInsert(db, fmt.Sprintf("w%d-%d", w, seq)); err != nil {
					t.Logf("writer %d seq %d failed: %v", w, seq, err)
					failedWrites.Add(1)
				} else {
					okCount.Add(1)
				}
			}
		}(w)
	}

	// Agent run A: join mid-load, converge, then take two fast online
	// backups from the live handle — zero downtime, the node never stops.
	if err := cluster.Mesh(); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("agent A join mesh: %v", err)
	}
	target := countRows(t, cluster.Node(0))
	if target < 100 {
		target = 100
	}
	joined := harness.WaitUntil(convergeTimeout, poll, func() bool {
		return countRows(t, cluster.Node(2)) >= target
	})
	if !joined {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("agent A did not converge on %d rows mid-load within %v", target, convergeTimeout)
	}
	t.Logf("agent A converged mid-load: %d rows (target %d)", countRows(t, cluster.Node(2)), target)
	if err := cluster.Node(2).Backup(b1); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("online backup b1: %v", err)
	}
	if err := cluster.Node(2).Backup(b2); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("online backup b2: %v", err)
	}
	if err := murmur.VerifyBackup(b1, key[:]); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("verify b1: %v", err)
	}
	if err := murmur.VerifyBackup(b2, key[:]); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("verify b2: %v", err)
	}
	t.Log("online backups mid-load: b1 b2 verified")

	// Interrupted-backup equivalence (throttled backup SIGKILLed
	// mid-stream): a truncated remnant — stream cut before the manifest
	// completed — must fail closed, and the failed restore must publish no
	// target dir (no intent file).
	remnant := filepath.Join(cluster.RuntimeDir(), "remnant")
	copyDirRecursive(t, b1, remnant)
	if err := os.Remove(filepath.Join(remnant, "manifest")); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("cut remnant manifest: %v", err)
	}
	if err := murmur.VerifyBackup(remnant, key[:]); err != nil {
		t.Logf("truncated remnant rejected by verify: %v", err)
	} else {
		stop.Store(true)
		wg.Wait()
		t.Fatal("truncated remnant verified, want rejection")
	}
	remnantTarget := filepath.Join(cluster.RuntimeDir(), "remnant-restore")
	var remnantFresh [16]byte
	if _, err := rand.Read(remnantFresh[:]); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("remnant id entropy: %v", err)
	}
	if err := murmur.RestoreWithOptions(remnant, remnantTarget, key[:], murmur.RestoreOptions{NewNodeID: &remnantFresh}); err != nil {
		t.Logf("truncated remnant rejected by restore: %v", err)
	} else {
		stop.Store(true)
		wg.Wait()
		t.Fatal("truncated remnant restored, want rejection")
	}
	if _, err := os.Stat(remnantTarget); !os.IsNotExist(err) {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("failed restore published %s", remnantTarget)
	}

	// Keep the load up for the configured duration, then quiesce.
	if elapsed := time.Since(loadStart); elapsed < writeDur {
		time.Sleep(writeDur - elapsed)
	}
	stop.Store(true)
	wg.Wait()

	failedTotal := failedWrites.Load()
	total := int(okCount.Load())
	if failedTotal != 0 {
		t.Fatalf("%d writes failed during backup load, want zero-downtime", failedTotal)
	}
	t.Logf("load phase: %d writes, zero failures", total)
	// Agent A leaves (it never originated rows, so it holds no unique
	// data); the survivors must converge on every write.
	cluster.StopNode(2)
	waitConverged(t, cluster, []int{0, 1}, total, quiesceTimeout)
	survivors := typedNames(t, cluster.Node(0))

	// Agent run B: rejoin fresh, converge on the quiesced total, take the
	// final online backup, and exit cleanly.
	agentRoot := filepath.Join(cluster.RuntimeDir(), "agent-b")
	if err := os.MkdirAll(agentRoot, 0o755); err != nil {
		t.Fatalf("agent-b root: %v", err)
	}
	agent := openAgent(t, cluster, key, filepath.Join(agentRoot, "data"), agentID, agentSeed)
	agentHandle := agent.Replication()
	if agentHandle == nil {
		t.Fatal("agent replication handle")
	}
	var agentAddr netip.AddrPort
	end := time.Now().Add(10 * time.Second)
	for {
		if addr := agentHandle.Status().Address; addr != nil {
			agentAddr = *addr
			break
		}
		if !time.Now().Before(end) {
			t.Fatal("agent endpoint did not come up within 10s")
		}
		time.Sleep(poll)
	}
	for i := 0; i < 2; i++ {
		h := cluster.Node(i).Replication()
		if h == nil {
			t.Fatal("node replication handle")
		}
		if err := h.AddPeer(agentAddr); err != nil {
			t.Fatalf("dial agent: %v", err)
		}
		if err := agentHandle.AddPeer(cluster.NodeInfo(i).ReplAddr); err != nil {
			t.Fatalf("agent dial seed: %v", err)
		}
	}
	bJoined := harness.WaitUntil(convergeTimeout, poll, func() bool {
		return countRows(t, agent) >= total
	})
	// Agent B is external to the cluster, so its state never appears in
	// cluster forensics; one compact line keeps join failures debuggable.
	st := agentHandle.Status()
	lastErr := "<nil>"
	if st.LastError != nil {
		lastErr = *st.LastError
	}
	t.Logf("agent B after join wait: rows=%d/%d applied=%d staged=%d recv=%d peers=%d err=%s",
		countRows(t, agent), total, st.AppliedTransactions, st.StagedTransactions,
		st.Metrics.ReceivedTransactions, st.Peers, lastErr)
	if !bJoined {
		t.Fatalf("agent B did not converge on %d rows within %v", total, convergeTimeout)
	}
	b4 := filepath.Join(backups, "b4")
	if err := agent.Backup(b4); err != nil {
		t.Fatalf("final online backup b4: %v", err)
	}
	if err := murmur.VerifyBackup(b4, key[:]); err != nil {
		t.Fatalf("verify b4: %v", err)
	}
	if err := agent.Close(); err != nil {
		t.Fatalf("agent clean close: %v", err)
	}
	t.Log("final online backup b4 verified")

	// Restore every completed backup: intermediates must be non-empty,
	// non-decreasing, and subsets of the final data; the final backup must
	// match the survivors exactly (zero lost rows).
	runtime := cluster.RuntimeDir()
	dbid := cluster.DatabaseID()
	b1Names := restoreNames(t, b1, filepath.Join(runtime, "restore-b1"), key, dbid)
	b2Names := restoreNames(t, b2, filepath.Join(runtime, "restore-b2"), key, dbid)
	if len(b1Names) == 0 || len(b2Names) < len(b1Names) {
		t.Fatalf("intermediate backup counts %d/%d not positive non-decreasing", len(b1Names), len(b2Names))
	}
	finalSet := make(map[string]bool, len(survivors))
	for _, name := range survivors {
		finalSet[name] = true
	}
	for _, leg := range []struct {
		label string
		names []string
	}{{"b1", b1Names}, {"b2", b2Names}} {
		for _, name := range leg.names {
			if !finalSet[name] {
				t.Fatalf("backup %s restores phantom row %s", leg.label, name)
			}
		}
		t.Logf("backup %s: %d rows all present in final set", leg.label, len(leg.names))
	}
	b4Names := restoreNames(t, b4, filepath.Join(runtime, "restore-b4"), key, dbid)
	if len(b4Names) != total {
		t.Fatalf("final backup count = %d, want %d (lost rows)", len(b4Names), total)
	}
	if len(b4Names) != len(survivors) {
		t.Fatal("final backup names differ from survivors")
	}
	for i := range b4Names {
		if b4Names[i] != survivors[i] {
			t.Fatal("final backup names differ from survivors")
		}
	}
	t.Logf("PASS: restored b1=%d b2=%d b4=%d rows; final matches survivors exactly", len(b1Names), len(b2Names), len(b4Names))
	failed = false
}
