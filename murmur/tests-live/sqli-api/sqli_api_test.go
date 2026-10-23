// Live scenario: SQL application routes stay removed.
//
// Go port of the Rust `sqli-api` scenario
// (/home/marc/MURMUR/tests-live/sqli-api/tests/sqli_api.rs),
// itself a port of the original Go `sqli-api` scenario
// (TestSQLApplicationRoutesRemoved). That suite pins the Phase 6
// cutover at the running node boundary: former SQL routes reject
// requests with 410 before parsing them, while the managed typed API
// stays writable and replicates. There are no HTTP routes here, so this
// port pins the security property those 410s existed to enforce --
// untrusted SQL text is never executed -- at the layers that do exist:
// SQL payloads replicate as inert byte-exact data with the schema
// untouched, and retired-route request bytes sprayed at the replication
// port die on arrival with no session and no state change.
package sqliapi

import (
	"net"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// tableDDL is the records table (Rust: typed records).
const tableDDL = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)"

const (
	// convergeTimeout bounds every convergence wait (Rust: 20s).
	convergeTimeout = 20 * time.Second
	// poll is the convergence poll interval.
	poll = 100 * time.Millisecond
)

// payloads must only ever exist as inert data: exfil/stacked bodies
// plus DDL attempts.
var payloads = []string{
	`{"query":"SELECT * FROM records"}`,
	`{"query":"INSERT INTO records VALUES (1); DROP TABLE records"}`,
	`{"query":"' UNION SELECT secret FROM secrets --"}`,
	"DROP TABLE live_typed_records",
	"CREATE TABLE pwned(id TEXT)",
	"'; DELETE FROM live_typed_records; --",
}

// routeBytes are retired-route request bytes, sprayed at the
// replication port: they must die on arrival -- no session, no
// execution, no state change.
var routeBytes = []string{
	"POST /v1/query HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\r\n{\"query\":\"SELECT * FROM records\"}",
	"POST /v1/exec HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\r\n{\"query\":\"INSERT INTO records VALUES (1); DROP TABLE records\"}",
	"{\"query\":\"' UNION SELECT secret FROM secrets --\"}",
}

// insertName inserts one record (Rust: `TypedInsert`).
func insertName(t *testing.T, db *murmur.Database, node int, name string) {
	t.Helper()
	_, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO live_typed_records(name) VALUES(?)", name)
		return nil, err
	})
	if err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("node%d flush after insert failed: %v", node, err)
	}
}

// countName counts rows named `name` on one node (Rust: `TypedCount`).
func countName(t *testing.T, db *murmur.Database, name string) int {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM live_typed_records WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("count name failed: %v", err)
	}
	return int(v.([][]sqlite.Value)[0][0].Int)
}

// schemaSnapshot is the full schema DDL snapshot on one node (proves
// payloads created, dropped, or altered nothing).
func schemaSnapshot(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read schema failed: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
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

// waitTypedCount polls until `name` counts `want` on every node.
func waitTypedCount(t *testing.T, cluster *harness.Cluster, name string, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			v, err := cluster.Node(i).Read(func(s *murmur.Sql) (any, error) {
				return s.Query("SELECT COUNT(*) FROM live_typed_records WHERE name = ?", name)
			})
			if err != nil {
				return false
			}
			if int(v.([][]sqlite.Value)[0][0].Int) != want {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("typed count for %q did not reach %d on all nodes within %v", name, want, timeout)
	}
}

// waitConverged polls exact total counts plus ordered-name equality.
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, poll, func() bool {
		var first []string
		for i := 0; i < cluster.Len(); i++ {
			v, err := cluster.Node(i).Read(func(s *murmur.Sql) (any, error) {
				return s.Query("SELECT name FROM live_typed_records ORDER BY name")
			})
			if err != nil {
				return false
			}
			var got []string
			for _, row := range v.([][]sqlite.Value) {
				got = append(got, row[0].Str)
			}
			if len(got) != want {
				return false
			}
			if first == nil {
				first = got
			} else if !equalStrings(first, got) {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not converge on %d rows with equal names within %v", want, timeout)
	}
}

// TestSQLApplicationRoutesRemoved proves SQL payloads stay inert data
// and retired-route bytes die at the replication port (Rust:
// sql_application_routes_removed).
func TestSQLApplicationRoutesRemoved(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "sqli-api-removed",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("sqli-api-no-convergence")
		}
		cluster.Finish(failed)
	}()
	for i := 0; i < cluster.Len(); i++ {
		info := cluster.NodeInfo(i)
		t.Logf("%s dir=%s repl=%s", info.Label, info.Dir, info.ReplAddr)
	}

	// Same schema on every node (Rust: typed fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, tableDDL); err != nil {
			t.Fatalf("node%d migrate records table: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("live_typed_records", true); err != nil {
			t.Fatalf("node%d opt-in live_typed_records: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}

	// Managed write before any SQL probe.
	insertName(t, cluster.Node(0), 1, "before-sql-probe")
	waitTypedCount(t, cluster, "before-sql-probe", 1, convergeTimeout)
	schemaBefore := make([][]string, cluster.Len())
	for i := 0; i < cluster.Len(); i++ {
		schemaBefore[i] = schemaSnapshot(t, cluster.Node(i))
	}

	// SQL payloads travel the managed typed path and land as inert
	// byte-exact data on every node.
	for _, payload := range payloads {
		insertName(t, cluster.Node(0), 1, payload)
		waitTypedCount(t, cluster, payload, 1, convergeTimeout)
	}
	for i := 0; i < cluster.Len(); i++ {
		for _, payload := range payloads {
			if got := countName(t, cluster.Node(i), payload); got != 1 {
				t.Fatalf("node%d payload %q count = %d, want 1 (executed or mangled?)", i+1, payload, got)
			}
		}
		if got := schemaSnapshot(t, cluster.Node(i)); !equalStrings(got, schemaBefore[i]) {
			t.Fatalf("node%d schema changed under SQL payloads (DDL executed?)", i+1)
		}
	}
	t.Logf("%d SQL payloads inert on all nodes, schemas untouched", len(payloads))

	// Retired-route request bytes at the replication port: dead on
	// arrival -- no session attaches, no rows appear, no schema changes.
	peersBefore := make([]int, cluster.Len())
	for i := 0; i < cluster.Len(); i++ {
		h := cluster.Node(i).Replication()
		if h == nil {
			t.Fatalf("node%d has no replication", i+1)
		}
		peersBefore[i] = h.Status().Peers
	}
	replAddr := cluster.NodeInfo(0).ReplAddr
	udpAddr, err := net.ResolveUDPAddr("udp", replAddr.String())
	if err != nil {
		t.Fatalf("probe addr: %v", err)
	}
	sock, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		t.Fatalf("probe socket: %v", err)
	}
	for round := 0; round < 5; round++ {
		for _, b := range routeBytes {
			_, _ = sock.Write([]byte(b))
			time.Sleep(50 * time.Millisecond)
		}
	}
	_ = sock.Close()
	time.Sleep(2 * time.Second)
	for i := 0; i < cluster.Len(); i++ {
		h := cluster.Node(i).Replication()
		if h == nil {
			t.Fatalf("node%d has no replication", i+1)
		}
		if got := h.Status().Peers; got != peersBefore[i] {
			t.Fatalf("node%d peers moved under retired-route bytes (session attached?)", i+1)
		}
		if got := schemaSnapshot(t, cluster.Node(i)); !equalStrings(got, schemaBefore[i]) {
			t.Fatalf("node%d schema changed under retired-route bytes", i+1)
		}
	}
	waitConverged(t, cluster, 1+len(payloads), convergeTimeout)
	t.Logf("retired-route bytes dead on arrival: no sessions, no state change")

	// Managed write after the probes still replicates, with per-node
	// counts exact.
	insertName(t, cluster.Node(0), 1, "after-sql-probe")
	waitTypedCount(t, cluster, "after-sql-probe", 1, convergeTimeout)
	for i := 0; i < cluster.Len(); i++ {
		for _, name := range []string{"before-sql-probe", "after-sql-probe"} {
			if got := countName(t, cluster.Node(i), name); got != 1 {
				t.Fatalf("node%d typed count for %q = %d, want 1", i+1, name, got)
			}
		}
	}
	t.Logf("PASS: SQL routes dead, typed API writable and replicating")
	failed = false
}
