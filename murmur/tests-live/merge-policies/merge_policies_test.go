// Live scenario: merge policies across disconnect, forward, and restart.
//
// Go port of the Rust `merge-policies` scenario
// (/home/marc/MURMUR/tests-live/merge-policies/tests/merge_policies.rs).
// TestDisconnectedPoliciesForwardAndRestart: three encrypted nodes write
// PN_COUNTER / OR_SET / MAX / MIN / JSON / LIST policy ops on one shared
// row while disconnected; nodes 0 and 1 connect and converge the counter;
// node 0 stops and node 1 forwards the causal operations to node 2;
// node 2 restarts and recovers; finally observed set, JSON, and list
// removes propagate. The unobserved remove (node 1 removing "shared"
// before seeing node 0's add) must not erase the add, while the later
// observed removes must.
//
// Plain SQL plus policy ops replace the Go managed typed HTTP API of the
// original.
package mergepolicies

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// table is the typed table (Rust: managed typed record with
// counter/set/extrema fields).
const table = "live_merge_policies"

// baseDDL is the base DDL. counter/tags/peak/floor/doc/nums are policy
// columns. Set projections start as x'00' (postcard empty vec), the
// convention for fresh set cells; doc and nums start NULL (empty
// document / empty list).
const baseDDL = "CREATE TABLE live_merge_policies(name TEXT NOT NULL PRIMARY KEY, counter INTEGER NOT NULL, tags BLOB NOT NULL, peak INTEGER NOT NULL, floor REAL NOT NULL, doc TEXT, nums TEXT)"

// row is the shared row (Rust: one row id seeded on every node).
const row = "merge-policy-row"

const (
	// wait is the convergence deadline (Rust: 30 s waits).
	wait = 30 * time.Second
	// poll is the convergence poll interval (Rust: 50 ms polls).
	poll = 50 * time.Millisecond
)

// mergePolicies maps (table, column) -> merge policy for the typed table
// (Rust: PN_COUNTER, OR_SET, numeric MAX/MIN field kinds).
func mergePolicies() map[replication.PolicyColumn]replication.Policy {
	return map[replication.PolicyColumn]replication.Policy{
		{Table: table, Column: "counter"}: replication.PolicyCounter,
		{Table: table, Column: "tags"}:    replication.PolicySet,
		{Table: table, Column: "peak"}:    replication.PolicyMax,
		{Table: table, Column: "floor"}:   replication.PolicyMin,
		{Table: table, Column: "doc"}:     replication.PolicyJson,
		{Table: table, Column: "nums"}:    replication.PolicyList,
	}
}

// migrateAll migrates every node with ddl (Rust: shared typed-table
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
		if err := cluster.Node(i).SetReplication("live_merge_policies", true); err != nil {
			t.Fatalf("node%d opt-in failed: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush after opt-in failed: %v", i+1, err)
		}
	}
}

// insertRow is a typed insert on one node (Rust: TypedInsertWithID).
func insertRow(t *testing.T, db *murmur.Database, name string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute(
			"INSERT INTO live_merge_policies(name, counter, tags, peak, floor, doc, nums) VALUES(?, 0, x'00', 0, 0.0, NULL, NULL)",
			name,
		)
		return nil, err
	}); err != nil {
		t.Fatalf("typed insert failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after typed insert: %v", err)
	}
}

// counterValue is the PN_COUNTER projection (Rust: TypedCounterValue).
func counterValue(t *testing.T, db *murmur.Database, name string) int64 {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT counter FROM live_merge_policies WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("read counter: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return -1 << 62
	}
	return rows[0][0].Int
}

// setValues is the OR_SET projection, sorted (Rust: TypedSetValues).
func setValues(t *testing.T, db *murmur.Database, name string) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT tags FROM live_merge_policies WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("read set: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return []string{"<read-error>"}
	}
	values, err := murmur.DecodeValues(rows[0][0].Blob)
	if err != nil {
		return []string{"<read-error>"}
	}
	var out []string
	for _, value := range values {
		if value.Tag == murmur.ValueText {
			out = append(out, value.S)
		}
	}
	slices.Sort(out)
	return out
}

// extrema is the numeric extrema projection (Rust: TypedExtremaValues).
func extrema(t *testing.T, db *murmur.Database, name string) (int64, float64) {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT peak, floor FROM live_merge_policies WHERE name = ?", name)
	})
	if err != nil {
		t.Fatalf("read extrema: %v", err)
	}
	rows := v.([][]sqlite.Value)
	if len(rows) == 0 {
		return -1 << 62, 0
	}
	return rows[0][0].Int, rows[0][1].Float
}

// counterAdd is a PN_COUNTER op (Rust: TypedCounterAdd).
func counterAdd(t *testing.T, db *murmur.Database, name string, delta int64) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		return nil, s.CounterAdd(table, []murmur.Value{murmur.TextValue(name)}, "counter", delta)
	}); err != nil {
		t.Fatalf("counter add failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after counter add: %v", err)
	}
}

// setAdd is an OR_SET add (Rust: TypedSetAdd).
func setAdd(t *testing.T, db *murmur.Database, name, value string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		return nil, s.SetAdd(table, []murmur.Value{murmur.TextValue(name)}, "tags", murmur.TextValue(value))
	}); err != nil {
		t.Fatalf("set add failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after set add: %v", err)
	}
}

// setRemove is an OR_SET remove (Rust: TypedSetRemove).
func setRemove(t *testing.T, db *murmur.Database, name, value string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		return nil, s.SetRemove(table, []murmur.Value{murmur.TextValue(name)}, "tags", murmur.TextValue(value))
	}); err != nil {
		t.Fatalf("set remove failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after set remove: %v", err)
	}
}

// jsonSet writes one JSON path.
func jsonSet(t *testing.T, db *murmur.Database, name, path string, value murmur.Value) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		return nil, s.JsonSet(table, []murmur.Value{murmur.TextValue(name)}, "doc", path, value)
	}); err != nil {
		t.Fatalf("json set failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after json set: %v", err)
	}
}

// jsonRemove deletes one JSON path.
func jsonRemove(t *testing.T, db *murmur.Database, name, path string) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		return nil, s.JsonRemove(table, []murmur.Value{murmur.TextValue(name)}, "doc", path)
	}); err != nil {
		t.Fatalf("json remove failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after json remove: %v", err)
	}
}

// jsonDoc reads the raw JSON projection.
func jsonDoc(t *testing.T, db *murmur.Database, name string) string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.QueryRow("SELECT doc FROM live_merge_policies WHERE name = '" + name + "'")
	})
	if err != nil {
		t.Fatalf("json read failed: %v", err)
	}
	return v.([]sqlite.Value)[0].Str
}

// waitJsonDocEqual waits until two nodes project byte-identical JSON
// holding every required path. Byte-equality alone is not convergence:
// two nodes can render the same partial prefix while a third node's
// writes are still in flight.
func waitJsonDocEqual(t *testing.T, cluster *harness.Cluster, a, b int, name string, required []string) string {
	t.Helper()
	var doc string
	ok := harness.WaitUntil(wait, poll, func() bool {
		da, db := jsonDoc(t, cluster.Node(a), name), jsonDoc(t, cluster.Node(b), name)
		if da == "" || da != db {
			return false
		}
		var parsed map[string]any
		if err := json.Unmarshal([]byte(da), &parsed); err != nil {
			return false
		}
		for _, k := range required {
			if _, ok := parsed[k]; !ok {
				return false
			}
		}
		doc = da
		return true
	})
	if !ok {
		t.Fatalf("json docs differ: node%d=%s node%d=%s", a+1, jsonDoc(t, cluster.Node(a), name), b+1, jsonDoc(t, cluster.Node(b), name))
	}
	return doc
}

// checkJsonDoc pins disjoint paths and one conflict winner.
func checkJsonDoc(t *testing.T, raw string) {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("json doc unparsable: %v", err)
	}
	if doc["n0"] != "a" || doc["n1"] != "b" || doc["n2"] != "c" {
		t.Fatalf("json doc = %s, want disjoint paths unioned", raw)
	}
	if s, ok := doc["shared"]; !ok || (s != "zero" && s != "one") {
		t.Fatalf("json doc = %s, want one shared winner", raw)
	}
}

// listAppend tags one element onto the list.
func listAppend(t *testing.T, db *murmur.Database, name string, value murmur.Value) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		return nil, s.ListAppend(table, []murmur.Value{murmur.TextValue(name)}, "nums", value)
	}); err != nil {
		t.Fatalf("list append failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after list append: %v", err)
	}
}

// listRemove drops every element equal to value.
func listRemove(t *testing.T, db *murmur.Database, name string, value murmur.Value) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		return nil, s.ListRemove(table, []murmur.Value{murmur.TextValue(name)}, "nums", value)
	}); err != nil {
		t.Fatalf("list remove failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after list remove: %v", err)
	}
}

// listDoc reads the raw list projection.
func listDoc(t *testing.T, db *murmur.Database, name string) string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.QueryRow("SELECT nums FROM live_merge_policies WHERE name = '" + name + "'")
	})
	if err != nil {
		t.Fatalf("list read failed: %v", err)
	}
	return v.([]sqlite.Value)[0].Str
}

// listMultiset parses a rendered list into an element multiset keyed by
// canonical JSON encoding.
func listMultiset(t *testing.T, raw string) map[string]int {
	t.Helper()
	var list []any
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		t.Fatalf("list unparsable: %v", err)
	}
	got := map[string]int{}
	for _, e := range list {
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		got[string(b)]++
	}
	return got
}

// multisetEqual reports whether two element multisets match.
func multisetEqual(got, want map[string]int) bool {
	if len(got) != len(want) {
		return false
	}
	for k, n := range want {
		if got[k] != n {
			return false
		}
	}
	return true
}

// waitListEqual waits until two nodes project byte-identical lists
// matching the expected multiset. Byte-equality alone is not
// convergence: two nodes can render the same partial prefix while a
// third node's appends are still in flight.
func waitListEqual(t *testing.T, cluster *harness.Cluster, a, b int, name string, want map[string]int) string {
	t.Helper()
	var list string
	ok := harness.WaitUntil(wait, poll, func() bool {
		la, lb := listDoc(t, cluster.Node(a), name), listDoc(t, cluster.Node(b), name)
		if la == "" || la != lb {
			return false
		}
		var parsed []any
		if err := json.Unmarshal([]byte(la), &parsed); err != nil {
			return false
		}
		got := map[string]int{}
		for _, e := range parsed {
			b, err := json.Marshal(e)
			if err != nil {
				return false
			}
			got[string(b)]++
		}
		if !multisetEqual(got, want) {
			return false
		}
		list = la
		return true
	})
	if !ok {
		t.Fatalf("lists differ: node%d=%s node%d=%s", a+1, listDoc(t, cluster.Node(a), name), b+1, listDoc(t, cluster.Node(b), name))
	}
	return list
}

// checkList pins the union multiset: duplicate "a" appends from two
// nodes both survive (distinct tags).
func checkList(t *testing.T, raw string, want map[string]int) {
	t.Helper()
	if got := listMultiset(t, raw); !multisetEqual(got, want) {
		t.Fatalf("list = %s, want multiset %v", raw, want)
	}
}

// waitNode1HasNode0Tail blocks until node 1 holds every disconnected
// write node 0 made: its counter share, its set adds, one JSON path,
// and its full list tail. The counter wait alone cannot gate the
// StopNode(0) below -- policy columns converge independently, and
// stopping node 0 mid-transfer would cut its log tail permanently.
func waitNode1HasNode0Tail(t *testing.T, cluster *harness.Cluster, name string) {
	t.Helper()
	ok := harness.WaitUntil(wait, poll, func() bool {
		if counterValue(t, cluster.Node(1), name) != 17 {
			return false
		}
		tags := setValues(t, cluster.Node(1), name)
		if !slices.Contains(tags, "red") || !slices.Contains(tags, "shared") {
			return false
		}
		var doc map[string]any
		if err := json.Unmarshal([]byte(jsonDoc(t, cluster.Node(1), name)), &doc); err != nil {
			return false
		}
		if _, ok := doc["n0"]; !ok {
			return false
		}
		var list []any
		if err := json.Unmarshal([]byte(listDoc(t, cluster.Node(1), name)), &list); err != nil {
			return false
		}
		got := map[string]int{}
		for _, e := range list {
			b, err := json.Marshal(e)
			if err != nil {
				return false
			}
			got[string(b)]++
		}
		// Node 1's own appends (2, "a", "tmp") plus node 0's (1, "a").
		return got["1"] >= 1 && got["2"] >= 1 && got[`"a"`] >= 2 && got[`"tmp"`] >= 1
	})
	if !ok {
		t.Fatalf("node2 lacks node1's tail: counter=%d tags=%v doc=%s list=%s",
			counterValue(t, cluster.Node(1), name), setValues(t, cluster.Node(1), name),
			jsonDoc(t, cluster.Node(1), name), listDoc(t, cluster.Node(1), name))
	}
}

// extremaUpdate is a numeric MIN/MAX op (Rust: TypedExtremaUpdate).
func extremaUpdate(t *testing.T, db *murmur.Database, name string, peak int64, floor float64) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		if err := s.Max(table, []murmur.Value{murmur.TextValue(name)}, "peak", murmur.IntValue(peak)); err != nil {
			return nil, err
		}
		return nil, s.Min(table, []murmur.Value{murmur.TextValue(name)}, "floor", murmur.RealValue(floor))
	}); err != nil {
		t.Fatalf("extrema update failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush after extrema update: %v", err)
	}
}

func waitCounter(t *testing.T, cluster *harness.Cluster, node int, name string, want int64) {
	t.Helper()
	ok := harness.WaitUntil(wait, poll, func() bool {
		return counterValue(t, cluster.Node(node), name) == want
	})
	if !ok {
		t.Fatalf("node%d counter for %q = %d, want %d", node+1, name, counterValue(t, cluster.Node(node), name), want)
	}
}

// waitPolicyProjection is the combined set + extrema projection wait
// (Rust: waitPolicyProjection).
func waitPolicyProjection(t *testing.T, cluster *harness.Cluster, node int, name string, wantTags []string, wantPeak int64, wantFloor float64) {
	t.Helper()
	ok := harness.WaitUntil(wait, poll, func() bool {
		gotTags := setValues(t, cluster.Node(node), name)
		gotPeak, gotFloor := extrema(t, cluster.Node(node), name)
		return slices.Equal(gotTags, wantTags) && gotPeak == wantPeak && gotFloor == wantFloor
	})
	if !ok {
		gotPeak, gotFloor := extrema(t, cluster.Node(node), name)
		t.Fatalf("node%d projection tags=%v extrema=(%d,%v); want tags=%v extrema=(%d,%v)",
			node+1, setValues(t, cluster.Node(node), name), gotPeak, gotFloor, wantTags, wantPeak, wantFloor)
	}
}

func TestDisconnectedPoliciesForwardAndRestart(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        "merge-policies",
		NumNodes:    3,
		ManualPeers: true,
		Replication: &harness.ReplicationOptions{MergePolicies: mergePolicies()},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("merge-policies-no-convergence")
		}
		cluster.Finish(failed)
	}()
	migrateAll(t, cluster, baseDDL)

	// Every node seeds the same logical row while disconnected.
	for i := 0; i < cluster.Len(); i++ {
		insertRow(t, cluster.Node(i), row)
	}

	// Each node makes independent typed CRDT updates while disconnected.
	counterAdd(t, cluster.Node(0), row, 10)
	setAdd(t, cluster.Node(0), row, "shared")
	setAdd(t, cluster.Node(0), row, "red")
	extremaUpdate(t, cluster.Node(0), row, 10, 30.0)
	jsonSet(t, cluster.Node(0), row, "n0", murmur.TextValue("a"))
	jsonSet(t, cluster.Node(0), row, "shared", murmur.TextValue("zero"))
	listAppend(t, cluster.Node(0), row, murmur.IntValue(1))
	listAppend(t, cluster.Node(0), row, murmur.TextValue("a"))

	counterAdd(t, cluster.Node(1), row, 7)
	// This remove has not observed node 0's add and must not erase it.
	setRemove(t, cluster.Node(1), row, "shared")
	setAdd(t, cluster.Node(1), row, "blue")
	extremaUpdate(t, cluster.Node(1), row, 20, 15.0)
	jsonSet(t, cluster.Node(1), row, "n1", murmur.TextValue("b"))
	jsonSet(t, cluster.Node(1), row, "shared", murmur.TextValue("one"))
	jsonSet(t, cluster.Node(1), row, "tmp", murmur.TextValue("x"))
	listAppend(t, cluster.Node(1), row, murmur.IntValue(2))
	listAppend(t, cluster.Node(1), row, murmur.TextValue("a"))
	listAppend(t, cluster.Node(1), row, murmur.TextValue("tmp"))

	counterAdd(t, cluster.Node(2), row, -2)
	setAdd(t, cluster.Node(2), row, "green")
	extremaUpdate(t, cluster.Node(2), row, 5, -7.0)
	jsonSet(t, cluster.Node(2), row, "n2", murmur.TextValue("c"))
	listAppend(t, cluster.Node(2), row, murmur.IntValue(3))

	// Nodes 0 and 1 connect (node 2 stays isolated); their counters sum.
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("connect 0 -> 1: %v", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatalf("connect 1 -> 0: %v", err)
	}
	waitCounter(t, cluster, 0, row, 17)
	waitCounter(t, cluster, 1, row, 17)
	// Node 1 must hold node 0's full log tail before node 0 stops;
	// the counter alone does not prove the other columns arrived.
	waitNode1HasNode0Tail(t, cluster, row)

	// The original writer stops; node 1 forwards its causal operations to
	// node 2.
	cluster.StopNode(0)
	if err := cluster.AddPeer(1, 2); err != nil {
		t.Fatalf("connect 1 -> 2: %v", err)
	}
	if err := cluster.AddPeer(2, 1); err != nil {
		t.Fatalf("connect 2 -> 1: %v", err)
	}
	waitCounter(t, cluster, 1, row, 15)
	waitCounter(t, cluster, 2, row, 15)
	waitPolicyProjection(t, cluster, 1, row, []string{"blue", "green", "red", "shared"}, 20, -7.0)
	waitPolicyProjection(t, cluster, 2, row, []string{"blue", "green", "red", "shared"}, 20, -7.0)
	jsonWant := []string{"n0", "n1", "n2", "shared", "tmp"}
	checkJsonDoc(t, waitJsonDocEqual(t, cluster, 1, 2, row, jsonWant))
	listWant := map[string]int{"1": 1, "2": 1, "3": 1, `"a"`: 2, `"tmp"`: 1}
	checkList(t, waitListEqual(t, cluster, 1, 2, row, listWant), listWant)

	// Node 2 restarts and recovers the merged projections from durable
	// state. The restart rebinds a fresh ephemeral port, so the 1 <-> 2
	// edges are re-added explicitly.
	cluster.StopNode(2)
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("restart node 2: %v", err)
	}
	if err := cluster.AddPeer(1, 2); err != nil {
		t.Fatalf("reconnect 1 -> 2: %v", err)
	}
	if err := cluster.AddPeer(2, 1); err != nil {
		t.Fatalf("reconnect 2 -> 1: %v", err)
	}
	waitCounter(t, cluster, 2, row, 15)
	waitPolicyProjection(t, cluster, 2, row, []string{"blue", "green", "red", "shared"}, 20, -7.0)
	checkJsonDoc(t, waitJsonDocEqual(t, cluster, 1, 2, row, jsonWant))
	checkList(t, waitListEqual(t, cluster, 1, 2, row, listWant), listWant)

	// Later observed removes must propagate after restart.
	setRemove(t, cluster.Node(1), row, "shared")
	waitPolicyProjection(t, cluster, 2, row, []string{"blue", "green", "red"}, 20, -7.0)
	jsonRemove(t, cluster.Node(1), row, "tmp")
	raw := waitJsonDocEqual(t, cluster, 1, 2, row, []string{"n0", "n1", "n2", "shared"})
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("json doc unparsable: %v", err)
	}
	if _, ok := doc["tmp"]; ok {
		t.Fatalf("json doc = %s, want tmp removed", raw)
	}
	listRemove(t, cluster.Node(1), row, murmur.TextValue("tmp"))
	raw = waitListEqual(t, cluster, 1, 2, row, map[string]int{"1": 1, "2": 1, "3": 1, `"a"`: 2})
	checkList(t, raw, map[string]int{"1": 1, "2": 1, "3": 1, `"a"`: 2})
	failed = false
}
