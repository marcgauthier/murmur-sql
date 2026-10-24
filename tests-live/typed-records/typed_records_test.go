// Live scenario: typed application rows travel through the SQL client.
//
// Go port of tests/typed_records.rs using tests-live/gosqlharness.
// Identical behavior across eight tests: typed inserts replicate with
// byte-identical values, updates and deletes converge, managed primary
// keys stay stable across a restart, transaction words are no-ops with
// atomic statements, node-local tables stay local and persist, populated
// keys rebuild with immutable application keys enforced, files replicate
// and download through the client, concurrent additive schema branches
// preserve both values, CRDT operations converge across an offline
// partition, and an empty peer recovers typed rows from a retention
// snapshot. The Rust edit_config closure becomes a SetConfig section
// write and replication_toml fragments become ReplicationYAML.
package typedrecords

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

// schema creates the shared typed fixture and enables its replication
// (Rust: schema).
func schema(t *testing.T, cluster *gosqlharness.Cluster) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, title TEXT NOT NULL, score REAL, payload BLOB, optional TEXT)"); err != nil {
			t.Fatalf("node %d create records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate records: %v", i, err)
		}
	}
}

func flushAll(t *testing.T, cluster *gosqlharness.Cluster) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush: %v", i, err)
		}
	}
}

func TestSQLTypedRecordsReplicateUpdateDeleteAndRebuildAfterRestart(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "typed-records-roundtrip",
		NumNodes: 3,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	schema(t, cluster)
	title := "quotes ' and unicode 日本語"
	payload := []byte{0, 1, 127, 128, 255}
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO records(id,title,score,payload,optional) VALUES($1,$2,$3,$4,$5)",
		int64(1), title, 1.25, payload, nil); err != nil {
		t.Fatalf("node 0 typed insert: %v", err)
	}
	if _, err := cluster.Node(1).Simple(
		"INSERT INTO records(id,title,score,payload,optional) VALUES(2,'second',-2.5,x'', 'present')"); err != nil {
		t.Fatalf("node 1 typed insert: %v", err)
	}
	flushAll(t, cluster)
	projection := "SELECT id,title,score,hex(payload),optional FROM records ORDER BY id"
	expected := [][]string{
		{"1", title, "1.25", "00017F80FF", ""},
		{"2", "second", "-2.5", "", "present"},
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1, 2}, projection, expected); err != nil {
		t.Fatalf("converge typed rows: %v", err)
	}
	keys, err := cluster.Node(0).Simple("SELECT id,id_murmur_primary_key FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("node 0 read keys: %v", err)
	}
	for i := 1; i < 3; i++ {
		got, err := cluster.Node(i).Simple("SELECT id,id_murmur_primary_key FROM records ORDER BY id")
		if err != nil {
			t.Fatalf("node %d read keys: %v", i, err)
		}
		if !reflect.DeepEqual(got, keys) {
			t.Fatalf("node %d keys = %v, want %v", i, got, keys)
		}
	}
	typed, err := cluster.Node(2).Query(
		"SELECT title,score,payload,optional FROM records WHERE id=$1", int64(1))
	if err != nil {
		t.Fatalf("node 2 typed read: %v", err)
	}
	if got := typed[0].Get(0); got != title {
		t.Fatalf("title = %q, want %q", got, title)
	}
	if got, err := typed[0].Float64(1); err != nil || got != 1.25 {
		t.Fatalf("score = %v, %v, want 1.25", got, err)
	}
	if got, err := typed[0].Bytes(2); err != nil || !reflect.DeepEqual(got, payload) {
		t.Fatalf("payload = %v, %v, want %v", got, err, payload)
	}
	if !typed[0].IsNull(3) {
		t.Fatalf("optional = %q, want NULL", typed[0].Get(3))
	}
	if _, err := cluster.Node(2).Execute(
		"UPDATE records SET title=$1 WHERE id=$2", "updated remotely", int64(1)); err != nil {
		t.Fatalf("node 2 remote update: %v", err)
	}
	if _, err := cluster.Node(1).Execute("DELETE FROM records WHERE id=$1", int64(2)); err != nil {
		t.Fatalf("node 1 delete: %v", err)
	}
	flushAll(t, cluster)
	finalRows := [][]string{{"1", "updated remotely", "1.25", "00017F80FF", ""}}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1, 2}, projection, finalRows); err != nil {
		t.Fatalf("converge final rows: %v", err)
	}
	if _, err := cluster.Node(2).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node 2 pre-restart flush: %v", err)
	}
	if err := cluster.StopNode(2); err != nil {
		t.Fatalf("stop node 2: %v", err)
	}
	if err := cluster.StartNode(2); err != nil {
		t.Fatalf("restart node 2: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{2}, projection, finalRows); err != nil {
		t.Fatalf("node 2 post-restart rows: %v", err)
	}
	got, err := cluster.Node(2).Simple("SELECT id,id_murmur_primary_key FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("node 2 post-restart keys: %v", err)
	}
	if !reflect.DeepEqual(got, [][]string{keys[0]}) {
		t.Fatalf("node 2 post-restart keys = %v, want %v", got, keys[0])
	}
	failed = false
}

func TestSQLTransactionWordsAreNoopsAndEachStatementIsAtomic(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "typed-records-rollback",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	schema(t, cluster)
	if _, err := cluster.Node(0).Simple(
		"BEGIN; INSERT INTO records(id,title) VALUES(1,'autocommitted'); ROLLBACK"); err != nil {
		t.Fatalf("noop transaction words: %v", err)
	}
	if _, err := cluster.Node(0).Simple(
		"BEGIN; INSERT INTO records(id,title) VALUES(2,'kept'); INSERT INTO records(id,title) VALUES(3,'also-kept'); COMMIT"); err != nil {
		t.Fatalf("multi-statement batch: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// The first row of a failed multi-row statement must not leak into RAM,
	// persistence, or replication when the second row violates a unique key.
	if _, err := cluster.Node(0).Simple(
		"INSERT INTO records(id,title) VALUES(4,'must-rollback'),(2,'duplicate')"); err == nil {
		t.Fatal("duplicate multi-row insert accepted")
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT id,title FROM records ORDER BY id",
		[][]string{
			{"1", "autocommitted"},
			{"2", "kept"},
			{"3", "also-kept"},
		}); err != nil {
		t.Fatalf("converge: %v", err)
	}
	failed = false
}

func TestSQLNodeLocalRecordsStayLocalAndPersistAfterRestart(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "typed-records-local",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	schema(t, cluster)
	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE private_records(id INTEGER PRIMARY KEY, body TEXT)"); err != nil {
			t.Fatalf("node %d create private_records: %v", i, err)
		}
		body := "private-to-node-" + strconv.Itoa(i)
		if _, err := cluster.Node(i).Execute(
			"INSERT INTO private_records(id,body) VALUES($1,$2)", int64(i), body); err != nil {
			t.Fatalf("node %d private insert: %v", i, err)
		}
		flags, err := cluster.Node(i).Simple("MURMUR SHOW TABLES REPLICATION")
		if err != nil {
			t.Fatalf("node %d show tables: %v", i, err)
		}
		found := false
		for _, f := range flags {
			found = found || (len(f) > 1 && f[0] == "private_records" && f[1] == "false")
		}
		if !found {
			t.Fatalf("node %d private_records not local-only: %v", i, flags)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush: %v", i, err)
		}
	}
	if _, err := cluster.Node(0).Simple(
		"INSERT INTO records(id,title) VALUES(10,'replicated-control')"); err != nil {
		t.Fatalf("control insert: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush control: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT title FROM records", [][]string{{"replicated-control"}}); err != nil {
		t.Fatalf("converge control: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := cluster.StopNode(i); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
		wantBody := "private-to-node-" + strconv.Itoa(i)
		got, err := cluster.Node(i).Simple("SELECT id,body FROM private_records")
		if err != nil {
			t.Fatalf("node %d post-restart read: %v", i, err)
		}
		want := [][]string{{strconv.Itoa(i), wantBody}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("node %d private rows = %v, want %v", i, got, want)
		}
		columns, err := cluster.Node(i).Simple(
			"SELECT name FROM pragma_table_info('private_records') WHERE pk=1")
		if err != nil {
			t.Fatalf("node %d pragma read: %v", i, err)
		}
		if !reflect.DeepEqual(columns, [][]string{{"id"}}) {
			t.Fatalf("node %d pk columns = %v, want [[id]]", i, columns)
		}
	}
	failed = false
}

func TestSQLReplicationRebuildsPopulatedKeysAndEnforcesImmutableApplicationKeys(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "typed-records-primary-key",
		NumNodes: 1,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if _, err := cluster.Node(0).Simple(
		"CREATE TABLE records(tenant TEXT NOT NULL, code INTEGER NOT NULL, body TEXT, PRIMARY KEY(tenant,code)); INSERT INTO records VALUES('one',7,'preserved')"); err != nil {
		t.Fatalf("seed records: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
		t.Fatalf("replicate records: %v", err)
	}
	first, err := cluster.Node(0).Simple("SELECT tenant,code,body,id_murmur_primary_key FROM records")
	if err != nil {
		t.Fatalf("read keys: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("got %d rows, want 1", len(first))
	}
	if first[0][3] == "" {
		t.Fatal("managed primary key is empty")
	}
	columns, err := cluster.Node(0).Simple("SELECT name FROM pragma_table_info('records') WHERE pk>0")
	if err != nil {
		t.Fatalf("pragma read: %v", err)
	}
	if !reflect.DeepEqual(columns, [][]string{{"id_murmur_primary_key"}}) {
		t.Fatalf("pk columns = %v, want [[id_murmur_primary_key]]", columns)
	}
	for _, sql := range []string{
		"UPDATE records SET tenant='other' WHERE code=7",
		"UPDATE records SET code=8 WHERE tenant='one'",
		"UPDATE records SET id_murmur_primary_key='forged'",
		"INSERT INTO records(tenant,code) VALUES(NULL,8)",
		"INSERT INTO records(tenant,code) VALUES('two',NULL)",
		"INSERT INTO records(tenant,code) VALUES('one',7)",
	} {
		if _, err := cluster.Node(0).Simple(sql); err == nil {
			t.Fatalf("invalid replicated key write accepted: %s", sql)
		}
	}
	got, err := cluster.Node(0).Simple("SELECT tenant,code,body,id_murmur_primary_key FROM records")
	if err != nil {
		t.Fatalf("reread: %v", err)
	}
	if !reflect.DeepEqual(got, first) {
		t.Fatalf("rows = %v, want %v", got, first)
	}
	if _, err := cluster.Node(0).Simple("MURMUR SET TABLE records REPLICATED false"); err != nil {
		t.Fatalf("unreplicate records: %v", err)
	}
	columns, err = cluster.Node(0).Simple(
		"SELECT name FROM pragma_table_info('records') WHERE pk>0 ORDER BY pk")
	if err != nil {
		t.Fatalf("pragma reread: %v", err)
	}
	if !reflect.DeepEqual(columns, [][]string{{"tenant"}, {"code"}}) {
		t.Fatalf("pk columns = %v, want [[tenant] [code]]", columns)
	}
	if _, err := cluster.Node(0).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
		t.Fatalf("re-replicate records: %v", err)
	}
	got, err = cluster.Node(0).Simple("SELECT tenant,code,body,id_murmur_primary_key FROM records")
	if err != nil {
		t.Fatalf("reread after re-replicate: %v", err)
	}
	if !reflect.DeepEqual(got, first) {
		t.Fatalf("rows = %v, want %v", got, first)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("pre-restart flush: %v", err)
	}
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node: %v", err)
	}
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("restart node: %v", err)
	}
	got, err = cluster.Node(0).Simple("SELECT tenant,code,body,id_murmur_primary_key FROM records")
	if err != nil {
		t.Fatalf("post-restart reread: %v", err)
	}
	if !reflect.DeepEqual(got, first) {
		t.Fatalf("rows = %v, want %v", got, first)
	}
	flags, err := cluster.Node(0).Simple("MURMUR SHOW TABLES REPLICATION")
	if err != nil {
		t.Fatalf("show tables: %v", err)
	}
	found := false
	for _, f := range flags {
		found = found || (len(f) > 1 && f[0] == "records" && f[1] == "true")
	}
	if !found {
		t.Fatalf("records not replicated after restart: %v", flags)
	}
	failed = false
}

func TestSQLFilesReplicateAndDownloadThroughTheGoClient(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "typed-records-files",
		NumNodes: 2,
		Files:    true,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	payload := make([]byte, 150_000)
	for i := range payload {
		payload[i] = byte((i*131 + i/251) % 256)
	}
	name := "typed/file ' unicode 日本語.bin"
	client := cluster.Node(0)
	upload, err := client.Query("MURMUR FILE UPLOAD BEGIN $1", name)
	if err != nil {
		t.Fatalf("upload begin: %v", err)
	}
	token := upload[0].Get(0)
	for off := 0; off < len(payload); off += 65536 {
		end := off + 65536
		if end > len(payload) {
			end = len(payload)
		}
		if _, err := client.Execute("MURMUR FILE UPLOAD APPEND $1 $2", token, payload[off:end]); err != nil {
			t.Fatalf("upload append at %d: %v", off, err)
		}
	}
	metadata, err := client.Query("MURMUR FILE UPLOAD FINISH $1", token)
	if err != nil {
		t.Fatalf("upload finish: %v", err)
	}
	if size, err := metadata[0].Int64(1); err != nil || size != int64(len(payload)) {
		t.Fatalf("upload size = %d, %v, want %d", size, err, len(payload))
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		cursor, err := cluster.Node(1).Query("MURMUR FILE LIST")
		if err != nil {
			t.Fatalf("node 1 file list: %v", err)
		}
		token := cursor[0].Get(0)
		found := false
		for {
			page, err := cluster.Node(1).Query("MURMUR FILE NEXT $1 LIMIT 100", token)
			if err != nil {
				t.Fatalf("node 1 file next: %v", err)
			}
			if len(page) == 0 {
				break
			}
			for _, row := range page {
				found = found || row.Get(0) == name
			}
		}
		if _, err := cluster.Node(1).Execute("MURMUR FILE CLOSE $1", token); err != nil {
			t.Fatalf("node 1 file close: %v", err)
		}
		if found {
			got, err := fileBytes(cluster.Node(1), name)
			if err == nil {
				if !reflect.DeepEqual(got, payload) {
					t.Fatal("replicated file bytes differ")
				}
				break
			}
			if goharness.ErrorCode(err) != "55000" {
				t.Fatalf("SQL file download: %v", err)
			}
		}
		if !time.Now().Before(deadline) {
			t.Fatal("replicated file did not fetch")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node 1 flush: %v", err)
	}
	if err := cluster.StopNode(1); err != nil {
		t.Fatalf("stop node 1: %v", err)
	}
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart node 1: %v", err)
	}
	got, err := fileBytes(cluster.Node(1), name)
	if err != nil {
		t.Fatalf("post-restart download: %v", err)
	}
	if !reflect.DeepEqual(got, payload) {
		t.Fatal("post-restart file bytes differ")
	}
	failed = false
}

func TestSQLConcurrentAdditiveSchemaBranchesPreserveBothValues(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:            "typed-records-schema-branches",
		NumNodes:        2,
		ReplicationYAML: "membership: false",
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	schema(t, cluster)
	if _, err := cluster.Node(0).Simple(
		"INSERT INTO records(id,title) VALUES(1,'branched-record')"); err != nil {
		t.Fatalf("seed branched record: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT title FROM records", [][]string{{"branched-record"}}); err != nil {
		t.Fatalf("converge seed: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d pre-split flush: %v", i, err)
		}
		if err := cluster.StopNode(i); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
		if err := cluster.SetPeers(i, nil); err != nil {
			t.Fatalf("node %d isolate: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
	}
	if _, err := cluster.Node(0).Simple(
		"ALTER TABLE records ADD COLUMN note TEXT; UPDATE records SET note='note-from-left' WHERE id=1"); err != nil {
		t.Fatalf("node 0 left branch: %v", err)
	}
	if _, err := cluster.Node(1).Simple(
		"ALTER TABLE records ADD COLUMN region TEXT; UPDATE records SET region='region-from-right' WHERE id=1"); err != nil {
		t.Fatalf("node 1 right branch: %v", err)
	}
	if _, err := cluster.Node(0).Simple("SELECT region FROM records"); err == nil {
		t.Fatal("node 0 sees unmerged region column")
	}
	if _, err := cluster.Node(1).Simple("SELECT note FROM records"); err == nil {
		t.Fatal("node 1 sees unmerged note column")
	}
	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d pre-join flush: %v", i, err)
		}
		if err := cluster.StopNode(i); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
		if err := cluster.SetPeers(i, []int{1 - i}); err != nil {
			t.Fatalf("node %d rejoin: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT title,note,region FROM records",
		[][]string{{"branched-record", "note-from-left", "region-from-right"}}); err != nil {
		t.Fatalf("converge merged schema: %v", err)
	}
	failed = false
}

func TestSQLCRDTOperationsConvergeAcrossOfflinePartition(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "typed-records-crdt",
		NumNodes: 2,
		ReplicationYAML: "membership: false\nmerge_policies:\n" +
			"  policies.counter: counter\n" +
			"  policies.tags: set\n" +
			"  policies.peak: max\n" +
			"  policies.floor: min\n",
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE IF NOT EXISTS policies(name TEXT PRIMARY KEY NOT NULL, counter INTEGER NOT NULL, tags BLOB NOT NULL, peak INTEGER NOT NULL, floor REAL NOT NULL)"); err != nil {
			t.Fatalf("node %d create policies: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE policies REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate policies: %v", i, err)
		}
	}
	if _, err := cluster.Node(0).Simple(
		"INSERT INTO policies(name,counter,tags,peak,floor) VALUES('shared',0,x'00',0,100.0)"); err != nil {
		t.Fatalf("seed shared policy: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR POLICY SET_ADD policies tags 'shared' 'seed'"); err != nil {
		t.Fatalf("seed set: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	initial, err := cluster.Node(0).Simple("SELECT name,counter,hex(tags),peak,floor FROM policies")
	if err != nil {
		t.Fatalf("read initial: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT name,counter,hex(tags),peak,floor FROM policies", initial); err != nil {
		t.Fatalf("converge initial: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d pre-split flush: %v", i, err)
		}
		if err := cluster.StopNode(i); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
		if err := cluster.SetPeers(i, nil); err != nil {
			t.Fatalf("node %d isolate: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
	}
	for _, sql := range []string{
		"MURMUR POLICY COUNTER_ADD policies counter 'shared' 4",
		"MURMUR POLICY SET_ADD policies tags 'shared' 'left'",
		"MURMUR POLICY SET_REMOVE policies tags 'shared' 'seed'",
		"MURMUR POLICY MAX policies peak 'shared' 40",
		"MURMUR POLICY MIN policies floor 'shared' 5.0",
	} {
		if _, err := cluster.Node(0).Simple(sql); err != nil {
			t.Fatalf("node 0 %s: %v", sql, err)
		}
	}
	for _, sql := range []string{
		"MURMUR POLICY COUNTER_ADD policies counter 'shared' 7",
		"MURMUR POLICY COUNTER_ADD policies counter 'shared' -2",
		"MURMUR POLICY SET_ADD policies tags 'shared' 'right'",
		"MURMUR POLICY SET_ADD policies tags 'shared' 'right'",
		"MURMUR POLICY MAX policies peak 'shared' 99",
		"MURMUR POLICY MIN policies floor 'shared' 0.5",
	} {
		if _, err := cluster.Node(1).Simple(sql); err != nil {
			t.Fatalf("node 1 %s: %v", sql, err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d pre-join flush: %v", i, err)
		}
		if err := cluster.StopNode(i); err != nil {
			t.Fatalf("stop node %d: %v", i, err)
		}
		if err := cluster.SetPeers(i, []int{1 - i}); err != nil {
			t.Fatalf("node %d rejoin: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := cluster.StartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
	}
	values := [][]string{{"9", "99", "0.5"}}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT counter,peak,floor FROM policies WHERE name='shared'", values); err != nil {
		t.Fatalf("converge CRDT scalars: %v", err)
	}
	// Build the expected materialized set through public operations, without
	// reimplementing the engine's postcard/CRDT representation in this client.
	if _, err := cluster.Node(0).Simple(
		"INSERT INTO policies(name,counter,tags,peak,floor) VALUES('control',0,x'00',0,100.0); MURMUR POLICY SET_ADD policies tags 'control' 'left'; MURMUR POLICY SET_ADD policies tags 'control' 'right'"); err != nil {
		t.Fatalf("seed control set: %v", err)
	}
	expectedSet, err := cluster.Node(0).Simple("SELECT hex(tags) FROM policies WHERE name='control'")
	if err != nil {
		t.Fatalf("read control set: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{0, 1},
		"SELECT hex(tags) FROM policies WHERE name='shared'", expectedSet); err != nil {
		t.Fatalf("converge CRDT set: %v", err)
	}
	if _, err := cluster.Node(0).Simple("UPDATE policies SET counter=100 WHERE name='shared'"); err == nil {
		t.Fatal("direct assignment bypassed CRDT policy")
	}
	if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node 1 pre-restart flush: %v", err)
	}
	if err := cluster.StopNode(1); err != nil {
		t.Fatalf("stop node 1: %v", err)
	}
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart node 1: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{1},
		"SELECT counter,peak,floor FROM policies WHERE name='shared'", values); err != nil {
		t.Fatalf("node 1 post-restart scalars: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{1},
		"SELECT hex(tags) FROM policies WHERE name='shared'", expectedSet); err != nil {
		t.Fatalf("node 1 post-restart set: %v", err)
	}
	failed = false
}

func TestSQLEmptyPeerRecoversTypedRowsFromARetentionSnapshot(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "typed-records-snapshot",
		NumNodes: 2,
		ReplicationYAML: "membership: false\nretention: \"1ms\"\n" +
			"retention_interval: \"60s\"\nminimum_retained: 1\n",
		ManualPeers: true,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	schema(t, cluster)
	// Authorize only this source to send snapshots to the empty receiver.
	source := cluster.NodeInfo(0).NodeID
	if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node 1 pre-stop flush: %v", err)
	}
	if err := cluster.StopNode(1); err != nil {
		t.Fatalf("stop node 1: %v", err)
	}
	if err := cluster.SetConfig(1, "replication", map[string]any{
		"trusted_snapshot_sources": []string{source},
	}); err != nil {
		t.Fatalf("trust snapshot source: %v", err)
	}
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart node 1: %v", err)
	}
	var expected [][]string
	for id := int64(0); id < 12; id++ {
		title := fmt.Sprintf("snapshot-row-%02d", id)
		if _, err := cluster.Node(0).Execute(
			"INSERT INTO records(id,title,score) VALUES($1,$2,$3)",
			id, title, float64(id)/2.0); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
		if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("flush %d: %v", id, err)
		}
		expected = append(expected, []string{strconv.FormatInt(id, 10), title})
	}
	time.Sleep(1100 * time.Millisecond)
	collected, err := cluster.Node(0).Simple("MURMUR RETENTION COLLECT")
	if err != nil {
		t.Fatalf("retention collect: %v", err)
	}
	pruned, err := strconv.ParseUint(collected[0][0], 10, 64)
	if err != nil {
		t.Fatalf("parse retention count: %v", err)
	}
	if pruned == 0 {
		t.Fatal("retention did not prune source log history")
	}
	before, err := cluster.Node(1).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		t.Fatalf("node 1 pre-join replication: %v", err)
	}
	// Bootstrap discovery is config-owned, so reopen the receiver with a donor.
	if err := cluster.StopNode(1); err != nil {
		t.Fatalf("stop node 1: %v", err)
	}
	if err := cluster.SetPeers(1, []int{0}); err != nil {
		t.Fatalf("node 1 join donor: %v", err)
	}
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart node 1: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{1},
		"SELECT id,title FROM records ORDER BY id", expected); err != nil {
		t.Fatalf("node 1 snapshot rows: %v", err)
	}
	after, err := cluster.Node(1).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		t.Fatalf("node 1 post-join replication: %v", err)
	}
	if before[0][5] != after[0][5] {
		t.Fatalf("receiver used log replay rather than the retained snapshot (%s -> %s)",
			before[0][5], after[0][5])
	}
	keys, err := cluster.Node(0).Simple("SELECT id,id_murmur_primary_key FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("node 0 read keys: %v", err)
	}
	got, err := cluster.Node(1).Simple("SELECT id,id_murmur_primary_key FROM records ORDER BY id")
	if err != nil {
		t.Fatalf("node 1 read keys: %v", err)
	}
	if !reflect.DeepEqual(got, keys) {
		t.Fatalf("node 1 keys = %v, want %v", got, keys)
	}
	if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("node 1 pre-restart flush: %v", err)
	}
	if err := cluster.StopNode(1); err != nil {
		t.Fatalf("stop node 1: %v", err)
	}
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart node 1: %v", err)
	}
	if err := gosqlharness.AssertRows(cluster, []int{1},
		"SELECT id,title FROM records ORDER BY id", expected); err != nil {
		t.Fatalf("node 1 post-restart rows: %v", err)
	}
	failed = false
}

// fileBytes downloads one file in pages. File handles remain on this
// client's one physical SQL connection (Rust: file_bytes).
func fileBytes(client *goharness.Client, name string) ([]byte, error) {
	opened, err := client.Query("MURMUR FILE DOWNLOAD $1", name)
	if err != nil {
		return nil, err
	}
	token := opened[0].Get(0)
	data, err := func() ([]byte, error) {
		var data []byte
		for {
			page, err := client.Query("MURMUR FILE DOWNLOAD NEXT $1 LIMIT 65536", token)
			if err != nil {
				return nil, err
			}
			offset, err := page[0].Int64(0)
			if err != nil {
				return nil, err
			}
			total, err := page[0].Int64(1)
			if err != nil {
				return nil, err
			}
			chunk, err := page[0].Bytes(2)
			if err != nil {
				return nil, err
			}
			if int(offset) != len(data) {
				return nil, fmt.Errorf("download offset %d, have %d bytes", offset, len(data))
			}
			data = append(data, chunk...)
			eof, err := page[0].Bool(3)
			if err != nil {
				return nil, err
			}
			if eof {
				if len(data) != int(total) {
					return nil, fmt.Errorf("download short read: %d of %d bytes", len(data), total)
				}
				return data, nil
			}
			if len(chunk) == 0 {
				return nil, fmt.Errorf("download empty nonfinal page")
			}
		}
	}()
	if _, cerr := client.Execute("MURMUR FILE CLOSE $1", token); cerr != nil {
		return nil, cerr
	}
	return data, err
}
