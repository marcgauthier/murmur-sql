// Live scenario: managed search (FTS + vectors) and JSON stay correct
// across the mesh.
//
// Go port of tests/search_mesh.rs using tests-live/gosqlharness.
// Identical behavior: node 0 is appointed schema master, creates the
// replicated docs table with FTS and vector declarations, and writes two
// rows; the replica adopts the table and declarations, serves MATCH,
// KNN, and JSON queries, tracks a replicated update after refresh, drops
// and rebuilds its FTS index across a disable/enable round trip, and
// rebuilds both indexes from rows after a restart. The Rust edit_config
// closures become SetConfig section writes.
package searchmesh

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	gosqlharness "github.com/marcgauthier/murmur-sql/tests-live/gosqlharness"
)

const (
	wait = 60 * time.Second
	poll = 100 * time.Millisecond
)

func f32le(values []float32) []byte {
	out := make([]byte, 0, 4*len(values))
	var buf [4]byte
	for _, v := range values {
		binary.LittleEndian.PutUint32(buf[:], math.Float32bits(v))
		out = append(out, buf[:]...)
	}
	return out
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// ftsIndex returns the ready FTS index table name for column, or false
// while not indexed.
func ftsIndex(cluster *gosqlharness.Cluster, node int, column string) (string, bool) {
	rows, err := cluster.Node(node).Query("MURMUR SHOW COLUMNS FTS")
	if err != nil {
		return "", false
	}
	for _, row := range rows {
		table, err := row.String(0)
		if err != nil {
			return "", false
		}
		col, err := row.String(1)
		if err != nil {
			return "", false
		}
		enabled, err := row.String(2)
		if err != nil {
			return "", false
		}
		name, nameOK := row.OptString(3)
		ready, readyOK := row.OptString(4)
		if table == "docs" && col == column && enabled == "true" && readyOK && ready == "true" {
			// A NULL name reads as not-ready, matching the
			// Rust `return name` (None while unnamed).
			return name, nameOK
		}
	}
	return "", false
}

// vectorIndex returns the ready vector index table name for column, or
// false while not indexed.
func vectorIndex(cluster *gosqlharness.Cluster, node int, column string) (string, bool) {
	rows, err := cluster.Node(node).Query("MURMUR SHOW COLUMNS VECTOR")
	if err != nil {
		return "", false
	}
	for _, row := range rows {
		table, err := row.String(0)
		if err != nil {
			return "", false
		}
		col, err := row.String(1)
		if err != nil {
			return "", false
		}
		enabled, err := row.String(2)
		if err != nil {
			return "", false
		}
		name, nameOK := row.OptString(5)
		ready, readyOK := row.OptString(6)
		if table == "docs" && col == column && enabled == "true" && readyOK && ready == "true" {
			// A NULL name reads as not-ready, matching the
			// Rust `return name` (None while unnamed).
			return name, nameOK
		}
	}
	return "", false
}

func rowCount(cluster *gosqlharness.Cluster, node int) int64 {
	rows, err := cluster.Node(node).Query("SELECT count(*) FROM docs")
	if err != nil || len(rows) == 0 {
		return -1
	}
	n, err := rows[0].Int64(0)
	if err != nil {
		return -1
	}
	return n
}

func waitRows(t *testing.T, cluster *gosqlharness.Cluster, node int, want int64, what string) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for rowCount(cluster, node) != want {
		if !time.Now().Before(deadline) {
			t.Fatalf("%s did not converge", what)
		}
		time.Sleep(poll)
	}
}

func waitIndex(t *testing.T, cluster *gosqlharness.Cluster, node int, column string, vector bool, what string) string {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		var found string
		var ok bool
		if vector {
			found, ok = vectorIndex(cluster, node, column)
		} else {
			found, ok = ftsIndex(cluster, node, column)
		}
		if ok {
			return found
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s never became ready", what)
		}
		time.Sleep(poll)
	}
}

// waitDisabled waits until column's FTS declaration reads back as disabled.
func waitDisabled(t *testing.T, cluster *gosqlharness.Cluster, node int, column, what string) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		rows, err := cluster.Node(node).Query("MURMUR SHOW COLUMNS FTS")
		if err != nil {
			rows = nil
		}
		disabled := false
		for _, row := range rows {
			// OptString presence mirrors the Rust
			// as_deref() == Some(..) checks: a NULL never
			// matches.
			tbl, tblOK := row.OptString(0)
			col, colOK := row.OptString(1)
			enabled, enOK := row.OptString(2)
			if tblOK && tbl == "docs" && colOK && col == column && enOK && enabled == "false" {
				disabled = true
				break
			}
		}
		if disabled {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s did not converge", what)
		}
		time.Sleep(poll)
	}
}

// matchIDs returns MATCH row ids, or false while the index rejects
// reads. Search is eventually consistent: an unavailable or rebuilding
// index may reject reads, so callers must retry.
func matchIDs(cluster *gosqlharness.Cluster, node int, index, query string) ([]int64, bool) {
	ident := quoteIdent(index)
	rows, err := cluster.Node(node).Query(
		fmt.Sprintf("SELECT d.id FROM docs d JOIN temp.%s f ON f.rowid=d.id WHERE %s MATCH $1 ORDER BY d.id", ident, ident),
		query)
	if err != nil {
		return nil, false
	}
	var out []int64
	for _, row := range rows {
		id, err := row.Int64(0)
		if err != nil {
			return nil, false
		}
		out = append(out, id)
	}
	return out, true
}

func waitMatch(t *testing.T, cluster *gosqlharness.Cluster, node int, index, query string, want []int64, what string) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		if got, ok := matchIDs(cluster, node, index, query); ok && equalInt64s(got, want) {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s did not converge", what)
		}
		time.Sleep(poll)
	}
}

// knnHit is one KNN (rowid, distance) hit.
type knnHit struct {
	id   int64
	dist float64
}

// knnHits returns KNN hits, or false while the index rejects reads.
func knnHits(cluster *gosqlharness.Cluster, node int, index, query string, k int64) ([]knnHit, bool) {
	ident := quoteIdent(index)
	rows, err := cluster.Node(node).Query(
		fmt.Sprintf("SELECT rowid,distance FROM temp.%s WHERE embedding MATCH $1 AND k=$2 ORDER BY distance", ident),
		query, k)
	if err != nil {
		return nil, false
	}
	var out []knnHit
	for _, row := range rows {
		id, err := row.Int64(0)
		if err != nil {
			return nil, false
		}
		dist, err := row.Float64(1)
		if err != nil {
			return nil, false
		}
		out = append(out, knnHit{id: id, dist: dist})
	}
	return out, true
}

func waitKNN(t *testing.T, cluster *gosqlharness.Cluster, node int, index, query string, k, wantFirst int64, wantLen int, what string) []knnHit {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		if hits, ok := knnHits(cluster, node, index, query, k); ok {
			if len(hits) == wantLen && len(hits) > 0 && hits[0].id == wantFirst {
				return hits
			}
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s did not converge", what)
		}
		time.Sleep(poll)
	}
}

// waitCell polls a single-cell query until it reads want.
func waitCell(t *testing.T, cluster *gosqlharness.Cluster, node int, query, want, what string) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		rows, err := cluster.Node(node).Query(query)
		if err != nil {
			rows = nil
		}
		var got string
		var ok bool
		if len(rows) > 0 {
			got, ok = rows[0].OptString(0)
		}
		if ok && got == want {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s did not converge", what)
		}
		time.Sleep(poll)
	}
}

func equalInt64s(a, b []int64) bool {
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

func TestManagedSearchAndJSONHoldAcrossTheMesh(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	cluster, err := gosqlharness.NewCluster(gosqlharness.ClusterOptions{
		Name:     "search-mesh",
		NumNodes: 2,
	})
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	// Appoint node 0 the schema master before any schema exists: without
	// a single schema authority, peers adopt each other's manifests in
	// arrival order and a declaration change can be wiped back by the
	// replica's stale manifest.
	master := cluster.NodeInfo(0).NodeID
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop node 0: %v", err)
	}
	if err := cluster.StopNode(1); err != nil {
		t.Fatalf("stop node 1: %v", err)
	}
	if err := cluster.SetConfig(0, "replication", map[string]any{
		"master":        true,
		"schema_master": master,
	}); err != nil {
		t.Fatalf("set node 0 schema master: %v", err)
	}
	if err := cluster.SetConfig(1, "replication", map[string]any{
		"schema_master": master,
	}); err != nil {
		t.Fatalf("set node 1 schema master: %v", err)
	}
	if err := cluster.StartNode(0); err != nil {
		t.Fatalf("start node 0: %v", err)
	}
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("start node 1: %v", err)
	}
	// Single schema writer: node 0 creates the replicated table and all
	// search declarations. Creating the same table on both nodes makes
	// two schema writers whose manifests overwrite each other's column
	// flags in arrival order, so the replica adopts the table (and its
	// declarations) instead of creating a competing one. Declaration
	// changes propagate when subsequent row batches carry the new schema
	// digest, so every declaration change below is followed by a row
	// write.
	mustSimple(t, cluster, 0, "CREATE TABLE IF NOT EXISTS docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT, embedding BLOB, meta TEXT)")
	mustSimple(t, cluster, 0, "MURMUR SET TABLE docs REPLICATED true")
	mustSimple(t, cluster, 0, "MURMUR SET COLUMN 'docs' 'body' FTS true")
	mustSimple(t, cluster, 0, "MURMUR SET COLUMN 'docs' 'embedding' VECTOR true DIMENSIONS 2 DISTANCE l2")
	mustSimple(t, cluster, 0, "MURMUR FLUSH")
	mustSimple(t, cluster, 0, "MURMUR REFRESH INDEXES")
	waitIndex(t, cluster, 0, "body", false, "node0 FTS declaration")

	first := f32le([]float32{1.0, 0.0})
	second := f32le([]float32{0.0, 1.0})
	if _, err := cluster.Node(0).Execute("INSERT INTO docs VALUES($1,$2,$3,$4,$5)",
		int64(1), "SQLite", "fast search", first, `{"lang":"en","tags":["db"]}`); err != nil {
		t.Fatalf("insert doc 1: %v", err)
	}
	if _, err := cluster.Node(0).Execute("INSERT INTO docs VALUES($1,$2,$3,$4,$5)",
		int64(2), "Notes", "other text", second, `{"lang":"en","tags":["misc"]}`); err != nil {
		t.Fatalf("insert doc 2: %v", err)
	}
	mustSimple(t, cluster, 0, "MURMUR FLUSH")
	// Row sync proves the replica adopted the table; the requested
	// schema manifest carries the search declarations with it.
	waitRows(t, cluster, 1, 2, "replica row sync")
	waitIndex(t, cluster, 1, "body", false, "replica FTS declaration")
	waitIndex(t, cluster, 1, "embedding", true, "replica vector declaration")
	mustSimple(t, cluster, 1, "MURMUR REFRESH INDEXES")

	// FTS MATCH works on the replica over replicated rows.
	fts := waitIndex(t, cluster, 1, "body", false, "replica FTS index")
	waitMatch(t, cluster, 1, fts, "fast AND search", []int64{1}, "replica MATCH")

	// Vector KNN on the replica: exact hit first, sqrt(2) neighbor second.
	vectors := waitIndex(t, cluster, 1, "embedding", true, "replica vector index")
	hits := waitKNN(t, cluster, 1, vectors, "[1,0]", 2, 1, 2, "replica KNN")
	if hits[0].dist != 0.0 {
		t.Fatalf("exact hit distance = %v, want 0", hits[0].dist)
	}
	if hits[1].id != 2 {
		t.Fatalf("neighbor id = %d, want 2", hits[1].id)
	}
	if math.Abs(hits[1].dist-math.Sqrt2) >= 1e-5 {
		t.Fatalf("neighbor distance %v", hits[1].dist)
	}

	// JSON operators read replicated JSONB-shaped text on the replica.
	rows, err := cluster.Node(1).Query("SELECT json_extract(meta,'$.lang'), meta ->> '$.tags[0]' FROM docs WHERE id = 1")
	if err != nil {
		t.Fatalf("JSON read: %v", err)
	}
	if lang, err := rows[0].String(0); err != nil || lang != "en" {
		t.Fatalf("JSON lang = %q, want en (%v)", lang, err)
	}
	if tag, err := rows[0].String(1); err != nil || tag != "db" {
		t.Fatalf("JSON tag = %q, want db (%v)", tag, err)
	}

	// Refresh tracks a replicated update on the replica's index.
	mustSimple(t, cluster, 0, "UPDATE docs SET body='updated' WHERE id=1")
	mustSimple(t, cluster, 0, "MURMUR FLUSH")
	// Confirm the replicated row content landed before querying the index.
	waitCell(t, cluster, 1, "SELECT body FROM docs WHERE id=1", "updated", "replicated UPDATE")
	mustSimple(t, cluster, 1, "MURMUR REFRESH INDEXES")
	fts = waitIndex(t, cluster, 1, "body", false, "post-update FTS index")
	waitMatch(t, cluster, 1, fts, "updated", []int64{1}, "MATCH after replicated update")

	// Disable on the schema writer drops the replica index once the
	// declaration propagates; re-enable rebuilds it. The meta updates
	// only carry the new schema digest to the replica.
	mustSimple(t, cluster, 0, "MURMUR SET COLUMN 'docs' 'body' FTS false")
	mustSimple(t, cluster, 0, "UPDATE docs SET meta='{}' WHERE id=1")
	mustSimple(t, cluster, 0, "MURMUR FLUSH")
	// Confirm the piggyback row arrived before waiting on the declaration.
	waitCell(t, cluster, 1, "SELECT meta FROM docs WHERE id=1", "{}", "disable piggyback UPDATE")
	waitDisabled(t, cluster, 1, "body", "replica FTS disable")
	if ids, _ := matchIDs(cluster, 1, fts, "updated"); len(ids) != 0 {
		t.Fatalf("MATCH after disable = %v, want no rows", ids)
	}
	mustSimple(t, cluster, 0, "MURMUR SET COLUMN 'docs' 'body' FTS true")
	mustSimple(t, cluster, 0, "UPDATE docs SET meta='{}' WHERE id=2")
	mustSimple(t, cluster, 0, "MURMUR FLUSH")
	waitCell(t, cluster, 1, "SELECT meta FROM docs WHERE id=2", "{}", "re-enable piggyback UPDATE")
	mustSimple(t, cluster, 1, "MURMUR REFRESH INDEXES")
	fts = waitIndex(t, cluster, 1, "body", false, "re-enabled FTS index")
	waitMatch(t, cluster, 1, fts, "updated", []int64{1}, "MATCH after re-enable")

	// Indexes rebuild from rows across a replica restart.
	if err := cluster.StopNode(1); err != nil {
		t.Fatalf("stop node 1: %v", err)
	}
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart node 1: %v", err)
	}
	waitRows(t, cluster, 1, 2, "post-restart rows")
	mustSimple(t, cluster, 1, "MURMUR REFRESH INDEXES")
	fts = waitIndex(t, cluster, 1, "body", false, "post-restart FTS index")
	waitMatch(t, cluster, 1, fts, "updated", []int64{1}, "MATCH after restart")
	vectors = waitIndex(t, cluster, 1, "embedding", true, "post-restart vector index")
	hits = waitKNN(t, cluster, 1, vectors, "[1,0]", 1, 1, 1, "KNN after restart")
	if hits[0].id != 1 {
		t.Fatalf("post-restart KNN id = %d, want 1", hits[0].id)
	}
	failed = false
}

func mustSimple(t *testing.T, cluster *gosqlharness.Cluster, node int, sql string) {
	t.Helper()
	if _, err := cluster.Node(node).Simple(sql); err != nil {
		t.Fatalf("node %d %s: %v", node, sql, err)
	}
}
