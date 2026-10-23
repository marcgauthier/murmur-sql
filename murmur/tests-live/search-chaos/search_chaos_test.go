// Live scenario: FTS and vector search indexes replicate across nodes
// and keep answering identical queries through crash and partition chaos.
//
// Three nodes share one replicated docs table with node 0 as schema
// master. Column search is enabled on the master only (FTS, then vectors);
// the search flags must propagate through schema adoption so every node
// builds a ready index, and MATCH/KNN queries must return identical id
// sets on all nodes. A chaos leg then crashes a node mid-index-build and
// flaps peering while all survivors keep inserting; after heal every index
// must be ready with no recorded error and every query must agree again.
package searchchaos

import (
	"encoding/binary"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

const (
	// docsDDL is the shared docs table (INTEGER PRIMARY KEY so the
	// search rowids join back to source ids).
	docsDDL = "CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT, embedding BLOB)"
	// convergeTimeout bounds every convergence wait.
	convergeTimeout = 90 * time.Second
	// poll is the convergence poll interval.
	poll = 200 * time.Millisecond
)

// doc is one baseline document: a single-term body plus a 2D point.
type doc struct {
	id   int64
	body string
	x, y float32
}

// baselineDocs keeps query expectations tie-free: every term maps to a
// fixed id set, and L2 distances from the origin are all distinct
// (0, 3, 4, 10, 10, ~9.9 except p5/p6 differ in the second axis, so the
// top-3 from [0,0] is exactly {1,2,3}).
var baselineDocs = []doc{
	{id: 1, body: "quick brown fox", x: 0, y: 0},
	{id: 2, body: "quick silver fox", x: 3, y: 0},
	{id: 3, body: "lazy dog sleeps", x: 0, y: 4},
	{id: 4, body: "brown dog runs", x: 10, y: 0},
	{id: 5, body: "silver moon rises", x: 0, y: 10},
	{id: 6, body: "river flows quietly", x: 7, y: 7},
}

// chaosDocsPerNode is the chaos-leg inserts per node. Bodies avoid every
// baseline query term and points sit far from the origin so the baseline
// top-k expectation is unchanged.
const chaosDocsPerNode = 4

// vectorBytes encodes a 2D point as a little-endian float32 blob.
func vectorBytes(x, y float32) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint32(out[0:], math.Float32bits(x))
	binary.LittleEndian.PutUint32(out[4:], math.Float32bits(y))
	return out
}

// quoteIdent double-quotes an index name for the temp schema.
func quoteIdent(name string) string {
	out := make([]byte, 0, len(name)+2)
	out = append(out, '"')
	for i := 0; i < len(name); i++ {
		if name[i] == '"' {
			out = append(out, '"')
		}
		out = append(out, name[i])
	}
	return string(append(out, '"'))
}

// insertDoc writes one document in its own transaction.
func insertDoc(t *testing.T, db *murmur.Database, d doc) {
	t.Helper()
	if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO docs(id, body, embedding) VALUES(?, ?, ?)",
			d.id, d.body, vectorBytes(d.x, d.y))
		return nil, err
	}); err != nil {
		t.Fatalf("insert doc %d: %v", d.id, err)
	}
}

// rowCount returns the docs row count on one node.
func rowCount(t *testing.T, db *murmur.Database) int {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM docs")
	})
	if err != nil {
		t.Fatalf("row count: %v", err)
	}
	return int(v.([][]sqlite.Value)[0][0].Int)
}

// waitRowCount waits until one node holds exactly want rows.
func waitRowCount(t *testing.T, cluster *harness.Cluster, idx, want int, what string) {
	t.Helper()
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		return rowCount(t, cluster.Node(idx)) == want
	}); !ok {
		t.Fatalf("%s: node%d holds %d rows, want %d", what, idx, rowCount(t, cluster.Node(idx)), want)
	}
}

// indexNames returns the ready index names on one node, failing on any
// index error.
func indexNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	_, statuses, err := db.SchemaWithSearchStatus()
	if err != nil {
		t.Fatalf("search status: %v", err)
	}
	var out []string
	for _, s := range statuses {
		if s.Error != nil {
			t.Fatalf("index %q recorded error %q", s.Name, *s.Error)
		}
		if s.Ready {
			out = append(out, s.Name)
		}
	}
	slices.Sort(out)
	return out
}

// waitIndexesReady waits until every node reports exactly the want index
// names ready.
func waitIndexesReady(t *testing.T, cluster *harness.Cluster, want []string, what string) {
	t.Helper()
	wantSorted := slices.Clone(want)
	slices.Sort(wantSorted)
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		for i := 0; i < cluster.Len(); i++ {
			if !slices.Equal(indexNames(t, cluster.Node(i)), wantSorted) {
				return false
			}
		}
		return true
	}); !ok {
		for i := 0; i < cluster.Len(); i++ {
			t.Logf("node%d ready indexes: %q", i, indexNames(t, cluster.Node(i)))
			schema, statuses, err := cluster.Node(i).SchemaWithSearchStatus()
			if err == nil {
				for _, tbl := range schema.Tables {
					for _, col := range tbl.Columns {
						t.Logf("node%d %s.%s fts=%v vector=%v", i, tbl.Name, col.Name, col.Fts, col.Vector != nil)
					}
				}
				t.Logf("node%d all statuses: %+v", i, statuses)
			}
			st := cluster.Node(i).Replication().Status()
			lastErr := "<nil>"
			if st.LastError != nil {
				lastErr = *st.LastError
			}
			t.Logf("node%d lastError=%s adoptions=%d pushes=%d", i, lastErr,
				st.Metrics.SchemaAdoptions, st.Metrics.SchemaPushes)
		}
		t.Fatalf("%s: indexes did not converge on %q", what, wantSorted)
	}
}

// matchIDs runs an FTS MATCH query on one node (same shape as the Go
// client FTS suite: temp index joined back to the source table).
func matchIDs(t *testing.T, db *murmur.Database, index, term string) []int64 {
	t.Helper()
	ident := quoteIdent(index)
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT d.id FROM docs d JOIN temp."+ident+" f ON f.rowid=d.id"+
			" WHERE "+ident+" MATCH ? ORDER BY d.id", term)
	})
	if err != nil {
		t.Fatalf("fts match %q: %v", term, err)
	}
	var out []int64
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Int)
	}
	return out
}

// knnIDs runs a KNN query on one node (same shape as the Go client
// vectors suite).
func knnIDs(t *testing.T, db *murmur.Database, index, query string, k int) []int64 {
	t.Helper()
	ident := quoteIdent(index)
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT d.id FROM temp."+ident+" v JOIN docs d ON d.id=v.rowid"+
			" WHERE v.embedding MATCH ? AND v.k=? ORDER BY v.distance", query, k)
	})
	if err != nil {
		t.Fatalf("knn %q k=%d: %v", query, k, err)
	}
	var out []int64
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Int)
	}
	return out
}

// assertQueriesAgree checks every FTS and KNN expectation on every node.
func assertQueriesAgree(t *testing.T, cluster *harness.Cluster, ftsIndex, vecIndex string) {
	t.Helper()
	for i := 0; i < cluster.Len(); i++ {
		db := cluster.Node(i)
		if got := matchIDs(t, db, ftsIndex, "quick"); !slices.Equal(got, []int64{1, 2}) {
			t.Fatalf("node%d fts 'quick' = %v, want [1 2]", i, got)
		}
		if got := matchIDs(t, db, ftsIndex, "dog"); !slices.Equal(got, []int64{3, 4}) {
			t.Fatalf("node%d fts 'dog' = %v, want [3 4]", i, got)
		}
		if got := knnIDs(t, db, vecIndex, "[0,0]", 3); !slices.Equal(got, []int64{1, 2, 3}) {
			t.Fatalf("node%d knn [0,0] k=3 = %v, want [1 2 3]", i, got)
		}
	}
}

func TestSearchIndexesReplicateAndSurviveChaos(t *testing.T) {
	// Node 0 is the schema master and performs every schema write: in
	// masterless mode a node can adopt a peer's stale manifest over its
	// own newer schema change (per-signer sequences cannot order schemas
	// across signers), silently and durably losing the change. The master
	// never adopts, and followers adopt only the master, so flags only
	// move forward.
	master := 0
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:       "search-chaos",
		NumNodes:   3,
		MasterNode: &master,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("search-chaos-no-convergence")
		}
		cluster.Finish(failed)
	}()

	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, docsDDL); err != nil {
			t.Fatalf("migrate docs table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("docs", true); err != nil {
			t.Fatalf("opt-in docs: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Baseline rows from node 0; every node must converge before any
	// index exists.
	for _, d := range baselineDocs {
		insertDoc(t, cluster.Node(0), d)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	for i := 0; i < cluster.Len(); i++ {
		waitRowCount(t, cluster, i, len(baselineDocs), "baseline")
	}

	// Schema sync is data-triggered: a peer requests its sender's schema
	// only when a batch arrives stamped with an unknown schema digest
	// (or at session start), so every flag change below is followed by a
	// marker write that carries the new digest to the peers.
	marker := func(node int, id int64) {
		insertDoc(t, cluster.Node(node), doc{
			id:   id,
			body: "marker filler words",
			x:    float32(200 + id%50),
			y:    float32(200 + id%50),
		})
		if err := cluster.Node(node).Flush(); err != nil {
			t.Fatalf("flush marker %d: %v", id, err)
		}
	}

	// Leg 1: FTS enabled on node 0 only. Schema adoption must carry the
	// flag to every node and every node must build a ready index.
	if err := cluster.Node(0).SetColumnFts("docs", "body", true); err != nil {
		t.Fatalf("enable fts: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush fts flag: %v", err)
	}
	marker(0, 101)
	var ftsIndex string
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		got := indexNames(t, cluster.Node(0))
		if len(got) != 1 {
			return false
		}
		ftsIndex = got[0]
		return true
	}); !ok {
		t.Fatalf("node0 never built its fts index")
	}
	waitIndexesReady(t, cluster, []string{ftsIndex}, "fts propagation")
	for i := 0; i < cluster.Len(); i++ {
		db := cluster.Node(i)
		if got := matchIDs(t, db, ftsIndex, "quick"); !slices.Equal(got, []int64{1, 2}) {
			t.Fatalf("node%d fts 'quick' = %v, want [1 2]", i, got)
		}
	}

	// Leg 2: vectors enabled on the master; same propagation proof plus
	// KNN agreement.
	if err := cluster.Node(0).SetColumnVector("docs", "embedding",
		&murmur.VectorColumnConfig{Dimensions: 2, Distance: murmur.DistL2}); err != nil {
		t.Fatalf("enable vectors: %v", err)
	}
	if err := cluster.Node(0).Flush(); err != nil {
		t.Fatalf("flush vector flag: %v", err)
	}
	marker(0, 102)
	var vecIndex string
	if ok := harness.WaitUntil(convergeTimeout, poll, func() bool {
		got := indexNames(t, cluster.Node(0))
		if len(got) != 2 {
			return false
		}
		for _, name := range got {
			if name != ftsIndex {
				vecIndex = name
			}
		}
		return vecIndex != ""
	}); !ok {
		t.Fatalf("node0 never built its vector index")
	}
	waitIndexesReady(t, cluster, []string{ftsIndex, vecIndex}, "vector propagation")
	assertQueriesAgree(t, cluster, ftsIndex, vecIndex)

	// Leg 3: chaos. Node 1 crashes while the survivors insert rows its
	// indexes never see, and the survivor peering flaps mid-write; after
	// heal every row, index, and query must agree.
	cluster.StopNode(1)
	writers := []int{0, 2}
	const markers = 2
	total := len(baselineDocs) + markers + len(writers)*chaosDocsPerNode
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, nodeIdx := range writers {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()
			db := cluster.Node(nodeIdx)
			<-start
			base := int64(1000 + nodeIdx*100)
			for n := 0; n < chaosDocsPerNode; n++ {
				id := base + int64(n)
				insertDoc(t, db, doc{
					id:   id,
					body: "unrelated filler words gamma",
					x:    float32(100 + id%50),
					y:    float32(100 + id%50),
				})
			}
			if err := db.Flush(); err != nil {
				t.Errorf("node%d flush chaos inserts: %v", nodeIdx, err)
			}
		}(nodeIdx)
	}
	close(start)
	time.Sleep(300 * time.Millisecond)
	if err := cluster.RemovePeer(0, 2); err != nil {
		t.Fatalf("flap remove peer: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if err := cluster.AddPeer(0, 2); err != nil {
		t.Fatalf("flap re-add peer: %v", err)
	}
	wg.Wait()
	if err := cluster.StartNode(1); err != nil {
		t.Fatalf("restart node1: %v", err)
	}
	if err := cluster.Mesh(); err != nil {
		t.Fatalf("re-mesh: %v", err)
	}
	// The restarted node rebuilds through the normal tick; a refresh
	// only hurries the rebuild, it must not be required for correctness.
	if err := cluster.Node(1).RefreshIndexes(); err != nil {
		t.Fatalf("refresh restarted indexes: %v", err)
	}
	for i := 0; i < cluster.Len(); i++ {
		waitRowCount(t, cluster, i, total, "post-chaos rows")
	}
	waitIndexesReady(t, cluster, []string{ftsIndex, vecIndex}, "post-chaos indexes")
	assertQueriesAgree(t, cluster, ftsIndex, vecIndex)
	t.Logf("search indexes converged: %d rows, fts=%q vec=%q", total, ftsIndex, vecIndex)
	failed = false
}
