// Live scenario: hostile schema manifests are refused and quarantined.
//
// Go port of tests/hostile_schema.rs using tests-live/goharness plus the
// goharness wire attacker. Identical behavior: a malicious authenticated
// peer (cluster-CA QUIC identity, untrusted origin key) serves hostile
// signed schema manifests at an honest 3-node SQL cluster: a revision
// with a corrupted signature, a self-consistent revision from an
// untrusted signer, an oversized revision, and a zero-sequence revision.
// Every forgery is refused and quarantined (verified via stream death,
// the error latch, and unchanged schema bytes/rows), and honest writes
// continue converging throughout.
//
// The Rust bespoke Quinn attacker becomes the shared goharness Attacker
// fixture (impersonating the stopped credential-donor node); manifest
// signing/verification reuses murmur/replication.
package hostileschema

import (
	"fmt"
	"net/netip"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

var (
	attackerID   = [16]byte{0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0}
	attackerSeed = [32]byte{
		0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
		0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
	}
	divergentSchema = []byte("attacker-divergent-schema-v9999")
	evilSchema      = []byte("attacker-evil-schema-v9999")
)

const (
	manifestTimeout = 15 * time.Second
	pongTimeout     = 10 * time.Second
	deathWindow     = 3 * time.Second
)

func msgName(m *replication.Message) string {
	switch m.Kind {
	case replication.MsgHello:
		return "Hello"
	case replication.MsgWelcome:
		return "Welcome"
	case replication.MsgBatches:
		return "Batches"
	case replication.MsgAck:
		return "Ack"
	case replication.MsgNeed:
		return "Need"
	case replication.MsgSnapshotRequest:
		return "SnapshotRequest"
	case replication.MsgSnapshotManifest:
		return "SnapshotManifest"
	case replication.MsgSnapshotChunk:
		return "SnapshotChunk"
	case replication.MsgSnapshotDone:
		return "SnapshotDone"
	case replication.MsgSnapshotResume:
		return "SnapshotResume"
	case replication.MsgSnapshotNeed:
		return "SnapshotNeed"
	case replication.MsgSnapshotApplied:
		return "SnapshotApplied"
	case replication.MsgSnapshotUnavailable:
		return "SnapshotUnavailable"
	case replication.MsgSnapshotCancel:
		return "SnapshotCancel"
	case replication.MsgPing:
		return "Ping"
	case replication.MsgPong:
		return "Pong"
	case replication.MsgError:
		return "Error"
	case replication.MsgSchemaRequest:
		return "SchemaRequest"
	case replication.MsgSchemaManifest:
		return "SchemaManifest"
	case replication.MsgSchemaAck:
		return "SchemaAck"
	case replication.MsgPlumtreeData:
		return "PlumtreeData"
	case replication.MsgIHave:
		return "IHave"
	case replication.MsgPrune:
		return "Prune"
	case replication.MsgGraft:
		return "Graft"
	case replication.MsgProgressRequest:
		return "ProgressRequest"
	case replication.MsgProgressPage:
		return "ProgressPage"
	case replication.MsgTransactionChunk:
		return "TransactionChunk"
	case replication.MsgMembersRequest:
		return "MembersRequest"
	case replication.MsgMembersPage:
		return "MembersPage"
	case replication.MsgChunkNeed:
		return "ChunkNeed"
	case replication.MsgChunkAvailabilityRequest:
		return "ChunkAvailabilityRequest"
	case replication.MsgChunkAvailabilityPage:
		return "ChunkAvailabilityPage"
	}
	return "Other"
}

func frameSummary(frames []replication.Message) string {
	if len(frames) == 0 {
		return "<none>"
	}
	names := make([]string, 0, len(frames))
	for i := range frames {
		names = append(names, msgName(&frames[i]))
	}
	return strings.Join(names, ",")
}

func isPong(nonce uint64) func(*replication.Message) bool {
	return func(m *replication.Message) bool {
		return m.Kind == replication.MsgPong && m.Nonce == nonce
	}
}

func dial(t *testing.T, attacker *goharness.Attacker, addr netip.AddrPort, db, expect [16]byte, schema []byte, what string) *goharness.Session {
	t.Helper()
	sess, err := attacker.DialWithSchema(addr, db, expect, schema)
	if err != nil {
		t.Fatalf("%s: attacker dial: %v", what, err)
	}
	return sess
}

func send(t *testing.T, sess *goharness.Session, msg *replication.Message, what string) {
	t.Helper()
	if err := sess.Send(msg); err != nil {
		t.Fatalf("%s: attacker send %s: %v", what, msgName(msg), err)
	}
}

// fetchManifest requests the served schema manifest (Rust:
// EvilSession::fetch_manifest).
func fetchManifest(t *testing.T, sess *goharness.Session, what string) replication.SignedSchema {
	t.Helper()
	send(t, sess, &replication.Message{Kind: replication.MsgSchemaRequest}, what)
	frames := sess.CollectUntil(manifestTimeout, func(m *replication.Message) bool {
		return m.Kind == replication.MsgSchemaManifest
	})
	for i := range frames {
		if frames[i].Kind == replication.MsgSchemaManifest {
			return frames[i].Signed
		}
	}
	t.Fatalf("%s: node served no schema manifest", what)
	return replication.SignedSchema{}
}

type recorderEntry struct {
	at   time.Time
	text string
}

// recorder is the background error-latch recorder on the target node
// (Rust: Recorder).
type recorder struct {
	mu   sync.Mutex
	log  []recorderEntry
	stop atomic.Bool
	wg   sync.WaitGroup
}

func startRecorder(cluster *goharness.Cluster) *recorder {
	r := &recorder{}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		client, _ := cluster.Connect(0)
		last := ""
		for !r.stop.Load() {
			if client == nil {
				client, _ = cluster.Connect(0)
			}
			if client != nil {
				rows, err := client.Simple("MURMUR SHOW REPLICATION")
				if err != nil {
					_ = client.Close()
					client = nil
				} else if len(rows) > 0 && len(rows[0]) >= 7 {
					cur := rows[0][6]
					if cur != "" && cur != last {
						last = cur
						r.mu.Lock()
						r.log = append(r.log, recorderEntry{time.Now(), cur})
						r.mu.Unlock()
					}
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		if client != nil {
			_ = client.Close()
		}
	}()
	return r
}

func (r *recorder) sawSince(since time.Time, needle string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.log {
		if !e.at.Before(since) && strings.Contains(e.text, needle) {
			return true
		}
	}
	return false
}

func (r *recorder) checkOrQuery(client *goharness.NodeClient, since time.Time, needle string) bool {
	if r.sawSince(since, needle) {
		return true
	}
	if rows, err := client.Simple("MURMUR SHOW REPLICATION"); err == nil {
		if len(rows) > 0 && len(rows[0]) >= 7 && strings.Contains(rows[0][6], needle) {
			return true
		}
	}
	return false
}

func (r *recorder) stopRecorder() {
	r.stop.Store(true)
	r.wg.Wait()
}

func assertStreamDead(t *testing.T, sess *goharness.Session, nonce uint64, what string) {
	t.Helper()
	_ = sess.Send(&replication.Message{Kind: replication.MsgPing, Nonce: nonce})
	frames := sess.CollectUntil(deathWindow, func(*replication.Message) bool { return false })
	for i := range frames {
		if frames[i].Kind == replication.MsgPong && frames[i].Nonce == nonce {
			t.Fatalf("node answered ping after %s (stream should be dead)", what)
		}
		if frames[i].Kind == replication.MsgSchemaAck {
			t.Fatalf("node acked the forged manifest after %s (saw %s)",
				what, frameSummary(frames))
		}
	}
	fmt.Fprintf(os.Stderr, "after %s the node sent only: %s\n", what, frameSummary(frames))
}

func expectPong(t *testing.T, sess *goharness.Session, nonce uint64, what string) {
	t.Helper()
	send(t, sess, &replication.Message{Kind: replication.MsgPing, Nonce: nonce}, what)
	frames := sess.CollectUntil(pongTimeout, isPong(nonce))
	for i := range frames {
		if frames[i].Kind == replication.MsgPong && frames[i].Nonce == nonce {
			return
		}
	}
	t.Fatalf("missing pong to %s; saw %s", what, frameSummary(frames))
}

func attackerManifest(db [16]byte, sequence uint64, schema []byte) replication.SignedSchema {
	signed := replication.SignedSchema{
		Database:  db,
		Signer:    attackerID,
		Sequence:  sequence,
		Schema:    schema,
		Signature: nil,
	}
	signed.Sign(&attackerSeed)
	return signed
}

func assertAuthorizedShape(t *testing.T, signed *replication.SignedSchema, db [16]byte, max int, what string) {
	t.Helper()
	trust := map[replication.NodeId][32]byte{attackerID: replication.PublicKey(&attackerSeed)}
	if err := signed.Verify(db, attackerID, trust, max); err != nil {
		t.Fatalf("%s is not even self-consistent: %v", what, err)
	}
}

func TestHostileSchemaManifestsQuarantined(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	const maxTX = 1 << 20 // 1 MiB transaction cap
	maxTxBytes := 1 << 20
	opts := goharness.DefaultClusterOptions("hostile-schema")
	opts.NumNodes = 4
	opts.Replication = &goharness.ReplicationOptions{MaxTransactionBytes: &maxTxBytes}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	// Node 0 is the target; Nodes 1 & 2 are honest witnesses.
	// Node 3 is stopped and donates its TLS identity for the attacker.
	cluster.StopNode(3)

	targetID := cluster.NodeID(0)
	targetAddr := cluster.NodeInfo(0).ReplAddr
	honest := []int{0, 1, 2}
	dbid := cluster.DatabaseID()

	// Initialize replicated table on honest nodes (Nodes 0, 1, 2).
	for _, i := range honest {
		if _, err := cluster.Node(i).Simple(
			"CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			t.Fatalf("node %d create records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			t.Fatalf("node %d replicate records: %v", i, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush schema: %v", i, err)
		}
	}

	// Positive control (pre-attack): honest writes replicate everywhere.
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(1), "baseline"); err != nil {
		t.Fatalf("insert baseline: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	preAttack := [][]string{{"baseline"}}
	if err := goharness.AssertRows(cluster, honest,
		"SELECT name FROM records ORDER BY name", preAttack); err != nil {
		t.Fatalf("converge baseline: %v", err)
	}

	// Attacker fixture impersonating the stopped node 3 (Rust: donated
	// CA-issued cert/key files; the TLS identity is the SAN URI, so a
	// fresh leaf for the same node id authenticates identically).
	attacker, err := goharness.Impersonate(cluster, 3, attackerSeed)
	if err != nil {
		t.Fatalf("attacker fixture: %v", err)
	}

	divergent := append([]byte(nil), divergentSchema...)
	var handshakes uint32

	// Start background error latch recorder on Node 0.
	rec := startRecorder(cluster)
	defer rec.stopRecorder()

	// --- S1: forged revision with a corrupted signature
	badDigest := dial(t, attacker, targetAddr, dbid, targetID, divergent, "digest-forge")
	handshakes++
	servedSchema := badDigest.WelcomeSchema()
	served := fetchManifest(t, badDigest, "digest-forge")
	if served.Signer != targetID {
		t.Fatal("served manifest is not signed by the target node")
	}
	if string(served.Schema) != string(servedSchema) {
		t.Fatal("served manifest bytes differ from Welcome bytes")
	}
	if served.Sequence == 0 {
		t.Fatal("served manifest has zero sequence")
	}

	forged := served
	forged.Signature[0] ^= 0xFF
	forged.Signature[17] ^= 0x01

	wire := replication.EncodeMessage(&replication.Message{Kind: replication.MsgSchemaManifest, Signed: forged})
	back, err := replication.DecodeMessage(wire)
	if err != nil {
		t.Fatalf("forged manifest does not decode locally: %v", err)
	}
	if back.Kind != replication.MsgSchemaManifest {
		t.Fatal("forged frame did not survive local round-trip")
	}

	sinceS1 := time.Now()
	send(t, badDigest, &replication.Message{Kind: replication.MsgSchemaManifest, Signed: forged}, "digest-forge")
	assertStreamDead(t, badDigest, 71, "digest-forged manifest")
	if !rec.checkOrQuery(cluster.Node(0), sinceS1, "invalid Ed25519 signature") {
		t.Fatal("node never latched the signature refusal for the digest forgery")
	}
	badDigest.Close()

	// The node survives the forgery.
	probe := dial(t, attacker, targetAddr, dbid, targetID, divergent, "probe-1")
	handshakes++
	expectPong(t, probe, 72, "probe after digest-forged manifest")
	probe.Close()

	// --- S2: self-consistent revision from untrusted signer
	evil := attackerManifest(dbid, 1, append([]byte(nil), evilSchema...))
	assertAuthorizedShape(t, &evil, dbid, maxTX, "evil revision")
	sinceS2 := time.Now()
	untrusted := dial(t, attacker, targetAddr, dbid, targetID, divergent, "untrusted-signer")
	handshakes++
	send(t, untrusted, &replication.Message{Kind: replication.MsgSchemaManifest, Signed: evil}, "untrusted-signer")
	assertStreamDead(t, untrusted, 73, "untrusted-signer manifest")
	if !rec.checkOrQuery(cluster.Node(0), sinceS2, "untrusted schema signer") {
		t.Fatal("node never latched the untrusted-signer refusal")
	}
	untrusted.Close()

	// --- S3: oversized revision (2 MiB against 1 MiB cap)
	big := attackerManifest(dbid, 1, make([]byte, 2<<20))
	for i := range big.Schema {
		big.Schema[i] = 0x55
	}
	assertAuthorizedShape(t, &big, dbid, 8<<20, "oversized revision")
	sinceS3 := time.Now()
	oversized := dial(t, attacker, targetAddr, dbid, targetID, divergent, "oversized")
	handshakes++
	send(t, oversized, &replication.Message{Kind: replication.MsgSchemaManifest, Signed: big}, "oversized")
	assertStreamDead(t, oversized, 74, "oversized manifest")
	if !rec.checkOrQuery(cluster.Node(0), sinceS3, "schema envelope identity or size") {
		t.Fatal("node never latched the envelope refusal for the oversized manifest")
	}
	oversized.Close()

	// --- S4: bad-epoch revision (zero sequence)
	zeroSeq := attackerManifest(dbid, 1, append([]byte(nil), evilSchema...))
	assertAuthorizedShape(t, &zeroSeq, dbid, maxTX, "zero-sequence revision")
	zeroSeq.Sequence = 0
	sinceS4 := time.Now()
	badEpoch := dial(t, attacker, targetAddr, dbid, targetID, divergent, "bad-epoch")
	handshakes++
	send(t, badEpoch, &replication.Message{Kind: replication.MsgSchemaManifest, Signed: zeroSeq}, "bad-epoch")
	assertStreamDead(t, badEpoch, 75, "zero-sequence manifest")
	if !rec.checkOrQuery(cluster.Node(0), sinceS4, "schema envelope identity or size") {
		t.Fatal("node never latched the envelope refusal for the zero-sequence manifest")
	}
	badEpoch.Close()

	// Quarantine: no adoption, served schema bytes unchanged, rows unchanged.
	if err := goharness.AssertRows(cluster, honest,
		"SELECT name FROM records ORDER BY name", preAttack); err != nil {
		t.Fatalf("rows moved during the schema attacks: %v", err)
	}

	finalProbe := dial(t, attacker, targetAddr, dbid, targetID, divergent, "final-schema")
	handshakes++
	if string(finalProbe.WelcomeSchema()) != string(servedSchema) {
		t.Fatal("schema bytes moved during the schema attacks")
	}
	finalProbe.Close()

	if handshakes != 6 {
		t.Fatalf("handshake count drifted (attach proof): %d", handshakes)
	}

	// --- Positive control (post-attack): honest nodes continue replicating
	if _, err := cluster.Node(1).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(2), "post-attack-1"); err != nil {
		t.Fatalf("insert post-attack-1: %v", err)
	}
	if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush post-attack-1: %v", err)
	}
	if _, err := cluster.Node(2).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(3), "post-attack-2"); err != nil {
		t.Fatalf("insert post-attack-2: %v", err)
	}
	if _, err := cluster.Node(2).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush post-attack-2: %v", err)
	}
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(4), "attacked-node-write"); err != nil {
		t.Fatalf("insert attacked-node-write: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush attacked-node-write: %v", err)
	}
	postAttack := [][]string{
		{"attacked-node-write"},
		{"baseline"},
		{"post-attack-1"},
		{"post-attack-2"},
	}
	if err := goharness.AssertRows(cluster, honest,
		"SELECT name FROM records ORDER BY name", postAttack); err != nil {
		t.Fatalf("converge post-attack: %v", err)
	}
	failed = false
}
