// Live scenario: hostile peer attacks are rejected on the wire and in
// product latches/counters.
//
// Go port of tests/hostile_peer.rs using tests-live/goharness plus the
// goharness wire attacker. Identical behavior: a malicious authenticated
// peer (cluster-CA QUIC identity, untrusted origin key) fires forged
// batches, invalid identities, a replayed handshake, a 256 MiB
// allocation-claim frame, a compressed snapshot bomb, and a
// far-future-HLC batch at an honest 2-node SQL cluster. Every attack is
// rejected on the wire and in product latches/counters, nothing foreign
// applies, and the honest pair stays converged and responsive
// throughout.
//
// The Rust bespoke Quinn attacker becomes the shared goharness Attacker
// fixture (impersonating the stopped credential-donor node) with raw
// frame helpers; transaction signing/verification reuses
// murmur/replication and the bomb compresses with murmur/compression.
package hostilepeer

import (
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

var (
	attackerID = [16]byte{
		0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0,
		0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0,
	}
	attackerSeed = [32]byte{
		0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
		0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
		0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
		0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
	}
	divergentSchema = []byte("attacker-divergent-schema-v9999")
)

const (
	batchesKind       = 3
	snapshotChunkKind = 8
	compressedFlag    = 0x8002
)

const (
	errorTimeout = 10 * time.Second
	pongTimeout  = 10 * time.Second
	drainTimeout = 5 * time.Second
	deathWindow  = 3 * time.Second
)

func nowMillis() uint64 {
	return uint64(time.Now().UnixMilli())
}

func msgName(m *replication.Message) string {
	switch m.Kind {
	case replication.MsgHello:
		return "Hello"
	case replication.MsgWelcome:
		return "Welcome"
	case replication.MsgNeed:
		return "Need"
	case replication.MsgBatches:
		return "Batches"
	case replication.MsgAck:
		return "Ack"
	case replication.MsgPing:
		return "Ping"
	case replication.MsgPong:
		return "Pong"
	case replication.MsgSnapshotChunk:
		return "SnapshotChunk"
	case replication.MsgError:
		return "Error"
	}
	return "Other"
}

func frameSummary(frames []replication.Message) string {
	if len(frames) == 0 {
		return "empty"
	}
	parts := make([]string, 0, len(frames))
	for i := range frames {
		switch frames[i].Kind {
		case replication.MsgError:
			parts = append(parts, fmt.Sprintf("Error(%d: %q)",
				frames[i].Code, frames[i].ErrMessage))
		case replication.MsgPong:
			parts = append(parts, fmt.Sprintf("Pong(%d)", frames[i].Nonce))
		default:
			parts = append(parts, msgName(&frames[i]))
		}
	}
	return strings.Join(parts, ", ")
}

func assertNoForeignData(t *testing.T, frames []replication.Message, honest [][16]byte, ctx string) {
	t.Helper()
	isHonest := func(id [16]byte) bool {
		for _, h := range honest {
			if h == id {
				return true
			}
		}
		return false
	}
	for i := range frames {
		switch frames[i].Kind {
		case replication.MsgBatches:
			for _, tx := range frames[i].Transactions {
				if !isHonest(tx.Origin) {
					t.Fatalf("attacker received a batch with foreign origin %x %s",
						tx.Origin, ctx)
				}
			}
		case replication.MsgPlumtreeData:
			if !isHonest(frames[i].Tx.Origin) {
				t.Fatalf("attacker received plumtree data with foreign origin %x %s",
					frames[i].Tx.Origin, ctx)
			}
		case replication.MsgTransactionChunk:
			if !isHonest(frames[i].Chunk.Proof.Origin) {
				t.Fatalf("attacker received a chunk with foreign origin %x %s",
					frames[i].Chunk.Proof.Origin, ctx)
			}
		}
	}
}

func attackerTx(db, origin [16]byte, seq uint64, schema [32]byte, payload []byte, millis uint64) replication.Transaction {
	tx := replication.Transaction{
		Database: db,
		Origin:   origin,
		Sequence: seq,
		ID:       [16]byte{0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0},
		Millis:   millis,
		Logical:  0,
		Schema:   schema,
		Payload:  payload,
	}
	tx.Sign(&attackerSeed)
	return tx
}

func assertRoundtrips(t *testing.T, tx *replication.Transaction, what string) {
	t.Helper()
	raw := replication.EncodeTransaction(tx)
	back, err := replication.DecodeTransaction(raw)
	if err != nil {
		t.Fatalf("%s does not decode locally: %v", what, err)
	}
	if back.Origin != tx.Origin || back.Sequence != tx.Sequence || string(back.Payload) != string(tx.Payload) {
		t.Fatalf("%s did not survive a local round-trip", what)
	}
}

func assertAuthorizedShape(t *testing.T, tx *replication.Transaction, db [16]byte, max int, what string) {
	t.Helper()
	trust := map[replication.NodeId][32]byte{attackerID: replication.PublicKey(&attackerSeed)}
	if err := tx.Verify(db, trust, max); err != nil {
		t.Fatalf("%s is not even self-consistent: %v", what, err)
	}
}

func isPong(nonce uint64) func(*replication.Message) bool {
	return func(m *replication.Message) bool {
		return m.Kind == replication.MsgPong && m.Nonce == nonce
	}
}

func isErrorCode(want uint16) func(*replication.Message) bool {
	return func(m *replication.Message) bool {
		return m.Kind == replication.MsgError && m.Code == want
	}
}

func isSchemaRequest(m *replication.Message) bool {
	return m.Kind == replication.MsgSchemaRequest
}

func lastErrorText(frames []replication.Message) (uint16, string, bool) {
	for i := len(frames) - 1; i >= 0; i-- {
		if frames[i].Kind == replication.MsgError {
			return frames[i].Code, frames[i].ErrMessage, true
		}
	}
	return 0, "", false
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

func assertStreamDead(t *testing.T, sess *goharness.Session, honest [][16]byte, nonce uint64, what string) []replication.Message {
	t.Helper()
	_ = sess.Send(&replication.Message{Kind: replication.MsgPing, Nonce: nonce})
	frames := sess.CollectUntil(deathWindow, func(*replication.Message) bool { return false })
	for i := range frames {
		if frames[i].Kind == replication.MsgPong && frames[i].Nonce == nonce {
			t.Fatalf("node answered ping after %s (stream should be dead)", what)
		}
	}
	assertNoForeignData(t, frames, honest, "after "+what)
	fmt.Fprintf(os.Stderr, "after %s the node sent only: %s\n", what, frameSummary(frames))
	return frames
}

func expectProtocolError(t *testing.T, sess *goharness.Session, needle, what string) []replication.Message {
	t.Helper()
	frames := sess.CollectUntil(errorTimeout, isErrorCode(1))
	if code, text, ok := lastErrorText(frames); ok && code == 1 && strings.Contains(text, needle) {
		return frames
	}
	t.Fatalf("%s drew no Error{code: 1, ..%s..}; saw %v", what, needle, frames)
	return nil
}

func expectPong(t *testing.T, sess *goharness.Session, nonce uint64, what string) []replication.Message {
	t.Helper()
	send(t, sess, &replication.Message{Kind: replication.MsgPing, Nonce: nonce}, what)
	frames := sess.CollectUntil(pongTimeout, isPong(nonce))
	for i := range frames {
		if frames[i].Kind == replication.MsgPong && frames[i].Nonce == nonce {
			return frames
		}
	}
	t.Fatalf("missing pong to %s; saw %s", what, frameSummary(frames))
	return nil
}

type counters struct {
	staged  uint64
	applied uint64
}

func getCounters(t *testing.T, client *goharness.NodeClient) counters {
	t.Helper()
	rows, err := client.Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		t.Fatalf("SHOW REPLICATION: %v", err)
	}
	var c counters
	// Unparseable counters read as zero (Rust: unwrap_or(0)).
	c.staged, _ = strconv.ParseUint(rows[0][4], 10, 64)
	c.applied, _ = strconv.ParseUint(rows[0][5], 10, 64)
	return c
}

func TestHostilePeerAttacksRejected(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	const maxTX = 1 << 20 // 1 MiB transaction cap
	maxTxBytes := 1 << 20
	opts := goharness.DefaultClusterOptions("hostile-peer")
	opts.NumNodes = 3
	opts.Replication = &goharness.ReplicationOptions{MaxTransactionBytes: &maxTxBytes}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	// Node 0 is the honest target; Node 1 is the honest witness.
	// Node 2 is stopped and serves as the credential donor for the attacker.
	cluster.StopNode(2)

	n0ID := cluster.NodeID(0)
	n1ID := cluster.NodeID(1)
	honest := [][16]byte{n0ID, n1ID}
	dbid := cluster.DatabaseID()

	// Initialize replicated table on honest nodes (Node 0 and Node 1).
	for i := 0; i < 2; i++ {
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

	// Positive control (pre-attack): honest writes replicate both ways.
	if _, err := cluster.Node(0).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(1), "honest-1"); err != nil {
		t.Fatalf("insert honest-1: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush honest-1: %v", err)
	}
	if err := goharness.AssertRows(cluster, []int{0, 1},
		"SELECT name FROM records ORDER BY name", [][]string{{"honest-1"}}); err != nil {
		t.Fatalf("converge honest-1: %v", err)
	}
	if _, err := cluster.Node(1).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(2), "honest-2"); err != nil {
		t.Fatalf("insert honest-2: %v", err)
	}
	if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush honest-2: %v", err)
	}
	preAttack := [][]string{{"honest-1"}, {"honest-2"}}
	if err := goharness.AssertRows(cluster, []int{0, 1},
		"SELECT name FROM records ORDER BY name", preAttack); err != nil {
		t.Fatalf("converge pre-attack: %v", err)
	}

	baseCounters := getCounters(t, cluster.Node(0))

	// Attacker fixture impersonating the stopped node 2 (Rust: donated
	// CA-issued cert/key files; the TLS identity is the SAN URI, so a
	// fresh leaf for the same node id authenticates identically).
	attacker, err := goharness.Impersonate(cluster, 2, attackerSeed)
	if err != nil {
		t.Fatalf("attacker fixture: %v", err)
	}

	targetAddr := cluster.NodeInfo(0).ReplAddr
	divergent := append([]byte(nil), divergentSchema...)
	var handshakes uint32

	// 1. Learn the node's real schema bytes from a divergent dial.
	learn := dial(t, attacker, targetAddr, dbid, n0ID, divergent, "schema-learn")
	handshakes++
	realSchema := learn.WelcomeSchema()
	if string(realSchema) == string(divergent) {
		t.Fatal("node schema equals divergent schema")
	}
	realDigest := replication.SchemaDigest(realSchema)
	learn.Close()

	time.Sleep(200 * time.Millisecond)

	// 2. Positive control: matched-schema hello welcomed with no SchemaRequest.
	control := dial(t, attacker, targetAddr, dbid, n0ID, realSchema, "control")
	handshakes++
	controlFrames := control.CollectUntil(2*time.Second, func(*replication.Message) bool { return false })
	for i := range controlFrames {
		if isSchemaRequest(&controlFrames[i]) {
			t.Fatalf("control session drew SchemaRequest; saw %s", frameSummary(controlFrames))
		}
	}
	pongFrames := expectPong(t, control, 7, "control ping")
	assertNoForeignData(t, pongFrames, honest, "on the control session")
	control.Close()

	time.Sleep(200 * time.Millisecond)

	// 3. Start error latch recorder.
	rec := startRecorder(cluster)
	defer rec.stopRecorder()

	// --- P1a: invalid identities on one watched divergent session
	watched := dial(t, attacker, targetAddr, dbid, n0ID, divergent, "watched")
	handshakes++

	var seen []replication.Message
	forgedDigest := [32]byte{
		0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0,
		0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0,
		0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0,
		0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0,
	}

	// Zero sequence.
	zeroSeq := attackerTx(dbid, attackerID, 0, forgedDigest, []byte("zero-seq"), nowMillis())
	assertRoundtrips(t, &zeroSeq, "zero-sequence batch")
	send(t, watched, &replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{zeroSeq}}, "zero-seq")
	seen = append(seen, expectProtocolError(t, watched, "transaction identity", "zero-sequence batch")...)

	// Wrong database id.
	wrongDB := attackerTx([16]byte{0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD},
		attackerID, 7, forgedDigest, []byte("wrong-db"), nowMillis())
	assertRoundtrips(t, &wrongDB, "wrong-database batch")
	send(t, watched, &replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{wrongDB}}, "wrong-db")
	seen = append(seen, expectProtocolError(t, watched, "transaction identity", "wrong-database batch")...)

	// Oversized payload: 2 MiB against 1 MiB cap.
	bigPayload := make([]byte, 2<<20)
	for i := range bigPayload {
		bigPayload[i] = 0x55
	}
	big := attackerTx(dbid, attackerID, 8, forgedDigest, bigPayload, nowMillis())
	assertRoundtrips(t, &big, "oversized batch")
	send(t, watched, &replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{big}}, "oversized")
	seen = append(seen, expectProtocolError(t, watched, "transaction identity", "oversized batch")...)

	// Invalid missing range.
	send(t, watched, &replication.Message{Kind: replication.MsgNeed, Origin: attackerID, From: 0, Through: 5}, "bad-need")
	seen = append(seen, expectProtocolError(t, watched, "invalid missing range", "zero-from Need")...)

	// Session must be alive: ping round-trips.
	seen = append(seen, expectPong(t, watched, 11, "ping-1 after invalid batches")...)

	// --- P2: replay handshake on live session
	send(t, watched, attacker.Hello(dbid, divergent), "replay-hello")
	seen = append(seen, expectProtocolError(t, watched, "repeated handshake", "replayed handshake")...)

	// Session still alive.
	seen = append(seen, expectPong(t, watched, 22, "ping-2 after replayed handshake")...)
	seen = append(seen, watched.CollectUntil(time.Second, func(*replication.Message) bool { return false })...)
	assertNoForeignData(t, seen, honest, "on watched session")
	watched.Close()

	time.Sleep(200 * time.Millisecond)

	// --- P1b: forged matched-schema batch from untrusted origin
	forged := attackerTx(dbid, attackerID, 1, realDigest, []byte("forged"), nowMillis())
	assertRoundtrips(t, &forged, "forged batch")
	assertAuthorizedShape(t, &forged, dbid, maxTX, "forged batch")

	sinceForged := time.Now()
	evil := dial(t, attacker, targetAddr, dbid, n0ID, divergent, "forged")
	handshakes++
	send(t, evil, &replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{forged}}, "forged")
	deadFrames := assertStreamDead(t, evil, honest, 0xF0, "forged batch")
	sawOnWire := false
	for i := range deadFrames {
		if deadFrames[i].Kind == replication.MsgError &&
			strings.Contains(deadFrames[i].ErrMessage, "untrusted transaction origin") {
			sawOnWire = true
			break
		}
	}
	sawInLatch := rec.checkOrQuery(cluster.Node(0), sinceForged, "untrusted transaction origin")
	if !sawOnWire && !sawInLatch {
		t.Fatal("node never rejected forged batch with untrusted-origin cause on wire or latch")
	}
	evil.Close()

	time.Sleep(200 * time.Millisecond)

	// Rejection counters unmoved; no attack batch applied.
	cAfter := getCounters(t, cluster.Node(0))
	if cAfter.staged != baseCounters.staged || cAfter.applied != baseCounters.applied {
		t.Fatalf("counters moved: staged %d->%d applied %d->%d",
			baseCounters.staged, cAfter.staged, baseCounters.applied, cAfter.applied)
	}
	if err := goharness.AssertRows(cluster, []int{0, 1},
		"SELECT name FROM records ORDER BY name", preAttack); err != nil {
		t.Fatalf("rows moved after forged batch: %v", err)
	}

	// --- P3: 256 MiB allocation-claim frame
	claim := dial(t, attacker, targetAddr, dbid, n0ID, divergent, "alloc-claim")
	handshakes++
	claim.CollectUntil(drainTimeout, isSchemaRequest)
	if err := claim.SendAllocClaim(batchesKind, 256<<20); err != nil {
		t.Fatalf("alloc-claim: send claim: %v", err)
	}
	assertStreamDead(t, claim, honest, 0xDEAD, "256MiB allocation claim")
	claim.Close()

	// Node survives: fresh session pings fine.
	probe := dial(t, attacker, targetAddr, dbid, n0ID, divergent, "probe-1")
	handshakes++
	expectPong(t, probe, 33, "probe after 256MiB claim")
	probe.Close()

	// --- P4: compressed snapshot bomb (zstd bomb)
	bomb := dial(t, attacker, targetAddr, dbid, n0ID, divergent, "bomb")
	handshakes++
	bombRaw := make([]byte, 4<<20)
	for i := range bombRaw {
		bombRaw[i] = 0xFF
	}
	compressed, err := compression.MustZstd(3).Compress(nil, bombRaw)
	if err != nil {
		t.Fatalf("bomb compress: %v", err)
	}
	if len(compressed) >= len(bombRaw)/4 {
		t.Fatalf("bomb did not compress: %d -> %d", len(bombRaw), len(compressed))
	}

	bomb.CollectUntil(drainTimeout, isSchemaRequest)
	body := []byte{
		byte(len(bombRaw) >> 24), byte(len(bombRaw) >> 16),
		byte(len(bombRaw) >> 8), byte(len(bombRaw)),
	}
	body = append(body, compressed...)
	if err := bomb.SendRaw(snapshotChunkKind, compressedFlag, body); err != nil {
		t.Fatalf("bomb: send raw: %v", err)
	}
	assertStreamDead(t, bomb, honest, 0xB0B, "snapshot bomb")
	bomb.Close()

	// Node survives: fresh session pings fine.
	probe2 := dial(t, attacker, targetAddr, dbid, n0ID, divergent, "probe-2")
	handshakes++
	expectPong(t, probe2, 44, "probe after snapshot bomb")
	probe2.Close()

	// --- P-HLC: far-future HLC batch
	hlc := dial(t, attacker, targetAddr, dbid, n0ID, divergent, "hlc")
	handshakes++
	if string(hlc.WelcomeSchema()) != string(realSchema) {
		t.Fatal("served schema changed")
	}

	farFuture := nowMillis() + 10*365*24*3600*1000
	hlcBatch := attackerTx(dbid, attackerID, 1, realDigest, []byte("far-future-hlc"), farFuture)
	assertRoundtrips(t, &hlcBatch, "far-future batch")
	assertAuthorizedShape(t, &hlcBatch, dbid, maxTX, "far-future batch")

	sinceHLC := time.Now()
	send(t, hlc, &replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{hlcBatch}}, "hlc")
	deadHLCFrames := assertStreamDead(t, hlc, honest, 55, "far-future batch")
	sawHLCOnWire := false
	for i := range deadHLCFrames {
		if deadHLCFrames[i].Kind == replication.MsgError &&
			strings.Contains(deadHLCFrames[i].ErrMessage, "untrusted transaction origin") {
			sawHLCOnWire = true
			break
		}
	}
	sawHLCInLatch := rec.checkOrQuery(cluster.Node(0), sinceHLC, "untrusted transaction origin")
	if !sawHLCOnWire && !sawHLCInLatch {
		t.Fatal("node never rejected far-future batch with untrusted-origin cause on wire or latch")
	}
	hlc.Close()

	if err := goharness.AssertRows(cluster, []int{0, 1},
		"SELECT name FROM records ORDER BY name", preAttack); err != nil {
		t.Fatalf("rows moved after far-future batch: %v", err)
	}

	// Positive control (post-attack): honest nodes stay healthy and replicate.
	finalProbe := dial(t, attacker, targetAddr, dbid, n0ID, divergent, "final-schema")
	handshakes++
	if string(finalProbe.WelcomeSchema()) != string(realSchema) {
		t.Fatal("served schema changed")
	}
	finalProbe.Close()

	if handshakes != 10 {
		t.Fatalf("handshake count drifted: %d", handshakes)
	}

	if _, err := cluster.Node(1).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(3), "post-attack-1"); err != nil {
		t.Fatalf("insert post-attack-1: %v", err)
	}
	if _, err := cluster.Node(1).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush post-attack-1: %v", err)
	}
	expected3 := [][]string{{"honest-1"}, {"honest-2"}, {"post-attack-1"}}
	if err := goharness.AssertRows(cluster, []int{0, 1},
		"SELECT name FROM records ORDER BY name", expected3); err != nil {
		t.Fatalf("converge post-attack-1: %v", err)
	}

	if _, err := cluster.Node(0).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", int64(4), "post-attack-2"); err != nil {
		t.Fatalf("insert post-attack-2: %v", err)
	}
	if _, err := cluster.Node(0).Simple("MURMUR FLUSH"); err != nil {
		t.Fatalf("flush post-attack-2: %v", err)
	}
	expected4 := [][]string{{"honest-1"}, {"honest-2"}, {"post-attack-1"}, {"post-attack-2"}}
	if err := goharness.AssertRows(cluster, []int{0, 1},
		"SELECT name FROM records ORDER BY name", expected4); err != nil {
		t.Fatalf("converge post-attack-2: %v", err)
	}
	failed = false
}
