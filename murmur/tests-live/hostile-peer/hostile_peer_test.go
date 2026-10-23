// Live scenario: a malicious authenticated peer's attacks are all rejected.
//
// Go port of the Rust `hostile-peer` scenario
// (/home/marc/MURMUR/tests-live/hostile-peer/tests/hostile_peer.rs:
// TestHostilePeerAttacksRejected). A malicious authenticated peer
// (CA-issued QUIC cert, untrusted origin key) fires forged batches,
// invalid identities, a replayed handshake, a 256 MiB allocation-claim
// frame, a compressed snapshot bomb, and a far-future-HLC batch at an
// honest 2-node mesh. Every attack must be rejected (observed on the wire
// and in product latches/counters, never assumed), nothing foreign may
// apply, and the honest pair must stay converged and responsive throughout.
//
// Plain SQL replaces the typed-records HTTP API, and the Prometheus
// counters map to replication Status observables.
package hostilepeer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
)

// attackerID is the fixed test id, asserted distinct from the node ids.
var attackerID = [16]byte{
	0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0,
	0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0,
}

// attackerSeed is the attacker origin signing key. CA-issued TLS cert, but
// this key is in NOBODY's trusted_origins, so every batch it signs must be
// refused as untrusted transaction origin.
var attackerSeed = [32]byte{
	0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
	0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
	0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
	0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
}

// divergentSchema holds divergent handshake schema bytes. Tolerated at
// attach (accept_remote_schema), never adopted without a signed manifest.
var divergentSchema = []byte("attacker-divergent-schema-v9999")

// Wire discriminants (proven by the control leg's Welcome).
const (
	batchesKind       uint16 = 3
	snapshotChunkKind uint16 = 8
)

// compressedFlag is the protocol compressed-frame flag.
const compressedFlag uint16 = 0x8002

const (
	// convergeTimeout is the convergence deadline per phase (Rust: 30 s).
	convergeTimeout = 30 * time.Second
	// convergePoll is the convergence poll interval (Rust: 200 ms).
	convergePoll = 200 * time.Millisecond
	// dialTimeout is the fixture dial/stream/setup deadline.
	dialTimeout = 10 * time.Second
	// welcomeTimeout is how long the fixture waits for a Welcome that must
	// arrive.
	welcomeTimeout = 5 * time.Second
	// errorTimeout is how long the fixture waits for an Error{code: 1}
	// reply (Rust: 10 s).
	errorTimeout = 10 * time.Second
	// pongTimeout is how long the fixture waits for a Pong that must
	// arrive (Rust: 10 s).
	pongTimeout = 10 * time.Second
	// slice is one gradual read inside a collection window (Rust: 500 ms).
	slice = 500 * time.Millisecond
	// drainTimeout is the opening-chatter drain while waiting for
	// attach-time requests.
	drainTimeout = 5 * time.Second
	// deathWindow is the post-fatal-probe silence window (Rust: 3 s).
	deathWindow = 3 * time.Second
	// peerTimeout is the session attach/teardown observation bound.
	peerTimeout = 5 * time.Second
)

// maxTX is the 1 MiB transaction cap: keeps the oversized leg small without
// affecting the tiny honest writes.
const maxTX = 1 << 20

// names returns the canonical ordered names on one node (Rust: TypedNames).
func names(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records ORDER BY name")
	})
	if err != nil {
		t.Fatalf("read names: %v", err)
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out
}

// rowCount returns the row count on one node.
func rowCount(t *testing.T, db *murmur.Database) int {
	t.Helper()
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT COUNT(*) FROM records")
	})
	if err != nil {
		t.Fatalf("row count: %v", err)
	}
	return int(v.([][]sqlite.Value)[0][0].Int)
}

// insert writes one single-row transaction (Rust: TypedInsert).
func insert(t *testing.T, cluster *harness.Cluster, node int, id int64, name string) {
	t.Helper()
	if _, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		t.Fatalf("node%d insert failed: %v", node, err)
	}
	if err := cluster.Node(node).Flush(); err != nil {
		t.Fatalf("node%d flush after insert: %v", node, err)
	}
}

// waitConverged waits until both honest nodes hold want rows with equal
// names (Rust: waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, convergePoll, func() bool {
		n0 := names(t, cluster.Node(0))
		n1 := names(t, cluster.Node(1))
		if len(n0) != want || len(n0) != len(n1) {
			return false
		}
		for i := range n0 {
			if n0[i] != n1[i] {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("nodes did not converge on %d rows with equal names within %v (node0=%v node1=%v)",
			want, timeout, names(t, cluster.Node(0)), names(t, cluster.Node(1)))
	}
}

// connectedPeers returns the connected replication sessions on one node.
func connectedPeers(cluster *harness.Cluster, idx int) int {
	h := cluster.Node(idx).Replication()
	if h == nil {
		return 0
	}
	return h.Status().Peers
}

// counters holds the staged (received) and applied counters on one node.
// received only moves when a batch passes verification into staging, so it
// is the must-not-move assert; applied covers the nothing-applied leg.
type counters struct {
	received uint64
	applied  uint64
}

func getCounters(cluster *harness.Cluster, idx int) counters {
	s := cluster.Node(idx).Replication().Status()
	return counters{received: s.Metrics.ReceivedTransactions, applied: s.AppliedTransactions}
}

func assertCounters(t *testing.T, cluster *harness.Cluster, idx int, base counters, ctx string) {
	t.Helper()
	got := getCounters(cluster, idx)
	if got.received != base.received || got.applied != base.applied {
		t.Fatalf("node%d counters moved %s: received %d -> %d, applied %d -> %d (an attack batch staged or applied)",
			idx+1, ctx, base.received, got.received, base.applied, got.applied)
	}
}

func nowMillis() uint64 {
	return uint64(time.Now().UnixMilli())
}

// attackerTx builds one attacker-signed transaction. The signature is
// always valid for attackerSeed; only cluster trust (or the deliberately
// broken identity field) is missing, so rejection is authorization shape,
// not malformedness.
func attackerTx(db, origin [16]byte, seq uint64, schema [32]byte, payload []byte, millis uint64) replication.Transaction {
	tx := replication.Transaction{
		Database: db,
		Origin:   origin,
		Sequence: seq,
		ID:       [16]byte{0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0},
		Millis:   millis,
		Schema:   schema,
		Payload:  payload,
	}
	tx.Sign(&attackerSeed)
	return tx
}

// assertRoundtrips is the non-vacuous guard: the payload encode/decode
// round-trips, so live rejection is product validation, not a corrupt test
// payload.
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

// assertAuthorizedShape is the non-vacuous guard: the transaction verifies
// cleanly against a trust map that DOES contain the attacker key, proving
// the live rejection is origin authorization (missing cluster trust), not a
// bad signature or malformed envelope.
func assertAuthorizedShape(t *testing.T, tx *replication.Transaction, db [16]byte, max int, what string) {
	t.Helper()
	trust := map[[16]byte][32]byte{attackerID: replication.PublicKey(&attackerSeed)}
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

// lastErrorText returns the text of the last Error frame seen.
func lastErrorText(frames []replication.Message) (uint16, string, bool) {
	for i := len(frames) - 1; i >= 0; i-- {
		if frames[i].Kind == replication.MsgError {
			return frames[i].Code, frames[i].ErrMessage, true
		}
	}
	return 0, "", false
}

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
	default:
		return "Unknown"
	}
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

// assertNoForeignData asserts every data frame on the attacker session
// carries honest origins only: nothing attacker-controlled is ever echoed
// or relayed back. (The engine does not data-gate divergent-schema peers,
// so "no data frames" cannot be asserted.)
func assertNoForeignData(t *testing.T, frames []replication.Message, honest [][16]byte, ctx string) {
	t.Helper()
	isHonest := func(origin [16]byte) bool {
		for _, h := range honest {
			if h == origin {
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
					t.Fatalf("attacker received a batch with foreign origin %x %s", tx.Origin, ctx)
				}
			}
		case replication.MsgPlumtreeData:
			if !isHonest(frames[i].Tx.Origin) {
				t.Fatalf("attacker received plumtree data with foreign origin %x %s", frames[i].Tx.Origin, ctx)
			}
		case replication.MsgTransactionChunk:
			if !isHonest(frames[i].Chunk.Proof.Origin) {
				t.Fatalf("attacker received a chunk with foreign origin %x %s", frames[i].Chunk.Proof.Origin, ctx)
			}
		}
	}
}

// attacker is the raw QUIC attacker speaking the replication wire protocol
// with a CA-issued certificate. It shares the cluster CA trust, so TLS can
// never be the refusal cause.
type attacker struct {
	tlsConf  *tls.Config
	quicConf *quic.Config
	db       [16]byte
}

func newAttacker(t *testing.T, creds replication.Credentials, db [16]byte) *attacker {
	t.Helper()
	key, err := x509.ParsePKCS8PrivateKey(creds.PrivateKey)
	if err != nil {
		t.Fatalf("parse attacker key: %v", err)
	}
	leaf, err := x509.ParseCertificate(creds.Chain[0])
	if err != nil {
		t.Fatalf("parse attacker leaf: %v", err)
	}
	roots := x509.NewCertPool()
	for _, der := range creds.Roots {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatalf("parse trust root: %v", err)
		}
		roots.AddCert(cert)
	}
	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: creds.Chain,
			PrivateKey:  key,
			Leaf:        leaf,
		}},
		ServerName:         "murmur.invalid",
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{string(replication.ALPN)},
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("missing peer certificate")
			}
			leaf, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return err
			}
			intermediates := x509.NewCertPool()
			for _, raw := range rawCerts[1:] {
				if cert, err := x509.ParseCertificate(raw); err == nil {
					intermediates.AddCert(cert)
				}
			}
			if _, err := leaf.Verify(x509.VerifyOptions{
				Roots:         roots,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			}); err != nil {
				return err
			}
			_, err = replication.NodeID(rawCerts[0])
			return err
		},
	}
	return &attacker{
		tlsConf: tlsConf,
		quicConf: &quic.Config{
			MaxIdleTimeout:        30 * time.Second,
			KeepAlivePeriod:       10 * time.Second,
			MaxIncomingStreams:    40,
			MaxIncomingUniStreams: -1,
			EnableDatagrams:       true,
		},
		db: db,
	}
}

func (a *attacker) hello(schema []byte) replication.Message {
	return replication.Message{
		Kind:           replication.MsgHello,
		Capabilities:   replication.Capabilities,
		Required:       replication.Required,
		Version:        replication.Version,
		Database:       a.db,
		Node:           attackerID,
		Schema:         schema,
		Mode:           replication.DisseminationGossip,
		MaxTransaction: 64 << 20,
	}
}

type inboundMsg struct {
	msg replication.Message
	err error
}

// evilSession is one attack session: the handshake stream stays open for
// further attack frames and for observing node replies. A reader goroutine
// owns the stream so framing is never split across read deadlines.
type evilSession struct {
	conn          *quic.Conn
	stream        *quic.Stream
	welcomeSchema []byte
	inbound       chan inboundMsg
}

// dial advertises the given schema identity; the Welcome assert is the
// attach proof (every dial carries its own proof).
func (a *attacker) dial(t *testing.T, addr netip.AddrPort, schema []byte, what string) *evilSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	conn, err := quic.DialAddr(ctx, addr.String(), a.tlsConf, a.quicConf)
	cancel()
	if err != nil {
		t.Fatalf("%s: attacker connect: %v", what, err)
	}
	openCtx, openCancel := context.WithTimeout(context.Background(), dialTimeout)
	stream, err := conn.OpenStreamSync(openCtx)
	openCancel()
	if err != nil {
		_ = conn.CloseWithError(0, what+" dial failed")
		t.Fatalf("%s: attacker open stream: %v", what, err)
	}
	hello := a.hello(schema)
	if err := replication.WriteMessage(stream, &hello); err != nil {
		_ = conn.CloseWithError(0, what+" dial failed")
		t.Fatalf("%s: attacker write hello: %v", what, err)
	}
	inbound := make(chan inboundMsg, 1024)
	go func() {
		defer close(inbound)
		for {
			msg, err := replication.ReadMessage(stream)
			inbound <- inboundMsg{msg: msg, err: err}
			if err != nil {
				return
			}
		}
	}()
	select {
	case in, ok := <-inbound:
		if !ok || in.err != nil {
			t.Fatalf("%s: attacker expected Welcome, stream died: %v", what, in.err)
		}
		if in.msg.Kind != replication.MsgWelcome {
			t.Fatalf("%s: attacker expected Welcome, got %s", what, msgName(&in.msg))
		}
		return &evilSession{conn: conn, stream: stream, welcomeSchema: append([]byte(nil), in.msg.Schema...), inbound: inbound}
	case <-time.After(welcomeTimeout):
		_ = conn.CloseWithError(0, what+" dial failed")
		t.Fatalf("%s: attacker Welcome timeout", what)
		return nil
	}
}

func (s *evilSession) send(t *testing.T, msg *replication.Message, what string) {
	t.Helper()
	if err := replication.WriteMessage(s.stream, msg); err != nil {
		t.Fatalf("%s: attacker send %s: %v", what, msgName(msg), err)
	}
}

// collect reads inbound messages until total elapses or the stream dies.
func (s *evilSession) collect(total time.Duration) []replication.Message {
	return s.collectUntil(total, func(*replication.Message) bool { return false })
}

// collectUntil reads until want matches, total elapses, or the stream dies.
func (s *evilSession) collectUntil(total time.Duration, want func(*replication.Message) bool) []replication.Message {
	var out []replication.Message
	deadline := time.Now().Add(total)
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining > slice {
			remaining = slice
		}
		select {
		case in, ok := <-s.inbound:
			if !ok {
				return out // stream died
			}
			if in.err != nil {
				return out
			}
			hit := want(&in.msg)
			out = append(out, in.msg)
			if hit {
				return out
			}
		case <-time.After(remaining):
		}
	}
	return out
}

// sendRaw writes one raw frame with an explicit body (mirroring the 12-byte
// header layout).
func (s *evilSession) sendRaw(t *testing.T, kind, flags uint16, body []byte, what string) {
	t.Helper()
	var h [12]byte
	copy(h[:2], "RD")
	binary.BigEndian.PutUint16(h[2:4], replication.Version)
	binary.BigEndian.PutUint16(h[4:6], kind)
	binary.BigEndian.PutUint16(h[6:8], flags)
	binary.BigEndian.PutUint32(h[8:12], uint32(len(body)))
	if _, err := s.stream.Write(h[:]); err != nil {
		t.Fatalf("%s: raw header write: %v", what, err)
	}
	if _, err := s.stream.Write(body); err != nil {
		t.Fatalf("%s: raw body write: %v", what, err)
	}
}

// sendAllocClaim writes a bare 12-byte header claiming claimedLen with no
// body behind it.
func (s *evilSession) sendAllocClaim(t *testing.T, kind uint16, claimedLen uint32, what string) {
	t.Helper()
	var h [12]byte
	copy(h[:2], "RD")
	binary.BigEndian.PutUint16(h[2:4], replication.Version)
	binary.BigEndian.PutUint16(h[4:6], kind)
	binary.BigEndian.PutUint16(h[6:8], 0)
	binary.BigEndian.PutUint32(h[8:12], claimedLen)
	if _, err := s.stream.Write(h[:]); err != nil {
		t.Fatalf("%s: evil header write: %v", what, err)
	}
}

// close queues the CLOSE frame and sleeps so it flushes; the caller's
// teardown gate still asserts node-side.
func (s *evilSession) close(what string) {
	_ = s.conn.CloseWithError(0, what+" done")
	time.Sleep(500 * time.Millisecond)
}

// recorder is the background last_error recorder. Honest nodes may dial
// the attacker address back on the bootstrap tick and those stray dials
// time out into handshake-timeout latches that can overwrite a single
// sample, so Auth-cause legs record every latch sample on a 10 ms poll and
// match by timestamp instead.
type recorder struct {
	mu   sync.Mutex
	log  []recordedError
	stop atomic.Bool
	wg   sync.WaitGroup
}

type recordedError struct {
	at  time.Time
	err string
}

func startRecorder(cluster *harness.Cluster, idx int) *recorder {
	r := &recorder{}
	handle := cluster.Node(idx).Replication()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for !r.stop.Load() {
			// Log every sample, not just transitions: consecutive legs
			// can latch the identical string, and a transitions-only log
			// cannot prove the repeat happened after since.
			cur := ""
			if lastErr := handle.Status().LastError; lastErr != nil {
				cur = *lastErr
			}
			r.mu.Lock()
			r.log = append(r.log, recordedError{at: time.Now(), err: cur})
			r.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return r
}

func (r *recorder) sawSince(since time.Time, needle string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.log {
		if !e.at.Before(since) && strings.Contains(e.err, needle) {
			return true
		}
	}
	return false
}

func (r *recorder) stopRecorder() {
	r.stop.Store(true)
	r.wg.Wait()
}

// assertStreamDead pings a stream a fatal probe should have killed and
// asserts the node never answers: no pong (the ping was never processed)
// and no foreign data. Stray repeats of attach-time control chatter are
// tolerated but logged.
func assertStreamDead(t *testing.T, sess *evilSession, honest [][16]byte, nonce uint64, what string) {
	t.Helper()
	ping := replication.Message{Kind: replication.MsgPing, Nonce: nonce}
	_ = replication.WriteMessage(sess.stream, &ping)
	frames := sess.collect(deathWindow)
	for i := range frames {
		if frames[i].Kind == replication.MsgPong && frames[i].Nonce == nonce {
			t.Fatalf("node answered ping after %s (stream should be dead)", what)
		}
	}
	assertNoForeignData(t, frames, honest, "after "+what)
	t.Logf("after %s the node sent only: %s", what, frameSummary(frames))
}

// expectProtocolError expects one Error{code: 1} whose text contains needle.
// Returns all frames seen.
func expectProtocolError(t *testing.T, sess *evilSession, needle, what string) []replication.Message {
	t.Helper()
	frames := sess.collectUntil(errorTimeout, isErrorCode(1))
	if code, text, ok := lastErrorText(frames); !ok || code != 1 || !strings.Contains(text, needle) {
		t.Fatalf("%s drew no Error{code: 1, ..%s..}; saw code=%d text=%q ok=%v", what, needle, code, text, ok)
	}
	return frames
}

// expectPong expects a pong to nonce.
func expectPong(t *testing.T, sess *evilSession, nonce uint64, what string) []replication.Message {
	t.Helper()
	ping := replication.Message{Kind: replication.MsgPing, Nonce: nonce}
	sess.send(t, &ping, what)
	frames := sess.collectUntil(pongTimeout, isPong(nonce))
	saw := false
	for i := range frames {
		if frames[i].Kind == replication.MsgPong && frames[i].Nonce == nonce {
			saw = true
			break
		}
	}
	if !saw {
		t.Fatalf("missing pong to %s; saw %s", what, frameSummary(frames))
	}
	return frames
}

func TestHostilePeerAttacksRejected(t *testing.T) {
	// Fixed node ids (asserted distinct from the attacker id) so no
	// handshake identity can alias.
	nodeIDs := [][16]byte{
		{0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10},
		{0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20},
	}
	for _, id := range nodeIDs {
		if id == attackerID {
			t.Fatal("attacker id collides with a node id")
		}
	}
	maxTransactionBytes := maxTX
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "hostile-peer",
		NumNodes: 2,
		NodeIDs:  nodeIDs,
		Replication: &harness.ReplicationOptions{
			MaxTransactionBytes: &maxTransactionBytes,
		},
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("hostile-peer")
		}
		cluster.Finish(failed)
	}()

	// Attacker aims at node0; node1 is the honest witness.
	const target = 0
	honest := [][16]byte{cluster.NodeID(0), cluster.NodeID(1)}
	dbid := cluster.DatabaseID()

	// Same schema on every node.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			t.Fatalf("migrate records table: %v", err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("records", true); err != nil {
			t.Fatalf("opt-in records: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush opt-in: %v", err)
		}
	}

	// Positive control (pre-attack): honest writes replicate both ways.
	insert(t, cluster, 0, 1, "honest-1")
	waitConverged(t, cluster, 1, convergeTimeout)
	insert(t, cluster, 1, 2, "honest-2")
	waitConverged(t, cluster, 2, convergeTimeout)
	// Quiesce the staging/apply lag before baselining: the applied counter
	// increments after the SQLite commit, so a baseline taken in between
	// observes converged rows with a stale counter, and the late increment
	// then trips the must-not-move assert below. Wait until every staged
	// honest transaction is counted as applied.
	if ok := harness.WaitUntil(convergeTimeout, 10*time.Millisecond, func() bool {
		settled := getCounters(cluster, target)
		return settled.applied == settled.received
	}); !ok {
		t.Fatal("honest backlog never drained before attacks")
	}
	preAttack := names(t, cluster.Node(target))
	base := getCounters(cluster, target)

	// Attacker fixture sharing the cluster CA trust.
	issued, err := cluster.IssueCert(attackerID)
	if err != nil {
		t.Fatalf("issue attacker cert: %v", err)
	}
	atk := newAttacker(t, issued.Credentials, dbid)
	nodeAddr := cluster.NodeInfo(target).ReplAddr
	divergent := append([]byte(nil), divergentSchema...)
	var handshakes uint32

	// Learn the node's real schema bytes from a divergent dial.
	learn := atk.dial(t, nodeAddr, divergent, "schema-learn")
	handshakes++
	realSchema := append([]byte(nil), learn.welcomeSchema...)
	if string(realSchema) == string(divergent) {
		t.Fatal("node schema equals the divergent bytes (leg vacuous)")
	}
	realDigest := replication.SchemaDigest(realSchema)
	if ok := harness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		return connectedPeers(cluster, target) == 2
	}); !ok {
		t.Fatal("learn session never registered before teardown")
	}
	learn.close("schema-learn")
	if ok := harness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		return connectedPeers(cluster, target) == 1
	}); !ok {
		t.Fatal("learn session did not tear down before the control")
	}

	// Positive control: the same fixture with the node's real schema MUST
	// be welcomed with no SchemaRequest. Proves TLS trust, identity, the
	// hand-rolled framer, and the matched-schema bytes are all correct, so
	// later rejections are caused by the attacks alone.
	control := atk.dial(t, nodeAddr, realSchema, "control")
	handshakes++
	controlFrames := control.collect(2 * time.Second)
	for i := range controlFrames {
		if controlFrames[i].Kind == replication.MsgSchemaRequest {
			t.Fatalf("control session drew a SchemaRequest (schema bytes wrong); saw %s", frameSummary(controlFrames))
		}
	}
	pongFrames := expectPong(t, control, 7, "control ping")
	assertNoForeignData(t, pongFrames, honest, "on the control session")
	control.close("control")
	if ok := harness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		return connectedPeers(cluster, target) == 1
	}); !ok {
		t.Fatal("control session did not tear down before the attacks")
	}
	t.Log("control: matched-schema hello welcomed (fixture identity OK)")

	// Latch recorder for the Auth-cause legs (see recorder).
	rec := startRecorder(cluster, target)
	defer rec.stopRecorder()

	// --- P1a: invalid identities on one watched divergent session.
	// All four are protocol errors: each must draw Error{code: 1} with the
	// session surviving.
	watched := atk.dial(t, nodeAddr, divergent, "watched")
	handshakes++
	if ok := harness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		return connectedPeers(cluster, target) == 2
	}); !ok {
		t.Fatal("attack handshake never attached (peers != 2)")
	}
	var seen []replication.Message
	var forgedDigest [32]byte
	for i := range forgedDigest {
		forgedDigest[i] = 0xA0
	}

	// Zero sequence.
	zeroSeq := attackerTx(dbid, attackerID, 0, forgedDigest, []byte("zero-seq"), nowMillis())
	assertRoundtrips(t, &zeroSeq, "zero-sequence batch")
	zeroMsg := replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{zeroSeq}}
	watched.send(t, &zeroMsg, "zero-seq")
	seen = append(seen, expectProtocolError(t, watched, "transaction identity", "zero-sequence batch")...)

	// Wrong database id.
	var wrongDBID [16]byte
	for i := range wrongDBID {
		wrongDBID[i] = 0xDD
	}
	wrongDB := attackerTx(wrongDBID, attackerID, 7, forgedDigest, []byte("wrong-db"), nowMillis())
	assertRoundtrips(t, &wrongDB, "wrong-database batch")
	wrongMsg := replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{wrongDB}}
	watched.send(t, &wrongMsg, "wrong-db")
	seen = append(seen, expectProtocolError(t, watched, "transaction identity", "wrong-database batch")...)

	// Oversized payload: 2 MiB against the 1 MiB cap.
	bigPayload := bytes2MiB()
	big := attackerTx(dbid, attackerID, 8, forgedDigest, bigPayload, nowMillis())
	assertRoundtrips(t, &big, "oversized batch")
	bigMsg := replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{big}}
	watched.send(t, &bigMsg, "oversized")
	seen = append(seen, expectProtocolError(t, watched, "transaction identity", "oversized batch")...)

	// Invalid missing range (stands in for the zero-HLC leg: the verifier
	// does not gate on HLC, so a structurally valid frame with invalid
	// content takes its place).
	badNeed := replication.Message{Kind: replication.MsgNeed, Origin: attackerID, From: 0, Through: 5}
	watched.send(t, &badNeed, "bad-need")
	seen = append(seen, expectProtocolError(t, watched, "invalid missing range", "zero-from Need")...)

	// Session must be alive: ping round-trips.
	seen = append(seen, expectPong(t, watched, 11, "ping-1 after invalid batches")...)

	// --- P2: replay the handshake on the live session. Hello is only
	// valid as the first frame; mid-session it must draw an error without
	// killing the session.
	replay := atk.hello(divergent)
	watched.send(t, &replay, "replay-hello")
	seen = append(seen, expectProtocolError(t, watched, "repeated handshake", "replayed handshake")...)

	// Session still alive after the replay.
	seen = append(seen, expectPong(t, watched, 22, "ping-2 after replayed handshake")...)
	// Final drain: give the node a window to (wrongly) push foreign data.
	seen = append(seen, watched.collect(2*time.Second)...)
	assertNoForeignData(t, seen, honest, "on the watched session")
	t.Logf("watched session carried only honest/control frames: %s", frameSummary(seen))
	watched.close("watched")
	if ok := harness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		return connectedPeers(cluster, target) == 1
	}); !ok {
		t.Fatal("watched session did not tear down")
	}

	// --- P1b: forged matched-schema batch from the untrusted origin.
	// Correctly signed and schema-matched, so only origin trust is
	// missing; the auth error kills the session after the latch (hence the
	// separate session).
	forged := attackerTx(dbid, attackerID, 1, realDigest, []byte("forged"), nowMillis())
	assertRoundtrips(t, &forged, "forged batch")
	assertAuthorizedShape(t, &forged, dbid, maxTX, "forged batch")
	sinceForged := time.Now()
	evil := atk.dial(t, nodeAddr, divergent, "forged")
	handshakes++
	forgedMsg := replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{forged}}
	evil.send(t, &forgedMsg, "forged")
	assertStreamDead(t, evil, honest, 0xF0, "forged batch")
	if ok := harness.WaitUntil(peerTimeout, 10*time.Millisecond, func() bool {
		return rec.sawSince(sinceForged, "untrusted transaction origin")
	}); !ok {
		t.Fatal("node never latched the untrusted-origin cause for the forged batch")
	}
	evil.close("forged")
	if ok := harness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		return connectedPeers(cluster, target) == 1
	}); !ok {
		t.Fatal("forged session did not tear down")
	}

	// Rejection counters unmoved; no attack batch applied.
	assertCounters(t, cluster, target, base, "after forged-batch attacks")
	if got := names(t, cluster.Node(target)); !equalStrings(got, preAttack) {
		t.Fatalf("node1 names changed after forged-batch attacks: %v", got)
	}
	if got := rowCount(t, cluster.Node(target)); got != 2 {
		t.Fatalf("node1 row count changed after forged-batch attacks: %d", got)
	}
	if got := names(t, cluster.Node(1)); !equalStrings(got, preAttack) {
		t.Fatalf("witness names changed after forged-batch attacks: %v", got)
	}

	// --- P3: 256 MiB allocation-claim frame. Must fail fast without a
	// huge alloc; the stream dies and the node stays up.
	claim := atk.dial(t, nodeAddr, divergent, "alloc-claim")
	handshakes++
	// Drain the opening control chatter so post-attack reads are not
	// polluted by it.
	claim.collectUntil(drainTimeout, isSchemaRequest)
	claim.sendAllocClaim(t, batchesKind, 256<<20, "alloc-claim")
	assertStreamDead(t, claim, honest, 0xDEAD, "256MiB allocation claim")
	claim.close("alloc-claim")
	// The node itself survives: a fresh session pings fine.
	probe := atk.dial(t, nodeAddr, divergent, "probe-1")
	handshakes++
	expectPong(t, probe, 33, "probe after 256MiB claim")
	probe.close("probe-1")

	// --- P4: compressed snapshot bomb. A tiny zstd frame expanding to
	// megabytes of undecodable chunk bytes; the stream must die, not the
	// node.
	bomb := atk.dial(t, nodeAddr, divergent, "bomb")
	handshakes++
	bombRaw := make([]byte, 4<<20)
	for i := range bombRaw {
		bombRaw[i] = 0xFF
	}
	zstd := compression.MustZstd(3)
	compressed, err := zstd.Compress(nil, bombRaw)
	if err != nil {
		t.Fatalf("bomb compress: %v", err)
	}
	if len(compressed) >= len(bombRaw)/4 {
		t.Fatalf("bomb does not compress: %d -> %d", len(bombRaw), len(compressed))
	}
	// Non-vacuous guard: the bomb decompresses locally but never decodes
	// as a message, so rejection is by product bounds/validation, not a
	// corrupt test payload.
	plain, err := zstd.Decompress(nil, compressed, len(bombRaw)+1)
	if err != nil {
		t.Fatalf("bomb decompress: %v", err)
	}
	if len(plain) != len(bombRaw) {
		t.Fatalf("bomb size changed locally: %d", len(plain))
	}
	if _, err := replication.DecodeMessage(plain); err == nil {
		t.Fatal("bomb unexpectedly decodes as a message")
	}
	bomb.collectUntil(drainTimeout, isSchemaRequest)
	body := make([]byte, 4, 4+len(compressed))
	binary.BigEndian.PutUint32(body, uint32(len(bombRaw)))
	body = append(body, compressed...)
	bomb.sendRaw(t, snapshotChunkKind, compressedFlag, body, "bomb")
	assertStreamDead(t, bomb, honest, 0xB0B, "snapshot bomb")
	bomb.close("bomb")
	probe2 := atk.dial(t, nodeAddr, divergent, "probe-2")
	handshakes++
	expectPong(t, probe2, 44, "probe after snapshot bomb")
	probe2.close("probe-2")

	// --- P-HLC: unsigned far-future input from an untrusted origin must be
	// rejected before HLC observation (stage verifies before observe); the
	// mesh stays responsive.
	hlc := atk.dial(t, nodeAddr, divergent, "hlc")
	handshakes++
	// The served schema is stable: the Welcome bytes match the pre-attack
	// capture.
	if string(hlc.welcomeSchema) != string(realSchema) {
		t.Fatal("served schema moved during the attacks")
	}
	farFuture := nowMillis() + 10*365*24*3600*1000
	hlcBatch := attackerTx(dbid, attackerID, 1, realDigest, []byte("far-future-hlc"), farFuture)
	assertRoundtrips(t, &hlcBatch, "far-future batch")
	assertAuthorizedShape(t, &hlcBatch, dbid, maxTX, "far-future batch")
	sinceHLC := time.Now()
	hlcMsg := replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{hlcBatch}}
	hlc.send(t, &hlcMsg, "hlc")
	// Round-trip before closing: the ordered stream guarantees the node
	// processed the batch before the death assert runs (closing immediately
	// could drop the batch in flight).
	assertStreamDead(t, hlc, honest, 55, "far-future batch")
	if ok := harness.WaitUntil(peerTimeout, 10*time.Millisecond, func() bool {
		return rec.sawSince(sinceHLC, "untrusted transaction origin")
	}); !ok {
		t.Fatal("node never latched the untrusted-origin cause for the far-future batch")
	}
	hlc.close("hlc")
	// The untrusted origin is rejected before HLC observation; the honest
	// pair stays unchanged.
	waitConverged(t, cluster, 2, convergeTimeout)

	// Every attack handshake attached (each dial asserts its own Welcome;
	// peers == 2 was observed on the watched session).
	if handshakes != 9 {
		t.Fatalf("handshake count drifted (attach proof): %d", handshakes)
	}
	t.Logf("%d attack handshakes attached and were answered", handshakes)

	// --- Positive control (post-attack): the pair stays responsive and
	// converged; schema identity untouched by the attacks.
	finalProbe := atk.dial(t, nodeAddr, divergent, "final-schema")
	handshakes++
	if string(finalProbe.welcomeSchema) != string(realSchema) {
		t.Fatal("schema bytes moved during the attacks")
	}
	finalProbe.close("final-schema")
	if handshakes != 10 {
		t.Fatalf("handshake count drifted (attach proof): %d", handshakes)
	}
	assertCounters(t, cluster, target, base, "across all attacks")
	if got := names(t, cluster.Node(target)); !equalStrings(got, preAttack) {
		t.Fatalf("node1 names changed across the attacks: %v", got)
	}
	insert(t, cluster, 1, 3, "post-attack-1")
	waitConverged(t, cluster, 3, convergeTimeout)
	insert(t, cluster, 0, 4, "post-attack-2")
	waitConverged(t, cluster, 4, convergeTimeout)

	failed = false
}

func bytes2MiB() []byte {
	out := make([]byte, 2<<20)
	for i := range out {
		out[i] = 0x55
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
