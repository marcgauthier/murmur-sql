// Live scenario: forwarded origin survives an offline origin and a
// hostile relay.
//
// Go port of the Rust `origin-signatures` scenario
// (/home/marc/MURMUR/tests-live/origin-signatures/tests/origin_signatures.rs),
// itself a port of the original Go `origin-signatures` scenario
// (TestForwardedOriginSurvivesOfflineOriginAndHostileRelay). Three
// encrypted nodes share one database id. A commits a transaction, B
// receives it, A stops, and C receives A's transaction exclusively
// through B. The fixture checks the actual stored origin proof, then a
// malicious relay authenticated as B sends modified, fabricated,
// unsigned, cross-DBID and chunk-identity attacks at C. Every attack
// must be rejected while rows, generation and receipts stay unchanged.
// Restart plus signed-duplicate replay check durability and idempotency,
// and an untrusted snapshot manifest must be refused.
//
// Plain SQL replaces the typed-records HTTP API. The rejection-counter
// legs map to per-leg wire + `last_error` latch evidence (the engine
// keeps no invalid-batch counters), and each signature-failure leg uses
// its own relay session because an auth failure kills the session that
// carried it.
package originsignatures

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/tests-live/harness"
	"github.com/quic-go/quic-go"
)

// Fixed node ids (the harness mints the seeds).
var (
	nodeA = [16]byte{0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10}
	nodeB = [16]byte{0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20}
	nodeC = [16]byte{0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30}
)

const (
	a = 0
	b = 1
	c = 2
)

// honest origins; data frames on relay sessions must never carry any
// other origin.
var honest = [][16]byte{nodeA, nodeB, nodeC}

// fabricateSeed re-signs the fabricated leg. The harness never
// discloses node seeds, so a dedicated attacker key takes B's origin
// key place (the check exercised is identical: the signature must fail
// against the claimed origin's trusted key).
var fabricateSeed = [32]byte{
	0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
	0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
	0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
	0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
}

// divergentSchema seeds the first relay dial (its Welcome teaches the
// relay the node's real schema).
var divergentSchema = []byte("relay-divergent-schema-probe")

// crossDB is the cross-DBID attack database id.
var crossDB = [16]byte{
	0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD,
	0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD, 0xDD,
}

// Distinct transaction ids per attack leg.
var (
	modifiedID   = [16]byte{0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1}
	fabricatedID = [16]byte{0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2, 0xB2}
	unsignedID   = [16]byte{0xB3, 0xB3, 0xB3, 0xB3, 0xB3, 0xB3, 0xB3, 0xB3, 0xB3, 0xB3, 0xB3, 0xB3, 0xB3, 0xB3, 0xB3, 0xB3}
	crossDBID    = [16]byte{0xB4, 0xB4, 0xB4, 0xB4, 0xB4, 0xB4, 0xB4, 0xB4, 0xB4, 0xB4, 0xB4, 0xB4, 0xB4, 0xB4, 0xB4, 0xB4}
	chunkID      = [16]byte{0xB5, 0xB5, 0xB5, 0xB5, 0xB5, 0xB5, 0xB5, 0xB5, 0xB5, 0xB5, 0xB5, 0xB5, 0xB5, 0xB5, 0xB5, 0xB5}
)

// maxTx is the local verification cap (the product default).
const maxTx = 64 << 20

const (
	// convergeTimeout bounds each convergence phase (Rust: 30s).
	convergeTimeout = 30 * time.Second
	// dialTimeout bounds fixture dial/stream/setup (Rust: 10s).
	dialTimeout = 10 * time.Second
	// welcomeTimeout bounds the wait for a Welcome that must arrive.
	welcomeTimeout = 5 * time.Second
	// needWindow bounds the served `Need` batches.
	needWindow = 5 * time.Second
	// errorTimeout bounds the wait for an `Error{code: 1}` reply.
	errorTimeout = 10 * time.Second
	// pongTimeout bounds the wait for a Pong that must arrive.
	pongTimeout = 10 * time.Second
	// probeTimeout bounds the wait for a chunk-availability page.
	probeTimeout = 10 * time.Second
	// slice is one gradual read inside a collection window.
	slice = 500 * time.Millisecond
	// deathWindow is the post-fatal-probe silence window (Rust: 3s).
	deathWindow = 3 * time.Second
	// peerTimeout bounds session attach/teardown observation.
	peerTimeout = 5 * time.Second
)

// queryNames reads canonical ordered names on one node.
func queryNames(db *murmur.Database) ([]string, error) {
	v, err := db.Read(func(s *murmur.Sql) (any, error) {
		return s.Query("SELECT name FROM records ORDER BY name")
	})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, row := range v.([][]sqlite.Value) {
		out = append(out, row[0].Str)
	}
	return out, nil
}

// mustNames reads canonical ordered names, failing the test on error.
func mustNames(t *testing.T, db *murmur.Database) []string {
	t.Helper()
	names, err := queryNames(db)
	if err != nil {
		t.Fatalf("read names: %v", err)
	}
	return names
}

// insert writes one single-row transaction.
func insert(t *testing.T, cluster *harness.Cluster, node int, id int64, name string) {
	t.Helper()
	_, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	})
	if err != nil {
		t.Fatalf("node %d insert failed: %v", node, err)
	}
}

// waitNames waits until node holds exactly want.
func waitNames(t *testing.T, cluster *harness.Cluster, node int, want []string, timeout time.Duration) {
	t.Helper()
	ok := harness.WaitUntil(timeout, 200*time.Millisecond, func() bool {
		got, err := queryNames(cluster.Node(node))
		if err != nil || len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("node%d did not converge on %q within %v (has %q)",
			node+1, want, timeout, mustNames(t, cluster.Node(node)))
	}
}

// connectedPeers reads connected replication sessions on one node.
func connectedPeers(t *testing.T, cluster *harness.Cluster, idx int) int {
	t.Helper()
	h := cluster.Node(idx).Replication()
	if h == nil {
		t.Fatalf("node%d has no replication handle", idx+1)
	}
	return h.Status().Peers
}

// counters are the staged (`received`) and applied counters on one
// node. `received` only moves when a batch passes verification into
// staging; `applied` covers the nothing-applied legs.
type counters struct {
	received uint64
	applied  uint64
}

func queryCounters(t *testing.T, cluster *harness.Cluster, idx int) counters {
	t.Helper()
	h := cluster.Node(idx).Replication()
	if h == nil {
		t.Fatalf("node%d has no replication handle", idx+1)
	}
	s := h.Status()
	return counters{received: s.Metrics.ReceivedTransactions, applied: s.AppliedTransactions}
}

// waitBacklogDrained waits until staged and applied counters agree (no
// backlog) and hold still across consecutive polls. Attack baselines
// must be quiesced: a legit batch staged-but-unapplied at baseline
// applies late and misattributes as an attack batch staging or
// applying.
func waitBacklogDrained(t *testing.T, cluster *harness.Cluster, idx int, ctx string) {
	t.Helper()
	deadline := time.Now().Add(convergeTimeout)
	stable := 0
	var last counters
	lastSet := false
	for {
		got := queryCounters(t, cluster, idx)
		if got.received == got.applied && lastSet && got == last {
			stable++
			if stable >= 2 {
				return
			}
		} else {
			stable = 0
			last = got
			lastSet = true
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s: backlog never drained (received %d, applied %d)",
				ctx, got.received, got.applied)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func assertCounters(t *testing.T, cluster *harness.Cluster, idx int, base counters, ctx string) {
	t.Helper()
	got := queryCounters(t, cluster, idx)
	if got != base {
		t.Fatalf("node%d counters moved %s: received %d -> %d, applied %d -> %d (an attack batch staged or applied)",
			idx+1, ctx, base.received, got.received, base.applied, got.applied)
	}
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

// assertNoForeignData requires every data frame on a relay session to
// carry honest origins only: nothing attacker-controlled is ever
// echoed or relayed back.
func assertNoForeignData(t *testing.T, frames []replication.Message, ctx string) {
	t.Helper()
	for i := range frames {
		f := &frames[i]
		switch f.Kind {
		case replication.MsgBatches:
			for j := range f.Transactions {
				if !isHonest(f.Transactions[j].Origin) {
					t.Fatalf("relay received a batch with foreign origin %x %s",
						f.Transactions[j].Origin, ctx)
				}
			}
		case replication.MsgPlumtreeData:
			if !isHonest(f.Tx.Origin) {
				t.Fatalf("relay received plumtree data with foreign origin %x %s",
					f.Tx.Origin, ctx)
			}
		case replication.MsgTransactionChunk:
			if !isHonest(f.Chunk.Proof.Origin) {
				t.Fatalf("relay received a chunk with foreign origin %x %s",
					f.Chunk.Proof.Origin, ctx)
			}
		}
	}
}

func isHonest(id [16]byte) bool {
	for _, h := range honest {
		if h == id {
			return true
		}
	}
	return false
}

// lastErrorText returns the last `Error` frame seen, if any.
func lastErrorText(frames []replication.Message) (uint16, string, bool) {
	for i := len(frames) - 1; i >= 0; i-- {
		if frames[i].Kind == replication.MsgError {
			return frames[i].Code, frames[i].ErrMessage, true
		}
	}
	return 0, "", false
}

// relay is a raw QUIC relay speaking the replication wire protocol
// while authenticated as B. The harness CA issues the relay a fresh
// leaf for B's NodeID, so TLS can never be the refusal cause: every
// rejection below is origin verification, not transport auth.
type relay struct {
	dialTLS  *tls.Config
	quicConf *quic.Config
	db       [16]byte
}

// relaySession is one relay session: the handshake stream stays open
// for further attack frames and for observing node replies.
type relaySession struct {
	conn          *quic.Conn
	stream        *quic.Stream
	welcomeSchema []byte
}

func parsePrivateKey(t *testing.T, der []byte) any {
	t.Helper()
	if key, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		return key
	}
	if key, err := x509.ParseECPrivateKey(der); err == nil {
		return key
	}
	key, err := x509.ParsePKCS1PrivateKey(der)
	if err != nil {
		t.Fatalf("invalid private key: %v", err)
	}
	return key
}

// newRelay builds the relay fixture from cluster-issued credentials,
// mirroring the engine mTLS client setup.
func newRelay(t *testing.T, creds replication.Credentials, db [16]byte) *relay {
	t.Helper()
	roots := x509.NewCertPool()
	for _, der := range creds.Roots {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatalf("invalid trust root: %v", err)
		}
		roots.AddCert(cert)
	}
	leaf, err := x509.ParseCertificate(creds.Chain[0])
	if err != nil {
		t.Fatalf("invalid leaf certificate: %v", err)
	}
	cert := tls.Certificate{
		Certificate: creds.Chain,
		PrivateKey:  parsePrivateKey(t, creds.PrivateKey),
		Leaf:        leaf,
	}
	clientTLS := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		ServerName:         "murmur.invalid",
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{string(replication.ALPN)},
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("missing peer certificate")
			}
			peer, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return err
			}
			intermediates := x509.NewCertPool()
			for _, raw := range rawCerts[1:] {
				if c, err := x509.ParseCertificate(raw); err == nil {
					intermediates.AddCert(c)
				}
			}
			if _, err := peer.Verify(x509.VerifyOptions{
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
	quicConf := &quic.Config{
		MaxIdleTimeout:        30 * time.Second,
		KeepAlivePeriod:       10 * time.Second,
		MaxIncomingStreams:    40,
		MaxIncomingUniStreams: -1,
		EnableDatagrams:       true,
	}
	return &relay{dialTLS: clientTLS, quicConf: quicConf, db: db}
}

func (r *relay) hello(schema []byte) *replication.Message {
	return &replication.Message{
		Kind:           replication.MsgHello,
		Capabilities:   replication.Capabilities,
		Required:       replication.Required,
		Version:        replication.Version,
		Database:       r.db,
		Node:           nodeB,
		Schema:         schema,
		Mode:           replication.DisseminationGossip,
		MaxTransaction: 64 << 20,
	}
}

// dial dials advertising the given schema identity; the Welcome assert
// is the attach proof (every dial carries its own proof).
func (r *relay) dial(t *testing.T, addr netip.AddrPort, schema []byte, what string) *relaySession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	conn, err := quic.DialAddr(ctx, addr.String(), r.dialTLS, r.quicConf)
	if err != nil {
		t.Fatalf("%s: relay connect: %v", what, err)
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(0, what+" open failed")
		t.Fatalf("%s: relay open stream: %v", what, err)
	}
	_ = stream.SetWriteDeadline(time.Now().Add(dialTimeout))
	if err := replication.WriteMessage(stream, r.hello(schema)); err != nil {
		_ = conn.CloseWithError(0, what+" hello failed")
		t.Fatalf("%s: relay write hello: %v", what, err)
	}
	_ = stream.SetReadDeadline(time.Now().Add(welcomeTimeout))
	msg, err := replication.ReadMessage(stream)
	if err != nil || msg.Kind != replication.MsgWelcome {
		_ = conn.CloseWithError(0, what+" no welcome")
		t.Fatalf("%s: relay expected Welcome, got %v %+v", what, err, msg)
	}
	return &relaySession{conn: conn, stream: stream, welcomeSchema: msg.Schema}
}

func (s *relaySession) send(t *testing.T, msg *replication.Message, what string) {
	t.Helper()
	_ = s.stream.SetWriteDeadline(time.Now().Add(dialTimeout))
	if err := replication.WriteMessage(s.stream, msg); err != nil {
		t.Fatalf("%s: relay send %s: %v", what, msgName(msg), err)
	}
}

// collect reads inbound messages until total elapses or the stream dies.
func (s *relaySession) collect(total time.Duration) []replication.Message {
	return s.collectUntil(total, nil)
}

// collectUntil reads until want matches, total elapses, or the stream
// dies.
func (s *relaySession) collectUntil(total time.Duration, want func(*replication.Message) bool) []replication.Message {
	var out []replication.Message
	deadline := time.Now().Add(total)
	for time.Now().Before(deadline) {
		_ = s.stream.SetReadDeadline(time.Now().Add(slice))
		msg, err := replication.ReadMessage(s.stream)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue // quiet slice, keep waiting
			}
			return out // stream died
		}
		out = append(out, msg)
		if want != nil && want(&msg) {
			return out
		}
	}
	return out
}

// close transmits the close and lets the node side tear down.
func (s *relaySession) close(what string) {
	_ = s.conn.CloseWithError(0, what+" done")
	time.Sleep(500 * time.Millisecond)
}

// recorder is a background `last_error` recorder. Stray dial-back
// failures can overwrite a single latch sample, so auth-cause legs
// record every latch sample on a 10 ms poll and match by timestamp
// instead.
type recorder struct {
	mu     sync.Mutex
	log    []recordedError
	stopCh chan struct{}
	wg     sync.WaitGroup
}

type recordedError struct {
	at  time.Time
	msg string
}

func startRecorder(cluster *harness.Cluster, idx int) *recorder {
	r := &recorder{stopCh: make(chan struct{})}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			select {
			case <-r.stopCh:
				return
			default:
			}
			// Log every sample, not just transitions:
			// consecutive legs can latch the identical string,
			// and a transitions-only log cannot prove the
			// repeat happened after `since`.
			cur := ""
			if h := cluster.Node(idx).Replication(); h != nil {
				if s := h.Status().LastError; s != nil {
					cur = *s
				}
			}
			r.mu.Lock()
			r.log = append(r.log, recordedError{at: time.Now(), msg: cur})
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
		if !e.at.Before(since) && strings.Contains(e.msg, needle) {
			return true
		}
	}
	return false
}

func (r *recorder) stop() {
	close(r.stopCh)
	r.wg.Wait()
}

// assertStreamDead pings a stream a fatal probe should have killed and
// asserts the node never answers: no pong (the ping was never
// processed) and no foreign data.
func assertStreamDead(t *testing.T, sess *relaySession, nonce uint64, what string) {
	t.Helper()
	_ = sess.stream.SetWriteDeadline(time.Now().Add(dialTimeout))
	_ = replication.WriteMessage(sess.stream, &replication.Message{Kind: replication.MsgPing, Nonce: nonce})
	frames := sess.collect(deathWindow)
	for i := range frames {
		if frames[i].Kind == replication.MsgPong && frames[i].Nonce == nonce {
			t.Fatalf("node answered ping after %s (stream should be dead)", what)
		}
	}
	assertNoForeignData(t, frames, "after "+what)
	t.Logf("after %s the node sent only: %s", what, frameSummary(frames))
}

// expectProtocolError expects one `Error{code: 1}` whose text contains
// needle on a session that must survive. Returns all frames seen.
func expectProtocolError(t *testing.T, sess *relaySession, needle, what string) []replication.Message {
	t.Helper()
	frames := sess.collectUntil(errorTimeout, func(m *replication.Message) bool {
		return m.Kind == replication.MsgError && m.Code == 1
	})
	code, text, ok := lastErrorText(frames)
	if !ok || code != 1 || !strings.Contains(text, needle) {
		t.Fatalf("%s drew no Error{code: 1, ..%s..}; saw %v %q", what, needle, code, text)
	}
	return frames
}

// expectPong expects a pong to nonce (the session survived the probe).
func expectPong(t *testing.T, sess *relaySession, nonce uint64, what string) []replication.Message {
	t.Helper()
	sess.send(t, &replication.Message{Kind: replication.MsgPing, Nonce: nonce}, what)
	frames := sess.collectUntil(pongTimeout, func(m *replication.Message) bool {
		return m.Kind == replication.MsgPong && m.Nonce == nonce
	})
	found := false
	for i := range frames {
		if frames[i].Kind == replication.MsgPong && frames[i].Nonce == nonce {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing pong to %s; saw %s", what, frameSummary(frames))
	}
	return frames
}

// waitQuiet waits until idx has no connected sessions (between relay
// legs, every session claims B's identity, so the previous one must be
// fully torn down first).
func waitQuiet(t *testing.T, cluster *harness.Cluster, idx int, what string) {
	t.Helper()
	ok := harness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		h := cluster.Node(idx).Replication()
		return h != nil && h.Status().Peers == 0
	})
	if !ok {
		t.Fatalf("%s: node%d sessions did not tear down", what, idx+1)
	}
}

func cloneTx(tx replication.Transaction) replication.Transaction {
	out := tx
	out.Payload = append([]byte(nil), tx.Payload...)
	out.Signature = append([]byte(nil), tx.Signature...)
	return out
}

// TestForwardedOriginSurvivesOfflineOriginAndHostileRelay proves C
// stores A's forwarded origin proof and rejects every hostile-relay
// attack (Rust:
// forwarded_origin_survives_offline_origin_and_hostile_relay).
func TestForwardedOriginSurvivesOfflineOriginAndHostileRelay(t *testing.T) {
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:        "origin-signatures",
		NumNodes:    3,
		NodeIDs:     [][16]byte{nodeA, nodeB, nodeC},
		ManualPeers: true,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("origin-signatures")
		}
		cluster.Finish(failed)
	}()

	const target = c
	dbid := cluster.DatabaseID()

	// Same table on every node (Rust: TypedRecords fixture tables).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i,
			"CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			t.Fatalf("node%d migrate records table: %v", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).SetReplication("records", true); err != nil {
			t.Fatalf("node%d opt-in records: %v", i+1, err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("node%d flush opt-in: %v", i+1, err)
		}
	}

	// A commits; B receives; A stops.
	if err := cluster.AddPeer(a, b); err != nil {
		t.Fatalf("mesh A->B: %v", err)
	}
	if err := cluster.AddPeer(b, a); err != nil {
		t.Fatalf("mesh B->A: %v", err)
	}
	insert(t, cluster, a, 1, "from-A")
	waitNames(t, cluster, b, []string{"from-A"}, convergeTimeout)
	cluster.StopNode(a)

	// C learns A's transaction exclusively from B while A stays stopped.
	if err := cluster.AddPeer(b, c); err != nil {
		t.Fatalf("mesh B->C: %v", err)
	}
	if err := cluster.AddPeer(c, b); err != nil {
		t.Fatalf("mesh C->B: %v", err)
	}
	waitNames(t, cluster, c, []string{"from-A"}, convergeTimeout)
	namesB := mustNames(t, cluster.Node(b))
	namesC := mustNames(t, cluster.Node(c))
	if len(namesB) != len(namesC) {
		t.Fatalf("forwarded state differs")
	}
	for i := range namesB {
		if namesB[i] != namesC[i] {
			t.Fatalf("forwarded state differs")
		}
	}

	// Honest baseline on C. The applied row is the receipt proof:
	// receipts commit atomically with the apply, and there is no public
	// receipt read API. Quiesce first: row visibility does not imply the
	// backlog drained, and a late legit apply misattributes as an
	// attack.
	waitBacklogDrained(t, cluster, target, "honest baseline")
	base := queryCounters(t, cluster, target)
	if got := mustNames(t, cluster.Node(target)); len(got) != 1 || got[0] != "from-A" {
		t.Fatalf("honest transaction missing on C before the attacks")
	}

	// B stops; the relay authenticates as B from here on.
	cluster.StopNode(b)
	waitQuiet(t, cluster, target, "B shutdown")

	// Relay fixture sharing the cluster CA trust.
	issued, err := cluster.IssueCert(nodeB)
	if err != nil {
		t.Fatalf("issue relay cert: %v", err)
	}
	relayConfig := replication.NewConfig(
		netip.MustParseAddrPort("127.0.0.1:0"), issued.Credentials, fabricateSeed)
	relay := newRelay(t, issued.Credentials, dbid)
	nodeAddr := cluster.NodeInfo(target).ReplAddr

	// --- Capture leg: fetch A's genuine batches from C's stored log.
	// The served bytes must verify under A's origin key with the origin
	// intact, proving C stores the forwarded origin proof (not a
	// B-minted copy) even though A is offline.
	fetch := relay.dial(t, nodeAddr, append([]byte(nil), divergentSchema...), "fetch")
	realSchema := fetch.welcomeSchema
	if string(realSchema) == string(divergentSchema) {
		t.Fatalf("node schema equals the divergent bytes (leg vacuous)")
	}
	fetch.send(t, &replication.Message{
		Kind: replication.MsgNeed, Origin: nodeA, From: 1, Through: 16,
	}, "fetch-need")
	fetchFrames := fetch.collect(needWindow)
	var aTxs []replication.Transaction
	for i := range fetchFrames {
		if fetchFrames[i].Kind == replication.MsgBatches {
			for _, tx := range fetchFrames[i].Transactions {
				if tx.Origin == nodeA {
					aTxs = append(aTxs, tx)
				}
			}
		}
	}
	if len(aTxs) == 0 {
		t.Fatalf("C served no A-origin batches for Need{A,1..=16}; saw %s",
			frameSummary(fetchFrames))
	}
	sort.Slice(aTxs, func(i, j int) bool { return aTxs[i].Sequence < aTxs[j].Sequence })
	deduped := aTxs[:0]
	for _, tx := range aTxs {
		if len(deduped) > 0 && deduped[len(deduped)-1].Sequence == tx.Sequence {
			// Distinct sequences legitimately differ; only
			// true duplicates (same sequence served twice)
			// must be byte-identical.
			prev := deduped[len(deduped)-1]
			if prev.Digest() != tx.Digest() {
				t.Fatalf("C served conflicting bytes for A/%d", tx.Sequence)
			}
			continue
		}
		deduped = append(deduped, tx)
	}
	aTxs = deduped
	original := aTxs[len(aTxs)-1]
	trust := map[[16]byte][32]byte{nodeA: cluster.NodeOriginPubkey(a)}
	if err := original.Verify(dbid, trust, maxTx); err != nil {
		t.Fatalf("served A batch must verify under A's origin key: %v", err)
	}
	t.Logf("captured genuine A/%d (%d A-origin batches) for the attack legs",
		original.Sequence, len(aTxs))
	fetch.close("fetch")
	waitQuiet(t, cluster, target, "fetch session")

	// Fresh sequences per attack.
	fresh := original.Sequence + 100
	rec := startRecorder(cluster, target)

	// --- Modified: tampered payload under the original signature.
	modified := cloneTx(original)
	modified.Sequence = fresh
	modified.ID = modifiedID
	if len(modified.Payload) == 0 {
		t.Fatalf("original has an empty payload (leg vacuous)")
	}
	modified.Payload[0] ^= 0x01
	if string(modified.Signature) != string(original.Signature) {
		t.Fatalf("fixture updated the signature it must leave stale")
	}
	s := relay.dial(t, nodeAddr, append([]byte(nil), realSchema...), "modified")
	since := time.Now()
	s.send(t, &replication.Message{Kind: replication.MsgBatches,
		Transactions: []replication.Transaction{modified}}, "modified")
	assertStreamDead(t, s, 0xD1, "modified batch")
	if !harness.WaitUntil(peerTimeout, 10*time.Millisecond, func() bool {
		return rec.sawSince(since, "payload digest mismatch")
	}) {
		t.Fatalf("node never latched the digest-mismatch cause for the modified batch")
	}
	s.close("modified")
	waitQuiet(t, cluster, target, "modified session")
	assertCounters(t, cluster, target, base, "after the modified batch")
	if got := mustNames(t, cluster.Node(target)); len(got) != 1 || got[0] != "from-A" {
		t.Fatalf("node3 rows changed after the modified batch")
	}

	// --- Fabricated: re-signed with the wrong key while claiming
	// origin A, with a bumped clock.
	fabricated := cloneTx(original)
	fabricated.Sequence = fresh + 1
	fabricated.ID = fabricatedID
	fabricated.Millis = original.Millis + 1000
	fabricated.Sign(&fabricateSeed)
	// Non-vacuous: the fabrication is self-consistent under the
	// attacker key, so only the origin/key binding is wrong.
	wrongTrust := map[[16]byte][32]byte{nodeA: replication.PublicKey(&fabricateSeed)}
	if err := fabricated.Verify(dbid, wrongTrust, maxTx); err != nil {
		t.Fatalf("fabrication must verify under the attacker key locally: %v", err)
	}
	if err := fabricated.Verify(dbid, trust, maxTx); err == nil {
		t.Fatalf("fabrication verifies under A's key locally (leg vacuous)")
	}
	s = relay.dial(t, nodeAddr, append([]byte(nil), realSchema...), "fabricated")
	since = time.Now()
	s.send(t, &replication.Message{Kind: replication.MsgBatches,
		Transactions: []replication.Transaction{fabricated}}, "fabricated")
	assertStreamDead(t, s, 0xD2, "fabricated batch")
	if !harness.WaitUntil(peerTimeout, 10*time.Millisecond, func() bool {
		return rec.sawSince(since, "invalid Ed25519 signature")
	}) {
		t.Fatalf("node never latched the signature cause for the fabricated batch")
	}
	s.close("fabricated")
	waitQuiet(t, cluster, target, "fabricated session")
	assertCounters(t, cluster, target, base, "after the fabricated batch")
	if got := mustNames(t, cluster.Node(target)); len(got) != 1 || got[0] != "from-A" {
		t.Fatalf("node3 rows changed after the fabricated batch")
	}

	// --- Unsigned: the signature stripped.
	unsigned := cloneTx(original)
	unsigned.Sequence = fresh + 2
	unsigned.ID = unsignedID
	unsigned.Signature = nil
	if err := unsigned.Verify(dbid, trust, maxTx); err == nil {
		t.Fatalf("unsigned batch verifies locally (leg vacuous)")
	}
	s = relay.dial(t, nodeAddr, append([]byte(nil), realSchema...), "unsigned")
	since = time.Now()
	s.send(t, &replication.Message{Kind: replication.MsgBatches,
		Transactions: []replication.Transaction{unsigned}}, "unsigned")
	assertStreamDead(t, s, 0xD3, "unsigned batch")
	if !harness.WaitUntil(peerTimeout, 10*time.Millisecond, func() bool {
		return rec.sawSince(since, "invalid Ed25519 point or signature length")
	}) {
		t.Fatalf("node never latched the signature cause for the unsigned batch")
	}
	s.close("unsigned")
	waitQuiet(t, cluster, target, "unsigned session")
	assertCounters(t, cluster, target, base, "after the unsigned batch")
	if got := mustNames(t, cluster.Node(target)); len(got) != 1 || got[0] != "from-A" {
		t.Fatalf("node3 rows changed after the unsigned batch")
	}

	// --- Cross-DBID: A's signature over a foreign database id.
	// Identity failures are protocol errors, so unlike the auth legs
	// above the session must survive.
	cross := cloneTx(original)
	cross.Sequence = fresh + 3
	cross.ID = crossDBID
	cross.Database = crossDB
	if err := cross.Verify(dbid, trust, maxTx); err == nil {
		t.Fatalf("cross-DBID batch verifies locally (leg vacuous)")
	}
	s = relay.dial(t, nodeAddr, append([]byte(nil), realSchema...), "cross-db")
	since = time.Now()
	s.send(t, &replication.Message{Kind: replication.MsgBatches,
		Transactions: []replication.Transaction{cross}}, "cross-db")
	seen := expectProtocolError(t, s, "transaction identity", "cross-DBID batch")
	assertNoForeignData(t, seen, "on the cross-DBID session")
	if !harness.WaitUntil(peerTimeout, 10*time.Millisecond, func() bool {
		return rec.sawSince(since, "transaction identity")
	}) {
		t.Fatalf("node never latched the identity cause for the cross-DBID batch")
	}
	// Session alive: the ping round-trips.
	pongFrames := expectPong(t, s, 0xD4, "ping after cross-DBID")
	assertNoForeignData(t, pongFrames, "after the cross-DBID batch")
	s.close("cross-db")
	waitQuiet(t, cluster, target, "cross-DBID session")
	assertCounters(t, cluster, target, base, "after the cross-DBID batch")
	if got := mustNames(t, cluster.Node(target)); len(got) != 1 || got[0] != "from-A" {
		t.Fatalf("node3 rows changed after the cross-DBID batch")
	}

	// --- Chunk identity: a tampered proof clock must fail before
	// durable staging or cleanup.
	chunkSeq := fresh + 4
	proof := cloneTx(original)
	proof.Payload = nil
	proof.Sequence = chunkSeq
	proof.ID = chunkID
	proof.Millis = original.Millis + 1
	// Signature deliberately NOT updated. Non-vacuous: the untampered
	// cleared-payload proof shape verifies, so the +1ms tamper is the
	// sole rejection cause.
	clean := cloneTx(original)
	clean.Payload = nil
	if err := clean.VerifyHeader(dbid, trust); err != nil {
		t.Fatalf("untampered cleared-payload proof must verify: %v", err)
	}
	if err := proof.VerifyHeader(dbid, trust); err == nil {
		t.Fatalf("tampered proof verifies locally (leg vacuous)")
	}
	chunkData := []byte("origin-chunk-attack-probe")
	chunk := replication.Chunk{
		Proof:    proof,
		Origin:   nodeA,
		Sequence: chunkSeq,
		ID:       chunkID,
		Index:    0,
		Count:    1,
		Total:    uint64(len(chunkData)),
		Digest:   sha256.Sum256(chunkData),
		Data:     chunkData,
	}
	// Well-formedness: every structural check passes; only the proof
	// signature is wrong.
	if want := (uint64(len(chunkData)) + replication.ChunkBytes - 1) / replication.ChunkBytes; uint64(chunk.Count) != want {
		t.Fatalf("fixture chunk count is structurally wrong")
	}
	s = relay.dial(t, nodeAddr, append([]byte(nil), realSchema...), "chunk")
	since = time.Now()
	s.send(t, &replication.Message{Kind: replication.MsgTransactionChunk, Chunk: chunk}, "chunk")
	assertStreamDead(t, s, 0xD5, "tampered chunk")
	if !harness.WaitUntil(peerTimeout, 10*time.Millisecond, func() bool {
		return rec.sawSince(since, "invalid Ed25519 signature")
	}) {
		t.Fatalf("node never latched the signature cause for the tampered chunk")
	}
	s.close("chunk")
	waitQuiet(t, cluster, target, "chunk session")
	assertCounters(t, cluster, target, base, "after the tampered chunk")
	if got := mustNames(t, cluster.Node(target)); len(got) != 1 || got[0] != "from-A" {
		t.Fatalf("node3 rows changed after the tampered chunk")
	}
	// Nothing durably staged: the availability index must not list the
	// attacked transfer (it lists durable chunk manifests).
	probe := relay.dial(t, nodeAddr, append([]byte(nil), realSchema...), "chunk-probe")
	probe.send(t, &replication.Message{Kind: replication.MsgChunkAvailabilityRequest, Offset: 0}, "chunk-probe")
	probeFrames := probe.collectUntil(probeTimeout, func(m *replication.Message) bool {
		return m.Kind == replication.MsgChunkAvailabilityPage
	})
	pageFound := false
	for i := range probeFrames {
		if probeFrames[i].Kind == replication.MsgChunkAvailabilityPage {
			pageFound = true
			if len(probeFrames[i].Availability) != 0 {
				t.Fatalf("rejected chunk was durably staged: %+v", probeFrames[i].Availability)
			}
			if probeFrames[i].More {
				t.Fatalf("unexpected second availability page")
			}
		}
	}
	if !pageFound {
		t.Fatalf("no chunk-availability page; saw %s", frameSummary(probeFrames))
	}
	probe.close("chunk-probe")
	waitQuiet(t, cluster, target, "chunk-probe session")

	// --- Restart C: the honest row and signed history are durable. A
	// and B stay stopped.
	rec.stop()
	cluster.StopNode(target)
	if err := cluster.StartNode(target); err != nil {
		t.Fatalf("restart node3: %v", err)
	}
	if got := mustNames(t, cluster.Node(target)); len(got) != 1 || got[0] != "from-A" {
		t.Fatalf("honest row lost on restart")
	}
	// Fresh process: counters restart at zero (verified premise for
	// the idempotency leg below, not an assumption).
	post := queryCounters(t, cluster, target)
	if post.received != 0 || post.applied != 0 {
		t.Fatalf("fresh-process counters must start at zero")
	}
	nodeAddr = cluster.NodeInfo(target).ReplAddr

	// --- Signed duplicate replay stays idempotent across restart: no
	// error, the session survives, and nothing is counted twice (the
	// engine dedups before counting, so `received` stays 0).
	replay := relay.dial(t, nodeAddr, append([]byte(nil), realSchema...), "replay")
	replay.send(t, &replication.Message{Kind: replication.MsgBatches,
		Transactions: []replication.Transaction{original}}, "replay")
	// Ordered stream: the pong proves the duplicate was already
	// processed (silently absorbed, not applied).
	replayFrames := expectPong(t, replay, 0xD6, "replay ping")
	for i := range replayFrames {
		if replayFrames[i].Kind == replication.MsgError {
			t.Fatalf("signed duplicate drew an error frame; saw %s", frameSummary(replayFrames))
		}
	}
	assertNoForeignData(t, replayFrames, "on the replay session")
	replay.close("replay")
	waitQuiet(t, cluster, target, "replay session")
	assertCounters(t, cluster, target, post, "after the signed-duplicate replay")
	if got := mustNames(t, cluster.Node(target)); len(got) != 1 || got[0] != "from-A" {
		t.Fatalf("node3 rows changed after the signed-duplicate replay")
	}

	// --- An authenticated relay is not automatically a trusted
	// snapshot source (C's `trusted_snapshot_sources` is empty by
	// default in the harness).
	rec2 := startRecorder(cluster, target)
	snapData := make([]byte, 4096)
	for i := range snapData {
		snapData[i] = 0x53
	}
	snapSchema := replication.SchemaDigest(realSchema)
	now := replication.NowMillis()
	manifest, _, err := replication.Build([2][16]byte{dbid, nodeB}, nodeC,
		snapData, nil, snapSchema, relayConfig, now)
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	// Non-vacuous: every manifest check except source trust passes, so
	// the refusal is the trust check alone.
	if manifest.Donor != nodeB {
		t.Fatalf("fixture manifest donor wrong")
	}
	if manifest.Recipient != nodeC {
		t.Fatalf("fixture manifest recipient wrong")
	}
	if manifest.Database != dbid {
		t.Fatalf("fixture manifest database wrong")
	}
	if manifest.Expires <= now {
		t.Fatalf("fixture manifest already expired")
	}
	if manifest.Bytes != uint64(len(snapData)) {
		t.Fatalf("fixture manifest size wrong")
	}
	if want := (len(snapData) + replication.ChunkBytes - 1) / replication.ChunkBytes; len(manifest.Chunks) != want {
		t.Fatalf("fixture manifest chunking wrong")
	}
	if manifest.Digest != sha256.Sum256(snapData) {
		t.Fatalf("fixture manifest digest wrong")
	}
	s = relay.dial(t, nodeAddr, append([]byte(nil), realSchema...), "snapshot")
	since = time.Now()
	s.send(t, &replication.Message{Kind: replication.MsgSnapshotManifest, Manifest: *manifest}, "snapshot")
	assertStreamDead(t, s, 0xD7, "untrusted snapshot manifest")
	if !harness.WaitUntil(peerTimeout, 10*time.Millisecond, func() bool {
		return rec2.sawSince(since, "snapshot identity, trust or expiry")
	}) {
		t.Fatalf("node never latched the trust cause for the untrusted manifest")
	}
	s.close("snapshot")
	waitQuiet(t, cluster, target, "snapshot session")
	assertCounters(t, cluster, target, post, "after the untrusted snapshot manifest")
	if got := mustNames(t, cluster.Node(target)); len(got) != 1 || got[0] != "from-A" {
		t.Fatalf("node3 rows changed after the untrusted snapshot manifest")
	}
	rec2.stop()
	failed = false
}
