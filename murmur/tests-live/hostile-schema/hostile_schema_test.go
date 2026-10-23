// Live scenario: hostile signed schema manifests are quarantined.
//
// Go port of the Rust `hostile-schema` scenario
// (/home/marc/MURMUR/tests-live/hostile-schema/tests/hostile_schema.rs:
// TestHostileSchemaManifestsQuarantined). A malicious authenticated
// peer (CA-issued QUIC cert, untrusted origin key) serves hostile
// signed schema manifests at an honest 3-node mesh: a revision with a
// corrupted signature, a self-consistent revision from the untrusted
// signer, an oversized revision, and a zero-sequence revision. Every
// forgery must be refused (observed on the wire and in the last_error
// latch, never assumed), never adopted, and the honest mesh must keep
// converging with equal digests throughout.
//
// Plain SQL replaces the typed-records HTTP API, and the Prometheus
// counters map to replication Status observables.
package hostileschema

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

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

// attackerSeed is the attacker origin signing key. CA-issued TLS cert,
// but this key is in NOBODY's trusted_origins, so every manifest it
// signs must be refused as untrusted schema signer.
var attackerSeed = [32]byte{
	0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
	0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
	0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
	0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1,
}

// divergentSchema holds divergent handshake schema bytes. Tolerated at
// attach (accept_remote_schema), never adopted without a signed
// manifest.
var divergentSchema = []byte("attacker-divergent-schema-v9999")

// evilSchema holds the schema bytes carried by attacker-signed
// manifests: well-formed on the wire, never the node's canonical bytes.
var evilSchema = []byte("attacker-evil-schema-v9999")

const (
	// convergeTimeout is the convergence deadline per phase (Rust: 30 s).
	convergeTimeout = 30 * time.Second
	// convergePoll is the convergence poll interval (Rust: 200 ms).
	convergePoll = 200 * time.Millisecond
	// dialTimeout is the fixture dial/stream/setup deadline.
	dialTimeout = 10 * time.Second
	// welcomeTimeout is how long the fixture waits for a Welcome that
	// must arrive.
	welcomeTimeout = 5 * time.Second
	// manifestTimeout is how long the fixture waits for a served schema
	// manifest (Rust: 15 s).
	manifestTimeout = 15 * time.Second
	// pongTimeout is how long the fixture waits for a Pong that must
	// arrive (Rust: 10 s).
	pongTimeout = 10 * time.Second
	// slice is one gradual read inside a collection window (Rust:
	// 500 ms).
	slice = 500 * time.Millisecond
	// deathWindow is the post-fatal-probe silence window (Rust: 3 s).
	deathWindow = 3 * time.Second
	// peerTimeout is the session attach/teardown observation bound.
	peerTimeout = 5 * time.Second
)

// maxTX is the 1 MiB transaction cap: keeps the oversized leg small
// without affecting the tiny honest writes.
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

// waitConverged waits until all three honest nodes hold want rows with
// equal names (Rust: waitConverged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		n0 := names(t, cluster.Node(0))
		n1 := names(t, cluster.Node(1))
		n2 := names(t, cluster.Node(2))
		if len(n0) == want && equalStrings(n0, n1) && equalStrings(n1, n2) {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("nodes did not converge on %d rows with equal names within %v (node0=%v node1=%v node2=%v)",
				want, timeout, n0, n1, n2)
		}
		time.Sleep(convergePoll)
	}
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

// connectedPeers returns the connected replication sessions on one node.
func connectedPeers(cluster *harness.Cluster, idx int) int {
	h := cluster.Node(idx).Replication()
	if h == nil {
		return 0
	}
	return h.Status().Peers
}

// schemaAdoptions returns the adopted-schema counter on one node. Only
// moves when a received manifest passes verification AND the adoption
// decision, so it is the must-not-move assert.
func schemaAdoptions(cluster *harness.Cluster, idx int) uint64 {
	return cluster.Node(idx).Replication().Status().Metrics.SchemaAdoptions
}

func isPong(nonce uint64) func(*replication.Message) bool {
	return func(m *replication.Message) bool {
		return m.Kind == replication.MsgPong && m.Nonce == nonce
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
	ns := make([]string, 0, len(frames))
	for i := range frames {
		ns = append(ns, msgName(&frames[i]))
	}
	return strings.Join(ns, ",")
}

// attacker is the raw QUIC attacker speaking the replication wire
// protocol with a CA-issued certificate. It shares the cluster CA
// trust, so TLS can never be the refusal cause.
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
// further attack frames and for observing node replies. A reader
// goroutine owns the stream so framing is never split across read
// deadlines.
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

// fetchManifest asks the node for its current schema tip (schema traffic
// flows even on divergent sessions).
func (s *evilSession) fetchManifest(t *testing.T, what string) replication.SignedSchema {
	t.Helper()
	req := replication.Message{Kind: replication.MsgSchemaRequest}
	s.send(t, &req, what)
	frames := s.collectUntil(manifestTimeout, func(m *replication.Message) bool {
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

// close queues the CLOSE frame and sleeps so it flushes; the caller's
// teardown gate still asserts node-side.
func (s *evilSession) close(what string) {
	_ = s.conn.CloseWithError(0, what+" done")
	time.Sleep(500 * time.Millisecond)
}

// recorder is the background last_error recorder. Honest nodes may dial
// the attacker address back on discovered-membership ticks and those
// stray dials time out into handshake-timeout latches that can overwrite
// a single sample, so refusal-cause legs record every latch sample on a
// 10 ms poll and match by timestamp instead.
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
			// Log every sample, not just transitions: consecutive
			// legs can latch the identical string, and a
			// transitions-only log cannot prove the repeat
			// happened after since.
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
// asserts the node never answers it and never acked the forgery: no pong
// (the ping was never processed) and no SchemaAck (the manifest never
// reached the adoption decision). Stray repeats of attach-time control
// chatter are tolerated but logged.
func assertStreamDead(t *testing.T, sess *evilSession, nonce uint64, what string) {
	t.Helper()
	ping := replication.Message{Kind: replication.MsgPing, Nonce: nonce}
	_ = replication.WriteMessage(sess.stream, &ping)
	frames := sess.collect(deathWindow)
	for i := range frames {
		if frames[i].Kind == replication.MsgPong && frames[i].Nonce == nonce {
			t.Fatalf("node answered ping after %s (stream should be dead)", what)
		}
		if frames[i].Kind == replication.MsgSchemaAck {
			t.Fatalf("node acked the forged manifest after %s (saw %s)", what, frameSummary(frames))
		}
	}
	t.Logf("after %s the node sent only: %s", what, frameSummary(frames))
}

// expectPong expects a pong to nonce (liveness probes after fatal attacks).
func expectPong(t *testing.T, sess *evilSession, nonce uint64, what string) {
	t.Helper()
	ping := replication.Message{Kind: replication.MsgPing, Nonce: nonce}
	sess.send(t, &ping, what)
	frames := sess.collectUntil(pongTimeout, isPong(nonce))
	for i := range frames {
		if frames[i].Kind == replication.MsgPong && frames[i].Nonce == nonce {
			return
		}
	}
	t.Fatalf("missing pong to %s; saw %s", what, frameSummary(frames))
}

// attackerManifest builds one attacker-signed manifest. Correctly signed
// for attackerSeed unless the caller corrupts it afterwards, so refusal
// is product validation, not malformedness.
func attackerManifest(db [16]byte, sequence uint64, schema []byte) replication.SignedSchema {
	signed := replication.SignedSchema{
		Database:  db,
		Signer:    attackerID,
		Sequence:  sequence,
		Schema:    schema,
		Signature: nil,
	}
	seed := attackerSeed
	signed.Sign(&seed)
	return signed
}

// assertAuthorizedShape is the non-vacuous guard: the manifest verifies
// cleanly against a trust map that DOES contain the attacker key,
// proving the live refusal is cluster authorization (missing trust),
// not a bad signature or malformed envelope.
func assertAuthorizedShape(t *testing.T, signed *replication.SignedSchema, db [16]byte, max int, what string) {
	t.Helper()
	trust := map[[16]byte][32]byte{attackerID: replication.PublicKey(&attackerSeed)}
	if err := signed.Verify(db, attackerID, trust, max); err != nil {
		t.Fatalf("%s is not even self-consistent: %v", what, err)
	}
}

func TestHostileSchemaManifestsQuarantined(t *testing.T) {
	// Fixed node ids (asserted distinct from the attacker id) so no
	// handshake identity can alias.
	nodeIDs := [][16]byte{
		{0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10},
		{0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20},
		{0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30},
	}
	for _, id := range nodeIDs {
		if id == attackerID {
			t.Fatal("attacker id collides with a node id")
		}
	}
	maxTransactionBytes := maxTX
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "hostile-schema",
		NumNodes: 3,
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
			cluster.DumpForensics("hostile-schema")
		}
		cluster.Finish(failed)
	}()

	// Attacker aims at node0; node1/node2 are the honest witnesses.
	const target = 0
	dbid := cluster.DatabaseID()
	targetID := cluster.NodeID(target)

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

	// Positive control (pre-attack): the honest 3-mesh converges.
	insert(t, cluster, 0, 1, "baseline")
	waitConverged(t, cluster, 1, convergeTimeout)
	preAttack := names(t, cluster.Node(target))
	baseAdoptions := schemaAdoptions(cluster, target)

	// Attacker fixture sharing the cluster CA trust.
	issued, err := cluster.IssueCert(attackerID)
	if err != nil {
		t.Fatalf("issue attacker cert: %v", err)
	}
	atk := newAttacker(t, issued.Credentials, dbid)
	nodeAddr := cluster.NodeInfo(target).ReplAddr
	divergent := append([]byte(nil), divergentSchema...)
	var handshakes uint32

	// Latch recorder for the refusal-cause legs (see recorder).
	rec := startRecorder(cluster, target)
	defer rec.stopRecorder()

	// --- S1: forged revision with a corrupted signature.
	// Fetch the served tip, then flip bytes inside the signature so
	// the frame stays structurally valid but authentication fails.
	badDigest := atk.dial(t, nodeAddr, divergent, "digest-forge")
	handshakes++
	if ok := harness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		return connectedPeers(cluster, target) == 3
	}); !ok {
		t.Fatal("attack handshake never attached (peers != 3)")
	}
	servedSchema := append([]byte(nil), badDigest.welcomeSchema...)
	served := badDigest.fetchManifest(t, "digest-forge")
	if served.Signer != targetID {
		t.Fatal("served manifest is not signed by the target node")
	}
	if !bytes.Equal(served.Schema, servedSchema) {
		t.Fatal("served manifest bytes differ from the advertised Welcome bytes")
	}
	if served.Sequence == 0 {
		t.Fatal("served manifest has a zero sequence (leg vacuous)")
	}
	forged := served
	forged.Signature = append([]byte(nil), served.Signature...)
	forged.Signature[0] ^= 0xFF
	forged.Signature[17] ^= 0x01
	// Non-vacuous guards: the corrupted bytes still encode/decode
	// as a message (the forgery targets the digest check, not the
	// framing) but must fail local authentication.
	wire := replication.EncodeMessage(&replication.Message{Kind: replication.MsgSchemaManifest, Signed: forged})
	back, err := replication.DecodeMessage(wire)
	if err != nil {
		t.Fatalf("forged manifest still decodes (framing intact): %v", err)
	}
	if back.Kind != replication.MsgSchemaManifest {
		t.Fatal("forged frame did not survive a local round-trip")
	}
	targetTrust := map[[16]byte][32]byte{targetID: cluster.NodeOriginPubkey(target)}
	if err := forged.Verify(dbid, forged.Signer, targetTrust, maxTX); err == nil {
		t.Fatal("corrupted manifest still verifies locally; forgery is vacuous")
	}
	sinceS1 := time.Now()
	forgedMsg := replication.Message{Kind: replication.MsgSchemaManifest, Signed: forged}
	badDigest.send(t, &forgedMsg, "digest-forge")
	// The stream must die: a ping sent after the forgery draws no
	// pong and no SchemaAck.
	assertStreamDead(t, badDigest, 71, "digest-forged manifest")
	if ok := harness.WaitUntil(peerTimeout, 10*time.Millisecond, func() bool {
		return rec.sawSince(sinceS1, "invalid Ed25519 signature")
	}); !ok {
		t.Fatal("node never latched the signature refusal for the digest forgery")
	}
	badDigest.close("digest-forge")
	if ok := harness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		return connectedPeers(cluster, target) == 2
	}); !ok {
		t.Fatal("digest-forge session did not tear down")
	}
	// The forgery never reached the adoption engine: the adoptions
	// counter (incremented only after successful verification and
	// the adoption decision) must not move.
	if got := schemaAdoptions(cluster, target); got != baseAdoptions {
		t.Fatalf("schema adoptions moved on the digest forgery (forgery reached adoption): %d -> %d", baseAdoptions, got)
	}
	// The node itself survives the forgery.
	probe := atk.dial(t, nodeAddr, divergent, "probe-1")
	handshakes++
	expectPong(t, probe, 72, "probe after digest-forged manifest")
	probe.close("probe-1")

	// --- S2: self-consistent revision from the untrusted signer
	// (valid encoding, valid signature, divergent content). Must be
	// refused as untrusted schema signer and never adopted.
	evil := attackerManifest(dbid, 1, append([]byte(nil), evilSchema...))
	assertAuthorizedShape(t, &evil, dbid, maxTX, "evil revision")
	sinceS2 := time.Now()
	untrusted := atk.dial(t, nodeAddr, divergent, "untrusted-signer")
	handshakes++
	evilMsg := replication.Message{Kind: replication.MsgSchemaManifest, Signed: evil}
	untrusted.send(t, &evilMsg, "untrusted-signer")
	assertStreamDead(t, untrusted, 73, "untrusted-signer manifest")
	if ok := harness.WaitUntil(peerTimeout, 10*time.Millisecond, func() bool {
		return rec.sawSince(sinceS2, "untrusted schema signer")
	}); !ok {
		t.Fatal("node never latched the untrusted-signer refusal")
	}
	untrusted.close("untrusted-signer")
	if ok := harness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		return connectedPeers(cluster, target) == 2
	}); !ok {
		t.Fatal("untrusted-signer session did not tear down")
	}

	// --- S3: oversized revision (2 MiB of schema bytes against the
	// 1 MiB cap). Must be refused as an envelope violation.
	big := attackerManifest(dbid, 1, bytes2MiB())
	// Non-vacuous guard: oversized but otherwise self-consistent
	// (verifies under a generous cap), so the live refusal is the
	// size envelope, not a bad signature.
	assertAuthorizedShape(t, &big, dbid, 8<<20, "oversized revision")
	{
		trust := map[[16]byte][32]byte{attackerID: replication.PublicKey(&attackerSeed)}
		if err := big.Verify(dbid, attackerID, trust, maxTX); err == nil {
			t.Fatal("oversized revision passes the cluster cap locally (leg vacuous)")
		}
	}
	sinceS3 := time.Now()
	oversized := atk.dial(t, nodeAddr, divergent, "oversized")
	handshakes++
	bigMsg := replication.Message{Kind: replication.MsgSchemaManifest, Signed: big}
	oversized.send(t, &bigMsg, "oversized")
	assertStreamDead(t, oversized, 74, "oversized manifest")
	if ok := harness.WaitUntil(peerTimeout, 10*time.Millisecond, func() bool {
		return rec.sawSince(sinceS3, "schema envelope identity or size")
	}); !ok {
		t.Fatal("node never latched the envelope refusal for the oversized manifest")
	}
	oversized.close("oversized")
	if ok := harness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		return connectedPeers(cluster, target) == 2
	}); !ok {
		t.Fatal("oversized session did not tear down")
	}

	// --- S4: bad-epoch revision (zero sequence). Must be refused
	// as an envelope violation.
	zeroSeq := attackerManifest(dbid, 1, append([]byte(nil), evilSchema...))
	assertAuthorizedShape(t, &zeroSeq, dbid, maxTX, "zero-sequence revision")
	zeroSeq.Sequence = 0
	sinceS4 := time.Now()
	badEpoch := atk.dial(t, nodeAddr, divergent, "bad-epoch")
	handshakes++
	zeroMsg := replication.Message{Kind: replication.MsgSchemaManifest, Signed: zeroSeq}
	badEpoch.send(t, &zeroMsg, "bad-epoch")
	assertStreamDead(t, badEpoch, 75, "zero-sequence manifest")
	if ok := harness.WaitUntil(peerTimeout, 10*time.Millisecond, func() bool {
		return rec.sawSince(sinceS4, "schema envelope identity or size")
	}); !ok {
		t.Fatal("node never latched the envelope refusal for the zero-sequence manifest")
	}
	badEpoch.close("bad-epoch")
	if ok := harness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		return connectedPeers(cluster, target) == 2
	}); !ok {
		t.Fatal("bad-epoch session did not tear down")
	}

	// Quarantine: no adoption, and the schema identity is
	// untouched by every forgery.
	if got := schemaAdoptions(cluster, target); got != baseAdoptions {
		t.Fatalf("schema adoptions moved during the schema attacks (forged schema adopted): %d -> %d", baseAdoptions, got)
	}
	if got := names(t, cluster.Node(target)); !equalStrings(got, preAttack) {
		t.Fatalf("node1 names changed during the schema attacks: %v", got)
	}
	if got := rowCount(t, cluster.Node(target)); got != 1 {
		t.Fatalf("node1 row count changed during the schema attacks: %d", got)
	}
	finalProbe := atk.dial(t, nodeAddr, divergent, "final-schema")
	handshakes++
	if !bytes.Equal(finalProbe.welcomeSchema, servedSchema) {
		t.Fatal("schema bytes moved during the schema attacks")
	}
	finalProbe.close("final-schema")

	// Every attack handshake attached (each dial asserts its own
	// Welcome; peers == 3 was observed on the first session).
	if handshakes != 6 {
		t.Fatalf("handshake count drifted (attach proof): %d", handshakes)
	}
	t.Logf("%d attack handshakes attached and were answered", handshakes)

	// --- Positive control (post-attack): no crash, no stall. The
	// attacked node and the honest pair all keep replicating and
	// converge.
	insert(t, cluster, 1, 2, "post-attack-1")
	waitConverged(t, cluster, 2, convergeTimeout)
	insert(t, cluster, 2, 3, "post-attack-2")
	waitConverged(t, cluster, 3, convergeTimeout)
	// The attacked node itself writes and the write replicates
	// everywhere.
	insert(t, cluster, 0, 4, "attacked-node-write")
	waitConverged(t, cluster, 4, convergeTimeout)
	// Explicit honest-pair check: nodes 2 and 3 agree bit-for-bit.
	w1, w2 := names(t, cluster.Node(1)), names(t, cluster.Node(2))
	if !equalStrings(w1, w2) {
		t.Fatalf("honest pair diverged: node2 %v != node3 %v", w1, w2)
	}

	failed = false
}

func bytes2MiB() []byte {
	out := make([]byte, 2<<20)
	for i := range out {
		out[i] = 0x55
	}
	return out
}
