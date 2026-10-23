// Live scenario: a stale node rejoins while its only available snapshot
// source is hostile; it must discard the corrupt snapshot, then converge
// via the honest snapshot path.
//
// Go port of the Rust `corrupt-snapshot` scenario
// (/home/marc/MURMUR/tests-live/corrupt-snapshot/tests/corrupt_snapshot.rs).
// The attacker induces a snapshot pull with an unsolicited
// history-unavailable error, then serves a structurally valid manifest
// plus one intact and one bit-flipped chunk. The receiver must reject the
// corrupt chunk with nothing partially published (row count, digest, and
// commit cursor untouched), discard the staged transfer, then converge via
// the honest snapshot path with bit-identical digests. Plain SQL replaces
// the typed-records HTTP API, close/reopen replaces process stop/start,
// and rejection is observed via the replication last_error plus wire
// behavior.
package corruptsnapshot

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
	"github.com/marcgauthier/murmur-sql-go/murmur/tests-live/harness"
)

// stale is the role index: node3 goes stale, then rejoins (Rust: STALE = 2).
const stale = 2

// attackerID is the fixed test id, asserted distinct from the node ids.
var attackerID = [16]byte{
	0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7,
	0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7, 0xA7,
}

// attackerSeed is the attacker origin signing key (the victim trusts its
// public half, mirroring the Rust test authorizing the hostile source).
var attackerSeed = [32]byte{
	0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E,
	0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E,
	0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E,
	0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E, 0x5E,
}

const (
	// forgedBytes is the forged snapshot payload: two 64 KiB wire chunks.
	forgedBytes = 70000
	// forgedLifetime is the forged manifest lifetime (attacker-side):
	// short so the victim's expiry reclamation clears the staged corrupt
	// transfer before the honest leg.
	forgedLifetime = 20 * time.Second
	// reclaimMargin is the reclaim margin past the forged expiry (the
	// engine's maintenance tick runs about every second).
	reclaimMargin = 10 * time.Second
	// retention is the log retention (Rust: 1 s).
	retention = time.Second
	// maximumOffline is the offline-peer memory (Rust: 20 s).
	maximumOffline = 20 * time.Second
	// minimumRetained is the newest transactions per origin the survivors
	// must keep (Rust: 10).
	minimumRetained = uint64(10)
	// minCollected is the minimum pruned transactions per survivor before
	// the attack: 60 survivor writes minus the 10 retained, with margin.
	// Proves the stale node's needed ranges are genuinely unrecoverable
	// from logs, so the rejoin must use the snapshot path.
	minCollected = uint64(40)
	// baselineTimeout is the baseline convergence deadline (Rust: 60 s).
	baselineTimeout = 60 * time.Second
	// rowTimeout is the survivor row-count deadline (Rust: 30 s).
	rowTimeout = 30 * time.Second
	// convergePoll is the convergence poll interval (Rust: 200 ms).
	convergePoll = 200 * time.Millisecond
	// collectTimeout is the GC collection-loop deadline past the expiry wait.
	collectTimeout = 60 * time.Second
	// dialTimeout is the attacker dial/handshake deadline (Rust: 15 s).
	dialTimeout = 15 * time.Second
	// requestTimeout is the snapshot-pull inducement deadline (Rust: 30 s).
	requestTimeout = 30 * time.Second
	// needTimeout is the forged-chunk request deadline (Rust: 15 s).
	needTimeout = 15 * time.Second
	// failTimeout is the corrupt-chunk rejection deadline (Rust: 30 s).
	failTimeout = 30 * time.Second
	// drainTimeout is the post-attack drain: no SnapshotApplied may
	// arrive (Rust: 3 s).
	drainTimeout = 3 * time.Second
	// settle is the post-attack settle before asserting nothing was
	// published (Rust: 2 s).
	settle = 2 * time.Second
	// honestTimeout is the honest-resync convergence deadline (Rust: 120 s).
	honestTimeout = 120 * time.Second
	// slice is one gradual read inside a collection window (hostile-peer
	// pattern: 500 ms).
	slice = 500 * time.Millisecond
)

// names returns the canonical ordered names on one node (Rust: names).
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

// digest is the canonical digest of a name set (Rust: sorted names joined
// with newlines).
func digest(nameList []string) string {
	return strings.Join(nameList, "\n")
}

// rowCount returns the row count on one node (cheap pre-check before full
// name reads).
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

// insert writes one single-row transaction (Rust: insert).
func insert(t *testing.T, cluster *harness.Cluster, node int, id int64, name string) {
	t.Helper()
	if _, err := cluster.Node(node).Transaction(func(s *murmur.Sql) (any, error) {
		_, err := s.Execute("INSERT INTO records(id, name) VALUES(?, ?)", id, name)
		return nil, err
	}); err != nil {
		t.Fatalf("node %d insert failed: %v", node, err)
	}
}

// waitRowCount waits until one node holds exactly want rows (Rust:
// wait_row_count).
func waitRowCount(t *testing.T, cluster *harness.Cluster, idx, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if rowCount(t, cluster.Node(idx)) == want {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("node %d record count = %d, want %d within %v",
				idx, rowCount(t, cluster.Node(idx)), want, timeout)
		}
		time.Sleep(convergePoll)
	}
}

// waitConverged waits until every node holds exactly want rows with equal
// digests (Rust: wait_converged).
func waitConverged(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		counts := make([]int, 0, cluster.Len())
		for i := 0; i < cluster.Len(); i++ {
			counts = append(counts, rowCount(t, cluster.Node(i)))
		}
		allWant := true
		for _, n := range counts {
			if n != want {
				allWant = false
				break
			}
		}
		if allWant {
			first := digest(names(t, cluster.Node(0)))
			equal := true
			for i := 1; i < cluster.Len(); i++ {
				if digest(names(t, cluster.Node(i))) != first {
					equal = false
					break
				}
			}
			if equal {
				return
			}
		}
		if !time.Now().Before(deadline) {
			for i := 0; i < cluster.Len(); i++ {
				nodeNames := names(t, cluster.Node(i))
				t.Logf("node %d at timeout: count=%d digest=%s", i, len(nodeNames), digest(nodeNames))
			}
			t.Fatalf("nodes did not converge on %d rows with equal digests within %v", want, timeout)
		}
		time.Sleep(convergePoll)
	}
}

// connectedPeers counts connected replication sessions on one node.
func connectedPeers(cluster *harness.Cluster, idx int) int {
	h := cluster.Node(idx).Replication()
	if h == nil {
		return 0
	}
	return h.Status().Peers
}

// lastError returns the latest replication error on one node (Rust:
// last_error; the corrupt-chunk rejection lands here).
func lastError(cluster *harness.Cluster, idx int) *string {
	h := cluster.Node(idx).Replication()
	if h == nil {
		return nil
	}
	return h.Status().LastError
}

// committed returns the local commit cursor (Rust: committed).
func committed(cluster *harness.Cluster, idx int) uint64 {
	return cluster.Node(idx).Status().Committed
}

// logCounters returns the log-path activity counters for one node:
// (received, applied). Snapshot application bypasses both counters, so a
// flat pair across the honest resync proves the rows arrived via snapshot.
func logCounters(cluster *harness.Cluster, idx int) (uint64, uint64) {
	m := cluster.Node(idx).Replication().Status().Metrics
	return m.ReceivedTransactions, m.AppliedTransactions
}

// collectHistory runs one bounded retention pass on one node; returns
// transactions pruned.
func collectHistory(t *testing.T, cluster *harness.Cluster, idx int) uint64 {
	t.Helper()
	pruned, err := cluster.Node(idx).CollectRetention()
	if err != nil {
		t.Fatalf("node %d collect retention: %v", idx, err)
	}
	return pruned
}

// collectedTotal returns total transactions ever pruned on one node.
func collectedTotal(t *testing.T, cluster *harness.Cluster, idx int) uint64 {
	t.Helper()
	status, err := cluster.Node(idx).RetentionStatus()
	if err != nil {
		t.Fatalf("retention status: %v", err)
	}
	return status.CollectedTransactions
}

// attacker is the malicious snapshot-source fixture: a raw QUIC client
// speaking the replication wire protocol (hostile-peer pattern).
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
	return &attacker{
		tlsConf: &tls.Config{
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
		},
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

type inboundMsg struct {
	msg replication.Message
	err error
}

// evilSession is one attack session with its handshake stream open. A
// reader goroutine owns the stream so framing is never split across read
// deadlines.
type evilSession struct {
	conn          *quic.Conn
	stream        *quic.Stream
	welcomeSchema []byte
	inbound       chan inboundMsg
}

// dial completes a handshake against the victim and returns the session
// plus the victim's schema bytes (revealed by its Welcome).
func (a *attacker) dial(t *testing.T, addr netip.AddrPort, expect [16]byte) *evilSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	conn, err := quic.DialAddr(ctx, addr.String(), a.tlsConf, a.quicConf)
	cancel()
	if err != nil {
		t.Fatalf("attacker connect: %v", err)
	}
	openCtx, openCancel := context.WithTimeout(context.Background(), dialTimeout)
	stream, err := conn.OpenStreamSync(openCtx)
	openCancel()
	if err != nil {
		_ = conn.CloseWithError(0, "dial failed")
		t.Fatalf("attacker open stream: %v", err)
	}
	hello := replication.Message{
		Kind:           replication.MsgHello,
		Capabilities:   replication.Capabilities,
		Required:       replication.Required,
		Version:        replication.Version,
		Database:       a.db,
		Node:           attackerID,
		Schema:         nil,
		Mode:           replication.DisseminationGossip,
		MaxTransaction: 64 << 20,
	}
	if err := replication.WriteMessage(stream, &hello); err != nil {
		_ = conn.CloseWithError(0, "dial failed")
		t.Fatalf("attacker write hello: %v", err)
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
			t.Fatalf("attacker expected Welcome, stream died: %v", in.err)
		}
		if in.msg.Kind != replication.MsgWelcome {
			t.Fatalf("attacker expected Welcome, got kind %d", in.msg.Kind)
		}
		if in.msg.Database != a.db {
			t.Fatalf("welcome database mismatch")
		}
		if in.msg.Node != expect {
			t.Fatalf("welcome node mismatch")
		}
		return &evilSession{conn: conn, stream: stream,
			welcomeSchema: append([]byte(nil), in.msg.Schema...), inbound: inbound}
	case <-time.After(10 * time.Second):
		_ = conn.CloseWithError(0, "dial failed")
		t.Fatalf("attacker Welcome timeout")
		return nil
	}
}

func (s *evilSession) send(t *testing.T, msg *replication.Message) {
	t.Helper()
	if err := replication.WriteMessage(s.stream, msg); err != nil {
		t.Fatalf("attacker send kind %d: %v", msg.Kind, err)
	}
}

// collectUntil reads until want matches, total elapses, or the session
// dies (patient across quiet slices).
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
			if !ok || in.err != nil {
				return out // session died; the caller interprets
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

func (s *evilSession) close() {
	_ = s.conn.CloseWithError(0, "attack done")
}

// forgedSnapshot is a manifest plus chunk payloads whose second chunk was
// bit-flipped after the manifest digests were computed.
type forgedSnapshot struct {
	manifest  *replication.Manifest
	transfer  [16]byte
	chunk0    []byte
	chunk1bad []byte
}

// buildCorruptSnapshot forges a structurally valid two-chunk snapshot for
// the victim's schema: the manifest passes every structural check, but the
// final chunk's bytes differ from what its chunk hash covers.
func buildCorruptSnapshot(t *testing.T, config *replication.Config, db, victim [16]byte, victimSchema []byte) *forgedSnapshot {
	t.Helper()
	data := make([]byte, forgedBytes)
	for i := range data {
		data[i] = 'a' + byte(i%26)
	}
	manifest, _, err := replication.Build([2][16]byte{db, attackerID}, victim, data, nil,
		replication.SchemaDigest(victimSchema), config, replication.NowMillis())
	if err != nil {
		t.Fatalf("forge manifest: %v", err)
	}
	if len(manifest.Chunks) != 2 {
		t.Fatalf("fixture: forged snapshot must span exactly two chunks, got %d", len(manifest.Chunks))
	}

	// Non-vacuous guards: the uncorrupted bytes verify against the
	// manifest, and the corrupted bytes must not.
	if sha256.Sum256(data) != manifest.Digest {
		t.Fatal("fixture: uncorrupted snapshot does not verify locally")
	}
	chunk0 := append([]byte(nil), data[:replication.ChunkBytes]...)
	chunk1bad := append([]byte(nil), data[replication.ChunkBytes:]...)
	if len(data) != len(chunk0)+len(chunk1bad) {
		t.Fatal("fixture: unexpected third chunk")
	}
	// Flip a byte mid-chunk. Only the digest differs.
	mid := len(chunk1bad) / 2
	chunk1bad[mid] ^= 0x01
	if len(chunk1bad) != len(data)-len(chunk0) {
		t.Fatal("fixture: corruption changed the encoded length")
	}
	if sha256.Sum256(chunk1bad) == manifest.Chunks[1] {
		t.Fatal("fixture: corrupted snapshot still verifies locally")
	}
	return &forgedSnapshot{manifest: manifest, transfer: manifest.Transfer, chunk0: chunk0, chunk1bad: chunk1bad}
}

func bytes16(b byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = b
	}
	return id
}

func TestStaleNodeDiscardsCorruptSnapshotThenConverges(t *testing.T) {
	// Fixed node ids (asserted distinct from the attacker id) so the
	// snapshot-trust overlays can be built before the cluster opens.
	nodeIDs := [][16]byte{bytes16(0x10), bytes16(0x20), bytes16(0x30)}
	for _, id := range nodeIDs {
		if id == attackerID {
			t.Fatal("attacker id collides with a node id")
		}
	}
	// Every node trusts every node as a snapshot donor; the victim
	// additionally authorizes the hostile source.
	allNodes := make(map[[16]byte]bool)
	for _, id := range nodeIDs {
		allNodes[id] = true
	}
	trustedSnapshotSources := make(map[int]map[[16]byte]bool)
	for i := range nodeIDs {
		cp := make(map[[16]byte]bool)
		for k, v := range allNodes {
			cp[k] = v
		}
		trustedSnapshotSources[i] = cp
	}
	victimSources := make(map[[16]byte]bool)
	for k, v := range allNodes {
		victimSources[k] = v
	}
	victimSources[attackerID] = true
	trustedSnapshotSources[stale] = victimSources
	extraTrustedOrigins := map[int]map[[16]byte][32]byte{
		stale: {attackerID: replication.PublicKey(&attackerSeed)},
	}

	ret := retention
	minRet := minimumRetained
	maxOff := maximumOffline
	cluster, err := harness.NewCluster(harness.ClusterOptions{
		Name:     "corrupt-snapshot",
		NumNodes: 3,
		NodeIDs:  nodeIDs,
		// Manual peers so the stale node restarts truly peerless: the
		// attacker becomes its only snapshot source by construction.
		ManualPeers: true,
		Replication: &harness.ReplicationOptions{
			Retention:       &ret,
			MinimumRetained: &minRet,
			MaximumOffline:  &maxOff,
		},
		TrustedSnapshotSources: trustedSnapshotSources,
		ExtraTrustedOrigins:    extraTrustedOrigins,
	})
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	failed := true
	defer func() {
		if failed {
			cluster.DumpForensics("corrupt-snapshot-no-convergence")
		}
		cluster.Finish(failed)
	}()

	// Same schema on every node.
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.Node(i).Migrate(murmur.NewMigration(
			"CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")); err != nil {
			t.Fatalf("migrate records table: %v", err)
		}
		if err := cluster.Node(i).Flush(); err != nil {
			t.Fatalf("flush schema: %v", err)
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

	// Phase 1: build the honest mesh explicitly and converge a baseline.
	for _, pair := range [][2]int{{0, 1}, {1, 0}, {0, 2}, {2, 0}, {1, 2}, {2, 1}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			t.Fatalf("AddPeer(%d,%d): %v", pair[0], pair[1], err)
		}
	}
	for i := 0; i < 5; i++ {
		insert(t, cluster, stale, int64(i+1), fmt.Sprintf("stale-%d", i))
	}
	if err := cluster.Node(stale).Flush(); err != nil {
		t.Fatalf("flush baseline: %v", err)
	}
	waitConverged(t, cluster, 5, baselineTimeout)
	preStopDigest := digest(names(t, cluster.Node(stale)))

	// Phase 2: isolate node3, stop it, and let the survivors write past
	// retention so the rejoin genuinely needs the snapshot path.
	for _, pair := range [][2]int{{2, 0}, {2, 1}, {0, 2}, {1, 2}} {
		if err := cluster.RemovePeer(pair[0], pair[1]); err != nil {
			t.Fatalf("RemovePeer(%d,%d): %v", pair[0], pair[1], err)
		}
	}
	cluster.StopNode(stale)
	// Flush every write: the product mints one replication log entry per
	// flush batch, not per SQL transaction.
	for i := 0; i < 60; i++ {
		insert(t, cluster, 0, int64(100+i), fmt.Sprintf("fresh-%d", i))
		if err := cluster.Node(0).Flush(); err != nil {
			t.Fatalf("flush survivor write: %v", err)
		}
	}
	waitRowCount(t, cluster, 0, 65, rowTimeout)
	waitRowCount(t, cluster, 1, 65, rowTimeout)
	// Member-deadline expiry, then explicit collection passes: the stale
	// node's retention pin must lapse before its needed ranges become
	// genuinely unrecoverable from logs.
	t.Log("waiting for member-deadline expiry and collection")
	time.Sleep(harness.EnvSeconds("MURMUR_CORRUPT_SNAPSHOT_GC_WAIT_SECONDS", 30))
	collectDeadline := time.Now().Add(collectTimeout)
	for {
		totals := []uint64{collectedTotal(t, cluster, 0), collectedTotal(t, cluster, 1)}
		if totals[0] >= minCollected && totals[1] >= minCollected {
			t.Logf("survivors pruned %v transactions past retention", totals)
			break
		}
		if !time.Now().Before(collectDeadline) {
			t.Fatalf("survivors collected only %v transactions, want >= %d each (snapshot path not forced)",
				totals, minCollected)
		}
		collectHistory(t, cluster, 0)
		collectHistory(t, cluster, 1)
		time.Sleep(2 * time.Second)
	}
	// Prove the deadline leg explicitly: the victim is now older than
	// every survivor's offline memory.
	for _, i := range []int{0, 1} {
		status, err := cluster.Node(i).RetentionStatus()
		if err != nil {
			t.Fatalf("retention status: %v", err)
		}
		victim := cluster.NodeID(stale)
		found := false
		var age uint64
		for _, p := range status.Peers {
			if p.Node == victim {
				age = replication.NowMillis() - p.LastSeenMillis
				if p.LastSeenMillis > replication.NowMillis() {
					age = 0
				}
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("node %d has no victim retention record", i)
		}
		if age <= uint64(maximumOffline.Milliseconds()) {
			t.Fatalf("node %d still pins the victim (last seen %d ms ago)", i, age)
		}
	}

	// Phase 3: node3 restarts peerless; the attacker's inbound session is
	// its only source. The attacker induces a snapshot pull, then serves
	// bit-flipped chunks.
	if err := cluster.StartNode(stale); err != nil {
		t.Fatalf("stale node restart: %v", err)
	}
	victimAddr := cluster.NodeInfo(stale).ReplAddr
	victimID := cluster.NodeID(stale)
	dbid := cluster.DatabaseID()
	genBeforeAttack := committed(cluster, stale)
	logRXBefore, logAppliedBefore := logCounters(cluster, stale)

	issued, err := cluster.IssueCert(attackerID)
	if err != nil {
		t.Fatalf("issue attacker cert: %v", err)
	}
	attackerConfig := replication.NewConfig(
		netip.MustParseAddrPort("127.0.0.1:0"), issued.Credentials, attackerSeed)
	attackerConfig.SnapshotTransferLifetime = forgedLifetime

	evil := newAttacker(t, issued.Credentials, dbid).dial(t, victimAddr, victimID)
	// Prove the "only available source" premise: exactly one peer. The
	// victim registers the inbound session asynchronously, so poll.
	onePeer := harness.WaitUntil(10*time.Second, 50*time.Millisecond, func() bool {
		return connectedPeers(cluster, stale) == 1
	})
	if !onePeer {
		t.Fatalf("stale node has %d connected peers during the attack, want 1",
			connectedPeers(cluster, stale))
	}
	// Induce the pull: an unsolicited history-unavailable error makes
	// the node request a snapshot from this peer over the live session.
	evil.send(t, &replication.Message{Kind: replication.MsgError, Code: 3, ErrMessage: "ranges unavailable"})
	reqs := evil.collectUntil(requestTimeout, func(m *replication.Message) bool {
		return m.Kind == replication.MsgSnapshotRequest
	})
	seenRequest := false
	for _, m := range reqs {
		if m.Kind == replication.MsgSnapshotRequest {
			seenRequest = true
			break
		}
	}
	if !seenRequest {
		t.Fatal("stale node never requested a snapshot from the attacker")
	}
	forged := buildCorruptSnapshot(t, attackerConfig, dbid, victimID, evil.welcomeSchema)
	evil.send(t, &replication.Message{Kind: replication.MsgSnapshotManifest, Manifest: *forged.manifest})
	// The manifest passed structural checks: the node asks for its
	// chunks (the received-counters-moved leg).
	needs := evil.collectUntil(needTimeout, func(m *replication.Message) bool {
		return m.Kind == replication.MsgSnapshotNeed
	})
	seenNeed := false
	for _, m := range needs {
		if m.Kind == replication.MsgSnapshotNeed && m.Transfer == forged.transfer {
			seenNeed = true
			break
		}
	}
	if !seenNeed {
		t.Fatal("victim never asked for the forged chunks (manifest not accepted)")
	}
	evil.send(t, &replication.Message{Kind: replication.MsgSnapshotChunk,
		Transfer: forged.transfer, Index: 0, Data: forged.chunk0})
	evil.send(t, &replication.Message{Kind: replication.MsgSnapshotChunk,
		Transfer: forged.transfer, Index: 1, Data: forged.chunk1bad})
	// The corrupt transfer must never complete: drain while asserting
	// no SnapshotApplied arrives (the victim closes the session on
	// the digest mismatch, which ends the drain early).
	tail := evil.collectUntil(drainTimeout, func(*replication.Message) bool { return false })
	for _, m := range tail {
		if m.Kind == replication.MsgSnapshotApplied {
			t.Fatal("victim applied the corrupt snapshot")
		}
	}
	evil.close()

	// The receiver must detect the digest mismatch and fail the chunk...
	failDeadline := time.Now().Add(failTimeout)
	for {
		if errText := lastError(cluster, stale); errText != nil && strings.Contains(*errText, "snapshot chunk") {
			t.Logf("victim rejected the corrupt chunk: %s", *errText)
			break
		}
		if !time.Now().Before(failDeadline) {
			t.Fatal("victim never recorded the corrupt-chunk rejection")
		}
		time.Sleep(convergePoll)
	}
	// ...while no snapshot completed and nothing was partially published.
	time.Sleep(settle)
	if n := rowCount(t, cluster.Node(stale)); n != 5 {
		t.Fatalf("stale node row count moved on the corrupt transfer (partial publication): %d", n)
	}
	if d := digest(names(t, cluster.Node(stale))); d != preStopDigest {
		t.Fatal("stale node digest changed on the corrupt transfer (partial publication)")
	}
	if c := committed(cluster, stale); c != genBeforeAttack {
		t.Fatalf("commit cursor moved on the corrupt transfer (partial publication): %d vs %d", c, genBeforeAttack)
	}
	if rx, applied := logCounters(cluster, stale); rx != logRXBefore || applied != logAppliedBefore {
		t.Fatal("replication counters moved on the corrupt transfer (partial publication)")
	}

	// The staged corrupt transfer is discarded on expiry (the only
	// staged-transfer discard path); the honest leg below proves it.
	time.Sleep(forgedLifetime + reclaimMargin)

	// Phase 4: honest peers return; the node retries from them and converges.
	for _, pair := range [][2]int{{2, 0}, {2, 1}, {0, 2}, {1, 2}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			t.Fatalf("honest AddPeer(%d,%d): %v", pair[0], pair[1], err)
		}
	}
	logPreHealRX, logPreHealApplied := logCounters(cluster, stale)
	waitConverged(t, cluster, 65, honestTimeout)
	// The rejoin completed via an honest snapshot, not the corrupt one
	// and not the logs: 60 rows arrived while the log-path counters
	// stayed flat (snapshot application bypasses both).
	if rx, applied := logCounters(cluster, stale); rx != logPreHealRX || applied != logPreHealApplied {
		t.Fatal("honest resync arrived over the log path, not a snapshot")
	}
	if c := committed(cluster, stale); c < genBeforeAttack {
		t.Fatal("commit cursor regressed (non-monotonic publication)")
	}
	// The stale node's acknowledged pre-stop state survives the merge.
	staleRows := 0
	for _, name := range names(t, cluster.Node(stale)) {
		if strings.HasPrefix(name, "stale-") {
			staleRows++
		}
	}
	if staleRows != 5 {
		t.Fatalf("stale rows after honest resync = %d, want 5", staleRows)
	}
	failed = false
}
