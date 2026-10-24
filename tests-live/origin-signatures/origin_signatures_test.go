// Live scenario: a forwarded origin survives an offline origin and a
// hostile relay.
//
// Go port of tests/origin_signatures.rs using tests-live/goharness plus
// the product murmur/replication wire types. Identical behavior: three
// nodes share one database id; A commits, B receives, A stops, and C
// receives A's transaction exclusively through B. A relay impersonating B
// (raw QUIC, product wire protocol via the shared harness attacker
// fixture) then serves capture plus modified / unsigned / cross-DBID /
// fabricated / chunk-identity attacks, all of which C must reject with
// rows, counters, and commit cursor untouched. Restart checks durability,
// a signed-duplicate replay checks idempotency, and an untrusted snapshot
// manifest checks donor trust. Plain SQL replaces the typed-records HTTP
// API. Rust enum message variants become replication.Message Kind +
// field sets; the Rust wait_until_async polls become
// goharness.WaitUntil.
package originsignatures

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

const (
	a = 0
	b = 1
	c = 2
)

// Convergence deadline per phase (Go: 30 s).
const convergeTimeout = 30 * time.Second

// Convergence poll interval (Go: 200 ms).
const poll = 200 * time.Millisecond

// Session attach/teardown observation bound (Go: 5 s).
const peerTimeout = 5 * time.Second

// Settle window after the idempotent re-offer (replay legs: 3-10 s).
const settle = 5 * time.Second

// Served-batch capture window (Need answers are immediate).
const captureTimeout = 10 * time.Second

// Chunk-availability probe window.
const probeTimeout = 10 * time.Second

// Post-send drain asserting no `SnapshotApplied`/no `Error` (Go: 3 s).
const drainTimeout = 3 * time.Second

// Per-leg rejection budget. C redials stopped B every 5 s and dial
// failures overwrite `last_error`, so a missed rejection text is resent
// on a fresh session until this budget elapses.
const rejectTimeout = 30 * time.Second

// `last_error` poll window per resend attempt.
const attemptWindow = 2 * time.Second

// Session pump slice while polling (drains control traffic so the
// victim's send queue never wedges mid-leg).
const pump = 100 * time.Millisecond

// Largest transaction the victim accepts (product default).
const maxTransaction = 64 << 20

func repeat16(v byte) [16]byte {
	var out [16]byte
	for i := range out {
		out[i] = v
	}
	return out
}

func repeat32(v byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = v
	}
	return out
}

var (
	// Forged transaction identity (asserted distinct from every real id).
	forgedID = repeat16(0xF0)
	// Chunk-proof identity (must differ from `chunkID`: that mismatch is
	// the chunk-identity attack).
	proofID = repeat16(0xF2)
	// Chunk envelope identity (tampered relative to its proof).
	chunkID = repeat16(0xF3)
	// Wrong database id for the cross-DBID leg.
	crossDB = repeat16(0xDB)
)

// names returns the canonical ordered names on one node (Go:
// `Cluster.TypedNames`).
func names(cluster *goharness.Cluster, idx int) ([]string, error) {
	rows, err := cluster.Node(idx).Query("SELECT name FROM records ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("node%d read names: %w", idx+1, err)
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.Get(0))
	}
	return out, nil
}

// insert writes one single-row transaction (Go: `TypedInsertWithID`).
func insert(cluster *goharness.Cluster, node int, id int64, name string) error {
	if _, err := cluster.Node(node).Execute(
		"INSERT INTO records(id, name) VALUES($1, $2)", id, name); err != nil {
		return fmt.Errorf("node%d insert failed: %w", node, err)
	}
	if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
		return fmt.Errorf("node%d flush after insert: %w", node, err)
	}
	return nil
}

// waitNames waits until `node` holds exactly `want` (Go: `waitRows` +
// `TypedNames`).
func waitNames(cluster *goharness.Cluster, node int, want []string, timeout time.Duration) error {
	converged := goharness.WaitUntil(timeout, poll, func() bool {
		got, err := names(cluster, node)
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
	if !converged {
		got, _ := names(cluster, node)
		return fmt.Errorf("node%d did not converge on %v within %v (has %v)", node+1, want, timeout, got)
	}
	return nil
}

// connectedPeers returns the connected replication sessions on one node
// (Go: `connected_peers`).
func connectedPeers(cluster *goharness.Cluster, idx int) (uint64, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return 0, fmt.Errorf("node%d SHOW REPLICATION: %w", idx+1, err)
	}
	if len(rows) == 0 || len(rows[0]) <= 2 {
		return 0, fmt.Errorf("node%d SHOW REPLICATION returned no peers", idx+1)
	}
	n, err := strconv.ParseUint(rows[0][2], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("node%d peers %q: %w", idx+1, rows[0][2], err)
	}
	return n, nil
}

// counters holds staged (`received`) and applied counters on one node.
// `received` only moves when a batch passes verification into staging,
// so it is the equivalent of the `batches_received_total` must-not-move
// assert; `applied` covers the nothing-applied / generation legs.
type counters struct {
	received uint64
	applied  uint64
}

func getCounters(cluster *goharness.Cluster, idx int) (counters, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return counters{}, fmt.Errorf("node%d SHOW REPLICATION: %w", idx+1, err)
	}
	if len(rows) == 0 {
		return counters{}, fmt.Errorf("node%d SHOW REPLICATION returned no rows", idx+1)
	}
	row := rows[0]
	if len(row) < 6 {
		return counters{}, fmt.Errorf("node%d SHOW REPLICATION has %d columns, want >= 6", idx+1, len(row))
	}
	received, err := strconv.ParseUint(row[4], 10, 64)
	if err != nil {
		return counters{}, fmt.Errorf("node%d staged %q: %w", idx+1, row[4], err)
	}
	applied, err := strconv.ParseUint(row[5], 10, 64)
	if err != nil {
		return counters{}, fmt.Errorf("node%d applied %q: %w", idx+1, row[5], err)
	}
	return counters{received: received, applied: applied}, nil
}

// waitCountersQuiet waits until no batch is staged and the applied cursor
// holds still: `staged` is a gauge, so a baseline sampled mid-flight
// would demand in-flight verified data never apply. A timeout means the
// node never quiesced (a liveness bug), not a slow test.
func waitCountersQuiet(cluster *goharness.Cluster, idx int, what string) (counters, error) {
	start := time.Now()
	last, err := getCounters(cluster, idx)
	if err != nil {
		return counters{}, err
	}
	var stable uint32
	for {
		current, err := getCounters(cluster, idx)
		if err != nil {
			return counters{}, err
		}
		if current.received == 0 && current.applied == last.applied {
			stable++
			if stable >= 3 {
				return current, nil
			}
		} else {
			stable = 0
		}
		last = current
		if time.Since(start) >= 30*time.Second {
			return counters{}, fmt.Errorf("%s: node%d counters never quiesced (staged=%d, applied=%d)",
				what, idx+1, last.received, last.applied)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func assertCounters(cluster *goharness.Cluster, idx int, base counters, ctx string) error {
	got, err := getCounters(cluster, idx)
	if err != nil {
		return err
	}
	if got.received != base.received || got.applied != base.applied {
		return fmt.Errorf("node%d counters moved %s: received %d -> %d, applied %d -> %d (a batch staged or applied)",
			idx+1, ctx, base.received, got.received, base.applied, got.applied)
	}
	return nil
}

// lastError returns the latest replication error on one node (attack
// rejections land here), or nil when empty.
func lastError(cluster *goharness.Cluster, idx int) (*string, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return nil, fmt.Errorf("node%d SHOW REPLICATION: %w", idx+1, err)
	}
	if len(rows) == 0 || len(rows[0]) <= 6 || rows[0][6] == "" {
		return nil, nil
	}
	s := rows[0][6]
	return &s, nil
}

func strPtrEq(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func strPtrDeref(s *string) string {
	if s == nil {
		return "<none>"
	}
	return *s
}

// committed returns the local commit cursor (Go: `state_generation`
// advance check).
func committed(cluster *goharness.Cluster, idx int) (uint64, error) {
	rows, err := cluster.Node(idx).Simple("MURMUR SHOW REPLICATION")
	if err != nil {
		return 0, fmt.Errorf("node%d SHOW REPLICATION: %w", idx+1, err)
	}
	if len(rows) == 0 || len(rows[0]) <= 7 {
		return 0, fmt.Errorf("node%d SHOW REPLICATION returned no commit cursor", idx+1)
	}
	n, err := strconv.ParseUint(rows[0][7], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("node%d SHOW REPLICATION returned no commit cursor", idx+1)
	}
	return n, nil
}

// baseline is everything a rejected attack must leave untouched.
type baseline struct {
	counters  counters
	committed uint64
}

func getBaseline(cluster *goharness.Cluster, idx int) (baseline, error) {
	c, err := getCounters(cluster, idx)
	if err != nil {
		return baseline{}, err
	}
	n, err := committed(cluster, idx)
	if err != nil {
		return baseline{}, err
	}
	return baseline{counters: c, committed: n}, nil
}

// assertProtected requires rows, counters, and commit cursor unchanged
// (Go: typed state, HLC, generation, and receipts unchanged).
func assertProtected(cluster *goharness.Cluster, base baseline, ctx string) error {
	if err := assertCounters(cluster, c, base.counters, ctx); err != nil {
		return err
	}
	got, err := committed(cluster, c)
	if err != nil {
		return err
	}
	if got != base.committed {
		return fmt.Errorf("node%d commit cursor moved %s: %d -> %d (a forgery published)",
			c+1, ctx, base.committed, got)
	}
	rows, err := names(cluster, c)
	if err != nil {
		return err
	}
	if len(rows) != 1 || rows[0] != "from-A" {
		return fmt.Errorf("node%d rows changed %s (a forgery applied)", c+1, ctx)
	}
	return nil
}

// waitQuiet waits until `idx` has no connected sessions (teardown
// observation, and the required gate before every relay dial: the engine
// closes a second inbound session for an id with a lingering entry as a
// duplicate).
func waitQuiet(cluster *goharness.Cluster, idx int, what string) error {
	converged := goharness.WaitUntil(peerTimeout, 50*time.Millisecond, func() bool {
		n, err := connectedPeers(cluster, idx)
		return err == nil && n == 0
	})
	if !converged {
		return fmt.Errorf("%s: node%d sessions did not tear down", what, idx+1)
	}
	return nil
}

// captureOriginal is the served-origin-proof capture (Go: reading A's
// committed batch from the stopped origin's store). The relay fetches A's
// batches from C over a `Need` session and verifies every served batch
// under A's origin key. It returns the row transaction (highest non-empty
// sequence) plus C's schema bytes from the handshake (for the
// untrusted-manifest leg).
func captureOriginal(cluster *goharness.Cluster, relay *goharness.Attacker) (replication.Transaction, []byte, error) {
	var zero replication.Transaction
	if err := waitQuiet(cluster, c, "origin-proof capture"); err != nil {
		return zero, nil, err
	}
	db := cluster.DatabaseID()
	idA, idC := cluster.NodeID(a), cluster.NodeID(c)
	addr := cluster.NodeInfo(c).ReplAddr
	s, err := relay.Dial(addr, db, idC)
	if err != nil {
		return zero, nil, fmt.Errorf("capture dial: %w", err)
	}
	if err := s.Send(&replication.Message{Kind: replication.MsgNeed, Origin: idA, From: 1, Through: math.MaxUint64}); err != nil {
		s.Close()
		return zero, nil, fmt.Errorf("capture need: %w", err)
	}
	frames := s.CollectUntil(captureTimeout, func(m *replication.Message) bool {
		if m.Kind != replication.MsgBatches {
			return false
		}
		for _, tx := range m.Transactions {
			if tx.Origin == idA && len(tx.Payload) > 0 {
				return true
			}
		}
		return false
	})
	// Drain stragglers so the highest served sequence wins, not the first.
	frames = append(frames, s.CollectUntil(2*time.Second, func(*replication.Message) bool { return false })...)
	schema := s.WelcomeSchema()
	s.Close()

	trusted := map[replication.NodeId][32]byte{idA: cluster.NodeOriginPubkey(a)}
	bySeq := map[uint64]replication.Transaction{}
	for _, message := range frames {
		if message.Kind != replication.MsgBatches {
			continue
		}
		for _, tx := range message.Transactions {
			if tx.Origin != idA {
				continue
			}
			if err := tx.Verify(db, trusted, maxTransaction); err != nil {
				return zero, nil, fmt.Errorf("served batch failed origin verification: %w", err)
			}
			bySeq[tx.Sequence] = tx
		}
	}
	if len(bySeq) == 0 {
		return zero, nil, fmt.Errorf("C served no batches for offline origin A")
	}
	var seqs []uint64
	for seq := range bySeq {
		seqs = append(seqs, seq)
	}
	// Sort descending so the highest served sequence wins.
	sort.Slice(seqs, func(a, b int) bool { return seqs[a] > seqs[b] })
	fmt.Fprintf(os.Stderr, "capture: C served verified origin-A batches at sequences %v\n", seqs)
	for _, seq := range seqs {
		if tx := bySeq[seq]; len(tx.Payload) > 0 {
			return tx, schema, nil
		}
	}
	return zero, nil, fmt.Errorf("C served only empty origin-A batches")
}

// cloneTransaction deep-copies a transaction's byte slices (Rust:
// `original.clone()`).
func cloneTransaction(tx replication.Transaction) replication.Transaction {
	tx.Payload = append([]byte(nil), tx.Payload...)
	tx.Signature = append([]byte(nil), tx.Signature...)
	return tx
}

// expectRejection sends one hostile message, retrying across fresh relay
// sessions until the victim records a *changed* `last_error` containing
// `want` (authentication failures close the session, so every attempt
// dials anew; redial noise can transiently overwrite `last_error`, so a
// miss resends instead of failing). It returns the new error for
// chaining.
func expectRejection(cluster *goharness.Cluster, relay *goharness.Attacker, make func() *replication.Message, prev *string, want, what string) (*string, error) {
	db := cluster.DatabaseID()
	victim := cluster.NodeID(c)
	addr := cluster.NodeInfo(c).ReplAddr
	deadline := time.Now().Add(rejectTimeout)
	for {
		if err := waitQuiet(cluster, c, what); err != nil {
			return nil, err
		}
		s, err := relay.Dial(addr, db, victim)
		if err != nil {
			return nil, fmt.Errorf("%s: relay dial: %w", what, err)
		}
		sent := s.Send(make()) == nil
		if sent {
			attemptEnd := time.Now().Add(attemptWindow)
			for {
				cur, err := lastError(cluster, c)
				if err != nil {
					s.Close()
					return nil, err
				}
				if !strPtrEq(cur, prev) && cur != nil && strings.Contains(*cur, want) {
					fmt.Fprintf(os.Stderr, "%s: rejected with %q\n", what, *cur)
					s.Close()
					return cur, nil
				}
				for _, message := range s.CollectUntil(pump, func(*replication.Message) bool { return false }) {
					if message.Kind == replication.MsgError {
						fmt.Fprintf(os.Stderr, "%s: wire error frame: code=%d %q\n",
							what, message.Code, message.ErrMessage)
					}
				}
				if !time.Now().Before(attemptEnd) {
					break
				}
			}
		}
		s.Close()
		if !time.Now().Before(deadline) {
			cur, _ := lastError(cluster, c)
			return nil, fmt.Errorf("%s: no %q rejection within %v (last_error: %s)",
				what, want, rejectTimeout, strPtrDeref(cur))
		}
	}
}

// probeAvailability is the chunk-availability probe: the forged chunk
// identities must have no durable staging (Go: tampering a chunk identity
// fails before durable staging or cleanup).
func probeAvailability(cluster *goharness.Cluster, relay *goharness.Attacker, forgedSeq, chunkSeq uint64) error {
	if err := waitQuiet(cluster, c, "chunk-availability probe"); err != nil {
		return err
	}
	db := cluster.DatabaseID()
	idA, idC := cluster.NodeID(a), cluster.NodeID(c)
	addr := cluster.NodeInfo(c).ReplAddr
	s, err := relay.Dial(addr, db, idC)
	if err != nil {
		return fmt.Errorf("probe dial: %w", err)
	}
	if err := s.Send(&replication.Message{Kind: replication.MsgChunkAvailabilityRequest, Offset: 0}); err != nil {
		s.Close()
		return fmt.Errorf("probe request: %w", err)
	}
	pages := s.CollectUntil(probeTimeout, func(m *replication.Message) bool {
		return m.Kind == replication.MsgChunkAvailabilityPage && !m.More
	})
	s.Close()
	sawPage := false
	for _, m := range pages {
		if m.Kind == replication.MsgChunkAvailabilityPage {
			sawPage = true
			break
		}
	}
	if !sawPage {
		return fmt.Errorf("C answered no chunk-availability page")
	}
	for _, message := range pages {
		if message.Kind != replication.MsgChunkAvailabilityPage {
			continue
		}
		for _, avail := range message.Availability {
			if avail.Origin == idA && (avail.Sequence == forgedSeq || avail.Sequence == chunkSeq) {
				return fmt.Errorf("forged chunks durably staged for origin A seq %d: %v",
					avail.Sequence, avail.Indexes)
			}
		}
	}
	return nil
}

// replayOriginal is the signed-duplicate replay (Go: after restart): the
// original batch is absorbed idempotently — no error frame, nothing
// counted twice, rows and cursor unchanged.
func replayOriginal(cluster *goharness.Cluster, relay *goharness.Attacker, original replication.Transaction) error {
	if err := waitQuiet(cluster, c, "signed-duplicate replay"); err != nil {
		return err
	}
	db := cluster.DatabaseID()
	idC := cluster.NodeID(c)
	addr := cluster.NodeInfo(c).ReplAddr
	s, err := relay.Dial(addr, db, idC)
	if err != nil {
		return fmt.Errorf("replay dial: %w", err)
	}
	if err := s.Send(&replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{original}}); err != nil {
		s.Close()
		return fmt.Errorf("replay send: %w", err)
	}
	tail := s.CollectUntil(drainTimeout, func(*replication.Message) bool { return false })
	s.Close()
	for _, m := range tail {
		if m.Kind == replication.MsgError {
			return fmt.Errorf("signed duplicate drew error %d: %s", m.Code, m.ErrMessage)
		}
	}
	return nil
}

func runBody(cluster *goharness.Cluster) error {
	// Same table on every node (Go: TypedRecords fixture tables), flagged
	// replicated on each node (forwarding needs the per-node flag).
	for i := 0; i < cluster.Len(); i++ {
		if err := cluster.EnsureMigrated(i, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
			return fmt.Errorf("migrate node%d: %w", i+1, err)
		}
	}
	for i := 0; i < cluster.Len(); i++ {
		if _, err := cluster.Node(i).Simple("MURMUR SET TABLE records REPLICATED true"); err != nil {
			return fmt.Errorf("node%d SET TABLE REPLICATED: %w", i+1, err)
		}
		if _, err := cluster.Node(i).Simple("MURMUR FLUSH"); err != nil {
			return fmt.Errorf("node%d flush after SET TABLE: %w", i+1, err)
		}
	}

	// A commits; B receives; A stops.
	if err := cluster.AddPeer(a, b); err != nil {
		return fmt.Errorf("mesh A->B: %w", err)
	}
	if err := cluster.AddPeer(b, a); err != nil {
		return fmt.Errorf("mesh B->A: %w", err)
	}
	if err := cluster.SyncAll(); err != nil {
		return fmt.Errorf("sync A<->B: %w", err)
	}
	if err := insert(cluster, a, 1, "from-A"); err != nil {
		return err
	}
	if err := waitNames(cluster, b, []string{"from-A"}, convergeTimeout); err != nil {
		return err
	}
	cluster.StopNode(a)

	// C learns A's transaction exclusively from B while A remains stopped.
	if err := cluster.AddPeer(b, c); err != nil {
		return fmt.Errorf("mesh B->C: %w", err)
	}
	if err := cluster.AddPeer(c, b); err != nil {
		return fmt.Errorf("mesh C->B: %w", err)
	}
	if err := cluster.SyncAll(); err != nil {
		return fmt.Errorf("sync B<->C: %w", err)
	}
	if err := waitNames(cluster, c, []string{"from-A"}, convergeTimeout); err != nil {
		return err
	}
	bNames, err := names(cluster, b)
	if err != nil {
		return err
	}
	cNames, err := names(cluster, c)
	if err != nil {
		return err
	}
	if len(bNames) != len(cNames) || (len(bNames) > 0 && bNames[0] != cNames[0]) {
		return fmt.Errorf("forwarded state differs between B and C")
	}

	// Honest baseline on C (Go: `before := status` + honest receipt).
	// The applied row is the receipt proof: receipts commit atomically
	// with the apply, and there is no public receipt read API. Drain
	// first: `staged` is a gauge, and a baseline sampled mid-flight
	// would demand in-flight verified data never apply.
	if _, err := waitCountersQuiet(cluster, c, "honest convergence"); err != nil {
		return err
	}
	base, err := getCounters(cluster, c)
	if err != nil {
		return err
	}
	honest, err := names(cluster, c)
	if err != nil {
		return err
	}
	if len(honest) != 1 || honest[0] != "from-A" {
		return fmt.Errorf("honest transaction missing on C")
	}

	// B stops; from here C is alone with its forwarded state. Batches
	// B sent before dying still apply afterwards (verified data must
	// never be dropped), so drain to quiescence and require only that
	// the applied cursor never moves backwards; the attack legs below
	// re-baseline and assert exact stillness instead.
	cluster.StopNode(b)
	if err := waitQuiet(cluster, c, "B shutdown"); err != nil {
		return err
	}
	drained, err := waitCountersQuiet(cluster, c, "B shutdown drain")
	if err != nil {
		return err
	}
	if drained.applied < base.applied {
		return fmt.Errorf("node%d applied cursor moved backwards after B shutdown: %d -> %d",
			c+1, base.applied, drained.applied)
	}
	relayBase, err := getBaseline(cluster, c)
	if err != nil {
		return err
	}

	// Hostile relay, authenticated as B (fresh TLS leaf for B's id, B's
	// origin seed for signatures the victim accepts). Forged identities
	// must not collide with real ones.
	db := cluster.DatabaseID()
	idA, idB, idC := cluster.NodeID(a), cluster.NodeID(b), cluster.NodeID(c)
	for _, forged := range [][16]byte{forgedID, proofID, chunkID, crossDB} {
		for _, real := range [][16]byte{idA, idB, idC, db} {
			if forged == real {
				return fmt.Errorf("forged test identity collides with a real id")
			}
		}
	}
	if proofID == chunkID {
		return fmt.Errorf("chunk proof/envelope test identities must differ")
	}
	seedA := cluster.NodeOriginSeed(a)
	seedB := cluster.NodeOriginSeed(b)
	relay, err := goharness.Impersonate(cluster, b, seedB)
	if err != nil {
		return fmt.Errorf("relay identity: %w", err)
	}

	// Served-origin-proof capture: fetch A's batches from C, verified
	// under A's origin key.
	original, cSchema, err := captureOriginal(cluster, relay)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "capture: original origin-A seq %d id %s (%d payload bytes)\n",
		original.Sequence, goharness.Hex(original.ID[:]), len(original.Payload))
	forgedSeq := original.Sequence + 1
	chunkSeq := original.Sequence + 2

	// Modified batch: fresh identity, tampered payload bytes, original
	// signature (Go: `modified`). Fails the payload digest.
	modified := cloneTransaction(original)
	modified.Sequence = forgedSeq
	modified.ID = forgedID
	modified.Payload[0] ^= 0x01
	prev, err := lastError(cluster, c)
	if err != nil {
		return err
	}
	prev, err = expectRejection(cluster, relay,
		func() *replication.Message {
			return &replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{modified}}
		}, prev, "payload digest", "modified batch")
	if err != nil {
		return err
	}
	if err := assertProtected(cluster, relayBase, "after the modified batch"); err != nil {
		return err
	}

	// Unsigned batch (Go: `unsigned`). Adjacent legs use distinct
	// rejection texts (ed25519 messages are not asserted verbatim).
	unsigned := cloneTransaction(original)
	unsigned.Sequence = forgedSeq
	unsigned.ID = forgedID
	unsigned.Signature = nil
	prev, err = expectRejection(cluster, relay,
		func() *replication.Message {
			return &replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{unsigned}}
		}, prev, "authentication", "unsigned batch")
	if err != nil {
		return err
	}
	if err := assertProtected(cluster, relayBase, "after the unsigned batch"); err != nil {
		return err
	}

	// Cross-DBID batch: valid A signature (verified locally first, so the
	// rejection isolates the database check), wrong database (Go: `crossDB`).
	cross := cloneTransaction(original)
	cross.Database = crossDB
	cross.Sequence = forgedSeq
	cross.ID = forgedID
	cross.Sign(&seedA)
	if err := cross.Verify(crossDB, map[replication.NodeId][32]byte{idA: cluster.NodeOriginPubkey(a)}, maxTransaction); err != nil {
		return fmt.Errorf("cross-DBID fixture must verify under its own DBID: %w", err)
	}
	prev, err = expectRejection(cluster, relay,
		func() *replication.Message {
			return &replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{cross}}
		}, prev, "transaction identity", "cross-DBID batch")
	if err != nil {
		return err
	}
	if err := assertProtected(cluster, relayBase, "after the cross-DBID batch"); err != nil {
		return err
	}

	// Fabricated batch: B's valid signature over an origin-A sequence
	// (Go: `fabricated`, signed with B's key). The relay's own key must
	// not authorize another origin's log.
	fabricated := cloneTransaction(original)
	fabricated.Sequence = forgedSeq
	fabricated.ID = forgedID
	fabricated.Sign(&seedB)
	if err := fabricated.Verify(db, map[replication.NodeId][32]byte{idA: cluster.NodeOriginPubkey(b)}, maxTransaction); err != nil {
		return fmt.Errorf("fabricated fixture must carry a valid B signature: %w", err)
	}
	prev, err = expectRejection(cluster, relay,
		func() *replication.Message {
			return &replication.Message{Kind: replication.MsgBatches, Transactions: []replication.Transaction{fabricated}}
		}, prev, "authentication", "fabricated batch")
	if err != nil {
		return err
	}
	if err := assertProtected(cluster, relayBase, "after the fabricated batch"); err != nil {
		return err
	}

	// Chunk-identity batch (Go: forged `TransactionChunk` with a tampered
	// identity). The proof is a valid A-signed header for the forged
	// sequence; the envelope claims a different id, so binding fails
	// before anything stages.
	proof := replication.Transaction{
		Database: db,
		Origin:   idA,
		Sequence: chunkSeq,
		ID:       proofID,
		Millis:   original.Millis,
		Logical:  original.Logical,
		Schema:   original.Schema,
	}
	proof.Sign(&seedA)
	if err := proof.VerifyHeader(db, map[replication.NodeId][32]byte{idA: cluster.NodeOriginPubkey(a)}); err != nil {
		return fmt.Errorf("chunk proof fixture must verify: %w", err)
	}
	chunk := replication.Chunk{
		Proof:    proof,
		Origin:   idA,
		Sequence: chunkSeq,
		ID:       chunkID,
		Index:    0,
		Count:    1,
		Total:    100,
		Digest:   repeat32(7),
		Data:     bytes.Repeat([]byte{7}, 100),
	}
	if _, err := expectRejection(cluster, relay,
		func() *replication.Message {
			return &replication.Message{Kind: replication.MsgTransactionChunk, Chunk: chunk}
		}, prev, "chunk proof identity", "chunk-identity batch"); err != nil {
		return err
	}
	if err := assertProtected(cluster, relayBase, "after the chunk-identity batch"); err != nil {
		return err
	}

	// Neither the batch legs nor the chunk leg may have staged anything.
	if err := probeAvailability(cluster, relay, forgedSeq, chunkSeq); err != nil {
		return err
	}
	if err := assertProtected(cluster, relayBase, "after the availability probe"); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "hostile relay rejected: rows, counters, and cursor untouched")

	// Restart C: the honest row is durable (A and B stay stopped).
	cluster.StopNode(c)
	if err := cluster.StartNode(c); err != nil {
		return fmt.Errorf("restart node3: %w", err)
	}
	restarted, err := names(cluster, c)
	if err != nil {
		return err
	}
	if len(restarted) != 1 || restarted[0] != "from-A" {
		return fmt.Errorf("honest row lost on restart")
	}
	// Fresh process: counters restart at zero (verified premise for the
	// idempotency legs below, not an assumption).
	post, err := getBaseline(cluster, c)
	if err != nil {
		return err
	}
	if post.counters.received != 0 || post.counters.applied != 0 {
		return fmt.Errorf("fresh-process counters must start at zero")
	}

	// Signed-duplicate replay over the relay: idempotent across restart
	// (no error frame, nothing counted twice, rows and cursor unchanged).
	if err := replayOriginal(cluster, relay, original); err != nil {
		return err
	}
	if err := assertProtected(cluster, post, "after the signed-duplicate replay"); err != nil {
		return err
	}

	// An authenticated relay is not automatically a trusted snapshot
	// source: a manifest with a valid B signature from donor B is still
	// refused (C trusts no snapshot donors). The schema matches C's own
	// so the refusal isolates donor trust.
	manifest, _, err := replication.Build([2]replication.NodeId{db, idB}, idC,
		bytes.Repeat([]byte{0xAB}, 100), nil, replication.SchemaDigest(cSchema),
		relay.Config(), replication.NowMillis())
	if err != nil {
		return fmt.Errorf("untrusted manifest build: %w", err)
	}
	if manifest.Donor != idB || len(manifest.Chunks) != 1 || len(manifest.Signature) == 0 {
		return fmt.Errorf("untrusted manifest fixture malformed")
	}
	prev, err = lastError(cluster, c)
	if err != nil {
		return err
	}
	if _, err := expectRejection(cluster, relay,
		func() *replication.Message {
			return &replication.Message{Kind: replication.MsgSnapshotManifest, Manifest: *manifest}
		}, prev, "snapshot identity", "untrusted snapshot manifest"); err != nil {
		return err
	}
	if err := assertProtected(cluster, post, "after the untrusted snapshot manifest"); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "forwarded origin survived offline origin, hostile relay, and restart")

	// B rejoins and re-offers the transaction C already holds; C must
	// absorb the duplicate idempotently: no error, nothing counted
	// twice, rows unchanged (the engine dedups before counting, so
	// `received` stays 0).
	if err := cluster.StartNode(b); err != nil {
		return fmt.Errorf("restart node2: %w", err)
	}
	if err := cluster.AddPeer(b, c); err != nil {
		return fmt.Errorf("re-mesh B->C: %w", err)
	}
	if err := cluster.AddPeer(c, b); err != nil {
		return fmt.Errorf("re-mesh C->B: %w", err)
	}
	if err := cluster.SyncAll(); err != nil {
		return fmt.Errorf("sync after B rejoin: %w", err)
	}
	time.Sleep(settle)
	if err := assertProtected(cluster, post, "after the idempotent re-offer"); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "honest re-offer absorbed idempotently after the relay legs")
	return nil
}

func TestForwardedOriginSurvivesOfflineOriginAndHostileRelay(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	opts := goharness.DefaultClusterOptions("origin-signatures")
	opts.ManualPeers = true
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	if cluster.Len() != 3 {
		t.Fatalf("this scenario is written for 3 nodes, got %d", cluster.Len())
	}
	failed := true
	defer func() { cluster.Finish(failed) }()
	if err := runBody(cluster); err != nil {
		fmt.Fprintf(os.Stderr, "scenario failed: %v\n", err)
		cluster.DumpForensics("origin-signatures")
		t.Fatalf("scenario failed: %v", err)
	}
	failed = false
}
