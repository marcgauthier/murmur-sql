package replication

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/netip"
	"net/url"
	"testing"
	"time"
)

// Large-transfer diagnostics: chunked delivery between two in-memory nodes.

type transferCreds struct {
	chain [][]byte
	key   []byte
}

func transferCredentials(t *testing.T, ids []NodeId) ([][]byte, []transferCreds) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "murmur-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	var creds []transferCreds
	for i, id := range ids {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i + 2)),
			Subject:      pkix.Name{CommonName: "murmur-test-node"},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			URIs:         []*url.URL{{Scheme: "replicateddb", Host: "node", Path: "/" + Hex(id[:])}},
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, key.Public(), caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		creds = append(creds, transferCreds{chain: [][]byte{der}, key: keyDER})
	}
	return [][]byte{caDER}, creds
}

func transferConfigs(t *testing.T) []*Config {
	t.Helper()
	ids := []NodeId{idOf(1), idOf(2)}
	seeds := [][32]byte{fill32(11), fill32(22)}
	roots, creds := transferCredentials(t, ids)
	trust := make(map[NodeId][32]byte)
	for i, id := range ids {
		seed := seeds[i]
		trust[id] = PublicKey(&seed)
	}
	var out []*Config
	for i, id := range ids {
		_ = id
		c := NewConfig(netip.MustParseAddrPort("127.0.0.1:0"),
			Credentials{Chain: creds[i].chain, PrivateKey: creds[i].key, Roots: roots},
			seeds[i])
		c.TrustedOrigins = trust
		c.AntiEntropyInterval = 500 * time.Millisecond
		c.IncomingInterval = 250 * time.Millisecond
		out = append(out, c)
	}
	return out
}

func transferBackend(node NodeId) *stubBackend {
	return newStubBackend(node, []byte("test-schema-v1"))
}

func sizedTransaction(payloadLen int) Transaction {
	tx := Transaction{
		Database: idOf(7), Origin: idOf(1), Sequence: 1, ID: idOf(9),
		Millis: 1, Logical: 1, Schema: SchemaDigest([]byte("test-schema-v1")),
		Payload: bytesRepeat(0xAB, payloadLen),
	}
	seed := fill32(11)
	tx.Sign(&seed)
	return tx
}

// craftChunks renders durable chunk records (manifest plus one record per
// index), exactly as the sender would have streamed them.
func craftChunks(t *testing.T, tx *Transaction, indexes []uint32) []Record {
	t.Helper()
	data := EncodeTransaction(tx)
	sum := sha256.Sum256(data)
	count := uint32((len(data) + ChunkBytes - 1) / ChunkBytes)
	proof := *tx
	proof.Payload = nil
	manifest := ChunkManifest{ID: tx.ID, Count: count, Total: uint64(len(data)), Digest: sum}
	records := []Record{PutRecord(ChunkManifestKey(tx.Origin, tx.Sequence), EncodeChunkManifest(&manifest))}
	for _, index := range indexes {
		start := int(index) * ChunkBytes
		end := start + ChunkBytes
		if end > len(data) {
			end = len(data)
		}
		chunk := Chunk{
			Proof: proof, Origin: tx.Origin, Sequence: tx.Sequence, ID: tx.ID,
			Index: index, Count: count, Total: uint64(len(data)),
			Digest: sum, Data: append([]byte(nil), data[start:end]...),
		}
		records = append(records, PutRecord(ChunkKey(tx.Origin, tx.Sequence, index), EncodeChunk(&chunk)))
	}
	return records
}

func TestLargeTransactionArrives(t *testing.T) {
	cfg := transferConfigs(t)
	backendA := transferBackend(idOf(1))
	backendB := transferBackend(idOf(2))
	tx := sizedTransaction(4 << 20)
	_ = tx.Digest()
	if err := backendA.Commit([]Record{
		PutRecord(LogKey(idOf(1), 1), EncodeTransaction(&tx)),
		PutRecord(RetainedKey(idOf(1)), EncodeProgress(&Progress{
			Origin: idOf(1), Observed: 1, RetainedFrom: 1, RetainedThrough: 1,
		})),
	}); err != nil {
		t.Fatal(err)
	}
	a, err := Start(cfg[0], backendA)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Start(cfg[1], backendB)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	addr := b.Status().Address
	if addr == nil {
		t.Fatal("no listen address")
	}
	if err := a.AddPeer(*addr); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	end := start.Add(120 * time.Second)
	lastLog := time.Now().Add(-10 * time.Second)
	for {
		raw, err := backendB.Get(ReceiptKey(idOf(1), 1))
		if err != nil {
			t.Fatal(err)
		}
		if raw != nil {
			break
		}
		if time.Since(lastLog) > 2*time.Second {
			ra, rb := a.Status(), b.Status()
			var aErr, bErr string
			if ra.LastError != nil {
				aErr = *ra.LastError
			}
			if rb.LastError != nil {
				bErr = *rb.LastError
			}
			t.Logf("waiting: t=%ds peers=%d/%d pushed=%d recv=%d staged=%d applied=%d a_err=%q b_err=%q",
				int(time.Since(start).Seconds()), ra.Peers, rb.Peers,
				ra.Metrics.PushedTransactions, rb.Metrics.ReceivedTransactions,
				rb.StagedTransactions, rb.AppliedTransactions, aErr, bErr)
			lastLog = time.Now()
		}
		if time.Now().After(end) {
			t.Fatal("large transaction never arrived")
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("arrived in %.1fs", time.Since(start).Seconds())
	// Payload integrity through the chunk path.
	raw, err := backendB.Get(LogKey(idOf(1), 1))
	if err != nil || raw == nil {
		t.Fatal("applied log record missing")
	}
	got, err := DecodeTransaction(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Payload) != string(tx.Payload) {
		t.Fatal("payload corrupted through chunk path")
	}
	if raw, _ := backendB.Get(ProgressKey(idOf(1))); raw == nil {
		t.Fatal("progress missing")
	}
}

// Cross-peer repair with no complete copy anywhere: two nodes hold
// disjoint staged chunks of one transaction and complete each other
// through ChunkNeed repair (no push, no full re-stream).
func TestDisjointPartialsRepair(t *testing.T) {
	tx := sizedTransaction(200_000)
	data := EncodeTransaction(&tx)
	count := uint32((len(data) + ChunkBytes - 1) / ChunkBytes)
	if count < 2 {
		t.Fatalf("need multiple chunks, got %d", count)
	}
	split := count / 2
	var low, high []uint32
	for i := uint32(0); i < split; i++ {
		low = append(low, i)
	}
	for i := split; i < count; i++ {
		high = append(high, i)
	}
	cfg := transferConfigs(t)
	backendD := transferBackend(idOf(1))
	backendE := transferBackend(idOf(2))
	if err := backendD.Commit(craftChunks(t, &tx, low)); err != nil {
		t.Fatal(err)
	}
	if err := backendE.Commit(craftChunks(t, &tx, high)); err != nil {
		t.Fatal(err)
	}
	d, err := Start(cfg[0], backendD)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	e, err := Start(cfg[1], backendE)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	addr := e.Status().Address
	if addr == nil {
		t.Fatal("no listen address")
	}
	if err := d.AddPeer(*addr); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	end := start.Add(60 * time.Second)
	for {
		dd, _ := backendD.Get(ReceiptKey(idOf(1), 1))
		ee, _ := backendE.Get(ReceiptKey(idOf(1), 1))
		if dd != nil && ee != nil {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("mutual repair did not converge (d=%v e=%v)", dd != nil, ee != nil)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("mutual repair converged in %.1fs", time.Since(start).Seconds())
	// Each side served exactly the indexes the other was missing: staged
	// relay, never a full transaction.
	if got := e.Status().Metrics.ChunkNeedServed; got != uint64(len(high)) {
		t.Fatalf("e served %d, want %d", got, len(high))
	}
	if got := d.Status().Metrics.ChunkNeedServed; got != uint64(len(low)) {
		t.Fatalf("d served %d, want %d", got, len(low))
	}
	if d.Status().Metrics.ChunkNeedSent == 0 || e.Status().Metrics.ChunkNeedSent == 0 {
		t.Fatal("no repair requests sent")
	}
}
