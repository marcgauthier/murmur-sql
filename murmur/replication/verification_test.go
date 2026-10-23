package replication

import "testing"

func verifyTx(sequence uint64) Transaction {
	tx := Transaction{
		Database: idOf(7), Origin: idOf(1), Sequence: sequence, ID: idOf(8),
		Millis: 10, Payload: bytesRepeat(42, 256),
	}
	seed := seedOf(11)
	tx.Sign(&seed)
	return tx
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func verifyTrust() map[NodeId][32]byte {
	seed := seedOf(11)
	return map[NodeId][32]byte{idOf(1): PublicKey(&seed)}
}

func TestVerifyCache(t *testing.T) {
	var cache Cache
	tx := verifyTx(1)
	trusted := verifyTrust()
	first, err := cache.Verify(&tx, idOf(7), trusted, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != tx.Digest() {
		t.Fatal("digest mismatch")
	}
	second, err := cache.Verify(&tx, idOf(7), trusted, 1024)
	if err != nil || string(second.Bytes) != string(first.Bytes) {
		t.Fatal("cache hit mismatch")
	}
	altered := tx
	altered.Payload = append([]byte(nil), tx.Payload...)
	altered.Payload[0] ^= 1
	if _, err := cache.Verify(&altered, idOf(7), trusted, 1024); err == nil {
		t.Fatal("tampered payload accepted")
	}
	altered = tx
	altered.Signature = append([]byte(nil), tx.Signature...)
	altered.Signature[0] ^= 1
	if _, err := cache.Verify(&altered, idOf(7), trusted, 1024); err == nil {
		t.Fatal("tampered signature accepted")
	}
	if _, err := cache.Verify(&tx, idOf(8), trusted, 1024); err == nil {
		t.Fatal("wrong database accepted")
	}
	if _, err := cache.Verify(&tx, idOf(7), trusted, 255); err == nil {
		t.Fatal("tight limit accepted")
	}
	if _, err := cache.Verify(&tx, idOf(7), map[NodeId][32]byte{}, 1024); err == nil {
		t.Fatal("revoked trust accepted")
	}
	changedSeed := seedOf(22)
	changed := map[NodeId][32]byte{idOf(1): PublicKey(&changedSeed)}
	if _, err := cache.Verify(&tx, idOf(7), changed, 1024); err == nil {
		t.Fatal("changed key accepted")
	}
	if len(cache.entries) != 1 {
		t.Fatalf("entries = %d", len(cache.entries))
	}
}

func TestVerifyEviction(t *testing.T) {
	var cache Cache
	trusted := verifyTrust()
	for sequence := uint64(1); sequence <= 8192+1; sequence++ {
		tx := verifyTx(sequence)
		if _, err := cache.Verify(&tx, idOf(7), trusted, 1024); err != nil {
			t.Fatal(err)
		}
	}
	if len(cache.entries) != 8192 || len(cache.order) != 8192 {
		t.Fatalf("entries = %d order = %d", len(cache.entries), len(cache.order))
	}
	expired := verifyTx(1)
	expired.Signature = append([]byte(nil), expired.Signature...)
	expired.Signature[0] ^= 1
	if _, err := cache.Verify(&expired, idOf(7), trusted, 1024); err == nil {
		t.Fatal("expired forgery accepted")
	}
	var restarted Cache
	if _, err := restarted.Verify(&expired, idOf(7), trusted, 1024); err == nil {
		t.Fatal("restarted forgery accepted")
	}
	fresh := verifyTx(1)
	if _, err := restarted.Verify(&fresh, idOf(7), trusted, 1024); err != nil {
		t.Fatal(err)
	}
}

func TestBind(t *testing.T) {
	tx := verifyTx(1)
	v, err := Bind(&tx, idOf(7), 1024)
	if err != nil || v.Digest != tx.Digest() {
		t.Fatal("bind failed")
	}
	bad := tx
	bad.PayloadDigest[0] ^= 1
	if _, err := Bind(&bad, idOf(7), 1024); err == nil {
		t.Fatal("bad digest accepted")
	}
}
