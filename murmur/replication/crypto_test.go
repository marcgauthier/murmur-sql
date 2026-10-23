package replication

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRFC8032(t *testing.T) {
	var seed [32]byte
	copy(seed[:], mustHex(t, "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"))
	var public [32]byte
	copy(public[:], mustHex(t, "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"))
	sig := mustHex(t, "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155"+
		"5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b")
	if PublicKey(&seed) != public {
		t.Fatal("public key mismatch")
	}
	if !bytes.Equal(Sign(&seed, []byte{}), sig) {
		t.Fatal("signature mismatch")
	}
	if err := Verify(&public, []byte{}, sig); err != nil {
		t.Fatal(err)
	}
	if err := Verify(&public, []byte("tampered"), sig); err == nil {
		t.Fatal("tampered message accepted")
	}
	if err := Verify(&public, []byte{}, sig[:63]); err == nil {
		t.Fatal("short signature accepted")
	}
	noncanonical := append([]byte(nil), sig...)
	noncanonical[63] |= 0x80
	if err := Verify(&public, []byte{}, noncanonical); err == nil {
		t.Fatal("noncanonical signature accepted")
	}
	var identity [32]byte
	identity[0] = 1
	forged := make([]byte, 64)
	forged[0] = 1
	if err := Verify(&identity, []byte("forged"), forged); err == nil {
		t.Fatal("low-order forgery accepted")
	}
	var zero [32]byte
	if err := Verify(&zero, []byte("forged"), forged); err == nil {
		t.Fatal("identity forgery accepted")
	}
}

func TestStrictPoints(t *testing.T) {
	vectors := []string{
		"0000000000000000000000000000000000000000000000000000000000000000",
		"0100000000000000000000000000000000000000000000000000000000000000",
		"26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc05",
		"c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a",
		"ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
		"edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
		"eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
	}
	var seed [32]byte
	for i := range seed {
		seed[i] = 7
	}
	pub := PublicKey(&seed)
	for _, hx := range vectors {
		point := mustHex(t, hx)
		for range 2 {
			if strictPoint(point) {
				t.Fatalf("low-order point accepted: %s", hx)
			}
			var p [32]byte
			copy(p[:], point)
			sig := make([]byte, 64)
			copy(sig, point)
			if err := Verify(&pub, []byte("message"), sig); err == nil {
				t.Fatalf("low-order R accepted: %s", hx)
			}
			if err := Verify(&p, []byte("message"), sig); err == nil {
				t.Fatalf("low-order key accepted: %s", hx)
			}
			point[31] ^= 0x80
		}
	}
}

func TestRFC7748(t *testing.T) {
	var alice, bob [32]byte
	copy(alice[:], mustHex(t, "77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a"))
	copy(bob[:], mustHex(t, "5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb"))
	var ap, bp [32]byte
	copy(ap[:], mustHex(t, "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a"))
	copy(bp[:], mustHex(t, "de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f"))
	var expected [32]byte
	copy(expected[:], mustHex(t, "4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742"))
	if RecipientPublic(&alice) != ap || RecipientPublic(&bob) != bp {
		t.Fatal("public key mismatch")
	}
	s1, err := Agree(&alice, &bp)
	if err != nil || s1 != expected {
		t.Fatalf("agree = %v, %v", s1, err)
	}
	s2, err := Agree(&bob, &ap)
	if err != nil || s2 != expected {
		t.Fatalf("agree = %v, %v", s2, err)
	}
	var zero [32]byte
	if _, err := Agree(&alice, &zero); err == nil {
		t.Fatal("zero peer accepted")
	}
	var low [32]byte
	low[0] = 1
	if _, err := Agree(&alice, &low); err == nil {
		t.Fatal("low-order peer accepted")
	}
}
