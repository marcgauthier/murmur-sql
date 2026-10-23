package spool

import (
	"bytes"
	"testing"
)

func FuzzBlockHeader(f *testing.F) {
	// Seed with valid block headers.
	nonce := make([]byte, blockNonceLen)
	hdr1, err := encodeBlockHeader(0, 0, false, 1, 100, 10, 1024, 512, nonce)
	if err == nil {
		f.Add(hdr1)
	}
	hdr2, err := encodeBlockHeader(0, 1, true, 2, 200, 0, 256, 128, nonce)
	if err == nil {
		f.Add(hdr2)
	}

	// Corrupted variations.
	if len(hdr1) > 0 {
		mutated := append([]byte(nil), hdr1...)
		mutated[0] = 0 // bad magic
		f.Add(mutated)

		short := hdr1[:20]
		f.Add(short)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		h, err := parseBlockHeader(data)
		if err != nil {
			return
		}
		if h == nil {
			t.Fatal("parseBlockHeader returned nil header without error")
		}
		// If valid, re-encoding must produce the same header bytes.
		reencoded, err := encodeBlockHeader(h.algo, h.compression, h.completion, h.keyID, h.blockSeq, h.recordCount, h.uncompressedLen, h.sealedLen, h.nonce)
		if err != nil {
			t.Fatalf("re-encoding valid parsed header failed: %v", err)
		}
		if !bytes.Equal(data[:blockHeaderLen], reencoded) {
			t.Fatalf("re-encoded header does not match parsed: got %x, want %x", reencoded, data[:blockHeaderLen])
		}
	})
}

func FuzzManifestParse(f *testing.F) {
	// Seed with valid manifests.
	m1 := &manifest{
		reqFeatures:  featureGroupsV2,
		nextFileID:   3,
		nextBlockID:  10,
		activeFileID: 1,
		generation:   1,
		nextGroupID:  5,
		members:      []uint64{1, 2},
	}
	f.Add(m1.encode())

	m2 := &manifest{
		reqFeatures:  featureGroupsV2 | featureCryptoV3,
		encrypted:    true,
		currentKeyID: 1,
		nextFileID:   5,
		nextBlockID:  20,
		activeFileID: 2,
		generation:   2,
		nextGroupID:  8,
		members:      []uint64{2, 3, 4},
		clean:        true,
		cleanFiles:   3,
		cleanBytes:   1024,
	}
	f.Add(m2.encode())

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := parseManifest(data)
		if err != nil {
			return
		}
		if m == nil {
			t.Fatal("parseManifest returned nil without error")
		}
		// The parsed manifest must satisfy our invariants.
		if err := assertManifestInvariants(m); err != nil {
			t.Fatalf("parsed manifest violated invariants: %v", err)
		}
	})
}

func FuzzCompletionDecode(f *testing.F) {
	c1 := completion{
		groupID:      1,
		blockCount:   2,
		totalRecords: 100,
		totalPlain:   2048,
		blockSeqs:    []uint64{10, 11},
		digest:       [32]byte{1, 2, 3, 4},
	}
	f.Add(encodeCompletion(c1))

	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := decodeCompletion(data)
		if err != nil {
			return
		}
		// Re-encode and decode must be idempotent.
		reencoded := encodeCompletion(c)
		c2, err := decodeCompletion(reencoded)
		if err != nil {
			t.Fatalf("re-encoded completion failed decode: %v", err)
		}
		if c2.groupID != c.groupID || c2.blockCount != c.blockCount || c2.totalRecords != c.totalRecords || c2.totalPlain != c.totalPlain {
			t.Fatalf("completion round-trip mismatch: got %+v, want %+v", c2, c)
		}
	})
}
