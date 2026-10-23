package spool

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func crc32Of(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// TestBodyCodecRoundTrip exercises group framing, flags, lengths,
// and multi-record framing through the raw body codec.
func TestBodyCodecRoundTrip(t *testing.T) {
	recs := []pendingRecord{
		{seq: 1, key: []byte("a"), value: []byte("1")},
		{seq: 2, key: []byte(""), value: []byte("empty-key-ok")},
		{seq: 3, key: []byte("empty-val"), value: []byte{}},
		{seq: 4, key: []byte("tomb"), tomb: true},
		{seq: ^uint64(0), key: bytes.Repeat([]byte("k"), 300), value: bytes.Repeat([]byte("v"), 5000)},
	}
	raw := encodeBody(7, 2, 5, recs)
	gid, bidx, bcnt, back, err := decodeBody(raw, uint32(len(recs)))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gid != 7 || bidx != 2 || bcnt != 5 {
		t.Fatalf("framing = %d/%d/%d, want 7/2/5", gid, bidx, bcnt)
	}
	if len(back) != len(recs) {
		t.Fatalf("got %d records, want %d", len(back), len(recs))
	}
	for i := range recs {
		if back[i].Sequence != recs[i].seq || !bytes.Equal(back[i].Key, recs[i].key) ||
			!bytes.Equal(back[i].Value, recs[i].value) || back[i].Deleted != recs[i].tomb {
			t.Fatalf("record %d mismatch: %+v vs %+v", i, back[i], recs[i])
		}
	}
}

// TestBodyCodecCorrupt feeds truncations at every offset and requires
// clean rejections, never panics.
func TestBodyCodecCorrupt(t *testing.T) {
	raw := encodeBody(1, 0, 2, []pendingRecord{
		{seq: 1, key: []byte("key"), value: []byte("value")},
		{seq: 2, key: []byte("t"), tomb: true},
	})
	for cut := 0; cut < len(raw); cut++ {
		if _, _, _, _, err := decodeBody(raw[:cut], 2); err == nil {
			t.Fatalf("truncation at %d decoded without error", cut)
		}
	}
	// Wrong count.
	if _, _, _, _, err := decodeBody(raw, 1); err == nil {
		t.Fatalf("wrong wantCount decoded without error")
	}
	// Trailing garbage.
	if _, _, _, _, err := decodeBody(append(append([]byte(nil), raw...), 0), 2); err == nil {
		t.Fatalf("trailing bytes decoded without error")
	}
	// Unknown flags.
	mut := append([]byte(nil), raw...)
	mut[groupFramingLen+4+8] = 0x80
	if _, _, _, _, err := decodeBody(mut, 2); err == nil {
		t.Fatalf("unknown flags decoded without error")
	}
	// Tombstone carrying a value.
	tomb := encodeBody(1, 0, 1, []pendingRecord{{seq: 1, key: []byte("k"), value: []byte("v"), tomb: true}})
	if _, _, _, _, err := decodeBody(tomb, 1); err == nil {
		t.Fatalf("valued tombstone decoded without error")
	}
	// Bad group framing: index past count.
	bad := encodeBody(1, 3, 2, []pendingRecord{{seq: 1, key: []byte("k"), value: []byte("v")}})
	if _, _, _, _, err := decodeBody(bad, 1); err == nil {
		t.Fatalf("bad group framing decoded without error")
	}
}

// TestCompletionCodecRoundTrip pins the completion body and its
// digest over sealed payloads.
func TestCompletionCodecRoundTrip(t *testing.T) {
	sealed := [][]byte{[]byte("sealed-one"), []byte("sealed-two")}
	seqs := []uint64{11, 12}
	d := groupDigest(9, 2, seqs, sealed)
	c := completion{groupID: 9, blockCount: 2, totalRecords: 30, totalPlain: 4000, blockSeqs: seqs, digest: d}
	back, err := decodeCompletion(encodeCompletion(c))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back.groupID != 9 || back.blockCount != 2 || back.totalRecords != 30 || back.totalPlain != 4000 {
		t.Fatalf("completion mismatch: %+v", back)
	}
	if back.digest != d || len(back.blockSeqs) != 2 || back.blockSeqs[1] != 12 {
		t.Fatalf("completion digest/seqs mismatch")
	}
	// Digest binds order: swapped inputs differ.
	swapped := groupDigest(9, 2, []uint64{12, 11}, [][]byte{sealed[1], sealed[0]})
	if swapped == d {
		t.Fatalf("digest ignores order")
	}
	// Truncations and size lies rejected.
	raw := encodeCompletion(c)
	for _, cut := range []int{0, 10, len(raw) - 33, len(raw) - 1} {
		if _, err := decodeCompletion(raw[:cut]); err == nil {
			t.Fatalf("truncation at %d decoded without error", cut)
		}
	}
	mut := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint32(mut[8:12], 3) // blockCount lie
	if _, err := decodeCompletion(mut); err == nil {
		t.Fatalf("block-count lie decoded without error")
	}
}

// TestHeaderValidation exercises structural acceptance and every
// fail-closed bound before any allocation happens.
func TestHeaderValidation(t *testing.T) {
	mk := func(completion bool, mutate func([]byte)) []byte {
		var count uint32 = 2
		if completion {
			count = 0
		}
		h, err := encodeBlockHeader(0, 0, completion, 0, 7, count, 100, 100, nil)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if mutate != nil {
			mutate(h)
		}
		return h
	}
	good := mk(false, nil)
	hdr, err := parseBlockHeader(good)
	if err != nil {
		t.Fatalf("valid header rejected: %v", err)
	}
	if hdr.blockSeq != 7 || hdr.recordCount != 2 || hdr.completion {
		t.Fatalf("header fields wrong: %+v", hdr)
	}
	chdr, err := parseBlockHeader(mk(true, nil))
	if err != nil {
		t.Fatalf("valid completion rejected: %v", err)
	}
	if !chdr.completion {
		t.Fatalf("completion flag lost")
	}
	cases := map[string][]byte{
		"truncated": good[:blockHeaderLen-1],
		"bad magic": mk(false, func(h []byte) { h[0] ^= 0xFF }),
		"bad crc":   mk(false, func(h []byte) { h[58] ^= 0xFF }),
		"bad nonce": mk(false, func(h []byte) {
			h[34] = 5
			binary.LittleEndian.PutUint32(h[59:63], crc32Of(h[:59]))
		}),
		"bad flags": mk(false, func(h []byte) {
			binary.LittleEndian.PutUint16(h[8:10], 0x2)
			binary.LittleEndian.PutUint32(h[59:63], crc32Of(h[:59]))
		}),
		"completion with count": mk(true, func(h []byte) {
			binary.LittleEndian.PutUint32(h[22:26], 1)
			binary.LittleEndian.PutUint32(h[59:63], crc32Of(h[:59]))
		}),
		"zero count": mk(false, func(h []byte) {
			binary.LittleEndian.PutUint32(h[22:26], 0)
			binary.LittleEndian.PutUint32(h[59:63], crc32Of(h[:59]))
		}),
		"huge count": mk(false, func(h []byte) {
			binary.LittleEndian.PutUint32(h[22:26], absMaxRecordsPerBlock+1)
			binary.LittleEndian.PutUint32(h[59:63], crc32Of(h[:59]))
		}),
		"zero sealed": mk(false, func(h []byte) {
			binary.LittleEndian.PutUint32(h[30:34], 0)
			binary.LittleEndian.PutUint32(h[59:63], crc32Of(h[:59]))
		}),
		"huge sealed": mk(false, func(h []byte) {
			binary.LittleEndian.PutUint32(h[30:34], absMaxBlockBytes+1)
			binary.LittleEndian.PutUint32(h[59:63], crc32Of(h[:59]))
		}),
	}
	for name, raw := range cases {
		if _, err := parseBlockHeader(raw); err == nil {
			t.Fatalf("%s: accepted, want rejection", name)
		} else if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s: err %v does not wrap ErrCorrupt", name, err)
		}
	}
	// Newer version is a distinct error for upgrade detection.
	newer := mk(false, func(h []byte) {
		binary.LittleEndian.PutUint16(h[4:6], blockVersion+1)
		binary.LittleEndian.PutUint32(h[59:63], crc32Of(h[:59]))
	})
	if _, err := parseBlockHeader(newer); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("newer version err = %v, want ErrUnsupportedVersion", err)
	}
	// Version 1 blocks fail closed inside v2 stores.
	v1 := mk(false, func(h []byte) {
		binary.LittleEndian.PutUint16(h[4:6], 1)
		binary.LittleEndian.PutUint32(h[59:63], crc32Of(h[:59]))
	})
	if _, err := parseBlockHeader(v1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("v1 block err = %v, want ErrCorrupt", err)
	}
}
