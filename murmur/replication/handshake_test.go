package replication

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestFrameVersionRefused pins the frame-header gate: a valid frame
// restamped with a foreign protocol version (as the version-skew
// fixture speaks) is rejected before any payload decode.
func TestFrameVersionRefused(t *testing.T) {
	prep, err := NewPrepared(&Message{Kind: MsgPing, Nonce: 7})
	if err != nil {
		t.Fatal(err)
	}
	good := append([]byte(nil), prep.Bytes...)
	if _, err := ReadMessage(bytes.NewReader(good)); err != nil {
		t.Fatalf("valid frame rejected: %v", err)
	}
	bad := append([]byte(nil), prep.Bytes...)
	binary.BigEndian.PutUint16(bad[2:4], 99)
	if _, err := ReadMessage(bytes.NewReader(bad)); err == nil {
		t.Fatal("version-99 frame accepted")
	}
	bad = append([]byte(nil), prep.Bytes...)
	binary.BigEndian.PutUint16(bad[2:4], Version+1)
	if _, err := ReadMessage(bytes.NewReader(bad)); err == nil {
		t.Fatal("version+1 frame accepted")
	}
	// Adjacent header gates stay strict: magic, flags, length.
	bad = append([]byte(nil), prep.Bytes...)
	copy(bad[:2], "XX")
	if _, err := ReadMessage(bytes.NewReader(bad)); err == nil {
		t.Fatal("bad-magic frame accepted")
	}
	bad = append([]byte(nil), prep.Bytes...)
	binary.BigEndian.PutUint16(bad[6:8], 0x0001)
	if _, err := ReadMessage(bytes.NewReader(bad)); err == nil {
		t.Fatal("bad-flags frame accepted")
	}
	bad = append([]byte(nil), prep.Bytes...)
	binary.BigEndian.PutUint32(bad[8:12], uint32(FrameBytes+1))
	if _, err := ReadMessage(bytes.NewReader(bad)); err == nil {
		t.Fatal("oversize frame accepted")
	}
}

// TestNeedServeCountsAndResponds pins need-driven delivery: a MsgNeed
// for retained transactions responds with the bytes and counts every
// served transaction in NeedServed (the push counter stays untouched:
// need responses are pulls, not pushes).
func TestNeedServeCountsAndResponds(t *testing.T) {
	backend := newStubBackend(stubOther, nil)
	tx3, data3 := signedChunkedTx(3, 1000)
	tx4, data4 := signedChunkedTx(4, 1000)
	if err := backend.Commit([]Record{
		PutRecord(LogKey(stubMaster, 3), data3),
		PutRecord(LogKey(stubMaster, 4), data4),
	}); err != nil {
		t.Fatal(err)
	}
	core := chunkRepairCore(backend)
	send, recv := newQueue(newByteLease(64<<20), 4<<20)
	drain := func() []Message {
		var out []Message
		for {
			q, ok := recv.TryRecv()
			if !ok {
				return out
			}
			out = append(out, q.Message)
		}
	}
	needServed := func() uint64 {
		core.control.statusMu.Lock()
		defer core.control.statusMu.Unlock()
		return core.control.status.Metrics.NeedServed
	}
	pushed := func() uint64 {
		core.control.statusMu.Lock()
		defer core.control.statusMu.Unlock()
		return core.control.status.Metrics.PushedTransactions
	}
	// Malformed ranges are rejected without serving or counting.
	for _, bad := range []Message{
		{Kind: MsgNeed, Origin: stubMaster, From: 0, Through: 3},
		{Kind: MsgNeed, Origin: stubMaster, From: 4, Through: 3},
	} {
		if err := core.message(stubOther, bad, send); err == nil {
			t.Fatalf("malformed need %+v accepted", bad)
		}
	}
	if got := drain(); len(got) != 0 {
		t.Fatalf("malformed needs produced %d responses", len(got))
	}
	if needServed() != 0 {
		t.Fatal("malformed needs counted")
	}
	// Single-transaction need: one batch, count 1, push untouched.
	if err := core.message(stubOther, Message{Kind: MsgNeed, Origin: stubMaster, From: 3, Through: 3}, send); err != nil {
		t.Fatal(err)
	}
	got := drain()
	if len(got) != 1 || got[0].Kind != MsgBatches || len(got[0].Transactions) != 1 ||
		got[0].Transactions[0].Sequence != 3 || got[0].Transactions[0].ID != tx3.ID {
		t.Fatalf("single-need responses = %+v", got)
	}
	if needServed() != 1 {
		t.Fatalf("NeedServed = %d, want 1", needServed())
	}
	if pushed() != 0 {
		t.Fatal("need response counted as push")
	}
	// Range need: one batch per transaction, count accumulates.
	if err := core.message(stubOther, Message{Kind: MsgNeed, Origin: stubMaster, From: 3, Through: 4}, send); err != nil {
		t.Fatal(err)
	}
	got = drain()
	if len(got) != 2 {
		t.Fatalf("range-need responses = %d, want 2", len(got))
	}
	var seqs []uint64
	for _, m := range got {
		if m.Kind != MsgBatches || len(m.Transactions) != 1 {
			t.Fatalf("range-need response = %+v", m)
		}
		seqs = append(seqs, m.Transactions[0].Sequence)
	}
	if len(seqs) != 2 || seqs[0] != 3 || seqs[1] != 4 {
		t.Fatalf("range-need seqs = %v", seqs)
	}
	if got, want := needServed(), uint64(3); got != want {
		t.Fatalf("NeedServed = %d, want %d", got, want)
	}
	_ = tx4
}

// TestCompatibleCaps pins capability negotiation: the peer must not
// require anything unknown locally, and must offer everything required
// locally. Extra capabilities on either side are fine.
func TestCompatibleCaps(t *testing.T) {
	if !compatibleCaps(Capabilities, Required) {
		t.Fatal("self negotiation rejected")
	}
	// Peer requires a bit nobody (locally) has: reject.
	unknown := ^(Capabilities | Required)
	if unknown == 0 {
		t.Skip("capability space exhausted")
	}
	unknownBit := unknown &^ (unknown - 1)
	if compatibleCaps(Capabilities, Required|unknownBit) {
		t.Fatal("unknown required bit accepted")
	}
	// Peer lacks a locally required bit: reject.
	if Required == 0 {
		t.Skip("no required capabilities to withhold")
	}
	missing := Capabilities &^ (Required &^ (Required - 1))
	if compatibleCaps(missing, Required) {
		t.Fatal("missing required bit accepted")
	}
	// Extra offered capabilities beyond the local set: accept.
	if !compatibleCaps(Capabilities|unknownBit, Required) {
		t.Fatal("extra offered capabilities rejected")
	}
}
