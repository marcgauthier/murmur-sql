package replication

import (
	"bytes"
	"net/netip"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
)

func idOf(b byte) NodeId {
	var id NodeId
	for i := range id {
		id[i] = b
	}
	return id
}

func fill32(b byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = b
	}
	return out
}

func seedOf(b byte) [32]byte { return fill32(b) }

func testTx(t *testing.T) Transaction {
	t.Helper()
	tx := Transaction{
		Database: idOf(7), Origin: idOf(1), Sequence: 3, ID: idOf(8),
		Millis: 10, Logical: 2, Schema: fill32(4),
		Payload: []byte{42, 43},
	}
	seed := seedOf(11)
	tx.Sign(&seed)
	return tx
}

func checkVector(t *testing.T, name, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s:\n got %s\nwant %s", name, got, want)
	}
}

func TestWireVectors(t *testing.T) {
	v4 := netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 8080)
	v6 := netip.AddrPortFrom(netip.AddrFrom16([16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}), 443)
	encAddr := func(a netip.AddrPort) string {
		return Hex(encode(func(e *postcard.Encoder) { encodeAddr(e, a) }))
	}
	checkVector(t, "sockv4", encAddr(v4), "007f000001903f")
	checkVector(t, "sockv6", encAddr(v6), "0100000000000000000000000000000001bb03")
	checkVector(t, "sock_some", Hex(encode(func(e *postcard.Encoder) {
		e.Some()
		encodeAddr(e, v4)
	})), "01007f000001903f")
	checkVector(t, "sock_none", Hex(encode(func(e *postcard.Encoder) { e.None() })), "00")

	tx := testTx(t)
	checkVector(t, "tx", Hex(EncodeTransaction(&tx)), "070707070707070707070707070707070101010101010101010101010101010103080808080808080808080808080808080a0204040404040404040404040404040404040404040404040404040404040404042341a9c4270f9019a09041df3884cae2426b73c9ee750d73ec0fbcb6c2250f45022a2b406506755f086d9f6c7f428bb820640b22c68a152c6339e9ad45ff5234c7cc93f468907a5889182d74b0856c7b00f4457412fb857e8e8c34f45f22ddb4224b4106")
	signedRef := "96016d75726d75722f73716c6974652d6f726967696e2d7472616e73616374696f6e2f7631070707070707070707070707070707070101010101010101010101010101010103080808080808080808080808080808080a0204040404040404040404040404040404040404040404040404040404040404042341a9c4270f9019a09041df3884cae2426b73c9ee750d73ec0fbcb6c2250f45"
	checkVector(t, "tx_signed", Hex(tx.signedBytes()), strings.TrimPrefix(signedRef, "9601"))

	ss := SignedSchema{Database: idOf(7), Signer: idOf(1), Sequence: 9, Schema: []byte{1, 2, 3}}
	seed := seedOf(11)
	ss.Sign(&seed)
	checkVector(t, "ss", Hex(EncodeSignedSchema(&ss)), "0707070707070707070707070707070701010101010101010101010101010101090301020340ad0f9b58b5a81dbf7902c0af76a1e555dd0fffd6a3dfa465125ba19e9c64819074063af1611e9c1e243763e4a037ce13c5aeb793a40e722f8822a33f1cf78f0b")

	ch := Chunk{Proof: tx, Origin: idOf(1), Sequence: 3, ID: idOf(8), Index: 1, Count: 2, Total: 100, Digest: fill32(6), Data: []byte{7, 8}}
	checkVector(t, "chunk", Hex(EncodeChunk(&ch)), "070707070707070707070707070707070101010101010101010101010101010103080808080808080808080808080808080a0204040404040404040404040404040404040404040404040404040404040404042341a9c4270f9019a09041df3884cae2426b73c9ee750d73ec0fbcb6c2250f45022a2b406506755f086d9f6c7f428bb820640b22c68a152c6339e9ad45ff5234c7cc93f468907a5889182d74b0856c7b00f4457412fb857e8e8c34f45f22ddb4224b41060101010101010101010101010101010103080808080808080808080808080808080102640606060606060606060606060606060606060606060606060606060606060606020708")
	cm := ChunkManifest{ID: idOf(8), Count: 2, Total: 100, Digest: fill32(6)}
	checkVector(t, "manifest", Hex(EncodeChunkManifest(&cm)), "0808080808080808080808080808080802640606060606060606060606060606060606060606060606060606060606060606")
	prog := Progress{Origin: idOf(1), Applied: 1, Observed: 2, RetainedThrough: 5}
	checkVector(t, "progress", Hex(EncodeProgress(&prog)), "0101010101010101010101010101010101020005")

	rp := PutRecord([]byte("ab"), []byte{1})
	checkVector(t, "record_put", Hex(EncodeRecord(&rp)), "026162010101")
	rd := DeleteRecord([]byte("ab"))
	checkVector(t, "record_del", Hex(EncodeRecord(&rd)), "02616200")
	ct := Content{SQLite: []byte{9}, Records: []Record{DeleteRecord([]byte("k"))}, Origin: idOf(1)}
	checkVector(t, "content", Hex(EncodeContent(&ct)), "01010901016b0001010101010101010101010101010101")

	checkVector(t, "policy", Hex(encode(func(e *postcard.Encoder) { encodePolicy(e, PolicySet) })), "02")
	tag := Tag{Database: idOf(7), Node: idOf(1), Transaction: idOf(2), Operation: 5}
	checkVector(t, "tag", Hex(EncodeTag(tag)), "07070707070707070707070707070707010101010101010101010101010101010202020202020202020202020202020205")
	var counter Counter
	if err := counter.Add(idOf(7), idOf(1), 5); err != nil {
		t.Fatal(err)
	}
	if err := counter.Add(idOf(7), idOf(1), -2); err != nil {
		t.Fatal(err)
	}
	checkVector(t, "counter", Hex(EncodeCounter(&counter)), "0107070707070707070707070707070707010101010101010101010101010101010501070707070707070707070707070707070101010101010101010101010101010102")
	var set Set
	set.Add([]byte{1, 2}, Tag{Database: idOf(7), Node: idOf(1), Transaction: idOf(2)})
	checkVector(t, "set", Hex(EncodeSet(&set)), "01020102010707070707070707070707070707070701010101010101010101010101010101020202020202020202020202020202020000")
	checkVector(t, "causal_c", Hex(EncodeCausal(&Causal{Counter: &counter})), "000107070707070707070707070707070707010101010101010101010101010101010501070707070707070707070707070707070101010101010101010101010101010102")
	checkVector(t, "causal_s", Hex(EncodeCausal(&Causal{Set: &set})), "0101020102010707070707070707070707070707070701010101010101010101010101010101020202020202020202020202020202020000")

	member := Member{Node: idOf(1), Address: v4, Incarnation: 4, State: MemberSuspect}
	checkVector(t, "member", Hex(EncodeMember(&member)), "01010101010101010101010101010101007f000001903f0401")
	checkVector(t, "state", Hex(encode(func(e *postcard.Encoder) { encodeMemberState(e, MemberLeft) })), "03")
	pkt := Packet{Kind: PacketPing, Database: idOf(7), Sender: idOf(1), Nonce: 3}
	checkVector(t, "packet", Hex(EncodePacket(&pkt)), "0207070707070707070707070707070707010101010101010101010101010101010300")
	checkVector(t, "dis_g", Hex(encode(func(e *postcard.Encoder) { e.Discriminant(0) })), "00")
	checkVector(t, "dis_p", Hex(encode(func(e *postcard.Encoder) { e.Discriminant(1) })), "01")

	msg := func(m Message) string { return Hex(EncodeMessage(&m)) }
	hello := Message{Kind: MsgHello, Capabilities: 1, Required: 2, Version: 13, Database: idOf(7), Node: idOf(1), Fetch: &v4, Schema: []byte{5}, Mode: DisseminationGossip, MaxTransaction: 64}
	checkVector(t, "hello", msg(hello), "0001020d070707070707070707070707070707070101010101010101010101010101010101007f000001903f01050040")
	checkVector(t, "batches", msg(Message{Kind: MsgBatches, Transactions: []Transaction{tx}}), "0201070707070707070707070707070707070101010101010101010101010101010103080808080808080808080808080808080a0204040404040404040404040404040404040404040404040404040404040404042341a9c4270f9019a09041df3884cae2426b73c9ee750d73ec0fbcb6c2250f45022a2b406506755f086d9f6c7f428bb820640b22c68a152c6339e9ad45ff5234c7cc93f468907a5889182d74b0856c7b00f4457412fb857e8e8c34f45f22ddb4224b4106")
	checkVector(t, "ping", msg(Message{Kind: MsgPing, Nonce: 7}), "0e07")
	checkVector(t, "snapreq", msg(Message{Kind: MsgSnapshotRequest}), "05")
	checkVector(t, "err", msg(Message{Kind: MsgError, Code: 3, ErrMessage: "boom"}), "100304626f6f6d")
	checkVector(t, "schemaack", msg(Message{Kind: MsgSchemaAck, Digest: fill32(6), Sequence: 11}), "1306060606060606060606060606060606060606060606060606060606060606060b")
	checkVector(t, "ihave", msg(Message{Kind: MsgIHave, Origin: idOf(1), Sequence: 4}), "150101010101010101010101010101010104")
	progPage := Message{Kind: MsgProgressPage, Offset: 2, More: true,
		Entries: []Progress{{Origin: idOf(1), Applied: 1, Observed: 1, RetainedThrough: 1}}}
	checkVector(t, "progpage", msg(progPage), "190201010101010101010101010101010101010101010001")
	checkVector(t, "memberspage", msg(Message{Kind: MsgMembersPage}), "1c000000")
	checkVector(t, "chunkneed", msg(Message{Kind: MsgChunkNeed, Origin: idOf(1), Sequence: 4, Indexes: []uint32{1, 2}}), "1d0101010101010101010101010101010104020102")
	checkVector(t, "chunkavail", msg(Message{Kind: MsgChunkAvailabilityPage,
		Availability: []ChunkAvailability{{Origin: idOf(1), Sequence: 4, Indexes: []uint32{1}}}, More: true}), "1f010101010101010101010101010101010104010101")
	checkVector(t, "snapneed", msg(Message{Kind: MsgSnapshotNeed, Transfer: idOf(9), Indexes: []uint32{3}}), "0a090909090909090909090909090909090103")

	prep, err := NewPrepared(&Message{Kind: MsgPing, Nonce: 7})
	if err != nil {
		t.Fatal(err)
	}
	checkVector(t, "frame", Hex(prep.Bytes), "5244000d000a0000000000020e07")
	if prep.Charge != 2+12 {
		t.Fatalf("charge = %d", prep.Charge)
	}
	bigTx := Transaction{Database: idOf(7), Origin: idOf(1), Sequence: 1, ID: idOf(8),
		Millis: 1, Payload: bytes.Repeat([]byte{7}, 3000)}
	bigPrep, err := NewPrepared(&Message{Kind: MsgBatches, Transactions: []Transaction{bigTx}})
	if err != nil {
		t.Fatal(err)
	}
	checkVector(t, "bigframe_flags", Hex(bigPrep.Bytes[6:8]), "8002")
	if bigPrep.Charge != 3132 {
		t.Fatalf("big charge = %d", bigPrep.Charge)
	}
	back, err := ReadMessage(bytes.NewReader(bigPrep.Bytes))
	if err != nil {
		t.Fatal(err)
	}
	if back.Kind != MsgBatches || len(back.Transactions) != 1 || len(back.Transactions[0].Payload) != 3000 {
		t.Fatalf("big roundtrip = %+v", back.Kind)
	}
	// Decode roundtrips.
	for _, m := range []Message{hello, {Kind: MsgPing, Nonce: 7}, progPage} {
		raw := EncodeMessage(&m)
		dec, err := DecodeMessage(raw)
		if err != nil {
			t.Fatal(err)
		}
		if Hex(EncodeMessage(&dec)) != Hex(raw) {
			t.Fatal("message roundtrip mismatch")
		}
	}
}

func TestMemberCodec(t *testing.T) {
	m := Member{
		Node:        idOf(5),
		Address:     netip.MustParseAddrPort("127.0.0.1:1234"),
		Incarnation: 42,
		State:       MemberAlive,
	}
	back, err := DecodeMember(EncodeMember(&m))
	if err != nil {
		t.Fatal(err)
	}
	if back != m {
		t.Fatalf("roundtrip = %+v", back)
	}
	if _, err := DecodeMember(nil); err == nil {
		t.Fatal("empty member accepted")
	}
	enc := EncodeMember(&m)
	if _, err := DecodeMember(enc[:len(enc)-1]); err == nil {
		t.Fatal("truncated member accepted")
	}
}
