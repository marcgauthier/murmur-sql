package spool

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// Block wire format v3 (all integers little-endian). Every commit is
// a group: bounded data blocks followed by one completion block. A
// group is accepted only with its completion validated; otherwise
// the whole group is discarded.
//
// Header (63 bytes):
//
//	off  size  field
//	0    4     magic "SPOL"
//	4    2     format version (3)
//	6    1     encryption algorithm id
//	7    1     compression id
//	8    2     flags (bit0 = completion block)
//	10   4     data key id (0 when unencrypted)
//	14   8     block sequence (store-wide, informational)
//	22   4     record count (data blocks; 0 for completion)
//	26   4     uncompressed body size
//	30   4     sealed payload size (ciphertext+tag, or body+crc)
//	34   1     nonce length (0 or 12)
//	35   24    nonce (prefix of length nonceLen)
//	59   4     CRC32-Castagnoli over bytes [0,59)
//
// Body: sealed payload of sealedLen bytes.
//
// Plaintext data body:
//
//	group id u64
//	block index u32 (0-based within the group)
//	block count u32 (data blocks in the group)
//	record count u32 (must match the header)
//	repeated record:
//	  sequence u64
//	  flags u8 (bit0 = tombstone)
//	  key length u32
//	  value length u32
//	  key bytes
//	  value bytes (absent for tombstones)
//
// Plaintext completion body:
//
//	group id u64
//	block count u32
//	total records u32
//	total plaintext data bytes u64
//	block sequences u64[blockCount] (data blocks in index order)
//	content digest [32] (SHA-256 over group framing + sealed payloads)
//
// The digest binds groupID || blockCount || for each data block in
// index order: blockSeq || sealedLen || sealed payload. It validates
// over sealed bytes, before decryption. AEAD still authenticates
// every payload; CRC32C only gates framing.
//
// In EncryptionNone mode the "sealed" payload is the compressed body
// followed by its CRC32-Castagnoli (4 bytes); sealedLen covers both.

const (
	blockMagic = "SPOL"
	// blockVersion 3 binds the AEAD associated data to the store
	// id and database context; version 2 blocks fail closed.
	blockVersion = 3

	blockHeaderLen = 63
	blockMagicLen  = 4
	blockNonceLen  = 24
	blockCRCSize   = 4

	flagFieldTombstone = 1 << 0
	flagBlockComplete  = 1 << 0

	// Absolute decode caps. These bound allocations on the read path
	// regardless of the writer's options; writer options must fit
	// inside them.
	absMaxRecordsPerBlock = 1 << 20
	absMaxBlockBytes      = 1 << 30
	absMaxKeySize         = 64 << 20
	absMaxValueSize       = 512 << 20
	absMaxGroupBlocks     = 1 << 20
	absMaxGroupRecords    = 1 << 24
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// blockHeader is the parsed block header.
type blockHeader struct {
	algo            uint8
	compression     uint8
	completion      bool
	keyID           uint32
	blockSeq        uint64
	recordCount     uint32
	uncompressedLen uint32
	sealedLen       uint32
	nonce           []byte
	raw             []byte // the 63 header bytes (AEAD associated data)
}

// encodeBlockHeader serializes the header (without CRC) and appends
// the CRC. The returned slice is freshly allocated.
func encodeBlockHeader(algo, compression uint8, completion bool, keyID uint32, blockSeq uint64, recordCount, uncompressedLen, sealedLen uint32, nonce []byte) ([]byte, error) {
	if len(nonce) > blockNonceLen {
		return nil, fmt.Errorf("spool: nonce of %d bytes exceeds %d", len(nonce), blockNonceLen)
	}
	out := make([]byte, blockHeaderLen)
	copy(out[0:4], blockMagic)
	binary.LittleEndian.PutUint16(out[4:6], blockVersion)
	out[6] = algo
	out[7] = compression
	if completion {
		binary.LittleEndian.PutUint16(out[8:10], flagBlockComplete)
	}
	binary.LittleEndian.PutUint32(out[10:14], keyID)
	binary.LittleEndian.PutUint64(out[14:22], blockSeq)
	binary.LittleEndian.PutUint32(out[22:26], recordCount)
	binary.LittleEndian.PutUint32(out[26:30], uncompressedLen)
	binary.LittleEndian.PutUint32(out[30:34], sealedLen)
	out[34] = uint8(len(nonce))
	copy(out[35:35+len(nonce)], nonce)
	binary.LittleEndian.PutUint32(out[59:63], crc32.Checksum(out[:59], castagnoli))
	return out, nil
}

// parseBlockHeader validates and parses one header. It performs only
// structural checks (magic, version, CRC, field bounds); semantic
// checks (algorithm ids, key lookup) belong to the caller.
func parseBlockHeader(raw []byte) (*blockHeader, error) {
	if len(raw) < blockHeaderLen {
		return nil, fmt.Errorf("spool: truncated block header: %w", ErrCorrupt)
	}
	raw = raw[:blockHeaderLen]
	if string(raw[0:4]) != blockMagic {
		return nil, fmt.Errorf("spool: bad block magic: %w", ErrCorrupt)
	}
	if v := binary.LittleEndian.Uint16(raw[4:6]); v != blockVersion {
		if v > blockVersion {
			return nil, fmt.Errorf("spool: block version %d: %w", v, ErrUnsupportedVersion)
		}
		return nil, fmt.Errorf("spool: block version %d: %w", v, ErrCorrupt)
	}
	want := binary.LittleEndian.Uint32(raw[59:63])
	if got := crc32.Checksum(raw[:59], castagnoli); got != want {
		return nil, fmt.Errorf("spool: block header checksum mismatch: %w", ErrCorrupt)
	}
	nonceLen := int(raw[34])
	if nonceLen != 0 && nonceLen != 12 {
		return nil, fmt.Errorf("spool: bad nonce length %d: %w", nonceLen, ErrCorrupt)
	}
	flags := binary.LittleEndian.Uint16(raw[8:10])
	if flags & ^uint16(flagBlockComplete) != 0 {
		return nil, fmt.Errorf("spool: unknown block flags %#x: %w", flags, ErrCorrupt)
	}
	h := &blockHeader{
		algo:            raw[6],
		compression:     raw[7],
		completion:      flags&flagBlockComplete != 0,
		keyID:           binary.LittleEndian.Uint32(raw[10:14]),
		blockSeq:        binary.LittleEndian.Uint64(raw[14:22]),
		recordCount:     binary.LittleEndian.Uint32(raw[22:26]),
		uncompressedLen: binary.LittleEndian.Uint32(raw[26:30]),
		sealedLen:       binary.LittleEndian.Uint32(raw[30:34]),
		nonce:           append([]byte(nil), raw[35:35+nonceLen]...),
		raw:             append([]byte(nil), raw...),
	}
	if h.completion {
		if h.recordCount != 0 {
			return nil, fmt.Errorf("spool: completion with record count %d: %w", h.recordCount, ErrCorrupt)
		}
	} else if h.recordCount == 0 || h.recordCount > absMaxRecordsPerBlock {
		return nil, fmt.Errorf("spool: bad record count %d: %w", h.recordCount, ErrCorrupt)
	}
	if h.uncompressedLen == 0 || h.uncompressedLen > absMaxBlockBytes {
		return nil, fmt.Errorf("spool: bad uncompressed size %d: %w", h.uncompressedLen, ErrCorrupt)
	}
	if h.sealedLen == 0 || h.sealedLen > absMaxBlockBytes {
		return nil, fmt.Errorf("spool: bad sealed size %d: %w", h.sealedLen, ErrCorrupt)
	}
	return h, nil
}

// groupFramingLen is the plaintext prefix binding a data body to
// its group: groupID u64 + blockIndex u32 + blockCount u32.
const groupFramingLen = 8 + 4 + 4

// encodeBody serializes records into the plaintext data body layout
// for one block of a group.
func encodeBody(groupID uint64, blockIndex, blockCount uint32, records []pendingRecord) []byte {
	size := groupFramingLen + 4
	for i := range records {
		size += 8 + 1 + 4 + 4 + len(records[i].key) + len(records[i].value)
	}
	out := make([]byte, 0, size)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], groupID)
	out = append(out, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], blockIndex)
	out = append(out, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], blockCount)
	out = append(out, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(records)))
	out = append(out, tmp[:4]...)
	for i := range records {
		r := &records[i]
		binary.LittleEndian.PutUint64(tmp[:], r.seq)
		out = append(out, tmp[:]...)
		if r.tomb {
			out = append(out, flagFieldTombstone)
		} else {
			out = append(out, 0)
		}
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(r.key)))
		out = append(out, tmp[:4]...)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(r.value)))
		out = append(out, tmp[:4]...)
		out = append(out, r.key...)
		out = append(out, r.value...)
	}
	return out
}

// decodeBody parses a plaintext data body into records sharing the
// body's backing array. wantCount must match the header's record
// count. It returns the body's group framing for group assembly.
func decodeBody(body []byte, wantCount uint32) (groupID uint64, blockIndex, blockCount uint32, _ []Record, err error) {
	if len(body) < groupFramingLen+4 {
		return 0, 0, 0, nil, fmt.Errorf("spool: truncated block body: %w", ErrCorrupt)
	}
	groupID = binary.LittleEndian.Uint64(body[0:8])
	blockIndex = binary.LittleEndian.Uint32(body[8:12])
	blockCount = binary.LittleEndian.Uint32(body[12:16])
	if blockCount == 0 || blockCount > absMaxGroupBlocks || blockIndex >= blockCount {
		return 0, 0, 0, nil, fmt.Errorf("spool: bad group framing %d/%d: %w", blockIndex, blockCount, ErrCorrupt)
	}
	if n := binary.LittleEndian.Uint32(body[16:20]); n != wantCount {
		return 0, 0, 0, nil, fmt.Errorf("spool: body count %d != header count %d: %w", n, wantCount, ErrCorrupt)
	}
	out := make([]Record, 0, min(wantCount, 1024))
	rest := body[groupFramingLen+4:]
	for i := uint32(0); i < wantCount; i++ {
		if len(rest) < 8+1+4+4 {
			return 0, 0, 0, nil, fmt.Errorf("spool: truncated record %d: %w", i, ErrCorrupt)
		}
		seq := binary.LittleEndian.Uint64(rest[:8])
		flags := rest[8]
		if flags & ^uint8(flagFieldTombstone) != 0 {
			return 0, 0, 0, nil, fmt.Errorf("spool: record %d has unknown flags %#x: %w", i, flags, ErrCorrupt)
		}
		keyLen := binary.LittleEndian.Uint32(rest[9:13])
		valLen := binary.LittleEndian.Uint32(rest[13:17])
		if keyLen > absMaxKeySize || valLen > absMaxValueSize {
			return 0, 0, 0, nil, fmt.Errorf("spool: record %d oversized (%d/%d): %w", i, keyLen, valLen, ErrCorrupt)
		}
		rest = rest[17:]
		if uint64(len(rest)) < uint64(keyLen)+uint64(valLen) {
			return 0, 0, 0, nil, fmt.Errorf("spool: truncated record %d payload: %w", i, ErrCorrupt)
		}
		deleted := flags&flagFieldTombstone != 0
		if deleted && valLen != 0 {
			return 0, 0, 0, nil, fmt.Errorf("spool: tombstone %d carries a value: %w", i, ErrCorrupt)
		}
		rec := Record{Key: rest[:keyLen], Sequence: seq, Deleted: deleted}
		if !deleted {
			rec.Value = rest[keyLen : keyLen+valLen]
		}
		out = append(out, rec)
		rest = rest[keyLen+valLen:]
	}
	if len(rest) != 0 {
		return 0, 0, 0, nil, fmt.Errorf("spool: %d trailing bytes in block body: %w", len(rest), ErrCorrupt)
	}
	return groupID, blockIndex, blockCount, out, nil
}

// completionFixedLen is the completion body without block sequences
// and digest: groupID u64 + blockCount u32 + totalRecords u32 +
// totalPlainLen u64.
const completionFixedLen = 8 + 4 + 4 + 8

// completion seals a group: ordered data block identities, declared
// totals, and the content digest.
type completion struct {
	groupID      uint64
	blockCount   uint32
	totalRecords uint32
	totalPlain   uint64
	blockSeqs    []uint64
	digest       [32]byte
}

// encodeCompletion serializes the completion body.
func encodeCompletion(c completion) []byte {
	out := make([]byte, 0, completionFixedLen+8*len(c.blockSeqs)+32)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], c.groupID)
	out = append(out, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], c.blockCount)
	out = append(out, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], c.totalRecords)
	out = append(out, tmp[:4]...)
	binary.LittleEndian.PutUint64(tmp[:], c.totalPlain)
	out = append(out, tmp[:]...)
	for _, s := range c.blockSeqs {
		binary.LittleEndian.PutUint64(tmp[:], s)
		out = append(out, tmp[:]...)
	}
	out = append(out, c.digest[:]...)
	return out
}

// decodeCompletion parses and bound-checks a completion body.
func decodeCompletion(body []byte) (completion, error) {
	var c completion
	if len(body) < completionFixedLen+32 {
		return c, fmt.Errorf("spool: truncated completion: %w", ErrCorrupt)
	}
	c.groupID = binary.LittleEndian.Uint64(body[0:8])
	c.blockCount = binary.LittleEndian.Uint32(body[8:12])
	c.totalRecords = binary.LittleEndian.Uint32(body[12:16])
	c.totalPlain = binary.LittleEndian.Uint64(body[16:24])
	if c.groupID == 0 || c.blockCount == 0 || c.blockCount > absMaxGroupBlocks {
		return c, fmt.Errorf("spool: bad completion framing: %w", ErrCorrupt)
	}
	if c.totalRecords == 0 || c.totalRecords > absMaxGroupRecords {
		return c, fmt.Errorf("spool: bad completion records %d: %w", c.totalRecords, ErrCorrupt)
	}
	want := completionFixedLen + 8*int(c.blockCount) + 32
	if len(body) != want {
		return c, fmt.Errorf("spool: completion size %d != %d: %w", len(body), want, ErrCorrupt)
	}
	c.blockSeqs = make([]uint64, c.blockCount)
	for i := range c.blockSeqs {
		c.blockSeqs[i] = binary.LittleEndian.Uint64(body[completionFixedLen+8*i:])
	}
	copy(c.digest[:], body[len(body)-32:])
	return c, nil
}

// groupDigest computes the completion content digest over sealed
// data payloads in index order. seqs and sealed run parallel.
func groupDigest(groupID uint64, blockCount uint32, seqs []uint64, sealed [][]byte) [32]byte {
	h := sha256.New()
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], groupID)
	h.Write(tmp[:])
	binary.LittleEndian.PutUint32(tmp[:4], blockCount)
	h.Write(tmp[:4])
	for i := range sealed {
		binary.LittleEndian.PutUint64(tmp[:], seqs[i])
		h.Write(tmp[:])
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(sealed[i])))
		h.Write(tmp[:4])
		h.Write(sealed[i])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
