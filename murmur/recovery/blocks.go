package recovery

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"io"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
)

// MaxBlockSealed caps one sealed block payload: plaintext bound plus
// compression slack, codec framing, and tag. Corrupt headers claiming
// more fail before any allocation.
const MaxBlockSealed = MaxBlockPlain + MaxBlockPlain/8 + 65536 + GCMTagLen

// blockWriter accumulates length-delimited records into compressed,
// integrity-protected blocks. Records are opaque bytes; key is the
// ordering key of the record (first/last become the block range).
type blockWriter struct {
	dst         io.Writer
	cipher      *Cipher
	codec       compression.Codec
	aad         func(block uint32) []byte
	targetPlain int
	file        string
	plain       []byte
	rows        uint32
	firstKey    []byte
	lastKey     []byte
	index       uint32
	bytesOut    int64
}

func newBlockWriter(dst io.Writer, cipher *Cipher, codec compression.Codec, file string, aad func(uint32) []byte, targetPlain int) *blockWriter {
	return &blockWriter{dst: dst, cipher: cipher, codec: codec, file: file, aad: aad, targetPlain: targetPlain}
}

// Add buffers one record, flushing a full block when the plaintext
// target is reached.
func (w *blockWriter) Add(key, record []byte) error {
	if len(record) == 0 {
		return fmt.Errorf("recovery: empty record for %s", w.file)
	}
	w.plain = append(w.plain, record...)
	if w.rows == 0 {
		w.firstKey = append([]byte(nil), key...)
	}
	w.lastKey = append([]byte(nil), key...)
	w.rows++
	if len(w.plain) >= w.targetPlain {
		return w.Flush()
	}
	return nil
}

// Flush writes any pending partial block.
func (w *blockWriter) Flush() error {
	if w.rows == 0 {
		return nil
	}
	comp, err := w.codec.Compress(nil, w.plain)
	if err != nil {
		return fmt.Errorf("recovery: compress %s: %w", w.file, err)
	}
	nonce, sealed, err := w.cipher.Seal(comp, w.aad(w.index))
	if err != nil {
		return err
	}
	var hdr []byte
	hdr = appendBytes(hdr, w.firstKey)
	hdr = appendBytes(hdr, w.lastKey)
	var tmp [12]byte
	binary.LittleEndian.PutUint32(tmp[0:4], w.rows)
	binary.LittleEndian.PutUint32(tmp[4:8], uint32(len(w.plain)))
	binary.LittleEndian.PutUint32(tmp[8:12], uint32(len(sealed)))
	hdr = append(hdr, tmp[:]...)
	hdr = append(hdr, nonce...)
	n, err := w.dst.Write(hdr)
	w.bytesOut += int64(n)
	if err != nil {
		return fmt.Errorf("recovery: write %s block: %w", w.file, err)
	}
	n, err = w.dst.Write(sealed)
	w.bytesOut += int64(n)
	if err != nil {
		return fmt.Errorf("recovery: write %s block: %w", w.file, err)
	}
	w.plain = w.plain[:0]
	w.rows = 0
	w.firstKey, w.lastKey = nil, nil
	w.index++
	return nil
}

// blockReader streams blocks written by blockWriter, verifying framing
// bounds, integrity, and plaintext length claims.
type blockReader struct {
	src      *bufio.Reader
	cipher   *Cipher
	codec    compression.Codec
	aad      func(block uint32) []byte
	maxPlain int
	file     string
	index    uint32
}

func newBlockReader(src io.Reader, cipher *Cipher, codec compression.Codec, file string, aad func(uint32) []byte, maxPlain int) *blockReader {
	return &blockReader{src: bufio.NewReader(src), cipher: cipher, codec: codec, file: file, aad: aad, maxPlain: maxPlain}
}

func (r *blockReader) readByte() (byte, error) {
	b, err := r.src.ReadByte()
	if err != nil {
		return 0, err
	}
	return b, nil
}

func (r *blockReader) readUvarint() (uint64, error) {
	var x uint64
	for i := 0; i < binary.MaxVarintLen64; i++ {
		b, err := r.readByte()
		if err != nil {
			return 0, err
		}
		if b < 0x80 {
			if i == binary.MaxVarintLen64-1 && b > 1 {
				return 0, corruptErr("varint overflow in %s", r.file)
			}
			return x | uint64(b)<<(7*i), nil
		}
		x |= uint64(b&0x7f) << (7 * i)
	}
	return 0, corruptErr("malformed varint in %s", r.file)
}

func (r *blockReader) readFull(n int, what string) ([]byte, error) {
	if n < 0 {
		return nil, corruptErr("negative %s length in %s", what, r.file)
	}
	out := make([]byte, n)
	if _, err := io.ReadFull(r.src, out); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, corruptErr("truncated %s in %s", what, r.file)
		}
		return nil, fmt.Errorf("recovery: read %s %s: %w", r.file, what, err)
	}
	return out, nil
}

// Next returns the next block's key range, plaintext, and claimed row
// count, or io.EOF at a clean block boundary.
func (r *blockReader) Next() (firstKey, lastKey, plain []byte, rows uint32, err error) {
	n, err := r.readUvarint()
	if err != nil {
		if err == io.EOF {
			return nil, nil, nil, 0, io.EOF
		}
		if IsCorrupt(err) {
			return nil, nil, nil, 0, err
		}
		return nil, nil, nil, 0, fmt.Errorf("recovery: read %s: %w", r.file, err)
	}
	if n > MaxKeyLen {
		return nil, nil, nil, 0, corruptErr("block min key too large in %s", r.file)
	}
	firstKey, err = r.readFull(int(n), "min key")
	if err != nil {
		return nil, nil, nil, 0, err
	}
	n, err = r.readUvarint()
	if err != nil {
		if IsCorrupt(err) {
			return nil, nil, nil, 0, err
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, nil, nil, 0, corruptErr("truncated block in %s", r.file)
		}
		return nil, nil, nil, 0, fmt.Errorf("recovery: read %s: %w", r.file, err)
	}
	if n > MaxKeyLen {
		return nil, nil, nil, 0, corruptErr("block max key too large in %s", r.file)
	}
	lastKey, err = r.readFull(int(n), "max key")
	if err != nil {
		return nil, nil, nil, 0, err
	}
	fixed, err := r.readFull(12, "block header")
	if err != nil {
		return nil, nil, nil, 0, err
	}
	rows = binary.LittleEndian.Uint32(fixed[0:4])
	plainLen := binary.LittleEndian.Uint32(fixed[4:8])
	sealedLen := binary.LittleEndian.Uint32(fixed[8:12])
	if rows == 0 || rows > MaxBlockRows {
		return nil, nil, nil, 0, corruptErr("bad block row count %d in %s", rows, r.file)
	}
	if plainLen == 0 || plainLen > uint32(r.maxPlain) {
		return nil, nil, nil, 0, corruptErr("bad block plain length %d in %s", plainLen, r.file)
	}
	if sealedLen == 0 || sealedLen > MaxBlockSealed {
		return nil, nil, nil, 0, corruptErr("bad block sealed length %d in %s", sealedLen, r.file)
	}
	nonce, err := r.readFull(NonceLen, "nonce")
	if err != nil {
		return nil, nil, nil, 0, err
	}
	sealed, err := r.readFull(int(sealedLen), "payload")
	if err != nil {
		return nil, nil, nil, 0, err
	}
	comp, err := r.cipher.Open(nonce, sealed, r.aad(r.index))
	if err != nil {
		return nil, nil, nil, 0, err
	}
	plain, err = r.codec.Decompress(nil, comp, int(plainLen))
	if err != nil {
		return nil, nil, nil, 0, corruptErr("decompress %s: %v", r.file, err)
	}
	if uint32(len(plain)) != plainLen {
		return nil, nil, nil, 0, corruptErr("plain length mismatch in %s", r.file)
	}
	r.index++
	return firstKey, lastKey, plain, rows, nil
}

// hashReader tees everything read into h for end-of-stream verification.
type hashReader struct {
	src io.Reader
	h   hash.Hash
}

func (r *hashReader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 {
		r.h.Write(p[:n])
	}
	return n, err
}
