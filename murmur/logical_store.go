package murmur

import (
	"bytes"
	"context"
	"fmt"

	"github.com/marcgauthier/murmur-sql-go/murmur/internal/postcard"
	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

// Logical values split into authenticated Spool blocks. Only Spool is
// authoritative.

var blobMagic = []byte("MURMUR-BLOB\x00\x01")

const (
	blobPiece  = 64 << 10
	blobInline = 1 << 20
	blobMax    = 512 << 20
)

// blobDescriptor splits a large logical value across piece records.
type blobDescriptor struct {
	inline bool
	value  []byte
	bytes  int
	digest [32]byte
	count  int
}

func encodeDescriptor(e *postcard.Encoder, d blobDescriptor) {
	if d.inline {
		e.Discriminant(0)
		e.ByteString(d.value)
		return
	}
	e.Discriminant(1)
	e.U64(uint64(d.bytes))
	e.Raw(d.digest[:])
	e.U64(uint64(d.count))
}

func decodeDescriptor(d *postcard.Decoder) (blobDescriptor, error) {
	switch d.Discriminant() {
	case 0:
		return blobDescriptor{inline: true, value: d.Bytes()}, d.Err()
	case 1:
		out := blobDescriptor{}
		out.bytes = int(d.U64())
		copy(out.digest[:], d.Raw(32))
		out.count = int(d.U64())
		return out, d.Err()
	default:
		return blobDescriptor{}, storageErr("bad blob descriptor")
	}
}

func pieceKey(key []byte, digest [32]byte, index int) []byte {
	return []byte(fmt.Sprintf("11/blob/%s/%s/%08d", hashHex(key), hexEncode(digest[:]), index))
}

func splitDescriptor(value []byte) (*blobDescriptor, error) {
	if !bytes.HasPrefix(value, blobMagic) {
		return nil, nil
	}
	d, err := decodeOne(value[len(blobMagic):], func(x *postcard.Decoder) blobDescriptor {
		desc, derr := decodeDescriptor(x)
		if derr != nil {
			x.Fail(derr)
		}
		return desc
	})
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// LogicalStore splits large values into authenticated Spool blocks.
type LogicalStore struct {
	store *spool.Store
}

// NewLogicalStore wraps a physical store.
func NewLogicalStore(store *spool.Store) *LogicalStore {
	return &LogicalStore{store: store}
}

// Physical returns the underlying store.
func (s *LogicalStore) Physical() *spool.Store { return s.store }

// KeysWithPrefix lists physical keys with a prefix.
func (s *LogicalStore) KeysWithPrefix(prefix []byte) [][]byte {
	return s.store.KeysWithPrefix(prefix)
}

// Get reads a logical value, reassembling split blobs.
func (s *LogicalStore) Get(key []byte) ([]byte, error) {
	value, err := s.store.Get(key)
	if err != nil {
		return nil, storageErr("%v", err)
	}
	if value == nil {
		return nil, nil
	}
	var desc *blobDescriptor
	if bytes.Compare(key, []byte("04/")) >= 0 {
		desc, err = splitDescriptor(value)
		if err != nil {
			return nil, err
		}
	}
	if desc == nil {
		return value, nil
	}
	if desc.inline {
		return desc.value, nil
	}
	if desc.count != (desc.bytes+blobPiece-1)/blobPiece || desc.bytes > blobMax {
		return nil, storageErr("blob descriptor bounds")
	}
	out := make([]byte, 0, desc.bytes)
	for index := 0; index < desc.count; index++ {
		part, err := s.store.Get(pieceKey(key, desc.digest, index))
		if err != nil {
			return nil, storageErr("%v", err)
		}
		if part == nil {
			return nil, storageErr("missing blob piece")
		}
		want := blobPiece
		if rest := desc.bytes - index*blobPiece; rest < want {
			want = rest
		}
		if len(part) != want {
			return nil, storageErr("blob piece length")
		}
		out = append(out, part...)
	}
	if hash(out) != desc.digest {
		return nil, storageErr("blob digest")
	}
	return out, nil
}

// Commit applies mutations, splitting large values and cleaning
// replaced pieces in the same atomic batch.
func (s *LogicalStore) Commit(mutations []spool.Mutation, durability spool.Durability) error {
	// Absent means unseen, nil means deleted by an earlier mutation.
	previous := make(map[string]*[]byte, len(mutations))
	var records []spool.Mutation
	for i := range mutations {
		m := &mutations[i]
		var oldValue []byte
		if old, ok := previous[string(m.Key)]; ok {
			if old != nil {
				oldValue = *old
			}
		} else {
			v, err := s.store.Get(m.Key)
			if err != nil {
				return storageErr("%v", err)
			}
			oldValue = v
		}
		if oldValue != nil && bytes.Compare(m.Key, []byte("04/")) >= 0 {
			desc, err := splitDescriptor(oldValue)
			if err != nil {
				return err
			}
			if desc != nil && !desc.inline {
				for index := 0; index < desc.count; index++ {
					records = append(records, spool.Mutation{
						Key:     pieceKey(m.Key, desc.digest, index),
						Deleted: true,
					})
				}
			}
		}
		var value []byte
		var deleted bool
		switch {
		case m.Deleted:
			deleted = true
		case bytes.Compare(m.Key, []byte("04/")) >= 0 && len(m.Value) > blobInline:
			digest := hash(m.Value)
			for index := 0; index*blobPiece < len(m.Value); index++ {
				end := (index + 1) * blobPiece
				if end > len(m.Value) {
					end = len(m.Value)
				}
				records = append(records, spool.Mutation{
					Key:   pieceKey(m.Key, digest, index),
					Value: append([]byte(nil), m.Value[index*blobPiece:end]...),
				})
			}
			desc := blobDescriptor{bytes: len(m.Value), digest: digest,
				count: (len(m.Value) + blobPiece - 1) / blobPiece}
			buf := encodeOne(func(e *postcard.Encoder) { encodeDescriptor(e, desc) })
			value = append(append([]byte(nil), blobMagic...), buf...)
		case bytes.Compare(m.Key, []byte("04/")) >= 0 && bytes.HasPrefix(m.Value, blobMagic):
			buf := encodeOne(func(e *postcard.Encoder) {
				encodeDescriptor(e, blobDescriptor{inline: true, value: m.Value})
			})
			value = append(append([]byte(nil), blobMagic...), buf...)
		default:
			value = m.Value
		}
		if deleted {
			records = append(records, spool.Mutation{Key: m.Key, Deleted: true})
		} else {
			records = append(records, spool.Mutation{Key: m.Key, Value: value})
		}
		if deleted {
			previous[string(m.Key)] = nil
		} else {
			v := value
			previous[string(m.Key)] = &v
		}
	}
	if err := s.store.Commit(records, durability); err != nil {
		return storageErr("%v", err)
	}
	return nil
}

// KeysPage lists up to limit physical keys with a prefix after after.
func (s *LogicalStore) KeysPage(prefix, after []byte, limit int) [][]byte {
	return s.store.KeysPage(prefix, after, limit)
}

// Close flushes and closes the store.
func (s *LogicalStore) Close() error {
	if err := s.store.Close(); err != nil {
		return storageErr("%v", err)
	}
	return nil
}

// Flush persists buffered writes.
func (s *LogicalStore) Flush() error {
	if err := s.store.Flush(); err != nil {
		return storageErr("%v", err)
	}
	return nil
}

// RotateKey rotates the data encryption key.
func (s *LogicalStore) RotateKey() error {
	if err := s.store.RotateKey(); err != nil {
		return storageErr("%v", err)
	}
	return nil
}

// RotateMasterKey re-protects the keyring under a new master key.
func (s *LogicalStore) RotateMasterKey(newKey []byte) error {
	if err := s.store.RotateMasterKey(newKey, ""); err != nil {
		return storageErr("%v", err)
	}
	return nil
}

// Reclaim compacts segments and drops superseded data.
func (s *LogicalStore) Reclaim() error {
	if err := s.store.Reclaim(); err != nil {
		return storageErr("%v", err)
	}
	return nil
}

// Checkpoint captures a consistent copy into destination.
func (s *LogicalStore) Checkpoint(destination string) (*spool.Checkpoint, error) {
	cp, err := s.store.Checkpoint(context.Background(), destination)
	if err != nil {
		return nil, storageErr("%v", err)
	}
	return cp, nil
}

// Stats snapshots store statistics.
func (s *LogicalStore) Stats() spool.Stats {
	return s.store.Stats()
}
