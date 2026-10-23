package spool_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/marcgauthier/murmur-sql-go/murmur/compression"
	"github.com/marcgauthier/murmur-sql-go/murmur/spool"
)

// countingCodec is a user codec (id 128) that counts decompressions.
type countingCodec struct {
	scans atomic.Int64
}

func (c *countingCodec) ID() uint8    { return 128 }
func (c *countingCodec) Name() string { return "counting-passthrough" }
func (c *countingCodec) Compress(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}
func (c *countingCodec) Decompress(dst, src []byte, maxPlain int) ([]byte, error) {
	c.scans.Add(1)
	if len(src) > maxPlain {
		return nil, fmt.Errorf("too large")
	}
	return append(dst, src...), nil
}

var _ compression.Codec = (*countingCodec)(nil)

func TestOrderedLoadSortedAndCached(t *testing.T) {
	dir := t.TempDir()
	codec := &countingCodec{}
	o := testOptions(t, dir)
	o.Codec = codec
	o.TargetBlockBytes = 2048
	s, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Insertion order shuffles keys across blocks; sorted delivery
	// must repeatedly return to each block instead of disk order.
	var muts []spool.Mutation
	for n := range 256 {
		key := (n * 73) % 256
		val := make([]byte, 4*24)
		for i := range 24 {
			binary.LittleEndian.PutUint32(val[4*i:], uint32(key))
		}
		muts = append(muts, spool.Mutation{Key: []byte(fmt.Sprintf("row/%04d", key)), Value: val})
	}
	if err := s.Commit(muts, spool.DurabilitySync); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	codec.scans.Store(0)
	var loaded []spool.Record
	store, err := spool.OpenAndLoadOrdered(o, func(recs []spool.Record) error {
		for _, r := range recs {
			loaded = append(loaded, spool.Record{
				Key:      append([]byte(nil), r.Key...),
				Value:    append([]byte(nil), r.Value...),
				Sequence: r.Sequence,
				Deleted:  r.Deleted,
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ordered open: %v", err)
	}
	defer store.Close()
	if len(loaded) != 256 {
		t.Fatalf("loaded %d records, want 256", len(loaded))
	}
	for i := 1; i < len(loaded); i++ {
		if bytes.Compare(loaded[i-1].Key, loaded[i].Key) >= 0 {
			t.Fatalf("records not sorted at %d", i)
		}
	}
	for n, r := range loaded {
		want := make([]byte, 4*24)
		for i := range 24 {
			binary.LittleEndian.PutUint32(want[4*i:], uint32(n))
		}
		if !bytes.Equal(r.Value, want) {
			t.Fatalf("record %d value mismatch", n)
		}
	}
	if scans := codec.scans.Load(); scans >= 128 {
		t.Fatalf("ordered scan decompressed %d blocks for 256 small records", scans)
	}
	scans := codec.scans.Load()
	for _, r := range loaded {
		got, err := store.Get(r.Key)
		if err != nil || !bytes.Equal(got, r.Value) {
			t.Fatalf("get after ordered load failed: %v", err)
		}
	}
	if now := codec.scans.Load(); now != scans {
		t.Fatalf("open discarded its hot block cache: %d -> %d", scans, now)
	}
}

func TestOrderedSurvivesCompactionWrongKeyReadOnly(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(t, dir)
	o.MaxSegmentSize = 4096
	o.TargetBlockBytes = 512
	o.MaxBlockBytes = 1024
	o.MaxKeySize = 128
	o.MaxValueSize = 768
	s, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for n := 29; n >= 0; n-- {
		k := []byte(fmt.Sprintf("delta/%08d", n))
		if err := s.Commit([]spool.Mutation{{Key: k, Value: bytes.Repeat([]byte{byte(n)}, 512)}}, spool.DurabilitySync); err != nil {
			t.Fatalf("commit %d: %v", n, err)
		}
	}
	if err := s.Commit([]spool.Mutation{{Key: []byte("delta/00000005"), Value: []byte("latest")}}, spool.DurabilitySync); err != nil {
		t.Fatalf("commit latest: %v", err)
	}
	if err := s.Reclaim(); err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	manifest, err := os.ReadFile(filepath.Join(dir, "manifest"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	keys, err := os.ReadFile(filepath.Join(dir, "keys.enc"))
	if err != nil {
		t.Fatalf("read keys: %v", err)
	}
	bad := o
	bad.MasterKey = bytes.Repeat([]byte{8}, 32)
	if _, err := spool.Open(bad); err == nil {
		t.Fatalf("wrong-key open succeeded")
	}
	manifest2, _ := os.ReadFile(filepath.Join(dir, "manifest"))
	keys2, _ := os.ReadFile(filepath.Join(dir, "keys.enc"))
	if !bytes.Equal(manifest, manifest2) || !bytes.Equal(keys, keys2) {
		t.Fatalf("wrong-key open modified manifest or keys.enc")
	}
	var loaded []spool.Record
	store, err := spool.OpenAndLoadOrdered(o, func(recs []spool.Record) error {
		loaded = append(loaded, recs...)
		return nil
	})
	if err != nil {
		t.Fatalf("ordered open: %v", err)
	}
	defer store.Close()
	if len(loaded) != 30 {
		t.Fatalf("loaded %d records, want 30", len(loaded))
	}
	for i := 1; i < len(loaded); i++ {
		if bytes.Compare(loaded[i-1].Key, loaded[i].Key) >= 0 {
			t.Fatalf("records not sorted at %d", i)
		}
	}
	if !bytes.Equal(loaded[5].Value, []byte("latest")) {
		t.Fatalf("record 5 = %q, want latest", loaded[5].Value)
	}
}
