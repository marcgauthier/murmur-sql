package spool_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/compression"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// notCodec is a trivial custom codec (bitwise NOT) with a call counter so
// tests can prove the hook is really on the write and read paths.
type notCodec struct {
	id    uint8
	calls *int
}

func (n notCodec) ID() uint8    { return n.id }
func (n notCodec) Name() string { return "not" }
func (n notCodec) Compress(dst, src []byte) ([]byte, error) {
	*n.calls++
	for _, b := range src {
		dst = append(dst, ^b)
	}
	return dst, nil
}
func (n notCodec) Decompress(dst, src []byte, max int) ([]byte, error) {
	if len(src) > max {
		return nil, compression.ErrTooLarge
	}
	for _, b := range src {
		dst = append(dst, ^b)
	}
	return dst, nil
}

func TestCustomCodecHook(t *testing.T) {
	dir := t.TempDir()
	var calls int
	cc := notCodec{id: 200, calls: &calls}

	o := testOptions(t, dir)
	o.Codec = cc
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	want := map[string]string{}
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("k-%04d", i)
		v := "v-" + string(bytes.Repeat([]byte{byte('a' + i%26)}, 40))
		want[k] = v
		if err := st.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if calls == 0 {
		t.Fatal("custom codec never invoked on write path")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Offline load without the codec registered must fail cleanly.
	err = spool.LoadWithOptions(spool.LoadOptions{Path: dir, MasterKey: testMasterKey},
		func([]spool.Record) error { return nil })
	if err == nil {
		t.Fatal("load without registered codec succeeded")
	}
	// With it registered the data is intact.
	got := map[string]string{}
	err = spool.LoadWithOptions(spool.LoadOptions{Path: dir, MasterKey: testMasterKey, Codecs: []compression.Codec{cc}},
		func(recs []spool.Record) error {
			for _, r := range recs {
				got[string(r.Key)] = string(r.Value)
			}
			return nil
		})
	if err != nil {
		t.Fatalf("load with codec: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("loaded %d keys, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("key %s mismatch", k)
		}
	}

	// Reopen with the default codec for new writes but the custom one
	// registered for reading: old blocks stay readable.
	o2 := testOptions(t, dir)
	o2.Codecs = []compression.Codec{cc}
	loaded := map[string]string{}
	st, err = spool.OpenAndLoad(o2, func(recs []spool.Record) error {
		for _, r := range recs {
			loaded[string(r.Key)] = string(r.Value)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("reopen with Codecs via OpenAndLoad: %v", err)
	}
	if v := loaded["k-0007"]; v != want["k-0007"] {
		t.Fatalf("OpenAndLoad old key: %q, want %q", v, want["k-0007"])
	}
	if err := st.Put([]byte("new"), []byte("deflated")); err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCustomCodecValidation(t *testing.T) {
	var calls int
	for name, mut := range map[string]func(*spool.Options){
		"builtin-range id":     func(o *spool.Options) { o.Codec = notCodec{id: 3, calls: &calls} },
		"retired id":           func(o *spool.Options) { o.Codec = notCodec{id: 1, calls: &calls} },
		"unregistered mode":    func(o *spool.Options) { o.Compression = spool.Compression(150) },
		"retired zstd mode id": func(o *spool.Options) { o.Compression = spool.Compression(2) },
	} {
		t.Run(name, func(t *testing.T) {
			o := testOptions(t, t.TempDir())
			mut(&o)
			if st, err := spool.Open(o); err == nil {
				st.Close()
				t.Fatal("Open accepted invalid compression configuration")
			}
		})
	}
}
