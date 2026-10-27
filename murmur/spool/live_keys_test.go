package spool_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

func liveKeyMap(t *testing.T, st *spool.Store, prefix string) map[string]uint64 {
	t.Helper()
	versions, err := st.LiveKeyVersions([]byte(prefix))
	if err != nil {
		t.Fatalf("LiveKeyVersions: %v", err)
	}
	out := make(map[string]uint64, len(versions))
	var prev []byte
	for i, kv := range versions {
		if i > 0 && bytes.Compare(kv.Key, prev) <= 0 {
			t.Fatalf("not strictly sorted at %d", i)
		}
		prev = append([]byte(nil), kv.Key...)
		if !bytes.HasPrefix(kv.Key, []byte(prefix)) {
			t.Fatalf("key %q lacks prefix %q", kv.Key, prefix)
		}
		if kv.Sequence == 0 {
			t.Fatalf("key %q has zero sequence", kv.Key)
		}
		out[string(kv.Key)] = kv.Sequence
	}
	return out
}

func checkKeySetMatches(t *testing.T, st *spool.Store, prefix string) {
	t.Helper()
	versions, err := st.LiveKeyVersions([]byte(prefix))
	if err != nil {
		t.Fatalf("LiveKeyVersions: %v", err)
	}
	keys := st.KeysWithPrefix([]byte(prefix))
	if len(versions) != len(keys) {
		t.Fatalf("versions=%d keys=%d", len(versions), len(keys))
	}
	for i := range versions {
		if !bytes.Equal(versions[i].Key, keys[i]) {
			t.Fatalf("entry %d: %q vs %q", i, versions[i].Key, keys[i])
		}
	}
}

func TestLiveKeyVersionsBasic(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	put := func(k, v string) {
		t.Helper()
		if err := st.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	put("04/meta/row/t1/aaa", "v1")
	put("04/meta/row/t1/bbb", "v1")
	put("04/meta/row/t2/aaa", "v1")
	put("07/replication/log/x", "v1")
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before := liveKeyMap(t, st, "04/meta/row/")
	if len(before) != 3 {
		t.Fatalf("keys = %d, want 3", len(before))
	}
	// Update advances exactly that key's sequence.
	put("04/meta/row/t1/aaa", "v2")
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	after := liveKeyMap(t, st, "04/meta/row/")
	if after["04/meta/row/t1/aaa"] <= before["04/meta/row/t1/aaa"] {
		t.Fatal("updated sequence did not advance")
	}
	if after["04/meta/row/t1/bbb"] != before["04/meta/row/t1/bbb"] {
		t.Fatal("untouched sequence changed")
	}
	// Delete excludes the key.
	if err := st.Delete([]byte("04/meta/row/t1/bbb")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	deleted := liveKeyMap(t, st, "04/meta/row/")
	if _, ok := deleted["04/meta/row/t1/bbb"]; ok {
		t.Fatal("deleted key still listed")
	}
	if len(deleted) != 2 {
		t.Fatalf("keys = %d, want 2", len(deleted))
	}
	// Reinsert returns with a newer sequence.
	put("04/meta/row/t1/bbb", "v3")
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	reins := liveKeyMap(t, st, "04/meta/row/")
	if reins["04/meta/row/t1/bbb"] <= before["04/meta/row/t1/bbb"] {
		t.Fatal("reinserted sequence did not advance")
	}
	checkKeySetMatches(t, st, "04/meta/row/")
}

func TestLiveKeyVersionsRandomized(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	rng := rand.New(rand.NewSource(20261026))
	const keys = 300
	for round := 0; round < 5; round++ {
		for i := 0; i < 400; i++ {
			k := fmt.Sprintf("04/meta/row/t%02d/key-%04d", rng.Intn(4), rng.Intn(keys))
			if rng.Intn(5) == 0 {
				if err := st.Delete([]byte(k)); err != nil {
					t.Fatalf("Delete: %v", err)
				}
			} else if err := st.Put([]byte(k), []byte(fmt.Sprintf("r%d-%d", round, i))); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if err := st.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		checkKeySetMatches(t, st, "04/meta/row/")
		liveKeyMap(t, st, "04/meta/row/") // sortedness + prefix + nonzero seqs
	}
}

// TestLiveKeyVersionsStableAcrossMaintenance pins the invariant the
// recovery cache relies on: compaction, key rotation, and reopen
// preserve every live (key, sequence) pair bit-for-bit.
func TestLiveKeyVersionsStableAcrossMaintenance(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	snap := func() map[string]uint64 { return liveKeyMap(t, st, "04/meta/row/") }
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("04/meta/row/t1/key-%04d", i)
		if err := st.Put([]byte(k), []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Churn to create compactable garbage: overwrites plus deletes.
	for round := 0; round < 3; round++ {
		for i := 0; i < 200; i += 2 {
			k := fmt.Sprintf("04/meta/row/t1/key-%04d", i)
			if err := st.Put([]byte(k), []byte(fmt.Sprintf("w-%d-%d", round, i))); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if err := st.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	for i := 1; i < 200; i += 4 {
		if err := st.Delete([]byte(fmt.Sprintf("04/meta/row/t1/key-%04d", i))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if stats := st.Stats(); stats.DeadRecords == 0 {
		t.Fatal("no dead records to compact; test would be vacuous")
	}
	want := snap()
	for i := 0; i < 3; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	if got := snap(); !equalVersions(got, want) {
		t.Fatal("versions changed across compaction")
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	if got := snap(); !equalVersions(got, want) {
		t.Fatal("versions changed across key rotation")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	st = st2
	if got := snap(); !equalVersions(got, want) {
		t.Fatal("versions changed across reopen")
	}
	// New mutations after reopen advance past every old sequence.
	var maxOld uint64
	for _, seq := range want {
		maxOld = max(maxOld, seq)
	}
	if err := st.Put([]byte("04/meta/row/t1/fresh"), []byte("new")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := snap()["04/meta/row/t1/fresh"]; got <= maxOld {
		t.Fatalf("post-reopen seq %d not past old max %d", got, maxOld)
	}
}

func equalVersions(a, b map[string]uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		if vb, ok := b[k]; !ok || va != vb {
			return false
		}
	}
	return true
}

func TestScanLiveKeyVersions(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	for i := 0; i < 150; i++ {
		k := fmt.Sprintf("04/meta/row/t1/key-%04d", i)
		if err := st.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Put([]byte("07/other"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	var scanned []spool.LiveKeyVersion
	if err := st.ScanLiveKeyVersions([]byte("04/meta/row/"), func(kv spool.LiveKeyVersion) error {
		scanned = append(scanned, spool.LiveKeyVersion{Key: append([]byte(nil), kv.Key...), Sequence: kv.Sequence})
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	versions, err := st.LiveKeyVersions([]byte("04/meta/row/"))
	if err != nil {
		t.Fatalf("LiveKeyVersions: %v", err)
	}
	sort.Slice(scanned, func(i, j int) bool { return bytes.Compare(scanned[i].Key, scanned[j].Key) < 0 })
	if len(scanned) != len(versions) {
		t.Fatalf("scanned=%d versions=%d", len(scanned), len(versions))
	}
	for i := range scanned {
		if !bytes.Equal(scanned[i].Key, versions[i].Key) || scanned[i].Sequence != versions[i].Sequence {
			t.Fatalf("entry %d mismatch", i)
		}
	}
	// Callback errors abort and propagate.
	sentinel := errors.New("stop")
	calls := 0
	if err := st.ScanLiveKeyVersions([]byte("04/meta/row/"), func(spool.LiveKeyVersion) error {
		calls++
		return sentinel
	}); !errors.Is(err, sentinel) || calls != 1 {
		t.Fatalf("abort err=%v calls=%d", err, calls)
	}
	// Nil callback fails fast.
	if err := st.ScanLiveKeyVersions([]byte("04/meta/row/"), nil); err == nil {
		t.Fatal("nil callback accepted")
	}
}

func TestLiveKeyVersionsConcurrent(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			k := fmt.Sprintf("04/meta/row/t1/key-%06d", i%500)
			_ = st.Put([]byte(k), []byte(fmt.Sprintf("v-%d", i)))
			if i%25 == 0 {
				_ = st.Flush()
			}
			i++
		}
	}()
	for i := 0; i < 50; i++ {
		versions, err := st.LiveKeyVersions([]byte("04/meta/row/"))
		if err != nil {
			t.Fatalf("LiveKeyVersions: %v", err)
		}
		for j := 1; j < len(versions); j++ {
			if bytes.Compare(versions[j].Key, versions[j-1].Key) <= 0 {
				t.Fatalf("not strictly sorted at scan %d entry %d", i, j)
			}
		}
		var scanned int
		if err := st.ScanLiveKeyVersions([]byte("04/meta/row/"), func(spool.LiveKeyVersion) error {
			scanned++
			return nil
		}); err != nil {
			t.Fatalf("Scan: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}

func TestLiveKeyVersionsEmpty(t *testing.T) {
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if got, err := st.LiveKeyVersions([]byte("04/meta/row/")); err != nil || len(got) != 0 {
		t.Fatalf("empty store: %v %d", err, len(got))
	}
	if err := st.Put([]byte("04/meta/row/t1/a"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got, err := st.LiveKeyVersions([]byte("09/nothing/")); err != nil || len(got) != 0 {
		t.Fatalf("missing prefix: %v %d", err, len(got))
	}
	if got, err := st.LiveKeyVersions(nil); err != nil || len(got) != 1 {
		t.Fatalf("nil prefix: %v %d", err, len(got))
	}
	var scanned int
	if err := st.ScanLiveKeyVersions([]byte("09/nothing/"), func(spool.LiveKeyVersion) error {
		scanned++
		return nil
	}); err != nil || scanned != 0 {
		t.Fatalf("empty scan: %v %d", err, scanned)
	}
	// Closed store fails instead of returning stale data.
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := st.LiveKeyVersions([]byte("04/")); !errors.Is(err, spool.ErrClosed) {
		t.Fatalf("after close: %v", err)
	}
	if err := st.ScanLiveKeyVersions([]byte("04/"), func(spool.LiveKeyVersion) error { return nil }); !errors.Is(err, spool.ErrClosed) {
		t.Fatalf("scan after close: %v", err)
	}
}
