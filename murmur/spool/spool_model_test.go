package spool_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// TestReferenceModel runs randomized mixed operations against an
// in-memory map model: puts, deletes, mixed-durability commits,
// flushes, syncs, rotation, reclamation, rewrites, and mid-run
// checkpoint captures. Checkpoint cuts must equal the model at
// capture time exactly, and the post-reopen load must equal the
// final model exactly: no acknowledged write lost, no unacknowledged
// partial group, no maintenance-induced drift.
func TestReferenceModel(t *testing.T) {
	for _, seed := range []int64{1, 7, 42} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			runReferenceModel(t, seed)
		})
	}
}

func runReferenceModel(t *testing.T, seed int64) {
	t.Helper()
	dir := t.TempDir()
	st, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	prng := rand.New(rand.NewSource(seed))
	model := make(map[string][]byte)
	key := func() string { return fmt.Sprintf("k%02d", prng.Intn(50)) }
	val := func() []byte {
		v := make([]byte, prng.Intn(200)+1)
		prng.Read(v)
		return v
	}
	apply := func(k string, v []byte, deleted bool) {
		if deleted {
			delete(model, k)
		} else {
			model[k] = append([]byte(nil), v...)
		}
	}
	snapshot := func() map[string][]byte {
		out := make(map[string][]byte, len(model))
		for k, v := range model {
			out[k] = append([]byte(nil), v...)
		}
		return out
	}
	check := func(what string, dir string, want map[string][]byte) {
		t.Helper()
		got := loadAll(t, dir)
		if len(got) != len(want) {
			t.Fatalf("%s: %d keys, model has %d", what, len(got), len(want))
		}
		for k, v := range want {
			if !bytes.Equal(got[k], v) {
				t.Fatalf("%s: key %q mismatch", what, k)
			}
		}
	}
	type pinnedCut struct {
		cp   *spool.Checkpoint
		want map[string][]byte
	}
	var checkpoints []pinnedCut
	defer func() {
		for _, p := range checkpoints {
			p.cp.Release()
		}
	}()
	const ops = 2000
	for i := 0; i < ops; i++ {
		switch prng.Intn(100) {
		case 0, 1, 2, 3, 4:
			// Sync commit of 1-8 mixed mutations.
			n := prng.Intn(8) + 1
			muts := make([]spool.Mutation, n)
			for j := range muts {
				k := key()
				if prng.Intn(5) == 0 {
					muts[j] = spool.Mutation{Key: []byte(k), Deleted: true}
				} else {
					muts[j] = spool.Mutation{Key: []byte(k), Value: val()}
				}
			}
			if err := st.Commit(muts, spool.DurabilitySync); err != nil {
				t.Fatalf("op %d commit: %v", i, err)
			}
			for _, m := range muts {
				apply(string(m.Key), m.Value, m.Deleted)
			}
		case 5, 6, 7, 8, 9:
			// Async commit.
			n := prng.Intn(8) + 1
			muts := make([]spool.Mutation, n)
			for j := range muts {
				k := key()
				if prng.Intn(5) == 0 {
					muts[j] = spool.Mutation{Key: []byte(k), Deleted: true}
				} else {
					muts[j] = spool.Mutation{Key: []byte(k), Value: val()}
				}
			}
			if err := st.Commit(muts, spool.DurabilityAsync); err != nil {
				t.Fatalf("op %d commit async: %v", i, err)
			}
			for _, m := range muts {
				apply(string(m.Key), m.Value, m.Deleted)
			}
		case 10, 11, 12, 13:
			// Flush fence.
			if err := st.Flush(); err != nil {
				t.Fatalf("op %d flush: %v", i, err)
			}
		case 14, 15:
			// Sync fence.
			if err := st.Sync(); err != nil {
				t.Fatalf("op %d sync: %v", i, err)
			}
		case 16:
			// Rotation.
			if err := st.RotateKey(); err != nil {
				t.Fatalf("op %d rotate: %v", i, err)
			}
		case 17:
			// Reclamation (tombstone-heavy key space exercises
			// marker retention alongside rewrites).
			if err := st.Reclaim(); err != nil {
				t.Fatalf("op %d reclaim: %v", i, err)
			}
		case 18:
			// Checkpoint capture: the cut must equal the model
			// exactly, since every admitted write precedes the
			// capture fence and nothing follows it yet.
			want := snapshot()
			cp, err := st.Checkpoint(context.Background(), filepath.Join(dir, fmt.Sprintf("cp-%d", i)))
			if err != nil {
				t.Fatalf("op %d checkpoint: %v", i, err)
			}
			checkpoints = append(checkpoints, pinnedCut{cp: cp, want: want})
			check(fmt.Sprintf("checkpoint@%d", i), cp.Dir, want)
		case 19:
			// Delete.
			k := key()
			if err := st.Delete([]byte(k)); err != nil {
				t.Fatalf("op %d delete: %v", i, err)
			}
			apply(k, nil, true)
		default:
			// Async put.
			k, v := key(), val()
			if err := st.Put([]byte(k), v); err != nil {
				t.Fatalf("op %d put: %v", i, err)
			}
			apply(k, v, false)
		}
		if i == ops/2 {
			// Mid-run full rewrite: logical state invariant.
			if err := st.RewriteDataKeys(); err != nil {
				t.Fatalf("rewrite: %v", err)
			}
		}
	}
	// Draining close persists everything acknowledged; the
	// reopened load must equal the final model exactly.
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st2, err := spool.Open(testOptions(t, dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	check("reopen", dir, model)
	// Every captured checkpoint still opens with its exact cut
	// after everything that followed (rotation, reclamation,
	// rewrite, more writes, close, reopen).
	for _, p := range checkpoints {
		check(fmt.Sprintf("checkpoint %s final", p.cp.ID), p.cp.Dir, p.want)
	}
}
