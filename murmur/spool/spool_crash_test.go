package spool_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// Crash-test protocol. The worker writes numbered batches with
// DurabilitySync, fsyncing a progress line per acked batch, and runs
// Reclaim/Checkpoint inline so kills land mid-maintenance. The parent
// SIGKILLs it, reopens, and verifies exact recovery: every acked batch
// present, at most the single in-flight batch partially visible, and
// Load succeeding (a kill yields prefix-consistent bytes: torn tails
// discard, never corrupt).
//
// Batch b, slot j: key k{(b*20+j)%200}, value v{b:06d}-{j:02d}.
// Every batch with b%5==4 also deletes two keys.
const (
	crashKeys     = 200
	crashBatchLen = 20
)

func crashBatchModel(batch int, model map[string]string) {
	for j := 0; j < crashBatchLen; j++ {
		k := fmt.Sprintf("k%04d", (batch*crashBatchLen+j)%crashKeys)
		model[k] = fmt.Sprintf("v%06d-%02d", batch, j)
	}
	if batch%5 == 4 {
		delete(model, fmt.Sprintf("k%04d", (batch*crashBatchLen)%crashKeys))
		delete(model, fmt.Sprintf("k%04d", (batch*crashBatchLen+1)%crashKeys))
	}
}

func crashOptions(dir string) spool.Options {
	o := spool.DefaultOptions(dir)
	o.MasterKey = testMasterKey
	o.Durability = spool.DurabilitySync
	o.MaxBlockBytes = 256 << 10
	o.MaxSegmentSize = 1 << 20
	o.TargetBlockBytes = 64 << 10
	o.MaxKeySize = 4 << 10
	o.MaxValueSize = 64 << 10
	o.ReclaimInterval = -1
	o.Flush.MaxDelay = -1
	return o
}

// TestCrashWorker is the kill victim; the parent test runs it as a
// subprocess and SIGKILLs it mid-write.
func TestCrashWorker(t *testing.T) {
	dir := os.Getenv("SPOOL_CRASH_WORKER")
	if dir == "" {
		t.Skip("subprocess only")
	}
	st, err := spool.Open(crashOptions(filepath.Join(dir, "store")))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	prog, err := os.OpenFile(filepath.Join(dir, "progress.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	defer prog.Close()
	bw := bufio.NewWriter(prog)
	for batch := 0; batch < 20000; batch++ {
		muts := make([]spool.Mutation, 0, crashBatchLen+2)
		for j := 0; j < crashBatchLen; j++ {
			muts = append(muts, spool.Mutation{
				Key:   []byte(fmt.Sprintf("k%04d", (batch*crashBatchLen+j)%crashKeys)),
				Value: []byte(fmt.Sprintf("v%06d-%02d", batch, j)),
			})
		}
		if batch%5 == 4 {
			muts = append(muts,
				spool.Mutation{Key: []byte(fmt.Sprintf("k%04d", (batch*crashBatchLen)%crashKeys)), Deleted: true},
				spool.Mutation{Key: []byte(fmt.Sprintf("k%04d", (batch*crashBatchLen+1)%crashKeys)), Deleted: true},
			)
		}
		if err := st.Commit(muts, spool.DurabilitySync); err != nil {
			t.Fatalf("batch %d: %v", batch, err)
		}
		if _, err := fmt.Fprintf(bw, "%d\n", batch); err != nil {
			t.Fatalf("progress: %v", err)
		}
		if err := bw.Flush(); err != nil {
			t.Fatalf("progress flush: %v", err)
		}
		if err := prog.Sync(); err != nil {
			t.Fatalf("progress sync: %v", err)
		}
		if batch%10 == 9 {
			_ = st.Reclaim()
		}
		if batch%25 == 24 {
			_, _ = st.Checkpoint(context.Background(), filepath.Join(dir, fmt.Sprintf("ckpt-%d", batch)))
		}
	}
}

// TestCrashKillRecovery SIGKILLs writers mid-batch/mid-maintenance and
// verifies exact recovery on reopen across several rounds.
func TestCrashKillRecovery(t *testing.T) {
	rounds := 6
	if v := os.Getenv("SPOOL_CRASH_ROUNDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			rounds = n
		}
	}
	maxMS := 1500
	if v := os.Getenv("SPOOL_CRASH_MS_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxMS = n
		}
	}
	for r := 0; r < rounds; r++ {
		t.Run(fmt.Sprint(r), func(t *testing.T) {
			dir := t.TempDir()
			st, err := spool.Open(crashOptions(filepath.Join(dir, "store")))
			if err != nil {
				t.Fatal(err)
			}
			seed := make([]spool.KV, 50)
			for i := range seed {
				seed[i] = spool.KV{Key: []byte(fmt.Sprintf("k%04d", i)), Value: []byte("seed")}
			}
			if err := st.PutBatch(seed); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestCrashWorker$", "-test.timeout=120s")
			cmd.Env = append(os.Environ(), "SPOOL_CRASH_WORKER="+dir)
			if err := cmd.Start(); err != nil {
				t.Fatalf("start worker: %v", err)
			}
			time.Sleep(time.Duration(300+r*137%maxMS) * time.Millisecond)
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			verifyCrashRecovery(t, dir)
		})
	}
}

func verifyCrashRecovery(t *testing.T, dir string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "progress.log"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	maxAcked := -1
	acked := map[int]bool{}
	for _, line := range splitLines(string(raw)) {
		if line == "" {
			continue
		}
		b, err := strconv.Atoi(line)
		if err != nil {
			t.Fatalf("bad progress line %q", line)
		}
		acked[b] = true
		if b > maxAcked {
			maxAcked = b
		}
	}
	// Batches ack in order from a single-threaded worker.
	for b := 0; b <= maxAcked; b++ {
		if !acked[b] {
			t.Fatalf("progress gap at batch %d (max %d)", b, maxAcked)
		}
	}
	model := map[string]string{}
	for i := 0; i < 50; i++ {
		model[fmt.Sprintf("k%04d", i)] = "seed"
	}
	for b := 0; b <= maxAcked; b++ {
		crashBatchModel(b, model)
	}
	// The single in-flight batch may be fully, partially, or not
	// visible; anything else must match the acked model exactly.
	inflight := map[string]string{}
	for k, v := range model {
		inflight[k] = v
	}
	crashBatchModel(maxAcked+1, inflight)

	st, err := spool.Open(crashOptions(filepath.Join(dir, "store")))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st.Close()
	got := map[string]string{}
	seqs := map[string]uint64{}
	if err := spool.Load(filepath.Join(dir, "store"), testMasterKey, func(recs []spool.Record) error {
		for _, r := range recs {
			k := string(r.Key)
			if r.Sequence < seqs[k] {
				continue
			}
			seqs[k] = r.Sequence
			if r.Deleted {
				delete(got, k)
			} else {
				got[k] = string(r.Value)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("load after kill: %v", err)
	}
	for k, want := range model {
		if got[k] != want && got[k] != inflight[k] {
			t.Fatalf("key %s: got %q want %q (inflight %q)", k, got[k], want, inflight[k])
		}
	}
	for k, gv := range got {
		mv, inModel := model[k]
		iv, inFlight := inflight[k]
		if (!inModel || mv != gv) && (!inFlight || iv != gv) {
			t.Fatalf("key %s: unexpected value %q", k, gv)
		}
	}
	// Atomic groups recover whole or not at all: every key the
	// in-flight batch touched must agree on one outcome.
	fb := maxAcked + 1
	touched := map[string]bool{}
	for j := 0; j < crashBatchLen; j++ {
		touched[fmt.Sprintf("k%04d", (fb*crashBatchLen+j)%crashKeys)] = true
	}
	if fb%5 == 4 {
		touched[fmt.Sprintf("k%04d", (fb*crashBatchLen)%crashKeys)] = true
		touched[fmt.Sprintf("k%04d", (fb*crashBatchLen+1)%crashKeys)] = true
	}
	votesModel, votesCand := 0, 0
	for k := range touched {
		mv, inModel := model[k]
		iv, inFlight := inflight[k]
		if inModel == inFlight && mv == iv {
			continue
		}
		gv, inGot := got[k]
		matchModel := inGot == inModel && gv == mv
		matchCand := inGot == inFlight && gv == iv
		if matchModel && !matchCand {
			votesModel++
		} else if matchCand && !matchModel {
			votesCand++
		} else {
			t.Fatalf("key %s: matches neither outcome (got %q)", k, gv)
		}
	}
	if votesModel > 0 && votesCand > 0 {
		t.Fatalf("torn atomic group %d: %d keys old, %d keys new", fb, votesModel, votesCand)
	}
	// The recovered store must keep working: write, reclaim, reload.
	if err := st.PutBatch([]spool.KV{{Key: []byte("k0099"), Value: []byte("postcrash")}}); err != nil {
		t.Fatalf("post-crash write: %v", err)
	}
	if err := st.Reclaim(); err != nil {
		t.Fatalf("post-crash reclaim: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("post-crash close: %v", err)
	}
	st2, err := spool.Open(crashOptions(filepath.Join(dir, "store")))
	if err != nil {
		t.Fatalf("second reopen: %v", err)
	}
	defer st2.Close()
}

func splitLines(s string) []string {
	var out []string
	cur := ""
	for _, c := range s {
		if c == '\n' {
			out = append(out, cur)
			cur = ""
		} else {
			cur += string(c)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
