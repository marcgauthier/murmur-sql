// Package harness is shared test scaffolding for the Go tests-live/
// scenario ports: in-process multi-Database cluster management, test CA +
// per-node QUIC/TLS credentials, open-with-watchdog, forensics, artifact
// retention, and fixture-subprocess helpers.
//
// It mirrors the Rust murmur-live-harness crate
// (/home/marc/MURMUR/tests-live/harness): a "node" is an open
// murmur.Database on its own directory (runtime/<scenario>/nodeN/);
// stop/restart is Close plus Open on the same dir with the same key; the
// replication mesh is QUIC with ephemeral 127.0.0.1:0 binds, so no port
// registry is needed. True SIGKILL/SIGSTOP/SIGTERM legs additionally need
// fixture subprocesses (see fixture.go) because signals cannot target one
// in-process node without freezing the test driver.
package harness

import (
	"os"
	"strconv"
	"time"
)

// EnvSeconds reads a positive-integer seconds env knob with fallback.
func EnvSeconds(name string, fallbackSecs uint64) time.Duration {
	if v, err := strconv.ParseUint(os.Getenv(name), 10, 64); err == nil && v > 0 {
		return time.Duration(v) * time.Second
	}
	return time.Duration(fallbackSecs) * time.Second
}

// DeadlineScale is the wall-clock multiplier for convergence deadlines on
// loaded machines. Same knob as the unit suites
// (MURMUR_TEST_DEADLINE_SCALE, default 1); loaded CI sets 3 or higher.
// Read fresh on every wait so a whole suite run honors one export.
func DeadlineScale() float64 {
	if v, err := strconv.ParseFloat(os.Getenv("MURMUR_TEST_DEADLINE_SCALE"), 64); err == nil && v > 0 && v == v && v < 1e300 {
		return v
	}
	return 1.0
}

// WaitUntil polls cond every poll interval until it holds or the (scaled)
// timeout elapses. It returns true when the condition was met (use with a
// fatal + forensics on false).
func WaitUntil(timeout, poll time.Duration, cond func() bool) bool {
	end := time.Now().Add(time.Duration(float64(timeout) * DeadlineScale()))
	for {
		if cond() {
			return true
		}
		if !time.Now().Before(end) {
			return false
		}
		time.Sleep(poll)
	}
}
