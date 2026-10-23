// Fixture-subprocess spawn/signal/wait helpers.
//
// In-process nodes cannot take SIGKILL/SIGSTOP/SIGTERM without freezing
// the test driver, so crash/signal legs run the testnode fixture binary
// (tests-live/testnode) as a child process. run.sh builds it before
// scenarios; BinaryPath locates it.
package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// CrashExitCode is the controlled-crash exit code (exit(73) convention).
const CrashExitCode = 73

// BinaryPath locates the testnode fixture binary.
//
// MURMUR_TESTNODE_BIN (then MURMUR_BIN) overrides the path; otherwise
// <tests-live>/bin/testnode as built by run.sh.
func BinaryPath() string {
	for _, v := range []string{"MURMUR_TESTNODE_BIN", "MURMUR_BIN"} {
		if p := os.Getenv(v); p != "" {
			return p
		}
	}
	return filepath.Join(TestsLiveDir(), "bin", "testnode")
}

// Spawn starts the fixture binary with args (stdin nulled).
func Spawn(args ...string) (*exec.Cmd, error) {
	cmd := exec.Command(BinaryPath(), args...)
	cmd.Stdin = nil
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

// WaitTimeout waits up to dur for cmd; it reports (false, nil) on timeout
// with the child still running.
func WaitTimeout(cmd *exec.Cmd, dur time.Duration) (bool, error) {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return true, err
	case <-time.After(dur):
		return false, nil
	}
}

// Signal sends Unix signal sig to the child process.
func Signal(cmd *exec.Cmd, sig syscall.Signal) error {
	return cmd.Process.Signal(sig)
}

// Terminate sends SIGTERM (graceful-shutdown legs).
func Terminate(cmd *exec.Cmd) error {
	return Signal(cmd, syscall.SIGTERM)
}

// StopProcess sends SIGSTOP (pause-resume legs).
func StopProcess(cmd *exec.Cmd) error {
	return Signal(cmd, syscall.SIGSTOP)
}

// ContinueProcess sends SIGCONT (pause-resume legs).
func ContinueProcess(cmd *exec.Cmd) error {
	return Signal(cmd, syscall.SIGCONT)
}

// Kill unconditionally kills the child (SIGKILL).
func Kill(cmd *exec.Cmd) error {
	return cmd.Process.Kill()
}
