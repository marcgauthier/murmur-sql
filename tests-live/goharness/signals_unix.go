//go:build unix

package goharness

import (
	"os"
	"syscall"
)

func terminateProcess(p *os.Process) error { return p.Signal(syscall.SIGTERM) }

func suspendProcess(p *os.Process) error { return p.Signal(syscall.SIGSTOP) }

func resumeProcess(p *os.Process) error { return p.Signal(syscall.SIGCONT) }
