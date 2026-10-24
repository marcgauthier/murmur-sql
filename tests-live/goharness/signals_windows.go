//go:build windows

package goharness

import (
	"errors"
	"os"
)

func terminateProcess(p *os.Process) error { return p.Kill() }

func suspendProcess(p *os.Process) error { return errors.New("node suspend needs unix signals") }

func resumeProcess(p *os.Process) error { return errors.New("node resume needs unix signals") }
