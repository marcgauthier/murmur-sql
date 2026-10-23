// Minimal CLI fixture for crash/signal scenario legs (NOT a daemon).
//
// In-process nodes cannot take signals without freezing the test driver,
// so legs that need SIGKILL/SIGSTOP/SIGTERM (or a controlled crash exit
// code) spawn this binary as a child via the harness fixture helpers and
// signal the child.
//
// Subcommands:
//
//	open --dir D --key-hex K [--mode new|existing] [--insert SQL]
//	[--ready FILE] [--hold-secs N] [--stop-file F] [--dbid-hex H] [--node-hex H]
//	  opens a database, optionally runs one INSERT-style statement, writes
//	  the ready file, then holds until the stop file appears or the hold
//	  expires; closes cleanly and exits 0.
//	wait --secs N: sleep N seconds, exit 0 (signal-target stub).
//	crash [--code N]: exit N immediately (default 73, the controlled
//	  crash-point convention).
//	version: print the fixture version, exit 0.
//
// Exit codes: 0 on success, the crash code for crash, 2 for usage and
// runtime errors. Same CLI/exit-code protocol as the Rust testnode
// (tests-live/harness/src/bin/testnode.rs).
//
// TODO (harness follow-ups, no product change):
//   - graceful SIGTERM handling (clean close + marker); today SIGTERM
//     kills with default disposition (no cleanup).
//   - replication-enabled fixture mode (QUIC mesh leg for mid-flight kills).
package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/marcgauthier/murmur-sql-go/murmur"
)

// Version is the fixture version printed by the version subcommand.
const Version = "0.1.0"

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "testnode: usage: testnode <open|wait|crash|version> ...")
		return 2
	}
	var err error
	switch args[0] {
	case "open":
		err = cmdOpen(args[1:])
	case "wait":
		err = cmdWait(args[1:])
	case "crash":
		return cmdCrash(args[1:])
	case "version":
		fmt.Println("testnode", Version)
		return 0
	default:
		err = fmt.Errorf("unknown subcommand: %s", args[0])
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "testnode: %v\n", err)
		return 2
	}
	return 0
}

// flagValue returns the value of --name value or --name=value.
func flagValue(args []string, name string) (string, bool) {
	for i := 0; i < len(args); i++ {
		if rest, ok := strings.CutPrefix(args[i], name+"="); ok {
			return rest, true
		}
		if args[i] == name && i+1 < len(args) {
			return args[i+1], true
		}
	}
	return "", false
}

func hexBytes(s string, expect int, what string) ([]byte, error) {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != expect {
		return nil, fmt.Errorf("%s: need %d hex chars", what, expect*2)
	}
	return b, nil
}

func cmdOpen(args []string) error {
	dir, ok := flagValue(args, "--dir")
	if !ok {
		return fmt.Errorf("--dir is required")
	}
	keyHex, ok := flagValue(args, "--key-hex")
	if !ok {
		return fmt.Errorf("--key-hex is required")
	}
	key, err := hexBytes(keyHex, 32, "--key-hex")
	if err != nil {
		return err
	}
	mode := murmur.OpenCreateNew
	if m, ok := flagValue(args, "--mode"); ok {
		switch m {
		case "new":
			mode = murmur.OpenCreateNew
		case "existing":
			mode = murmur.OpenExisting
		default:
			return fmt.Errorf("--mode must be new|existing, got %s", m)
		}
	}
	opts := murmur.DefaultOptions()
	opts.Mode = mode
	if h, ok := flagValue(args, "--dbid-hex"); ok {
		b, err := hexBytes(h, 16, "--dbid-hex")
		if err != nil {
			return err
		}
		var id [16]byte
		copy(id[:], b)
		opts.DatabaseID = &id
	}
	if h, ok := flagValue(args, "--node-hex"); ok {
		b, err := hexBytes(h, 16, "--node-hex")
		if err != nil {
			return err
		}
		var id [16]byte
		copy(id[:], b)
		opts.NodeID = &id
	}

	db, err := murmur.Open(dir, key, opts)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()
	if sql, ok := flagValue(args, "--insert"); ok {
		if _, err := db.Transaction(func(s *murmur.Sql) (any, error) {
			_, err := s.Execute(sql)
			return nil, err
		}); err != nil {
			return err
		}
		if err := db.Flush(); err != nil {
			return err
		}
	}
	if ready, ok := flagValue(args, "--ready"); ok {
		if err := os.WriteFile(ready, []byte(fmt.Sprintf("ready pid=%d\n", os.Getpid())), 0o644); err != nil {
			return err
		}
	}

	hold := uint64(^uint64(0))
	if v, ok := flagValue(args, "--hold-secs"); ok {
		hold, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
	}
	stopFile, hasStop := flagValue(args, "--stop-file")
	var end time.Time
	if hold == uint64(^uint64(0)) {
		end = time.Now().Add(100 * 365 * 24 * time.Hour)
	} else {
		end = time.Now().Add(time.Duration(hold) * time.Second)
	}
	for {
		if hasStop {
			if _, err := os.Stat(stopFile); err == nil {
				break
			}
		}
		if !time.Now().Before(end) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	closed = true
	return db.Close()
}

func cmdWait(args []string) error {
	v, ok := flagValue(args, "--secs")
	if !ok {
		return fmt.Errorf("--secs is required")
	}
	secs, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return err
	}
	time.Sleep(time.Duration(secs) * time.Second)
	return nil
}

func cmdCrash(args []string) int {
	code := 73
	if v, ok := flagValue(args, "--code"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "testnode: %v\n", err)
			return 2
		}
		code = n
	}
	return code
}
