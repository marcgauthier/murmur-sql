// Package tamperedbackup is the Go port of the Rust tampered-backup live
// scenario (tests/tampered_backup.rs), driven by
// tests-live/gosqlharness.
//
// The scenario itself lives in tampered_backup_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package tamperedbackup
