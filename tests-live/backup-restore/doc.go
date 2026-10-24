// Package backuprestore is the Go port of the Rust backup-restore live
// scenario (tests/backup_restore.rs), driven by
// tests-live/gosqlharness.
//
// The scenario itself lives in backup_restore_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package backuprestore
