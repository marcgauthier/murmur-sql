// Package backupunderfire is the Go port of the Rust backup-under-fire
// live scenario (tests/backup_under_fire.rs), driven by
// tests-live/gosqlharness.
//
// The scenario itself lives in backup_under_fire_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package backupunderfire
