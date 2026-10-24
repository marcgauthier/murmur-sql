// Package migrationcrash is the Go port of the Rust migration-crash live
// scenario (tests/migration_crash.rs), driven by tests-live/goharness.
//
// The scenario itself lives in migration_crash_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package migrationcrash
