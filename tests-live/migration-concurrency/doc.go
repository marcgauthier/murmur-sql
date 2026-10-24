// Package migrationconcurrency is the Go port of the Rust
// migration-concurrency live scenario (tests/migration_concurrency.rs),
// driven by tests-live/gosqlharness.
//
// The scenario itself lives in migration_concurrency_test.go; this file
// only gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package migrationconcurrency
