// Package snapshotresync is the Go port of the Rust snapshot-resync
// live scenario (tests/snapshot_resync.rs), driven by
// tests-live/goharness.
//
// The scenario itself lives in snapshot_resync_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package snapshotresync
