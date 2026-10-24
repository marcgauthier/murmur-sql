// Package corruptsnapshot is the Go port of the Rust corrupt-snapshot live
// scenario (tests/corrupt_snapshot.rs), driven by tests-live/goharness.
//
// The scenario itself lives in corrupt_snapshot_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package corruptsnapshot
