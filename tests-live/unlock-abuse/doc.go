// Package unlockabuse is the Go port of the Rust unlock-abuse live scenario
// (tests/unlock_abuse.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in unlock_abuse_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package unlockabuse
