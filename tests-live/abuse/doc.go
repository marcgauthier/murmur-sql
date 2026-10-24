// Package abuse is the Go port of the Rust abuse live scenario
// (tests/abuse.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in abuse_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package abuse
