// Package dosclient is the Go port of the Rust dos-client live scenario
// (tests/dos_client.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in dos_client_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package dosclient
