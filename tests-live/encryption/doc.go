// Package encryption is the Go port of the Rust encryption live scenario
// (tests/encryption.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in encryption_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package encryption
