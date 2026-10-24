// Package dbidisolation is the Go port of the Rust dbid-isolation live
// scenario (tests/dbid_isolation.rs), driven by
// tests-live/gosqlharness.
//
// The scenario itself lives in dbid_isolation_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package dbidisolation
