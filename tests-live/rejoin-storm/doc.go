// Package rejoinstorm is the Go port of the Rust rejoin-storm live scenario
// (tests/rejoin_storm.rs), driven by tests-live/goharness.
//
// The scenario itself lives in rejoin_storm_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package rejoinstorm
