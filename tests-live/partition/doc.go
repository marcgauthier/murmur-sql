// Package partition is the Go port of the Rust partition live scenario
// (tests/partition.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in partition_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package partition
