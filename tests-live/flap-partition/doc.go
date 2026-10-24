// Package flappartition is the Go port of the Rust flap-partition live
// scenario (tests/flap_partition.rs), driven by tests-live/goharness.
//
// The scenario itself lives in flap_partition_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package flappartition
