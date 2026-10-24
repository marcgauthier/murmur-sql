// Package deletepruning is the Go port of the Rust delete-pruning live
// scenario (tests/delete_pruning.rs), driven by tests-live/goharness.
//
// The scenario itself lives in delete_pruning_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package deletepruning
