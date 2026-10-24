// Package mergepolicies is the Go port of the Rust merge-policies live
// scenario (tests/merge_policies.rs), driven by tests-live/goharness.
//
// The scenario itself lives in merge_policies_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only directories
// as build failures).
package mergepolicies
