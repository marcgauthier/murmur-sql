// Package versionskew is the Go port of the Rust version-skew live
// scenario (tests/version_skew.rs), driven by
// tests-live/gosqlharness.
//
// The scenario itself lives in version_skew_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package versionskew
