// Package churnretirement is the Go port of the Rust churn-retirement
// live scenario (tests/churn_retirement.rs), driven by
// tests-live/goharness.
//
// The scenario itself lives in churn_retirement_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package churnretirement
