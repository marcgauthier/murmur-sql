// Package overloadbudgets is the Go port of the Rust overload-budgets
// live scenario (tests/overload_budgets.rs), driven by
// tests-live/goharness.
//
// The scenario itself lives in overload_budgets_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package overloadbudgets
