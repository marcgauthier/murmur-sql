// Package tailrepair is the Go port of the Rust tail-repair live scenario
// (tests/tail_repair.rs), driven by tests-live/goharness.
//
// The scenario itself lives in tail_repair_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package tailrepair
