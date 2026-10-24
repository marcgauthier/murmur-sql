// Package soakslo is the Go port of the Rust soak-slo live scenario
// (tests/soak_slo.rs), driven by tests-live/goharness.
//
// The scenario itself lives in soak_slo_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package soakslo
