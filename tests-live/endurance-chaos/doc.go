// Package endurancechaos is the Go port of the Rust endurance-chaos
// live scenario (tests/endurance_chaos.rs), driven by
// tests-live/goharness.
//
// The scenario itself lives in endurance_chaos_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package endurancechaos
