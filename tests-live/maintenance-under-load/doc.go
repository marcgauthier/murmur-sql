// Package maintenanceunderload is the Go port of the Rust
// maintenance-under-load live scenario (tests/maintenance_under_load.rs),
// driven by tests-live/goharness.
//
// The scenario itself lives in maintenance_under_load_test.go; this file
// only gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package maintenanceunderload
