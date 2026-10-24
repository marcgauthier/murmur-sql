// Package filepermissions is the Go port of the Rust file-permissions live
// scenario (tests/file_permissions.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in file_permissions_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package filepermissions
