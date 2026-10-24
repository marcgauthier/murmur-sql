// Package apimtls is the Go port of the Rust api-mtls live scenario
// (tests/api_mtls.rs), driven by tests-live/gosqlharness.
//
// The scenario itself lives in api_mtls_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package apimtls
