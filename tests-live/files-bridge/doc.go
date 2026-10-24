// Package filesbridge is the Go port of the Rust files-bridge live
// scenario (tests/files_bridge.rs), driven by tests-live/goharness.
//
// The scenario itself lives in files_bridge_test.go; this file only
// gives `go build ./...` a non-test file (Go reports test-only
// directories as build failures).
package filesbridge
