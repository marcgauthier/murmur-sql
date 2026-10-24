// Package pauseresume is the Go port of the Rust pause-resume live
// scenario (tests/pause_resume.rs), driven by tests-live/goharness.
//
// The scenario itself lives in pause_resume_test.go; this file only gives
// `go build ./...` a non-test file (Go reports test-only directories as
// build failures).
package pauseresume
