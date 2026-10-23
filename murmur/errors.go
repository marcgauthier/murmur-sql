package murmur

import (
	"errors"
	"fmt"
)

// Kind classifies an Error, mirroring the Rust Error variants.
type Kind int

const (
	KindSql Kind = iota + 1
	KindIo
	KindStorage
	KindWrongKey
	KindInvalid
	KindCorrupt
	KindLimit
	KindSchemaMismatch
	KindBusy
	KindClosed
	KindCallbackPanicked
	KindNotFound
)

// Error reports a murmur failure with its kind, message, and cause.
type Error struct {
	Kind Kind
	Msg  string
	Err  error
}

func (e *Error) Error() string {
	switch e.Kind {
	case KindSql:
		return fmt.Sprintf("murmur: SQLite: %v", e.Err)
	case KindIo:
		return fmt.Sprintf("murmur: I/O: %v", e.Err)
	case KindStorage:
		return fmt.Sprintf("murmur: storage: %s", e.Msg)
	case KindWrongKey:
		return "murmur: incorrect encryption key"
	case KindInvalid:
		return fmt.Sprintf("murmur: invalid configuration or operation: %s", e.Msg)
	case KindCorrupt:
		return fmt.Sprintf("murmur: corrupt or unsupported storage: %s", e.Msg)
	case KindLimit:
		return fmt.Sprintf("murmur: resource limit: %s", e.Msg)
	case KindSchemaMismatch:
		return "murmur: database identity or schema history mismatch"
	case KindBusy:
		return "murmur: checkpoint queue full or merge baseline changed"
	case KindClosed:
		return "murmur: database is closed"
	case KindCallbackPanicked:
		return "murmur: SQL callback panicked; transaction rolled back"
	case KindNotFound:
		return fmt.Sprintf("murmur: no such row: %s", e.Msg)
	}
	return "murmur: unknown error"
}

// Unwrap returns the underlying cause, if any.
func (e *Error) Unwrap() error { return e.Err }

// Is matches errors by kind so callers can use errors.Is with the
// sentinels below or with another *Error of the same kind.
func (e *Error) Is(target error) bool {
	var t *Error
	if errors.As(target, &t) {
		return e.Kind == t.Kind
	}
	return false
}

func sqlErr(err error) *Error { return &Error{Kind: KindSql, Err: err} }
func ioErr(err error) *Error  { return &Error{Kind: KindIo, Err: err} }
func storageErr(format string, a ...any) *Error {
	return &Error{Kind: KindStorage, Msg: fmt.Sprintf(format, a...)}
}
func invalidErr(format string, a ...any) *Error {
	return &Error{Kind: KindInvalid, Msg: fmt.Sprintf(format, a...)}
}
func corruptErr(format string, a ...any) *Error {
	return &Error{Kind: KindCorrupt, Msg: fmt.Sprintf(format, a...)}
}
func limitErr(format string, a ...any) *Error {
	return &Error{Kind: KindLimit, Msg: fmt.Sprintf(format, a...)}
}
func notFoundErr(format string, a ...any) *Error {
	return &Error{Kind: KindNotFound, Msg: fmt.Sprintf(format, a...)}
}

// Sentinel errors for errors.Is matching.
var (
	ErrWrongKey         = &Error{Kind: KindWrongKey}
	ErrSchemaMismatch   = &Error{Kind: KindSchemaMismatch}
	ErrBusy             = &Error{Kind: KindBusy}
	ErrClosed           = &Error{Kind: KindClosed}
	ErrCallbackPanicked = &Error{Kind: KindCallbackPanicked}
	ErrNotFound         = &Error{Kind: KindNotFound}
)
