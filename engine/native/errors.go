package native

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"

	"github.com/marcgauthier/murmur-sql-go/murmur"
	"github.com/marcgauthier/murmur-sql-go/murmur/replication"
	"github.com/marcgauthier/murmur-sql-go/engine"
)

func translate(err error) error {
	if err == nil {
		return nil
	}
	var own *engine.Error
	if errors.As(err, &own) {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &engine.Error{Code: "57014", Message: "query canceled", Cause: err}
	}
	code := "XX000"
	message := err.Error()
	var m *murmur.Error
	if errors.As(err, &m) {
		switch m.Kind {
		case murmur.KindWrongKey:
			code = "28000"
			message = "incorrect key"
		case murmur.KindBusy:
			code = "40001"
		case murmur.KindClosed:
			code = "08003"
		case murmur.KindNotFound:
			code = "02000"
		case murmur.KindInvalid:
			code = "22023"
		case murmur.KindCorrupt:
			code = "XX001"
		case murmur.KindLimit:
			code = "54000"
		case murmur.KindSchemaMismatch:
			code = "55000"
		case murmur.KindStorage, murmur.KindIo:
			code = "58000"
		case murmur.KindSql:
			code = sqliteState(m.Err)
		}
	} else {
		var r *replication.Error
		if errors.As(err, &r) {
			switch r.Kind {
			case replication.KindInvalid:
				code = "22023"
			case replication.KindAuth:
				code = "28000"
			case replication.KindOverloaded:
				code = "53400"
			case replication.KindUnavailable:
				code = "55000"
			case replication.KindStorage, replication.KindIo:
				code = "58000"
			default:
				code = "08006"
			}
		} else if errors.Is(err, os.ErrNotExist) {
			code = "58P01"
		} else if errors.Is(err, os.ErrExist) {
			code = "23505"
		} else {
			var pathError *os.PathError
			if errors.As(err, &pathError) {
				code = "58000"
			} else {
				code = sqliteState(err)
			}
		}
	}
	return &engine.Error{Code: code, Message: message, Cause: err}
}
func sqliteState(err error) string {
	// SQLite's concrete error lives in the engine's internal package. Read its
	// exported diagnostics without importing or forking that package.
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		v := reflect.ValueOf(cause)
		if v.Kind() == reflect.Pointer && !v.IsNil() {
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct {
			continue
		}
		code, extended := v.FieldByName("Code"), v.FieldByName("ExtCode")
		if !code.IsValid() || !extended.IsValid() || code.Kind() != reflect.Int || extended.Kind() != reflect.Int {
			continue
		}
		switch code.Int() {
		case 19:
			switch extended.Int() {
			case 1555, 2067:
				return "23505"
			case 1299:
				return "23502"
			case 787:
				return "23503"
			case 275:
				return "23514"
			}
			return "23000"
		case 23:
			return "42501"
		case 5, 6:
			return "55P03"
		case 8:
			return "25006"
		case 11:
			return "XX001"
		case 18:
			return "54000"
		case 10, 14, 13, 26:
			return "58000"
		case 20:
			return "42804"
		case 25:
			return "22023"
		case 9:
			return "57014"
		}
	}
	if err == nil {
		return "XX000"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "no such table"), strings.Contains(msg, "no such index"):
		return "42P01"
	case strings.Contains(msg, "no such column"):
		return "42703"
	case strings.Contains(msg, "no such function"):
		return "42883"
	case strings.Contains(msg, "primary-key component must be supplied"):
		return "23502"
	case strings.Contains(msg, "sqlite"):
		return "42601"
	}
	return "XX000"
}
