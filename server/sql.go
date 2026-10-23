package server

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/marcgauthier/murmur-sql/engine"
)

func sqlError(code, msg string) error { return &engine.Error{Code: code, Message: msg} }
func syntax(msg string) error         { return sqlError("42601", msg) }

// scanSQL keeps quoted strings, identifiers and comments opaque. Its callback
// visits only SQL syntax, preventing parameter substitution inside literals.
func scanSQL(s string, visit func(int) (int, error)) error {
	for i := 0; i < len(s); {
		switch {
		case i+1 < len(s) && s[i:i+2] == "--":
			j := strings.IndexByte(s[i:], '\n')
			if j < 0 {
				return nil
			}
			i += j + 1
		case i+1 < len(s) && s[i:i+2] == "/*":
			j := strings.Index(s[i+2:], "*/")
			if j < 0 {
				return syntax("unterminated SQL comment")
			}
			i += j + 4
		case s[i] == '\'' || s[i] == '"' || s[i] == '`' || s[i] == '[':
			q := s[i]
			end := q
			if q == '[' {
				end = ']'
			}
			i++
			closed := false
			for i < len(s) {
				if s[i] == end {
					if q != '[' && i+1 < len(s) && s[i+1] == end {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return syntax("unterminated quoted SQL value")
			}
		default:
			n, e := visit(i)
			if e != nil {
				return e
			}
			if n <= i {
				n = i + 1
			}
			i = n
		}
	}
	return nil
}
func SplitStatements(sql string) ([]string, error) {
	var out []string
	start := 0
	e := scanSQL(sql, func(i int) (int, error) {
		if sql[i] == ';' {
			s := strings.TrimSpace(sql[start:i])
			if stripTrivia(s) != "" {
				out = append(out, s)
			}
			start = i + 1
		}
		return i + 1, nil
	})
	if e != nil {
		return nil, e
	}
	s := strings.TrimSpace(sql[start:])
	if stripTrivia(s) != "" {
		out = append(out, s)
	}
	return out, nil
}
func RewriteParameters(sql string) (string, int, error) {
	var b strings.Builder
	last, max := 0, 0
	e := scanSQL(sql, func(i int) (int, error) {
		if sql[i] == '$' && i+1 < len(sql) && sql[i+1] >= '0' && sql[i+1] <= '9' {
			j := i + 1
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				j++
			}
			n, e := strconv.Atoi(sql[i+1 : j])
			if e != nil || n == 0 || n > 65535 {
				return 0, sqlError("22023", "parameter index must be between 1 and 65535")
			}
			if n > max {
				max = n
			}
			b.WriteString(sql[last:i])
			b.WriteByte('?')
			b.WriteString(sql[i+1 : j])
			last = j
			return j, nil
		}
		return i + 1, nil
	})
	if e != nil {
		return "", 0, e
	}
	b.WriteString(sql[last:])
	return b.String(), max, nil
}
func stripTrivia(s string) string {
	for {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(s, "--") {
			i := strings.IndexByte(s, '\n')
			if i < 0 {
				return ""
			}
			s = s[i+1:]
			continue
		}
		if strings.HasPrefix(s, "/*") {
			i := strings.Index(s[2:], "*/")
			if i < 0 {
				return ""
			}
			s = s[i+4:]
			continue
		}
		return s
	}
}
func firstWord(s string) string {
	s = stripTrivia(s)
	i := 0
	for i < len(s) && (s[i] >= 'a' && s[i] <= 'z' || s[i] >= 'A' && s[i] <= 'Z' || s[i] == '_') {
		i++
	}
	return strings.ToUpper(s[:i])
}

type token struct {
	value  string
	quoted bool
	param  int
}

func tokenize(s string) ([]token, error) {
	var out []token
	s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), ";"))
	for i := 0; i < len(s); {
		if unicode.IsSpace(rune(s[i])) {
			i++
			continue
		}
		if s[i] == ',' {
			out = append(out, token{value: ","})
			i++
			continue
		}
		q := s[i] == '\''
		if q {
			i++
			var b strings.Builder
			closed := false
			for i < len(s) {
				if s[i] == '\'' {
					i++
					if i < len(s) && s[i] == '\'' {
						b.WriteByte('\'')
						i++
						continue
					}
					closed = true
					break
				}
				b.WriteByte(s[i])
				i++
			}
			if !closed {
				return nil, syntax("unterminated command string")
			}
			out = append(out, token{value: b.String(), quoted: true})
			continue
		}
		start := i
		for i < len(s) && !unicode.IsSpace(rune(s[i])) && s[i] != ',' {
			i++
		}
		v := s[start:i]
		t := token{value: v}
		if strings.HasPrefix(v, "$") {
			n, e := strconv.Atoi(v[1:])
			if e != nil || n < 1 || n > 65535 {
				return nil, syntax("invalid command parameter")
			}
			t.param = n
		}
		out = append(out, t)
	}
	return out, nil
}
func boolean(s string) (bool, error) {
	switch strings.ToLower(s) {
	case "true", "1", "on", "yes":
		return true, nil
	case "false", "0", "off", "no":
		return false, nil
	}
	return false, sqlError("22023", fmt.Sprintf("expected true or false, got %q", s))
}
