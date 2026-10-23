// Package cli keeps command parsing testable without os.Exit.
package cli

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/marcgauthier/murmur-sql-go/auth"
	"github.com/marcgauthier/murmur-sql-go/config"
	"github.com/marcgauthier/murmur-sql-go/engine"
	"github.com/marcgauthier/murmur-sql-go/logging"
	"github.com/marcgauthier/murmur-sql-go/server"
)

const ServerUsage = "usage: murmur-server [--config <path>] [--validate-config]\n       murmur-server /install | /uninstall\n"
const AdminUsage = `usage: murmur-admin <command> [options] [arguments]
commands:
  status <data-dir>
  inspect <data-dir>
  verify <data-dir> [--backup]
  backup <data-dir> <dest-dir>
  restore <backup-dir> <dest-dir>
  keys <data-dir>
  hash-password [--password <value>]  (reads stdin when omitted)
options:
  --json --passphrase <value> --key-hex <64-hex>
  --fresh-node-id[=<32-hex>] --fresh-db-id[=<32-hex>]
  -h, --help  -v, --version
`

func option(args []string, i *int, name string) (string, error) {
	a := args[*i]
	if _, v, ok := strings.Cut(a, "="); ok {
		return v, nil
	}
	if *i+1 >= len(args) {
		return "", fmt.Errorf("%s needs a value", name)
	}
	*i++
	return args[*i], nil
}
func Server(ctx context.Context, args []string, out, errOut io.Writer, f engine.Factory) int {
	path := "./config.yaml"
	validate := false
	for i := 0; i < len(args); i++ {
		name, _, _ := strings.Cut(args[i], "=")
		switch name {
		case "--config":
			v, e := option(args, &i, name)
			if e != nil {
				fmt.Fprintln(errOut, e)
				return 1
			}
			path = v
		case "--validate-config":
			validate = true
		case "-h", "--help":
			fmt.Fprint(out, ServerUsage)
			return 0
		case "-v", "--version":
			fmt.Fprintln(out, "murmur-server", server.Version)
			return 0
		default:
			fmt.Fprintf(errOut, "unknown argument %q\n%s", args[i], ServerUsage)
			return 1
		}
	}
	c, e := config.Load(path)
	if e != nil {
		fmt.Fprintln(errOut, "cannot load config:", e)
		return 1
	}
	if _, e = server.TLSConfig(c); e != nil {
		fmt.Fprintln(errOut, "invalid TLS configuration:", e)
		return 1
	}
	if e = c.ValidateCredentials(); e != nil {
		fmt.Fprintln(errOut, "invalid replication credentials:", e)
		return 1
	}
	if validator, ok := f.(engine.ConfigValidator); ok {
		if e = validator.ValidateConfig(c); e != nil {
			fmt.Fprintln(errOut, "invalid engine configuration:", e)
			return 1
		}
	}
	if validate {
		fmt.Fprintf(out, "config OK (bind %s, data %q)\n", c.Server.Bind, c.Storage.DataDir)
		return 0
	}
	logger, sink, e := logging.New(c.Logging)
	if e != nil {
		fmt.Fprintln(errOut, e)
		return 1
	}
	defer sink.Close()
	s, e := server.New(path, c, f, server.WithLogger(logger))
	if e != nil {
		fmt.Fprintln(errOut, e)
		return 1
	}
	e = s.ListenAndServe(ctx)
	if e != nil {
		fmt.Fprintln(errOut, e)
		var bind *server.BindError
		if errors.As(e, &bind) {
			return 2
		}
		return 1
	}
	return 0
}
func Admin(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer, f engine.Factory) int {
	if f == nil {
		f = engine.Unavailable{}
	}
	var positional []string
	jsonOut := false
	var passphrase, password, keyHex *string
	request := engine.AdminRequest{}
	for i := 0; i < len(args); i++ {
		name, value, equal := strings.Cut(args[i], "=")
		switch name {
		case "-h", "--help":
			fmt.Fprint(out, AdminUsage)
			return 0
		case "-v", "--version":
			fmt.Fprintln(out, "murmur-admin", server.Version)
			return 0
		case "--json":
			jsonOut = true
		case "--backup":
			request.Backup = true
		case "--password", "--passphrase", "--key-hex":
			v, e := option(args, &i, name)
			if e != nil {
				fmt.Fprintln(errOut, e)
				return 1
			}
			switch name {
			case "--password":
				password = &v
			case "--passphrase":
				passphrase = &v
			case "--key-hex":
				keyHex = &v
			}
		case "--fresh-node-id", "--fresh-db-id":
			var id [16]byte
			if equal {
				b, e := config.ParseHex(value, 16)
				if e != nil {
					fmt.Fprintln(errOut, e)
					return 1
				}
				copy(id[:], b)
			} else {
				b, e := randomID()
				if e != nil {
					fmt.Fprintln(errOut, e)
					return 1
				}
				id = b
			}
			if name == "--fresh-node-id" {
				request.FreshNodeID = &id
			} else {
				request.FreshDatabaseID = &id
			}
		default:
			if strings.HasPrefix(name, "-") {
				fmt.Fprintf(errOut, "unknown option %q\n", args[i])
				return 1
			}
			positional = append(positional, args[i])
		}
	}
	if len(positional) == 0 {
		fmt.Fprint(errOut, AdminUsage)
		return 1
	}
	op := positional[0]
	if op == "hash-password" {
		if len(positional) != 1 {
			fmt.Fprint(errOut, AdminUsage)
			return 1
		}
		var raw []byte
		if password != nil {
			raw = []byte(*password)
		} else {
			r := bufio.NewReader(io.LimitReader(in, 1<<20))
			line, e := r.ReadString('\n')
			if e != nil && e != io.EOF {
				fmt.Fprintln(errOut, e)
				return 1
			}
			raw = []byte(strings.TrimRight(line, "\r\n"))
		}
		defer clear(raw)
		hash, e := auth.HashPassword(raw)
		if e != nil {
			fmt.Fprintln(errOut, e)
			return 1
		}
		fmt.Fprintln(out, hash)
		return 0
	}
	n := 2
	switch op {
	case "status", "inspect", "verify", "keys":
	case "backup", "restore":
		n = 3
	default:
		fmt.Fprintf(errOut, "unknown command %q\n%s", op, AdminUsage)
		return 1
	}
	if len(positional) != n {
		fmt.Fprint(errOut, AdminUsage)
		return 1
	}
	request.Operation = op
	request.DataDir = positional[1]
	if n == 3 {
		request.Destination = positional[2]
	}
	if keyHex != nil {
		b, e := hex.DecodeString(strings.TrimSpace(*keyHex))
		if e != nil || len(b) != 32 {
			fmt.Fprintln(errOut, "key-hex must be exactly 32 bytes (64 hex characters)")
			return 1
		}
		copy(request.Key[:], b)
		clear(b)
	} else if passphrase != nil {
		request.Key = auth.ResolvePassphrase(*passphrase)
	} else {
		copy(request.Key[:], "0123456789abcdef0123456789abcdef")
		fmt.Fprintln(errOut, "warning: no --key-hex or --passphrase given; using the built-in development key")
	}
	defer clear(request.Key[:])
	result, e := f.Admin(ctx, request)
	if e != nil && result == nil {
		fmt.Fprintln(errOut, e)
		return 1
	}
	operationErr := e
	if jsonOut {
		if e = json.NewEncoder(out).Encode(result); e != nil {
			fmt.Fprintln(errOut, e)
			return 1
		}
	} else {
		switch v := result.(type) {
		case string:
			fmt.Fprintln(out, v)
		case fmt.Stringer:
			fmt.Fprintln(out, v.String())
		default:
			b, e := json.MarshalIndent(v, "", "  ")
			if e != nil {
				fmt.Fprintln(errOut, e)
				return 1
			}
			fmt.Fprintln(out, string(b))
		}
	}
	if operationErr != nil {
		fmt.Fprintln(errOut, operationErr)
		return 1
	}
	return 0
}
