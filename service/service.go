// Package service owns interactive OS deployment, never database storage.
package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/marcgauthier/murmur-sql/config"
)

type installation struct {
	Install string `json:"install"`
	Runtime string `json:"runtime"`
	Admin   bool   `json:"admin"`
}

func (i installation) executable() string { return filepath.Join(i.Install, "murmur-server"+suffix()) }
func (i installation) config() string     { return filepath.Join(i.Runtime, "config", "config.yaml") }
func suffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}
func within(base, p string) bool {
	if runtime.GOOS == "windows" {
		base = strings.ToLower(base)
		p = strings.ToLower(p)
	}
	r, e := filepath.Rel(base, p)
	return e == nil && (r == "." || r != ".." && !strings.HasPrefix(r, ".."+string(filepath.Separator)) && !filepath.IsAbs(r))
}
func safeDirectory(p string) error {
	if !filepath.IsAbs(p) || filepath.Clean(p) != p || filepath.Dir(p) == p || strings.ContainsAny(p, "\r\n\x00") {
		return errors.New("use a clean absolute directory path below a filesystem root")
	}
	for q := p; ; q = filepath.Dir(q) {
		i, e := os.Lstat(q)
		if e == nil {
			if i.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("symlink in service path: %s", q)
			}
			if !i.IsDir() {
				return fmt.Errorf("not a directory: %s", q)
			}
		} else if !errors.Is(e, fs.ErrNotExist) {
			return e
		}
		if q == filepath.Dir(q) {
			break
		}
	}
	return nil
}
func (i installation) validate() error {
	for _, p := range []string{i.Install, i.Runtime} {
		if e := safeDirectory(p); e != nil {
			return e
		}
		protected := p
		if p == i.Runtime {
			protected = filepath.Dir(p)
		}
		if e := validateAncestors(protected); e != nil {
			return e
		}
	}
	if within(i.Install, i.Runtime) || within(i.Runtime, i.Install) {
		return errors.New("installation and runtime directories must not overlap")
	}
	metadata := filepath.Dir(recordPath())
	if within(metadata, i.Install) || within(i.Install, metadata) || within(metadata, i.Runtime) || within(i.Runtime, metadata) {
		return errors.New("service paths overlap installer metadata")
	}
	return nil
}

type console struct {
	in  *bufio.Reader
	out io.Writer
}

func (c console) ask(prompt string) (string, error) {
	fmt.Fprint(c.out, prompt+" ")
	s, e := c.in.ReadString('\n')
	if e != nil {
		return "", errors.New("input closed; installation cancelled")
	}
	return strings.TrimSpace(s), nil
}
func (c console) yes(prompt string, def bool) (bool, error) {
	for {
		s, e := c.ask(prompt)
		if e != nil {
			return false, e
		}
		switch strings.ToLower(s) {
		case "":
			return def, nil
		case "y", "yes":
			return true, nil
		case "n", "no":
			return false, nil
		}
		fmt.Fprintln(c.out, "Enter yes or no.")
	}
}
func atomicWrite(path string, b []byte, mode fs.FileMode) error {
	if info, e := os.Lstat(path); e == nil && info.Mode()&os.ModeSymlink != 0 {
		return errors.New("refusing to replace a symlink")
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".murmur-install-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if e = f.Chmod(mode); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	return os.Rename(f.Name(), path)
}
func checked(ctx context.Context, name string, args ...string) error {
	out, e := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if e != nil {
		return fmt.Errorf("%s: %w: %s", name, e, strings.TrimSpace(string(out)))
	}
	return nil
}
func loadRecord() (installation, error) {
	var i installation
	path := recordPath()
	info, e := os.Lstat(path)
	if e != nil {
		return i, e
	}
	if !info.Mode().IsRegular() {
		return i, errors.New("invalid installer metadata")
	}
	b, e := os.ReadFile(path)
	if e != nil {
		return i, e
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e = d.Decode(&i); e != nil {
		return i, e
	}
	return i, i.validate()
}
func writeRecord(i installation) error {
	p := recordPath()
	if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
		return e
	}
	b, e := json.MarshalIndent(i, "", "  ")
	if e != nil {
		return e
	}
	if e = atomicWrite(p, b, 0600); e != nil {
		return e
	}
	return protectRecord()
}

// Dispatch handles /install and /uninstall alone. Prompts are part of the
// installed product and are not executed by building or testing this package.
func Dispatch(ctx context.Context, args []string, in io.Reader, out io.Writer) (bool, error) {
	has := false
	for _, a := range args {
		if a == "/install" || a == "/uninstall" {
			has = true
		}
	}
	if !has {
		return false, nil
	}
	if len(args) != 1 {
		return true, errors.New("/install and /uninstall must be used alone")
	}
	if e := preflight(); e != nil {
		return true, e
	}
	logName := "murmur-service-" + time.Now().UTC().Format("20060102-150405") + ".log"
	logFile, e := os.OpenFile(logName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return true, e
	}
	defer logFile.Close()
	c := console{bufio.NewReader(in), io.MultiWriter(out, logFile)}
	fmt.Fprintln(c.out, "Service deployment transcript:", logName)
	e = deploy(ctx, args[0], c)
	if e != nil {
		fmt.Fprintln(c.out, "Deployment failed:", e)
	}
	logFile.Sync()
	return true, e
}
func deploy(ctx context.Context, action string, c console) error {
	old, e := loadRecord()
	managed := e == nil
	if e != nil && !errors.Is(e, fs.ErrNotExist) {
		return e
	}
	exists, e := registered(ctx)
	if e != nil {
		return e
	}
	if exists && !managed {
		return errors.New("refusing to replace an unmanaged service")
	}
	if action == "/uninstall" {
		if !managed {
			return errors.New("no managed installation found")
		}
		yes, e := c.yes("Stop and remove the managed service? [y/N]", false)
		if e != nil || !yes {
			return e
		}
		if exists {
			if e = stop(ctx); e != nil {
				return e
			}
			if e = unregister(ctx); e != nil {
				return e
			}
		}
		for _, entry := range []struct{ p, label string }{{old.Install, "installed binaries"}, {old.Runtime, "configuration, data, logs and backups"}} {
			yes, e = c.yes("Delete "+entry.label+" in "+entry.p+"? [y/N]", false)
			if e != nil {
				return e
			}
			if yes {
				if e = safeDirectory(entry.p); e != nil {
					return e
				}
				if e = os.RemoveAll(entry.p); e != nil {
					return e
				}
			}
		}
		return os.Remove(recordPath())
	}
	installDir, runtimeDir := defaults()
	if managed {
		installDir, runtimeDir = old.Install, old.Runtime
	}
	var v string
	if v, e = c.ask("Installation directory [" + installDir + "]:"); e != nil {
		return e
	}
	if v != "" {
		installDir = v
	}
	if v, e = c.ask("Data/runtime directory [" + runtimeDir + "]:"); e != nil {
		return e
	}
	if v != "" {
		runtimeDir = v
	}
	i := installation{Install: installDir, Runtime: runtimeDir, Admin: old.Admin}
	if e = i.validate(); e != nil {
		return e
	}
	if managed && (i.Install != old.Install || i.Runtime != old.Runtime) {
		return errors.New("upgrades preserve installation and runtime paths")
	}
	if !managed {
		for _, p := range []string{i.Install, i.Runtime} {
			entries, e := os.ReadDir(p)
			if e != nil && !errors.Is(e, fs.ErrNotExist) {
				return e
			}
			if len(entries) > 0 {
				return errors.New("fresh installation requires empty directories")
			}
		}
	}
	var raw []byte
	var cfg config.Config
	if managed {
		if e = safeDirectory(filepath.Dir(i.config())); e != nil {
			return e
		}
		info, err := os.Lstat(i.config())
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("configuration must be a regular file")
		}
		raw, e = os.ReadFile(i.config())
		if e != nil {
			return e
		}
		cfg, e = config.Parse(raw)
	} else {
		bind := "127.0.0.1:5432"
		v, e = c.ask("SQL listen address [" + bind + "]:")
		if e != nil {
			return e
		}
		if v != "" {
			bind = v
		}
		raw = []byte(fmt.Sprintf("[server]\nbind = %q\n[storage]\ndata_dir = %q\n[logging]\ndir = %q\n[backups]\ndir = %q\n", bind, filepath.Join(i.Runtime, "data"), filepath.Join(i.Runtime, "logs"), filepath.Join(i.Runtime, "backups")))
		cfg, e = config.Parse(raw)
	}
	if e != nil {
		return e
	}
	if e = validateWritable(i, cfg); e != nil {
		return e
	}
	fmt.Fprintf(c.out, "Executable: %s\nConfiguration: %s\nSQL listener: %s\n", i.executable(), i.config(), cfg.Server.Bind)
	yes, e := c.yes("Install/upgrade this service? [y/N]", false)
	if e != nil || !yes {
		return e
	}
	wasRunning := false
	if exists {
		wasRunning, e = running(ctx)
		if e != nil {
			return e
		}
		if wasRunning {
			if e = stop(ctx); e != nil {
				return e
			}
		}
	}
	for _, p := range []string{i.Install, i.Runtime, filepath.Dir(i.config())} {
		if e = os.MkdirAll(p, 0755); e != nil {
			return e
		}
	}
	if e = identity(ctx); e != nil {
		return e
	}
	if !managed {
		if e = atomicWrite(i.config(), raw, 0600); e != nil {
			return e
		}
	}
	source, e := os.Executable()
	if e != nil {
		return e
	}
	source, e = filepath.EvalSymlinks(source)
	if e != nil {
		return e
	}
	type replacement struct {
		path     string
		previous []byte
		existed  bool
	}
	var replacements []replacement
	rollback := func() {
		for n := len(replacements) - 1; n >= 0; n-- {
			r := replacements[n]
			if r.existed {
				atomicWrite(r.path, r.previous, 0755)
			} else {
				os.Remove(r.path)
			}
		}
		if managed {
			register(ctx, old)
			if wasRunning {
				start(ctx)
			}
		} else if ok, _ := registered(ctx); ok {
			unregister(ctx)
		}
	}
	for _, name := range []string{"murmur-server", "murmur-admin"} {
		src := source
		if name == "murmur-admin" {
			src = filepath.Join(filepath.Dir(source), name+suffix())
		}
		b, e := os.ReadFile(src)
		if name == "murmur-admin" && errors.Is(e, fs.ErrNotExist) {
			fmt.Fprintln(c.out, "murmur-admin not beside server; preserving existing admin binary.")
			continue
		}
		if e != nil {
			rollback()
			return e
		}
		dest := filepath.Join(i.Install, name+suffix())
		oldBytes, e := os.ReadFile(dest)
		if e != nil && !errors.Is(e, fs.ErrNotExist) {
			rollback()
			return e
		}
		replacements = append(replacements, replacement{dest, oldBytes, e == nil})
		if e = atomicWrite(dest, b, 0755); e != nil {
			rollback()
			return e
		}
		if name == "murmur-admin" {
			i.Admin = true
		}
	}
	if e = register(ctx, i); e == nil {
		e = permissions(ctx, i, cfg)
	}
	if e == nil {
		e = checked(ctx, i.executable(), "--config", i.config(), "--validate-config")
	}
	if e == nil {
		e = writeRecord(i)
	}
	if e != nil {
		rollback()
		return e
	}
	yes, e = c.yes("Start the service now? [Y/n]", true)
	if e != nil {
		return e
	}
	if yes || wasRunning {
		if e = start(ctx); e != nil {
			rollback()
			return e
		}
		if e = waitListener(ctx, cfg.Server.Bind); e != nil {
			return e
		}
	}
	fmt.Fprintln(c.out, "Completed. Firewall unchanged. Add auth.users to config.yaml before connecting.")
	return nil
}
func validateWritable(i installation, c config.Config) error {
	paths := []string{c.Storage.DataDir, c.Logging.Dir, c.Backups.Dir}
	if c.Replication.Files != nil {
		paths = append(paths, c.Replication.Files.Directory)
	}
	if c.Replication.Bridge != nil {
		paths = append(paths, c.Replication.Bridge.Directory)
	}
	for _, p := range paths {
		if !filepath.IsAbs(p) || !within(i.Runtime, p) || p == i.Runtime {
			return errors.New("service writable paths must be absolute subdirectories of runtime")
		}
		if within(p, filepath.Dir(i.config())) || within(filepath.Dir(i.config()), p) {
			return errors.New("data paths must not overlap configuration")
		}
		if e := safeDirectory(p); e != nil {
			return e
		}
	}
	return nil
}
func writable(i installation, c config.Config) []string {
	paths := []string{filepath.Dir(i.config()), c.Storage.DataDir, c.Logging.Dir, c.Backups.Dir}
	if c.Replication.Files != nil {
		paths = append(paths, c.Replication.Files.Directory)
	}
	if c.Replication.Bridge != nil {
		paths = append(paths, c.Replication.Bridge.Directory)
	}
	return paths
}
func waitListener(ctx context.Context, addr string) error {
	host, port, e := net.SplitHostPort(addr)
	if e != nil {
		return e
	}
	if host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	if host == "::" {
		host = "::1"
	}
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		d := net.Dialer{Timeout: 500 * time.Millisecond}
		conn, e := d.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
		if e == nil {
			conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("service SQL listener did not become ready")
		case <-ticker.C:
		}
	}
}

func credentials(c config.Config) []string {
	var paths []string
	for _, p := range []*string{c.Server.TLS.CertFile, c.Server.TLS.KeyFile, c.Server.TLS.CaFile, c.Replication.CertChainFile, c.Replication.PrivateKeyFile, c.Replication.CaFile, c.Replication.SigningKeyFile} {
		if p != nil && *p != "" {
			paths = append(paths, *p)
		}
	}
	return paths
}
