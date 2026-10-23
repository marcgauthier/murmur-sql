//go:build linux

package service

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/marcgauthier/murmur-sql/config"
)

const unitName = "murmur-sql.service"
const unitPath = "/etc/systemd/system/" + unitName

func recordPath() string         { return "/etc/murmur-sql/install.json" }
func defaults() (string, string) { return "/opt/murmur-sql", "/var/lib/murmur-sql" }
func protectRecord() error       { return os.Chmod(filepath.Dir(recordPath()), 0700) }
func preflight() error {
	if os.Geteuid() != 0 {
		return errors.New("service installation requires root")
	}
	if _, e := os.Stat("/run/systemd/system"); e != nil {
		return errors.New("service installation requires a running systemd manager")
	}
	return nil
}
func ctl(ctx context.Context, args ...string) error {
	return checked(ctx, "/usr/bin/systemctl", args...)
}
func status(ctx context.Context, property string) (string, error) {
	out, e := exec.CommandContext(ctx, "/usr/bin/systemctl", "show", unitName, "--property="+property, "--value").CombinedOutput()
	if e != nil {
		return "", fmt.Errorf("systemctl: %w: %s", e, out)
	}
	return strings.TrimSpace(string(out)), nil
}
func registered(ctx context.Context) (bool, error) {
	s, e := status(ctx, "LoadState")
	return s != "not-found" && s != "", e
}
func running(ctx context.Context) (bool, error) {
	s, e := status(ctx, "ActiveState")
	return s == "active" || s == "activating" || s == "reloading" || s == "deactivating", e
}
func stop(ctx context.Context) error  { return ctl(ctx, "stop", unitName) }
func start(ctx context.Context) error { return ctl(ctx, "start", unitName) }
func identity(ctx context.Context) error {
	if exec.CommandContext(ctx, "/usr/bin/id", "-u", "murmur").Run() == nil {
		return nil
	}
	shell := "/usr/sbin/nologin"
	if _, e := os.Stat(shell); e != nil {
		shell = "/sbin/nologin"
	}
	return checked(ctx, "/usr/sbin/useradd", "--system", "--user-group", "--no-create-home", "--shell", shell, "murmur")
}
func systemdQuote(s string) (string, error) {
	if strings.ContainsAny(s, "\r\n\x00") {
		return "", errors.New("invalid systemd path")
	}
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, `%`, `%%`).Replace(s) + `"`, nil
}
func unit(i installation) (string, error) {
	exe, e := systemdQuote(i.executable())
	if e != nil {
		return "", e
	}
	cfg, e := systemdQuote(i.config())
	if e != nil {
		return "", e
	}
	dir, e := systemdQuote(i.Install)
	if e != nil {
		return "", e
	}
	return fmt.Sprintf("[Unit]\nDescription=Murmur SQL server\nAfter=network.target\n\n[Service]\nType=simple\nUser=murmur\nWorkingDirectory=%s\nExecStart=:%s --config %s\nRestart=on-failure\nRestartSec=60s\nTimeoutStopSec=120s\nKillSignal=SIGTERM\nUMask=0077\nNoNewPrivileges=true\n\n[Install]\nWantedBy=multi-user.target\n", dir, exe, cfg), nil
}
func register(ctx context.Context, i installation) error {
	body, e := unit(i)
	if e != nil {
		return e
	}
	if e = atomicWrite(unitPath, []byte(body), 0644); e != nil {
		return e
	}
	if e = ctl(ctx, "daemon-reload"); e != nil {
		return e
	}
	return ctl(ctx, "enable", unitName)
}
func unregister(ctx context.Context) error {
	if e := ctl(ctx, "disable", unitName); e != nil {
		return e
	}
	if e := os.Remove(unitPath); e != nil && !errors.Is(e, fs.ErrNotExist) {
		return e
	}
	return ctl(ctx, "daemon-reload")
}
func permissions(ctx context.Context, i installation, c config.Config) error {
	for _, p := range []string{i.Install, i.Runtime} {
		if e := os.Chmod(p, 0755); e != nil {
			return e
		}
	}
	for _, p := range writable(i, c) {
		if e := os.MkdirAll(p, 0700); e != nil {
			return e
		}
		if e := filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.Type()&os.ModeSymlink != 0 {
				return errors.New("symlink in service writable tree")
			}
			mode := fs.FileMode(0600)
			if d.IsDir() {
				mode = 0700
			}
			return os.Chmod(path, mode)
		}); e != nil {
			return e
		}
		if e := checked(ctx, "/usr/bin/chown", "-R", "murmur", p); e != nil {
			return e
		}
	}
	for _, p := range credentials(c) {
		if !filepath.IsAbs(p) {
			p = filepath.Join(i.Install, p)
		}
		info, e := os.Lstat(p)
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return errors.New("credential must be a regular file")
		}
		if e = checked(ctx, "/usr/bin/chgrp", "murmur", p); e != nil {
			return e
		}
		if e = os.Chmod(p, 0640); e != nil {
			return e
		}
	}
	// The engine creates and atomically swaps the storage directory itself.
	// Its parent must permit service-owned directory entries, without recursively
	// changing ownership of the runtime root or unrelated sibling directories.
	storageParent := filepath.Dir(c.Storage.DataDir)
	if e := checked(ctx, "/usr/bin/chown", "root:murmur", storageParent); e != nil {
		return e
	}
	if e := os.Chmod(storageParent, 0770); e != nil {
		return e
	}
	// Never grant recursive ownership outside configured writable trees.
	for _, p := range []string{i.Install, i.executable()} {
		if e := checked(ctx, "/usr/bin/chown", "root:root", p); e != nil {
			return e
		}
	}
	return nil
}
func Host(ctx context.Context, run func(context.Context) int) (bool, int) { return false, 0 }
func validateAncestors(p string) error {
	for q := p; ; q = filepath.Dir(q) {
		info, e := os.Stat(q)
		if e != nil {
			if !errors.Is(e, fs.ErrNotExist) {
				return e
			}
		} else {
			st, ok := info.Sys().(*syscall.Stat_t)
			if !ok || st.Uid != 0 || info.Mode().Perm()&0022 != 0 && info.Mode()&os.ModeSticky == 0 {
				return fmt.Errorf("service ancestor must be root-owned and protected from writes: %s", q)
			}
		}
		if q == filepath.Dir(q) {
			break
		}
	}
	return nil
}
