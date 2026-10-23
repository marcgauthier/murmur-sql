//go:build windows

package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/marcgauthier/murmur-sql-go/config"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

const serviceName = "MurmurSQL"

func envDefault(k, v string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return v
}
func recordPath() string {
	return filepath.Join(envDefault("ProgramData", `C:\ProgramData`), "MurmurSQL-Installer", "install.json")
}
func defaults() (string, string) {
	return filepath.Join(envDefault("ProgramFiles", `C:\Program Files`), serviceName), filepath.Join(envDefault("ProgramData", `C:\ProgramData`), serviceName)
}
func preflight() error {
	sid, e := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if e != nil {
		return e
	}
	ok, e := windows.Token(0).IsMember(sid)
	if e != nil {
		return e
	}
	if !ok {
		return errors.New("run service installation in an elevated Administrator console")
	}
	return nil
}
func protectRecord() error { return acl(context.Background(), filepath.Dir(recordPath()), false) }
func withService(fn func(*mgr.Service) error) error {
	m, e := mgr.Connect()
	if e != nil {
		return e
	}
	defer m.Disconnect()
	s, e := m.OpenService(serviceName)
	if e != nil {
		return e
	}
	defer s.Close()
	return fn(s)
}
func registered(context.Context) (bool, error) {
	e := withService(func(*mgr.Service) error { return nil })
	if errors.Is(e, windows.ERROR_SERVICE_DOES_NOT_EXIST) {
		return false, nil
	}
	return e == nil, e
}
func running(context.Context) (bool, error) {
	var state svc.State
	e := withService(func(s *mgr.Service) error { v, e := s.Query(); state = v.State; return e })
	return state != svc.Stopped, e
}
func stop(ctx context.Context) error {
	return withService(func(s *mgr.Service) error {
		st, e := s.Query()
		if e != nil {
			return e
		}
		if st.State == svc.Stopped {
			return nil
		}
		if _, e = s.Control(svc.Stop); e != nil {
			return e
		}
		timer := time.NewTimer(120 * time.Second)
		defer timer.Stop()
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			st, e = s.Query()
			if e != nil {
				return e
			}
			if st.State == svc.Stopped {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return errors.New("service stop timed out")
			case <-tick.C:
			}
		}
	})
}
func start(ctx context.Context) error {
	return withService(func(s *mgr.Service) error { return s.Start() })
}
func identity(context.Context) error { return nil }
func register(ctx context.Context, i installation) error {
	m, e := mgr.Connect()
	if e != nil {
		return e
	}
	defer m.Disconnect()
	cfg := mgr.Config{DisplayName: "Murmur SQL server", Description: "PostgreSQL-wire shell over MURMUR-GO", StartType: mgr.StartAutomatic, ServiceStartName: `NT SERVICE\MurmurSQL`, SidType: windows.SERVICE_SID_TYPE_UNRESTRICTED}
	s, e := m.OpenService(serviceName)
	if errors.Is(e, windows.ERROR_SERVICE_DOES_NOT_EXIST) {
		s, e = m.CreateService(serviceName, i.executable(), cfg, "--config", i.config())
	} else if e == nil {
		cfg.BinaryPathName = windows.ComposeCommandLine([]string{i.executable(), "--config", i.config()})
		e = s.UpdateConfig(cfg)
	}
	if e != nil {
		if s != nil {
			s.Close()
		}
		return e
	}
	defer s.Close()
	return s.SetRecoveryActions([]mgr.RecoveryAction{{Type: mgr.ServiceRestart, Delay: 60 * time.Second}}, 86400)
}
func unregister(ctx context.Context) error {
	return withService(func(s *mgr.Service) error { return s.Delete() })
}
func acl(ctx context.Context, path string, writable bool) error {
	args := []string{path, "/inheritance:r", "/grant:r", "*S-1-5-18:(OI)(CI)F", "*S-1-5-32-544:(OI)(CI)F"}
	if writable {
		args = append(args, `NT SERVICE\MurmurSQL:(OI)(CI)M`)
	}
	return checked(ctx, "icacls.exe", args...)
}
func permissions(ctx context.Context, i installation, c config.Config) error {
	if e := acl(ctx, i.Install, false); e != nil {
		return e
	}
	if e := checked(ctx, "icacls.exe", i.Install, "/grant", `NT SERVICE\MurmurSQL:(OI)(CI)RX`); e != nil {
		return e
	}
	if e := acl(ctx, i.Runtime, false); e != nil {
		return e
	}
	if e := checked(ctx, "icacls.exe", i.Runtime, "/grant", `NT SERVICE\MurmurSQL:RX`); e != nil {
		return e
	}
	if e := checked(ctx, "icacls.exe", filepath.Dir(c.Storage.DataDir), "/grant", `NT SERVICE\MurmurSQL:M`); e != nil {
		return e
	}
	for _, p := range writable(i, c) {
		if e := os.MkdirAll(p, 0700); e != nil {
			return e
		}
		if e := acl(ctx, p, true); e != nil {
			return e
		}
	}
	return nil
}

type host struct {
	ctx  context.Context
	run  func(context.Context) int
	code int
}

func (h *host) Execute(args []string, requests <-chan svc.ChangeRequest, status chan<- svc.Status) (bool, uint32) {
	if exe, e := os.Executable(); e == nil {
		if e = os.Chdir(filepath.Dir(exe)); e != nil {
			return true, 1
		}
	}
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	status <- svc.Status{State: svc.StartPending}
	done := make(chan int, 1)
	go func() { done <- h.run(ctx) }()
	status <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}
	for {
		select {
		case code := <-done:
			h.code = code
			return code != 0, uint32(code)
		case request := <-requests:
			switch request.Cmd {
			case svc.Interrogate:
				status <- request.CurrentStatus
			case svc.Stop, svc.Shutdown:
				status <- svc.Status{State: svc.StopPending}
				cancel()
			}
		}
	}
}
func Host(ctx context.Context, run func(context.Context) int) (bool, int) {
	is, e := svc.IsWindowsService()
	if e != nil {
		return true, 1
	}
	if !is {
		return false, 0
	}
	h := &host{ctx: ctx, run: run}
	if e = svc.Run(serviceName, h); e != nil {
		return true, 1
	}
	return true, h.code
}
func validateAncestors(string) error { return nil }
