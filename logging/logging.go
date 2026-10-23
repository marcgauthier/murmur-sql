// Package logging provides daily file logs and bounded, best-effort VictoriaLogs
// delivery using slog and net/http. Remote failures never block SQL requests.
package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/murmur-sql/config"
)

type Sink struct {
	mu        sync.Mutex
	cfg       config.LoggingConfig
	file      *os.File
	day       string
	closed    bool
	queue     chan [2]string
	done      chan struct{}
	Dropped   atomic.Uint64
	client    *http.Client
	remoteCtx context.Context
	cancel    context.CancelFunc
}

func New(c config.LoggingConfig) (*slog.Logger, *Sink, error) {
	cfg := config.DefaultConfig()
	cfg.Logging = c
	if e := cfg.Validate(); e != nil {
		return nil, nil, e
	}
	if e := os.MkdirAll(c.Dir, 0700); e != nil {
		return nil, nil, e
	}
	s := &Sink{cfg: cfg.Snapshot().Logging, done: make(chan struct{}), client: &http.Client{Timeout: 5 * time.Second}}
	s.remoteCtx, s.cancel = context.WithCancel(context.Background())
	if e := s.rotate(time.Now().UTC()); e != nil {
		s.cancel()
		return nil, nil, e
	}
	if c.Victorialogs.Enabled {
		s.queue = make(chan [2]string, 1000)
		go s.remote()
	} else {
		close(s.done)
	}
	level := slog.LevelInfo
	switch strings.ToLower(c.Level) {
	case "error":
		level = slog.LevelError
	case "warn":
		level = slog.LevelWarn
	case "debug":
		level = slog.LevelDebug
	case "trace":
		level = slog.LevelDebug - 4
	}
	return slog.New(slog.NewJSONHandler(s, &slog.HandlerOptions{Level: level})), s, nil
}
func (s *Sink) rotate(now time.Time) error {
	day := now.Format("2006-01-02")
	if day == s.day {
		return nil
	}
	f, e := os.OpenFile(filepath.Join(s.cfg.Dir, s.cfg.FilePrefix+"."+day), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	if s.file != nil {
		s.file.Sync()
		s.file.Close()
	}
	s.file = f
	s.day = day
	_, e = EnforceRetention(s.cfg.Dir, s.cfg.FilePrefix, s.cfg.RetentionDays, now)
	return e
}
func (s *Sink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, os.ErrClosed
	}
	if e := s.rotate(time.Now().UTC()); e != nil {
		return 0, e
	}
	n, e := s.file.Write(p)
	if s.cfg.Console {
		_, ce := os.Stderr.Write(p)
		e = errors.Join(e, ce)
	}
	if s.queue != nil {
		line := [2]string{strconv.FormatInt(time.Now().UnixNano(), 10), strings.TrimSuffix(string(p), "\n")}
		select {
		case s.queue <- line:
		default:
			s.Dropped.Add(1)
		}
	}
	return n, e
}
func (s *Sink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.queue != nil {
		close(s.queue)
	}
	e := errors.Join(s.file.Sync(), s.file.Close())
	s.mu.Unlock()
	select {
	case <-s.done:
	case <-time.After(6 * time.Second):
		s.cancel()
		<-s.done
	}
	s.cancel()
	s.client.CloseIdleConnections()
	return e
}
func EnforceRetention(dir, prefix string, days uint32, now time.Time) (int, error) {
	if days == 0 {
		return 0, nil
	}
	entries, e := os.ReadDir(dir)
	if e != nil {
		return 0, e
	}
	removed := 0
	today := now.UTC().Truncate(24 * time.Hour)
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		date, ok := strings.CutPrefix(entry.Name(), prefix+".")
		if !ok {
			continue
		}
		t, e := time.Parse("2006-01-02", date)
		if e != nil {
			continue
		}
		if today.Sub(t) > time.Duration(days)*24*time.Hour {
			if e = os.Remove(filepath.Join(dir, entry.Name())); e == nil {
				removed++
			}
		}
	}
	return removed, nil
}
func (s *Sink) remote() {
	defer close(s.done)
	timer := time.NewTicker(2 * time.Second)
	defer timer.Stop()
	batch := make([][2]string, 0, 100)
	push := func() {
		if len(batch) == 0 {
			return
		}
		s.push(batch)
		batch = batch[:0]
	}
	for {
		select {
		case <-s.remoteCtx.Done():
			return
		case line, ok := <-s.queue:
			if !ok {
				push()
				return
			}
			batch = append(batch, line)
			if len(batch) == 100 {
				push()
			}
		case <-timer.C:
			push()
		}
	}
}
func (s *Sink) push(batch [][2]string) {
	c := s.cfg.Victorialogs
	labels := map[string]string{"app": "murmur-sql"}
	for k, v := range c.Labels {
		labels[k] = v
	}
	payload := struct {
		Streams []any `json:"streams"`
	}{[]any{struct {
		Stream map[string]string `json:"stream"`
		Values [][2]string       `json:"values"`
	}{labels, batch}}}
	raw, e := json.Marshal(payload)
	if e != nil {
		return
	}
	req, e := http.NewRequestWithContext(s.remoteCtx, "POST", strings.TrimRight(c.URL, "/")+"/loki/api/v1/push", bytes.NewReader(raw))
	if e != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if c.BearerToken != nil {
		req.Header.Set("Authorization", "Bearer "+*c.BearerToken)
	} else if c.Username != nil {
		password := ""
		if c.Password != nil {
			password = *c.Password
		}
		req.SetBasicAuth(*c.Username, password)
	}
	resp, e := s.client.Do(req)
	if e != nil {
		fmt.Fprintln(os.Stderr, "VictoriaLogs push failed:", e)
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		fmt.Fprintln(os.Stderr, "VictoriaLogs push failed:", resp.Status)
	}
}
