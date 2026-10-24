// Wire connect + MURMUR UNLOCK under the stall/overall watchdog.
//
// Same env knobs as the await-unlock (MURMUR_LIVE_UNLOCK_STALL_SECONDS=120,
// MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS=1800). The watchdog observes redial
// progress: any change in the last connection error resets the stall timer.
// One MURMUR UNLOCK attempt runs to completion once issued; there is no
// intra-open progress API over the wire, so a wedged open surfaces as an
// overall timeout, not a stall.
package goharness

import (
	"fmt"
	"net/netip"
	"os"
	"strings"
	"time"
)

// ConnectWithWatchdog connects to sqlAddr, authenticates user/password,
// and unlocks with the 64-hex-char keyHex (used raw by the server).
// It retries until the overall timeout, and fails fast when nothing
// changes for the stall window.
func ConnectWithWatchdog(sqlAddr netip.AddrPort, user, password, keyHex string) (*NodeClient, error) {
	stall := EnvSeconds("MURMUR_LIVE_UNLOCK_STALL_SECONDS", 120)
	overall := EnvSeconds("MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS", 1800)
	start := time.Now()
	lastChange := start
	lastErr := ""
	lastLog := start
	for {
		client, err := connectOnce(sqlAddr, user, password, keyHex, 5*time.Second)
		if err == nil {
			return client, nil
		}
		msg := shortError(err)
		if msg != lastErr {
			lastChange = time.Now()
			lastErr = msg
		}
		elapsed := time.Since(start)
		if time.Since(lastChange) >= stall {
			return nil, &UnlockTimeoutError{Elapsed: elapsed, Reason: "no unlock-progress advancement"}
		}
		if elapsed >= overall {
			return nil, &UnlockTimeoutError{Elapsed: elapsed, Reason: "overall unlock timeout"}
		}
		if time.Since(lastLog) >= 30*time.Second {
			lastLog = time.Now()
			fmt.Fprintf(os.Stderr, "unlock waiting after %v: %s\n", elapsed, lastErr)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// connectOnce dials, authenticates, and unlocks a single connection with
// the given connect timeout. Forensics uses this directly for short-lived
// isolated connections.
func connectOnce(sqlAddr netip.AddrPort, user, password, keyHex string, timeout time.Duration) (*NodeClient, error) {
	cfg := NewConfig(user, password, keyHex)
	cfg.Host = sqlAddr.Addr().String()
	cfg.Port = sqlAddr.Port()
	cfg.ConnectTimeout = timeout
	client, err := Connect(cfg)
	if err != nil {
		return nil, err
	}
	return &NodeClient{client: client}, nil
}

func shortError(err error) string {
	var b strings.Builder
	for _, r := range err.Error() {
		if r >= 0x20 && r != 0x7f {
			b.WriteRune(r)
		}
		if b.Len() >= 200 {
			break
		}
	}
	if b.Len() == 0 {
		return "<empty error>"
	}
	return b.String()
}
