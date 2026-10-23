package replication

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Minimal FTP/FTPS upload client for bridge bundles: banner, explicit
// AUTH TLS for ftps://, login with anonymous defaults, TYPE I, CWD with
// MKD fallback, passive data connection, STOR. Synchronous by design.

const (
	ftpDefaultPort    = 21
	ftpDefaultTimeout = 30 * time.Second
	ftpMaxNameLen     = 256
)

// FtpOptions holds TLS trust and timing for ftps:// uploads.
type FtpOptions struct {
	// RootsPEM holds PEM-encoded CA certificates trusted for control
	// and data connections. Required: cross-domain links pin a private CA.
	RootsPEM []byte
	// Timeout is the socket timeout. Zero selects the 30s default.
	Timeout time.Duration
}

type ftpTarget struct {
	tls  bool
	host string
	port uint16
	user string
	pass string
	dir  string
	file string
}

func ftpInvalid(format string, a ...any) *Error {
	return &Error{Kind: KindInvalid, Msg: "ftp URL: " + fmt.Sprintf(format, a...)}
}

// ftpUnescape percent-decodes one URL component. '+' stays literal.
func ftpUnescape(s string) (string, error) {
	var out []byte
	for i := 0; i < len(s); {
		if s[i] != '%' {
			out = append(out, s[i])
			i++
			continue
		}
		if i+2 >= len(s) {
			return "", ftpInvalid("truncated escape")
		}
		v, err := strconv.ParseUint(s[i+1:i+3], 16, 8)
		if err != nil {
			return "", ftpInvalid("bad escape")
		}
		out = append(out, byte(v))
		i += 3
	}
	for i := 0; i < len(out); {
		// Validate UTF-8.
		c := out[i]
		size := 1
		switch {
		case c < 0x80:
			size = 1
		case c>>5 == 0x6:
			size = 2
		case c>>4 == 0xE:
			size = 3
		case c>>3 == 0x1E:
			size = 4
		default:
			return "", ftpInvalid("non-UTF8 component")
		}
		if i+size > len(out) {
			return "", ftpInvalid("non-UTF8 component")
		}
		for _, d := range out[i+1 : i+size] {
			if d>>6 != 0x2 {
				return "", ftpInvalid("non-UTF8 component")
			}
		}
		i += size
	}
	return string(out), nil
}

// checkFTPName enforces the portable name policy: bounded length, no
// separators, parent references, or hidden files.
func checkFTPName(name string) error {
	if name == "" || len(name) > ftpMaxNameLen {
		return invalidErr("ftp name length %d", len(name))
	}
	if strings.HasPrefix(name, ".") || strings.Contains(name, "..") {
		return invalidErr("ftp name %q rejected", name)
	}
	for i := 0; i < len(name); i++ {
		b := name[i]
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' ||
			b == '-' || b == '_' || b == '.' {
			continue
		}
		return invalidErr("ftp name %q rejected", name)
	}
	return nil
}

func parseFTPURL(url string) (*ftpTarget, string, error) {
	scheme, rest, ok := strings.Cut(url, "://")
	if !ok {
		return nil, "", ftpInvalid("missing scheme")
	}
	var tls bool
	switch scheme {
	case "ftp":
		tls = false
	case "ftps":
		tls = true
	default:
		return nil, "", ftpInvalid("scheme %q", scheme)
	}
	authority, path := rest, ""
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		authority, path = rest[:i], rest[i+1:]
	}
	if authority == "" {
		return nil, "", ftpInvalid("missing host")
	}
	userinfo, hostport := "", authority
	hasUserinfo := false
	if i := strings.LastIndexByte(authority, '@'); i >= 0 {
		hasUserinfo = true
		userinfo, hostport = authority[:i], authority[i+1:]
	}
	var user, pass string
	if u, p, ok := strings.Cut(userinfo, ":"); ok {
		var err error
		if user, err = ftpUnescape(u); err != nil {
			return nil, "", err
		}
		if pass, err = ftpUnescape(p); err != nil {
			return nil, "", err
		}
	} else if !hasUserinfo {
		user, pass = "", ""
	} else {
		user, _ = ftpUnescape(userinfo)
		pass = ""
	}
	if hasUserinfo && user == "" {
		return nil, "", ftpInvalid("empty username")
	}
	var host string
	port := uint16(ftpDefaultPort)
	if strings.HasPrefix(hostport, "[") {
		inner := hostport[1:]
		i := strings.IndexByte(inner, ']')
		if i < 0 {
			return nil, "", ftpInvalid("bad IPv6 host")
		}
		host = inner[:i]
		rest := inner[i+1:]
		if strings.HasPrefix(rest, ":") {
			p, err := strconv.ParseUint(rest[1:], 10, 16)
			if err != nil {
				return nil, "", ftpInvalid("bad port")
			}
			port = uint16(p)
		} else if rest != "" {
			return nil, "", ftpInvalid("bad IPv6 host")
		}
	} else if i := strings.LastIndexByte(hostport, ':'); i >= 0 && i+1 < len(hostport) {
		// Rust rsplit_once(':'): a trailing segment after the last colon
		// is always a port (bare IPv6 needs brackets).
		v, err := strconv.ParseUint(hostport[i+1:], 10, 16)
		if err != nil {
			return nil, "", ftpInvalid("bad port")
		}
		host, port = hostport[:i], uint16(v)
	} else {
		host, port = hostport, ftpDefaultPort
	}
	if host == "" {
		return nil, "", ftpInvalid("missing host")
	}
	var segments []string
	for _, seg := range strings.Split(path, "/") {
		if seg == "" {
			continue
		}
		if seg == "." || seg == ".." {
			return nil, "", ftpInvalid("path escapes directory")
		}
		u, err := ftpUnescape(seg)
		if err != nil {
			return nil, "", err
		}
		segments = append(segments, u)
	}
	var dir, urlFile string
	if len(segments) > 0 && !strings.HasSuffix(path, "/") {
		dir = strings.Join(segments[:len(segments)-1], "/")
		urlFile = segments[len(segments)-1]
	} else {
		dir = strings.Join(segments, "/")
	}
	return &ftpTarget{tls: tls, host: host, port: port, user: user, pass: pass, dir: dir}, urlFile, nil
}

type ftpConn struct {
	raw     net.Conn
	tlsConn *tls.Conn
	timeout time.Duration
}

func (c *ftpConn) stream() net.Conn {
	if c.tlsConn != nil {
		return c.tlsConn
	}
	return c.raw
}

func (c *ftpConn) readByte() (byte, bool, error) {
	if err := c.stream().SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, false, err
	}
	var b [1]byte
	n, err := c.stream().Read(b[:])
	if err != nil {
		return 0, false, err
	}
	if n == 0 {
		return 0, false, nil
	}
	return b[0], true, nil
}

func (c *ftpConn) sendLine(line string) error {
	if err := c.stream().SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return unavailableErr("ftp write: %v", err)
	}
	if _, err := c.stream().Write([]byte(line + "\r\n")); err != nil {
		return unavailableErr("ftp write: %v", err)
	}
	return nil
}

// readReply reads one reply; the first line's code must be in accept.
// Multiline replies are consumed fully.
func (c *ftpConn) readReply(accept []uint16) (uint16, string, error) {
	var text strings.Builder
	var code uint16
	haveCode := false
	for {
		var line []byte
		for {
			b, ok, err := c.readByte()
			if err != nil {
				return 0, "", unavailableErr("ftp read: %v", err)
			}
			if !ok {
				return 0, "", protocolErr("ftp: connection closed")
			}
			if b == '\n' {
				break
			}
			if len(line) > 4096 {
				return 0, "", protocolErr("ftp: overlong reply")
			}
			line = append(line, b)
		}
		for len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		s := string(line)
		if len(s) < 4 || !isDigits(s[:3]) {
			return 0, "", protocolErr("ftp: bad reply %q", s)
		}
		v, _ := strconv.ParseUint(s[:3], 10, 16)
		more := s[3] == '-'
		if !haveCode {
			code, haveCode = uint16(v), true
		}
		text.WriteString(s)
		text.WriteByte('\n')
		if !more {
			if uint16(v) != code {
				return 0, "", protocolErr("ftp: mixed reply %q", s)
			}
			accepted := false
			for _, a := range accept {
				if a == code {
					accepted = true
					break
				}
			}
			if !accepted {
				return 0, "", protocolErr("ftp: reply %d: %s", code, strings.TrimSpace(text.String()))
			}
			return code, text.String(), nil
		}
	}
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

type ftpSession struct {
	control    *ftpConn
	tlsConfig  *tls.Config
	serverName string
	timeout    time.Duration
}

func ftpConnect(t *ftpTarget, tlsConfig *tls.Config, serverName string, timeout time.Duration) (*ftpSession, error) {
	addr := net.JoinHostPort(t.host, strconv.Itoa(int(t.port)))
	sock, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, unavailableErr("ftp dial %s: %v", addr, err)
	}
	s := &ftpSession{control: &ftpConn{raw: sock, timeout: timeout},
		tlsConfig: tlsConfig, serverName: serverName, timeout: timeout}
	if _, _, err := s.control.readReply([]uint16{220}); err != nil {
		sock.Close()
		return nil, err
	}
	if tlsConfig != nil {
		if err := s.control.sendLine("AUTH TLS"); err != nil {
			sock.Close()
			return nil, err
		}
		if _, _, err := s.control.readReply([]uint16{234}); err != nil {
			sock.Close()
			return nil, err
		}
		if err := s.upgradeControl(); err != nil {
			return nil, err
		}
		// Protection buffers, best effort.
		_, _, _ = s.cmd([]uint16{200}, "PBSZ 0")
		_, _, _ = s.cmd([]uint16{200}, "PROT P")
	}
	user := t.user
	if user == "" {
		user = "anonymous"
	}
	code, _, err := s.cmd([]uint16{230, 331}, "USER "+user)
	if err != nil {
		sock.Close()
		return nil, authErr("ftp user: %v", err)
	}
	if code != 230 {
		pass := t.pass
		if pass == "" {
			pass = "anonymous@"
		}
		if _, _, err := s.cmd([]uint16{230}, "PASS "+pass); err != nil {
			sock.Close()
			if perr, ok := err.(*Error); ok && perr.Kind == KindProtocol {
				return nil, authErr("ftp login: %s", perr.Msg)
			}
			return nil, err
		}
	}
	if _, _, err := s.cmd([]uint16{200}, "TYPE I"); err != nil {
		sock.Close()
		return nil, err
	}
	if t.dir != "" {
		if _, _, err := s.cmd([]uint16{250}, "CWD "+t.dir); err != nil {
			_, _, _ = s.cmd([]uint16{257}, "MKD "+t.dir)
			if _, _, err := s.cmd([]uint16{250}, "CWD "+t.dir); err != nil {
				sock.Close()
				return nil, err
			}
		}
	}
	return s, nil
}

func (s *ftpSession) cmd(accept []uint16, line string) (uint16, string, error) {
	if err := s.control.sendLine(line); err != nil {
		return 0, "", err
	}
	return s.control.readReply(accept)
}

func (s *ftpSession) upgradeControl() error {
	if s.tlsConfig == nil {
		return invalidErr("ftp: no TLS config")
	}
	if s.control.tlsConn != nil {
		return protocolErr("ftp: already secure")
	}
	tlsConn := tls.Client(s.control.raw, s.tlsConfig)
	if err := tlsConn.SetDeadline(time.Now().Add(s.timeout)); err != nil {
		return unavailableErr("ftp tls: %v", err)
	}
	if err := tlsConn.Handshake(); err != nil {
		return unavailableErr("ftp tls handshake: %v", err)
	}
	s.control.tlsConn = tlsConn
	return nil
}

func (s *ftpSession) openData() (*ftpConn, error) {
	_, msg, err := s.cmd([]uint16{227}, "PASV")
	if err != nil {
		return nil, err
	}
	ip, port, err := parsePASV(msg)
	if err != nil {
		return nil, err
	}
	addr := net.JoinHostPort(ip, strconv.Itoa(int(port)))
	sock, err := net.DialTimeout("tcp", addr, s.timeout)
	if err != nil {
		return nil, unavailableErr("ftp data dial %s: %v", addr, err)
	}
	c := &ftpConn{raw: sock, timeout: s.timeout}
	if s.tlsConfig != nil {
		tlsConn := tls.Client(sock, s.tlsConfig)
		if err := tlsConn.SetDeadline(time.Now().Add(s.timeout)); err != nil {
			sock.Close()
			return nil, unavailableErr("ftp data tls: %v", err)
		}
		if err := tlsConn.Handshake(); err != nil {
			sock.Close()
			return nil, unavailableErr("ftp data tls handshake: %v", err)
		}
		c.tlsConn = tlsConn
	}
	return c, nil
}

func (s *ftpSession) store(name string, data []byte) error {
	dataConn, err := s.openData()
	if err != nil {
		return err
	}
	if err := s.control.sendLine("STOR " + name); err != nil {
		closeFTPData(dataConn)
		return err
	}
	// 150 starts the transfer; 125 means it is already underway.
	if _, _, err := s.control.readReply([]uint16{125, 150}); err != nil {
		closeFTPData(dataConn)
		return err
	}
	for i := 0; i < len(data); i += 128 * 1024 {
		end := i + 128*1024
		if end > len(data) {
			end = len(data)
		}
		if err := dataConn.stream().SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
			closeFTPData(dataConn)
			return unavailableErr("ftp data write: %v", err)
		}
		if _, err := dataConn.stream().Write(data[i:end]); err != nil {
			closeFTPData(dataConn)
			return unavailableErr("ftp data write: %v", err)
		}
	}
	closeFTPData(dataConn)
	_, _, err = s.control.readReply([]uint16{226, 250})
	return err
}

func (s *ftpSession) quit() {
	// Best effort; reading the farewell also consumes any trailing
	// post-handshake bytes so the close stays a clean FIN.
	if s.control.sendLine("QUIT") != nil {
		return
	}
	_, _, _ = s.control.readReply([]uint16{221})
}

func (s *ftpSession) close() {
	if s.control.tlsConn != nil {
		s.control.tlsConn.Close()
	} else {
		s.control.raw.Close()
	}
}

// closeFTPData closes a data connection gracefully. Always drain to EOF
// first: closing with unread bytes sends RST instead of FIN.
func closeFTPData(c *ftpConn) {
	drain := func(r net.Conn) {
		_ = r.SetReadDeadline(time.Now().Add(5 * time.Second))
		var buf [1024]byte
		for {
			n, err := r.Read(buf[:])
			if n == 0 || err != nil {
				break
			}
		}
	}
	if c.tlsConn != nil {
		_ = c.tlsConn.CloseWrite()
		drain(c.tlsConn)
		_ = c.tlsConn.Close()
	} else if tcp, ok := c.raw.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
		drain(tcp)
		_ = tcp.Close()
	} else {
		_ = c.raw.Close()
	}
}

func parsePASV(msg string) (string, uint16, error) {
	start := strings.IndexByte(msg, '(')
	if start < 0 {
		return "", 0, protocolErr("ftp: bad PASV %q", msg)
	}
	rel := strings.IndexByte(msg[start:], ')')
	if rel < 0 {
		return "", 0, protocolErr("ftp: bad PASV %q", msg)
	}
	parts := strings.Split(msg[start+1:start+rel], ",")
	if len(parts) != 6 {
		return "", 0, protocolErr("ftp: bad PASV %q", msg)
	}
	var n [6]uint64
	for i, p := range parts {
		v, err := strconv.ParseUint(strings.TrimSpace(p), 10, 8)
		if err != nil {
			return "", 0, protocolErr("ftp: bad PASV %q", msg)
		}
		n[i] = v
	}
	return fmt.Sprintf("%d.%d.%d.%d", n[0], n[1], n[2], n[3]), uint16(n[4])<<8 | uint16(n[5]), nil
}

func ftpTLSConfig(rootsPEM []byte, serverName string) (*tls.Config, error) {
	pool := x509.NewCertPool()
	remaining := rootsPEM
	certs := 0
	const endMarker = "-----END CERTIFICATE-----"
	for {
		remaining = bytes.TrimLeft(remaining, " \t\r\n")
		if len(remaining) == 0 {
			break
		}
		if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, invalidErr("ftp trust roots: expected certificate PEM")
		}
		end := bytes.Index(remaining, []byte(endMarker))
		if end < 0 {
			return nil, invalidErr("ftp trust roots: incomplete certificate PEM")
		}
		block := remaining[:end+len(endMarker)]
		// Strip armor lines and whitespace, base64-decode the body.
		body := block[len("-----BEGIN CERTIFICATE-----") : len(block)-len(endMarker)]
		var compact []byte
		for _, b := range body {
			if b != '\r' && b != '\n' && b != ' ' && b != '\t' {
				compact = append(compact, b)
			}
		}
		der, err := base64.StdEncoding.DecodeString(string(compact))
		if err != nil {
			return nil, invalidErr("ftp trust roots: %v", err)
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, invalidErr("ftp trust roots: %v", err)
		}
		pool.AddCert(cert)
		certs++
		remaining = remaining[end+len(endMarker):]
	}
	if certs == 0 {
		return nil, invalidErr("ftp trust roots: no certificates")
	}
	return &tls.Config{RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS12}, nil
}

func ftpServerName(host string) (string, error) {
	if ip := net.ParseIP(host); ip != nil {
		return ip.String(), nil
	}
	if !validDNSName(host) {
		return "", invalidErr("ftp host %q", host)
	}
	return host, nil
}

func validDNSName(host string) bool {
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		for i := 0; i < len(label); i++ {
			b := label[i]
			if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '-' {
				continue
			}
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
	}
	return true
}

// FtpUpload uploads data as one file. defaultName is used when the URL
// carries no file name. tls selects explicit FTPS (ftps://); nil selects
// plain FTP (ftp://) and rejects ftps:// URLs.
func FtpUpload(url, defaultName string, data []byte, opts *FtpOptions) error {
	target, urlFile, err := parseFTPURL(url)
	if err != nil {
		return err
	}
	if target.tls && opts == nil {
		return invalidErr("ftps URL requires trust roots")
	}
	if !target.tls && opts != nil {
		return invalidErr("ftp URL with TLS options")
	}
	if urlFile == "" {
		target.file = defaultName
	} else {
		target.file = urlFile
	}
	if err := checkFTPName(target.file); err != nil {
		return err
	}
	timeout := ftpDefaultTimeout
	if opts != nil && opts.Timeout > 0 {
		timeout = opts.Timeout
	}
	var config *tls.Config
	var name string
	if opts != nil {
		name, err = ftpServerName(target.host)
		if err != nil {
			return err
		}
		config, err = ftpTLSConfig(opts.RootsPEM, name)
		if err != nil {
			return err
		}
	}
	session, err := ftpConnect(target, config, name, timeout)
	if err != nil {
		return err
	}
	result := session.store(target.file, data)
	session.quit()
	session.close()
	return result
}
