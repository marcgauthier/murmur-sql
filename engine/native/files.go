package native

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"math"
	"sync"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/engine"
)

const sessionFileBytes = 16 << 20
const globalFileBytes = 128 << 20
const handleIdle = 5 * time.Minute

type fileHandle struct {
	owner           uint64
	touched         time.Time
	rows            [][]engine.Value
	position, bytes int
	upload          *replication.FileUpload
	download        *replication.FileDownload
	objects         *replication.FileObjects
	kind            string
}
type fileHandles struct {
	mu      sync.Mutex
	entries map[string]*fileHandle
}

func (h *fileHandle) close() {
	if h.upload != nil {
		h.upload.Abort()
	}
	if h.download != nil {
		h.download.Close()
	}
}
func (h *fileHandles) clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, e := range h.entries {
		e.close()
		delete(h.entries, id)
	}
}
func (h *fileHandles) disconnect(owner uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, e := range h.entries {
		if e.owner == owner {
			e.close()
			delete(h.entries, id)
		}
	}
}
func (h *fileHandles) expire() {
	for id, e := range h.entries {
		if time.Since(e.touched) >= handleIdle {
			e.close()
			delete(h.entries, id)
		}
	}
}
func (h *fileHandles) budget(owner uint64) int {
	local, global := 0, 0
	for _, e := range h.entries {
		global += e.bytes
		if e.owner == owner {
			local += e.bytes
		}
	}
	return min(max(0, sessionFileBytes-local), max(0, globalFileBytes-global))
}
func (h *fileHandles) insert(owner uint64, e *fileHandle) (string, error) {
	count := 0
	for _, v := range h.entries {
		if v.owner == owner {
			count++
		}
	}
	if count >= 16 || len(h.entries) >= 1024 {
		e.close()
		return "", userError("53400", "too many file handles")
	}
	for {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			e.close()
			return "", err
		}
		token := hex.EncodeToString(id[:])
		if _, exists := h.entries[token]; exists {
			continue
		}
		e.owner = owner
		e.touched = time.Now()
		h.entries[token] = e
		return token, nil
	}
}
func (h *fileHandles) get(owner uint64, token, kind string) (*fileHandle, error) {
	e, ok := h.entries[token]
	if !ok || e.owner != owner {
		return nil, userError("34000", "file handle is unknown, expired, or belongs to another connection")
	}
	if kind != "" && e.kind != kind {
		return nil, userError("42601", "wrong file handle kind")
	}
	e.touched = time.Now()
	return e, nil
}
func metadata(objects *replication.FileObjects, m *replication.FileMetadata) ([]engine.Value, error) {
	if m.Bytes > math.MaxInt64 || m.Millis > math.MaxInt64 {
		return nil, userError("22003", "file metadata integer overflow")
	}
	local, e := objects.Available(m.Name)
	if e != nil {
		return nil, fileError(e)
	}
	return []engine.Value{m.Name, int64(m.Bytes), int64(m.Millis), hex.EncodeToString(m.Digest[:]), local}, nil
}
func fileError(err error) error {
	if err == nil {
		return nil
	}
	code := "58030"
	if e, ok := err.(*replication.Error); ok {
		switch e.Kind {
		case replication.KindOverloaded:
			code = "53400"
		case replication.KindInvalid:
			code = "22023"
		case replication.KindUnavailable:
			code = "55000"
		}
	}
	return &engine.Error{Code: code, Message: err.Error(), Cause: err}
}
func (d *database) fileCommand(ctx context.Context, owner uint64, c engine.Command) (engine.Result, error) {
	result := engine.Result{}
	handle := d.db.Replication()
	if handle == nil || handle.Files() == nil {
		return result, userError("55000", "file storage is not configured at open")
	}
	objects := handle.Files()
	h := &d.files
	h.mu.Lock()
	defer h.mu.Unlock()
	h.expire()
	if e := ctx.Err(); e != nil {
		return result, e
	}
	args := c.Arguments
	switch c.Operation {
	case "FILE LIST", "FILE SEARCH":
		pattern := ""
		if c.Operation == "FILE SEARCH" {
			pattern = args[0].(string)
			if _, e := like("", pattern); e != nil {
				return result, e
			}
		}
		metas, e := objects.ListBounded(h.budget(owner))
		if e != nil {
			return result, fileError(e)
		}
		entry := &fileHandle{kind: "list"}
		for i := range metas {
			if c.Operation == "FILE SEARCH" {
				matches, e := like(metas[i].Name, pattern)
				if e != nil {
					return result, e
				}
				if !matches {
					continue
				}
			}
			row, e := metadata(objects, &metas[i])
			if e != nil {
				return result, e
			}
			entry.rows = append(entry.rows, row)
			entry.bytes += len(metas[i].Name) + 256
		}
		token, e := h.insert(owner, entry)
		if e != nil {
			return result, e
		}
		result.Rows = [][]engine.Value{{token}}
		return result, nil
	case "FILE NEXT":
		entry, e := h.get(owner, args[0].(string), "list")
		if e != nil {
			return result, e
		}
		end := min(len(entry.rows), entry.position+int(args[1].(int64)))
		result.Rows = entry.rows[entry.position:end]
		entry.position = end
		return result, nil
	case "FILE CLOSE":
		entry, e := h.get(owner, args[0].(string), "")
		if e != nil {
			return result, e
		}
		entry.close()
		delete(h.entries, args[0].(string))
		result.Tag = "CLOSE"
		return result, nil
	case "FILE UPLOAD BEGIN":
		upload, e := objects.BeginUpload(args[0].(string))
		if e != nil {
			return result, fileError(e)
		}
		token, e := h.insert(owner, &fileHandle{kind: "upload", objects: objects, upload: upload})
		if e != nil {
			return result, e
		}
		result.Rows = [][]engine.Value{{token}}
		return result, nil
	case "FILE UPLOAD APPEND":
		token := args[0].(string)
		entry, e := h.get(owner, token, "upload")
		if e != nil {
			return result, e
		}
		chunk := args[1].([]byte)
		if len(chunk) > 1<<20 {
			return result, userError("22023", "upload chunks may not exceed 1 MiB")
		}
		if e = entry.upload.Append(chunk); e != nil {
			entry.close()
			delete(h.entries, token)
			return result, fileError(e)
		}
		result.Tag = "UPLOAD"
		return result, nil
	case "FILE UPLOAD FINISH":
		token := args[0].(string)
		entry, e := h.get(owner, token, "upload")
		if e != nil {
			return result, e
		}
		delete(h.entries, token)
		m, e := entry.objects.FinishUpload(entry.upload)
		if e != nil {
			entry.close()
			return result, fileError(e)
		}
		entry.upload = nil
		row, e := metadata(entry.objects, m)
		if e != nil {
			return result, e
		}
		result.Rows = [][]engine.Value{row}
		return result, nil
	case "FILE UPLOAD ABORT":
		token := args[0].(string)
		entry, e := h.get(owner, token, "upload")
		if e != nil {
			return result, e
		}
		entry.close()
		delete(h.entries, token)
		result.Tag = "UPLOAD"
		return result, nil
	case "FILE DOWNLOAD":
		download, e := objects.Download(args[0].(string))
		if e != nil {
			return result, fileError(e)
		}
		m := download.Meta()
		if m.Bytes > math.MaxInt64 {
			download.Close()
			return result, userError("22003", "file size overflow")
		}
		token, e := h.insert(owner, &fileHandle{kind: "download", download: download})
		if e != nil {
			return result, e
		}
		result.Rows = [][]engine.Value{{token, m.Name, int64(m.Bytes)}}
		return result, nil
	case "FILE DOWNLOAD NEXT":
		token := args[0].(string)
		entry, e := h.get(owner, token, "download")
		if e != nil {
			return result, e
		}
		offset, chunk, eof, e := entry.download.Next(int(args[1].(int64)))
		if e != nil {
			entry.close()
			delete(h.entries, token)
			return result, fileError(e)
		}
		if offset > math.MaxInt64 || entry.download.Meta().Bytes > math.MaxInt64 {
			return result, userError("22003", "file offset overflow")
		}
		result.Rows = [][]engine.Value{{int64(offset), int64(entry.download.Meta().Bytes), chunk, eof}}
		return result, nil
	}
	return result, failed(c.Operation)
}

// like follows the shell's case-sensitive Unicode LIKE dialect with backslash
// escaping. It does not use regex packages or materialize a quadratic matrix.
func like(name, pattern string) (bool, error) {
	type part struct {
		r    rune
		wild bool
	}
	var tokens []part
	p := []rune(pattern)
	for i := 0; i < len(p); i++ {
		c := p[i]
		if c == '\\' {
			i++
			if i >= len(p) {
				return false, userError("22025", "LIKE pattern ends in escape")
			}
			tokens = append(tokens, part{r: p[i]})
		} else {
			tokens = append(tokens, part{r: c, wild: c == '%' || c == '_'})
		}
	}
	previous := make([]bool, len(tokens)+1)
	previous[0] = true
	for i, t := range tokens {
		previous[i+1] = previous[i] && t.wild && t.r == '%'
	}
	for _, c := range name {
		next := make([]bool, len(previous))
		for i, t := range tokens {
			if t.wild && t.r == '%' {
				next[i+1] = next[i] || previous[i+1]
			} else {
				next[i+1] = previous[i] && (t.wild && t.r == '_' || t.r == c)
			}
		}
		previous = next
	}
	return previous[len(tokens)], nil
}
