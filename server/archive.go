package server

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/marcgauthier/murmur-sql-go/engine"
)

const archiveMagic = "MMBK01\n"
const archiveSuffix = ".murmur-backup"

func validBackupName(s string) bool {
	if s == "" || len(s) > 255 || s[0] == '.' {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func checkedRelative(s string) bool {
	if s == "" || len(s) > 4096 || !utf8.ValidString(s) || strings.ContainsAny(s, "\\:\x00") || strings.HasPrefix(s, "/") {
		return false
	}
	for _, v := range strings.Split(s, "/") {
		if v == "" || v == "." || v == ".." {
			return false
		}
	}
	return filepath.IsLocal(filepath.FromSlash(s))
}

// packArchive preserves the Rust MMBK01 container byte format. Only regular
// checkpoint files are included, in lexical order; payloads are streamed.
func packArchive(src string, w io.Writer) error {
	root, e := os.OpenRoot(src)
	if e != nil {
		return e
	}
	defer root.Close()
	if _, e = io.WriteString(w, archiveMagic); e != nil {
		return e
	}
	return fs.WalkDir(root.FS(), ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if !checkedRelative(name) {
			return fmt.Errorf("invalid checkpoint path")
		}
		f, e := root.Open(name)
		if e != nil {
			return e
		}
		defer f.Close()
		if e = binary.Write(w, binary.BigEndian, uint64(len(name))); e != nil {
			return e
		}
		if _, e = io.WriteString(w, name); e != nil {
			return e
		}
		if e = binary.Write(w, binary.BigEndian, uint64(info.Size())); e != nil {
			return e
		}
		_, e = io.CopyN(w, f, info.Size())
		return e
	})
}
func unpackArchive(r io.Reader, dest string) error {
	if e := os.Mkdir(dest, 0700); e != nil {
		return e
	}
	root, e := os.OpenRoot(dest)
	if e != nil {
		return e
	}
	defer root.Close()
	magic := make([]byte, len(archiveMagic))
	if _, e = io.ReadFull(r, magic); e != nil {
		return e
	}
	if string(magic) != archiveMagic {
		return errors.New("invalid backup magic")
	}
	for count := 0; ; count++ {
		var raw [8]byte
		n, e := io.ReadFull(r, raw[:])
		if e == io.EOF && n == 0 {
			return nil
		}
		if e != nil {
			return e
		}
		if count >= 1<<20 {
			return errors.New("too many backup entries")
		}
		size := binary.BigEndian.Uint64(raw[:])
		if size == 0 || size > 4096 {
			return errors.New("invalid backup entry name length")
		}
		name := make([]byte, int(size))
		if _, e = io.ReadFull(r, name); e != nil {
			return e
		}
		if !checkedRelative(string(name)) {
			return errors.New("unsafe backup entry path")
		}
		if _, e = io.ReadFull(r, raw[:]); e != nil {
			return e
		}
		size = binary.BigEndian.Uint64(raw[:])
		if size > math.MaxInt64 {
			return errors.New("invalid backup entry size")
		}
		parent := path.Dir(string(name))
		if parent != "." {
			parts := strings.Split(parent, "/")
			for i := range parts {
				p := strings.Join(parts[:i+1], "/")
				if e = root.Mkdir(p, 0700); e != nil && !errors.Is(e, fs.ErrExist) {
					return e
				}
			}
		}
		f, e := root.OpenFile(filepath.FromSlash(string(name)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		_, e = io.CopyN(f, r, int64(size))
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
	}
}
func backupIO(e error) error {
	if e == nil {
		return nil
	}
	if errors.Is(e, fs.ErrNotExist) {
		return sqlError("58P01", "backup file not found")
	}
	if errors.Is(e, fs.ErrExist) {
		return sqlError("23505", "backup already exists")
	}
	return &engine.Error{Code: "58000", Message: "backup storage error: " + e.Error(), Cause: e}
}
func openRegular(root *os.Root, name string, flags int) (*os.File, error) {
	i, e := root.Lstat(name)
	if e != nil {
		return nil, e
	}
	if !i.Mode().IsRegular() {
		return nil, errors.New("backup path must be a regular file")
	}
	return root.OpenFile(name, flags, 0600)
}
func (s *Server) backupCommand(ctx context.Context, id uint64, db engine.Database, op string, args []engine.Value) (engine.Result, error) {
	s.backupMu.Lock()
	defer s.backupMu.Unlock()
	result := engine.Result{Columns: cols(commandColumns[op]), Tag: ""}
	if strings.HasPrefix(op, "BACKUP UPLOAD ") && len(result.Columns) == 0 {
		result.Tag = "UPLOAD"
	}
	dir := s.Config().Backups.Dir
	if len(args) > 0 {
		if !validBackupName(args[0].(string)) {
			return result, sqlError("22023", "invalid backup name")
		}
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return result, backupIO(e)
	}
	root, e := os.OpenRoot(dir)
	if e != nil {
		return result, backupIO(e)
	}
	defer root.Close()
	switch op {
	case "BACKUP SAVE":
		checkpoint, e := os.MkdirTemp(dir, ".checkpoint-*")
		if e != nil {
			return result, backupIO(e)
		}
		os.Remove(checkpoint)
		defer os.RemoveAll(checkpoint)
		if _, e = db.Command(ctx, id, engine.Command{Operation: "BACKUP DIRECTORY", Arguments: []engine.Value{checkpoint}}); e != nil {
			return result, e
		}
		base := "murmur-" + time.Now().UTC().Format("20060102-150405") + archiveSuffix
		name := base
		var f *os.File
		for n := 1; n < 10000; n++ {
			f, e = root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if !errors.Is(e, fs.ErrExist) {
				break
			}
			name = strings.TrimSuffix(base, archiveSuffix) + "-" + strconv.Itoa(n+1) + archiveSuffix
		}
		if e != nil {
			return result, backupIO(e)
		}
		writer := bufio.NewWriter(f)
		e = packArchive(checkpoint, writer)
		if e == nil {
			e = writer.Flush()
		}
		if e == nil {
			e = f.Sync()
		}
		i, se := f.Stat()
		ce := f.Close()
		if e == nil {
			e = se
		}
		if e == nil {
			e = ce
		}
		if e != nil {
			root.Remove(name)
			return result, backupIO(e)
		}
		result.Rows = [][]engine.Value{{name, i.Size()}}
		return result, nil
	case "BACKUP LIST":
		entries, e := fs.ReadDir(root.FS(), ".")
		if e != nil {
			return result, backupIO(e)
		}
		for _, entry := range entries {
			if !validBackupName(entry.Name()) || !strings.HasSuffix(entry.Name(), archiveSuffix) {
				continue
			}
			i, e := root.Lstat(entry.Name())
			if e != nil {
				return result, backupIO(e)
			}
			if !i.Mode().IsRegular() {
				continue
			}
			result.Rows = append(result.Rows, []engine.Value{entry.Name(), i.Size(), i.ModTime().UTC().Format(time.RFC3339)})
		}
		sort.Slice(result.Rows, func(i, j int) bool { return result.Rows[i][0].(string) < result.Rows[j][0].(string) })
		return result, nil
	case "BACKUP UPLOAD BEGIN":
		name := args[0].(string)
		if _, e = root.Lstat(name); e == nil {
			return result, sqlError("23505", "backup already exists")
		} else if !errors.Is(e, fs.ErrNotExist) {
			return result, backupIO(e)
		}
		f, e := root.OpenFile("."+name+".part", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return result, backupIO(e)
		}
		return result, backupIO(f.Close())
	case "BACKUP UPLOAD APPEND":
		name := "." + args[0].(string) + ".part"
		chunk, e := base64.StdEncoding.DecodeString(args[1].(string))
		if e != nil {
			return result, sqlError("22023", "invalid base64 upload chunk")
		}
		f, e := openRegular(root, name, os.O_APPEND|os.O_WRONLY)
		if e != nil {
			return result, backupIO(e)
		}
		_, e = f.Write(chunk)
		if e == nil {
			e = f.Sync()
		}
		ce := f.Close()
		if e == nil {
			e = ce
		}
		return result, backupIO(e)
	case "BACKUP UPLOAD FINISH":
		name := args[0].(string)
		part := "." + name + ".part"
		f, e := openRegular(root, part, os.O_RDONLY)
		if e != nil {
			return result, backupIO(e)
		}
		i, e := f.Stat()
		if e == nil {
			e = f.Sync()
		}
		f.Close()
		if e != nil {
			return result, backupIO(e)
		}
		if e = os.Link(filepath.Join(dir, part), filepath.Join(dir, name)); e != nil {
			return result, backupIO(e)
		}
		if e = root.Remove(part); e != nil {
			return result, backupIO(e)
		}
		result.Rows = [][]engine.Value{{name, i.Size()}}
		return result, nil
	case "BACKUP UPLOAD ABORT":
		e = root.Remove("." + args[0].(string) + ".part")
		if errors.Is(e, fs.ErrNotExist) {
			e = nil
		}
		return result, backupIO(e)
	case "BACKUP DOWNLOAD":
		f, e := openRegular(root, args[0].(string), os.O_RDONLY)
		if e != nil {
			return result, backupIO(e)
		}
		defer f.Close()
		i, e := f.Stat()
		if e != nil {
			return result, backupIO(e)
		}
		offset, limit := args[1].(int64), args[2].(int64)
		if offset > i.Size() {
			return result, sqlError("22023", "backup offset exceeds file size")
		}
		if limit > i.Size()-offset {
			limit = i.Size() - offset
		}
		chunk := make([]byte, int(limit))
		_, e = f.ReadAt(chunk, offset)
		if e != nil && !errors.Is(e, io.EOF) {
			return result, backupIO(e)
		}
		result.Rows = [][]engine.Value{{offset, i.Size(), chunk}}
		return result, nil
	case "RESTORE":
		f, e := openRegular(root, args[0].(string), os.O_RDONLY)
		if e != nil {
			return result, backupIO(e)
		}
		defer f.Close()
		dest, e := os.MkdirTemp(dir, ".unpack-*")
		if e != nil {
			return result, backupIO(e)
		}
		os.Remove(dest)
		defer os.RemoveAll(dest)
		if e = unpackArchive(bufio.NewReader(f), dest); e != nil {
			return result, sqlError("XX001", "corrupt backup file: "+e.Error())
		}
		_, e = db.Command(ctx, id, engine.Command{Operation: "RESTORE DIRECTORY", Arguments: []engine.Value{dest, args[1]}})
		if e != nil {
			return result, e
		}
		s.generation.Add(1)
		result.Tag = "RESTORE"
		return result, nil
	}
	return result, errors.New("unhandled backup operation")
}
