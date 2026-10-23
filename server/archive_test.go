package server

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"github.com/marcgauthier/murmur-sql/config"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestArchiveRustFormatAndUnsafePaths(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source")
	os.MkdirAll(filepath.Join(src, "segments"), 0700)
	os.WriteFile(filepath.Join(src, "manifest"), []byte("encrypted manifest fixture"), 0600)
	os.WriteFile(filepath.Join(src, "segments", "000001.spool"), []byte{0, 1, 2, 3, 255}, 0600)
	if runtime.GOOS != "windows" {
		os.Symlink("../source", filepath.Join(src, "cycle"))
	}
	var archive bytes.Buffer
	if e := packArchive(src, &archive); e != nil {
		t.Fatal(e)
	}
	if !strings.HasPrefix(archive.String(), "MMBK01\n") {
		t.Fatal("changed Rust archive format")
	}
	dest := filepath.Join(dir, "restored")
	if e := unpackArchive(bytes.NewReader(archive.Bytes()), dest); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(dest, "segments", "000001.spool"))
	if e != nil || !bytes.Equal(b, []byte{0, 1, 2, 3, 255}) {
		t.Fatal(b, e)
	}
	if runtime.GOOS != "windows" {
		info, _ := os.Stat(filepath.Join(dest, "manifest"))
		if info.Mode().Perm() != 0600 {
			t.Fatal(info.Mode())
		}
	}
	for _, name := range []string{"../escape", "/absolute", "x/../escape", "x//file", `x\file`, "C:/file", "NUL:"} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			buf.WriteString(archiveMagic)
			binary.Write(&buf, binary.BigEndian, uint64(len(name)))
			buf.WriteString(name)
			binary.Write(&buf, binary.BigEndian, uint64(0))
			if e := unpackArchive(&buf, filepath.Join(t.TempDir(), "dest")); e == nil {
				t.Fatal("accepted unsafe path")
			}
		})
	}
	raw := archive.Bytes()
	for _, n := range []int{0, 3, 8, len(raw) - 1} {
		if e := unpackArchive(bytes.NewReader(raw[:n]), filepath.Join(t.TempDir(), "truncated")); e == nil {
			t.Fatal("accepted truncated archive", n)
		}
	}
}
func dataValues(t *testing.T, ms []message) [][]string {
	t.Helper()
	var out [][]string
	for _, m := range ms {
		if m.kind != 'D' {
			continue
		}
		p := packet{b: m.body}
		n := int(p.u16())
		row := make([]string, n)
		for i := range row {
			size := int32(p.u32())
			if size >= 0 {
				row[i] = string(p.read(int(size)))
			}
		}
		if e := p.end(); e != nil {
			t.Fatal(e)
		}
		out = append(out, row)
	}
	return out
}
func TestBackupCommands(t *testing.T) {
	f := &fixtureEngine{}
	_, addr, _ := startFixture(t, config.DefaultConfig(), f)
	w := mustConnect(t, addr)
	unlockFixture(t, w)
	rows := dataValues(t, w.query(t, "MURMUR BACKUP SAVE"))
	if len(rows) != 1 || !validBackupName(rows[0][0]) {
		t.Fatal(rows)
	}
	name := rows[0][0]
	rows = dataValues(t, w.query(t, "MURMUR BACKUP LIST"))
	if len(rows) != 1 || rows[0][0] != name {
		t.Fatal(rows)
	}
	rows = dataValues(t, w.query(t, "MURMUR BACKUP DOWNLOAD '"+name+"' LIMIT 7"))
	if len(rows) != 1 || rows[0][2] != "\\x4d4d424b30310a" {
		t.Fatal(rows)
	}
	if code := findCode(w.query(t, "MURMUR RESTORE FROM '"+name+"' KEY 'wrong'")); code != "28000" {
		t.Fatal(code)
	}
	if code := findCode(w.query(t, "MURMUR RESTORE FROM '"+name+"' KEY 'fixture-secret'")); code != "" {
		t.Fatal(code)
	}
	for _, q := range []string{"MURMUR BACKUP UPLOAD BEGIN 'uploaded.murmur-backup'", "MURMUR BACKUP UPLOAD APPEND 'uploaded.murmur-backup' '" + base64.StdEncoding.EncodeToString([]byte(archiveMagic)) + "'", "MURMUR BACKUP UPLOAD FINISH 'uploaded.murmur-backup'"} {
		if code := findCode(w.query(t, q)); code != "" {
			t.Fatal(q, code)
		}
	}
	if code := findCode(w.query(t, "MURMUR BACKUP UPLOAD BEGIN 'uploaded.murmur-backup'")); code != "23505" {
		t.Fatal(code)
	}
	if code := findCode(w.query(t, "MURMUR BACKUP DOWNLOAD '../secret'")); code != "22023" {
		t.Fatal(code)
	}
	if code := findCode(w.query(t, "MURMUR BACKUP DOWNLOAD 'missing'")); code != "58P01" {
		t.Fatal(code)
	}
}
