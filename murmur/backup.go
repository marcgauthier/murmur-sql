package murmur

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// DefaultRestoreOptions returns default restore options.
func DefaultRestoreOptions() RestoreOptions {
	return RestoreOptions{}
}

func copyFileSync(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return ioErr(err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		return ioErr(err)
	}
	f, err := os.Open(dst)
	if err != nil {
		return ioErr(err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return ioErr(err)
	}
	return nil
}

func syncDir(dir string) error {
	_ = os.Chmod(dir, 0o700)
	f, err := os.Open(dir)
	if err != nil {
		return ioErr(err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return ioErr(err)
	}
	return nil
}

// CopyStoreDir copies a spool store directory to a new directory.
func CopyStoreDir(src, dst string) error {
	if info, err := os.Stat(filepath.Join(src, "manifest")); err != nil || info.IsDir() {
		return invalidErr("backup storage does not exist")
	}
	if err := os.Mkdir(dst, 0o700); err != nil {
		return invalidErr("restore destination must be a new directory")
	}
	step := func() error {
		if err := copyFileSync(filepath.Join(src, "manifest"), filepath.Join(dst, "manifest")); err != nil {
			return err
		}
		if keys := filepath.Join(src, "keys.enc"); isFile(keys) {
			if err := copyFileSync(keys, filepath.Join(dst, "keys.enc")); err != nil {
				return err
			}
		}
		srcSeg, dstSeg := filepath.Join(src, "segments"), filepath.Join(dst, "segments")
		if err := os.Mkdir(dstSeg, 0o700); err != nil {
			return ioErr(err)
		}
		if isDir(srcSeg) {
			entries, err := os.ReadDir(srcSeg)
			if err != nil {
				return ioErr(err)
			}
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			sort.Strings(names)
			for _, name := range names {
				s := filepath.Join(srcSeg, name)
				if !isFile(s) {
					continue
				}
				if err := copyFileSync(s, filepath.Join(dstSeg, name)); err != nil {
					return err
				}
			}
		}
		if err := syncDir(dstSeg); err != nil {
			return err
		}
		if err := syncDir(dst); err != nil {
			return err
		}
		if parent := filepath.Dir(dst); parent != "" && parent != "." {
			if f, err := os.Open(parent); err == nil {
				_ = f.Sync()
				f.Close()
			}
		}
		return nil
	}
	if err := step(); err != nil {
		_ = os.RemoveAll(dst)
		return err
	}
	return nil
}

func isFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// CopyCheckpointFiles copies checkpoint files to a final backup directory.
func CopyCheckpointFiles(tmpDir string, files []string, dst string) error {
	if err := os.Mkdir(dst, 0o700); err != nil {
		return invalidErr("backup destination must be a new directory")
	}
	step := func() error {
		for _, f := range files {
			rel, err := filepath.Rel(tmpDir, f)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return corruptErr("checkpoint file escapes destination")
			}
			out := filepath.Join(dst, rel)
			if parent := filepath.Dir(out); parent != "" {
				if err := os.MkdirAll(parent, 0o700); err != nil {
					return ioErr(err)
				}
			}
			if err := copyFileSync(f, out); err != nil {
				return err
			}
		}
		if complete := filepath.Join(tmpDir, "COMPLETE"); isFile(complete) {
			if err := copyFileSync(complete, filepath.Join(dst, "COMPLETE")); err != nil {
				return err
			}
		}
		if seg := filepath.Join(dst, "segments"); isDir(seg) {
			if err := syncDir(seg); err != nil {
				return err
			}
		}
		if err := syncDir(dst); err != nil {
			return err
		}
		if parent := filepath.Dir(dst); parent != "" && parent != "." {
			if f, err := os.Open(parent); err == nil {
				_ = f.Sync()
				f.Close()
			}
		}
		return nil
	}
	if err := step(); err != nil {
		_ = os.RemoveAll(dst)
		return err
	}
	return nil
}

func restoreOptions() Options {
	options := DefaultOptions()
	options.Mode = OpenExisting
	return options
}

func verifyStoreDir(path string, key []byte) (Identity, error) {
	return verifyStoreDirWithCredential(path, KeyCredential(key))
}

func verifyStoreDirWithCredential(path string, cred Credential) (Identity, error) {
	var zero Identity
	options := restoreOptions()
	progress := NewOpenProgressReporter(nil)
	opened, err := OpenStorageWithCredential(path, cred, &options, progress)
	if err != nil {
		return zero, err
	}
	if opened.Store.Stats().TruncatedTails > 0 {
		opened.Close()
		return zero, corruptErr("backup segment truncated")
	}
	identity := opened.Identity
	connection, err := RecoverCurrent(opened, progress)
	if err != nil {
		opened.Close()
		return zero, err
	}
	connection.Close()
	opened.Close()
	return identity, nil
}

// RestoreBackup restores an online backup to a new live directory.
func RestoreBackup(backup, dst string, key []byte, options *RestoreOptions) error {
	return RestoreBackupWithCredential(backup, dst, KeyCredential(key), options)
}

// RestoreBackupWithCredential restores an online backup with a master
// key or passphrase credential.
func RestoreBackupWithCredential(backup, dst string, cred Credential, options *RestoreOptions) error {
	if err := cred.validate(); err != nil {
		return err
	}
	if !isFile(filepath.Join(backup, "manifest")) {
		return invalidErr("backup storage does not exist")
	}
	if err := spool.ValidateCredential(backup, cred.Key, cred.Passphrase, nil, ""); err != nil {
		return keyError(err)
	}
	if err := CopyStoreDir(backup, dst); err != nil {
		return err
	}
	step := func() error {
		if options.NewDatabaseID != nil || options.NewNodeID != nil {
			restoreOpts := restoreOptions()
			opened, err := OpenStorageWithCredential(dst, cred, &restoreOpts, NewOpenProgressReporter(nil))
			if err != nil {
				return err
			}
			identity := opened.Identity
			if options.NewDatabaseID != nil {
				identity.Database = *options.NewDatabaseID
			}
			if options.NewNodeID != nil {
				identity.Node = *options.NewNodeID
			}
			err = opened.Store.Commit([]spool.Mutation{{Key: []byte("00/identity"),
				Value: EncodeIdentity(identity)}}, spool.DurabilitySync)
			opened.Close()
			if err != nil {
				return err
			}
		}
		_, err := verifyStoreDirWithCredential(dst, cred)
		return err
	}
	if err := step(); err != nil {
		_ = os.RemoveAll(dst)
		return err
	}
	return nil
}

// VerifyBackupDir verifies a backup directory without mutating it.
func VerifyBackupDir(backup string, key []byte) error {
	return VerifyBackupDirWithCredential(backup, KeyCredential(key))
}

// VerifyBackupDirWithCredential verifies a backup directory with a
// master key or passphrase credential, without mutating it.
func VerifyBackupDirWithCredential(backup string, cred Credential) error {
	if err := cred.validate(); err != nil {
		return err
	}
	if !isFile(filepath.Join(backup, "manifest")) {
		return invalidErr("backup storage does not exist")
	}
	if err := spool.ValidateCredential(backup, cred.Key, cred.Passphrase, nil, ""); err != nil {
		return keyError(err)
	}
	parent := filepath.Dir(backup)
	if parent == "" {
		parent = "."
	}
	for i := 0; i < 8; i++ {
		var rnd [8]byte
		_, _ = rand.Read(rnd[:])
		tmp := filepath.Join(parent, fmt.Sprintf(".verify-tmp-%d-%x", time.Now().UnixNano(), rnd))
		if _, err := os.Stat(tmp); err == nil {
			continue
		}
		if err := CopyStoreDir(backup, tmp); err != nil {
			return err
		}
		_, err := verifyStoreDirWithCredential(tmp, cred)
		_ = os.RemoveAll(tmp)
		return err
	}
	return invalidErr("could not allocate verification directory")
}
