package murmur

import (
	"io"
	"strings"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

func fileStore(db *Database) (*replication.FileObjects, error) {
	if db.Status().Closed {
		return nil, ErrClosed
	}
	handle := db.Replication()
	if handle == nil {
		return nil, invalidErr("file storage is not configured at open")
	}
	objects := handle.Files()
	if objects == nil {
		return nil, invalidErr("file storage is not configured at open")
	}
	return objects, nil
}

func fileErr(err error) error {
	return storageErr("%v", err)
}

// UploadFile streams a file into encrypted storage and publishes its
// replicated metadata.
func (db *Database) UploadFile(name string, input io.Reader) (*replication.FileMetadata, error) {
	store, err := fileStore(db)
	if err != nil {
		return nil, err
	}
	meta, err := store.UploadOne(name, input)
	if err != nil {
		return nil, fileErr(err)
	}
	return meta, nil
}

// ListFiles lists nondeleted file metadata, sorted by name, including
// remote files whose contents have not yet been fetched.
func (db *Database) ListFiles() ([]replication.FileMetadata, error) {
	store, err := fileStore(db)
	if err != nil {
		return nil, err
	}
	metas, err := store.List()
	if err != nil {
		return nil, fileErr(err)
	}
	return metas, nil
}

// SearchFiles finds files whose names contain the case-sensitive query.
// An empty query returns all files.
func (db *Database) SearchFiles(query string) ([]replication.FileMetadata, error) {
	files, err := db.ListFiles()
	if err != nil {
		return nil, err
	}
	var out []replication.FileMetadata
	for _, file := range files {
		if strings.Contains(file.Name, query) {
			out = append(out, file)
		}
	}
	return out, nil
}

// FileMetadata looks up a file by exact name. Deleted files return nil.
func (db *Database) FileMetadata(name string) (*replication.FileMetadata, error) {
	store, err := fileStore(db)
	if err != nil {
		return nil, err
	}
	meta, err := store.Metadata(name)
	if err != nil {
		return nil, fileErr(err)
	}
	if meta == nil || meta.Deleted {
		return nil, nil
	}
	return meta, nil
}

// GetFile streams locally available file contents into output.
func (db *Database) GetFile(name string, output io.Writer) (*replication.FileMetadata, error) {
	store, err := fileStore(db)
	if err != nil {
		return nil, err
	}
	meta, err := store.Read(name, output)
	if err != nil {
		return nil, fileErr(err)
	}
	return meta, nil
}

// FileAvailability reports whether the file is local, known at recently
// observed peers, or unavailable.
func (db *Database) FileAvailability(name string) (*replication.FileAvailability, error) {
	store, err := fileStore(db)
	if err != nil {
		return nil, err
	}
	availability, err := store.Availability(name)
	if err != nil {
		return nil, fileErr(err)
	}
	return availability, nil
}

// FileKeyRotationStatus checks for old-generation objects or transfers
// that still require the previous file key.
func (db *Database) FileKeyRotationStatus() (*replication.FileKeyRotationStatus, error) {
	store, err := fileStore(db)
	if err != nil {
		return nil, err
	}
	status, err := store.KeyRotationStatus()
	if err != nil {
		return nil, fileErr(err)
	}
	return status, nil
}

// FileAvailable reports whether nondeleted file contents are present locally.
func (db *Database) FileAvailable(name string) (bool, error) {
	store, err := fileStore(db)
	if err != nil {
		return false, err
	}
	available, err := store.Available(name)
	if err != nil {
		return false, fileErr(err)
	}
	return available, nil
}

// DeleteFile publishes a deletion tombstone for the named file.
func (db *Database) DeleteFile(name string) error {
	store, err := fileStore(db)
	if err != nil {
		return err
	}
	if err := store.Delete(name); err != nil {
		return fileErr(err)
	}
	return nil
}
