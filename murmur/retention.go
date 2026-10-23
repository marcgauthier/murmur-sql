package murmur

import (
	"github.com/marcgauthier/murmur-sql/murmur/replication"
)

// Type aliases for administrative replication operations.
type (
	// RetentionStatus reports durable peer acknowledgements.
	RetentionStatus = replication.RetentionStatus
	// PeerRetention reports one peer's retention state.
	PeerRetention = replication.PeerRetention
	// FileMetadata describes one stored file.
	FileMetadata = replication.FileMetadata
	// FileAvailability reports file content availability.
	FileAvailability = replication.FileAvailability
	// FileKeyRotationStatus reports old-generation key usage.
	FileKeyRotationStatus = replication.FileKeyRotationStatus
)

func (db *Database) retentionHandle() (*replication.Handle, error) {
	if db.Status().Closed {
		return nil, ErrClosed
	}
	if db.onWriterThread() {
		return nil, invalidErr("replication administration cannot run inside SQL callbacks")
	}
	handle := db.Replication()
	if handle == nil {
		return nil, invalidErr("replication is not enabled")
	}
	return handle, nil
}

// RetirePeer permanently excludes a peer and durably releases its
// retention obligation.
func (db *Database) RetirePeer(node [16]byte) error {
	handle, err := db.retentionHandle()
	if err != nil {
		return err
	}
	if err := handle.RetirePeer(node); err != nil {
		return storageErr("%v", err)
	}
	return nil
}

// RetentionStatus reads durable peer acknowledgements and collection
// progress from Spool.
func (db *Database) RetentionStatus() (*RetentionStatus, error) {
	handle, err := db.retentionHandle()
	if err != nil {
		return nil, err
	}
	status, err := handle.RetentionStatus()
	if err != nil {
		return nil, storageErr("%v", err)
	}
	return status, nil
}

// CollectRetention runs a bounded background pass now, preserving
// configured retention limits. Returns deleted log records.
func (db *Database) CollectRetention() (uint64, error) {
	handle, err := db.retentionHandle()
	if err != nil {
		return 0, err
	}
	pruned, err := handle.CollectRetention()
	if err != nil {
		return 0, storageErr("%v", err)
	}
	return pruned, nil
}
