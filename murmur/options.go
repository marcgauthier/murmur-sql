package murmur

import (
	"time"

	"github.com/marcgauthier/murmur-sql/murmur/replication"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

// Options configures Database open behavior.
type Options struct {
	// Node-local, rebuildable search indexes over ordinary source tables.
	SearchIndexes []SearchIndex
	// Byte budget for disposable indexing work; overflow triggers a rebuild.
	SearchQueueBytes int
	// Maximum rows processed per maintenance turn.
	SearchBatchRows int
	SearchInterval  time.Duration
	// Replication engine configuration; nil disables replication.
	Replication *replication.Config
	Mode        OpenMode
	// How often the writer captures pending session changes.
	CaptureInterval time.Duration
	// Session changeset bytes that force a capture.
	CaptureBytes int
	// Worker queue byte budget.
	QueueBytes int
	// Single transaction byte budget.
	TransactionBytes int
	// Prepared statement cache entries per SQL callback; zero disables
	// the cache.
	StmtCacheSize int
	// Spool segment size target.
	SegmentBytes int
	// Target plaintext bytes per independently compressed/encrypted Spool block.
	SpoolBlockBytes int
	// Spool hot block cache budget; zero disables the cache.
	SpoolCacheBytes int
	SnapshotDeltas  int
	SnapshotBytes   int
	// Zstd level for snapshot/delta payloads.
	CompressionLevel int
	StorageWorkers   int
	DataKeyMaxAge    *time.Duration
	DatabaseID       *[16]byte
	NodeID           *[16]byte
	FaultHooks       *spool.FaultHooks
	OnOpenProgress   func(OpenProgress)
	// Recovery configures the optional disposable recovery cache.
	// Nil disables the feature (legacy open path).
	Recovery  *RecoveryOptions
	Scheduler SchedulerConfig
}

// DefaultOptions returns the default database options.
func DefaultOptions() Options {
	return Options{
		SearchQueueBytes: 8 << 20,
		SearchBatchRows:  256,
		SearchInterval:   100 * time.Millisecond,
		Mode:             OpenExisting,
		CaptureInterval:  time.Second,
		CaptureBytes:     8 << 20,
		QueueBytes:       64 << 20,
		TransactionBytes: 64 << 20,
		StmtCacheSize:    32,
		SegmentBytes:     256 << 20,
		SpoolBlockBytes:  64 << 10,
		SpoolCacheBytes:  32 << 20,
		SnapshotDeltas:   128,
		SnapshotBytes:    256 << 20,
		CompressionLevel: 3,
		StorageWorkers:   2,
		Scheduler:        DefaultSchedulerConfig(),
	}
}

// Validate checks worker limits and sub-configurations.
func (o *Options) Validate() error {
	if err := o.Scheduler.Validate(); err != nil {
		return err
	}
	if err := ValidateSearchOptions(o); err != nil {
		return err
	}
	if o.Replication != nil {
		if err := o.Replication.Validate(); err != nil {
			return invalidErr("%v", err)
		}
	}
	if err := o.Recovery.Validate(); err != nil {
		return err
	}
	if o.CaptureInterval <= 0 ||
		o.CaptureBytes == 0 ||
		o.QueueBytes < o.CaptureBytes ||
		o.TransactionBytes > o.QueueBytes ||
		o.TransactionBytes == 0 ||
		o.QueueBytes > 128<<20 ||
		o.StmtCacheSize < 0 ||
		o.SpoolBlockBytes == 0 ||
		o.SpoolBlockBytes > 64<<20 ||
		o.SegmentBytes <= 64<<20 ||
		o.SnapshotDeltas == 0 ||
		o.SnapshotBytes == 0 ||
		o.StorageWorkers == 0 {
		return invalidErr("invalid worker limits (queue maximum 128 MiB)")
	}
	return nil
}

// Status reports database health and cursors.
type Status struct {
	SearchIndexes   []SearchIndexStatus
	Committed       uint64
	Durable         uint64
	PendingBytes    int
	StorageError    *string
	CheckpointPause time.Duration
	Closed          bool
	OpenProgress    *OpenProgress
	// OpenStages carries per-stage open timings. Always populated on
	// successful open, even without an OnOpenProgress callback.
	OpenStages  *OpenStageTimings
	Sqlite      *SqliteMetrics
	Spool       *SpoolMetrics
	Scheduler   *SchedulerSnapshot
	Replication *replication.Status
}
