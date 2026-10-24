// Murmur Operational Admin CLI (murmur-admin).
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/marcgauthier/murmur-sql/keyprovider"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/compression"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

const version = "0.1.0"

var defaultKey = [32]byte{
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
}

type globalOpts struct {
	json           bool
	passphrase     *string
	keyHex         *string
	keyProviderURI *string
	newPassphrase  *string
	newKeyHex      *string
	isBackup       bool
	freshNodeID    *[16]byte
	freshDBID      *[16]byte
	maxPasses      int
	outDir         *string
}

func (o *globalOpts) resolveCredential(dir string) (murmur.Credential, error) {
	sources := 0
	for _, s := range []*string{o.keyHex, o.passphrase, o.keyProviderURI} {
		if s != nil {
			sources++
		}
	}
	if sources > 1 {
		return murmur.Credential{}, fmt.Errorf("set at most one of --key-hex, --passphrase, and --key-provider-uri")
	}
	if o.keyProviderURI != nil {
		key, err := keyprovider.ResolveURI(context.Background(), *o.keyProviderURI)
		if err != nil {
			return murmur.Credential{}, err
		}
		out := murmur.KeyCredential(append([]byte(nil), key[:]...))
		clear(key[:])
		return out, nil
	}
	if o.keyHex != nil {
		raw, err := hex.DecodeString(strings.TrimSpace(*o.keyHex))
		if err != nil {
			return murmur.Credential{}, fmt.Errorf("invalid hex key: %v", err)
		}
		if len(raw) != 32 {
			return murmur.Credential{}, fmt.Errorf("key-hex must be exactly 32 bytes (64 hex characters)")
		}
		var key [32]byte
		copy(key[:], raw)
		return murmur.KeyCredential(key[:]), nil
	}
	if o.passphrase != nil {
		// Unified with SQL: a 64-hex secret is always a raw key, and
		// other passphrases take the envelope KDF. Unreadable
		// envelopes fall back to the legacy mapping so the open
		// reports the store's own error.
		kdf := uint8(spool.KDFDirect)
		if hint, err := spool.ReadKeyHint(dir); err == nil && hint.Encrypted {
			kdf = hint.KDF
		}
		return murmur.CredentialForSecret(*o.passphrase, kdf), nil
	}
	return murmur.KeyCredential(defaultKey[:]), nil
}

func hexID(id [16]byte) string { return hex.EncodeToString(id[:]) }

func parseID16(s string) ([16]byte, error) {
	var id [16]byte
	raw, err := hex.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return id, fmt.Errorf("invalid hex id: %v", err)
	}
	if len(raw) != 16 {
		return id, fmt.Errorf("expected 16-byte hex (32 chars), got %d bytes", len(raw))
	}
	copy(id[:], raw)
	return id, nil
}

func printUsage() {
	fmt.Printf(`Murmur Operational Admin CLI (v%s)

Usage:
  murmur-admin <command> [options] [arguments]

Commands:
  status   <data-dir>                      Database status and storage/SQLite metrics
  inspect  <data-dir>                      Low-level storage inspector (segments, schema, disk size)
  verify   <data-dir>                      Replay & verify durable state or backup directory
  backup   <data-dir> <dest-dir>           Consistent online hot backup
  restore  <backup-dir> <dest-dir>         Restore backup into clean destination directory
  keys     <data-dir>                      Inspect key registry and active data keys
  rekey    <data-dir>                      Re-protect the store under a new key or passphrase
  compact  <data-dir>                      Offline compaction (server must be stopped)
  repair   <data-dir> [--out=<dir>]        Salvage readable records into a new directory

Options:
  --json                                  Output structured JSON format
  --passphrase=<str>                      Passphrase for database encryption
  --key-hex=<hex>                         32-byte (64 hex char) raw encryption key
  --key-provider-uri=<uri>                Key provider URI (file:/env:/exec:) for the store key
  --new-passphrase=<str>                  New passphrase for rekey
  --new-key-hex=<hex>                     New 32-byte raw key for rekey
  --backup                                Treat target directory as backup archive in `+"`verify`"+`
  --fresh-node-id[=<hex>]                 Assign new node identity on restore
  --fresh-db-id[=<hex>]                   Assign new database identity on restore
  --max-passes=<n>                        Cap compact passes (default: run until quiescent)
  --out=<dir>                             Repair destination (default: <data-dir>.salvaged)
  -v, --version                           Print version
  -h, --help                              Print help
`, version)
}

func dirSize(path string) (uint64, int) {
	var total uint64
	var count int
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0, 0
	}
	for _, e := range entries {
		p := filepath.Join(path, e.Name())
		if e.IsDir() {
			subBytes, subCount := dirSize(p)
			total += subBytes
			count += subCount
		} else if info, err := e.Info(); err == nil {
			total += uint64(info.Size())
			count++
		}
	}
	return total, count
}

func openExistingDB(path string, cred murmur.Credential) (*murmur.Database, error) {
	options := murmur.DefaultOptions()
	options.Mode = murmur.OpenExisting
	db, err := murmur.OpenWithCredential(path, cred, options)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}
	return db, nil
}

// offlineSpoolOptions maps an admin credential to spool open options
// for the offline commands (compact, repair): the murmur data
// directory is the spool directory. It mirrors murmur's storage
// mapping (see spoolOptionsWithCredential): the zstd user codec
// reads the store's id-128 blocks, and block/cache/segment sizing
// matches so rewritten files keep the store's standard shape.
// Background reclamation stays off: compact drives ReclaimStep
// explicitly. Plaintext stores ignore key material, exactly like a
// database open.
func offlineSpoolOptions(dataDir string, cred murmur.Credential) spool.Options {
	mopts := murmur.DefaultOptions()
	opts := spool.DefaultOptions(dataDir)
	opts.MasterKey = append([]byte(nil), cred.Key...)
	opts.Passphrase = cred.Passphrase
	opts.Codec = compression.MustZstd(mopts.CompressionLevel)
	opts.TargetBlockBytes = mopts.SpoolBlockBytes
	opts.ReadCacheBytes = mopts.SpoolCacheBytes
	opts.Workers = mopts.StorageWorkers
	opts.Durability = spool.DurabilitySync
	opts.MaxSegmentSize = int64(mopts.SegmentBytes)
	opts.ReclaimInterval = -1
	return opts
}

// offlineCodec returns the block codec murmur stores write: repair
// registers it for reading the source and standardizes the
// destination on it.
func offlineCodec() compression.Codec {
	return compression.MustZstd(murmur.DefaultOptions().CompressionLevel)
}

func printJSON(v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(raw))
	return nil
}

// status command

type sqliteReport struct {
	MemoryUsedBytes       int64  `json:"memory_used_bytes"`
	MemoryHighwaterBytes  int64  `json:"memory_highwater_bytes"`
	PageSize              int64  `json:"page_size"`
	PageCount             int64  `json:"page_count"`
	DatabaseSizeBytes     int64  `json:"database_size_bytes"`
	TableCount            int    `json:"table_count"`
	IndexCount            int    `json:"index_count"`
	TotalRows             uint64 `json:"total_rows"`
	SessionChangesetBytes int    `json:"session_changeset_bytes"`
}

type spoolReport struct {
	Keys             uint64  `json:"keys"`
	Segments         uint64  `json:"segments"`
	DiskBytes        uint64  `json:"disk_bytes"`
	LiveBytes        uint64  `json:"live_bytes"`
	LiveRecords      uint64  `json:"live_records"`
	DeadRecords      uint64  `json:"dead_records"`
	BlocksWritten    uint64  `json:"blocks_written"`
	CompressionRatio float64 `json:"compression_ratio"`
	Compactions      uint64  `json:"compactions"`
	Commits          uint64  `json:"commits"`
}

type statusReport struct {
	DataDir           string        `json:"data_dir"`
	DatabaseID        string        `json:"database_id"`
	NodeID            string        `json:"node_id"`
	CommittedSeq      uint64        `json:"committed_seq"`
	DurableSeq        uint64        `json:"durable_seq"`
	PendingBytes      int           `json:"pending_bytes"`
	CheckpointPauseMs uint64        `json:"checkpoint_pause_ms"`
	Closed            bool          `json:"closed"`
	StorageError      *string       `json:"storage_error"`
	Sqlite            *sqliteReport `json:"sqlite"`
	Spool             *spoolReport  `json:"spool"`
}

func cmdStatus(dataDir string, opts *globalOpts) error {
	if _, err := os.Stat(dataDir); err != nil {
		return fmt.Errorf("directory does not exist: %s", dataDir)
	}
	cred, err := opts.resolveCredential(dataDir)
	if err != nil {
		return err
	}
	db, err := openExistingDB(dataDir, cred)
	if err != nil {
		return err
	}
	status := db.Status()
	sqliteMetrics, _ := db.SqliteMetrics()
	spoolMetrics := db.SpoolMetrics()
	dbID := hexID(db.DatabaseID())
	nodeID := hexID(db.NodeID())
	report := statusReport{
		DataDir: dataDir, DatabaseID: dbID, NodeID: nodeID,
		CommittedSeq: status.Committed, DurableSeq: status.Durable,
		PendingBytes:      status.PendingBytes,
		CheckpointPauseMs: uint64(status.CheckpointPause.Milliseconds()),
		Closed:            status.Closed,
		StorageError:      status.StorageError,
	}
	if sqliteMetrics != nil {
		report.Sqlite = &sqliteReport{
			MemoryUsedBytes: sqliteMetrics.MemoryUsedBytes, MemoryHighwaterBytes: sqliteMetrics.MemoryHighwaterBytes,
			PageSize: sqliteMetrics.PageSize, PageCount: sqliteMetrics.PageCount,
			DatabaseSizeBytes: sqliteMetrics.DatabaseSizeBytes, TableCount: sqliteMetrics.TableCount,
			IndexCount: sqliteMetrics.IndexCount, TotalRows: sqliteMetrics.TotalRows,
			SessionChangesetBytes: sqliteMetrics.SessionChangesetBytes,
		}
	}
	if spoolMetrics != nil {
		report.Spool = &spoolReport{
			Keys: spoolMetrics.Keys, Segments: spoolMetrics.Segments, DiskBytes: spoolMetrics.DiskBytes,
			LiveBytes: spoolMetrics.LiveBytes, LiveRecords: spoolMetrics.LiveRecords,
			DeadRecords: spoolMetrics.DeadRecords, BlocksWritten: spoolMetrics.BlocksWritten,
			CompressionRatio: spoolMetrics.CompressionRatio, Compactions: spoolMetrics.Compactions,
			Commits: spoolMetrics.Commits,
		}
	}
	_ = db.Close()
	if opts.json {
		return printJSON(report)
	}
	fmt.Println("=== Murmur Database Status ===")
	fmt.Printf("Data Directory:      %s\n", report.DataDir)
	fmt.Printf("Database ID:         %s\n", report.DatabaseID)
	fmt.Printf("Node ID:             %s\n", report.NodeID)
	fmt.Printf("Committed Sequence:  %d\n", report.CommittedSeq)
	fmt.Printf("Durable Sequence:    %d\n", report.DurableSeq)
	fmt.Printf("Pending Bytes:       %d\n", report.PendingBytes)
	if report.StorageError != nil {
		fmt.Printf("Storage Error:       %s\n", *report.StorageError)
	}
	if report.Sqlite != nil {
		s := report.Sqlite
		fmt.Println("\n--- SQLite In-Memory Engine ---")
		fmt.Printf("Memory Used:         %d bytes (highwater: %d bytes)\n", s.MemoryUsedBytes, s.MemoryHighwaterBytes)
		fmt.Printf("Page Count / Size:   %d pages @ %d bytes\n", s.PageCount, s.PageSize)
		fmt.Printf("Database Size:       %d bytes\n", s.DatabaseSizeBytes)
		fmt.Printf("Tables / Indexes:    %d tables, %d indexes\n", s.TableCount, s.IndexCount)
		fmt.Printf("Total Rows:          %d\n", s.TotalRows)
		fmt.Printf("Session Changeset:   %d bytes\n", s.SessionChangesetBytes)
	}
	if report.Spool != nil {
		sp := report.Spool
		fmt.Println("\n--- Spool Persistence Engine ---")
		fmt.Printf("Keys Tracked:        %d\n", sp.Keys)
		fmt.Printf("Segments / Disk:     %d segments (%d bytes)\n", sp.Segments, sp.DiskBytes)
		fmt.Printf("Live Records / Bytes:%d records (%d bytes)\n", sp.LiveRecords, sp.LiveBytes)
		fmt.Printf("Dead Records:        %d\n", sp.DeadRecords)
		fmt.Printf("Blocks Written:      %d\n", sp.BlocksWritten)
		fmt.Printf("Compression Ratio:   %.2fx\n", sp.CompressionRatio)
		fmt.Printf("Compactions / Commits:%d compactions, %d commits\n", sp.Compactions, sp.Commits)
	}
	return nil
}

// inspect command

type tableSummary struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	ColumnsCount int    `json:"columns_count"`
	PrimaryKey   int    `json:"primary_key"`
}

type inspectReport struct {
	DataDir         string         `json:"data_dir"`
	DatabaseID      string         `json:"database_id"`
	NodeID          string         `json:"node_id"`
	StorageFiles    int            `json:"storage_files"`
	StorageDiskByte uint64         `json:"storage_disk_bytes"`
	WrappingKeyID   string         `json:"wrapping_key_id"`
	KeyringSeq      uint64         `json:"keyring_seq"`
	TableCount      int            `json:"table_count"`
	IndexCount      int            `json:"index_count"`
	Tables          []tableSummary `json:"tables"`
}

func cmdInspect(dataDir string, opts *globalOpts) error {
	if _, err := os.Stat(dataDir); err != nil {
		return fmt.Errorf("directory does not exist: %s", dataDir)
	}
	diskBytes, fileCount := dirSize(dataDir)
	hint, _ := spool.ReadKeyHint(dataDir)
	cred, err := opts.resolveCredential(dataDir)
	if err != nil {
		return err
	}
	db, err := openExistingDB(dataDir, cred)
	if err != nil {
		return err
	}
	dbID := hexID(db.DatabaseID())
	nodeID := hexID(db.NodeID())
	schema, _ := db.Schema()
	_ = db.Close()
	var tables []tableSummary
	tableCount, indexCount := 0, 0
	if schema != nil {
		tableCount, indexCount = len(schema.Tables), len(schema.Indexes)
		for _, t := range schema.Tables {
			pk := 0
			for _, c := range t.Columns {
				if c.PrimaryKey > 0 {
					pk = c.PrimaryKey
					break
				}
			}
			tables = append(tables, tableSummary{ID: t.ID, Name: t.Name, ColumnsCount: len(t.Columns), PrimaryKey: pk})
		}
	}
	report := inspectReport{
		DataDir: dataDir, DatabaseID: dbID, NodeID: nodeID,
		StorageFiles: fileCount, StorageDiskByte: diskBytes,
		WrappingKeyID: hint.WrappingKeyID, KeyringSeq: hint.KeyringSeq,
		TableCount: tableCount, IndexCount: indexCount, Tables: tables,
	}
	if opts.json {
		return printJSON(report)
	}
	fmt.Println("=== Murmur Storage Inspection ===")
	fmt.Printf("Data Directory:      %s\n", report.DataDir)
	fmt.Printf("Database ID:         %s\n", report.DatabaseID)
	fmt.Printf("Node ID:             %s\n", report.NodeID)
	fmt.Printf("Files On Disk:       %d files (%d bytes / %.2f MB)\n",
		report.StorageFiles, report.StorageDiskByte, float64(report.StorageDiskByte)/(1024.0*1024.0))
	fmt.Printf("Wrapping Key ID:     %s\n", report.WrappingKeyID)
	fmt.Printf("Keyring Sequence:    %d\n", report.KeyringSeq)
	fmt.Printf("Tables Registered:   %d\n", report.TableCount)
	fmt.Printf("Indexes Registered:  %d\n", report.IndexCount)
	if len(report.Tables) > 0 {
		fmt.Println("\n--- Tables ---")
		for _, t := range report.Tables {
			fmt.Printf("  • %-20s (columns: %d, id: %s)\n", t.Name, t.ColumnsCount, t.ID)
		}
	}
	return nil
}

// verify command

type verifyReport struct {
	TargetPath    string   `json:"target_path"`
	Valid         bool     `json:"valid"`
	Mode          string   `json:"mode"`
	DatabaseID    *string  `json:"database_id"`
	NodeID        *string  `json:"node_id"`
	TablesChecked int      `json:"tables_checked"`
	Errors        []string `json:"errors"`
}

func cmdVerify(target string, opts *globalOpts) error {
	if _, err := os.Stat(target); err != nil {
		return fmt.Errorf("target does not exist: %s", target)
	}
	cred, err := opts.resolveCredential(target)
	if err != nil {
		return err
	}
	if opts.isBackup {
		verr := murmur.VerifyBackupDirWithCredential(target, cred)
		report := verifyReport{TargetPath: target, Valid: verr == nil, Mode: "backup", Errors: []string{}}
		if verr != nil {
			report.Errors = append(report.Errors, verr.Error())
		}
		if opts.json {
			_ = printJSON(report)
			if !report.Valid {
				return fmt.Errorf("verification failed")
			}
			return nil
		}
		if report.Valid {
			fmt.Printf("PASS: Backup directory verified successfully at %s\n", target)
			return nil
		}
		fmt.Printf("FAIL: Backup verification failed at %s:\n", target)
		for _, e := range report.Errors {
			fmt.Printf("  - %s\n", e)
		}
		return fmt.Errorf("verification failed")
	}
	var errors []string
	var dbID, nodeID *string
	tableCount := 0
	if db, err := openExistingDB(target, cred); err != nil {
		errors = append(errors, err.Error())
	} else {
		id := hexID(db.DatabaseID())
		nid := hexID(db.NodeID())
		dbID, nodeID = &id, &nid
		if schema, err := db.Schema(); err == nil && schema != nil {
			tableCount = len(schema.Tables)
		}
		_ = db.Close()
	}
	report := verifyReport{TargetPath: target, Valid: len(errors) == 0, Mode: "database",
		DatabaseID: dbID, NodeID: nodeID, TablesChecked: tableCount, Errors: errors}
	if report.Errors == nil {
		report.Errors = []string{}
	}
	if opts.json {
		_ = printJSON(report)
		if !report.Valid {
			return fmt.Errorf("verification failed")
		}
		return nil
	}
	if report.Valid {
		fmt.Printf("PASS: Database storage integrity verified at %s\n", target)
		fmt.Printf("Database ID:     %s\n", *report.DatabaseID)
		fmt.Printf("Node ID:         %s\n", *report.NodeID)
		fmt.Printf("Tables Checked:  %d\n", report.TablesChecked)
		return nil
	}
	fmt.Printf("FAIL: Storage verification failed at %s:\n", target)
	for _, e := range report.Errors {
		fmt.Printf("  - %s\n", e)
	}
	return fmt.Errorf("verification failed")
}

// backup command

type backupReport struct {
	SourceDir      string `json:"source_dir"`
	DestinationDir string `json:"destination_dir"`
	DatabaseID     string `json:"database_id"`
	NodeID         string `json:"node_id"`
	Success        bool   `json:"success"`
}

func cmdBackup(source, dest string, opts *globalOpts) error {
	if _, err := os.Stat(source); err != nil {
		return fmt.Errorf("source directory does not exist: %s", source)
	}
	if _, err := os.Stat(dest); err == nil {
		return fmt.Errorf("destination path already exists: %s", dest)
	}
	cred, err := opts.resolveCredential(source)
	if err != nil {
		return err
	}
	db, err := openExistingDB(source, cred)
	if err != nil {
		return err
	}
	dbID := hexID(db.DatabaseID())
	nodeID := hexID(db.NodeID())
	berr := db.Backup(dest)
	_ = db.Close()
	if berr != nil {
		return fmt.Errorf("backup failed: %v", berr)
	}
	report := backupReport{SourceDir: source, DestinationDir: dest, DatabaseID: dbID, NodeID: nodeID, Success: true}
	if opts.json {
		return printJSON(report)
	}
	fmt.Println("PASS: Consistent hot backup created successfully.")
	fmt.Printf("Source:       %s\n", report.SourceDir)
	fmt.Printf("Destination:  %s\n", report.DestinationDir)
	fmt.Printf("Database ID:  %s\n", report.DatabaseID)
	fmt.Printf("Node ID:      %s\n", report.NodeID)
	return nil
}

// restore command

type restoreReport struct {
	BackupDir      string  `json:"backup_dir"`
	DestinationDir string  `json:"destination_dir"`
	Success        bool    `json:"success"`
	NewDatabaseID  *string `json:"new_database_id"`
	NewNodeID      *string `json:"new_node_id"`
}

func cmdRestore(backupDir, destDir string, opts *globalOpts) error {
	if _, err := os.Stat(backupDir); err != nil {
		return fmt.Errorf("backup directory does not exist: %s", backupDir)
	}
	if _, err := os.Stat(destDir); err == nil {
		return fmt.Errorf("destination path already exists: %s", destDir)
	}
	cred, err := opts.resolveCredential(backupDir)
	if err != nil {
		return err
	}
	restoreOpts := murmur.DefaultRestoreOptions()
	restoreOpts.NewDatabaseID = opts.freshDBID
	restoreOpts.NewNodeID = opts.freshNodeID
	if err := murmur.RestoreBackupWithCredential(backupDir, destDir, cred, &restoreOpts); err != nil {
		return fmt.Errorf("restore failed: %v", err)
	}
	report := restoreReport{BackupDir: backupDir, DestinationDir: destDir, Success: true}
	if opts.freshDBID != nil {
		id := hex.EncodeToString(opts.freshDBID[:])
		report.NewDatabaseID = &id
	}
	if opts.freshNodeID != nil {
		id := hex.EncodeToString(opts.freshNodeID[:])
		report.NewNodeID = &id
	}
	if opts.json {
		return printJSON(report)
	}
	fmt.Println("PASS: Database restored successfully.")
	fmt.Printf("Backup Source:    %s\n", report.BackupDir)
	fmt.Printf("Destination:      %s\n", report.DestinationDir)
	if report.NewDatabaseID != nil {
		fmt.Printf("New Database ID:  %s\n", *report.NewDatabaseID)
	}
	if report.NewNodeID != nil {
		fmt.Printf("New Node ID:      %s\n", *report.NewNodeID)
	}
	return nil
}

// keys command

type keyEntry struct {
	ID                 uint32 `json:"id"`
	IDHex              string `json:"id_hex"`
	CreatedAtEpochSecs uint64 `json:"created_at_epoch_secs"`
	Status             string `json:"status"`
}

type keysReport struct {
	DataDir            string     `json:"data_dir"`
	Encrypted          bool       `json:"encrypted"`
	WrappingKeyID      string     `json:"wrapping_key_id"`
	KDF                string     `json:"kdf"`
	ActiveDataKeyID    uint32     `json:"active_data_key_id"`
	KeyringSeq         uint64     `json:"keyring_seq"`
	ManifestGeneration uint64     `json:"manifest_generation"`
	DataKeysCount      int        `json:"data_keys_count"`
	DataKeys           []keyEntry `json:"data_keys"`
}

func cmdKeys(dataDir string, opts *globalOpts) error {
	if _, err := os.Stat(dataDir); err != nil {
		return fmt.Errorf("directory does not exist: %s", dataDir)
	}
	cred, err := opts.resolveCredential(dataDir)
	if err != nil {
		return err
	}
	db, err := openExistingDB(dataDir, cred)
	if err != nil {
		return err
	}
	inv := db.KeyInventory()
	_ = db.Close()
	var dataKeys []keyEntry
	for _, k := range inv.DataKeys {
		status := "retired"
		if k.Status == spool.KeyStatusActive {
			status = "active"
		}
		dataKeys = append(dataKeys, keyEntry{
			ID: k.ID, IDHex: fmt.Sprintf("%08x", k.ID),
			CreatedAtEpochSecs: uint64(k.CreatedAt.Unix()),
			Status:             status,
		})
	}
	kdf := "unknown"
	if hint, err := spool.ReadKeyHint(dataDir); err == nil {
		kdf = kdfName(hint.KDF)
	}
	report := keysReport{
		DataDir: dataDir, Encrypted: inv.Encrypted, WrappingKeyID: inv.WrappingKeyID, KDF: kdf,
		ActiveDataKeyID: inv.ActiveDataKeyID, KeyringSeq: inv.KeyringSeq,
		ManifestGeneration: inv.ManifestGeneration, DataKeysCount: len(dataKeys), DataKeys: dataKeys,
	}
	if opts.json {
		return printJSON(report)
	}
	fmt.Println("=== Murmur Key Registry & Inventory ===")
	fmt.Printf("Data Directory:         %s\n", report.DataDir)
	fmt.Printf("Encrypted:              %v\n", report.Encrypted)
	fmt.Printf("Wrapping Key ID:        %s\n", report.WrappingKeyID)
	fmt.Printf("Envelope KDF:           %s\n", report.KDF)
	fmt.Printf("Active Data Key ID:     %08x (#%d)\n", report.ActiveDataKeyID, report.ActiveDataKeyID)
	fmt.Printf("Keyring Persist Seq:    %d\n", report.KeyringSeq)
	fmt.Printf("Manifest Generation:    %d\n", report.ManifestGeneration)
	fmt.Printf("Total Data Keys:        %d\n", report.DataKeysCount)
	if len(report.DataKeys) > 0 {
		fmt.Println("\n  Key ID    Status    Created At (UNIX secs)")
		fmt.Println("  --------  --------  ----------------------")
		for _, k := range report.DataKeys {
			fmt.Printf("  %8s  %-8s  %d\n", k.IDHex, k.Status, k.CreatedAtEpochSecs)
		}
	}
	return nil
}

// kdfName renders a keys.enc KDF id for reports.
func kdfName(kdf uint8) string {
	switch kdf {
	case spool.KDFArgon2id:
		return "argon2id"
	case spool.KDFDirect:
		return "direct"
	default:
		return fmt.Sprintf("unknown(%d)", kdf)
	}
}

type rekeyReport struct {
	DataDir string `json:"data_dir"`
	Success bool   `json:"success"`
	KDF     string `json:"kdf"`
}

func cmdRekey(dataDir string, opts *globalOpts) error {
	if _, err := os.Stat(dataDir); err != nil {
		return fmt.Errorf("directory does not exist: %s", dataDir)
	}
	if (opts.newKeyHex != nil) == (opts.newPassphrase != nil) {
		return fmt.Errorf("rekey needs exactly one of --new-key-hex and --new-passphrase")
	}
	cred, err := opts.resolveCredential(dataDir)
	if err != nil {
		return err
	}
	db, err := openExistingDB(dataDir, cred)
	if err != nil {
		return err
	}
	var newCred murmur.Credential
	if opts.newKeyHex != nil {
		raw, err := hex.DecodeString(strings.TrimSpace(*opts.newKeyHex))
		if err != nil || len(raw) != 32 {
			_ = db.Close()
			return fmt.Errorf("new-key-hex must be exactly 32 bytes (64 hex characters)")
		}
		var key [32]byte
		copy(key[:], raw)
		newCred = murmur.KeyCredential(key[:])
	} else {
		// Classify against Argon2id: a 64-hex secret is always a
		// raw key, anything else becomes the new passphrase.
		newCred = murmur.CredentialForSecret(*opts.newPassphrase, spool.KDFArgon2id)
	}
	if err := db.RotateCredential(newCred); err != nil {
		_ = db.Close()
		return fmt.Errorf("rekey failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		_ = db.Close()
		return fmt.Errorf("flush after rekey failed: %v", err)
	}
	_ = db.Close()
	kdf := "unknown"
	if hint, err := spool.ReadKeyHint(dataDir); err == nil {
		kdf = kdfName(hint.KDF)
	}
	report := rekeyReport{DataDir: dataDir, Success: true, KDF: kdf}
	if opts.json {
		return printJSON(report)
	}
	fmt.Println("PASS: Store re-protected successfully.")
	fmt.Printf("Data Directory:   %s\n", report.DataDir)
	fmt.Printf("Envelope KDF:     %s\n", report.KDF)
	return nil
}

// compact command

type compactReport struct {
	DataDir           string `json:"data_dir"`
	Passes            int    `json:"passes"`
	Quiesced          bool   `json:"quiesced"`
	SegmentsBefore    uint64 `json:"segments_before"`
	SegmentsAfter     uint64 `json:"segments_after"`
	DiskBytesBefore   uint64 `json:"disk_bytes_before"`
	DiskBytesAfter    uint64 `json:"disk_bytes_after"`
	DeadRecordsBefore uint64 `json:"dead_records_before"`
	DeadRecordsAfter  uint64 `json:"dead_records_after"`
	LiveRecords       uint64 `json:"live_records"`
	Compactions       uint64 `json:"compactions"`
	ReclaimedBytes    uint64 `json:"reclaimed_bytes"`
}

func cmdCompact(dataDir string, opts *globalOpts) error {
	if _, err := os.Stat(dataDir); err != nil {
		return fmt.Errorf("directory does not exist: %s", dataDir)
	}
	if opts.maxPasses < 0 {
		return fmt.Errorf("--max-passes must be >= 0")
	}
	cred, err := opts.resolveCredential(dataDir)
	if err != nil {
		return err
	}
	st, err := spool.Open(offlineSpoolOptions(dataDir, cred))
	if err != nil {
		if errors.Is(err, spool.ErrLocked) {
			return fmt.Errorf("store is locked (server running?): stop the server and retry: %v", err)
		}
		return fmt.Errorf("failed to open store: %v", err)
	}
	before := st.Stats()
	// Exclusive open (LOCK): no writer can interleave, so the
	// until-quiescent loop provably terminates. --max-passes is
	// an escape hatch, not the norm.
	passes := 0
	quiesced := false
	for {
		if opts.maxPasses > 0 && passes >= opts.maxPasses {
			break
		}
		worked, err := st.ReclaimStep()
		passes++
		if err != nil {
			_ = st.Close()
			return fmt.Errorf("compaction pass %d failed: %v", passes, err)
		}
		if !worked {
			quiesced = true
			break
		}
	}
	after := st.Stats()
	if err := st.Close(); err != nil {
		return fmt.Errorf("close after compact failed: %v", err)
	}
	var reclaimed uint64
	if after.DiskBytes < before.DiskBytes {
		reclaimed = before.DiskBytes - after.DiskBytes
	}
	report := compactReport{
		DataDir: dataDir, Passes: passes, Quiesced: quiesced,
		SegmentsBefore: before.Segments, SegmentsAfter: after.Segments,
		DiskBytesBefore: before.DiskBytes, DiskBytesAfter: after.DiskBytes,
		DeadRecordsBefore: before.DeadRecords, DeadRecordsAfter: after.DeadRecords,
		LiveRecords: after.LiveRecords, Compactions: after.Compactions - before.Compactions,
		ReclaimedBytes: reclaimed,
	}
	if opts.json {
		return printJSON(report)
	}
	fmt.Println("PASS: Offline compaction complete.")
	fmt.Printf("Data Directory:      %s\n", report.DataDir)
	fmt.Printf("Passes:              %d (%s)\n", report.Passes, map[bool]string{true: "quiesced", false: "capped by --max-passes"}[report.Quiesced])
	fmt.Printf("Segments:            %d -> %d\n", report.SegmentsBefore, report.SegmentsAfter)
	fmt.Printf("Disk Bytes:          %d -> %d (reclaimed %d)\n", report.DiskBytesBefore, report.DiskBytesAfter, report.ReclaimedBytes)
	fmt.Printf("Dead Records:        %d -> %d\n", report.DeadRecordsBefore, report.DeadRecordsAfter)
	fmt.Printf("Live Records:        %d\n", report.LiveRecords)
	fmt.Printf("Compactions:         %d\n", report.Compactions)
	return nil
}

// repair command

type repairGap struct {
	File   uint64 `json:"file"`
	Start  int64  `json:"start"`
	End    int64  `json:"end"`
	Reason string `json:"reason"`
	Detail string `json:"detail"`
}

type repairReport struct {
	SourceDir  string      `json:"source_dir"`
	DestDir    string      `json:"dest_dir"`
	Records    int64       `json:"records"`
	Files      int         `json:"files"`
	Gaps       []repairGap `json:"gaps"`
	GapCount   int         `json:"gap_count"`
	Verified   bool        `json:"verified"`
	DatabaseID *string     `json:"database_id"`
}

func cmdRepair(dataDir string, opts *globalOpts) error {
	if _, err := os.Stat(dataDir); err != nil {
		return fmt.Errorf("directory does not exist: %s", dataDir)
	}
	destDir := dataDir + ".salvaged"
	if opts.outDir != nil {
		destDir = *opts.outDir
	}
	if _, err := os.Stat(destDir); err == nil {
		return fmt.Errorf("destination path already exists: %s", destDir)
	}
	cred, err := opts.resolveCredential(dataDir)
	if err != nil {
		return err
	}
	zc := offlineCodec()
	rep, err := spool.Salvage(spool.SalvageOptions{
		SrcDir:     dataDir,
		DestDir:    destDir,
		MasterKey:  cred.Key,
		Passphrase: cred.Passphrase,
		Codecs:     []compression.Codec{zc},
		Codec:      zc,
	})
	if err != nil {
		if errors.Is(err, spool.ErrLocked) {
			return fmt.Errorf("store is locked (server running?): stop the server and retry: %v", err)
		}
		return fmt.Errorf("salvage failed (destination left for forensics): %v", err)
	}
	gaps := make([]repairGap, 0, len(rep.Gaps))
	for _, g := range rep.Gaps {
		gaps = append(gaps, repairGap{
			File: g.File, Start: g.Start, End: g.End,
			Reason: string(g.Reason), Detail: g.Detail,
		})
	}
	// Prove the salvaged store opens as a database before handing
	// it to the operator: spool-readable is necessary, not
	// sufficient.
	report := repairReport{
		SourceDir: dataDir, DestDir: destDir,
		Records: rep.Records, Files: rep.Files, Gaps: gaps, GapCount: len(gaps),
	}
	db, err := openExistingDB(destDir, cred)
	if err != nil {
		report.Verified = false
		if opts.json {
			_ = printJSON(report)
		}
		return fmt.Errorf("salvaged store failed database verification (destination left for forensics): %v", err)
	}
	id := hexID(db.DatabaseID())
	report.DatabaseID = &id
	_ = db.Close()
	report.Verified = true
	if opts.json {
		return printJSON(report)
	}
	if len(gaps) == 0 {
		fmt.Println("PASS: Store salvaged with no gaps; destination verified as a database.")
	} else {
		fmt.Printf("PASS: Store salvaged with %d gap(s); destination verified as a database.\n", len(gaps))
		fmt.Println("Gaps (unrecoverable source byte ranges):")
		for _, g := range gaps {
			end := fmt.Sprintf("%d", g.End)
			if g.End < 0 {
				end = "?"
			}
			fmt.Printf("  • segment %d bytes %d-%s (%s): %s\n", g.File, g.Start, end, g.Reason, g.Detail)
		}
	}
	fmt.Printf("Source:       %s (untouched)\n", report.SourceDir)
	fmt.Printf("Destination:  %s\n", report.DestDir)
	fmt.Printf("Records:      %d from %d file(s)\n", report.Records, report.Files)
	fmt.Printf("Database ID:  %s\n", *report.DatabaseID)
	fmt.Println("Next: point the server at the destination, or move it over the source while stopped.")
	return nil
}

func randomID16() ([16]byte, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return id, err
}

func main() {
	os.Exit(run())
}

func run() int {
	args := os.Args
	if len(args) < 2 {
		printUsage()
		return 0
	}
	opts := &globalOpts{}
	var positional []string
	i := 1
	for i < len(args) {
		arg := args[i]
		switch {
		case arg == "-h" || arg == "--help":
			printUsage()
			return 0
		case arg == "-v" || arg == "--version":
			fmt.Printf("murmur-admin version %s\n", version)
			return 0
		case arg == "--json":
			opts.json = true
		case arg == "--backup":
			opts.isBackup = true
		case strings.HasPrefix(arg, "--passphrase="):
			v := strings.TrimPrefix(arg, "--passphrase=")
			opts.passphrase = &v
		case arg == "--passphrase" && i+1 < len(args):
			i++
			opts.passphrase = &args[i]
		case strings.HasPrefix(arg, "--key-hex="):
			v := strings.TrimPrefix(arg, "--key-hex=")
			opts.keyHex = &v
		case arg == "--key-hex" && i+1 < len(args):
			i++
			opts.keyHex = &args[i]
		case strings.HasPrefix(arg, "--key-provider-uri="):
			v := strings.TrimPrefix(arg, "--key-provider-uri=")
			opts.keyProviderURI = &v
		case arg == "--key-provider-uri" && i+1 < len(args):
			i++
			opts.keyProviderURI = &args[i]
		case strings.HasPrefix(arg, "--new-passphrase="):
			v := strings.TrimPrefix(arg, "--new-passphrase=")
			opts.newPassphrase = &v
		case arg == "--new-passphrase" && i+1 < len(args):
			i++
			opts.newPassphrase = &args[i]
		case strings.HasPrefix(arg, "--new-key-hex="):
			v := strings.TrimPrefix(arg, "--new-key-hex=")
			opts.newKeyHex = &v
		case arg == "--new-key-hex" && i+1 < len(args):
			i++
			opts.newKeyHex = &args[i]
		case arg == "--fresh-node-id":
			id, err := randomID16()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				return 1
			}
			opts.freshNodeID = &id
		case strings.HasPrefix(arg, "--fresh-node-id="):
			id, err := parseID16(strings.TrimPrefix(arg, "--fresh-node-id="))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				return 1
			}
			opts.freshNodeID = &id
		case arg == "--fresh-db-id":
			id, err := randomID16()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				return 1
			}
			opts.freshDBID = &id
		case strings.HasPrefix(arg, "--fresh-db-id="):
			id, err := parseID16(strings.TrimPrefix(arg, "--fresh-db-id="))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				return 1
			}
			opts.freshDBID = &id
		case strings.HasPrefix(arg, "--max-passes="):
			n, err := strconv.Atoi(strings.TrimPrefix(arg, "--max-passes="))
			if err != nil || n < 0 {
				fmt.Fprintf(os.Stderr, "Error: --max-passes must be >= 0\n")
				return 1
			}
			opts.maxPasses = n
		case arg == "--max-passes" && i+1 < len(args):
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil || n < 0 {
				fmt.Fprintf(os.Stderr, "Error: --max-passes must be >= 0\n")
				return 1
			}
			opts.maxPasses = n
		case strings.HasPrefix(arg, "--out="):
			v := strings.TrimPrefix(arg, "--out=")
			opts.outDir = &v
		case arg == "--out" && i+1 < len(args):
			i++
			opts.outDir = &args[i]
		default:
			positional = append(positional, arg)
		}
		i++
	}
	if len(positional) == 0 {
		printUsage()
		return 0
	}
	cmd, cmdArgs := positional[0], positional[1:]
	var result error
	switch cmd {
	case "status":
		if len(cmdArgs) == 0 {
			result = fmt.Errorf("missing <data-dir>. Usage: murmur-admin status <data-dir>")
		} else {
			result = cmdStatus(cmdArgs[0], opts)
		}
	case "inspect":
		if len(cmdArgs) == 0 {
			result = fmt.Errorf("missing <data-dir>. Usage: murmur-admin inspect <data-dir>")
		} else {
			result = cmdInspect(cmdArgs[0], opts)
		}
	case "verify":
		if len(cmdArgs) == 0 {
			result = fmt.Errorf("missing <data-dir>. Usage: murmur-admin verify <data-dir> [--backup]")
		} else {
			result = cmdVerify(cmdArgs[0], opts)
		}
	case "backup":
		if len(cmdArgs) < 2 {
			result = fmt.Errorf("missing arguments. Usage: murmur-admin backup <source-dir> <dest-dir>")
		} else {
			result = cmdBackup(cmdArgs[0], cmdArgs[1], opts)
		}
	case "restore":
		if len(cmdArgs) < 2 {
			result = fmt.Errorf("missing arguments. Usage: murmur-admin restore <backup-dir> <dest-dir>")
		} else {
			result = cmdRestore(cmdArgs[0], cmdArgs[1], opts)
		}
	case "keys":
		if len(cmdArgs) == 0 {
			result = fmt.Errorf("missing <data-dir>. Usage: murmur-admin keys <data-dir>")
		} else {
			result = cmdKeys(cmdArgs[0], opts)
		}
	case "rekey":
		if len(cmdArgs) == 0 {
			result = fmt.Errorf("missing <data-dir>. Usage: murmur-admin rekey <data-dir> --new-passphrase <str> | --new-key-hex <hex>")
		} else {
			result = cmdRekey(cmdArgs[0], opts)
		}
	case "compact":
		if len(cmdArgs) == 0 {
			result = fmt.Errorf("missing <data-dir>. Usage: murmur-admin compact <data-dir> [--max-passes <n>]")
		} else {
			result = cmdCompact(cmdArgs[0], opts)
		}
	case "repair":
		if len(cmdArgs) == 0 {
			result = fmt.Errorf("missing <data-dir>. Usage: murmur-admin repair <data-dir> [--out <dir>]")
		} else {
			result = cmdRepair(cmdArgs[0], opts)
		}
	default:
		result = fmt.Errorf("unknown command: %q. Run 'murmur-admin --help' for usage.", cmd)
	}
	if result != nil {
		if opts.json {
			raw, _ := json.Marshal(map[string]any{"error": true, "message": result.Error()})
			fmt.Fprintln(os.Stderr, string(raw))
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", result)
		}
		return 1
	}
	return 0
}
