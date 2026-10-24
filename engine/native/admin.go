package native

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcgauthier/murmur-sql/engine"
	"github.com/marcgauthier/murmur-sql/keyprovider"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

type adminReport struct {
	values map[string]any
	human  string
}

func (r adminReport) MarshalJSON() ([]byte, error) { return json.Marshal(r.values) }
func (r adminReport) String() string               { return r.human }
func (Factory) Admin(ctx context.Context, r engine.AdminRequest) (result any, err error) {
	defer clear(r.Key[:])
	defer clear(r.NewKey[:])
	defer func() { r.Passphrase, r.NewPassphrase = "", "" }()
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if _, e := os.Stat(r.DataDir); e != nil {
		return nil, translate(e)
	}
	var providerKey [32]byte
	useProvider := false
	if r.KeyProviderURI != "" {
		k, e := keyprovider.ResolveURI(ctx, r.KeyProviderURI)
		if e != nil {
			return nil, translate(e)
		}
		providerKey, useProvider = k, true
		clear(k[:])
	}
	defer clear(providerKey[:])
	credFor := func(dir string) murmur.Credential {
		if useProvider {
			return murmur.KeyCredential(providerKey[:])
		}
		return credentialFor(dir, r.Key, r.Passphrase)
	}
	var db *murmur.Database
	open := func() error {
		options := murmur.DefaultOptions()
		options.Mode = murmur.OpenExisting
		var e error
		db, e = murmur.OpenWithCredential(r.DataDir, credFor(r.DataDir), options)
		return translate(e)
	}
	defer func() {
		if db != nil {
			if e := db.Close(); err == nil {
				err = translate(e)
			}
		}
	}()
	hexID := func(id [16]byte) string { return hex.EncodeToString(id[:]) }
	report := map[string]any{}
	switch r.Operation {
	case "status":
		if e := open(); e != nil {
			return nil, e
		}
		status := db.Status()
		sqlite, e := db.SqliteMetrics()
		if e != nil {
			return nil, translate(e)
		}
		spool := db.SpoolMetrics()
		report = map[string]any{"data_dir": r.DataDir, "database_id": hexID(db.DatabaseID()), "node_id": hexID(db.NodeID()), "committed_seq": status.Committed, "durable_seq": status.Durable, "pending_bytes": status.PendingBytes, "checkpoint_pause_ms": status.CheckpointPause.Milliseconds(), "closed": status.Closed, "storage_error": status.StorageError, "sqlite": selectFields(jsonFieldsValue(sqlite), "memory_used_bytes", "memory_highwater_bytes", "page_size", "page_count", "database_size_bytes", "table_count", "index_count", "total_rows", "session_changeset_bytes"), "spool": selectFields(jsonFieldsValue(spool), "keys", "segments", "disk_bytes", "live_bytes", "live_records", "dead_records", "blocks_written", "compression_ratio", "compactions", "commits")}
	case "inspect":
		bytes, files, e := directorySize(r.DataDir)
		if e != nil {
			return nil, translate(e)
		}
		hint, _ := spool.ReadKeyHint(r.DataDir)
		if e = open(); e != nil {
			return nil, e
		}
		schema, e := db.Schema()
		if e != nil {
			return nil, translate(e)
		}
		tables := make([]map[string]any, 0, len(schema.Tables))
		for _, t := range schema.Tables {
			pk := 0
			for _, c := range t.Columns {
				if c.PrimaryKey > 0 {
					pk = c.PrimaryKey
					break
				}
			}
			tables = append(tables, map[string]any{"id": t.ID, "name": t.Name, "columns_count": len(t.Columns), "primary_key": pk})
		}
		report = map[string]any{"data_dir": r.DataDir, "database_id": hexID(db.DatabaseID()), "node_id": hexID(db.NodeID()), "storage_files": files, "storage_disk_bytes": bytes, "wrapping_key_id": hint.WrappingKeyID, "kdf": kdfName(hint.KDF), "keyring_seq": hint.KeyringSeq, "table_count": len(schema.Tables), "index_count": len(schema.Indexes), "tables": tables}
	case "keys":
		if e := open(); e != nil {
			return nil, e
		}
		hint, _ := spool.ReadKeyHint(r.DataDir)
		inventory := db.KeyInventory()
		keys := make([]map[string]any, 0, len(inventory.DataKeys))
		for _, k := range inventory.DataKeys {
			status := "retired"
			if k.Status == spool.KeyStatusActive {
				status = "active"
			}
			created := k.CreatedAt.Unix()
			if created < 0 {
				created = 0
			}
			keys = append(keys, map[string]any{"id": k.ID, "id_hex": fmt.Sprintf("%08x", k.ID), "created_at_epoch_secs": created, "status": status})
		}
		report = map[string]any{"data_dir": r.DataDir, "encrypted": inventory.Encrypted, "wrapping_key_id": inventory.WrappingKeyID, "kdf": kdfName(hint.KDF), "active_data_key_id": inventory.ActiveDataKeyID, "keyring_seq": inventory.KeyringSeq, "manifest_generation": inventory.ManifestGeneration, "data_keys_count": len(keys), "data_keys": keys}
	case "rekey":
		if e := open(); e != nil {
			return nil, e
		}
		if r.NewPassphrase == "" && r.NewKey == [32]byte{} {
			return nil, userError("22023", "rekey needs --new-key-hex or --new-passphrase")
		}
		newCred := murmur.KeyCredential(r.NewKey[:])
		if r.NewPassphrase != "" {
			newCred = murmur.PassphraseCredential(r.NewPassphrase)
		}
		if e := db.RotateCredential(newCred); e != nil {
			return nil, translate(e)
		}
		if e := db.Flush(); e != nil {
			return nil, translate(e)
		}
		kdf := "unknown"
		if hint, e := spool.ReadKeyHint(r.DataDir); e == nil {
			kdf = kdfName(hint.KDF)
		}
		report = map[string]any{"data_dir": r.DataDir, "success": true, "kdf": kdf}
	case "verify":
		mode := "database"
		var verification error
		var databaseID, nodeID any
		count := 0
		if r.Backup {
			mode = "backup"
			verification = murmur.VerifyBackupWithCredential(r.DataDir, credFor(r.DataDir))
		} else {
			verification = open()
			if verification == nil {
				databaseID = hexID(db.DatabaseID())
				nodeID = hexID(db.NodeID())
				schema, e := db.Schema()
				if e != nil {
					verification = e
				} else {
					count = len(schema.Tables)
				}
			}
		}
		failures := []string{}
		if verification != nil {
			failures = append(failures, verification.Error())
		}
		report = map[string]any{"target_path": r.DataDir, "valid": verification == nil, "mode": mode, "database_id": databaseID, "node_id": nodeID, "tables_checked": count, "errors": failures}
		if verification != nil {
			err = translate(verification)
		}
	case "backup":
		if e := open(); e != nil {
			return nil, e
		}
		if e := db.Backup(r.Destination); e != nil {
			return nil, translate(e)
		}
		report = map[string]any{"source_dir": r.DataDir, "destination_dir": r.Destination, "database_id": hexID(db.DatabaseID()), "node_id": hexID(db.NodeID()), "success": true}
	case "restore":
		options := murmur.RestoreOptions{NewDatabaseID: r.FreshDatabaseID, NewNodeID: r.FreshNodeID}
		if e := murmur.RestoreWithCredentialAndOptions(r.DataDir, r.Destination, credFor(r.DataDir), options); e != nil {
			return nil, translate(e)
		}
		var databaseID, nodeID any
		if r.FreshDatabaseID != nil {
			databaseID = hexID(*r.FreshDatabaseID)
		}
		if r.FreshNodeID != nil {
			nodeID = hexID(*r.FreshNodeID)
		}
		report = map[string]any{"backup_dir": r.DataDir, "destination_dir": r.Destination, "success": true, "new_database_id": databaseID, "new_node_id": nodeID}
	default:
		return nil, failed(r.Operation)
	}
	return adminReport{values: report, human: formatReport(r.Operation, report)}, err
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

func directorySize(path string) (uint64, int, error) {
	var size uint64
	count := 0
	e := filepath.WalkDir(path, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.Type().IsRegular() {
			i, e := d.Info()
			if e != nil {
				return e
			}
			size += uint64(i.Size())
			count++
		}
		return nil
	})
	return size, count, e
}
func selectFields(values map[string]any, names ...string) map[string]any {
	out := map[string]any{}
	for _, name := range names {
		out[name] = values[name]
	}
	return out
}
func jsonFieldsValue(value any) map[string]any {
	raw, e := json.Marshal(value)
	if e != nil {
		return map[string]any{}
	}
	var out map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&out) != nil {
		return map[string]any{}
	}
	if _, ok := out["MemoryUsedBytes"]; ok {
		return remapKeys(out)
	}
	if _, ok := out["Keys"]; ok {
		return remapKeys(out)
	}
	return out
}
func remapKeys(m map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[snake(k)] = v
	}
	return out
}
func formatReport(op string, r map[string]any) string {
	var out strings.Builder
	line := func(label, key string) { fmt.Fprintf(&out, "%-21s%v\n", label+":", r[key]) }
	switch op {
	case "status":
		out.WriteString("=== Murmur Database Status ===\n")
		line("Data Directory", "data_dir")
		line("Database ID", "database_id")
		line("Node ID", "node_id")
		line("Committed Seq", "committed_seq")
		line("Durable Seq", "durable_seq")
		line("Pending Bytes", "pending_bytes")
		if r["storage_error"] != nil {
			if p, ok := r["storage_error"].(*string); ok && p != nil {
				fmt.Fprintf(&out, "Storage Error:       %s\n", *p)
			}
		}
		if m, ok := r["sqlite"].(map[string]any); ok {
			fmt.Fprintf(&out, "\n--- SQLite In-Memory Engine ---\nMemory Used:         %v bytes (highwater: %v bytes)\nPage Count / Size:   %v pages @ %v bytes\nDatabase Size:       %v bytes\nTables / Indexes:    %v tables, %v indexes\nTotal Rows:          %v\nSession Changeset:   %v bytes\n", m["memory_used_bytes"], m["memory_highwater_bytes"], m["page_count"], m["page_size"], m["database_size_bytes"], m["table_count"], m["index_count"], m["total_rows"], m["session_changeset_bytes"])
		}
		if m, ok := r["spool"].(map[string]any); ok {
			fmt.Fprintf(&out, "\n--- Spool Persistence Engine ---\nKeys Tracked:        %v\nSegments / Disk:     %v segments (%v bytes)\nLive Records / Bytes:%v records (%v bytes)\nDead Records:        %v\nBlocks Written:      %v\nCompression Ratio:   %vx\nCompactions / Commits:%v compactions, %v commits\n", m["keys"], m["segments"], m["disk_bytes"], m["live_records"], m["live_bytes"], m["dead_records"], m["blocks_written"], m["compression_ratio"], m["compactions"], m["commits"])
		}
	case "inspect":
		out.WriteString("=== Murmur Storage Inspection ===\n")
		for _, pair := range [][2]string{{"Data Directory", "data_dir"}, {"Database ID", "database_id"}, {"Node ID", "node_id"}, {"Files On Disk", "storage_files"}, {"Disk Bytes", "storage_disk_bytes"}, {"Wrapping Key ID", "wrapping_key_id"}, {"Envelope KDF", "kdf"}, {"Keyring Sequence", "keyring_seq"}, {"Tables Registered", "table_count"}, {"Indexes Registered", "index_count"}} {
			line(pair[0], pair[1])
		}
		if tables, ok := r["tables"].([]map[string]any); ok && len(tables) > 0 {
			out.WriteString("\n--- Tables ---\n")
			for _, t := range tables {
				fmt.Fprintf(&out, "  • %-20v (columns: %v, id: %v)\n", t["name"], t["columns_count"], t["id"])
			}
		}
	case "keys":
		out.WriteString("=== Murmur Key Registry & Inventory ===\n")
		fmt.Fprintf(&out, "Data Directory:         %v\nEncrypted:              %v\nWrapping Key ID:        %v\nEnvelope KDF:           %v\nActive Data Key ID:     %08x (#%v)\nKeyring Persist Seq:    %v\nManifest Generation:    %v\nTotal Data Keys:        %v\n", r["data_dir"], r["encrypted"], r["wrapping_key_id"], r["kdf"], r["active_data_key_id"], r["active_data_key_id"], r["keyring_seq"], r["manifest_generation"], r["data_keys_count"])
		if keys, ok := r["data_keys"].([]map[string]any); ok && len(keys) > 0 {
			out.WriteString("\n  Key ID    Status    Created At (UNIX secs)\n  --------  --------  ----------------------\n")
			for _, k := range keys {
				fmt.Fprintf(&out, "  %8v  %-8v  %v\n", k["id_hex"], k["status"], k["created_at_epoch_secs"])
			}
		}

	case "rekey":
		fmt.Fprintf(&out, "PASS: Store re-protected successfully.\nData Directory:   %v\nEnvelope KDF:     %v\n", r["data_dir"], r["kdf"])
	case "verify":
		if r["valid"] == true {
			if r["mode"] == "backup" {
				fmt.Fprintf(&out, "PASS: Backup directory verified successfully at %v\n", r["target_path"])
			} else {
				fmt.Fprintf(&out, "PASS: Database storage integrity verified at %v\n", r["target_path"])
			}
			if r["database_id"] != nil {
				line("Database ID", "database_id")
				line("Node ID", "node_id")
				line("Tables Checked", "tables_checked")
			}
		} else {
			if r["mode"] == "backup" {
				fmt.Fprintf(&out, "FAIL: Backup verification failed at %v:\n", r["target_path"])
			} else {
				fmt.Fprintf(&out, "FAIL: Storage verification failed at %v:\n", r["target_path"])
			}
			for _, e := range r["errors"].([]string) {
				fmt.Fprintf(&out, "  - %s\n", e)
			}
		}
	case "backup":
		fmt.Fprintf(&out, "PASS: Consistent hot backup created successfully.\nSource:       %v\nDestination:  %v\nDatabase ID:  %v\nNode ID:      %v\n", r["source_dir"], r["destination_dir"], r["database_id"], r["node_id"])
	case "restore":
		fmt.Fprintf(&out, "PASS: Database restored successfully.\nBackup Source:    %v\nDestination:      %v\n", r["backup_dir"], r["destination_dir"])
		if r["new_database_id"] != nil {
			line("New Database ID", "new_database_id")
		}
		if r["new_node_id"] != nil {
			line("New Node ID", "new_node_id")
		}
	}
	return strings.TrimSuffix(out.String(), "\n")
}
