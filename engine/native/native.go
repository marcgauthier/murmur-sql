package native

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/marcgauthier/murmur-sql/engine"
	"github.com/marcgauthier/murmur-sql/murmur"
	"github.com/marcgauthier/murmur-sql/murmur/spool"
)

type Factory struct{}
type database struct {
	cfgMu   sync.Mutex
	mu      sync.RWMutex
	db      *murmur.Database
	request engine.OpenRequest
	keyHash [32]byte
	closed  bool
	files   fileHandles
}

// credentialFor maps request key material to a store credential. A
// non-empty passphrase selects the envelope KDF for existing stores
// (Argon2id, or the legacy SHA-256 mapping for KDF-0 envelopes) and
// passphrase protection for new ones; otherwise the raw key is used
// unchanged. Unreadable envelopes fall through to the passphrase form
// so opens fail downstream with the store's own error.
func credentialFor(dir string, key [32]byte, passphrase string) murmur.Credential {
	if passphrase == "" {
		return murmur.KeyCredential(key[:])
	}
	if hint, err := spool.ReadKeyHint(dir); err == nil && hint.Encrypted {
		return murmur.CredentialForSecret(passphrase, hint.KDF)
	}
	return murmur.PassphraseCredential(passphrase)
}

// credentialHash canonicalizes request key material for session
// verification: the passphrase wins when set, else the raw key. The
// comparison is an equality check against the opening credential, not
// key derivation.
func credentialHash(key [32]byte, passphrase string) [32]byte {
	if passphrase != "" {
		return sha256.Sum256([]byte(passphrase))
	}
	return sha256.Sum256(key[:])
}

// splitCredential decodes a rotation/restore credential argument:
// []byte is a raw key, string is a passphrase.
func splitCredential(v engine.Value, key *[32]byte, passphrase *string) error {
	switch t := v.(type) {
	case []byte:
		if len(t) != 32 {
			return userError("22023", "key must be 32 bytes")
		}
		copy(key[:], t)
	case string:
		if t == "" {
			return userError("28000", "key must be non-empty")
		}
		*passphrase = t
	default:
		return userError("22023", "key must be bytes or text")
	}
	return nil
}

// credentialEqual reports same-form equality, plus the cross-form
// no-op: rotating a passphrase to the raw legacy SHA-256 of that same
// passphrase would rewrap a KDF-0 envelope under identical material.
// A new passphrase always changes the protector, so the reverse
// direction needs no check.
func credentialEqual(oldKey, newKey [32]byte, oldPass, newPass string) bool {
	if oldPass == "" && newPass == "" {
		return oldKey == newKey
	}
	if oldPass != "" && newPass != "" {
		return oldPass == newPass
	}
	if newPass == "" {
		return newKey == sha256.Sum256([]byte(oldPass))
	}
	return false
}

var _ engine.Factory = Factory{}
var _ engine.Database = (*database)(nil)

func (Factory) Open(ctx context.Context, r engine.OpenRequest) (engine.Database, engine.OpenOutcome, error) {
	defer clear(r.Key[:])
	defer func() { r.Passphrase = "" }()
	if e := ctx.Err(); e != nil {
		return nil, 0, e
	}
	emptyDirectory := false
	mode := murmur.OpenExisting
	outcome := engine.OpenedExisting
	if _, e := os.Stat(r.Config.Storage.DataDir); errors.Is(e, os.ErrNotExist) {
		mode = murmur.OpenCreateNew
		outcome = engine.OpenedNew
	} else if e != nil {
		return nil, 0, translate(e)
	} else {
		entries, e := os.ReadDir(r.Config.Storage.DataDir)
		if e != nil {
			return nil, 0, translate(e)
		}
		if len(entries) == 0 {
			emptyDirectory = true
			mode = murmur.OpenCreateNew
			outcome = engine.OpenedNew
		}
	}
	options, e := BuildOptions(r.Config, mode)
	if e != nil {
		return nil, 0, translate(e)
	}
	if emptyDirectory {
		if e = os.Remove(r.Config.Storage.DataDir); e != nil {
			return nil, 0, translate(e)
		}
	}
	db, e := murmur.OpenWithCredentialAndFlags(r.Config.Storage.DataDir, credentialFor(r.Config.Storage.DataDir, r.Key, r.Passphrase), options, r.Config.Replication.Tables)
	if e != nil {
		return nil, 0, translate(e)
	}
	d := &database{db: db, request: r, keyHash: credentialHash(r.Key, r.Passphrase)}
	clear(d.request.Key[:])
	d.request.Passphrase = ""
	d.request.Config = r.Config.Snapshot()
	if e = d.applyFlags(db); e != nil {
		db.Close()
		return nil, 0, e
	}
	d.files.entries = map[string]*fileHandle{}
	return d, outcome, nil
}
func (d *database) applyFlags(db *murmur.Database) error {
	if e := db.BindTableFlagsSink(d.flagsSink); e != nil {
		return translate(e)
	}
	d.cfgMu.Lock()
	flags := d.request.Config.Snapshot().Replication.Tables
	d.cfgMu.Unlock()
	owned, e := db.TableConfigOwned()
	if e != nil {
		return translate(e)
	}
	if len(flags) == 0 && !owned {
		schema, e := db.Schema()
		if e != nil {
			return translate(e)
		}
		if len(schema.TableReplication) > 0 {
			flags = map[string]bool{}
			for id, value := range schema.TableReplication {
				flags[id] = value
			}
			if e = d.flagsSink(flags); e != nil {
				return e
			}
		}
	}
	if e := db.SetTableReplicationMemory(flags); e != nil {
		return translate(e)
	}
	return translate(db.MarkTableConfigOwned())
}
func (d *database) verify(key [32]byte, passphrase string) error {
	if d.closed || d.db == nil {
		return translate(murmur.ErrClosed)
	}
	hash := credentialHash(key, passphrase)
	if subtle.ConstantTimeCompare(hash[:], d.keyHash[:]) != 1 {
		return translate(murmur.ErrWrongKey)
	}
	return nil
}
func (d *database) Unlock(ctx context.Context, key [32]byte, passphrase string) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.verify(key, passphrase)
}
func (d *database) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	d.files.clear()
	clear(d.keyHash[:])
	if d.db == nil {
		return nil
	}
	e := d.db.Close()
	d.db = nil
	return translate(e)
}
func (d *database) Disconnect(id uint64) { d.files.disconnect(id) }
func (d *database) Migrate(ctx context.Context, sql string) (engine.Result, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if e := d.ready(ctx); e != nil {
		return engine.Result{}, e
	}
	e := d.db.Migrate(murmur.NewMigration(sql))
	tokens, _ := murmur.Tokens(sql)
	tag := "DDL"
	if len(tokens) > 0 {
		tag = upper(tokens[0].Text)
	}
	if len(tokens) > 1 && (upper(tokens[1].Text) == "TABLE" || upper(tokens[1].Text) == "INDEX") {
		tag += " " + upper(tokens[1].Text)
	}
	return engine.Result{Tag: tag}, translate(e)
}
func (d *database) ready(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if d.closed || d.db == nil {
		return translate(murmur.ErrClosed)
	}
	return nil
}
func (d *database) rotate(ctx context.Context, args []engine.Value) (engine.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(ctx); e != nil {
		return engine.Result{}, e
	}
	var previous, next [32]byte
	var previousPass, nextPass string
	if e := splitCredential(args[0], &previous, &previousPass); e != nil {
		return engine.Result{}, e
	}
	if e := splitCredential(args[1], &next, &nextPass); e != nil {
		return engine.Result{}, e
	}
	defer clear(previous[:])
	defer clear(next[:])
	if e := d.verify(previous, previousPass); e != nil {
		return engine.Result{}, e
	}
	if credentialEqual(previous, next, previousPass, nextPass) {
		return engine.Result{}, userError("22023", "new key must differ from previous key")
	}
	cred := murmur.KeyCredential(next[:])
	if nextPass != "" {
		cred = murmur.PassphraseCredential(nextPass)
	}
	if e := d.db.RotateCredential(cred); e != nil {
		return engine.Result{}, translate(e)
	}
	d.keyHash = credentialHash(next, nextPass)
	return engine.Result{Tag: "ROTATE"}, translate(d.db.Flush())
}
func (d *database) restore(ctx context.Context, args []engine.Value) (engine.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(ctx); e != nil {
		return engine.Result{}, e
	}
	var key [32]byte
	var passphrase string
	if e := splitCredential(args[1], &key, &passphrase); e != nil {
		return engine.Result{}, e
	}
	defer clear(key[:])
	if e := d.verify(key, passphrase); e != nil {
		return engine.Result{}, e
	}
	// The backup predates any rotation since it was taken, so select
	// the credential against the backup envelope, not the live one.
	cred := credentialFor(args[0].(string), key, passphrase)
	live := filepath.Clean(d.request.Config.Storage.DataDir)
	parent := filepath.Dir(live)
	stage, e := os.MkdirTemp(parent, ".murmur-restoring-*")
	if e != nil {
		return engine.Result{}, translate(e)
	}
	os.Remove(stage)
	defer os.RemoveAll(stage)
	if e = murmur.RestoreWithCredential(args[0].(string), stage, cred); e != nil {
		return engine.Result{}, translate(e)
	}
	d.cfgMu.Lock()
	cfg := d.request.Config.Snapshot()
	d.cfgMu.Unlock()
	options, e := BuildOptions(cfg, murmur.OpenExisting)
	if e != nil {
		return engine.Result{}, translate(e)
	}
	// Validate the restored store without binding the live node's peer listener.
	trialOptions := options
	trialOptions.Replication = nil
	trial, e := murmur.OpenWithCredentialAndFlags(stage, cred, trialOptions, cfg.Replication.Tables)
	if e != nil {
		return engine.Result{}, translate(e)
	}
	if e = trial.Close(); e != nil {
		return engine.Result{}, translate(e)
	}
	d.files.clear()
	if e = d.db.Close(); e != nil {
		return engine.Result{}, translate(e)
	}
	d.db = nil
	replaced := filepath.Join(parent, "."+filepath.Base(live)+".replaced-"+time.Now().UTC().Format("20060102-150405.000000000"))
	reopen := func() error {
		db, err := murmur.OpenWithCredentialAndFlags(live, cred, options, cfg.Replication.Tables)
		if err != nil {
			return err
		}
		d.db = db
		return d.applyFlags(db)
	}
	if e = os.Rename(live, replaced); e != nil {
		return engine.Result{}, translate(errors.Join(e, reopen()))
	}
	if e = os.Rename(stage, live); e != nil {
		rollback := os.Rename(replaced, live)
		return engine.Result{}, translate(errors.Join(e, rollback, reopen()))
	}
	if e = reopen(); e != nil {
		if d.db != nil {
			d.db.Close()
			d.db = nil
		}
		failed := stage + ".failed"
		moveErr := os.Rename(live, failed)
		rollback := os.Rename(replaced, live)
		reopenErr := reopen()
		return engine.Result{}, translate(errors.Join(e, moveErr, rollback, reopenErr))
	}
	return engine.Result{Tag: "RESTORE"}, nil
}
func userError(code, msg string) error { return &engine.Error{Code: code, Message: msg} }
func failed(operation string) error {
	return fmt.Errorf("invalid native engine operation %s", operation)
}

func (d *database) flagsSink(flags map[string]bool) error {
	if d.request.TableFlagsSink != nil {
		if e := d.request.TableFlagsSink(flags); e != nil {
			return e
		}
	}
	d.cfgMu.Lock()
	defer d.cfgMu.Unlock()
	d.request.Config.Replication.Tables = map[string]bool{}
	for k, v := range flags {
		d.request.Config.Replication.Tables[k] = v
	}
	return nil
}
