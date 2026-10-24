# murmur — embedded encrypted database

`murmur` is the in-process database engine behind Murmur-SQL: encrypted
persistence, masterless peer-to-peer replication, full-text and vector
search, merge policies, migrations, and backup, importable directly from
Go with no server process.

```go
import "github.com/marcgauthier/murmur-sql/murmur"
```

Requirements: Go 1.24+ and a C compiler. The engine binds the vendored
SQLite amalgamation (with FTS5 and a built-in `vec0` vector module) and
vendored zstd via cgo; everything lives under `murmur/` including
`murmur/third_party/`, so no system libraries are needed.

Runnable versions of every snippet below live in
[example_test.go](example_test.go) and run with:

```sh
go test -run Example ./murmur/
```

## Open, migrate, transact

```go
options := murmur.DefaultOptions()
options.Mode = murmur.OpenCreateNew // or murmur.OpenExisting
db, err := murmur.Open(dir+"/store", key, options)
if err != nil { /* ... */ }
defer db.Close()

err = db.Migrate(murmur.NewMigration(
    `CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`))

_, err = db.Transaction(func(s *murmur.Sql) (any, error) {
    _, err := s.Execute("INSERT INTO users(id, name) VALUES(?, ?)", 1, "ada")
    return nil, err
})

v, err := db.Read(func(s *murmur.Sql) (any, error) {
    return s.Query("SELECT name FROM users WHERE id = ?", 1)
})
name := v.([][]sqlite.Value)[0][0].Str // "ada"
```

`Transaction` runs one read-write callback; `Read` runs a read-only one.
`Execute`/`Query`/`QueryRow` take `?` placeholders. Statements commit in
memory; `db.Flush()` waits for the engine's durability boundary.

## Keys and reopening

`Open` takes a 32-byte data key and opens (or creates) an encrypted store
at `path`. Reopening with the same key unlocks it; a wrong key fails with
`murmur.KindWrongKey` (SQLSTATE `28000` over the wire).

```go
key := make([]byte, 32) // load from a KMS, HSM, or encrypted key file
```

`OpenWithCredential` also accepts a passphrase, which new stores stretch
with Argon2id (parameters stored in the envelope); a 64-character hex
string is always a raw 32-byte key instead:

```go
db, err := murmur.OpenWithCredential(dir+"/store",
    murmur.PassphraseCredential("correct horse battery staple"), options)
```

Stores created before passphrase support keep the legacy mapping
(SHA-256 of the passphrase as the master key) and still open through
`murmur.CredentialForSecret` with the envelope KDF. `RotateCredential`
to a passphrase migrates the envelope to Argon2id; afterwards the
legacy mapping is rejected.

`RotateDataKey` retires the active data key without rewriting stored
blocks; `KeyReferences` reports per-key live references for progress
tracking, and `PruneDataKeys` drops retired keys nothing references
(a safe no-op while references remain).

## Replication

Replication is off unless `Options.Replication` is set. Each node needs a
stable identity, TLS credentials, and a signing seed:

```go
config := replication.NewConfig(bindAddr, replication.Credentials{
    Chain:      leafChainDER,   // CA-issued leaf chain
    PrivateKey: leafKeyDER,     // PKCS8 private key
    Roots:      [][]byte{caDER},
}, signingSeed /* [32]byte */)
options.Replication = config
options.NodeID = &nodeID         // [16]byte, stable per node
options.DatabaseID = &databaseID // [16]byte, shared per database
```

Then opt tables in and peer explicitly (see `Example_replication`):

```go
db.SetReplication("records", true)
db.Replication().AddPeer(peerAddr) // netip.AddrPort of a peer's QUIC listener
```

Tables replicate only after `SetReplication`; flags are node-local and can
be persisted with `OpenWithFlags`/`SetReplicationSink`. Concurrent cell
writes resolve with last-writer-wins; one node may be designated schema
master (`SchemaMaster` + `Master`) so schema edits have a single writer —
followers then adopt only the master's manifests. The
[client-failover](../murmur/tests-live/client-failover/) and
[search-chaos](../murmur/tests-live/search-chaos/) live scenarios exercise
this wiring end to end.

## Search: FTS and vectors

Enable per column; the index builds in the background and queries run
against a `temp` table joined back to the source on `rowid`:

```go
db.SetColumnFts("docs", "body", true)
db.RefreshIndexes() // optional nudge; the maintenance tick builds anyway

// SELECT d.id FROM docs d
// JOIN temp."<index>" f ON f.rowid = d.id
// WHERE "<index>" MATCH ? ORDER BY d.id
```

Vector columns hold little-endian float32 blobs and answer KNN queries:

```go
db.SetColumnVector("docs", "embedding",
    &murmur.VectorColumnConfig{Dimensions: 2, Distance: murmur.DistL2})

// SELECT d.id FROM temp."<index>" v JOIN docs d ON d.id = v.rowid
// WHERE v.embedding MATCH '[1,0]' AND v.k = 2 ORDER BY v.distance
```

`SchemaWithSearchStatus` reports index names, readiness, and errors; the
search flags propagate to peers through schema adoption.

## Merge policies

Plain columns resolve conflicts with last-writer-wins. Counter, set, min,
and max columns are declared through the replication config's
`MergePolicies` map and updated with policy operations instead of SQL
writes:

```go
s.CounterAdd("counters", []murmur.Value{murmur.IntValue(7)}, "n", 1)
s.SetAdd("tags", []murmur.Value{murmur.IntValue(7)}, "labels", murmur.TextValue("go"))
s.Min("readings", []murmur.Value{murmur.IntValue(7)}, "temp", murmur.IntValue(3))
```

## Backup and restore

```go
db.Backup(dir + "/backup") // consistent, still-encrypted copy

restore := murmur.DefaultRestoreOptions()
murmur.RestoreBackup(dir+"/backup", dir+"/restored", key, &restore)
// open dir+"/restored" with OpenExisting as usual
```

## Errors

Failures surface as `*murmur.Error` with a machine-readable `Kind`
(`KindWrongKey`, `KindBusy`, `KindNotFound`, `KindInvalid`,
`KindSchemaMismatch`, `KindCorrupt`, `KindLimit`, `KindStorage`,
`KindIo`, `KindSql`, `KindClosed`). The server maps each kind to a
SQLSTATE; see [Error codes](../docs/USAGE.md#error-codes).

## Further reading

- [example_test.go](example_test.go): every snippet above, runnable.
- [tests-live](tests-live/): multi-node fault-injection scenarios that pin
  replication, search, backup, and consistency behavior.
- [docs/USAGE.md](../docs/USAGE.md): server binaries, SQL wire interface,
  `MURMUR` commands, and operations.
