> Go-port status: the production server now uses MURMUR-GO through the native adapter. This integration is source-ported and has not been built or tested. See [port status](../../docs/PARITY.md).

# Murmur Python client

Pure-Python client over the PostgreSQL wire protocol, built on
[pg8000](https://github.com/tlocke/pg8000). No native library, no libpq, no
embedded binding. Application SQL uses the standard DB-API; MURMUR commands are
helper methods.

```sh
pip install -e clients/python          # runtime: pg8000
pip install -e 'clients/python[test]'  # adds pytest + cryptography for tests
```

```python
import murmur

with murmur.connect(murmur.Config(
        host="127.0.0.1", port=5432, user="admin", password="s3cret",
        key="database-encryption-key")) as db:
    db.execute("CREATE TABLE greetings(id INTEGER PRIMARY KEY, lang TEXT, text TEXT)")
    db.execute("INSERT INTO greetings VALUES (%s, %s, %s)", (1, "en", "hello"))
    rows = db.execute("SELECT text FROM greetings WHERE lang = %s", ("en",))  # [["hello"]]
    db.flush()                       # durable checkpoint
    cur = db.cursor()                # raw pg8000 DB-API cursor when needed
```

`connect` runs `MURMUR UNLOCK` with `Config.key`; a wrong key raises with SQLSTATE
`28000` (`murmur.code_of(err)`, `murmur.is_locked(err)`). Placeholders are `%s`
(pg8000 `format` style, sent as `$N`). Rows are lists.

| Feature | API |
|---|---|
| Connect and unlock | `murmur.connect(Config)` |
| CRUD with parameters | `db.execute(sql, params)` / `db.cursor()` |
| Replication flags | `db.set_table_replicated(table, bool)`, `db.table_replication()` |
| Automatic replication keys | Managed UUIDv5 keys; application keys remain required |
| Durability flush | `db.flush()` |
| Verified TLS | `Config(tls=TLSConfig(mode="verify-full", ca_file=..., cert_file=..., key_file=...))` (`disable`, `require`, `verify-ca`, `verify-full`) |
| Remote backup and restore | `backup_save`, `backup_list`, `backup_download`, `backup_upload`, `restore` |
| Key rotation | `db.rotate_key(old, new)`, then reconnect with the new key |
| Engine metrics | `db.metrics(scope="")` |
| Encrypted file transfer and cursors | `upload_file`, `download_file`, `list_files`, `search_files` (generators) |
| Managed FTS and vector indexes | `set_column_fts`, `column_fts`, `set_column_vector`, `column_vectors`, `refresh_indexes` |
| JSON and JSONB | SQLite JSON functions through parameterized SQL |
| Replication peers | `peer_add`, `peer_remove`, `peer_retire`, `peer_sync`, `show_peers`, `show_replication` |
| Retention | `show_retention`, `retention_collect` |
| CRDT policy operations | `policy_counter_add`, `policy_set_add`, `policy_set_remove`, `policy_min`, `policy_max` |
| Bridge ownership release | `bridge_release(table, key, column=None)` |

## Restrictions

- A `Connection` is not thread-safe; use one connection per thread (each unlocks itself).
- The server autocommits each statement; `BEGIN`/`COMMIT`/`ROLLBACK` are accepted no-ops
  with no multi-statement rollback.
- File commands need a server built with the `replication` feature, enabled replication with
  node credentials, and `[replication.files]`. Remote-only files raise SQLSTATE `55000`.
- Replicated tables, `id_murmur_primary_key`, and other server SQL rules are as in
  [docs/USAGE.md](../../docs/USAGE.md).
- Consume `list_files()`/`search_files()` fully (or `close()` the generator) before the next
  call on the same connection: the cursor holds server state until exhausted.

## Tests and examples

Tests and examples start a scratch `murmur-server` (Go builds it on first use; set
`MURMUR_SERVER_BIN` to a prebuilt `--features replication` binary). From `clients/python`:

```sh
python -m pytest
python -m examples.basic     # also: tables, backup, metrics, files, tls, keys, json, search, replication, bridge
```

`examples/coverage.json` maps each feature above to its runnable example and is checked by
the test suite.


## Managed search and JSON

```python
db.set_column_fts("documents", "body", True)
db.set_column_vector("documents", "embedding", murmur.VectorOptions(2, "l2"))
db.refresh_indexes()
columns = db.column_vectors()
```

Source tables need one INTEGER application key. FTS columns must be TEXT;
vector columns BLOB with finite little-endian float32 values. Dimensions are
1–8192; distances are `l2` (default), `cosine`, or `l1`. Vector search
requires the server's default `vector` Cargo feature. Pass `None` to
`set_column_vector` to disable it. Disabled columns have absent configuration
and status; otherwise status includes readiness, rebuilding, queue counts and
an optional error. Discover and SQL-quote the returned index name, then use
bound MATCH/KNN parameters. Refresh waits for local collected writes, not peers
or durable storage. Run `python -m examples.search` for checked queries,
refresh and disabling. Index data rebuilds locally.

JSON needs no setting: use SQLite JSON functions with `%s` parameters.
`python -m examples.json` checks extraction, updates, arrays and JSONB.
Managed migrations reject JSON expression indexes, generated columns and CHECK
constraints; replication resolves the whole JSON column value.

## Replication commands

```python
status = db.show_replication()
db.peer_add("127.0.0.1:9001")
db.peer_sync()
peers = db.show_peers()
entries = db.show_retention()
collected = db.retention_collect()
db.policy_counter_add("stats", "count", "row-1", 5)
db.policy_set_add("stats", "tags", "row-1", "blue")
db.policy_set_remove("stats", "tags", "row-1", "blue")
db.policy_min("stats", "lowest", "row-1", -1.5)
db.policy_max("stats", "highest", "row-1", 42)
released = db.bridge_release("notes", "row-1", "title")
```

These commands require the `replication` Cargo feature and enabled replication
credentials; otherwise they raise SQLSTATE `55000`. Peer addresses are IP:port
(IPv6 in brackets); node IDs are 32 hex characters. Add queues a dial; sync
requests progress without waiting for convergence. Remove keeps retention
obligations and can be reversed by adding again; retire is permanent.
Nullable peer and retention fields remain `None`; boolean fields are Python
booleans.

Policies require single-TEXT-key rows and matching `replication.merge_policies`.
MIN/MAX accept signed int64, finite float or str, excluding bool.
Missing rows raise `02000`; policy mismatches raise `22023`.
Bridge release additionally requires a configured high-receiver and addresses
single-TEXT-key rows. The optional column selects a field; omit it for row
ownership. `False` means busy (retry); invalid/unowned targets raise `22023`.
The replication and bridge examples provision scratch credentials; no external
services are required. The bridge example checks ownership release without
exchanging transport bundles.

## Key rotation and streaming

`python -m examples.keys` checks rotation, old-key rejection and preserved
data through a new connection. Reconnect with the new key after rotation;
existing unlocked sessions remain usable.

File and backup downloads handle partial binary writes until the chunk is
complete. A destination that returns zero or None without accepting data raises
`BlockingIOError`; writer exceptions propagate. Use blocking binary streams.

The client covers the server SQL/MURMUR interface. Server configuration, node
credentials, bridge transport, recovery tooling and scheduling stay on the
server. Embedded callbacks, subscriptions and custom transport traits are not
exposed over the PostgreSQL protocol.
