# search-mesh

Managed search and JSON over a two-node replication mesh. Node 0 declares
column FTS and a 2-D L2 vector index on a replicated table; the test
checks the declarations arrive on node 1, inserts documents with text,
little-endian float embeddings, and JSON metadata, and verifies on the
replica:

- `MATCH` full-text queries over replicated rows,
- KNN vector search with exact L2 distances,
- `json_extract` / `->>` reads over replicated JSON,
- `MURMUR REFRESH INDEXES` tracking a replicated update,
- FTS disable dropping the index and re-enable rebuilding it,
- MATCH and KNN still working after a replica restart (rebuild from rows).

All database operations use explicit SQL through `clients/rust` against
live `murmur-server` child processes. No embedded database handles are
linked or opened.

Run from the repository root:

```sh
tests-live/run.sh search-mesh
```

Use `MURMUR_SERVER_BIN` to reuse a prebuilt replication-enabled binary.
Success prints `RESULT: PASS`; failure retains runtime artifacts for
inspection.
