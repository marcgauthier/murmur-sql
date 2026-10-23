# files-wire

End-to-end coverage of the `MURMUR FILE` commands over the PostgreSQL
wire against two real peered servers sharing one file key.

## What it pins

- **Upload**: `FILE UPLOAD BEGIN/APPEND/FINISH` stores a file in chunks;
  the finish receipt carries name, size, sha, and locality.
- **Catalog**: `FILE LIST` + `FILE NEXT` pages entries (`NEXT` past the
  end is empty); `FILE SEARCH` matches by pattern and misses cleanly;
  `FILE CLOSE` releases cursors.
- **Local download**: `FILE DOWNLOAD` + `FILE DOWNLOAD NEXT` reassemble
  the exact uploaded bytes.
- **Remote download**: the peer serves the same bytes after mesh sync,
  proving wire-driven cross-node file fetch.
- **Negatives**: missing files, unknown handles/cursors,
  cross-connection handles, wrong handle kinds (`42601`), and
  over-1-MiB chunks fail with stable codes; `UPLOAD ABORT` drops the
  staged upload.
- **Restart**: the catalog and bytes survive a full server stop.

## Run

```sh
go test -count=1 -timeout 600s ./murmur/tests-live/files-wire/
```
