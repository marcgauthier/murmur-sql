# bridge-ftp

Fresh scenario (no Rust source): sealed file-bridge bundles cross an
FTP dropzone instead of a shared staging directory.

## What it proves

A two-node Low mesh and a two-node High mesh (separate DBIDs, CAs,
storage keys, and file object keys: `0x11..` on Low, `0x22..` on High)
move two files across real FTP legs with separate staging areas per
domain:

- Leg 1 (plain FTP): Low uploads 384 KiB `shared/field-map.bin`
  (stream `field-files`), the Low mesh converges, Low-0 exports sealed
  bundles, and **both** Low exporters publish every bundle concurrently
  (`Bridge.PublishFTP`) to a real in-test FTP server (login, `TYPE I`,
  `CWD`/`MKD`, `PASV` data connections, `STOR`). The server holds
  exactly the published set byte-identical with no document plaintext;
  retrieval into the High staging area plus the `Process` pump imports
  the file with re-encryption under the High object key, and the High
  mesh converges with byte-identical downloads.
- Leg 2 (explicit FTPS): 48 KiB `shared/field-notes.bin` crosses a
  second server over `ftps://` with a private CA pinned via
  `FtpOptions.RootsPEM`; the server must observe TLS on every control
  and data connection. Same end-to-end import and mesh assertions.
- Leg 3 (auth failure): publishing with a wrong password returns an
  error and stores nothing.
- Leg 4 (HTTP): one sealed bundle crosses over HTTP `PUT`
  (`Bridge.PublishHTTP`) byte-identical, and replaying those bytes
  through the High importer is a harmless no-op (double-import
  idempotency: both files still download byte-identical).

The FTP server is test scaffolding (stdlib only, no new
dependencies): concurrent control connections, fixed credentials,
directory creation, `PASV`/`STOR`/`RETR`, and `AUTH TLS` + `PROT P`
with a minted CA/server certificate. Retrieval is scaffolding too:
the product FTP surface is upload-only (`PublishFTP`/`FtpUpload`),
so `RETR` exists to close the end-to-end loop and every retrieved
byte is asserted against the staged bundle and, after import, against
the original upload.

Pass criteria: both High nodes download byte-identical content with
the upload digests; High at-rest `*.spfo` bytes differ from Low's;
`SearchFiles("shared/")` on High-1 returns exactly the two files with
the upload digests; the FTPS server records TLS control and data
connections; the wrong-password publish fails with no server-side
artifact.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`). Convergence waits are 30 s
with 50 ms polls; peer fetch runs on a 100 ms `fetch_interval`.

## Deviations

- No HTTP and no shared staging: uploads, downloads, metadata, and
  search use the direct `Database` file API; bundles move over FTP.
- Two exporters share one Low staging directory (both must publish).
