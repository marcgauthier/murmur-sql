"""Murmur client: a thin layer over pg8000's DB-API connection.

Application SQL uses the plain DB-API (``conn.cursor()``, ``%s`` placeholders).
MURMUR administration commands are exposed as helper methods. Nothing here
re-implements server logic; every helper issues a MURMUR SQL command.
"""

from __future__ import annotations

import base64
import re
import ssl
from dataclasses import dataclass, field
from typing import BinaryIO, Iterator, List, Optional, Sequence

from .commands import AdminCommands

import pg8000.dbapi

pg8000.dbapi.paramstyle = "format"

TLS_DISABLE = "disable"
TLS_REQUIRE = "require"
TLS_VERIFY_CA = "verify-ca"
TLS_VERIFY_FULL = "verify-full"

_UPLOAD_CHUNK = 256 << 10
_DOWNLOAD_CHUNK = 1 << 20
_FILE_CHUNK = 64 << 10
_BACKUP_NAME = re.compile(r"^[A-Za-z0-9._-]{1,255}$")


@dataclass
class TLSConfig:
    """TLS settings mirroring Postgres ``sslmode``.

    ``mode``: ``""``/``disable`` (plaintext), ``require`` (encrypted, server
    certificate not verified), ``verify-ca`` or ``verify-full`` (both need
    ``ca_file``; ``verify-full`` also checks the host name).
    """

    mode: str = ""
    ca_file: str = ""
    cert_file: str = ""
    key_file: str = ""


@dataclass
class Config:
    user: str
    password: str
    key: str  # encryption key or passphrase for MURMUR UNLOCK
    host: str = "127.0.0.1"
    port: int = 5432
    database: str = "murmur"
    tls: TLSConfig = field(default_factory=TLSConfig)
    connect_timeout: float = 10.0

    def validate(self) -> None:
        if not self.user:
            raise ValueError("murmur: user is required")
        if not self.password:
            raise ValueError("murmur: password is required")
        if not self.key:
            raise ValueError("murmur: key is required")
        if not 1 <= self.port <= 65535:
            raise ValueError(f"murmur: invalid port {self.port}")
        mode = self.tls.mode
        if mode not in ("", TLS_DISABLE, TLS_REQUIRE, TLS_VERIFY_CA, TLS_VERIFY_FULL):
            raise ValueError(f"murmur: invalid TLS mode {mode!r}")
        if mode in (TLS_VERIFY_CA, TLS_VERIFY_FULL) and not self.tls.ca_file:
            raise ValueError(f"murmur: TLS mode {mode!r} needs ca_file")
        if bool(self.tls.cert_file) != bool(self.tls.key_file):
            raise ValueError("murmur: cert_file and key_file must be set together")

    def _ssl_context(self) -> Optional[ssl.SSLContext]:
        mode = self.tls.mode
        if mode in ("", TLS_DISABLE):
            return None
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if mode == TLS_REQUIRE:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        else:
            ctx.load_verify_locations(self.tls.ca_file)
            ctx.verify_mode = ssl.CERT_REQUIRED
            ctx.check_hostname = mode == TLS_VERIFY_FULL
        if self.tls.cert_file:
            ctx.load_cert_chain(self.tls.cert_file, self.tls.key_file)
        return ctx


def _quote(value: str) -> str:
    """Single-quoted MURMUR word with embedded quotes doubled."""
    return "'" + value.replace("'", "''") + "'"


def _check_backup_name(name: str) -> None:
    if not _BACKUP_NAME.match(name) or name.startswith("."):
        raise ValueError(f"murmur: invalid backup name {name!r}")


def _write_all(dest: BinaryIO, data: bytes) -> None:
    """Handle partial writes without silently discarding bytes."""
    view = memoryview(data)
    while view:
        written = dest.write(view)
        if written is None or written == 0:
            raise BlockingIOError("murmur: download destination made no progress")
        if not isinstance(written, int) or written < 0 or written > len(view):
            raise OSError("murmur: invalid download write count")
        view = view[written:]


def code_of(err: BaseException) -> str:
    """SQLSTATE of a driver error, or ``""``.

    Useful codes: 28000 (locked, wrong key, TLS required), 42P01 (unknown
    table), 23505 (unique violation), 42601 (syntax), 55000 (remote-only file).
    """
    args = getattr(err, "args", ())
    if args and isinstance(args[0], dict):
        return str(args[0].get("C", ""))
    return ""


def is_locked(err: BaseException) -> bool:
    """True when ``err`` is the locked-connection error (no MURMUR UNLOCK yet)."""
    args = getattr(err, "args", ())
    return (
        code_of(err) == "28000"
        and bool(args)
        and isinstance(args[0], dict)
        and "locked" in str(args[0].get("M", ""))
    )


@dataclass
class BackupFile:
    name: str
    size: int
    modified: str = ""


@dataclass
class TableFlag:
    table: str
    replicated: bool


@dataclass
class Metric:
    subsystem: str
    metric: str
    value: str


@dataclass
class FileInfo:
    name: str
    size: int
    modified_millis: int
    sha256: str
    local: bool


class Connection(AdminCommands):
    """One unlocked Murmur connection (not thread-safe; use one per thread).

    Created by :func:`connect`. ``cursor()`` returns a pg8000 DB-API cursor;
    the connection is in autocommit mode. The server autocommits every
    statement; BEGIN/COMMIT/ROLLBACK are accepted but are no-ops, so there is
    no multi-statement rollback.
    """

    def __init__(self, raw: pg8000.dbapi.Connection):
        self._raw = raw

    # --- plain DB-API pass-through -------------------------------------
    @property
    def raw(self) -> pg8000.dbapi.Connection:
        return self._raw

    def cursor(self) -> pg8000.dbapi.Cursor:
        return self._raw.cursor()

    def execute(self, sql: str, params: Sequence = ()) -> List[tuple]:
        """Run one statement and return all rows (``[]`` when none)."""
        cur = self._raw.cursor()
        try:
            cur.execute(sql, tuple(params))
            return list(cur.fetchall()) if cur.description else []
        finally:
            cur.close()

    def close(self) -> None:
        self._raw.close()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # --- MURMUR administration -----------------------------------------
    def flush(self) -> None:
        """Wait for a durable checkpoint of everything written so far."""
        self.execute("MURMUR FLUSH")

    def set_table_replicated(self, table: str, replicated: bool) -> None:
        """Enable or disable replication for ``table`` (default: local-only).

        Enabling rebuilds the table with a read-only ``id_murmur_primary_key``
        UUIDv5 key; disabling restores the application primary key.
        """
        if not table.strip():
            raise ValueError("murmur: table name must not be empty")
        self.execute(f"MURMUR SET TABLE {_quote(table)} REPLICATED {'true' if replicated else 'false'}")

    def table_replication(self) -> List[TableFlag]:
        rows = self.execute("MURMUR SHOW TABLES REPLICATION")
        return [TableFlag(str(t), str(r) == "true") for t, r in rows]

    def metrics(self, scope: str = "") -> List[Metric]:
        """Live SQLite and Spool metrics; ``scope`` may be ``"sqlite"`` or ``"spool"``."""
        cmd = "MURMUR SHOW METRICS"
        if scope.strip():
            cmd += " " + scope.strip().upper()
        return [Metric(str(a), str(b), str(c)) for a, b, c in self.execute(cmd)]

    def rotate_key(self, previous: str, new: str) -> None:
        """Rewrap the key registry; reconnect with the new key afterwards."""
        if not previous or not new:
            raise ValueError("murmur: rotate needs non-empty previous and new keys")
        if previous == new:
            raise ValueError("murmur: new key must differ from the previous key")
        self.execute(f"MURMUR ROTATE KEY {_quote(previous)} {_quote(new)}")

    # --- backups ---------------------------------------------------------
    def backup_save(self) -> BackupFile:
        (name, size), = self.execute("MURMUR BACKUP SAVE")
        return BackupFile(str(name), int(size))

    def backup_list(self) -> List[BackupFile]:
        return [BackupFile(str(n), int(s), str(m)) for n, s, m in self.execute("MURMUR BACKUP LIST")]

    def backup_upload(self, name: str, source: BinaryIO) -> None:
        """Stream ``source`` into the server's backup folder; published on FINISH only."""
        _check_backup_name(name)
        quoted = _quote(name)
        self.execute(f"MURMUR BACKUP UPLOAD BEGIN {quoted}")
        try:
            while True:
                chunk = source.read(_UPLOAD_CHUNK)
                if not chunk:
                    break
                encoded = base64.b64encode(chunk).decode("ascii")
                self.execute(f"MURMUR BACKUP UPLOAD APPEND {quoted} '{encoded}'")
            self.execute(f"MURMUR BACKUP UPLOAD FINISH {quoted}")
        except BaseException:
            try:
                self.execute(f"MURMUR BACKUP UPLOAD ABORT {quoted}")
            except Exception:
                pass
            raise

    def backup_download(self, name: str, dest: BinaryIO) -> None:
        _check_backup_name(name)
        offset = 0
        while True:
            (off, total, chunk), = self.execute(
                f"MURMUR BACKUP DOWNLOAD {_quote(name)} OFFSET {offset} LIMIT {_DOWNLOAD_CHUNK}"
            )
            chunk = bytes(chunk)
            if not chunk:
                return
            _write_all(dest, chunk)
            nxt = int(off) + len(chunk)
            if nxt >= int(total) or nxt <= offset:
                return
            offset = nxt

    def restore(self, name: str, key: str) -> None:
        """Live-restore a backup; ``key`` re-authenticates this destructive step."""
        _check_backup_name(name)
        if not key:
            raise ValueError("murmur: restore needs a non-empty key")
        self.execute(f"MURMUR RESTORE FROM {_quote(name)} KEY {_quote(key)}")

    # --- encrypted files ---------------------------------------------------
    def list_files(self) -> Iterator[FileInfo]:
        """Iterate every logical file sorted by name (snapshot cursor)."""
        return self._file_cursor("MURMUR FILE LIST", ())

    def search_files(self, pattern: str) -> Iterator[FileInfo]:
        """Case-sensitive LIKE search (``%``, ``_``; backslash escapes)."""
        return self._file_cursor("MURMUR FILE SEARCH %s", (pattern,))

    def _file_cursor(self, sql: str, params: Sequence) -> Iterator[FileInfo]:
        ((token,),) = self.execute(sql, params)
        try:
            while True:
                rows = self.execute("MURMUR FILE NEXT %s", (token,))
                if not rows:
                    return
                for name, size, mtime, sha, local in rows:
                    yield FileInfo(str(name), int(size), int(mtime), str(sha), bool(local))
        finally:
            self.execute("MURMUR FILE CLOSE %s", (token,))

    def upload_file(self, name: str, source: BinaryIO) -> FileInfo:
        """Upload bounded BYTEA chunks; a replacement is visible only after FINISH."""
        ((token,),) = self.execute("MURMUR FILE UPLOAD BEGIN %s", (name,))
        try:
            while True:
                chunk = source.read(_FILE_CHUNK)
                if not chunk:
                    break
                self.execute("MURMUR FILE UPLOAD APPEND %s %s", (token, bytes(chunk)))
            ((n, size, mtime, sha, local),) = self.execute("MURMUR FILE UPLOAD FINISH %s", (token,))
        except BaseException:
            try:
                self.execute("MURMUR FILE UPLOAD ABORT %s", (token,))
            except Exception:
                pass
            raise
        return FileInfo(str(n), int(size), int(mtime), str(sha), bool(local))

    def download_file(self, name: str, dest: BinaryIO) -> None:
        """Stream the pinned file version into ``dest``.

        Remote-only content raises with SQLSTATE 55000; retry after the
        background peer fetch completes.
        """
        ((token, _sha, size),) = self.execute("MURMUR FILE DOWNLOAD %s", (name,))
        size = int(size)
        try:
            position = 0
            while True:
                ((offset, total, chunk, eof),) = self.execute("MURMUR FILE DOWNLOAD NEXT %s", (token,))
                chunk = bytes(chunk) if chunk is not None else b""
                if (
                    int(offset) != position
                    or int(total) != size
                    or len(chunk) > size - position
                    or (not eof and not chunk)
                ):
                    raise RuntimeError("murmur: inconsistent file download")
                _write_all(dest, chunk)
                position += len(chunk)
                if eof:
                    if position != size:
                        raise RuntimeError("murmur: unexpected end of file download")
                    return
        finally:
            try:
                self.execute("MURMUR FILE CLOSE %s", (token,))
            except Exception:
                pass


def connect(cfg: Config) -> Connection:
    """Connect, verify the server, and run ``MURMUR UNLOCK`` with ``cfg.key``.

    A wrong key fails here with SQLSTATE 28000.
    """
    cfg.validate()
    raw = pg8000.dbapi.connect(
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        ssl_context=cfg._ssl_context(),
        timeout=cfg.connect_timeout,
    )
    raw.autocommit = True
    conn = Connection(raw)
    try:
        conn.execute(f"MURMUR UNLOCK KEY {_quote(cfg.key)}")
    except BaseException:
        conn.close()
        raise
    return conn
