"""Typed helpers for the server's search and replication commands."""
from __future__ import annotations

import ipaddress
import math
import re
from dataclasses import dataclass
from typing import List, Optional, Union


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _target(table: str, column: str) -> str:
    if not table.strip() or not column.strip():
        raise ValueError("murmur: table and column must not be empty")
    return f"{_quote(table)} {_quote(column)}"


def _boolean(value) -> bool:
    return value is True or value in ("true", "t")


def _node(node: str) -> str:
    if not re.fullmatch(r"[0-9a-fA-F]{32}", node):
        raise ValueError("murmur: node id must be 32 hex characters")
    return _quote(node)


def _literal(value: Union[int, float, str]) -> str:
    if isinstance(value, bool):
        raise ValueError("murmur: policy values must be int, finite float or str")
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise ValueError("murmur: policy integer overflows int64")
        return str(value)
    if isinstance(value, float) and math.isfinite(value):
        return repr(value)
    if isinstance(value, str):
        return _quote(value)
    raise ValueError("murmur: policy values must be int, finite float or str")


PolicyKey = Union[str, List[Union[int, float, str]]]


def _policy_key(key: PolicyKey) -> tuple:
    """Render a policy key span plus the value separator.

    A plain string keeps the legacy single-key form; a list uses the
    KEY ... VALUE form. Returns (key_span, value_prefix)."""
    if isinstance(key, str):
        return _quote(key), ""
    if not isinstance(key, (list, tuple)) or len(key) == 0:
        raise ValueError("murmur: policy key must be a string or a non-empty list")
    return "KEY " + " ".join(_literal(k) for k in key), "VALUE "


def _policy_key_marked(key: PolicyKey) -> str:
    """Render a policy key span with the KEY marker always present,
    for operations without a legacy form."""
    if isinstance(key, str):
        return "KEY " + _quote(key)
    if not isinstance(key, (list, tuple)) or len(key) == 0:
        raise ValueError("murmur: policy key must be a string or a non-empty list")
    return "KEY " + " ".join(_literal(k) for k in key)


def _policy_path(path: str) -> str:
    if not isinstance(path, str) or not path:
        raise ValueError("murmur: json path must be a non-empty string")
    return _quote(path)


@dataclass
class VectorOptions:
    dimensions: int
    distance: str = "l2"


@dataclass
class IndexStatus:
    index_name: str
    ready: bool
    rebuilding: bool
    pending_keys: int
    queued_bytes: int
    error: Optional[str]


def _status(values) -> Optional[IndexStatus]:
    name, ready, rebuilding, pending, queued, error = values
    if name is None:
        return None
    return IndexStatus(name, _boolean(ready), _boolean(rebuilding), pending, queued, error)


@dataclass
class ColumnFTS:
    table: str
    column: str
    enabled: bool
    status: Optional[IndexStatus]


@dataclass
class ColumnVector:
    table: str
    column: str
    options: Optional[VectorOptions]
    status: Optional[IndexStatus]


@dataclass
class PeerInfo:
    node_id: str
    address: Optional[str]
    connected: bool
    last_seen_millis_ago: Optional[int]
    sent_watermark: int
    schema_agreed: bool
    push_mode: bool


@dataclass
class ReplicationStatus:
    running: bool
    address: Optional[str]
    peers: int
    members: int
    staged_transactions: int
    applied_transactions: int
    last_error: Optional[str]
    committed: int


@dataclass
class RetentionEntry:
    node_id: str
    last_seen_millis: int
    retired: bool
    origin: Optional[str]
    acknowledged: Optional[int]
    collected_transactions: int
    last_collection_millis: int


class AdminCommands:
    """Shared command methods; Connection supplies execute()."""

    def set_column_fts(self, table: str, column: str, enabled: bool) -> None:
        """Configure TEXT FTS on a table with one INTEGER application key."""
        self.execute(f"MURMUR SET COLUMN {_target(table, column)} FTS {'true' if enabled else 'false'}")

    def column_fts(self) -> List[ColumnFTS]:
        return [ColumnFTS(r[0], r[1], _boolean(r[2]), _status(r[3:]))
                for r in self.execute("MURMUR SHOW COLUMNS FTS")]

    def refresh_indexes(self) -> None:
        """Wait for local collected writes to reach indexes, not peers or disk."""
        self.execute("MURMUR REFRESH INDEXES")

    def set_column_vector(self, table: str, column: str, options: Optional[VectorOptions]) -> None:
        """Configure a BLOB vector index; None disables it."""
        target = _target(table, column)
        setting = "false"
        if options is not None:
            if type(options.dimensions) is not int or not 1 <= options.dimensions <= 8192:
                raise ValueError("murmur: vector dimensions must be 1..8192")
            if options.distance not in ("l2", "cosine", "l1"):
                raise ValueError("murmur: vector distance must be l2, cosine or l1")
            setting = f"true DIMENSIONS {options.dimensions} DISTANCE {options.distance}"
        self.execute(f"MURMUR SET COLUMN {target} VECTOR {setting}")

    def column_vectors(self) -> List[ColumnVector]:
        return [ColumnVector(r[0], r[1], VectorOptions(r[3], r[4]) if r[3] is not None else None,
                             _status(r[5:]))
                for r in self.execute("MURMUR SHOW COLUMNS VECTOR")]

    def peer_add(self, address: str) -> None:
        """Queue a dial to an IP:port; this does not wait for connection."""
        try:
            host, port = address.rsplit(":", 1)
            if host.startswith("[") and host.endswith("]"):
                host = host[1:-1]
                if ipaddress.ip_address(host).version != 6:
                    raise ValueError()
            elif ":" in host:
                raise ValueError()
            ipaddress.ip_address(host)
            if not port.isascii() or not port.isdecimal() or not 0 <= int(port) <= 65535:
                raise ValueError()
        except ValueError:
            raise ValueError("murmur: peer address must be IP:port") from None
        self.execute(f"MURMUR PEER ADD {_quote(address)}")

    def peer_remove(self, node: str) -> None:
        """Disconnect/exclude a peer; retention obligations remain."""
        self.execute(f"MURMUR PEER REMOVE {_node(node)}")

    def peer_retire(self, node: str) -> None:
        """Permanently retire a peer and release retention obligations."""
        self.execute(f"MURMUR PEER RETIRE {_node(node)}")

    def peer_sync(self) -> None:
        """Request progress; does not wait for convergence."""
        self.execute("MURMUR PEER SYNC")

    def show_peers(self) -> List[PeerInfo]:
        return [PeerInfo(r[0], r[1], _boolean(r[2]), r[3], r[4], _boolean(r[5]), _boolean(r[6]))
                for r in self.execute("MURMUR SHOW PEERS")]

    def show_replication(self) -> ReplicationStatus:
        (r,) = self.execute("MURMUR SHOW REPLICATION")
        return ReplicationStatus(_boolean(r[0]), *r[1:])

    def show_retention(self) -> List[RetentionEntry]:
        return [RetentionEntry(r[0], r[1], _boolean(r[2]), *r[3:])
                for r in self.execute("MURMUR SHOW RETENTION")]

    def retention_collect(self) -> int:
        """Run one bounded collection pass and return the deleted log count."""
        return int(self.execute("MURMUR RETENTION COLLECT")[0][0])

    def policy_counter_add(self, table: str, column: str, key: PolicyKey, delta: int) -> None:
        if type(delta) is not int:
            raise ValueError("murmur: counter delta must be int64")
        ks, vp = _policy_key(key)
        self.execute(f"MURMUR POLICY COUNTER_ADD {_target(table, column)} {ks} {vp}{_literal(delta)}")

    def policy_set_add(self, table: str, column: str, key: PolicyKey, value: str) -> None:
        ks, vp = _policy_key(key)
        self.execute(f"MURMUR POLICY SET_ADD {_target(table, column)} {ks} {vp}{_quote(value)}")

    def policy_set_remove(self, table: str, column: str, key: PolicyKey, value: str) -> None:
        ks, vp = _policy_key(key)
        self.execute(f"MURMUR POLICY SET_REMOVE {_target(table, column)} {ks} {vp}{_quote(value)}")

    def policy_min(self, table: str, column: str, key: PolicyKey, value: Union[int, float, str]) -> None:
        ks, vp = _policy_key(key)
        self.execute(f"MURMUR POLICY MIN {_target(table, column)} {ks} {vp}{_literal(value)}")

    def policy_max(self, table: str, column: str, key: PolicyKey, value: Union[int, float, str]) -> None:
        ks, vp = _policy_key(key)
        self.execute(f"MURMUR POLICY MAX {_target(table, column)} {ks} {vp}{_literal(value)}")

    def policy_json_set(self, table: str, column: str, key: PolicyKey, path: str,
                        value: Union[int, float, str, None]) -> None:
        literal = "NULL" if value is None else _literal(value)
        self.execute(f"MURMUR POLICY JSON_SET {_target(table, column)} {_policy_key_marked(key)} PATH {_policy_path(path)} VALUE {literal}")

    def policy_json_remove(self, table: str, column: str, key: PolicyKey, path: str) -> None:
        self.execute(f"MURMUR POLICY JSON_REMOVE {_target(table, column)} {_policy_key_marked(key)} PATH {_policy_path(path)}")

    def policy_list_append(self, table: str, column: str, key: PolicyKey,
                           value: Union[int, float, str, None]) -> None:
        literal = "NULL" if value is None else _literal(value)
        ks, vp = _policy_key(key)
        self.execute(f"MURMUR POLICY LIST_APPEND {_target(table, column)} {ks} {vp}{literal}")

    def policy_list_remove(self, table: str, column: str, key: PolicyKey,
                           value: Union[int, float, str, None]) -> None:
        literal = "NULL" if value is None else _literal(value)
        ks, vp = _policy_key(key)
        self.execute(f"MURMUR POLICY LIST_REMOVE {_target(table, column)} {ks} {vp}{literal}")

    def bridge_release(self, table: str, key: str, column: Optional[str] = None) -> bool:
        """Release High ownership; false means busy (retry). Single-TEXT-key rows only."""
        if not table.strip() or (column is not None and not column.strip()):
            raise ValueError("murmur: table and optional column must not be empty")
        suffix = "" if column is None else f" COLUMN {_quote(column)}"
        rows = self.execute(f"MURMUR BRIDGE RELEASE {_quote(table)} {_quote(key)}{suffix}")
        return _boolean(rows[0][0])
