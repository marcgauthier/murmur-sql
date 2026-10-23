"""Scratch murmur-server harness for the Python tests and examples.

Builds ``murmur-server`` through Go once per process (or reuses
``MURMUR_SERVER_BIN``) and spawns throwaway instances. Dev-only: not part of
the installable ``murmur`` package. PKI helpers need the ``cryptography``
package (``pip install -e .[test]``).
"""

from __future__ import annotations

import datetime
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

ROOT = Path(__file__).resolve().parents[2]
_EXE = ".exe" if sys.platform == "win32" else ""
_built: Optional[tuple] = None
_hash: Optional[str] = None
PASSWORD = "test-password"


def build() -> tuple:
    """Return ``(server_bin, admin_bin)``, compiling with Go when needed."""
    global _built
    if _built:
        return _built
    prebuilt = os.environ.get("MURMUR_SERVER_BIN")
    if prebuilt:
        _built = (prebuilt, str(Path(prebuilt).parent / ("murmur-admin" + _EXE)))
        return _built
    target = ROOT / "bin"
    target.mkdir(exist_ok=True)
    for name in ("murmur-server", "murmur-admin"):
        subprocess.run(["go", "build", "-o", str(target / (name + _EXE)), "./cmd/" + name], cwd=ROOT, check=True)

    _built = (str(target / ("murmur-server" + _EXE)), str(target / ("murmur-admin" + _EXE)))
    return _built


def password_hash() -> str:
    global _hash
    if _hash is None:
        _, admin = build()
        out = subprocess.run(
            [admin, "hash-password", "--password", PASSWORD], check=True, capture_output=True, text=True
        ).stdout.strip()
        assert out.startswith("$argon2id$"), out
        _hash = out
    return _hash


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class TLSFiles:
    cert: str
    key: str
    ca: str


class Server:
    def __init__(self, directory: str, port: int, proc: subprocess.Popen):
        self.dir = directory
        self.port = port
        self._proc = proc

    def stop(self) -> None:
        if self._proc.poll() is None:
            self._proc.kill()
            self._proc.wait()


def start(
    directory: str,
    node_id: str = "",
    tls: Optional[TLSFiles] = None,
    extra_yaml: str = "",
    database_id: str = "",
) -> Server:
    """Write a scratch config (user ``admin``) and spawn murmur-server."""
    server_bin, _ = build()
    port = free_port()
    config = Path(directory) / "config.yaml"
    tls_section = ""
    if tls:
        tls_section = (
            f"  tls:\n    enabled: true\n    cert_file: {_q(tls.cert)}\n"
            f"    key_file: {_q(tls.key)}\n"
        )
    node_line = f"  node_id: {_q(node_id)}\n" if node_id else ""
    if database_id:
        node_line += f"  database_id: {_q(database_id)}\n"
    config.write_text(
        f"server:\n  bind: \"127.0.0.1:{port}\"\n  production: false\n{tls_section}\n"
        f"storage:\n  data_dir: {_q(Path(directory) / 'data')}\n{node_line}\n"
        f"auth:\n  users:\n    - name: \"admin\"\n      password_hash: {_q(password_hash())}\n\n"
        f"logging:\n  dir: {_q(Path(directory) / 'logs')}\n\n"
        f"backups:\n  dir: {_q(Path(directory) / 'backups')}\n\n{extra_yaml}"
    )
    proc = subprocess.Popen([server_bin, "--config", str(config)], stderr=subprocess.PIPE)
    deadline = time.time() + 20
    while True:
        try:
            socket.create_connection(("127.0.0.1", port), 0.2).close()
            break
        except OSError:
            if proc.poll() is not None:
                raise RuntimeError("server exited early: " + proc.stderr.read().decode())
            if time.time() > deadline:
                proc.kill()
                raise RuntimeError(f"server on port {port} did not start")
            time.sleep(0.05)
    return Server(directory, port, proc)


def _p(path) -> str:
    return str(path).replace("\\", "/")


def _q(value) -> str:
    import json

    return json.dumps(_p(value))


class Scratch:
    """Temp directory plus servers, all cleaned up on ``close``."""

    def __init__(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="murmur-py-")
        self._servers: List[Server] = []

    def start(self, **kwargs) -> Server:
        server = start(self.dir, **kwargs)
        self._servers.append(server)
        return server

    def close(self) -> None:
        for s in self._servers:
            s.stop()
        shutil.rmtree(self.dir, ignore_errors=True)

    def __enter__(self) -> "Scratch":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# --- PKI (requires `cryptography`) ------------------------------------------


def _key():
    from cryptography.hazmat.primitives.asymmetric import ec

    return ec.generate_private_key(ec.SECP256R1())


def _pem_cert(cert) -> bytes:
    from cryptography.hazmat.primitives import serialization

    return cert.public_bytes(serialization.Encoding.PEM)


def _pem_key(key) -> bytes:
    from cryptography.hazmat.primitives import serialization

    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _issue(subject: str, issuer_name, issuer_key, public_key, serial: int, ca: bool, usages, sans):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.x509.oid import NameOID

    now = datetime.datetime.now(datetime.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, subject)]))
        .issuer_name(issuer_name)
        .public_key(public_key)
        .serial_number(serial)
        .not_valid_before(now - datetime.timedelta(hours=1))
        .not_valid_after(now + datetime.timedelta(hours=24))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
    )
    if usages:
        builder = builder.add_extension(x509.ExtendedKeyUsage(usages), critical=False)
    if sans:
        builder = builder.add_extension(x509.SubjectAlternativeName(sans), critical=False)
    return builder.sign(issuer_key, hashes.SHA256())


@dataclass
class PKI:
    ca: bytes
    server_cert: bytes
    server_key: bytes
    client_cert: bytes
    client_key: bytes

    def write_server_files(self, directory: str) -> TLSFiles:
        files = {"ca.crt": self.ca, "server.crt": self.server_cert, "server.key": self.server_key}
        for name, data in files.items():
            (Path(directory) / name).write_bytes(data)
        return TLSFiles(*(str(Path(directory) / n) for n in ("server.crt", "server.key", "ca.crt")))

    def write_client_files(self, directory: str) -> tuple:
        cert, key = Path(directory) / "client.crt", Path(directory) / "client.key"
        cert.write_bytes(self.client_cert)
        key.write_bytes(self.client_key)
        return str(cert), str(key)


def new_pki() -> PKI:
    import ipaddress

    from cryptography import x509
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    ca_key = _key()
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "murmur test CA")])
    ca = _issue("murmur test CA", ca_name, ca_key, ca_key.public_key(), 1, True, None, None)
    server_key, client_key = _key(), _key()
    server = _issue(
        "murmur test leaf", ca_name, ca_key, server_key.public_key(), 2, False,
        [ExtendedKeyUsageOID.SERVER_AUTH], [x509.IPAddress(ipaddress.ip_address("127.0.0.1"))],
    )
    client = _issue(
        "murmur test leaf", ca_name, ca_key, client_key.public_key(), 3, False,
        [ExtendedKeyUsageOID.CLIENT_AUTH], None,
    )
    return PKI(_pem_cert(ca), _pem_cert(server), _pem_key(server_key), _pem_cert(client), _pem_key(client_key))


def file_options(directory: str) -> dict:
    """``start`` kwargs for a standalone encrypted file store (no peers)."""
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    node = "09" * 16
    ca_key, leaf_key = _key(), _key()
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "files CA")])
    ca = _issue("files CA", ca_name, ca_key, ca_key.public_key(), 10, True, None, None)
    leaf = _issue(
        "files node", ca_name, ca_key, leaf_key.public_key(), 11, False,
        [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH],
        [x509.UniformResourceIdentifier("replicateddb://node/" + node)],
    )
    d = Path(directory)
    (d / "file-ca.pem").write_bytes(_pem_cert(ca))
    (d / "file-node.pem").write_bytes(_pem_cert(leaf))
    (d / "file-key.pem").write_bytes(_pem_key(leaf_key))
    signing = ed25519.Ed25519PrivateKey.from_private_bytes(b"\x03" * 32)
    public_hex = signing.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    ).hex()
    yaml = (
        'replication:\n  enabled: true\n  bind: "127.0.0.1:0"\n'
        f'  cert_chain_file: {_q(d / "file-node.pem")}\n'
        f'  private_key_file: {_q(d / "file-key.pem")}\n'
        f'  ca_file: {_q(d / "file-ca.pem")}\n'
        f'  signing_key_hex: "{"03" * 32}"\n'
        f'  trusted_origins:\n    "{node}": "{public_hex}"\n'
        f'  files:\n    directory: {_q(d / "objects")}\n    key_hex: "{"07" * 32}"\n'
    )
    return {"node_id": node, "extra_yaml": yaml}

def mesh_options(directories, replication_extra=""):
    """Shared CA/database id and distinct trusted node identities; no initial peers."""
    import secrets
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    ca_key = _key()
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "mesh CA")])
    ca = _issue("mesh CA", ca_name, ca_key, ca_key.public_key(), 1, True, None, None)
    nodes = [secrets.token_hex(16) for _ in directories]
    seeds = [secrets.token_bytes(32) for _ in directories]
    public = [ed25519.Ed25519PrivateKey.from_private_bytes(seed).public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw).hex() for seed in seeds]
    trusted = "\n".join(f'    "{node}": "{key}"' for node, key in zip(nodes, public))
    database_id = secrets.token_hex(16)
    options = []
    for i, (directory, node, seed) in enumerate(zip(directories, nodes, seeds)):
        d = Path(directory)
        d.mkdir(parents=True, exist_ok=True)
        leaf_key = _key()
        leaf = _issue("mesh node", ca_name, ca_key, leaf_key.public_key(), i + 2, False,
                      [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH],
                      [x509.UniformResourceIdentifier("replicateddb://node/" + node)])
        (d / "mesh-ca.pem").write_bytes(_pem_cert(ca))
        (d / "mesh-node.pem").write_bytes(_pem_cert(leaf))
        (d / "mesh-key.pem").write_bytes(_pem_key(leaf_key))
        extra = f"\n{replication_extra.rstrip()}\n" if replication_extra else ""
        yaml = (
            'replication:\n  enabled: true\n  bind: "127.0.0.1:0"\n'
            f'  cert_chain_file: {_q(d / "mesh-node.pem")}\n'
            f'  private_key_file: {_q(d / "mesh-key.pem")}\n'
            f'  ca_file: {_q(d / "mesh-ca.pem")}\n'
            f'  signing_key_hex: "{seed.hex()}"\n'
            f'  trusted_origins:\n{trusted}{extra}\n'
        )
        options.append(dict(node_id=node, database_id=database_id, extra_yaml=yaml))
    return options
