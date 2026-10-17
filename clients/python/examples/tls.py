"""Verified TLS against a minted test PKI."""
import os

import servertest
import murmur


def main():
    with servertest.Scratch() as scratch:
        pki = servertest.new_pki()
        server = scratch.start(tls=pki.write_server_files(scratch.dir))
        ca = os.path.join(scratch.dir, "ca.crt")
        cfg = murmur.Config(
            port=server.port, user="admin", password=servertest.PASSWORD, key="example-key",
            tls=murmur.TLSConfig(mode="verify-full", ca_file=ca),
        )
        with murmur.connect(cfg) as db:
            assert db.execute("SELECT 1") == [[1]]
        print("tls: verified verify-full connection")


if __name__ == "__main__":
    main()
