"""Encrypted binary upload/download and LIKE cursor search."""
import io

import servertest
import murmur


def main():
    with servertest.Scratch() as scratch:
        server = scratch.start(**servertest.file_options(scratch.dir))
        with murmur.connect(
            murmur.Config(port=server.port, user="admin", password=servertest.PASSWORD, key="files-example")
        ) as db:
            payload = bytes([0, 1, 255, 128]) * 40000
            info = db.upload_file("reports/sample.bin", io.BytesIO(payload))
            found = list(db.search_files("reports/%.bin"))
            assert [(f.name, f.size, f.local) for f in found] == [(info.name, len(payload), True)], found
            out = io.BytesIO()
            db.download_file(info.name, out)
            assert out.getvalue() == payload
            print(f"Verified {info.name}: {info.size} bytes, SHA256 {info.sha256}")


if __name__ == "__main__":
    main()
