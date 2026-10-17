"""SQLite and Spool operational metrics."""
import servertest
import murmur


def main():
    with servertest.Scratch() as scratch:
        server = scratch.start()
        with murmur.connect(
            murmur.Config(port=server.port, user="admin", password=servertest.PASSWORD, key="example-key")
        ) as db:
            everything, sqlite = db.metrics(), db.metrics("sqlite")
            assert everything and sqlite and len(sqlite) <= len(everything)
            print(f"metrics: {len(everything)} total, {len(sqlite)} sqlite")


if __name__ == "__main__":
    main()
