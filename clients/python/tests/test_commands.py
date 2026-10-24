import io
import pytest
import murmur
from murmur.commands import _literal
from murmur.client import _write_all


@pytest.mark.parametrize("value", [True, False, 2**63, -(2**63)-1, float("nan"), float("inf"), None, b"x"])
def test_invalid_policy_literal(value):
    with pytest.raises(ValueError):
        _literal(value)


def test_policy_literals():
    assert _literal("a'b") == "'a''b'"
    assert _literal(2**63-1) == str(2**63-1)
    assert _literal(-(2**63)) == str(-(2**63))
    assert _literal(100.0) == "100.0"


def test_policy_key_forms():
    from murmur.commands import _policy_key
    assert _policy_key("k") == ("'k'", "")
    assert _policy_key(["a", 9, 2.5]) == ("KEY 'a' 9 2.5", "VALUE ")
    for bad in ([], [True], 7, None):
        with pytest.raises(ValueError):
            _policy_key(bad)


def test_policy_builders_render_composite_keys():
    class Recording(murmur.Connection):
        def __init__(self):
            self.sent = []

        def execute(self, sql, params=()):
            self.sent.append(sql)
            return []

    db = Recording()
    db.policy_counter_add("t", "c", "k", 1)
    db.policy_counter_add("t", "c", ["a", 9], 1)
    db.policy_min("t", "c", [1.5], -1)
    db.policy_json_set("t", "c", "k", "a", 1)
    db.policy_json_set("t", "c", ["a", 9], "b", None)
    db.policy_json_remove("t", "c", ["a", 9], "b")
    db.policy_list_append("t", "c", "k", 1)
    db.policy_list_append("t", "c", ["a", 9], None)
    db.policy_list_remove("t", "c", "k", "x")
    assert db.sent == [
        "MURMUR POLICY COUNTER_ADD 't' 'c' 'k' 1",
        "MURMUR POLICY COUNTER_ADD 't' 'c' KEY 'a' 9 VALUE 1",
        "MURMUR POLICY MIN 't' 'c' KEY 1.5 VALUE -1",
        "MURMUR POLICY JSON_SET 't' 'c' KEY 'k' PATH 'a' VALUE 1",
        "MURMUR POLICY JSON_SET 't' 'c' KEY 'a' 9 PATH 'b' VALUE NULL",
        "MURMUR POLICY JSON_REMOVE 't' 'c' KEY 'a' 9 PATH 'b'",
        "MURMUR POLICY LIST_APPEND 't' 'c' 'k' 1",
        "MURMUR POLICY LIST_APPEND 't' 'c' KEY 'a' 9 VALUE NULL",
        "MURMUR POLICY LIST_REMOVE 't' 'c' 'k' 'x'",
    ]
    with pytest.raises(ValueError):
        db.policy_json_set("t", "c", "k", "", 1)


class PartialWriter:
    def __init__(self):
        self.output = bytearray()

    def write(self, value):
        part = value[:3]
        self.output.extend(part)
        return len(part)


def test_partial_writes():
    dest = PartialWriter()
    _write_all(dest, b"abcdefghij")
    assert dest.output == b"abcdefghij"


@pytest.mark.parametrize("count", [None, 0, -1, 100])
def test_failed_write_counts(count):
    class Writer:
        def write(self, value):
            return count
    with pytest.raises(OSError):
        _write_all(Writer(), b"abc")


def test_helpers_validate_before_sending():
    class NoSQL(murmur.Connection):
        def __init__(self):
            pass

        def execute(self, *args):
            raise AssertionError("invalid command reached server")

    db = NoSQL()
    for addr in ["bad", "::1:123", "127.0.0.1:99999", "[127.0.0.1]:42"]:
        with pytest.raises(ValueError):
            db.peer_add(addr)
    for fn in [db.peer_remove, db.peer_retire]:
        with pytest.raises(ValueError):
            fn("z" * 32)
    for options in [murmur.VectorOptions(0), murmur.VectorOptions(8193),
                    murmur.VectorOptions(1.5), murmur.VectorOptions(2, "bad")]:
        with pytest.raises(ValueError):
            db.set_column_vector("t", "c", options)
    with pytest.raises(ValueError):
        db.set_column_fts("", "c", True)
    with pytest.raises(ValueError):
        db.bridge_release("t", "k", " ")
    with pytest.raises(ValueError):
        db.policy_counter_add("t", "c", "k", True)


def test_commands_need_replication():
    import servertest
    with servertest.Scratch() as scratch:
        server = scratch.start()
        with murmur.connect(murmur.Config(port=server.port, user="admin",
                            password=servertest.PASSWORD, key="key")) as db:
            for fn in [db.show_peers, db.show_replication, db.show_retention,
                       db.retention_collect, db.peer_sync,
                       lambda: db.peer_add("127.0.0.1:9"),
                       lambda: db.policy_counter_add("t", "c", "k", 1),
                       lambda: db.policy_set_add("t", "c", "k", "x"),
                       lambda: db.policy_set_remove("t", "c", "k", "x"),
                       lambda: db.policy_min("t", "c", "k", -1.5),
                       lambda: db.policy_max("t", "c", "k", 2),
                       lambda: db.bridge_release("t", "k")]:
                with pytest.raises(Exception) as err:
                    fn()
                assert murmur.code_of(err.value) == "55000"
