import importlib
import json
import pathlib

import pytest

EXAMPLES = pathlib.Path(__file__).resolve().parents[1] / "examples"


def test_coverage_manifest_matches_examples():
    coverage = json.loads((EXAMPLES / "coverage.json").read_text())
    on_disk = {p.stem for p in EXAMPLES.glob("*.py") if p.stem != "__init__"}
    listed = {name for names in coverage.values() for name in names}
    assert listed == on_disk
    readme = (EXAMPLES.parent / "README.md").read_text()
    documented = {line.split("|")[1].strip() for line in readme.splitlines()
                  if line.startswith("| ") and not line.startswith("| Feature")}
    assert set(coverage) == documented
    assert all(coverage.values())


@pytest.mark.parametrize("name", sorted(p.stem for p in EXAMPLES.glob("*.py") if p.stem != "__init__"))
def test_example_runs(name):
    if name in ("files", "tls", "replication", "bridge"):
        pytest.importorskip("cryptography")
    importlib.import_module(f"examples.{name}").main()
