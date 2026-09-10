"""Guard against published STAC Item examples with a null geometry.

The eoapi/pgstac server used to serve Monty data rejects Items whose
``geometry`` is ``null`` (see https://github.com/IFRCGo/pystac-monty/issues/226),
so none of the curated examples under the ``monty-stac-extension`` submodule
should ever ship one.
"""

from __future__ import annotations

import json
from pathlib import Path


def _examples_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "monty-stac-extension" / "examples"


def test_examples_have_no_null_geometry() -> None:
    examples_dir = _examples_dir()
    example_paths = sorted(examples_dir.rglob("*.json"))
    if not example_paths:
        raise AssertionError(f"no example files found under {examples_dir}; is the monty-stac-extension submodule initialized?")

    offenders: list[str] = []
    for path in example_paths:
        doc = json.loads(path.read_text(encoding="utf-8"))
        if doc.get("type") == "Feature" and doc.get("geometry") is None:
            offenders.append(str(path.relative_to(examples_dir)))

    assert not offenders, f"examples with null geometry (not accepted by the STAC API server): {offenders}"
