"""The worked examples in monty-stac-extension must still come out of the transformers.

monty-stac-extension says which sources it can regenerate: each source with a
`regenerate:` recipe in its `.github/etl-watch.yml` names a batch exporter of this
package and a fixture in that repository. This test runs the checker of that
repository (`scripts/check_example_drift.py`) on each of those sources, against the
code in this checkout. So the comparison has one implementation, and a new recipe
in the extension is tested here when the submodule moves to it.

The checker ignores `links`, `created`, `updated` and `processing:*`, and compares
`keywords` and `roles` as sets. A source can have its own, stricter test (CEMS and
Charter compare the links too).

If this test fails because the change is intended, update the examples in
monty-stac-extension and move the submodule. CONTRIBUTING.md gives the steps.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

EXTENSION = Path(__file__).resolve().parents[1] / "monty-stac-extension"
CHECKER = EXTENSION / "scripts" / "check_example_drift.py"


def _load_checker() -> ModuleType | None:
    if not CHECKER.is_file():
        return None
    spec = importlib.util.spec_from_file_location("check_example_drift", CHECKER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # the checker puts its own directory on sys.path
    return module


checker = _load_checker()


def _recipes() -> dict[str, tuple[dict[str, Any], dict[str, Any]]]:
    if checker is None:
        return {}
    entries = checker.load_watch()["sources"]
    sources = {source["id"]: source for source in checker.load_sources()}
    return {
        source_id: (sources[source_id], entry["regenerate"])
        for source_id, entry in entries.items()
        if (entry or {}).get("regenerate")
    }


RECIPES = _recipes()


def _report(result: Any) -> str:
    lines = []
    if result.rejected:
        lines.append(f"the transformer rejected the fixture: {result.rejected}")
    for item_id, path, closest in result.missing:
        hint = f" (closest generated id: {closest})" if closest else ""
        lines.append(f"no longer produced: {item_id} [{path.relative_to(EXTENSION)}]{hint}")
    for item_id, path, diffs in result.changed:
        lines.append(f"changed: {item_id} [{path.relative_to(EXTENSION)}]")
        lines.extend(f"    {diff}" for diff in diffs)
    return "\n".join(lines)


@pytest.mark.skipif(checker is None, reason="monty-stac-extension submodule not initialized")
def test_extension_declares_recipes() -> None:
    assert RECIPES, "no source in monty-stac-extension/.github/etl-watch.yml has a `regenerate:` recipe"


@pytest.mark.skipif(checker is None, reason="monty-stac-extension submodule not initialized")
@pytest.mark.parametrize("source_id", sorted(RECIPES))
def test_extension_examples_reproduce(source_id: str) -> None:
    source, recipe = RECIPES[source_id]
    assert checker is not None

    committed = checker.committed_items(source, recipe)
    assert committed, f"the {source_id} recipe matches no committed example, so it checks nothing"

    result = checker.check_source(source, recipe)
    # In this repository the package is importable, so a skip means that the recipe
    # names an exporter or a fixture that does not exist.
    assert not result.skipped, f"{source_id}: {result.skipped}"
    assert not result.drifted, (
        f"{source_id}: the transformer no longer reproduces the monty-stac-extension examples.\n"
        f"{_report(result)}\n"
        f"If the change is intended, run "
        f"`python scripts/check_example_drift.py --source {source_id} --write` in monty-stac-extension "
        f"with this checkout installed, open a PR there and move the submodule (see CONTRIBUTING.md)."
    )
