"""Each transformer and its source doc in monty-stac-extension name each other (IFRCGo/pystac-monty#180).

`docs/model/sources/sources.yml` in monty-stac-extension is the registry of the sources.
Each entry gives the source doc (`doc`) and the transformer (`etl`, a URL into this
repository). Each transformer here gives the same two values: `source_name` is the
`id` of the entry, and `source_doc` is the path of the source doc.

These tests fail when:

- a transformer has no entry in the registry, or names a different source doc;
- an entry of the registry names a transformer that does not exist;
- a source doc does not exist.
"""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path
from typing import Any

import pytest
import yaml

import pystac_monty.sources
from pystac_monty.sources.common import MontyDataTransformer

REPOSITORY = Path(__file__).resolve().parents[1]
EXTENSION = REPOSITORY / "monty-stac-extension"
SOURCES_YML = EXTENSION / "docs" / "model" / "sources" / "sources.yml"
ETL_URL_PREFIX = "https://github.com/IFRCGo/pystac-monty/blob/main/"

pytestmark = pytest.mark.skipif(not SOURCES_YML.is_file(), reason="monty-stac-extension submodule not initialized")


def _transformers() -> dict[str, type[MontyDataTransformer[Any]]]:
    """Return the transformer classes of the package, by `source_name`."""
    for module in pkgutil.iter_modules(pystac_monty.sources.__path__):
        importlib.import_module(f"{pystac_monty.sources.__name__}.{module.name}")

    found: dict[str, type[MontyDataTransformer[Any]]] = {}
    pending = list(MontyDataTransformer.__subclasses__())
    while pending:
        cls = pending.pop()
        pending.extend(cls.__subclasses__())
        if not cls.__module__.startswith(f"{pystac_monty.sources.__name__}."):
            continue  # a subclass that a test defines
        name = getattr(cls, "source_name", None)
        assert name, f"{cls.__name__} has no `source_name`"
        assert name not in found, f"{cls.__name__} and {found[name].__name__} have the same `source_name`: {name}"
        found[name] = cls
    return found


def _registry() -> dict[str, dict[str, Any]]:
    """Return the entries of `sources.yml`, by `id`."""
    if not SOURCES_YML.is_file():
        return {}
    data = yaml.safe_load(SOURCES_YML.read_text(encoding="utf-8"))
    return {entry["id"]: entry for entry in data["sources"]}


TRANSFORMERS = _transformers()
REGISTRY = _registry()


def _module_path(cls: type) -> str:
    return cls.__module__.replace(".", "/") + ".py"


def test_the_package_has_transformers() -> None:
    # A change to the discovery must not make the other tests pass with no input.
    assert len(TRANSFORMERS) >= 13


@pytest.mark.parametrize("source_name", sorted(TRANSFORMERS))
def test_transformer_names_its_source_doc(source_name: str) -> None:
    cls = TRANSFORMERS[source_name]
    source_doc = getattr(cls, "source_doc", None)
    assert source_doc, f"{cls.__name__} has no `source_doc`: give the path of its source doc in monty-stac-extension"
    assert (EXTENSION / source_doc).is_file(), f"{cls.__name__}.source_doc: {source_doc} is not a file in monty-stac-extension"

    assert source_name in REGISTRY, (
        f"{cls.__name__}: `{source_name}` is not an `id` in monty-stac-extension/docs/model/sources/sources.yml. "
        f"A transformer needs a source analysis (see CONTRIBUTING.md)."
    )
    entry = REGISTRY[source_name]
    assert entry["doc"] is not None, f"sources.yml: `{source_name}` has a transformer here, but no `doc`"
    assert source_doc == f"docs/model/sources/{entry['doc']}", (
        f"{cls.__name__}.source_doc is {source_doc}, but sources.yml gives docs/model/sources/{entry['doc']}"
    )
    assert entry["etl"] == f"{ETL_URL_PREFIX}{_module_path(cls)}", (
        f"sources.yml: the `etl` of `{source_name}` is {entry['etl']}, but the transformer is in {_module_path(cls)}"
    )


@pytest.mark.parametrize("source_id", sorted(REGISTRY))
def test_registry_entry_names_a_transformer_that_exists(source_id: str) -> None:
    etl = REGISTRY[source_id]["etl"]
    if etl is None:
        assert source_id not in TRANSFORMERS, (
            f"sources.yml: `{source_id}` has `etl: null`, but {TRANSFORMERS[source_id].__name__} implements it"
        )
        return

    assert etl.startswith(ETL_URL_PREFIX), f"sources.yml: the `etl` of `{source_id}` must start with {ETL_URL_PREFIX}"
    path = etl.removeprefix(ETL_URL_PREFIX)
    assert (REPOSITORY / path).is_file(), f"sources.yml: the `etl` of `{source_id}` names {path}, which does not exist"
    assert source_id in TRANSFORMERS, f"sources.yml: `{source_id}` names {path}, but no transformer has that `source_name`"
