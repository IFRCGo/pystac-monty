"""Media types of STAC links (IFRCGo/pystac-monty#165).

A link to a STAC Item must use `application/geo+json`. A link to a STAC Catalog or
Collection must use `application/json`. The STAC JSON Schema does not check this.

Two checks enforce the rule:

- `check_link_media_types` in `tests/conftest.py` checks each link that a test adds
  with `add_link`, for all the sources.
- `test_link_calls_in_the_package_use_the_correct_media_type` reads the source code,
  so it also finds a `Link(...)` call that no test runs.
"""

from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pystac import Collection, Item, Link

import pystac_monty
from tests.conftest import ITEM_TARGETING_RELS, link_media_type_error, record_link_media_type_errors
from tests.utils.test_cases import ARBITRARY_BBOX, ARBITRARY_EXTENT, ARBITRARY_GEOM

PACKAGE_DIR = Path(pystac_monty.__file__).resolve().parent


def _item(item_id: str = "a") -> Item:
    return Item(item_id, ARBITRARY_GEOM, ARBITRARY_BBOX, datetime(2026, 1, 1, tzinfo=timezone.utc), {})


@pytest.mark.parametrize(
    ("rel", "media_type"),
    [
        ("related", "application/geo+json"),
        ("derived_from", "application/geo+json"),
        ("derived_from", "text/html"),
        ("prev", "application/geo+json"),
        ("next", "application/geo+json"),
        ("item", "application/geo+json"),
        ("root", "application/json"),
        ("parent", "application/json"),
        ("collection", "application/json"),
        # Relations with no rule: the target is not a STAC object.
        ("via", "application/json"),
        ("via", "text/csv"),
        ("license", "text/html"),
        ("self", None),
    ],
)
def test_correct_media_type_is_accepted(rel: str, media_type: str | None) -> None:
    assert link_media_type_error(rel, media_type) is None


@pytest.mark.parametrize(
    ("rel", "media_type"),
    [
        ("related", "application/json"),
        ("related", None),
        ("related", "text/html"),
        ("derived_from", "application/json"),
        ("derived_from", None),
        ("prev", "application/json"),
        ("next", None),
        ("item", "application/json"),
        ("collection", "application/geo+json"),
        ("parent", None),
    ],
)
def test_wrong_media_type_is_rejected(rel: str, media_type: str | None) -> None:
    assert link_media_type_error(rel, media_type) is not None


def test_pystac_hierarchy_links_use_the_correct_media_type() -> None:
    """The links that pystac writes itself agree with the rule (the autouse fixture checks them)."""
    collection = Collection("test-collection", "A test collection", ARBITRARY_EXTENT)
    item = _item("test-item")
    collection.add_item(item)

    assert {link.rel: link.media_type for link in item.links} == {
        "root": "application/json",
        "parent": "application/json",
        "collection": "application/json",
    }
    assert [link.media_type for link in collection.links if link.rel == "item"] == ["application/geo+json"]


def _link_calls() -> list[tuple[str, int, ast.expr | None, ast.expr | None]]:
    """Return (file, line, rel, media_type) for each `Link(...)` call of the package."""
    calls = []
    for path in sorted(PACKAGE_DIR.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", None)
            if name != "Link":
                continue
            keywords = {keyword.arg: keyword.value for keyword in node.keywords}
            rel = keywords.get("rel", node.args[0] if node.args else None)
            media_type = keywords.get("media_type", node.args[2] if len(node.args) > 2 else None)
            calls.append((str(path.relative_to(PACKAGE_DIR.parent)), node.lineno, rel, media_type))
    return calls


def test_link_calls_in_the_package_use_the_correct_media_type() -> None:
    calls = _link_calls()
    # A refactoring that renames `Link` must not make this test pass with no input.
    assert len(calls) >= 30
    assert any(isinstance(rel, ast.Constant) and rel.value in ITEM_TARGETING_RELS for _, _, rel, _ in calls)

    errors = []
    for file, line, rel, media_type in calls:
        if not isinstance(rel, ast.Constant):
            errors.append(f"{file}:{line}: write `rel` as a string literal, so that this test can check the link")
            continue
        if media_type is not None and not isinstance(media_type, ast.Constant):
            if rel.value in ITEM_TARGETING_RELS:
                errors.append(f"{file}:{line}: write `media_type` as a string literal, so that this test can check the link")
            continue
        error = link_media_type_error(rel.value, media_type.value if media_type is not None else None)
        if error:
            errors.append(f"{file}:{line}: {error}")
    assert not errors, "\n".join(errors)


def test_recorder_reports_a_wrong_link() -> None:
    """The recorder of the autouse fixture finds an Item link with `application/json`."""
    with record_link_media_type_errors() as errors:
        item = _item()
        item.add_link(Link(rel="related", target="./b.json", media_type="application/geo+json"))
        item.add_link(Link(rel="related", target="./c.json", media_type="application/json"))
        item.add_link(Link(rel="derived_from", target="./d.json"))
        item.add_link(Link(rel="via", target="https://example.org/d", media_type="application/json"))

    assert errors == [
        'Item a: rel="related" targets a STAC Item: the type must be "application/geo+json", not \'application/json\'',
        'Item a: rel="derived_from" targets a STAC Item: the type must be "application/geo+json", not None',
    ]
    assert len(item.links) == 4


def test_recorder_ignores_the_links_of_an_item_that_is_read() -> None:
    """An item that a test reads from a file is input, so its links are not checked."""
    item_dict = _item().to_dict()
    item_dict["links"] = [{"rel": "related", "href": "./b.json", "type": "application/json"}]

    with record_link_media_type_errors() as errors:
        item = Item.from_dict(item_dict)
        assert errors == []
        item.add_link(Link(rel="prev", target="./c.json", media_type="application/json"))

    assert len(errors) == 1


def test_link_objects_keep_the_media_type_in_the_item_dict() -> None:
    item = _item()
    item.add_link(Link(rel="related", target="./b.json", media_type="application/geo+json", extra_fields={"roles": ["event"]}))
    assert item.to_dict()["links"] == [{"rel": "related", "href": "./b.json", "type": "application/geo+json", "roles": ["event"]}]
