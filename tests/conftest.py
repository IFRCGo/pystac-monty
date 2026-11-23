# TODO move all test case code to this file

import shutil
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pystac import Asset, Catalog, Collection, Item, Link, STACObject

from .utils import ARBITRARY_BBOX, ARBITRARY_EXTENT, ARBITRARY_GEOM, TestCases

here = Path(__file__).resolve().parent

ITEM_MEDIA_TYPE = "application/geo+json"
CATALOG_MEDIA_TYPE = "application/json"
WEB_PAGE_MEDIA_TYPE = "text/html"

# Link relations whose target is a STAC Item (a GeoJSON Feature).
ITEM_TARGETING_RELS = frozenset(
    {"related", "derived_from", "prev", "next", "item", "latest-version", "predecessor-version", "successor-version"}
)
# Link relations whose target is a STAC Catalog or Collection.
CATALOG_TARGETING_RELS = frozenset({"root", "parent", "collection"})
# `derived_from` can also point to the web page of the source (a CEMS or Charter activation page).
WEB_PAGE_RELS = frozenset({"derived_from"})


def link_media_type_error(rel: str, media_type: str | None) -> str | None:
    """Return the reason why `media_type` is wrong for a link with relation `rel`, or None.

    The STAC JSON Schema does not constrain the `type` of a link, so the schema
    validation cannot find a wrong value (IFRCGo/pystac-monty#165).
    """
    if rel in ITEM_TARGETING_RELS:
        if media_type == ITEM_MEDIA_TYPE:
            return None
        if media_type == WEB_PAGE_MEDIA_TYPE and rel in WEB_PAGE_RELS:
            return None
        return f'rel="{rel}" targets a STAC Item: the type must be "{ITEM_MEDIA_TYPE}", not {media_type!r}'
    if rel in CATALOG_TARGETING_RELS and media_type != CATALOG_MEDIA_TYPE:
        return f'rel="{rel}" targets a STAC Catalog or Collection: the type must be "{CATALOG_MEDIA_TYPE}", not {media_type!r}'
    return None


_ADD_LINK = STACObject.add_link
_FROM_DICT = {cls: cls.__dict__["from_dict"].__func__ for cls in (Item, Collection, Catalog)}


@contextmanager
def record_link_media_type_errors() -> Iterator[list[str]]:
    """Record the links with a wrong media type that the code adds in the `with` block.

    A link of an object that is read with `from_dict` or `from_file` is not checked:
    it is test input, not output of the code under test.
    """
    errors: list[str] = []
    reading = 0

    def checked_add_link(self: STACObject, link: Link) -> None:
        if not reading:
            error = link_media_type_error(str(link.rel), link.media_type)
            if error:
                errors.append(f"{type(self).__name__} {self.id}: {error}")
        _ADD_LINK(self, link)

    def unchecked_from_dict(function: Any) -> Any:
        def from_dict(cls: Any, *args: Any, **kwargs: Any) -> Any:
            nonlocal reading
            reading += 1
            try:
                return function(cls, *args, **kwargs)
            finally:
                reading -= 1

        return classmethod(from_dict)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(STACObject, "add_link", checked_add_link)
        for cls, function in _FROM_DICT.items():
            monkeypatch.setattr(cls, "from_dict", unchecked_from_dict(function))
        yield errors


@pytest.fixture(autouse=True)
def check_link_media_types() -> Iterator[None]:
    """Fail a test when the code under test adds a link with a wrong media type.

    The check applies to each test, so each transformer is checked on the items that
    its own tests emit. The errors are collected and reported at the end of the test,
    because a transformer can catch an exception that is raised in `add_link`.
    """
    with record_link_media_type_errors() as errors:
        yield
    assert not errors, "Links with a wrong media type:\n" + "\n".join(sorted(set(errors)))


@pytest.fixture
def catalog() -> Catalog:
    return Catalog("test-catalog", "A test catalog")


@pytest.fixture
def collection() -> Catalog:
    return Collection("test-collection", "A test collection", ARBITRARY_EXTENT)


@pytest.fixture
def item() -> Item:
    return Item("test-item", ARBITRARY_GEOM, ARBITRARY_BBOX, datetime.now(), {})


@pytest.fixture
def asset(item: Item) -> Asset:
    item.add_asset("foo", Asset("https://example.tif"))
    return item.assets["foo"]


@pytest.fixture
def link(item: Item) -> Link:
    item.add_link(Link(rel="child", target="https://example.tif"))
    return item.links[0]


@pytest.fixture
def test_case_1_catalog() -> Catalog:
    return TestCases.case_1()


@pytest.fixture
def test_case_8_collection() -> Collection:
    return TestCases.case_8()


@pytest.fixture
def projection_landsat8_item() -> Item:
    path = TestCases.get_path("data-files/projection/example-landsat8.json")
    return Item.from_file(path)


def get_data_file(rel_path: str) -> str:
    return str(here / "data-files" / rel_path)


@pytest.fixture
def sample_item() -> Item:
    return Item.from_file(TestCases.get_path("data-files/item/sample-item.json"))


@pytest.fixture(scope="function")
def tmp_asset(tmp_path: Path) -> Asset:
    """Copy the entirety of test-case-2 to tmp and"""
    src = get_data_file("catalogs/test-case-2")
    dst = str(tmp_path / str(uuid.uuid4()))
    shutil.copytree(src, dst)

    catalog = Catalog.from_file(f"{dst}/catalog.json")
    item = next(catalog.get_items(recursive=True))
    return next(v for v in item.assets.values())


@pytest.fixture(autouse=True)
def clear_validator() -> None:
    from pystac.validation import RegisteredValidator

    RegisteredValidator._validator = None
