"""GDACS provider cross-references (`glide`, `source`/`sourceid`) become `related` links on the event item."""

import copy
import datetime
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pystac import Item

from pystac_monty.sources.gdacs import GDACSTransformer
from pystac_monty.validators.gdacs_events import GdacsEventDataValidator

GDACS_FIXTURES = Path(__file__).resolve().parent.parent / "data-files" / "gdacs"
EOAPI = "https://montandon.test/stac"


def _transformer() -> GDACSTransformer:
    transformer = GDACSTransformer.__new__(GDACSTransformer)
    transformer.data_source = SimpleNamespace(eoapi_url=EOAPI)
    return transformer


def _event(**properties) -> GdacsEventDataValidator:
    raw = json.loads((GDACS_FIXTURES / "fl-1102983-ep1-event.json").read_text(encoding="utf-8"))
    raw = copy.deepcopy(raw)
    raw["properties"].update(properties)
    return GdacsEventDataValidator(**raw)


def _related(data: GdacsEventDataValidator) -> list[tuple[str, str | None, list]]:
    item = Item(id="gdacs-event-1-1", geometry=None, bbox=None, datetime=datetime.datetime(2024, 10, 27), properties={})
    _transformer().add_cross_reference_links(item=item, data=data)
    return [(link.href, link.title, link.extra_fields.get("roles")) for link in item.links if link.rel == "related"]


def test_glide_number_links_to_glide_event() -> None:
    links = _related(_event(glide="FL-2024-000199-ESP", source="GLOFAS", sourceid="5c949dea-419b-4f50-90f0-e7271adf619d"))
    assert links == [
        (f"{EOAPI}/collections/glide-events/items/glide-event-FL-2024-000199-ESP", "GLIDE FL-2024-000199-ESP", ["event"])
    ]


def test_neic_sourceid_links_to_usgs_event() -> None:
    links = _related(_event(eventtype="EQ", glide="", source="NEIC", sourceid="us7000txz3"))
    assert links == [(f"{EOAPI}/collections/usgs-events/items/usgs-event-us7000txz3", "USGS event us7000txz3", ["event"])]


@pytest.mark.parametrize("source", ["GLOFAS", "GWIS", "JTWC", "NOAA", "GDO", "DARWIN"])
def test_source_without_monty_collection_gets_no_link(source: str) -> None:
    assert _related(_event(glide="", source=source, sourceid="16077684")) == []


@pytest.mark.parametrize("glide", ["", "  ", "FL-2024-ESP", "n/a"])
def test_empty_or_malformed_glide_gets_no_link(glide: str) -> None:
    assert _related(_event(glide=glide, source="GLOFAS", sourceid="")) == []


def test_glide_is_normalised() -> None:
    links = _related(_event(glide=" tc-2026-000199-ind ", source="JTWC", sourceid=""))
    assert [href for href, _, _ in links] == [f"{EOAPI}/collections/glide-events/items/glide-event-TC-2026-000199-IND"]
