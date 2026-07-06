"""Tests for static Monty STAC export."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pystac
import pytest
from pystac import Item
from pystac.provider import Provider, ProviderRole

from pystac_monty.exporter import (
    MONTY_STAC_EXAMPLES_BASE_URL,
    BatchExportConfig,
    MontyCollectionSpec,
    build_empty_static_source_collection,
    export_collected_items,
    export_monty_collection,
    extent_for_monty_static_collection,
    log_batch_role_counts,
    partition_monty_source_items,
    summaries_for_monty_static_collection,
)
from pystac_monty.extension import SCHEMA_URI, MontyExtension
from pystac_monty.sources.batch_export import BATCH_EXPORTS, run_batch

_PROVIDER = Provider(name="test", roles=[ProviderRole.PRODUCER])
_GEOM = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]
_BBOX = [0.0, 0.0, 1.0, 1.0]
_WHEN = datetime(2021, 1, 1, tzinfo=timezone.utc)


def _source_item(item_id: str, role: str) -> Item:
    item = Item(
        id=item_id,
        geometry=_GEOM,
        bbox=_BBOX,
        datetime=_WHEN,
        properties={
            "roles": ["source", role],
            "monty:country_codes": ["ESP"],
            "monty:hazard_codes": ["HM-FLOOD"],
            "keywords": [role],
        },
    )
    MontyExtension.add_to(item)
    return item


def test_is_source_response() -> None:
    item = _source_item("response-1", "response")
    assert MontyExtension.ext(item).is_source_response()


def test_partition_monty_source_items() -> None:
    items = [
        _source_item("event-1", "event"),
        _source_item("hazard-1", "hazard"),
        _source_item("impact-1", "impact"),
        _source_item("response-1", "response"),
    ]
    events, hazards, impacts, responses = partition_monty_source_items(items)
    assert [item.id for item in events] == ["event-1"]
    assert [item.id for item in hazards] == ["hazard-1"]
    assert [item.id for item in impacts] == ["impact-1"]
    assert [item.id for item in responses] == ["response-1"]


def test_extent_for_empty_collection_uses_world_extent() -> None:
    extent = extent_for_monty_static_collection([])
    assert extent.spatial.bboxes == [[-180.0, -90.0, 180.0, 90.0]]


def test_summaries_for_monty_static_collection_sorts_arrays() -> None:
    item = _source_item("event-1", "event")
    item.properties["monty:country_codes"] = ["DEU", "ESP"]
    summaries = summaries_for_monty_static_collection([item], "event")
    assert summaries.get_list("roles") == ["event", "source"]
    assert summaries.get_list("monty:country_codes") == ["DEU", "ESP"]


def test_build_empty_static_source_collection_omits_monty_extension() -> None:
    collection = build_empty_static_source_collection(
        collection_id="demo-response",
        title="Demo response",
        description="Empty response collection",
        role="response",
        provider=_PROVIDER,
    )
    assert collection.stac_extensions == []
    assert collection.extra_fields["roles"] == ["response", "source"]


def test_export_monty_collection_writes_collection_and_items(tmp_path: Path) -> None:
    items = [_source_item("event-1", "event"), _source_item("event-2", "event")]
    export_monty_collection(
        MontyCollectionSpec(
            out_dir=tmp_path / "demo-events",
            collection_id="demo-events",
            title="Demo events",
            description="Demo source events",
            role="event",
            items=items,
            provider=_PROVIDER,
        )
    )
    collection_path = tmp_path / "demo-events" / "demo-events.json"
    assert collection_path.is_file()
    collection_doc = json.loads(collection_path.read_text(encoding="utf-8"))
    assert collection_doc["id"] == "demo-events"
    assert collection_doc["stac_extensions"] == [SCHEMA_URI]
    assert collection_doc["summaries"]["roles"] == ["event", "source"]
    assert {link["rel"] for link in collection_doc["links"]} >= {"self", "item"}

    for item in items:
        item_path = tmp_path / "demo-events" / f"{item.id}.json"
        item_doc = json.loads(item_path.read_text(encoding="utf-8"))
        assert item_doc["id"] == item.id
        assert item_doc["collection"] == "demo-events"
        assert not any(link["rel"] in {"parent", "root", "collection"} for link in item_doc.get("links", []))


def test_export_collected_items_partitions_by_role(tmp_path: Path) -> None:
    items = [
        _source_item("event-1", "event"),
        _source_item("hazard-1", "hazard"),
        _source_item("impact-1", "impact"),
    ]
    config = BatchExportConfig(source_slug="demo", provider=_PROVIDER)
    counts = export_collected_items(config, items, tmp_path)
    assert counts == (1, 1, 1, 0)
    assert (tmp_path / "demo-events" / "demo-events.json").is_file()
    assert (tmp_path / "demo-hazards" / "demo-hazards.json").is_file()
    assert (tmp_path / "demo-impacts" / "demo-impacts.json").is_file()
    assert not (tmp_path / "demo-response").exists()


def test_export_collected_items_emits_empty_response_collection(tmp_path: Path) -> None:
    items = [_source_item("event-1", "event")]
    config = BatchExportConfig(
        source_slug="demo",
        provider=_PROVIDER,
        emit_empty_response_collection=True,
    )
    counts = export_collected_items(config, items, tmp_path)
    assert counts == (1, 0, 0, 0)
    response_collection = tmp_path / "demo-response" / "demo-response.json"
    assert response_collection.is_file()
    response_doc = json.loads(response_collection.read_text(encoding="utf-8"))
    assert response_doc["stac_extensions"] == []
    assert response_doc["summaries"]["roles"] == ["response", "source"]


def test_extent_for_items_without_bbox_uses_world_spatial_extent() -> None:
    item = _source_item("event-1", "event")
    item.bbox = None
    extent = extent_for_monty_static_collection([item])
    assert extent.spatial.bboxes == [[-180.0, -90.0, 180.0, 90.0]]


def test_extent_for_non_finite_bbox_uses_world_extent() -> None:
    item = _source_item("event-1", "event")
    item.bbox = [float("nan"), 0.0, 1.0, 1.0]
    extent = extent_for_monty_static_collection([item])
    assert extent.spatial.bboxes == [[-180.0, -90.0, 180.0, 90.0]]


def test_summaries_datetime_from_item_properties() -> None:
    item = _source_item("event-1", "event")
    item.datetime = None
    item.properties["start_datetime"] = "2021-01-01T00:00:00Z"
    item.properties["end_datetime"] = "2021-01-02T00:00:00Z"
    item.properties["datetime"] = "2021-01-01T12:00:00Z"
    summaries = summaries_for_monty_static_collection([item], "event")
    datetime_summary = summaries.get_range("datetime")
    assert datetime_summary is not None
    assert datetime_summary.minimum == "2021-01-01T12:00:00Z"


def test_partition_ignores_items_without_source_role() -> None:
    item = Item(
        id="reference-1",
        geometry=_GEOM,
        bbox=_BBOX,
        datetime=_WHEN,
        properties={"roles": ["reference", "event"]},
    )
    MontyExtension.add_to(item)
    assert partition_monty_source_items([item]) == ([], [], [], [])


def test_export_published_example_layout(tmp_path: Path) -> None:
    """Published examples: absolute self/collection hrefs, keywords outside summaries."""
    item = _source_item("event-1", "event")
    item.add_link(pystac.Link(rel="related", target="../demo-hazards/hazard-1.json"))
    collection_id = "demo-events"
    export_monty_collection(
        MontyCollectionSpec(
            out_dir=tmp_path / collection_id,
            collection_id=collection_id,
            title="Demo events",
            description="Demo source events",
            role="event",
            items=[item],
            provider=_PROVIDER,
            omit_keywords_from_summaries=True,
            public_href_base=MONTY_STAC_EXAMPLES_BASE_URL,
        )
    )
    base = MONTY_STAC_EXAMPLES_BASE_URL
    collection_doc = json.loads((tmp_path / collection_id / f"{collection_id}.json").read_text(encoding="utf-8"))
    item_doc = json.loads((tmp_path / collection_id / "event-1.json").read_text(encoding="utf-8"))

    assert collection_doc["keywords"] == ["event"]
    assert "keywords" not in collection_doc["summaries"]
    assert collection_doc["links"] == [
        {"rel": "item", "href": "./event-1.json", "type": "application/json"},
        {"rel": "self", "href": f"{base}/{collection_id}/{collection_id}.json", "type": "application/json"},
    ]

    item_links = {link["rel"]: link["href"] for link in item_doc["links"]}
    assert item_links["related"] == "../demo-hazards/hazard-1.json"
    assert item_links["self"] == f"{base}/{collection_id}/event-1.json"
    assert item_links["collection"] == f"{base}/{collection_id}/{collection_id}.json"


def test_export_without_preserve_transformer_links(tmp_path: Path) -> None:
    item = _source_item("event-1", "event")
    item.add_link(pystac.Link(rel="related", target="./event-2.json"))
    export_monty_collection(
        MontyCollectionSpec(
            out_dir=tmp_path / "demo-events",
            collection_id="demo-events",
            title="Demo events",
            description="Demo source events",
            role="event",
            items=[item],
            provider=_PROVIDER,
            preserve_transformer_item_links=False,
        )
    )
    item_doc = json.loads((tmp_path / "demo-events" / "event-1.json").read_text(encoding="utf-8"))
    assert [link["rel"] for link in item_doc.get("links", [])] == ["self"]


def test_export_collected_items_writes_response_items(tmp_path: Path) -> None:
    items = [_source_item("response-1", "response")]
    counts = export_collected_items(BatchExportConfig(source_slug="demo", provider=_PROVIDER), items, tmp_path)
    assert counts == (0, 0, 0, 1)
    assert (tmp_path / "demo-response" / "demo-response.json").is_file()


def test_log_batch_role_counts(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("INFO")
    log_batch_role_counts(1, 2, 3, 4)
    assert "Created 1 events, 2 hazards, 3 impacts, 4 response" in caplog.text


def test_run_batch(tmp_path: Path) -> None:
    seen: dict[str, Path] = {}

    def _fake_export(input_path: Path, output_dir: Path) -> None:
        seen["input"] = input_path
        seen["output"] = output_dir

    BATCH_EXPORTS["demo"] = _fake_export
    try:
        run_batch("demo", tmp_path / "in", tmp_path / "out")
    finally:
        BATCH_EXPORTS.pop("demo", None)
    assert seen["input"] == tmp_path / "in"
    assert seen["output"] == tmp_path / "out"
