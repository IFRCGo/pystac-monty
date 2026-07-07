"""Static Monty STAC on disk and batch export helpers.

Write :class:`pystac.Collection` JSON and sidecar :class:`pystac.Item` files from items
returned by :meth:`~pystac_monty.sources.common.MontyDataTransformer.get_stac_items`.
Per-source ingestion (globs, CSV, APIs) lives in ``sources/<name>.py``.

Item bodies match PySTAC ``save_object`` / :meth:`pystac.Item.to_dict`. Catalog hierarchy
links are stripped from items; ``collection_id`` is kept.

CLI batch sources register in :mod:`pystac_monty.sources.batch_export` as ``convert_<name>``
functions mapped by :data:`~pystac_monty.sources.batch_export.BATCH_EXPORTS`.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional, Sequence, cast

import pystac
from pystac import Item
from pystac.provider import Provider
from pystac.summaries import RangeSummary, Summaries, Summarizer, SummaryStrategy
from pystac.utils import datetime_to_str

from pystac_monty.extension import SCHEMA_URI, ItemMontyExtension, MontyExtension

logger = logging.getLogger(__name__)

MONTY_STAC_EXAMPLES_BASE_URL = "https://ifrcgo.org/monty-stac-extension/examples"
MONTY_SOURCE_DATETIME_PROPERTY = "__monty_source_datetime"

MONTY_STATIC_SUMMARY_FIELDS: dict[str, SummaryStrategy] = {
    "monty:country_codes": SummaryStrategy.ARRAY,
    "monty:hazard_codes": SummaryStrategy.ARRAY,
    "keywords": SummaryStrategy.ARRAY,
}
_MONTY_SUMMARY_SORT_KEYS: tuple[str, ...] = ("monty:country_codes", "monty:hazard_codes", "keywords")
_SPATIAL_WORLD = pystac.SpatialExtent([[-180.0, -90.0, 180.0, 90.0]])
_WORLD_EXTENT = pystac.Extent(
    spatial=_SPATIAL_WORLD,
    temporal=pystac.TemporalExtent(cast(list[list[Optional[datetime]]], [[None, None]])),
)

MontyRole = Literal["event", "hazard", "impact", "response"]
_DEFAULT_ROLE_TITLES: dict[MontyRole, tuple[str, str]] = {
    "event": ("{slug} source events", "Monty source event items"),
    "hazard": ("{slug} source hazards", "Monty source hazard items"),
    "impact": ("{slug} source impacts", "Monty source impact items"),
    "response": ("{slug} source response", "Monty source response items"),
}


def extent_for_monty_static_collection(items: list[Item]) -> pystac.Extent:
    if not items:
        return _WORLD_EXTENT
    if not any(item.bbox is not None for item in items):
        return pystac.Extent(spatial=_SPATIAL_WORLD, temporal=pystac.Extent.from_items(items).temporal)
    extent = pystac.Extent.from_items(items)
    bbox = extent.spatial.bboxes[0]
    if not all(math.isfinite(float(x)) for x in bbox):
        return _WORLD_EXTENT
    return extent


def summaries_for_monty_static_collection(
    items: list[Item],
    role: MontyRole,
    *,
    include_keywords: bool = True,
) -> Summaries:
    fields = (
        MONTY_STATIC_SUMMARY_FIELDS
        if include_keywords
        else {k: v for k, v in MONTY_STATIC_SUMMARY_FIELDS.items() if k != "keywords"}
    )
    summaries = Summarizer(fields).summarize(items)
    summaries.add("roles", [role, "source"])
    for key in _MONTY_SUMMARY_SORT_KEYS:
        if values := summaries.get_list(key):
            summaries.remove(key)
            summaries.add(key, sorted(values))
    datetimes: list[str] = []
    for item in items:
        source_datetime = item.properties.get(MONTY_SOURCE_DATETIME_PROPERTY)
        if isinstance(source_datetime, str):
            datetimes.append(source_datetime)
        elif item.datetime is not None:
            datetimes.append(datetime_to_str(item.datetime))
        elif item.properties.get("datetime") is not None:
            value = item.properties["datetime"]
            datetimes.append(value if isinstance(value, str) else datetime_to_str(value))
    if datetimes:
        summaries.add("datetime", RangeSummary(min(datetimes), max(datetimes)))
    return summaries


def partition_monty_source_items(
    items: Sequence[Item],
) -> tuple[list[Item], list[Item], list[Item], list[Item]]:
    """Event / hazard / impact / response lists (Monty *source* roles)."""
    events: list[Item] = []
    hazards: list[Item] = []
    impacts: list[Item] = []
    responses: list[Item] = []
    for item in items:
        monty = cast(ItemMontyExtension, MontyExtension.ext(item))
        if monty.is_source_event():
            events.append(item)
        elif monty.is_source_hazard():
            hazards.append(item)
        elif monty.is_source_response():
            responses.append(item)
        elif monty.is_source_impact():
            impacts.append(item)
    return events, hazards, impacts, responses


def _strip_stac_hierarchy_for_static_item(item: Item) -> None:
    """Drop catalog layout links only; keep :attr:`~pystac.Item.collection_id`."""
    for rel in (
        pystac.RelType.SELF,
        pystac.RelType.PARENT,
        pystac.RelType.ROOT,
        pystac.RelType.COLLECTION,
    ):
        item.remove_links(rel)


def build_monty_static_collection(
    items: list[Item],
    *,
    collection_id: str,
    title: str,
    description: str,
    role: MontyRole,
    provider: Provider,
    omit_keywords_from_summaries: bool = False,
) -> pystac.Collection:
    keywords = sorted({kw for item in items for kw in (item.properties.get("keywords") or [])})
    summaries = summaries_for_monty_static_collection(items, role, include_keywords=not omit_keywords_from_summaries)
    return pystac.Collection(
        id=collection_id,
        title=title,
        description=description,
        license="proprietary",
        extent=extent_for_monty_static_collection(items),
        stac_extensions=[SCHEMA_URI],
        providers=[provider],
        summaries=summaries,
        extra_fields={"roles": [role, "source"], "keywords": keywords},
        catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED,
    )


def build_empty_static_source_collection(
    *,
    collection_id: str,
    title: str,
    description: str,
    role: MontyRole,
    provider: Provider,
) -> pystac.Collection:
    """Empty collection without the Monty extension (no items → required summary fields absent)."""
    summaries = Summaries(summaries={"roles": [role, "source"]})
    return pystac.Collection(
        id=collection_id,
        title=title,
        description=description,
        license="proprietary",
        extent=_WORLD_EXTENT,
        stac_extensions=[],
        providers=[provider],
        summaries=summaries,
        extra_fields={"roles": [role, "source"], "keywords": []},
        catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED,
    )


def _published_example_href(base: str, collection_id: str, name: str) -> str:
    return f"{base.rstrip('/')}/{collection_id}/{name}"


def _json_link(rel: str, href: str) -> dict[str, str]:
    return {"rel": rel, "href": href, "type": "application/json"}


def save_static_monty_collection(
    collection: pystac.Collection,
    items: Sequence[Item],
    dest_dir: Path,
    *,
    preserve_transformer_item_links: bool = True,
    public_href_base: str | None = None,
) -> None:
    """Write collection + items; each item JSON matches :meth:`pystac.Item.to_dict`."""
    out_dir = dest_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    collection_file = out_dir / f"{collection.id}.json"
    ordered = sorted(items, key=lambda item: item.id)
    stac_io = pystac.StacIO.default()

    for item in ordered:
        if preserve_transformer_item_links:
            _strip_stac_hierarchy_for_static_item(item)
        else:
            item.clear_links()
        item.collection_id = collection.id
        item_doc = item.to_dict(include_self_link=False, transform_hrefs=False)
        if isinstance(source_datetime := item_doc.get("properties", {}).pop(MONTY_SOURCE_DATETIME_PROPERTY, None), str):
            item_doc["properties"]["datetime"] = source_datetime
        transformer_links = item_doc.get("links", [])
        if public_href_base:
            self_href = _published_example_href(public_href_base, collection.id, f"{item.id}.json")
            coll_href = _published_example_href(public_href_base, collection.id, f"{collection.id}.json")
            item_doc["links"] = [
                *transformer_links,
                _json_link("self", self_href),
                _json_link("collection", coll_href),
            ]
        else:
            # Static on-disk layout: collection property only; no collection link (see test_exporter).
            item_doc["links"] = [
                *transformer_links,
                _json_link("self", f"./{item.id}.json"),
            ]
        stac_io.save_json(str(out_dir / f"{item.id}.json"), item_doc)

    collection_doc = collection.to_dict(include_self_link=False, transform_hrefs=False)
    collection_self = (
        _published_example_href(public_href_base, collection.id, f"{collection.id}.json")
        if public_href_base
        else f"./{collection.id}.json"
    )
    collection_doc["links"] = [_json_link("item", f"./{item.id}.json") for item in ordered] + [
        _json_link("self", collection_self)
    ]
    if not collection.stac_extensions and "stac_extensions" not in collection_doc:
        collection_doc["stac_extensions"] = []
    stac_io.save_json(str(collection_file), collection_doc)


@dataclass(frozen=True)
class MontyCollectionSpec:
    """Inputs for writing one Monty source :class:`pystac.Collection` and its items."""

    out_dir: Path
    collection_id: str
    title: str
    description: str
    role: MontyRole
    items: list[Item]
    provider: Provider
    preserve_transformer_item_links: bool = True
    omit_keywords_from_summaries: bool = False
    public_href_base: str | None = None


def export_monty_collection(spec: MontyCollectionSpec) -> None:
    if spec.items:
        collection = build_monty_static_collection(
            spec.items,
            collection_id=spec.collection_id,
            title=spec.title,
            description=spec.description,
            role=spec.role,
            provider=spec.provider,
            omit_keywords_from_summaries=spec.omit_keywords_from_summaries,
        )
    else:
        collection = build_empty_static_source_collection(
            collection_id=spec.collection_id,
            title=spec.title,
            description=spec.description,
            role=spec.role,
            provider=spec.provider,
        )
    save_static_monty_collection(
        collection,
        spec.items,
        spec.out_dir,
        preserve_transformer_item_links=spec.preserve_transformer_item_links,
        public_href_base=spec.public_href_base,
    )


def _role_titles(source_slug: str, overrides: dict[MontyRole, tuple[str, str]] | None) -> dict[MontyRole, tuple[str, str]]:
    defaults = {role: (title.format(slug=source_slug), desc) for role, (title, desc) in _DEFAULT_ROLE_TITLES.items()}
    return {**defaults, **(overrides or {})}


def _collection_spec(
    config: BatchExportConfig,
    output_root: Path,
    role: MontyRole,
    items: list[Item],
    collection_id: str,
    titles: dict[MontyRole, tuple[str, str]],
) -> MontyCollectionSpec:
    title, description = titles[role]
    return MontyCollectionSpec(
        out_dir=output_root / collection_id,
        collection_id=collection_id,
        title=title,
        description=description,
        role=role,
        items=items,
        provider=config.provider,
        preserve_transformer_item_links=config.preserve_transformer_item_links,
        omit_keywords_from_summaries=config.omit_keywords_from_summaries,
        public_href_base=config.public_href_base,
    )


def export_collected_items(
    config: BatchExportConfig,
    items: Sequence[Item],
    output_root: Path,
) -> tuple[int, int, int, int]:
    """Partition *items* by Monty role and write ``{source_slug}-events|hazards|impacts|response`` folders."""
    events, hazards, impacts, responses = partition_monty_source_items(items)
    titles = _role_titles(config.source_slug, config.titles)
    blocks: list[tuple[MontyRole, list[Item], str]] = [
        ("event", events, f"{config.source_slug}-events"),
        ("hazard", hazards, f"{config.source_slug}-hazards"),
        ("impact", impacts, f"{config.source_slug}-impacts"),
        ("response", responses, f"{config.source_slug}-response"),
    ]
    wrote_response = False
    for role, role_items, collection_id in blocks:
        if not role_items:
            continue
        if role == "response":
            wrote_response = True
        export_monty_collection(_collection_spec(config, output_root, role, list(role_items), collection_id, titles))
    if config.emit_empty_response_collection and not wrote_response:
        collection_id = f"{config.source_slug}-response"
        export_monty_collection(_collection_spec(config, output_root, "response", [], collection_id, titles))
    return len(events), len(hazards), len(impacts), len(responses)


@dataclass(frozen=True)
class BatchExportConfig:
    """Shared metadata for :func:`export_collected_items`."""

    source_slug: str
    provider: Provider
    titles: dict[MontyRole, tuple[str, str]] | None = None
    preserve_transformer_item_links: bool = True
    emit_empty_response_collection: bool = False
    omit_keywords_from_summaries: bool = False
    public_href_base: str | None = None


def log_batch_role_counts(events: int, hazards: int, impacts: int, responses: int = 0) -> None:
    logger.info(
        "Created %d events, %d hazards, %d impacts, %d response",
        events,
        hazards,
        impacts,
        responses,
    )
