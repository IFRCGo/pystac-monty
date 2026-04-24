"""Static Monty STAC on disk and CLI batch export registry.

**Writing STAC** — build :class:`pystac.Collection` files + sidecar :class:`pystac.Item` JSON from
items produced by :meth:`~pystac_monty.sources.common.MontyDataTransformer.get_stac_items` (see
:func:`export_monty_items_to_role_subcatalogs` for event / hazard / impact / optional response
subfolders). Per-source ingestion (globs, CSV) stays in ``sources/<name>.py``.

Items use the same serialization as PySTAC ``save_object`` / :meth:`pystac.Item.to_dict` (what
``montandon-etl`` queues). Collections register items with ``add_item(..., set_parent=False)`` so
item bodies are not polluted with catalog parent links; hierarchy links are stripped without
clearing ``collection_id``.

**CLI** — :data:`BATCH_EXPORTS` maps source names to ``convert_<name>`` callables. Register new
sources with a **lazy** wrapper (import inside the function) to avoid circular imports.
"""

from __future__ import annotations

import logging
import math
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Literal, Optional, Protocol, Sequence, cast

import pystac
from pystac import Item
from pystac.layout import CustomLayoutStrategy
from pystac.provider import Provider
from pystac.summaries import RangeSummary, Summaries, Summarizer, SummaryStrategy
from pystac.utils import datetime_to_str

from pystac_monty.extension import SCHEMA_URI, MontyExtension

logger = logging.getLogger(__name__)

# Public URL prefix for static examples published with monty-stac-extension (STAC 1.1.0 IRI link hrefs).
MONTY_STAC_EXAMPLES_BASE_URL = "https://ifrcgo.org/monty-stac-extension/examples"

MONTY_FLAT_STRATEGY = CustomLayoutStrategy(
    item_func=lambda item, pdir: os.path.join(pdir, f"{item.id}.json"),
)
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


def extent_for_monty_static_collection(items: list[Item]) -> pystac.Extent:
    if not items:
        return _WORLD_EXTENT
    if not any(i.bbox is not None for i in items):
        return pystac.Extent(spatial=_SPATIAL_WORLD, temporal=pystac.Extent.from_items(items).temporal)
    e = pystac.Extent.from_items(items)
    b = e.spatial.bboxes[0]
    if not all(math.isfinite(float(x)) for x in b):
        return _WORLD_EXTENT
    return e


def summaries_for_monty_static_collection(items: list[Item], role: str) -> Summaries:
    s = Summarizer(MONTY_STATIC_SUMMARY_FIELDS).summarize(items)
    s.add("roles", [role, "source"])
    for k in _MONTY_SUMMARY_SORT_KEYS:
        if lst := s.get_list(k):
            s.remove(k)
            s.add(k, sorted(lst))
    dts = [i.properties.get("datetime") for i in items if i.properties.get("datetime") is not None]
    dts = [d if isinstance(d, str) else datetime_to_str(d) for d in dts]
    if dts:
        s.add("datetime", RangeSummary(min(dts), max(dts)))
    return s


def partition_monty_source_items(
    items: Sequence[Item],
) -> tuple[list[Item], list[Item], list[Item], list[Item]]:
    """Event / hazard / impact / response lists (Monty *source* roles)."""
    ev, ha, im, rsp = [], [], [], []
    for it in items:
        m = MontyExtension.ext(it)
        if m.is_source_event():
            ev.append(it)
        elif m.is_source_hazard():
            ha.append(it)
        elif m.is_source_response():
            rsp.append(it)
        elif m.is_source_impact():
            im.append(it)
    return ev, ha, im, rsp


def _strip_stac_hierarchy_for_static_item(item: Item) -> None:
    """Drop catalog layout links only; keep :attr:`~pystac.Item.collection_id` (ETL ``to_dict`` includes ``collection``)."""
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
    role: str,
    provider: Provider,
) -> pystac.Collection:
    s = summaries_for_monty_static_collection(items, role)
    kws = sorted(list(s.get_list("keywords") or ()))
    return pystac.Collection(
        id=collection_id,
        title=title,
        description=description,
        license="proprietary",
        extent=extent_for_monty_static_collection(items),
        stac_extensions=[SCHEMA_URI],
        providers=[provider],
        summaries=s,
        extra_fields={"roles": [role, "source"], "keywords": kws},
        catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED,
    )


def build_empty_static_source_collection(
    *,
    collection_id: str,
    title: str,
    description: str,
    role: str,
    provider: Provider,
) -> pystac.Collection:
    """Collection JSON with no items and no Monty extension (e.g. empty *-response subcatalog for Charter).

    A Monty collection with the schema registered but no items fails validation: summaries would require
    non-empty ``monty:country_codes`` / ``monty:hazard_codes`` — so the extension is omitted.
    """
    s = Summaries(summaries={"roles": [role, "source"]})
    return pystac.Collection(
        id=collection_id,
        title=title,
        description=description,
        license="proprietary",
        extent=_WORLD_EXTENT,
        stac_extensions=[],
        providers=[provider],
        summaries=s,
        extra_fields={"roles": [role, "source"], "keywords": []},
        catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED,
    )


def _static_item_href(public_href_base: str | None, collection_id: str, item_id: str) -> str:
    if public_href_base:
        return f"{public_href_base.rstrip('/')}/{collection_id}/{item_id}.json"
    return f"./{item_id}.json"


def _static_collection_href(public_href_base: str | None, collection_id: str) -> str:
    if public_href_base:
        return f"{public_href_base.rstrip('/')}/{collection_id}/{collection_id}.json"
    return f"./{collection_id}.json"


# Spurious local STAC-API path segments produced when ``Link(target=item)`` used pre-export self hrefs.
_RE_COLLECTIONS_ITEMS = re.compile(r"/collections/([^/]+)/items/([^/?#]+)")


def _absolutize_example_stac_hrefs(doc: dict[str, Any], public_href_base: str, collection_id: str) -> None:
    """Rewrite STAC 1.1.0 example ``links`` to published ``https://ifrcgo.org/.../examples/...`` IRIs.

    PySTAC leaves *root* / *item* as ``./...``; cross-folder links can be ``../...``; some related links can
    resolve to ``.../collections/.../items/...`` before export normalizes *self* — normalize all here.
    """
    base = public_href_base.rstrip("/")
    coll = f"{base}/{collection_id}/{collection_id}.json"
    typ = doc.get("type")
    for link in doc.get("links", []):
        if not isinstance(link, dict):
            continue
        href = link.get("href")
        if not isinstance(href, str) or not href:
            continue
        rel = str(link.get("rel", ""))
        if href.startswith("https://ifrcgo.org/"):
            continue
        m_api = _RE_COLLECTIONS_ITEMS.search(href)
        if m_api:
            other_col, iid = m_api.group(1), m_api.group(2)
            if not iid.endswith(".json"):
                iid = f"{iid}.json"
            link["href"] = f"{base}/{other_col}/{iid.split('/')[-1]}"
            continue
        if href.startswith("https://") and "supervisor.disasterscharter" in href:
            continue
        if rel in ("root", "parent", "collection") or (rel == "self" and typ == "Collection"):
            link["href"] = coll
            continue
        if rel == "self" and typ == "Feature":
            iid = str(doc.get("id", "item"))
            if not iid.endswith(".json"):
                iid = f"{iid}.json"
            link["href"] = f"{base}/{collection_id}/{iid}"
            continue
        if rel == "item" or (rel in ("self",) and href.startswith("./")):
            name = href.removeprefix("./").lstrip("/").split("/")[-1]
            if name:
                link["href"] = f"{base}/{collection_id}/{name}"
            continue
        if rel in ("related", "derived_from"):
            if href.startswith("https://") and "ifrcgo.org" in href:
                continue
            if href.startswith("../"):
                segs = [p for p in href.split("/") if p and p != ".."]
                if len(segs) >= 2 and segs[0].startswith("charter-"):
                    link["href"] = f"{base}/{segs[0]}/{segs[-1]}"
            elif href.startswith("./"):
                name = href.removeprefix("./").lstrip("/").split("/")[-1]
                if name:
                    link["href"] = f"{base}/{collection_id}/{name}"
            continue


def save_static_monty_collection(
    col: pystac.Collection,
    items: Sequence[Item],
    dest_dir: Path,
    *,
    preserve_transformer_item_links: bool = True,
    public_href_base: str | None = None,
) -> None:
    """Write collection + items; each item JSON matches :meth:`pystac.Item.to_dict` / ``save_object``.

    When *public_href_base* is set (e.g. :data:`MONTY_STAC_EXAMPLES_BASE_URL`), item and collection link
    ``href`` values use that prefix so they validate as IRIs (STAC 1.1.0) when published with examples.
    """
    out = dest_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    collection_file = out / f"{col.id}.json"
    collection_id = col.id
    ordered = sorted(items, key=lambda i: i.id)
    stac_io = pystac.StacIO.default()
    col.set_root(col)
    for it in ordered:
        if preserve_transformer_item_links:
            _strip_stac_hierarchy_for_static_item(it)
        else:
            it.clear_links()
        it.set_self_href(_static_item_href(public_href_base, collection_id, it.id))
        # Register on collection for collection.json item links; do not attach catalog parent to item body.
        col.add_item(it, strategy=MONTY_FLAT_STRATEGY, set_parent=False)
    col.set_self_href(_static_collection_href(public_href_base, collection_id))
    for it in ordered:
        item_doc = it.to_dict(include_self_link=True, transform_hrefs=True)
        if public_href_base:
            _absolutize_example_stac_hrefs(item_doc, public_href_base, collection_id)
        stac_io.save_json(str(out / f"{it.id}.json"), item_doc)
    col_doc = col.to_dict(include_self_link=True, transform_hrefs=True)
    if not col.stac_extensions and "stac_extensions" not in col_doc:
        col_doc["stac_extensions"] = []
    if public_href_base:
        _absolutize_example_stac_hrefs(col_doc, public_href_base, collection_id)
    stac_io.save_json(str(collection_file), col_doc)


@dataclass(frozen=True)
class MontySubcatalog:
    out_dir: Path
    collection_id: str
    title: str
    description: str
    role: str
    items: list[Item]
    provider: Provider
    preserve_transformer_item_links: bool = field(default=True)
    public_href_base: str | None = None


def export_monty_subcatalog(spec: MontySubcatalog) -> None:
    if not spec.items:
        col = build_empty_static_source_collection(
            collection_id=spec.collection_id,
            title=spec.title,
            description=spec.description,
            role=spec.role,
            provider=spec.provider,
        )
    else:
        col = build_monty_static_collection(
            spec.items,
            collection_id=spec.collection_id,
            title=spec.title,
            description=spec.description,
            role=spec.role,
            provider=spec.provider,
        )
    save_static_monty_collection(
        col,
        spec.items,
        spec.out_dir,
        preserve_transformer_item_links=spec.preserve_transformer_item_links,
        public_href_base=spec.public_href_base,
    )


MontyRole = Literal["event", "hazard", "impact", "response"]


def export_monty_items_to_role_subcatalogs(
    source_name: str,
    items: Sequence[Item],
    output_root: Path,
    provider: Provider,
    *,
    titles: dict[MontyRole, tuple[str, str]] | None = None,
    preserve_transformer_item_links: bool = True,
    emit_empty_response_subcatalog: bool = False,
    public_href_base: str | None = None,
) -> tuple[int, int, int, int]:
    """Partition *items* and write folders ``{source_name}-events|hazards|impacts|response``.

    The ``{source_name}-response`` folder is written when there are response items, or when
    *emit_empty_response_subcatalog* is true (empty collection JSON only).
    """
    ev, ha, im, rsp = partition_monty_source_items(items)
    default_titles: dict[MontyRole, tuple[str, str]] = {
        "event": (f"{source_name} source events", "Monty source event items"),
        "hazard": (f"{source_name} source hazards", "Monty source hazard items"),
        "impact": (f"{source_name} source impacts", "Monty source impact items"),
        "response": (f"{source_name} source response", "Monty source response items"),
    }
    tmap = {**default_titles, **(titles or {})}
    blocks: list[tuple[MontyRole, list[Item], str]] = [
        ("event", ev, f"{source_name}-events"),
        ("hazard", ha, f"{source_name}-hazards"),
        ("impact", im, f"{source_name}-impacts"),
        ("response", rsp, f"{source_name}-response"),
    ]
    wrote_response = False
    for role, lst, cid in blocks:
        if not lst:
            continue
        if role == "response":
            wrote_response = True
        title, desc = tmap[role]
        export_monty_subcatalog(
            MontySubcatalog(
                out_dir=output_root / cid,
                collection_id=cid,
                title=title,
                description=desc,
                role=role,
                items=list(lst),
                provider=provider,
                preserve_transformer_item_links=preserve_transformer_item_links,
                public_href_base=public_href_base,
            )
        )
    if emit_empty_response_subcatalog and not wrote_response:
        cid = f"{source_name}-response"
        title, desc = tmap["response"]
        export_monty_subcatalog(
            MontySubcatalog(
                out_dir=output_root / cid,
                collection_id=cid,
                title=title,
                description=desc,
                role="response",
                items=[],
                provider=provider,
                preserve_transformer_item_links=preserve_transformer_item_links,
                public_href_base=public_href_base,
            )
        )
    return len(ev), len(ha), len(im), len(rsp)


# --- CLI batch registry (lazy shims for sources) ---


class MontySourceBatchExport(Protocol):
    """CLI entrypoint: read source-specific ``input_path``, write STAC under ``output_dir``."""

    def __call__(self, input_path: Path, output_dir: Path) -> None: ...


class MontyBatchItemSource(Protocol):
    """Map local ``--input`` to STAC items; pair with :func:`export_collected_items`."""

    def __call__(self, input_path: Path) -> Iterator[Item]: ...


@dataclass(frozen=True)
class BatchExportConfig:
    """Shared metadata for :func:`export_collected_items` (folder prefix ``{source_slug}-events`` etc.)."""

    source_slug: str
    provider: Provider
    titles: dict[MontyRole, tuple[str, str]] | None = None
    preserve_transformer_item_links: bool = True
    emit_empty_response_subcatalog: bool = False
    #: If set, written STAC link ``href`` values use this prefix (published examples / STAC 1.1.0 IRI).
    public_href_base: str | None = None


def export_collected_items(
    config: BatchExportConfig,
    items: Sequence[Item],
    output_root: Path,
) -> tuple[int, int, int, int]:
    """Partition *items* by Monty role and write static subcatalogs under *output_root*."""
    return export_monty_items_to_role_subcatalogs(
        config.source_slug,
        items,
        output_root,
        config.provider,
        titles=config.titles,
        preserve_transformer_item_links=config.preserve_transformer_item_links,
        emit_empty_response_subcatalog=config.emit_empty_response_subcatalog,
        public_href_base=config.public_href_base,
    )


def export_collected_items_and_log(
    config: BatchExportConfig,
    items: Sequence[Item],
    output_root: Path,
) -> None:
    """Like :func:`export_collected_items` followed by :func:`log_batch_role_counts`."""
    ne, nh, ni, nr = export_collected_items(config, items, output_root)
    log_batch_role_counts(ne, nh, ni, nr)


def log_batch_role_counts(ne: int, nh: int, ni: int, nr: int = 0) -> None:
    parts = [f"{ne} events", f"{nh} hazards"]
    if ni:
        parts.append(f"{ni} impacts")
    if nr:
        parts.append(f"{nr} response")
    logger.info("Created %s", ", ".join(parts))


def run_batch(name: str, input_path: Path, output_dir: Path) -> None:
    BATCH_EXPORTS[name](input_path, output_dir)


def _charter_export(input_path: Path, output_dir: Path) -> None:
    from pystac_monty.sources.charter import convert_charter_activations

    convert_charter_activations(input_path, output_dir)


BATCH_EXPORTS: dict[str, MontySourceBatchExport] = {
    "charter": _charter_export,
}
