"""Run existing Monty transformers and write static STAC via :mod:`pystac_monty.exporter`.

This is a thin developer harness for exercising a transformer over a local input file
and writing the resulting static STAC. Source extraction/ingestion is owned by
``montandon-etl``; keep this module simple and avoid re-implementing that logic here.
Most sources expect a single native payload file; GDACS expects a pre-built
:class:`~pystac_monty.sources.common.GdacsDataSourceType` bundle JSON (see
:func:`load_gdacs_data_source_from_bundle`).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pystac import Item
from pystac.provider import Provider, ProviderRole

from pystac_monty.exporter import BatchExportConfig, export_collected_items, log_batch_role_counts
from pystac_monty.geocoding import MockGeocoder, MontyGeoCoder
from pystac_monty.sources.common import (
    DataType,
    File,
    GdacsDataSourceType,
    GdacsEpisodes,
    GenericDataSource,
    MontyDataTransformer,
)


def file_generic_data_source(input_path: Path, source_url: str | None = None) -> GenericDataSource:
    path = input_path.resolve()
    return GenericDataSource(
        source_url=source_url or path.as_uri(),
        input_data=File(path=str(path), data_type=DataType.FILE),
    )


def _resolve_file_path(path: str, base_dir: Path) -> str:
    file_path = Path(path)
    if file_path.is_absolute():
        return str(file_path)
    return str((base_dir / file_path).resolve())


def _gdacs_file_input(path: str, base_dir: Path) -> File:
    return File(path=_resolve_file_path(path, base_dir), data_type=DataType.FILE)


def _gdacs_episode_from_dict(data: dict[str, Any], base_dir: Path) -> GdacsEpisodes:
    input_data = data["data"]["input_data"]
    return GdacsEpisodes(
        type=data["type"],
        hazard_type=data.get("hazard_type"),
        data=GenericDataSource(
            source_url=data["data"]["source_url"],
            input_data=_gdacs_file_input(input_data["path"], base_dir),
        ),
    )


def load_gdacs_data_source_from_bundle(bundle_path: Path) -> Any:
    """Load a :class:`~pystac_monty.sources.gdacs.GDACSDataSource` from a bundle JSON file.

    *bundle_path* must contain a serialized :class:`~pystac_monty.sources.common.GdacsDataSourceType`
    document. Relative ``path`` values are resolved against the bundle directory.
    Bundle assembly belongs in ``montandon-etl``; this loader only wires files into the transformer.
    """
    from pystac_monty.sources.gdacs import GDACSDataSource

    base_dir = bundle_path.parent
    raw = json.loads(bundle_path.read_text(encoding="utf-8"))
    episodes: list[tuple[GdacsEpisodes, GdacsEpisodes, GdacsEpisodes | None]] = []
    for episode_event, episode_geometry, episode_impact in raw["episodes"]:
        episodes.append(
            (
                _gdacs_episode_from_dict(episode_event, base_dir),
                _gdacs_episode_from_dict(episode_geometry, base_dir),
                _gdacs_episode_from_dict(episode_impact, base_dir) if episode_impact is not None else None,
            )
        )
    return GDACSDataSource(
        data=GdacsDataSourceType(
            source_url=raw["source_url"],
            event_data=_gdacs_file_input(raw["event_data"]["path"], base_dir),
            episodes=episodes,
        )
    )


def use_local_collection_examples() -> None:
    examples = Path(__file__).resolve().parents[2] / "monty-stac-extension" / "examples"
    if examples.is_dir():
        MontyDataTransformer.base_collection_url = str(examples)


def default_batch_geocoder() -> MontyGeoCoder:
    """Test-oriented geocoder; swap for a real :class:`~pystac_monty.geocoding.MontyGeoCoder` in production."""
    return MockGeocoder()


def collect_transformer_items(transformer: MontyDataTransformer[Any]) -> list[Item]:
    use_local_collection_examples()
    return list(transformer.get_stac_items())


def export_transformer_items(
    *,
    items: list[Item],
    source_slug: str,
    provider: Provider,
    output_dir: Path,
    emit_empty_response_collection: bool = False,
) -> tuple[int, int, int, int]:
    config = BatchExportConfig(
        source_slug=source_slug,
        provider=provider,
        emit_empty_response_collection=emit_empty_response_collection,
    )
    counts = export_collected_items(config, items, output_dir)
    log_batch_role_counts(*counts)
    return counts


def iter_glide_stac_items(input_path: Path) -> list[Item]:
    from pystac_monty.sources.glide import GlideDataSource, GlideTransformer

    data_source = GlideDataSource(data=file_generic_data_source(input_path))
    return collect_transformer_items(GlideTransformer(data_source, default_batch_geocoder()))


def convert_glide(input_path: Path, output_dir: Path) -> None:
    """Export GLIDE data. *input_path*: GLIDE ``jsonglideset`` API JSON file."""
    export_transformer_items(
        items=iter_glide_stac_items(input_path),
        source_slug="glide",
        provider=Provider(name="GLIDE", roles=[ProviderRole.PRODUCER], url="https://www.glidenumber.net/"),
        output_dir=output_dir,
    )


def iter_gfd_stac_items(input_path: Path) -> list[Item]:
    from pystac_monty.sources.gfd import GFDDataSource, GFDTransformer

    data_source = GFDDataSource(data=file_generic_data_source(input_path))
    return collect_transformer_items(GFDTransformer(data_source, default_batch_geocoder()))


def convert_gfd(input_path: Path, output_dir: Path) -> None:
    """Export Global Flood Database data. *input_path*: JSON array of GFD event records."""
    export_transformer_items(
        items=iter_gfd_stac_items(input_path),
        source_slug="gfd",
        provider=Provider(
            name="Global Flood Database", roles=[ProviderRole.PRODUCER], url="https://global-flood-database.cloudtostreet.info/"
        ),
        output_dir=output_dir,
    )


def iter_gdacs_stac_items(input_path: Path) -> list[Item]:
    from pystac_monty.sources.gdacs import GDACSTransformer

    data_source = load_gdacs_data_source_from_bundle(input_path)
    return collect_transformer_items(GDACSTransformer(data_source, default_batch_geocoder()))


def convert_gdacs(input_path: Path, output_dir: Path) -> None:
    """Export GDACS data. *input_path*: ``GdacsDataSourceType`` bundle JSON with relative file paths."""
    export_transformer_items(
        items=iter_gdacs_stac_items(input_path),
        source_slug="gdacs",
        provider=Provider(name="GDACS", roles=[ProviderRole.PRODUCER], url="https://www.gdacs.org/"),
        output_dir=output_dir,
    )


BatchExportFn = Callable[[Path, Path], None]

BATCH_EXPORTS: dict[str, BatchExportFn] = {
    "glide": convert_glide,
    "gfd": convert_gfd,
    "gdacs": convert_gdacs,
}


def run_batch(name: str, input_path: Path, output_dir: Path) -> None:
    BATCH_EXPORTS[name](input_path, output_dir)
