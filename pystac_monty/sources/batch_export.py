"""Run existing Monty transformers and write static STAC via :mod:`pystac_monty.exporter`."""

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

_GDACS_EVENT = "geteventdata"
_GDACS_GEOMETRY = "getgeometry"
_GDACS_IMPACT = "getimpact"


def file_generic_data_source(input_path: Path, source_url: str | None = None) -> GenericDataSource:
    path = input_path.resolve()
    return GenericDataSource(
        source_url=source_url or path.as_uri(),
        input_data=File(path=str(path), data_type=DataType.FILE),
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
    emit_empty_response_subcatalog: bool = False,
) -> tuple[int, int, int, int]:
    config = BatchExportConfig(
        source_slug=source_slug,
        provider=provider,
        emit_empty_response_subcatalog=emit_empty_response_subcatalog,
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


def _gdacs_episode(path: Path, episode_type: str, hazard_type: str | None) -> GdacsEpisodes:
    return GdacsEpisodes(
        type=episode_type,
        data=file_generic_data_source(path),
        hazard_type=hazard_type,
    )


def _gdacs_data_source_from_manifest(manifest_path: Path) -> Any:
    from pystac_monty.sources.gdacs import GDACSDataSource

    base = manifest_path.parent
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    hazard_type = manifest.get("hazard_type")
    source_url = manifest.get("source_url", manifest_path.as_uri())
    event_path = base / manifest["event_data"]
    episodes: list[tuple[GdacsEpisodes, GdacsEpisodes, GdacsEpisodes | None]] = []
    for episode in manifest.get("episodes", []):
        event = _gdacs_episode(base / episode["event"], _GDACS_EVENT, hazard_type)
        geometry = _gdacs_episode(base / episode["geometry"], _GDACS_GEOMETRY, hazard_type)
        impact_path = episode.get("impact")
        impact = _gdacs_episode(base / impact_path, _GDACS_IMPACT, hazard_type) if impact_path else None
        episodes.append((event, geometry, impact))
    return GDACSDataSource(
        data=GdacsDataSourceType(
            source_url=source_url,
            event_data=File(path=str(event_path.resolve()), data_type=DataType.FILE),
            episodes=episodes,
        )
    )


def iter_gdacs_stac_items(input_path: Path) -> list[Item]:
    from pystac_monty.sources.gdacs import GDACSTransformer

    data_source = _gdacs_data_source_from_manifest(input_path)
    return collect_transformer_items(GDACSTransformer(data_source, default_batch_geocoder()))


def convert_gdacs(input_path: Path, output_dir: Path) -> None:
    """Export GDACS data. *input_path*: manifest JSON with relative ``event_data`` and ``episodes`` paths."""
    export_transformer_items(
        items=iter_gdacs_stac_items(input_path),
        source_slug="gdacs",
        provider=Provider(name="GDACS", roles=[ProviderRole.PRODUCER], url="https://www.gdacs.org/"),
        output_dir=output_dir,
    )


def convert_charter(input_path: Path, output_dir: Path) -> None:
    """Export Charter activations. *input_path*: directory of ``act-*-activation.json`` model fixtures."""
    from pystac_monty.sources.charter import convert_charter_activations

    convert_charter_activations(input_path, output_dir)


BatchExportFn = Callable[[Path, Path], None]

BATCH_EXPORTS: dict[str, BatchExportFn] = {
    "glide": convert_glide,
    "gfd": convert_gfd,
    "gdacs": convert_gdacs,
    "charter": convert_charter,
}


def run_batch(name: str, input_path: Path, output_dir: Path) -> None:
    BATCH_EXPORTS[name](input_path, output_dir)
