"""Copernicus EMS Rapid Mapping STAC source transformer.

Transforms CEMS RM activation detail JSON into Monty STAC items (event, hazard,
response, impact). Batch export uses :mod:`pystac_monty.exporter`; this module
provides :func:`convert_cems` and :func:`iter_cems_stac_items`.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import re
from dataclasses import replace
from pathlib import Path
from typing import Any, Generator, Iterable, Sequence

import pycountry
import requests  # type: ignore[import-untyped]
from pystac import Asset, Collection, Item, Link
from pystac.provider import Provider, ProviderRole
from pystac.utils import datetime_to_str
from shapely import wkt  # type: ignore[import-untyped]
from shapely.geometry import mapping  # type: ignore[import-untyped]

from pystac_monty.exporter import (
    MONTY_SOURCE_DATETIME_PROPERTY,
    MONTY_STAC_EXAMPLES_BASE_URL,
    BatchExportConfig,
    export_collected_items,
    log_batch_role_counts,
)
from pystac_monty.extension import (
    SCHEMA_URI,
    HazardDetail,
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
    MontyMethodology,
    MontyResponseStatus,
)
from pystac_monty.geocoding import MockGeocoder, MontyGeoCoder, WorldAdministrativeBoundariesGeocoder
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.response import build_response_item, link_monitoring_update
from pystac_monty.sources.common import (
    DataType,
    GenericDataSource,
    Memory,
    MontyDataSourceV3,
    MontyDataTransformer,
    sanitize_stac_item_id,
)
from pystac_monty.validators.cems import CEMSDetailEnvelope

logger = logging.getLogger(__name__)

CEMS_DASHBOARD_API = "https://rapidmapping.emergency.copernicus.eu/backend/dashboard-api"
CEMS_PORTAL_BASE = "https://rapidmapping.emergency.copernicus.eu"
CEMS_AWS_VIEWER = "https://rapidmapping-viewer.s3.eu-west-1.amazonaws.com"
GDACS_API_BASE = "https://www.gdacs.org/gdacsapi/api/events"
PROCESSING_SCHEMA_URI = "https://stac-extensions.github.io/processing/v1.2.0/schema.json"

# CEMS category (+ subCategory refinement) -> [UNDRR-2025, GLIDE, EM-DAT]
CEMS_HAZARD_CODES: dict[str, list[str]] = {
    "flood": ["MH0600", "FL", "nat-hyd-flo-flo"],
    "flood_riverine": ["MH0604", "FL", "nat-hyd-flo-flo"],
    "flood_flash": ["MH0603", "FF", "nat-hyd-flo-flo"],
    "flood_coastal": ["MH0605", "SS", "nat-hyd-flo-flo"],
    "wildfire": ["MH1301", "WF", "nat-cli-wil-for"],
    "storm": ["MH0400", "ST", "nat-met-sto"],
    "storm_tropical": ["MH0403", "TC", "nat-met-sto-tro"],
    "earthquake": ["GH0101", "EQ", "nat-geo-ear-gro"],
    "earthquake_tsunami": ["GH0301", "TS", "nat-geo-ear-tsu"],
    "mass_movement": ["MH0901", "LS", "nat-geo-mmd-lan"],
    "mass_movement_avalanche": ["MH1201", "AV", "nat-geo-mmd-ava"],
    "volcanic_activity": ["GH0201", "VO", "nat-geo-vol"],
    "industrial_accident": ["TH0300", "tec-ind-che"],
    "industrial_explosion": ["TH0600", "tec-ind-exp"],
    "transport_accident": ["tec-tra"],
    "humanitarian_crisis": ["CE"],
    "other": ["OT"],
}

MANUAL_REVIEW_CATEGORIES = {
    "transport accident",
    "humanitarian crisis",
    "environmental degradation",
    "other",
}

HAZARD_FOOTPRINT_CLASSES = {
    "flooded area",
    "flood trace",
    "landslide",
    "burnt area",
}

AGGREGATE_STAT_CLASSES = {
    "maximum of all extents",
    "maximum of all extents**",
    "na",
}

RESPONSE_TYPE_SENDAI: dict[str, list[str]] = {
    "eo-ref": ["G"],
    "eo-fep": ["D", "G"],
    "eo-del": ["D", "G"],
    "eo-gra": ["C", "D"],
    "eo-sr": ["G"],
}

PRODUCT_TYPE_TO_RESPONSE: dict[str, str] = {
    "REF": "eo-ref",
    "FEP": "eo-fep",
    "DEL": "eo-del",
    "GRA": "eo-gra",
    "GRM": "eo-gra",
}

PRODUCT_TYPE_SLUG: dict[str, str] = {
    "REF": "ref",
    "FEP": "fep",
    "DEL": "del",
    "GRA": "gra",
    "GRM": "gra",
}

PRODUCT_TYPE_TITLE: dict[str, str] = {
    "REF": "reference",
    "FEP": "first estimate",
    "DEL": "delineation",
    "GRA": "grading",
    "GRM": "grading",
}

FOOTPRINT_TO_HAZARD_KEY: dict[str, str] = {
    "flooded area": "flood",
    "flood trace": "flood",
    "landslide": "mass_movement",
    "burnt area": "wildfire",
}

CATEGORY_TO_HAZARD_KEY: dict[str, str] = {
    "flood": "flood",
    "wildfire": "wildfire",
    "storm": "storm",
    "earthquake": "earthquake",
    "mass movement": "mass_movement",
    "volcanic activity": "volcanic_activity",
    "industrial accident": "industrial_accident",
    "transport accident": "transport_accident",
    "humanitarian crisis": "humanitarian_crisis",
    "environmental degradation": "other",
    "other": "other",
}

SUBCATEGORY_TO_HAZARD_KEY: dict[str, str] = {
    "riverine flood": "flood_riverine",
    "flash flood": "flood_flash",
    "coastal flood": "flood_coastal",
    "storm surge": "flood_coastal",
    "tropical cyclone, hurricane, typhoon": "storm_tropical",
    "tropical cyclone": "storm_tropical",
    "hurricane": "storm_tropical",
    "typhoon": "storm_tropical",
    "tsunami": "earthquake_tsunami",
    "avalanche": "mass_movement_avalanche",
    "forest fire": "wildfire",
    "explosion": "industrial_explosion",
    "chemical": "industrial_accident",
    "oil spill": "industrial_accident",
}

HAZARD_KEY_TO_SLUG: dict[str, str] = {
    "flood": "flood",
    "flood_riverine": "flood",
    "flood_flash": "flood",
    "flood_coastal": "flood",
    "wildfire": "wildfire",
    "storm": "storm",
    "storm_tropical": "storm",
    "earthquake": "earthquake",
    "earthquake_tsunami": "earthquake",
    "mass_movement": "landslide",
    "mass_movement_avalanche": "landslide",
    "volcanic_activity": "volcano",
    "industrial_accident": "industrial",
    "industrial_explosion": "industrial",
    "transport_accident": "transport",
    "humanitarian_crisis": "crisis",
    "other": "other",
}

CEMS_COUNTRY_ALIASES: dict[str, str] = {
    "bolivia (plurinational state of)": "BOL",
    "cote d'ivoire": "CIV",
    "côte d'ivoire": "CIV",
    "democratic republic of the congo": "COD",
    "iran (islamic republic of)": "IRN",
    "korea, republic of": "KOR",
    "lao people's democratic republic": "LAO",
    "republic of korea": "KOR",
    "republic of the congo": "COG",
    "russian federation": "RUS",
    "syrian arab republic": "SYR",
    "tanzania, united republic of": "TZA",
    "united kingdom of great britain and northern ireland": "GBR",
    "united states of america": "USA",
    "venezuela (bolivarian republic of)": "VEN",
    "viet nam": "VNM",
}

_MONTY_WORLD_ADMIN_BOUNDARIES_FGB_ENV = "MONTY_WORLD_ADMIN_BOUNDARIES_FGB"

IMPACT_THEMATIC_SLUG: dict[str, str] = {
    "estimated population": "population",
    "built-up": "builtup",
    "transportation": "transportation",
    "land use": "landuse",
    "facilities": "facilities",
    "blocked road / interruption": "roads",
    "temporary camp": "camps",
}

_CEMS_PROVIDER = Provider(
    "Copernicus Emergency Management Service",
    roles=[ProviderRole.PRODUCER, ProviderRole.LICENSOR],
    url="https://mapping.emergency.copernicus.eu/",
)


def _normalize_key(value: str | None) -> str:
    return (value or "").strip().lower()


def _parse_datetime(value: Any) -> datetime.datetime:
    if isinstance(value, str):
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime.datetime):
        raise TypeError(f"Expected datetime or ISO datetime string, got {type(value).__name__}")
    if value.tzinfo is None:
        return value.replace(tzinfo=datetime.timezone.utc)
    return value


def _format_source_datetime(value: datetime.datetime | str) -> str:
    dt = _parse_datetime(value) if isinstance(value, str) else value
    return datetime_to_str(dt)


def _apply_delivery_datetime(item: Item, delivery_time: Any) -> None:
    """Normalize CEMS delivery timestamps to RFC 3339 UTC for STAC export."""
    if delivery_time is None:
        return
    dt = _parse_datetime(delivery_time)
    item.datetime = dt
    item.properties[MONTY_SOURCE_DATETIME_PROPERTY] = _format_source_datetime(dt)


CURATED_CEMS_EXAMPLE_IDS: tuple[str, ...] = (
    "cems-event-EMSR847",
    "cems-hazard-EMSR847-aoi01-storm",
    "cems-hazard-EMSR847-aoi01-landslide",
    "cems-response-EMSR847-aoi01-gra",
    "cems-impact-EMSR847-aoi01-gra-population",
)

_CEMS_CROSS_COLLECTION_PREFIXES = ("cems-events/", "cems-hazards/", "cems-response/", "cems-impacts/")
_EXTERNAL_EXAMPLE_PREFIXES = ("gdacs-events/", "charter-events/")


def _item_id_from_link_href(href: str) -> str | None:
    if not href:
        return None
    stem = Path(href).stem
    return stem if stem.startswith("cems-") else None


def _is_external_example_href(href: str) -> bool:
    return any(prefix in href for prefix in _EXTERNAL_EXAMPLE_PREFIXES)


def _is_cems_cross_collection_href(href: str) -> bool:
    return any(prefix in href for prefix in _CEMS_CROSS_COLLECTION_PREFIXES)


def _prune_curated_cems_links(item: Item, curated_ids: frozenset[str]) -> None:
    """Keep only curated CEMS sibling links plus intentional external published examples."""
    kept_links: list[Link] = []
    for link in item.links:
        href = link.get_href() or ""
        if link.rel in {"related", "derived_from", "prev"}:
            if _is_external_example_href(href):
                kept_links.append(link)
                continue
            item_id = _item_id_from_link_href(href)
            if item_id is None and link.rel == "derived_from":
                kept_links.append(link)
                continue
            if item_id and _is_cems_cross_collection_href(href):
                if item_id in curated_ids:
                    kept_links.append(link)
                continue
            if item_id and item_id in curated_ids:
                kept_links.append(link)
            continue
        kept_links.append(link)

    item.links = kept_links


def _prepare_curated_cems_items(items: Sequence[Item], curated_ids: frozenset[str]) -> list[Item]:
    selected = [item for item in items if item.id in curated_ids]
    missing = curated_ids.difference(item.id for item in selected)
    if missing:
        raise ValueError(f"Curated CEMS examples missing items: {sorted(missing)}")
    for item in selected:
        _prune_curated_cems_links(item, curated_ids)
    return selected


def _wkt_to_geometry(wkt_value: str | None) -> dict[str, Any] | None:
    if not wkt_value:
        return None
    try:
        geom = mapping(wkt.loads(wkt_value))
        return geom if geom.get("type") else None
    except Exception:
        logger.warning("Failed to parse WKT geometry", exc_info=True)
        return None


def _bbox_from_geometry(geom: dict[str, Any] | None) -> list[float] | None:
    if not geom:
        return None
    try:
        from shapely.geometry import shape as shapely_shape

        bounds = shapely_shape(geom).bounds
        return [float(bounds[0]), float(bounds[1]), float(bounds[2]), float(bounds[3])]
    except Exception:
        return None


def _normalize_iso3(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().upper()
    if normalized == "UNK":
        return None
    return normalized


def _iso3_from_country_name(name: str, geocoder: MontyGeoCoder) -> str | None:
    key = _normalize_key(name)
    if not key:
        return None

    alias = CEMS_COUNTRY_ALIASES.get(key)
    if alias:
        return alias

    try:
        return pycountry.countries.lookup(name).alpha_3
    except LookupError:
        pass

    for country in pycountry.countries:
        candidate_names = {country.name}
        official_name = getattr(country, "official_name", None)
        common_name = getattr(country, "common_name", None)
        if official_name:
            candidate_names.add(official_name)
        if common_name:
            candidate_names.add(common_name)
        if any(_normalize_key(candidate) == key for candidate in candidate_names):
            return country.alpha_3

    geom = geocoder.get_geometry_by_country_name(name, simplified=True)
    if geom and isinstance(geom.get("iso3"), str):
        return _normalize_iso3(geom["iso3"])

    return None


def _resolve_world_admin_boundaries_fgb_path() -> Path:
    env_path = os.environ.get(_MONTY_WORLD_ADMIN_BOUNDARIES_FGB_ENV)
    if env_path:
        path = Path(env_path)
        if path.is_file():
            return path
        raise FileNotFoundError(f"{_MONTY_WORLD_ADMIN_BOUNDARIES_FGB_ENV}={env_path!r} is not a readable FlatGeobuf file.")

    repo_test_fgb = Path(__file__).resolve().parents[2] / "tests" / "data-files" / "world-administrative-boundaries.fgb"
    if repo_test_fgb.is_file():
        return repo_test_fgb

    raise FileNotFoundError(
        f"World administrative boundaries FlatGeobuf not found. Set {_MONTY_WORLD_ADMIN_BOUNDARIES_FGB_ENV} to a valid .fgb path."
    )


def default_cems_export_geocoder() -> MontyGeoCoder:
    """Return a real geocoder for CEMS batch export and example regeneration."""
    return WorldAdministrativeBoundariesGeocoder(str(_resolve_world_admin_boundaries_fgb_path()), 0.1)


def _country_codes(countries: Iterable[dict[str, Any]], geocoder: MontyGeoCoder) -> list[str]:
    codes: list[str] = []
    for country in countries:
        name = country.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        iso3 = _iso3_from_country_name(name, geocoder)
        if not iso3:
            logger.warning("Unknown CEMS country name %r; using UNK", name)
            iso3 = "UNK"
        if iso3 not in codes:
            codes.append(iso3)
    return codes


def _primary_country_code(geometry: dict[str, Any] | None, country_codes: list[str], geocoder: MontyGeoCoder) -> list[str]:
    if not country_codes:
        return []
    primary = _normalize_iso3(geocoder.get_iso3_from_geometry(geometry)) if geometry else None
    if primary and primary in country_codes:
        return [primary, *[code for code in country_codes if code != primary]]
    if primary and primary not in country_codes:
        return [primary, *country_codes]
    return country_codes


def _hazard_keys_for_activation(category: str | None, sub_category: str | None) -> list[str]:
    cat_key = _normalize_key(category)
    if cat_key in MANUAL_REVIEW_CATEGORIES:
        logger.warning("CEMS category %r requires manual review", category)
        return []
    base_key = CATEGORY_TO_HAZARD_KEY.get(cat_key)
    if not base_key:
        logger.warning("Unmapped CEMS category %r", category)
        return []
    sub_key = SUBCATEGORY_TO_HAZARD_KEY.get(_normalize_key(sub_category))
    return [sub_key or base_key]


def _hazard_codes_for_keys(keys: list[str], hazard_profiles: MontyHazardProfiles) -> list[str]:
    merged: list[str] = []
    for key in keys:
        raw_codes = [code for code in CEMS_HAZARD_CODES.get(key, []) if code]
        if not raw_codes:
            continue
        stub = Item(
            id="stub",
            geometry={"type": "Point", "coordinates": [0.0, 0.0]},
            bbox=[0.0, 0.0, 0.0, 0.0],
            datetime=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
            properties={"monty:hazard_codes": raw_codes},
        )
        MontyExtension.add_to(stub)
        merged.extend(hazard_profiles.get_canonical_hazard_codes(stub))
    return list(dict.fromkeys(merged))


def _hazard_slug_for_key(key: str) -> str:
    return HAZARD_KEY_TO_SLUG.get(key, sanitize_stac_item_id(key))


def _map_product_response_type(product_type: str | None) -> str | None:
    if not product_type:
        return None
    return PRODUCT_TYPE_TO_RESPONSE.get(product_type.upper())


def _map_product_status(version: dict[str, Any] | None) -> MontyResponseStatus | None:
    if not version:
        return None
    status_code = version.get("statusCode")
    reason = str(version.get("reason") or "").lower()
    if status_code == "F":
        return MontyResponseStatus.PUBLISHED
    if status_code == "I":
        return MontyResponseStatus.IN_PRODUCTION
    if status_code == "W":
        return MontyResponseStatus.PLANNED
    if status_code == "N":
        if "no change" in reason:
            return MontyResponseStatus.NO_IMPACT
        return MontyResponseStatus.WITHDRAWN
    return None


def _aoi_number_slug(number: int | None) -> str:
    return f"aoi{int(number or 0):02d}"


def _product_id_suffix(product: dict[str, Any]) -> str:
    product_type = str(product.get("type", "")).upper()
    slug = PRODUCT_TYPE_SLUG.get(product_type, sanitize_stac_item_id(product_type.lower()))
    monitoring_number = int(product.get("monitoringNumber") or 0)
    if product.get("monitoring") and monitoring_number > 0:
        return f"{slug}-m{monitoring_number}"
    return slug


def _parse_gdacs_id(gdacs_id: str | None) -> tuple[str, str] | None:
    if not gdacs_id:
        return None
    match = re.fullmatch(r"([A-Za-z]{2})(\d+)", gdacs_id.strip())
    if not match:
        return None
    return match.group(1).upper(), match.group(2)


def resolve_gdacs_current_episode(gdacs_id: str | None, *, timeout: int = 30) -> int | None:
    """Resolve the current GDACS episode id for a ``gdacsId`` like ``TC1001230``."""
    parsed = _parse_gdacs_id(gdacs_id)
    if not parsed:
        return None
    event_type, event_id = parsed
    url = f"{GDACS_API_BASE}/geteventdata"
    try:
        response = requests.get(url, params={"eventtype": event_type, "eventid": event_id}, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        logger.warning("GDACS episode lookup failed for %s", gdacs_id, exc_info=True)
        return 1
    properties = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
    direct_episode_id = (
        properties.get("episodeid") or properties.get("episodeId") or payload.get("episodeid") or payload.get("episodeId")
    )
    if direct_episode_id is not None:
        return int(direct_episode_id)

    episodes = payload.get("episodes") or properties.get("episodes") or []
    if isinstance(episodes, list):
        for episode in episodes:
            if isinstance(episode, dict) and episode.get("iscurrent"):
                episode_id = episode.get("episodeid") or episode.get("episodeId")
                if episode_id is not None:
                    return int(episode_id)
        if episodes:
            last = episodes[-1]
            if isinstance(last, dict):
                episode_id = last.get("episodeid") or last.get("episodeId")
                if episode_id is not None:
                    return int(episode_id)
    return 1


def _impact_value_and_unit(stat_entry: dict[str, Any]) -> tuple[float | None, str | None]:
    value = stat_entry.get("affected")
    if value in (None, "", "NA"):
        value = stat_entry.get("total")
    if value in (None, "", "NA"):
        return None, stat_entry.get("unit")
    if isinstance(value, (int, float)):
        return float(value), stat_entry.get("unit")
    if isinstance(value, str):
        try:
            return float(value), stat_entry.get("unit")
        except ValueError:
            return None, stat_entry.get("unit")
    return None, stat_entry.get("unit")


def _impact_detail_from_value(thematic_class: str, value: float, unit: str | None) -> ImpactDetail:
    key = _normalize_key(thematic_class)
    if key.startswith("estimated population") or key == "population":
        return ImpactDetail(
            MontyImpactExposureCategory.ALL_PEOPLE,
            MontyImpactType.TOTAL_AFFECTED,
            value,
            unit or "people",
            MontyEstimateType.PRIMARY,
        )
    if key.startswith("built-up"):
        return ImpactDetail(
            MontyImpactExposureCategory.BUILDINGS,
            MontyImpactType.TOTAL_AFFECTED,
            value,
            unit or "count",
            MontyEstimateType.PRIMARY,
        )
    if key == "transportation":
        return ImpactDetail(
            MontyImpactExposureCategory.ROADS,
            MontyImpactType.DISRUPTED,
            value,
            unit or "km",
            MontyEstimateType.PRIMARY,
        )
    if key == "land use":
        return ImpactDetail(
            MontyImpactExposureCategory.CROPS,
            MontyImpactType.TOTAL_AFFECTED,
            value,
            unit or "ha",
            MontyEstimateType.PRIMARY,
        )
    if key == "facilities":
        return ImpactDetail(
            MontyImpactExposureCategory.BUILDINGS,
            MontyImpactType.TOTAL_AFFECTED,
            value,
            unit or "count",
            MontyEstimateType.PRIMARY,
        )
    return ImpactDetail(
        MontyImpactExposureCategory.TOTAL_AFFECTED,
        MontyImpactType.TOTAL_AFFECTED,
        value,
        unit,
        MontyEstimateType.PRIMARY,
    )


def _impact_detail_for_thematic(thematic_class: str, stat_entry: dict[str, Any]) -> ImpactDetail | None:
    value, unit = _impact_value_and_unit(stat_entry)
    if value is None:
        return None
    return _impact_detail_from_value(thematic_class, value, unit)


def _impact_detail_for_subclasses(thematic_class: str, subclasses: dict[str, Any]) -> ImpactDetail | None:
    total = 0.0
    unit: str | None = None
    found_value = False
    for stat_entry in subclasses.values():
        if not isinstance(stat_entry, dict):
            continue
        value, entry_unit = _impact_value_and_unit(stat_entry)
        if value is None:
            continue
        total += value
        unit = unit or entry_unit
        found_value = True
    if not found_value:
        return None
    return _impact_detail_from_value(thematic_class, total, unit)


def _is_exposure_thematic(thematic_class: str) -> bool:
    key = _normalize_key(thematic_class)
    if key in HAZARD_FOOTPRINT_CLASSES or key in AGGREGATE_STAT_CLASSES:
        return False
    return True


def _footprint_hazard_keys(stats: dict[str, Any] | None) -> list[str]:
    if not stats:
        return []
    keys: list[str] = []
    for thematic_class in stats:
        key = _normalize_key(thematic_class)
        if key in HAZARD_FOOTPRINT_CLASSES:
            hazard_key = FOOTPRINT_TO_HAZARD_KEY.get(key)
            if hazard_key and hazard_key not in keys:
                keys.append(hazard_key)
    return keys


def _hazard_detail_for_footprint(thematic_class: str, subclasses: dict[str, Any]) -> HazardDetail | None:
    total = 0.0
    unit: str | None = None
    found_value = False
    for stat_entry in subclasses.values():
        if not isinstance(stat_entry, dict):
            continue
        value, entry_unit = _impact_value_and_unit(stat_entry)
        if value is None:
            continue
        total += value
        unit = unit or entry_unit
        found_value = True
    if not found_value:
        return None
    return HazardDetail(
        severity_value=total,
        severity_unit=unit or "count",
        severity_label=str(thematic_class),
        estimate_type=MontyEstimateType.PRIMARY,
    )


def _footprint_hazard_details(stats: dict[str, Any] | None) -> dict[str, HazardDetail]:
    if not stats:
        return {}
    details: dict[str, HazardDetail] = {}
    for thematic_class, subclasses in stats.items():
        key = _normalize_key(str(thematic_class))
        if key not in HAZARD_FOOTPRINT_CLASSES or not isinstance(subclasses, dict):
            continue
        hazard_key = FOOTPRINT_TO_HAZARD_KEY.get(key)
        detail = _hazard_detail_for_footprint(str(thematic_class), subclasses)
        if hazard_key and detail is not None:
            details[hazard_key] = detail
    return details


def _hazard_details_for_aoi(
    activation: dict[str, Any],
    aoi: dict[str, Any],
    primary_hazard_key: str | None,
) -> dict[str, HazardDetail]:
    details: dict[str, HazardDetail] = {}
    max_extent = (activation.get("stats") or {}).get("max_extent")
    if primary_hazard_key and isinstance(max_extent, (int, float)):
        details[primary_hazard_key] = HazardDetail(
            severity_value=float(max_extent),
            severity_unit="km2",
            severity_label="Maximum extent",
        )
    for product in aoi.get("products") or []:
        if str(product.get("type", "")).upper() in {"GRA", "GRM"}:
            details.update(_footprint_hazard_details(product.get("stats")))
    return details


def _latest_del_product(products: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates = [
        product
        for product in products
        if str(product.get("type", "")).upper() == "DEL" and product.get("version", {}).get("statusCode") == "F"
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda product: int(product.get("monitoringNumber") or 0))


def _product_assets(product: dict[str, Any]) -> dict[str, Asset]:
    assets: dict[str, Asset] = {}
    download_path = product.get("downloadPath")
    if isinstance(download_path, str) and download_path.strip():
        assets["download"] = Asset(
            href=download_path,
            media_type="application/zip",
            roles=["data"],
            title="Product package (vector + raster)",
            extra_fields={"type": "application/zip"},
        )
    for idx, layer in enumerate(product.get("layers") or []):
        if not isinstance(layer, dict):
            continue
        layer_format = _normalize_key(layer.get("format"))
        layer_name = layer.get("name")
        if not isinstance(layer_name, str) or not layer_name.strip():
            continue
        if layer_format == "cog":
            href = f"{CEMS_AWS_VIEWER}/{layer_name}" if not layer_name.startswith("http") else layer_name
            assets.setdefault(
                "grading_cog" if str(product.get("type", "")).upper() in {"GRA", "GRM"} else f"layer_{idx}",
                Asset(
                    href=href,
                    media_type="image/tiff; application=geotiff; profile=cloud-optimized",
                    roles=["data", "visual"],
                    title="Grading product (COG)" if str(product.get("type", "")).upper() in {"GRA", "GRM"} else layer_name,
                    extra_fields={"type": "image/tiff; application=geotiff; profile=cloud-optimized"},
                ),
            )
    return assets


class CEMSDataSource(MontyDataSourceV3):
    """CEMS data source containing one activation detail payload."""

    activation_data: dict[str, Any]

    def __init__(self, data: GenericDataSource, eoapi_url: str | None = None):
        super().__init__(root=data, eoapi_url=eoapi_url)
        input_data = data.input_data
        if input_data.data_type == DataType.MEMORY:
            file_data = input_data.content
        elif input_data.data_type == DataType.FILE:
            with open(input_data.path, "r", encoding="utf-8") as handle:
                file_data = json.load(handle)
        else:
            file_data = {}
        CEMSDetailEnvelope.model_validate(file_data)
        if isinstance(file_data, dict) and file_data.get("results"):
            self.activation_data = file_data["results"][0]
        else:
            self.activation_data = file_data


class CEMSTransformer(MontyDataTransformer[CEMSDataSource]):
    """Transforms CEMS activation detail JSON into Monty STAC items."""

    hazard_profiles = MontyHazardProfiles()
    source_name = "cems"

    def __init__(self, data_source: CEMSDataSource, geocoder: MontyGeoCoder | None = None) -> None:
        super().__init__(data_source, geocoder or MockGeocoder())
        self._response_collection_cache: Collection | None = None
        self.response_collection_url = f"{MontyDataTransformer.base_collection_url}/cems-response/cems-response.json"
        self._gdacs_episode_cache: dict[str, int | None] = {}

    def get_response_collection(self) -> Collection:
        if self._response_collection_cache is None:
            url = self.response_collection_url
            if url.startswith("http"):
                collection_dict = json.loads(requests.get(url, timeout=60).text)
            else:
                with open(url, encoding="utf-8") as handle:
                    collection_dict = json.load(handle)
            collection = Collection.from_dict(collection_dict)
            collection.set_self_href(url)
            self._response_collection_cache = collection
        return self._response_collection_cache

    @staticmethod
    def _relative_item_href(collection_id: str, item_id: str) -> str:
        return f"../{collection_id}/{item_id}.json"

    def _canonical_codes_for_keys(self, hazard_keys: list[str]) -> list[str]:
        return _hazard_codes_for_keys(hazard_keys, self.hazard_profiles)

    def _activation_keywords(self, activation: dict[str, Any], hazard_codes: list[str]) -> list[str]:
        keywords = [str(activation.get("code") or "").strip(), str(activation.get("category") or "").strip()]
        if sub_category := activation.get("subCategory"):
            keywords.append(str(sub_category).strip())
        title = str(activation.get("name") or "")
        if " in " in title:
            keywords.append(title.split(" in ", 1)[0].strip())
        hazard_keywords = self.hazard_profiles.get_keywords(hazard_codes)
        return sorted({kw for kw in keywords + hazard_keywords if kw})

    def _gdacs_episode(self, gdacs_id: str | None) -> int | None:
        if not gdacs_id:
            return None
        if gdacs_id not in self._gdacs_episode_cache:
            self._gdacs_episode_cache[gdacs_id] = resolve_gdacs_current_episode(gdacs_id)
        return self._gdacs_episode_cache[gdacs_id]

    def make_event_item(self, activation: dict[str, Any]) -> Item | None:
        code = activation.get("code")
        if not code:
            logger.warning("Skipping CEMS activation without code")
            return None

        hazard_keys = _hazard_keys_for_activation(activation.get("category"), activation.get("subCategory"))
        if not hazard_keys:
            return None

        event_time = activation.get("eventTime")
        if not event_time:
            logger.warning("Skipping CEMS activation %s without eventTime", code)
            return None

        centroid_geom = _wkt_to_geometry(activation.get("centroid"))
        if not centroid_geom:
            logger.warning("Skipping CEMS activation %s without centroid", code)
            return None

        dt = _parse_datetime(event_time)
        extent_geom = _wkt_to_geometry(activation.get("extent"))
        bbox = _bbox_from_geometry(extent_geom) or _bbox_from_geometry(centroid_geom)
        country_codes = _country_codes(activation.get("countries") or [], self.geocoder)
        country_codes = _primary_country_code(centroid_geom, country_codes, self.geocoder)

        item = Item(
            id=f"cems-event-{sanitize_stac_item_id(str(code))}",
            geometry=centroid_geom,
            bbox=bbox,
            datetime=dt,
            properties={
                "title": activation.get("name") or f"CEMS activation {code}",
                "description": activation.get("reason") or "",
            },
        )
        item.properties["roles"] = ["event", "source"]

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.country_codes = country_codes
        monty.episode_number = 1
        primary_codes = self._canonical_codes_for_keys(hazard_keys[:1])
        monty.hazard_codes = primary_codes
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)
        monty.hazard_codes = self._canonical_codes_for_keys(hazard_keys)
        item.properties["keywords"] = self._activation_keywords(activation, primary_codes)

        if report_link := activation.get("reportLink"):
            item.add_link(
                Link(
                    rel="via",
                    target=str(report_link),
                    media_type="text/html",
                    title="CEMS situational report (StoryMap)",
                )
            )

        if gdacs_id := activation.get("gdacsId"):
            parsed = _parse_gdacs_id(str(gdacs_id))
            episode = self._gdacs_episode(str(gdacs_id))
            if parsed and episode is not None:
                _, event_id = parsed
                item.add_link(
                    Link(
                        rel="related",
                        target=self._relative_item_href("gdacs-events", f"{event_id}-{episode}"),
                        media_type="application/geo+json",
                        title=f"GDACS {gdacs_id} (episode {episode})",
                        extra_fields={"roles": ["event"]},
                    )
                )

        if charter_number := activation.get("charterNumber"):
            item.add_link(
                Link(
                    rel="related",
                    target=self._relative_item_href("charter-events", f"charter-event-{charter_number}"),
                    media_type="application/geo+json",
                    title=f"Charter activation {charter_number}",
                    extra_fields={"roles": ["event"]},
                )
            )

        for related_code in activation.get("relatedevents") or []:
            if related_code and related_code != code:
                item.add_link(
                    Link(
                        rel="related",
                        target=self._relative_item_href("cems-events", f"cems-event-{related_code}"),
                        media_type="application/geo+json",
                        extra_fields={"roles": ["event"]},
                    )
                )

        item.set_collection(self.get_event_collection())
        return item

    def _hazard_keys_for_aoi(self, activation: dict[str, Any], aoi: dict[str, Any]) -> list[str]:
        keys = _hazard_keys_for_activation(activation.get("category"), activation.get("subCategory"))
        footprint_keys: list[str] = []
        for product in aoi.get("products") or []:
            if str(product.get("type", "")).upper() in {"GRA", "GRM"}:
                footprint_keys.extend(_footprint_hazard_keys(product.get("stats")))
        for footprint_key in footprint_keys:
            if footprint_key not in keys:
                keys.append(footprint_key)
        return keys

    def make_hazard_items(
        self,
        activation: dict[str, Any],
        event_item: Item,
        event_corr_id: str,
    ) -> list[Item]:
        code = str(activation.get("code") or "x")
        event_time = activation.get("eventTime")
        if not event_time:
            return []
        dt = _parse_datetime(event_time)
        activation_countries = _country_codes(activation.get("countries") or [], self.geocoder)
        category_keys = set(_hazard_keys_for_activation(activation.get("category"), activation.get("subCategory")))
        items: list[Item] = []

        for aoi in activation.get("aois") or []:
            aoi_number = aoi.get("number")
            aoi_slug = _aoi_number_slug(aoi_number)
            hazard_keys = self._hazard_keys_for_aoi(activation, aoi)
            if not hazard_keys:
                continue
            hazard_details = _hazard_details_for_aoi(activation, aoi, hazard_keys[0])

            del_product = _latest_del_product(aoi.get("products") or [])
            if del_product and del_product.get("extent"):
                hazard_geom = _wkt_to_geometry(del_product.get("extent"))
            else:
                hazard_geom = _wkt_to_geometry(aoi.get("extent"))
            if not hazard_geom:
                continue

            bbox = _bbox_from_geometry(hazard_geom)
            aoi_name = aoi.get("name") or f"AOI {aoi_number}"
            country_codes = _primary_country_code(hazard_geom, activation_countries, self.geocoder)

            for hazard_key in hazard_keys:
                hazard_slug = _hazard_slug_for_key(hazard_key)
                if hazard_key in category_keys:
                    hazard_label = str(activation.get("subCategory") or activation.get("category") or hazard_slug).strip().lower()
                else:
                    hazard_label = hazard_slug
                hazard_codes = self._canonical_codes_for_keys([hazard_key])
                item_id = f"cems-hazard-{sanitize_stac_item_id(code)}-{aoi_slug}-{hazard_slug}"

                item = Item(
                    id=item_id,
                    geometry=hazard_geom,
                    bbox=bbox,
                    datetime=dt,
                    properties={
                        "title": f"{aoi_name} — {hazard_label} hazard area",
                    },
                )
                item.properties["roles"] = ["hazard", "source"]
                MontyExtension.add_to(item)
                monty = MontyExtension.ext(item)
                monty.country_codes = country_codes
                monty.hazard_codes = hazard_codes
                monty.correlation_id = event_corr_id

                if hazard_detail := hazard_details.get(hazard_key):
                    monty.hazard_detail = hazard_detail

                item.set_collection(self.get_hazard_collection())
                items.append(item)

        return items

    def make_response_items(
        self,
        activation: dict[str, Any],
        event_item: Item,
        hazard_items: list[Item],
    ) -> list[Item]:
        code = str(activation.get("code") or "x")
        event_monty = MontyExtension.ext(event_item)
        event_href = self._relative_item_href("cems-events", event_item.id)
        activation_countries = _country_codes(activation.get("countries") or [], self.geocoder)
        items: list[Item] = []
        prev_by_key: dict[tuple[int, str], Item] = {}

        for aoi in activation.get("aois") or []:
            aoi_number = int(aoi.get("number") or 0)
            aoi_slug = _aoi_number_slug(aoi_number)
            hazard_keys = self._hazard_keys_for_aoi(activation, aoi)
            hazard_codes = self._canonical_codes_for_keys(
                hazard_keys[:1] or _hazard_keys_for_activation(activation.get("category"), activation.get("subCategory"))
            )
            latest_del = _latest_del_product(aoi.get("products") or [])

            for product in sorted(
                aoi.get("products") or [],
                key=lambda product: (
                    str(product.get("type", "")),
                    int(product.get("monitoringNumber") or 0),
                ),
            ):
                response_type = _map_product_response_type(product.get("type"))
                if not response_type:
                    continue

                version = product.get("version") or {}
                status = _map_product_status(version)
                product_geom = _wkt_to_geometry(product.get("extent")) or _wkt_to_geometry(aoi.get("extent"))
                delivery_time = version.get("deliveryTime")
                dt = _parse_datetime(delivery_time) if delivery_time else event_item.datetime
                country_codes = _primary_country_code(product_geom, activation_countries, self.geocoder)

                item_id = f"cems-response-{sanitize_stac_item_id(code)}-{aoi_slug}-{_product_id_suffix(product)}"
                product_type_slug = PRODUCT_TYPE_SLUG.get(str(product.get("type", "")).upper(), "product")
                product_title = PRODUCT_TYPE_TITLE.get(str(product.get("type", "")).upper(), product_type_slug)
                title = f"CEMS {product_title} product — {aoi.get('name') or aoi_slug} ({code})"

                item = build_response_item(
                    id=item_id,
                    geometry=product_geom,
                    bbox=_bbox_from_geometry(product_geom),
                    datetime=dt,
                    correlation_id=event_monty.correlation_id,
                    country_codes=country_codes,
                    hazard_codes=hazard_codes,
                    type=response_type,
                    source_id=str(code),
                    status=status,
                    monitoring_number=int(product["monitoringNumber"])
                    if product.get("monitoring") and int(product.get("monitoringNumber") or 0) > 0
                    else None,
                    producer="Copernicus EMS",
                    methodology=MontyMethodology.HUMAN_INTERPRETED,
                    sendai_targets=RESPONSE_TYPE_SENDAI.get(response_type),
                    properties={
                        "title": title,
                        "processing:level": "L3",
                        "roles": ["response", "source"],
                    },
                )
                _apply_delivery_datetime(item, delivery_time)

                item.stac_extensions = list(dict.fromkeys([SCHEMA_URI, PROCESSING_SCHEMA_URI]))
                item.assets = _product_assets(product)

                item.add_link(
                    Link(rel="related", target=event_href, media_type="application/geo+json", extra_fields={"roles": ["event"]})
                )
                for hazard_item in hazard_items:
                    if f"-{aoi_slug}-" in hazard_item.id:
                        item.add_link(
                            Link(
                                rel="related",
                                target=self._relative_item_href("cems-hazards", hazard_item.id),
                                media_type="application/geo+json",
                                extra_fields={"roles": ["hazard"]},
                            )
                        )
                        if str(product.get("type", "")).upper() == "DEL" and product is latest_del and product.get("extent"):
                            hazard_item.add_link(
                                Link(
                                    rel="related",
                                    target=self._relative_item_href("cems-response", item.id),
                                    media_type="application/geo+json",
                                    extra_fields={"roles": ["response"]},
                                )
                            )
                item.add_link(
                    Link(
                        rel="derived_from",
                        target=f"{CEMS_PORTAL_BASE}/{code}",
                        media_type="text/html",
                        title=f"CEMS {code} activation",
                    )
                )

                monitoring_key = (aoi_number, str(product.get("type", "")).upper())
                monitoring_number = int(product.get("monitoringNumber") or 0)
                if product.get("monitoring") and monitoring_number > 0:
                    prev_item = prev_by_key.get(monitoring_key)
                    if prev_item is not None:
                        link_monitoring_update(item, prev_item)
                prev_by_key[monitoring_key] = item

                item.set_collection(self.get_response_collection())
                items.append(item)

        if report_link := activation.get("reportLink"):
            sr_item = build_response_item(
                id=f"cems-response-{sanitize_stac_item_id(code)}-sr",
                geometry=None,
                bbox=None,
                datetime=event_item.datetime,
                correlation_id=event_monty.correlation_id,
                country_codes=list(event_monty.country_codes or []),
                hazard_codes=self._canonical_codes_for_keys(
                    _hazard_keys_for_activation(activation.get("category"), activation.get("subCategory"))
                ),
                type="eo-sr",
                source_id=str(code),
                status=MontyResponseStatus.PUBLISHED,
                producer="Copernicus EMS",
                methodology=MontyMethodology.HUMAN_INTERPRETED,
                sendai_targets=RESPONSE_TYPE_SENDAI["eo-sr"],
                properties={"title": f"CEMS situational report — {code}", "roles": ["response", "source"]},
            )
            sr_item.assets = {
                "storymap": Asset(
                    href=str(report_link),
                    media_type="text/html",
                    roles=["metadata"],
                    title="CEMS situational report (StoryMap)",
                    extra_fields={"type": "text/html"},
                )
            }
            sr_item.add_link(
                Link(rel="related", target=event_href, media_type="application/geo+json", extra_fields={"roles": ["event"]})
            )
            sr_item.set_collection(self.get_response_collection())
            items.append(sr_item)

        return items

    def make_impact_items(
        self,
        activation: dict[str, Any],
        event_item: Item,
        response_items: list[Item],
    ) -> list[Item]:
        code = str(activation.get("code") or "x")
        event_monty = MontyExtension.ext(event_item)
        activation_countries = _country_codes(activation.get("countries") or [], self.geocoder)
        items: list[Item] = []
        prev_by_key: dict[tuple[int, str], Item] = {}

        response_by_id = {item.id: item for item in response_items}
        for aoi in activation.get("aois") or []:
            aoi_number = int(aoi.get("number") or 0)
            aoi_slug = _aoi_number_slug(aoi_number)
            hazard_codes = self._canonical_codes_for_keys(self._hazard_keys_for_aoi(activation, aoi)[:1])

            for product in aoi.get("products") or []:
                if str(product.get("type", "")).upper() not in {"GRA", "GRM"}:
                    continue
                stats = product.get("stats")
                if not isinstance(stats, dict):
                    continue

                response_id = f"cems-response-{sanitize_stac_item_id(code)}-{aoi_slug}-{_product_id_suffix(product)}"
                response_item = response_by_id.get(response_id)
                if response_item is None:
                    continue

                version = product.get("version") or {}
                delivery_time = version.get("deliveryTime")
                dt = _parse_datetime(delivery_time) if delivery_time else response_item.datetime
                product_geom = response_item.geometry
                bbox = response_item.bbox

                for thematic_class, subclasses in stats.items():
                    if not _is_exposure_thematic(str(thematic_class)):
                        continue
                    if not isinstance(subclasses, dict):
                        continue
                    impact_detail = _impact_detail_for_subclasses(str(thematic_class), subclasses)
                    if impact_detail is None:
                        continue
                    thematic_key = _normalize_key(str(thematic_class))
                    slug = IMPACT_THEMATIC_SLUG.get(thematic_key, sanitize_stac_item_id(str(thematic_class)))
                    monitoring_suffix = ""
                    if product.get("monitoring") and int(product.get("monitoringNumber") or 0) > 0:
                        monitoring_suffix = f"-m{int(product['monitoringNumber'])}"
                    item_id = f"cems-impact-{sanitize_stac_item_id(code)}-{aoi_slug}-gra-{slug}{monitoring_suffix}"

                    item = Item(
                        id=item_id,
                        geometry=product_geom,
                        bbox=bbox,
                        datetime=dt,
                        properties={
                            "title": (
                                f"Estimated affected population — {aoi.get('name') or aoi_slug} ({code} GRA)"
                                if _normalize_key(str(thematic_class)).startswith("estimated population")
                                else f"{thematic_class} — {aoi.get('name') or aoi_slug} ({code} GRA)"
                            ),
                        },
                    )
                    item.properties["roles"] = ["impact", "source"]
                    MontyExtension.add_to(item)
                    monty = MontyExtension.ext(item)
                    monty.country_codes = _primary_country_code(product_geom, activation_countries, self.geocoder)
                    monty.hazard_codes = hazard_codes
                    monty.correlation_id = event_monty.correlation_id
                    monty.impact_detail = impact_detail

                    if response_item is not None:
                        item.add_link(
                            Link(
                                rel="derived_from",
                                target=self._relative_item_href("cems-response", response_item.id),
                                media_type="application/geo+json",
                                extra_fields={"roles": ["response"]},
                            )
                        )

                    prev_key = (aoi_number, slug)
                    prev_item = prev_by_key.get(prev_key)
                    if prev_item is not None:
                        item.add_link(Link(rel="prev", target=prev_item, media_type="application/geo+json"))
                    prev_by_key[prev_key] = item

                    item.set_collection(self.get_impact_collection())
                    items.append(item)

        return items

    def add_cems_related_links(
        self,
        event_item: Item,
        hazard_items: list[Item],
        response_items: list[Item],
        impact_items: list[Item],
    ) -> None:
        for hazard in hazard_items:
            event_item.add_link(
                Link(
                    rel="related",
                    target=self._relative_item_href("cems-hazards", hazard.id),
                    media_type="application/geo+json",
                    extra_fields={"roles": ["hazard"]},
                )
            )
            hazard.add_link(
                Link(
                    rel="related",
                    target=self._relative_item_href("cems-events", event_item.id),
                    media_type="application/geo+json",
                    extra_fields={"roles": ["event"]},
                )
            )
        for response in response_items:
            event_item.add_link(
                Link(
                    rel="related",
                    target=self._relative_item_href("cems-response", response.id),
                    media_type="application/geo+json",
                    extra_fields={"roles": ["response"]},
                )
            )
        for impact in impact_items:
            event_item.add_link(
                Link(
                    rel="related",
                    target=self._relative_item_href("cems-impacts", impact.id),
                    media_type="application/geo+json",
                    extra_fields={"roles": ["impact"]},
                )
            )

    def get_stac_items(self) -> Generator[Item, None, None]:
        self.transform_summary.mark_as_started()
        self.transform_summary.increment_rows()
        try:
            activation = self.data_source.activation_data
            event_item = self.make_event_item(activation)
            if not event_item:
                self.transform_summary.increment_failed_rows()
                self.transform_summary.mark_as_complete()
                return

            event_monty = MontyExtension.ext(event_item)
            hazard_items = self.make_hazard_items(activation, event_item, event_monty.correlation_id)
            response_items = self.make_response_items(activation, event_item, hazard_items)
            impact_items = self.make_impact_items(activation, event_item, response_items)
            self.add_cems_related_links(event_item, hazard_items, response_items, impact_items)

            yield event_item
            for hazard_item in hazard_items:
                yield hazard_item
            for response_item in response_items:
                yield response_item
            for impact_item in impact_items:
                yield impact_item
        except Exception:
            self.transform_summary.increment_failed_rows()
            logger.warning("Failed to process CEMS activation", exc_info=True)
        self.transform_summary.mark_as_complete()

    def make_items(self) -> list[Item]:
        return list(self.get_stac_items())


_CEMS_LICENSE = "other"
_CEMS_LICENSE_URL = "https://mapping.emergency.copernicus.eu/terms-and-conditions/"
_CEMS_LICENSE_TITLE = "Copernicus EMS On-Demand Mapping Terms and Conditions"

_CEMS_BATCH = BatchExportConfig(
    source_slug="cems",
    provider=_CEMS_PROVIDER,
    license=_CEMS_LICENSE,
    license_url=_CEMS_LICENSE_URL,
    license_title=_CEMS_LICENSE_TITLE,
    titles={
        "event": (
            "Copernicus EMS RM Events",
            "Copernicus EMS Rapid Mapping activations as Monty Event items.",
        ),
        "hazard": (
            "Copernicus EMS RM Hazards",
            "Copernicus EMS Rapid Mapping Areas of Interest as Monty Hazard items.",
        ),
        "response": (
            "Copernicus EMS RM Response",
            "Copernicus EMS Rapid Mapping products (REF/FEP/DEL/GRA/SR) as Monty Response items.",
        ),
        "impact": (
            "Copernicus EMS RM Impacts",
            "Damage/exposure statistics from CEMS grading products as Monty Impact items.",
        ),
    },
)


def _load_json(path: Path) -> dict[str, Any]:
    data: Any = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def _activation_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("results"):
        results = payload["results"]
        if not results:
            raise ValueError("CEMS detail payload has empty results")
        activation = results[0]
        if not isinstance(activation, dict):
            raise ValueError("CEMS activation result must be an object")
        return activation
    return payload


def iter_cems_stac_items(
    input_path: Path,
    *,
    geocoder: MontyGeoCoder | None = None,
) -> Generator[Item, None, None]:
    """Yield STAC items from CEMS detail JSON file(s).

    *input_path* may be a single ``*-detail.json`` file or a directory containing
    such files (for example ``monty-stac-extension/docs/model/sources/CEMS/api-files``).
    """
    from pystac_monty.sources.batch_export import use_local_collection_examples

    use_local_collection_examples()
    resolved_geocoder = geocoder if geocoder is not None else default_cems_export_geocoder()
    paths: list[Path]
    if input_path.is_file():
        paths = [input_path]
    else:
        paths = sorted(input_path.glob("*-detail.json"))
        if not paths and (input_path / "api-files").is_dir():
            paths = sorted((input_path / "api-files").glob("*-detail.json"))

    for path in paths:
        payload = _load_json(path)
        activation = _activation_from_payload(payload)
        code = activation.get("code", path.stem)
        source = CEMSDataSource(
            data=GenericDataSource(
                source_url=f"{CEMS_DASHBOARD_API}/public-activations/?code={code}",
                input_data=Memory(content=payload, data_type=DataType.MEMORY),
            )
        )
        yield from CEMSTransformer(source, resolved_geocoder).get_stac_items()


def convert_cems(
    input_path: Path,
    output_dir: Path,
    public_href_base: str | None = None,
    *,
    geocoder: MontyGeoCoder | None = None,
) -> None:
    """Read CEMS activation detail JSON from *input_path* and export Monty STAC collections."""
    config = replace(_CEMS_BATCH, public_href_base=public_href_base)
    log_batch_role_counts(*export_collected_items(config, list(iter_cems_stac_items(input_path, geocoder=geocoder)), output_dir))


def export_curated_cems_examples(
    items: Sequence[Item],
    output_dir: Path,
    *,
    curated_ids: Sequence[str] = CURATED_CEMS_EXAMPLE_IDS,
    public_href_base: str | None = MONTY_STAC_EXAMPLES_BASE_URL,
) -> None:
    """Export a curated CEMS example slice with collections matching on-disk item files."""
    curated = _prepare_curated_cems_items(items, frozenset(curated_ids))
    config = replace(_CEMS_BATCH, public_href_base=public_href_base)
    log_batch_role_counts(*export_collected_items(config, curated, output_dir))


def regenerate_cems_examples(
    input_path: Path,
    output_dir: Path,
    *,
    geocoder: MontyGeoCoder | None = None,
) -> None:
    """Regenerate published CEMS examples with absolute public self/collection links."""
    export_curated_cems_examples(list(iter_cems_stac_items(input_path, geocoder=geocoder)), output_dir)
