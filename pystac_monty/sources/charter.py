"""Charter STAC source transformer

Transforms International Charter on Space and Major Disasters data into Monty STAC items.
Batch export uses :mod:`pystac_monty.exporter`;
this module provides :func:`convert_charter_activations` and :func:`iter_charter_stac_items`.

Maps activations to events, areas to hazards (one item per disaster type), and VAP sidecars
to response items with ``monty:response_detail`` (v1.3.0) when ``act-*-vap-*.json`` fixtures
are present alongside activation/area model files.
"""

import datetime
import hashlib
import json
import logging
import re
import xml.etree.ElementTree as ET
from copy import deepcopy
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Generator, List, Optional
from urllib.parse import unquote

import requests  # type: ignore[import-untyped]
from pystac import Asset, Collection, Item, Link
from pystac.provider import Provider, ProviderRole

from pystac_monty.exporter import (
    MONTY_SOURCE_DATETIME_PROPERTY,
    MONTY_STAC_EXAMPLES_BASE_URL,
    BatchExportConfig,
    export_collected_items,
    log_batch_role_counts,
)
from pystac_monty.extension import SCHEMA_URI, HazardDetail, MontyEstimateType, MontyExtension, MontyMethodology
from pystac_monty.geocoding import MockGeocoder, MontyGeoCoder
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.response import build_response_item, link_related_response
from pystac_monty.sources.common import (
    CharterDataSourceType,
    DataType,
    GenericDataSource,
    Memory,
    MontyDataSourceV3,
    MontyDataTransformer,
    sanitize_stac_item_id,
)
from pystac_monty.validators.charter import CharterSourceModel

logger = logging.getLogger(__name__)

CHARTER_API_BASE = "https://supervisor.disasterscharter.org/api"
DISASTER_SCHEMA_URI = "https://terradue.github.io/stac-extensions-disaster/v1.1.0/schema.json"
LEGACY_DISASTER_SCHEMA_URI = "https://terradue.github.io/disaster/v1.0.0/schema.json"

# Charter disaster:type to [UNDRR-2025, EM-DAT, GLIDE]
CHARTER_HAZARD_CODES = {
    "flood": ["MH0600", "nat-hyd-flo-flo", "FL"],
    "fire": ["MH1301", "nat-cli-wil-for", "WF"],
    "earthquake": ["GH0101", "nat-geo-ear-gro", "EQ"],
    "volcano": ["GH0201", "nat-geo-vol", "VO"],
    "storm_hurricane": ["MH0400", "nat-met-sto", "ST"],
    "cyclone": ["MH0403", "nat-met-sto-tro", "TC"],
    "tsunami": ["GH0301", "nat-geo-ear-tsu", "TS"],
    "landslide": ["MH0901", "nat-geo-mmd-lan", "LS"],
    "snow_hazard": ["MH1202", "nat-met-ext-col", "SW"],
    "ice": ["MH0801", "nat-met-ext-col", "CW"],
    "oil_spill": ["TH0300", "tec-ind-che"],
    "explosive_event": ["TH0600", "tec-ind-exp"],
    # Deprecated types (older activations)
    "storm_hurricane_rural": ["MH0400", "nat-met-sto", "ST"],
    "storm_hurricane_urban": ["MH0400", "nat-met-sto", "ST"],
    "flood_large": ["MH0604", "nat-hyd-flo-flo", "FL"],
    "flood_flash": ["MH0603", "nat-hyd-flo-flo", "FF"],
}

MANUAL_REVIEW_DISASTER_TYPES = {"other"}

# CPE status to estimate_type (interim mapping)
CPE_STATUS_MAPPING = {
    "notificationNew": "primary",
    "notificationImported": "primary",
}

RESPONSE_TYPE_SENDAI: dict[str, list[str]] = {
    "eo-dat": ["D", "G"],
    "eo-ref": ["G"],
    "eo-gra": ["C", "D"],
    "eo-del": ["D", "G"],
    "eo-fep": ["D", "G"],
    "eo-mon": ["D", "G"],
    "eo-pop": ["B"],
    "eo-vap": ["D", "G"],
    "eo-sr": ["G"],
}

_INTERNAL_CHARTER_PROPS = {
    "cpe:status",
    "cpe:notified",
    "cpe:cos2_xml",
    "cpe:cos2_id",
    "cpe:processing_monitoring_id",
    "cpe:notification_source",
}

_CHARTER_PROVIDER = Provider(
    "International Charter Space and Major Disasters",
    roles=[ProviderRole.PRODUCER],
    url="https://disasterscharter.org/",
    description=(
        "The Charter is an international collaboration through which satellite data "
        "is made available for disaster management purposes."
    ),
)


@dataclass
class CharterDataSource(MontyDataSourceV3):
    """Charter data source containing activation, area, and optional VAP data."""

    activation_data: dict[str, Any]
    areas_data: List[dict[str, Any]]
    vaps_data: List[dict[str, Any]]
    calibrated_datasets_data: List[dict[str, Any]]

    def __init__(self, data: GenericDataSource | CharterDataSourceType, eoapi_url: Optional[str] = None):
        super().__init__(root=data, eoapi_url=eoapi_url)

        input_data = data.input_data if isinstance(data, GenericDataSource) else data.activation_data
        if input_data.data_type == DataType.MEMORY:
            file_data = input_data.content
        elif input_data.data_type == DataType.FILE:
            with open(input_data.path, "r", encoding="utf-8") as f:
                file_data = json.load(f)
        else:
            file_data = {}

        CharterSourceModel.model_validate(file_data)
        self.activation_data = file_data
        self.areas_data = file_data.get("areas", [])
        self.vaps_data = file_data.get("vaps", [])
        self.calibrated_datasets_data = file_data.get("calibrated_datasets", [])


class CharterTransformer(MontyDataTransformer[CharterDataSource]):
    """Transforms Charter activation data into STAC Items

    Following Charter mapping specification:
    - Activation → Event item
    - Area → Hazard item(s) (one per disaster:type for multi-hazard)
    - ValueAddedProduct → Response item (when VAP sidecars are available)
    """

    hazard_profiles = MontyHazardProfiles()
    source_name = "charter"

    def __init__(self, data_source: CharterDataSource, geocoder: MontyGeoCoder | None = None) -> None:
        super().__init__(data_source, geocoder or MockGeocoder())
        self._response_collection_cache: Collection | None = None
        self.response_collection_url = f"{MontyDataTransformer.base_collection_url}/charter-response/charter-response.json"

    def get_response_collection(self) -> Collection:
        """Collection for Charter response items (``charter-response``)."""
        if self._response_collection_cache is None:
            url = self.response_collection_url
            if url.startswith("http"):
                collection_dict = json.loads(requests.get(url, timeout=60).text)
            else:
                with open(url, encoding="utf-8") as f:
                    collection_dict = json.load(f)
            collection = Collection.from_dict(collection_dict)
            collection.set_self_href(url)
            self._response_collection_cache = collection
        return self._response_collection_cache

    @staticmethod
    def _relative_item_href(collection_id: str, item_id: str) -> str:
        return f"../{collection_id}/{item_id}.json"

    def _canonical_codes_for_types(self, disaster_types: List[str]) -> List[str]:
        """Canonical hazard code trios per disaster type, concatenated in type order."""
        merged: List[str] = []
        for dtype in disaster_types:
            if dtype not in CHARTER_HAZARD_CODES:
                continue
            stub = Item(
                id="stub",
                geometry={"type": "Point", "coordinates": [0.0, 0.0]},
                bbox=[0.0, 0.0, 0.0, 0.0],
                datetime=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
                properties={"monty:hazard_codes": list(CHARTER_HAZARD_CODES[dtype])},
            )
            MontyExtension.add_to(stub)
            merged.extend(self.hazard_profiles.get_canonical_hazard_codes(stub))
        return merged

    @staticmethod
    def parse_area_description(description: str) -> tuple[Optional[float], Optional[int], Optional[float]]:
        """Parse radius, priority, and surface area from area description."""
        radius: Optional[float] = None
        priority: Optional[int] = None
        surface_area: Optional[float] = None

        if not description:
            return radius, priority, surface_area

        radius_match = re.search(r"Radius\s*\(km\)\s*:\s*(\d+\.?\d*)", description, re.IGNORECASE)
        if radius_match:
            radius = float(radius_match.group(1))

        priority_match = re.search(r"Priority\s*:\s*(\d+)", description, re.IGNORECASE)
        if priority_match:
            priority = int(priority_match.group(1))

        surface_match = re.search(r"SurfaceArea:\s*(\d+\.?\d*)", description, re.IGNORECASE)
        if surface_match:
            surface_area = float(surface_match.group(1))

        return radius, priority, surface_area

    @staticmethod
    def _as_disaster_type_list(value: Any) -> list[str]:
        """Normalize Charter API disaster type spellings to a list of known type strings."""
        if value is None:
            return []
        values = value if isinstance(value, list) else [value]
        result: list[str] = []
        for raw in values:
            if not isinstance(raw, str):
                continue
            candidate = raw.strip()
            if not candidate:
                continue
            key = candidate.lower().replace("-", "_").replace(" ", "_")
            if key in CHARTER_HAZARD_CODES:
                result.append(key)
                continue
            # API area catalogs can expose labels or URLs rather than the exact key.
            for known_type in CHARTER_HAZARD_CODES:
                if known_type in key:
                    result.append(known_type)
                    break
        return list(dict.fromkeys(result))

    @staticmethod
    def _manual_review_disaster_types(value: Any) -> list[str]:
        """Return normalized source types that need a human mapping decision."""
        values = value if isinstance(value, list) else [value]
        result: list[str] = []
        for raw in values:
            if not isinstance(raw, str):
                continue
            key = raw.strip().lower().replace("-", "_").replace(" ", "_")
            if key in MANUAL_REVIEW_DISASTER_TYPES:
                result.append(key)
        return list(dict.fromkeys(result))

    @staticmethod
    def _area_disaster_types(area_props: dict[str, Any], activation_types: list[str]) -> list[str]:
        for key in ("disaster:type", "disaster:types", "disastertype", "disastertypes", "disaster_type", "disaster_types"):
            types = CharterTransformer._as_disaster_type_list(area_props.get(key))
            if types:
                return types
        return activation_types

    @staticmethod
    def _parse_datetime(value: Any) -> datetime.datetime:
        if isinstance(value, str):
            return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime.datetime):
            raise TypeError(f"Expected datetime or ISO datetime string, got {type(value).__name__}")
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value

    @staticmethod
    def _is_test_activation(props: dict[str, Any]) -> bool:
        title = str(props.get("title", "")).strip().lower()
        return bool(re.match(r"^\[?test\b", title) or "test activation" in title)

    @staticmethod
    def _area_slug(area: dict[str, Any], activation_id: Any) -> str:
        raw_area_id = str(area.get("id", "")).strip()
        uid_match = re.search(r"[?&]uid=([^&]+)", raw_area_id)
        if uid_match:
            raw_area_id = unquote(uid_match.group(1))
        else:
            raw_area_id = raw_area_id.rstrip("/").split("/")[-1].removesuffix(".json")

        raw_area_id = re.sub(rf"-area-act-{re.escape(str(activation_id))}$", "", raw_area_id, flags=re.IGNORECASE)
        return sanitize_stac_item_id(raw_area_id.lower())

    def make_event_item(self, activation: dict[str, Any]) -> Optional[Item]:
        """Create Event STAC item from Charter activation."""
        props = activation.get("properties", {})

        activation_id = props.get("disaster:activation_id")
        if not activation_id:
            logger.warning("Missing disaster:activation_id")
            return None

        raw_disaster_types = props.get("disaster:type", [])
        disaster_types = self._as_disaster_type_list(raw_disaster_types)
        manual_review_types = self._manual_review_disaster_types(raw_disaster_types)
        if not disaster_types:
            if manual_review_types:
                logger.warning(
                    "Skipping activation %s: disaster types require manual review: %s",
                    activation_id,
                    ", ".join(manual_review_types),
                )
            else:
                logger.warning("Skipping activation %s: no mapped disaster types", activation_id)
            return None

        if props.get("cpe:activation_status") == "cancelled":
            logger.warning("Skipping cancelled activation %s", activation_id)
            return None
        if self._is_test_activation(props):
            logger.warning("Skipping test activation %s", activation_id)
            return None

        geom = activation.get("geometry")
        if not geom:
            logger.warning(f"Missing geometry for activation {activation_id}")
            return None

        datetime_str = props.get("datetime")
        if not datetime_str:
            logger.warning(f"Missing datetime for activation {activation_id}")
            return None

        dt = self._parse_datetime(datetime_str)

        safe_activation_id = sanitize_stac_item_id(str(activation_id))
        item = Item(
            id=f"charter-event-{safe_activation_id}",
            geometry=geom,
            bbox=activation.get("bbox"),
            datetime=dt,
            properties={
                "title": props.get("title", f"Charter Activation {activation_id}"),
                "description": props.get("description", ""),
            },
        )

        item.properties["roles"] = ["event", "source"]

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)

        country = props.get("disaster:country")
        monty.country_codes = [country] if country else []

        monty.episode_number = 1
        # corr_id is derived from the primary hazard only; set all canonical codes afterwards.
        primary_codes = self._canonical_codes_for_types([disaster_types[0]]) if disaster_types else []
        monty.hazard_codes = primary_codes
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)
        monty.hazard_codes = self._canonical_codes_for_types(disaster_types)

        hazard_keywords = self.hazard_profiles.get_keywords(primary_codes)
        item.properties["keywords"] = sorted(set(hazard_keywords + monty.country_codes))

        item.add_link(Link("via", f"{CHARTER_API_BASE}/activations/act-{activation_id}", "application/json"))

        item.set_collection(self.get_event_collection())

        return item

    def make_hazard_items(self, activation: dict[str, Any], areas: List[dict[str, Any]], event_corr_id: str) -> List[Item]:
        """Create Hazard STAC items from Charter areas (one per area per disaster:type)."""
        items: List[Item] = []
        used_hazard_ids: set[str] = set()
        props = activation.get("properties", {})
        activation_id = props.get("disaster:activation_id")
        datetime_str = props.get("datetime")
        activation_types = self._as_disaster_type_list(props.get("disaster:type", []))
        country = props.get("disaster:country")

        dt = self._parse_datetime(datetime_str)

        safe_activation_id = sanitize_stac_item_id(str(activation_id)) if activation_id else "x"

        for area in areas:
            area_props = area.get("properties", {})
            area_id = area.get("id", "unknown")
            area_title = area_props.get("title", "Unknown Area")
            area_geom = area.get("geometry")

            if not area_geom:
                continue

            description = (
                area_props.get("description") or area_props.get("summary") or area_props.get("content", {}).get("value", "")
            )
            radius, _priority, surface_area = self.parse_area_description(description)

            cpe_status = area_props.get("cpe:status", {})
            stage = cpe_status.get("stage", "notificationNew")
            estimate_type = CPE_STATUS_MAPPING.get(stage, "primary")

            area_types = self._area_disaster_types(area_props, activation_types)
            for dtype in area_types:
                if dtype not in CHARTER_HAZARD_CODES:
                    continue

                area_slug = self._area_slug(area, activation_id)
                safe_dtype = sanitize_stac_item_id(dtype)
                base_hazard_id = f"charter-hazard-{safe_activation_id}-{area_slug}-{safe_dtype}"
                item_id = base_hazard_id
                if item_id in used_hazard_ids:
                    tag = hashlib.sha256(f"{area_id!s}:{dtype}".encode()).hexdigest()[:8]
                    item_id = f"{base_hazard_id}-{tag}"
                used_hazard_ids.add(item_id)

                item = Item(
                    id=item_id,
                    geometry=area_geom,
                    bbox=self._stac_bbox(area, area_geom),
                    datetime=dt,
                    properties={
                        "title": area_title,
                        "description": description,
                    },
                )

                item.properties["roles"] = ["hazard", "source"]

                MontyExtension.add_to(item)
                monty = MontyExtension.ext(item)

                monty.country_codes = [country] if country else []
                monty.hazard_codes = self._canonical_codes_for_types([dtype])
                monty.correlation_id = event_corr_id

                hazard_detail_kwargs: dict[str, Any] = {"estimate_type": MontyEstimateType(estimate_type)}
                if radius is not None:
                    hazard_detail_kwargs.update(
                        severity_value=radius,
                        severity_unit="km",
                        severity_label="Area radius",
                    )
                elif surface_area is not None:
                    hazard_detail_kwargs.update(
                        severity_value=surface_area,
                        severity_unit="km2",
                        severity_label="Surface area",
                    )
                monty.hazard_detail = HazardDetail(**hazard_detail_kwargs)

                hazard_keywords = self.hazard_profiles.get_keywords(monty.hazard_codes)
                item.properties["keywords"] = sorted(set(hazard_keywords + monty.country_codes))

                item.set_collection(self.get_hazard_collection())

                items.append(item)

        return items

    @staticmethod
    def _activation_web_href(activation: dict[str, Any]) -> Optional[str]:
        for link in activation.get("links", []):
            if link.get("rel") == "about" and link.get("href"):
                return str(link["href"])
        return None

    @staticmethod
    def _vap_source_id(vap: dict[str, Any]) -> Optional[str]:
        props = vap.get("properties", {})
        cos2_xml = props.get("cpe:cos2_xml")
        if isinstance(cos2_xml, str) and cos2_xml.strip():
            try:
                root = ET.fromstring(cos2_xml)
                for elem in root.iter():
                    if elem.tag.rsplit("}", 1)[-1] == "identifier" and elem.text:
                        identifier = elem.text.strip()
                        if re.fullmatch(r"\d+-\d+", identifier):
                            return identifier
            except ET.ParseError:
                match = re.search(r"<identifier>\s*(\d+-\d+)\s*</identifier>", cos2_xml)
                if match:
                    return match.group(1)

        cos2_id = props.get("cpe:cos2_id") or vap.get("id", "")
        match = re.search(r"vap-(\d+-\d+)$", str(cos2_id))
        if match:
            return match.group(1)
        vap_id = str(vap.get("id", ""))
        match = re.search(r"vap-(\d+-\d+)$", vap_id)
        return match.group(1) if match else None

    @staticmethod
    def _vap_call_ids(source_id: str) -> list[int | str]:
        call_id = source_id.split("-", 1)[0]
        try:
            return [int(call_id)]
        except ValueError:
            return [call_id]

    @staticmethod
    def _infer_response_type(title: str, description: str) -> str:
        # Charter sidecars do not expose a normalized response type yet.
        text = f"{title} {description}".lower()
        if "damage assessment" in text or "damaged building" in text or "geological risk" in text or "landslide" in text:
            return "eo-gra"
        if "affected area" in text or "extent" in text:
            return "eo-del"
        if "population" in text or "exposure" in text:
            return "eo-pop"
        return "eo-vap"

    @staticmethod
    def _infer_producer(copyright: str) -> Optional[str]:
        if not copyright:
            return None
        text = copyright.lower()
        if "inpe" in text:
            return "INPE"
        if "airbus" in text:
            return "Airbus"
        if "maxar" in text or "digitalglobe" in text or "worldview" in text or "geoeye" in text:
            return "Maxar"
        if "copernicus" in text or "sentinel" in text or "esa" in text:
            return "ESA/EC (Copernicus)"
        if "usgs" in text or "landsat" in text or "nasa" in text:
            return "USGS/NASA"
        if "dlr" in text or "terrasar" in text or "tsx" in text:
            return "DLR"
        return None

    @staticmethod
    def _infer_resolution_class(copyright: str) -> Optional[str]:
        if not copyright:
            return None
        text = copyright.lower()
        if any(token in text for token in ("pleiades", "pléiades", "airbus", "maxar", "worldview", "geoeye")):
            return "VHR"
        if any(token in text for token in ("spot", "rapideye", "planetscope")):
            return "HR"
        if any(token in text for token in ("sentinel", "landsat", "copernicus")):
            return "MR"
        return None

    @staticmethod
    def _normalize_polygon_geometry(geom: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        if not geom or geom.get("type") != "Polygon":
            return geom
        cleaned_rings = []
        for ring in geom.get("coordinates", []):
            pts = list(ring)
            while len(pts) >= 2 and pts[-1] == pts[-2]:
                pts.pop()
            if pts and pts[0] != pts[-1]:
                pts.append(pts[0])
            cleaned_rings.append(pts)
        return {"type": "Polygon", "coordinates": cleaned_rings}

    @staticmethod
    def _bbox_from_geometry(geom: Optional[dict[str, Any]]) -> Optional[list[float]]:
        if not geom:
            return None
        coords: list[Any]
        if geom.get("type") == "Point":
            coords = [geom.get("coordinates", [])]
        elif geom.get("type") == "Polygon":
            coords = [point for ring in geom.get("coordinates", []) for point in ring]
        else:
            return None
        xs = [float(point[0]) for point in coords if len(point) >= 2]
        ys = [float(point[1]) for point in coords if len(point) >= 2]
        if not xs or not ys:
            return None
        return [min(xs), min(ys), max(xs), max(ys)]

    @staticmethod
    def _stac_bbox(doc: dict[str, Any], geom: Optional[dict[str, Any]]) -> Optional[list[float]]:
        bbox = doc.get("bbox")
        if isinstance(bbox, list) and len(bbox) in {4, 6}:
            return bbox
        return CharterTransformer._bbox_from_geometry(geom)

    @staticmethod
    def _dataset_source_id(dataset: dict[str, Any]) -> str:
        return str(dataset.get("id", ""))

    @staticmethod
    def _clean_response_properties(props: dict[str, Any]) -> dict[str, Any]:
        cleaned = dict(props)
        for key in _INTERNAL_CHARTER_PROPS:
            cleaned.pop(key, None)
        if "disaster:type" in cleaned:
            cleaned["disaster:types"] = cleaned.pop("disaster:type")
        if "disaster:region" in cleaned:
            cleaned["disaster:regions"] = cleaned.pop("disaster:region")
        for key in list(cleaned):
            if key.startswith("cpe:") or cleaned[key] is None:
                cleaned.pop(key, None)
        return cleaned

    @staticmethod
    def _response_stac_extensions(source_doc: dict[str, Any]) -> list[str]:
        props = source_doc.get("properties", {})
        extensions = [
            SCHEMA_URI,
            *[
                uri
                for uri in source_doc.get("stac_extensions", [])
                if uri not in {SCHEMA_URI, LEGACY_DISASTER_SCHEMA_URI, DISASTER_SCHEMA_URI}
                and not (
                    uri == "https://stac-extensions.github.io/projection/v1.0.0/schema.json" and props.get("proj:epsg") is None
                )
            ],
            DISASTER_SCHEMA_URI,
        ]
        return list(dict.fromkeys(extensions))

    @staticmethod
    def _matched_activation_call_id(dataset: dict[str, Any], call_ids: list[str]) -> Optional[str]:
        dataset_call_ids = set(map(str, dataset.get("properties", {}).get("disaster:call_ids", [])))
        for call_id in call_ids:
            if call_id in dataset_call_ids:
                return call_id
        return None

    @staticmethod
    def _dataset_response_id(dataset: dict[str, Any]) -> Optional[str]:
        source_id = CharterTransformer._dataset_source_id(dataset)
        if not source_id:
            return None
        match = re.match(r"DS_([A-Za-z0-9]+)_.*_([^_]+)_([^_]+)-calibrated$", source_id)
        if match:
            instrument, strip_id, scene_id = match.groups()
            return f"{sanitize_stac_item_id(instrument.lower())}-{strip_id.lower()}-{scene_id.lower()}"
        fallback = source_id.removesuffix("-calibrated")
        return sanitize_stac_item_id(fallback.lower())

    @staticmethod
    def _dataset_producer(dataset: dict[str, Any]) -> Optional[str]:
        for provider in dataset.get("properties", {}).get("providers", []):
            name = provider.get("name")
            if isinstance(name, str) and name:
                return name
        return None

    @staticmethod
    def _hazard_types_for_product(title: str, description: str, activation_types: list[str]) -> list[str]:
        text = f"{title} {description}".lower()
        if "landslide" in text or "geological risk" in text:
            return ["landslide"] if "landslide" in activation_types else activation_types[:1]
        if "flood" in text:
            return ["flood"] if "flood" in activation_types else activation_types[:1]
        return activation_types[:1]

    @staticmethod
    def _dataset_matches_vap(vap: dict[str, Any], dataset_item: Item) -> bool:
        props = vap.get("properties", {})
        text = f"{props.get('title', '')} {props.get('copyright', '')} {props.get('additional_information', '')}".lower()
        dataset_id = dataset_item.properties.get("monty:response_detail", {}).get("source_id", dataset_item.id).lower()
        if any(token in text for token in ("pleiades", "pléiades", "airbus", "cnes")):
            return "phr" in dataset_id or "pleiades" in dataset_id
        if "sentinel" in text or "copernicus" in text:
            return dataset_id.startswith(("s1", "s2")) or "sentinel" in dataset_id
        if "landsat" in text or "usgs" in text or "nasa" in text:
            return dataset_id.startswith(("lc", "le", "lt")) or "landsat" in dataset_id
        if "terrasar" in text or "tsx" in text or "dlr" in text:
            return dataset_id.startswith("tsx") or "terrasar" in dataset_id
        return False

    @staticmethod
    def _relativize_related_response_link(item: Item, related_item: Item) -> None:
        for link in item.links:
            if link.rel == "related" and link.target is related_item and link.extra_fields.get("roles") == ["response"]:
                link.target = f"./{related_item.id}.json"

    def _hazard_items_for_types(self, hazard_items: List[Item], disaster_types: list[str]) -> list[Item]:
        selected: list[Item] = []
        canonical = set(self._canonical_codes_for_types(disaster_types))
        for hazard_item in hazard_items:
            hazard_codes = set(MontyExtension.ext(hazard_item).hazard_codes or [])
            if hazard_codes & canonical:
                selected.append(hazard_item)
        return selected

    def _add_response_item_links(
        self,
        item: Item,
        event_href: str,
        hazard_items: List[Item],
        disaster_types: list[str],
        web_href: Optional[str],
        activation_id: Any,
        dataset_items: tuple[Item, ...] = (),
    ) -> None:
        """Add ``related`` event/hazard/dataset links and the ``derived_from`` web link."""
        item.add_link(
            Link(rel="related", target=event_href, media_type="application/geo+json", extra_fields={"roles": ["event"]})
        )
        for hazard_item in self._hazard_items_for_types(hazard_items, disaster_types):
            item.add_link(
                Link(
                    rel="related",
                    target=self._relative_item_href("charter-hazards", hazard_item.id),
                    media_type="application/geo+json",
                    extra_fields={"roles": ["hazard"]},
                )
            )
        for dataset_item in dataset_items:
            item.add_link(
                Link(
                    rel="related",
                    target=f"./{dataset_item.id}.json",
                    media_type="application/geo+json",
                    extra_fields={"roles": ["response"]},
                )
            )
        if web_href:
            item.add_link(
                Link(
                    rel="derived_from",
                    target=web_href,
                    media_type="text/html",
                    title=f"International Charter activation Act-{activation_id}",
                )
            )

    def make_calibrated_dataset_response_items(
        self,
        activation: dict[str, Any],
        datasets: List[dict[str, Any]],
        event_item: Item,
        hazard_items: List[Item],
    ) -> List[Item]:
        """Create Response STAC items from calibrated dataset sidecars."""
        if not datasets:
            return []

        act_props = activation.get("properties", {})
        activation_types = self._as_disaster_type_list(act_props.get("disaster:type", []))
        call_ids = list(map(str, act_props.get("disaster:call_ids", [])))
        if not call_ids:
            return []

        event_monty = MontyExtension.ext(event_item)
        event_href = self._relative_item_href("charter-events", event_item.id)
        web_href = self._activation_web_href(activation)
        response_items: List[Item] = []

        for dataset in datasets:
            source_id = self._dataset_source_id(dataset)
            response_id = self._dataset_response_id(dataset)
            matched_call_id = self._matched_activation_call_id(dataset, call_ids)
            if not source_id or not response_id or not matched_call_id:
                continue

            dataset_doc = deepcopy(dataset)
            props = self._clean_response_properties(dataset_doc.setdefault("properties", {}))
            props["disaster:class"] = "acquisition"
            props["disaster:types"] = props.get("disaster:types", activation_types)
            props["disaster:country"] = act_props.get("disaster:country")
            props["roles"] = ["response", "source"]

            item = build_response_item(
                id=f"charter-response-{matched_call_id}-{response_id}",
                geometry=dataset_doc.get("geometry"),
                bbox=self._stac_bbox(dataset_doc, dataset_doc.get("geometry")),
                datetime=self._parse_datetime(props["datetime"]),
                correlation_id=event_monty.correlation_id,
                country_codes=list(event_monty.country_codes or []),
                hazard_codes=self._canonical_codes_for_types(props["disaster:types"]),
                type="eo-dat",
                source_id=source_id,
                producer=self._dataset_producer(dataset),
                sendai_targets=RESPONSE_TYPE_SENDAI["eo-dat"],
                properties=props,
            )
            if isinstance(props.get("datetime"), str):
                item.properties["datetime"] = props["datetime"]
                item.properties[MONTY_SOURCE_DATETIME_PROPERTY] = props["datetime"]
            item.stac_extensions = self._response_stac_extensions(dataset_doc)
            item.assets = {key: Asset.from_dict(asset) for key, asset in dataset_doc.get("assets", {}).items()}
            self._add_response_item_links(
                item, event_href, hazard_items, props["disaster:types"], web_href, act_props.get("disaster:activation_id")
            )
            item.set_collection(self.get_response_collection())
            response_items.append(item)

        return response_items

    def make_response_items(
        self,
        activation: dict[str, Any],
        vaps: List[dict[str, Any]],
        calibrated_dataset_items: List[Item],
        event_item: Item,
        hazard_items: List[Item],
    ) -> List[Item]:
        """Create Response STAC items from Charter VAP sidecars."""
        if not vaps:
            return []

        act_props = activation.get("properties", {})
        activation_id = act_props.get("disaster:activation_id")
        if not activation_id:
            return []

        safe_activation_id = sanitize_stac_item_id(str(activation_id))
        event_monty = MontyExtension.ext(event_item)
        event_href = self._relative_item_href("charter-events", event_item.id)
        web_href = self._activation_web_href(activation)
        items: List[Item] = []

        for vap in vaps:
            vap_props = vap.get("properties", {})
            api_sidecar = bool(vap.get("_charter_api_sidecar"))
            source_id = self._vap_source_id(vap)
            if not source_id:
                logger.warning("Skipping VAP without identifiable source_id: %s", vap.get("id"))
                continue

            item_id = f"charter-response-{safe_activation_id}-{source_id}"
            datetime_str = vap_props.get("datetime")
            if not datetime_str:
                continue
            dt = self._parse_datetime(datetime_str)

            title = vap_props.get("title", item_id)
            description = vap_props.get("additional_information") or vap_props.get("description", "")
            copyright_text = vap_props.get("copyright", "")

            response_type = self._infer_response_type(title, description)
            producer = self._infer_producer(copyright_text)
            resolution_class = self._infer_resolution_class(copyright_text)

            activation_types = self._as_disaster_type_list(act_props.get("disaster:type", []))
            hazard_types = self._hazard_types_for_product(title, description, activation_types)
            response_hazard_codes = self._canonical_codes_for_types(hazard_types)

            properties = {
                "title": title,
                "description": description,
                "disaster:class": "vap",
                "disaster:activation_id": activation_id,
                "disaster:call_ids": self._vap_call_ids(source_id),
                "disaster:country": act_props.get("disaster:country"),
                "disaster:types": hazard_types,
                "disaster:activation_status": act_props.get("cpe:activation_status"),
                "roles": ["response", "source"],
            }
            if api_sidecar:
                properties.pop("description", None)
                for key in ("updated", "created", "copyright", "additional_information", "vap_status", "version"):
                    if vap_props.get(key) is not None:
                        properties[key] = vap_props[key]
            if resolution_class:
                properties["disaster:resolution_class"] = resolution_class

            item = build_response_item(
                id=item_id,
                geometry=self._normalize_polygon_geometry(vap.get("geometry")),
                bbox=self._stac_bbox(vap, self._normalize_polygon_geometry(vap.get("geometry"))),
                datetime=dt,
                correlation_id=event_monty.correlation_id,
                country_codes=list(event_monty.country_codes or []),
                hazard_codes=response_hazard_codes,
                type=response_type,
                source_id=source_id,
                producer=producer,
                methodology=MontyMethodology.HUMAN_INTERPRETED,
                sendai_targets=RESPONSE_TYPE_SENDAI[response_type],
                properties=properties,
            )
            if isinstance(datetime_str, str):
                item.properties["datetime"] = datetime_str
                item.properties[MONTY_SOURCE_DATETIME_PROPERTY] = datetime_str
            item.stac_extensions = self._response_stac_extensions(vap) if api_sidecar else [SCHEMA_URI, DISASTER_SCHEMA_URI]

            if api_sidecar:
                item.assets = {key: Asset.from_dict(asset) for key, asset in vap.get("assets", {}).items()}
            else:
                hazard_keywords = self.hazard_profiles.get_keywords(response_hazard_codes)
                item.properties["keywords"] = sorted(
                    set(hazard_keywords + list(event_monty.country_codes or []) + ["ValueAddedProduct"])
                )

            related_datasets = tuple(
                dataset_item for dataset_item in calibrated_dataset_items if self._dataset_matches_vap(vap, dataset_item)
            )
            for dataset_item in related_datasets:
                link_related_response(item, dataset_item)
                self._relativize_related_response_link(item, dataset_item)
                self._relativize_related_response_link(dataset_item, item)

            self._add_response_item_links(
                item,
                event_href,
                hazard_items,
                hazard_types,
                web_href,
                activation_id,
            )

            item.set_collection(self.get_response_collection())
            items.append(item)

        return items

    def add_charter_related_links(
        self,
        event_item: Item,
        hazard_items: List[Item],
        response_items: List[Item],
    ) -> None:
        """Add bidirectional ``related`` links using relative example paths."""
        for hazard in hazard_items:
            event_item.add_link(
                Link(
                    rel="related",
                    target=self._relative_item_href("charter-hazards", hazard.id),
                    media_type="application/geo+json",
                    extra_fields={"roles": ["hazard"]},
                )
            )
            hazard.add_link(
                Link(
                    rel="related",
                    target=self._relative_item_href("charter-events", event_item.id),
                    media_type="application/geo+json",
                    extra_fields={"roles": ["event"]},
                )
            )
        for response in response_items:
            event_item.add_link(
                Link(
                    rel="related",
                    target=self._relative_item_href("charter-response", response.id),
                    media_type="application/geo+json",
                    extra_fields={"roles": ["response"]},
                )
            )

    def add_derived_from_links(self, event_item: Item, hazard_items: List[Item]) -> None:
        """Add derived_from links from hazard items to parent event item."""
        if not hazard_items:
            return

        event_href = self._relative_item_href("charter-events", event_item.id)

        for hazard_item in hazard_items:
            hazard_item.add_link(
                Link(rel="derived_from", target=event_href, media_type="application/json", title="Parent Charter Event")
            )

    def get_stac_items(self) -> Generator[Item, None, None]:
        """Generate STAC items from Charter activation data."""
        self.transform_summary.mark_as_started()
        self.transform_summary.increment_rows()

        try:
            event_item = self.make_event_item(self.data_source.activation_data)
            if not event_item:
                self.transform_summary.increment_failed_rows()
                self.transform_summary.mark_as_complete()
                return

            monty = MontyExtension.ext(event_item)
            hazard_items = self.make_hazard_items(
                self.data_source.activation_data, self.data_source.areas_data, monty.correlation_id
            )
            calibrated_dataset_response_items = self.make_calibrated_dataset_response_items(
                self.data_source.activation_data,
                self.data_source.calibrated_datasets_data,
                event_item,
                hazard_items,
            )
            response_items = self.make_response_items(
                self.data_source.activation_data,
                self.data_source.vaps_data,
                calibrated_dataset_response_items,
                event_item,
                hazard_items,
            )
            response_items.extend(calibrated_dataset_response_items)

            self.add_charter_related_links(event_item, hazard_items, response_items)
            self.add_derived_from_links(event_item, hazard_items)

            yield event_item
            for hazard_item in hazard_items:
                yield hazard_item
            for response_item in response_items:
                yield response_item

        except Exception:
            self.transform_summary.increment_failed_rows()
            logger.warning("Failed to process Charter activation", exc_info=True)

        self.transform_summary.mark_as_complete()

    def make_items(self) -> list[Item]:
        """Deprecated: use get_stac_items()"""
        return list(self.get_stac_items())


# --- batch export (CLI) ---

_CHARTER_BATCH = BatchExportConfig(
    source_slug="charter",
    provider=_CHARTER_PROVIDER,
    titles={
        "event": (
            "Charter Source Events",
            "International Charter Space and Major Disasters activation events providing satellite imagery for disaster response",
        ),
        "hazard": (
            "Charter Source Hazards",
            "Areas of Interest (AOI) from Charter activations representing hazard extents for satellite imagery acquisition",
        ),
        "response": (
            "Charter Source Response",
            (
                "International Charter Value-Added Products (map products) and calibrated satellite acquisition datasets "
                "(eo-dat), mapped to Monty Response items."
            ),
        ),
    },
)


def _load_json(path: Path) -> dict[str, Any]:
    data: Any = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def _local_json_for_link(api_dir: Path, href: str) -> Optional[Path]:
    name = href.rstrip("/").split("/")[-1]
    candidates = [
        api_dir / name,
        api_dir / name.removesuffix(".json") / name,
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _item_links(doc: dict[str, Any]) -> list[dict[str, Any]]:
    return [link for link in doc.get("links", []) if link.get("rel") == "item" and link.get("href")]


def _load_areas(source_dir: Path, api_dir: Path, act_id: str, activation_id: Any) -> list[dict[str, Any]]:
    api_areas = api_dir / f"activation-{activation_id}-areas.json"
    if api_areas.is_file():
        return list(_load_json(api_areas).get("features", []))
    return [_load_json(p) for p in sorted(source_dir.glob(f"{act_id}-area-*.json"))]


def _load_vaps(source_dir: Path, api_dir: Path, act_id: str) -> list[dict[str, Any]]:
    listing = api_dir / f"{act_id}-vaps.json"
    if listing.is_file():
        vaps: list[dict[str, Any]] = []
        missing = 0
        for link in _item_links(_load_json(listing)):
            path = _local_json_for_link(api_dir, str(link["href"]))
            if path is None:
                missing += 1
                continue
            vap = _load_json(path)
            vap["_charter_api_sidecar"] = True
            vaps.append(vap)
        if missing:
            logger.warning(
                "Charter %s VAP listing references %d item bodies not present under %s; exported responses are fixture-bounded",
                act_id,
                missing,
                api_dir,
            )
        return vaps

    vap_paths = list(source_dir.glob(f"{act_id}-vap-*.json"))
    if api_dir.is_dir():
        vap_paths.extend(api_dir.glob(f"{act_id}-vap-*.json"))
    vaps = []
    for path in sorted(vap_paths):
        vap = _load_json(path)
        if path.parent == api_dir:
            vap["_charter_api_sidecar"] = True
        vaps.append(vap)
    return vaps


def _load_calibrated_datasets(api_dir: Path, call_ids: list[str]) -> list[dict[str, Any]]:
    datasets: list[dict[str, Any]] = []
    seen: set[str] = set()
    for call_id in call_ids:
        listing = api_dir / f"call-{call_id}-calibratedDatasets.json"
        if not listing.is_file():
            continue
        missing = 0
        for link in _item_links(_load_json(listing)):
            path = _local_json_for_link(api_dir, str(link["href"]))
            if path is None:
                missing += 1
                continue
            if str(path) in seen:
                continue
            seen.add(str(path))
            dataset = _load_json(path)
            if CharterTransformer._matched_activation_call_id(dataset, call_ids) is not None:
                datasets.append(dataset)
        if missing:
            logger.warning(
                "Charter call-%s calibrated dataset listing references %d item bodies not present under %s; "
                "exported responses are fixture-bounded",
                call_id,
                missing,
                api_dir,
            )
    return datasets


def iter_charter_stac_items(source_dir: Path) -> Generator[Item, None, None]:
    """Yield STAC items for each ``act-*-activation.json`` under *source_dir* and sidecars."""
    from pystac_monty.sources.batch_export import use_local_collection_examples

    use_local_collection_examples()
    api_dir = source_dir / "api-files"
    for act_path in sorted(source_dir.glob("act-*-activation.json")):
        act_id = act_path.stem.replace("-activation", "")
        data = _load_json(act_path)
        activation_id = data.get("properties", {}).get("disaster:activation_id", act_id.replace("act-", ""))
        data["areas"] = _load_areas(source_dir, api_dir, act_id, activation_id)
        data["vaps"] = _load_vaps(source_dir, api_dir, act_id)
        call_ids = list(map(str, data.get("properties", {}).get("disaster:call_ids", [])))
        data["calibrated_datasets"] = _load_calibrated_datasets(api_dir, call_ids) if api_dir.is_dir() else []
        source = CharterDataSource(
            data=GenericDataSource(
                source_url=f"{CHARTER_API_BASE}/activations/{act_id}",
                input_data=Memory(content=data, data_type=DataType.MEMORY),
            )
        )
        yield from CharterTransformer(source, None).get_stac_items()


def convert_charter_activations(source_dir: Path, output_dir: Path, public_href_base: str | None = None) -> None:
    """Read ``act-*-activation.json`` plus matching area/VAP sidecars from *source_dir*.

    The Charter model directory in the ``monty-stac-extension`` submodule
    (``docs/model/sources/Charter``) is a valid *source_dir* layout for end-to-end checks.
    """
    config = replace(_CHARTER_BATCH, public_href_base=public_href_base)
    log_batch_role_counts(*export_collected_items(config, list(iter_charter_stac_items(source_dir)), output_dir))


def regenerate_charter_examples(source_dir: Path, output_dir: Path) -> None:
    """Regenerate published Charter examples with absolute public self/collection links."""
    convert_charter_activations(source_dir, output_dir, public_href_base=MONTY_STAC_EXAMPLES_BASE_URL)
