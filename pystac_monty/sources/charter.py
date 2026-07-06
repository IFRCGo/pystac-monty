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
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, List, Optional, Sequence, cast

import pytz
from pystac import Asset, Collection, Item, Link
from pystac.provider import Provider, ProviderRole

from pystac_monty.exporter import MONTY_STAC_EXAMPLES_BASE_URL, BatchExportConfig, export_collected_items, log_batch_role_counts
from pystac_monty.extension import SCHEMA_URI, HazardDetail, MontyEstimateType, MontyExtension
from pystac_monty.geocoding import MontyGeoCoder
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import (
    DataType,
    GenericDataSource,
    Memory,
    MontyDataSourceV3,
    MontyDataTransformer,
    sanitize_stac_item_id,
)

logger = logging.getLogger(__name__)

CHARTER_API_BASE = "https://supervisor.disasterscharter.org/api"
DISASTER_SCHEMA_URI = "https://terradue.github.io/stac-extensions-disaster/v1.1.0/schema.json"

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

# CPE status to estimate_type (interim mapping)
CPE_STATUS_MAPPING = {
    "notificationNew": "primary",
    "notificationImported": "primary",
    "readyToDeliver": "secondary",
    "readyToArchive": "secondary",
}

RESPONSE_TYPE_SENDAI: dict[str, list[str]] = {
    "eo-dat": ["D", "G"],
    "eo-gra": ["C", "D"],
    "eo-del": ["C"],
    "eo-pop": ["C"],
    "eo-vap": ["C"],
}

CHARTER_EXAMPLE_VAP_SOURCE_IDS: dict[str, set[str]] = {"act-1019": {"1166-19"}}
CHARTER_EXAMPLE_CALIBRATED_DATASET_IDS: dict[str, set[str]] = {
    "act-1019": {"DS_PHR1A_202603021304008_FR1_PX_W044S22_0907_00777-calibrated"}
}
CHARTER_RESPONSE_EXAMPLE_COLLECTION_OVERRIDES = {
    "keywords": [
        "AFG",
        "BRA",
        "Earthquake",
        "Flood",
        "Landslide",
        "Geological",
        "Seismic",
        "ValueAddedProduct",
        "Acquisition",
    ],
    "extent": {
        "spatial": {
            "bbox": [
                [-43.363, -21.906, 67.799, 36.727],
                [67.642, 36.559, 67.799, 36.727],
                [-43.363, -21.906, -43.275, -21.728],
            ]
        },
        "temporal": {"interval": [["2025-11-11T08:00:29Z", "2026-03-10T19:47:05Z"]]},
    },
    "summaries": {
        "roles": ["response", "source"],
        "monty:country_codes": ["AFG", "BRA"],
        "monty:hazard_codes": [
            "EQ",
            "GH0101",
            "nat-geo-ear-gro",
            "FL",
            "MH0600",
            "nat-hyd-flo-flo",
            "LS",
            "MH0901",
            "nat-geo-mmd-lan",
        ],
        "datetime": {"minimum": "2025-11-11T08:00:29Z", "maximum": "2026-03-10T19:47:05Z"},
    },
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

    activation_data: dict
    areas_data: List[dict]
    vaps_data: List[dict]
    calibrated_datasets_data: List[dict]

    def __init__(self, data: GenericDataSource, eoapi_url: Optional[str] = None):
        super().__init__(root=data, eoapi_url=eoapi_url)

        if data.input_data.data_type == DataType.MEMORY:
            self.activation_data = data.input_data.content
            self.areas_data = data.input_data.content.get("areas", [])
            self.vaps_data = data.input_data.content.get("vaps", [])
            self.calibrated_datasets_data = data.input_data.content.get("calibrated_datasets", [])
        elif data.input_data.data_type == DataType.FILE:
            with open(data.input_data.path, "r") as f:
                file_data = json.load(f)
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
        super().__init__(data_source, cast(MontyGeoCoder, geocoder))
        self._charter_response_collection_cache: Collection | None = None
        ext_root = Path(__file__).resolve().parents[2] / "monty-stac-extension"
        ev = ext_root / "examples" / "charter-events" / "charter-events.json"
        if ev.is_file():
            self.events_collection_url = str(ev)
            self.hazards_collection_url = str(ext_root / "examples" / "charter-hazards" / "charter-hazards.json")
        rsp = ext_root / "examples" / "charter-response" / "charter-response.json"
        self._charter_response_collection_url = (
            str(rsp) if rsp.is_file() else f"{MontyDataTransformer.base_collection_url}/charter-response/charter-response.json"
        )

    def get_response_collection(self) -> Collection:
        """Collection for Charter response items (``charter-response``)."""
        if self._charter_response_collection_cache is None:
            url = self._charter_response_collection_url
            if url.startswith("http"):
                import requests

                collection_dict = json.loads(requests.get(url, timeout=60).text)
            else:
                with open(url, encoding="utf-8") as f:
                    collection_dict = json.load(f)
            collection = Collection.from_dict(collection_dict)
            collection.set_self_href(url)
            self._charter_response_collection_cache = collection
        return self._charter_response_collection_cache

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
                datetime=datetime.datetime(2020, 1, 1, tzinfo=pytz.utc),
                properties={"monty:hazard_codes": list(CHARTER_HAZARD_CODES[dtype])},
            )
            MontyExtension.add_to(stub)
            merged.extend(self.hazard_profiles.get_canonical_hazard_codes(stub))
        return merged

    def parse_area_description(self, description: str) -> tuple[Optional[float], Optional[int], Optional[float]]:
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

    def make_event_item(self, activation: dict) -> Optional[Item]:
        """Create Event STAC item from Charter activation."""
        props = activation.get("properties", {})

        activation_id = props.get("disaster:activation_id")
        if not activation_id:
            logger.warning("Missing disaster:activation_id")
            return None

        disaster_types = props.get("disaster:type", [])
        if not disaster_types or disaster_types == ["other"]:
            logger.warning(f"Skipping activation {activation_id}: no valid disaster types")
            return None

        geom = activation.get("geometry")
        if not geom:
            logger.warning(f"Missing geometry for activation {activation_id}")
            return None

        datetime_str = props.get("datetime")
        if not datetime_str:
            logger.warning(f"Missing datetime for activation {activation_id}")
            return None

        if isinstance(datetime_str, str):
            dt = pytz.utc.localize(datetime.datetime.fromisoformat(datetime_str.replace("Z", "")))
        else:
            dt = pytz.utc.localize(datetime_str)

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

    def make_hazard_items(self, activation: dict, areas: List[dict], event_corr_id: str) -> List[Item]:
        """Create Hazard STAC items from Charter areas (one per area per disaster:type)."""
        items: List[Item] = []
        used_hazard_ids: set[str] = set()
        props = activation.get("properties", {})
        activation_id = props.get("disaster:activation_id")
        datetime_str = props.get("datetime")
        activation_types = props.get("disaster:type", [])
        country = props.get("disaster:country")

        if isinstance(datetime_str, str):
            dt = pytz.utc.localize(datetime.datetime.fromisoformat(datetime_str.replace("Z", "")))
        else:
            dt = pytz.utc.localize(datetime_str)

        safe_activation_id = sanitize_stac_item_id(str(activation_id)) if activation_id else "x"

        for area in areas:
            area_props = area.get("properties", {})
            area_id = area.get("id", "unknown")
            area_title = area_props.get("title", "Unknown Area")
            area_geom = area.get("geometry")

            if not area_geom:
                continue

            description = area_props.get("description", "")
            radius, _priority, surface_area = self.parse_area_description(description)

            cpe_status = area_props.get("cpe:status", {})
            stage = cpe_status.get("stage", "notificationNew")
            estimate_type = CPE_STATUS_MAPPING.get(stage, "primary")

            area_types = area_props.get("disaster:type") or activation_types
            for dtype in area_types:
                if dtype not in CHARTER_HAZARD_CODES:
                    continue

                area_slug = sanitize_stac_item_id(area_id.split("/")[-1].replace(".json", "").lower())
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
                    bbox=area.get("bbox"),
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
                monty.hazard_codes = CHARTER_HAZARD_CODES[dtype]
                monty.hazard_codes = self.hazard_profiles.get_canonical_hazard_codes(item)
                monty.correlation_id = event_corr_id

                if radius is not None:
                    monty.hazard_detail = HazardDetail(
                        severity_value=radius,
                        severity_unit="km",
                        severity_label="Area radius",
                        estimate_type=MontyEstimateType(estimate_type),
                    )
                elif surface_area is not None:
                    monty.hazard_detail = HazardDetail(
                        severity_value=surface_area,
                        severity_unit="km2",
                        severity_label="Surface area",
                        estimate_type=MontyEstimateType(estimate_type),
                    )

                hazard_keywords = self.hazard_profiles.get_keywords(monty.hazard_codes)
                item.properties["keywords"] = sorted(set(hazard_keywords + monty.country_codes))

                item.set_collection(self.get_hazard_collection())

                items.append(item)

        return items

    @staticmethod
    def _activation_web_href(activation: dict) -> Optional[str]:
        for link in activation.get("links", []):
            if link.get("rel") == "about" and link.get("href"):
                return str(link["href"])
        return None

    @staticmethod
    def _vap_source_id(vap: dict) -> Optional[str]:
        cos2_id = vap.get("properties", {}).get("cpe:cos2_id") or vap.get("id", "")
        match = re.search(r"vap-(\d+-\d+)$", str(cos2_id))
        if match:
            return match.group(1)
        vap_id = str(vap.get("id", ""))
        match = re.search(r"vap-(\d+-\d+)$", vap_id)
        return match.group(1) if match else None

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
        if "inpe" in copyright.lower():
            return "INPE"
        if "airbus" in copyright.lower():
            return "Airbus"
        return None

    @staticmethod
    def _infer_resolution_class(copyright: str) -> Optional[str]:
        if copyright and ("pleiades" in copyright.lower() or "airbus" in copyright.lower()):
            return "VHR"
        return None

    @staticmethod
    def _normalize_polygon_geometry(geom: Optional[dict]) -> Optional[dict]:
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
    def _dataset_source_id(dataset: dict) -> str:
        return str(dataset.get("id", ""))

    @staticmethod
    def _matches_activation_call_ids(dataset: dict, call_ids: set[str]) -> bool:
        dataset_call_ids = dataset.get("properties", {}).get("disaster:call_ids", [])
        return bool(call_ids and set(map(str, dataset_call_ids)) & call_ids)

    @staticmethod
    def _dataset_response_id(dataset: dict) -> Optional[str]:
        source_id = CharterTransformer._dataset_source_id(dataset)
        if not source_id:
            return None
        match = re.match(r"DS_([A-Za-z0-9]+)_.*_([^_]+)_([^_]+)-calibrated$", source_id)
        if match:
            instrument, strip_id, scene_id = match.groups()
            return f"{sanitize_stac_item_id(instrument.lower())}-{strip_id.lower()}-{scene_id.lower()}"
        return None

    @staticmethod
    def _dataset_producer(dataset: dict) -> Optional[str]:
        for provider in dataset.get("properties", {}).get("providers", []):
            name = provider.get("name")
            if isinstance(name, str) and name:
                return name
        return None

    @staticmethod
    def _response_types_for_product(title: str, description: str, activation_types: list[str]) -> list[str]:
        text = f"{title} {description}".lower()
        if "landslide" in text or "geological risk" in text:
            return ["landslide"] if "landslide" in activation_types else activation_types[:1]
        if "flood" in text:
            return ["flood"] if "flood" in activation_types else activation_types[:1]
        return activation_types[:1]

    def _hazard_items_for_types(self, hazard_items: List[Item], disaster_types: list[str]) -> list[Item]:
        selected: list[Item] = []
        canonical = set(self._canonical_codes_for_types(disaster_types))
        for hazard_item in hazard_items:
            hazard_codes = set(MontyExtension.ext(hazard_item).hazard_codes or [])
            if hazard_codes & canonical:
                selected.append(hazard_item)
        return selected

    def make_calibrated_dataset_response_items(
        self,
        activation: dict,
        datasets: List[dict],
        event_item: Item,
        hazard_items: List[Item],
    ) -> List[Item]:
        """Create Response STAC items from calibrated dataset sidecars."""
        if not datasets:
            return []

        act_props = activation.get("properties", {})
        activation_types = act_props.get("disaster:type", [])
        call_ids = act_props.get("disaster:call_ids", [])
        if not call_ids:
            return []

        event_monty = MontyExtension.ext(event_item)
        event_href = self._relative_item_href("charter-events", event_item.id)
        web_href = self._activation_web_href(activation)
        response_items: List[Item] = []

        for dataset in datasets:
            source_id = self._dataset_source_id(dataset)
            response_id = self._dataset_response_id(dataset)
            if not source_id or not response_id:
                continue

            dataset_doc = deepcopy(dataset)
            props = dataset_doc.setdefault("properties", {})
            props.pop("cpe:status", None)
            props.pop("cpe:notified", None)
            props.pop("cpe:cos2_xml", None)
            props.pop("cpe:cos2_id", None)
            props.pop("cpe:processing_monitoring_id", None)
            props["disaster:class"] = "acquisition"
            props["disaster:types"] = props.pop("disaster:type", activation_types)
            props["disaster:country"] = act_props.get("disaster:country")
            props["roles"] = ["response", "source"]
            props["monty:country_codes"] = list(event_monty.country_codes or [])
            props["monty:hazard_codes"] = self._canonical_codes_for_types(props["disaster:types"])
            props["monty:corr_id"] = event_monty.correlation_id
            props["monty:response_detail"] = {
                "type": "eo-dat",
                "source_id": source_id,
                "producer": self._dataset_producer(dataset),
                "sendai_targets": RESPONSE_TYPE_SENDAI["eo-dat"],
            }

            stac_extensions = [
                SCHEMA_URI,
                *[
                    uri
                    for uri in dataset_doc.get("stac_extensions", [])
                    if uri != "https://terradue.github.io/disaster/v1.0.0/schema.json"
                ],
                DISASTER_SCHEMA_URI,
            ]
            item = Item(
                id=f"charter-response-{call_ids[0]}-{response_id}",
                geometry=dataset_doc.get("geometry"),
                bbox=dataset_doc.get("bbox"),
                datetime=datetime.datetime.fromisoformat(str(props["datetime"]).replace("Z", "+00:00")),
                properties=props,
                stac_extensions=stac_extensions,
                assets={key: Asset.from_dict(asset) for key, asset in dataset_doc.get("assets", {}).items()},
            )
            item.add_link(
                Link(rel="related", target=event_href, media_type="application/geo+json", extra_fields={"roles": ["event"]})
            )
            for hazard_item in self._hazard_items_for_types(hazard_items, props["disaster:types"]):
                item.add_link(
                    Link(
                        rel="related",
                        target=self._relative_item_href("charter-hazards", hazard_item.id),
                        media_type="application/geo+json",
                        extra_fields={"roles": ["hazard"]},
                    )
                )
            if web_href:
                item.add_link(
                    Link(
                        rel="derived_from",
                        target=web_href,
                        media_type="text/html",
                        title=f"International Charter activation Act-{act_props.get('disaster:activation_id')}",
                    )
                )
            item.set_collection(self.get_response_collection())
            response_items.append(item)

        return response_items

    def make_response_items(
        self,
        activation: dict,
        vaps: List[dict],
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
            if isinstance(datetime_str, str):
                dt = pytz.utc.localize(datetime.datetime.fromisoformat(datetime_str.replace("Z", "")))
            else:
                dt = pytz.utc.localize(datetime_str)

            title = vap_props.get("title", item_id)
            description = vap_props.get("additional_information") or vap_props.get("description", "")
            copyright_text = vap_props.get("copyright", "")

            response_type = self._infer_response_type(title, description)
            producer = self._infer_producer(copyright_text)
            resolution_class = self._infer_resolution_class(copyright_text)

            activation_types = act_props.get("disaster:type", [])
            response_types = self._response_types_for_product(title, description, activation_types)
            response_hazard_codes = self._canonical_codes_for_types(response_types)

            item = Item(
                id=item_id,
                geometry=self._normalize_polygon_geometry(vap.get("geometry")),
                bbox=vap.get("bbox"),
                datetime=dt,
                properties={
                    "title": title,
                    "description": description,
                    "disaster:class": "vap",
                    "disaster:activation_id": activation_id,
                    "disaster:call_ids": act_props.get("disaster:call_ids", []),
                    "disaster:country": act_props.get("disaster:country"),
                    "disaster:types": response_types,
                    "disaster:activation_status": act_props.get("cpe:activation_status"),
                },
            )
            if api_sidecar:
                item.properties.pop("description", None)
            for key in ("updated", "created", "copyright", "additional_information", "vap_status", "version"):
                if api_sidecar and vap_props.get(key) is not None:
                    item.properties[key] = vap_props[key]
            if resolution_class:
                item.properties["disaster:resolution_class"] = resolution_class

            item.properties["roles"] = ["response", "source"]
            item.stac_extensions = [SCHEMA_URI, DISASTER_SCHEMA_URI]
            if api_sidecar:
                item.stac_extensions = [
                    SCHEMA_URI,
                    *[
                        uri
                        for uri in vap.get("stac_extensions", [])
                        if uri != "https://terradue.github.io/disaster/v1.0.0/schema.json"
                    ],
                    DISASTER_SCHEMA_URI,
                ]

            MontyExtension.add_to(item)
            monty = MontyExtension.ext(item)
            monty.country_codes = list(event_monty.country_codes or [])
            monty.hazard_codes = response_hazard_codes
            monty.correlation_id = event_monty.correlation_id

            response_detail: dict[str, Any] = {
                "type": response_type,
                "source_id": source_id,
                "methodology": "human_interpreted",
                "sendai_targets": RESPONSE_TYPE_SENDAI.get(response_type, ["C"]),
            }
            if producer:
                response_detail["producer"] = producer
            item.properties["monty:response_detail"] = response_detail

            hazard_keywords = self.hazard_profiles.get_keywords(response_hazard_codes)
            item.properties["keywords"] = sorted(set(hazard_keywords + monty.country_codes + ["ValueAddedProduct"]))
            if api_sidecar:
                item.assets = {key: Asset.from_dict(asset) for key, asset in vap.get("assets", {}).items()}
                item.properties.pop("keywords", None)

            item.add_link(
                Link(
                    rel="related",
                    target=event_href,
                    media_type="application/geo+json",
                    extra_fields={"roles": ["event"]},
                )
            )
            for hazard_item in self._hazard_items_for_types(hazard_items, response_types):
                item.add_link(
                    Link(
                        rel="related",
                        target=self._relative_item_href("charter-hazards", hazard_item.id),
                        media_type="application/geo+json",
                        extra_fields={"roles": ["hazard"]},
                    )
                )
            for dataset_item in calibrated_dataset_items:
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

    def get_stac_items(self):
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
    emit_empty_response_collection=False,
)

_CHARTER_EXAMPLES_BATCH = BatchExportConfig(
    source_slug="charter",
    provider=_CHARTER_PROVIDER,
    titles=_CHARTER_BATCH.titles,
    emit_empty_response_collection=False,
    public_href_base=MONTY_STAC_EXAMPLES_BASE_URL,
    omit_keywords_from_summaries=True,
)


def _export_charter_items(config: BatchExportConfig, items: Sequence[Item], output_dir: Path) -> None:
    log_batch_role_counts(*export_collected_items(config, items, output_dir))


def _finalize_charter_example_collections(output_dir: Path) -> None:
    for collection_path in output_dir.glob("charter-*/charter-*.json"):
        doc = json.loads(collection_path.read_text(encoding="utf-8"))
        if doc.get("type") != "Collection":
            continue
        doc["stac_version"] = "1.1.0"
        for link in doc.get("links", []):
            if isinstance(link, dict) and link.get("rel") == "self":
                link.pop("type", None)
        if doc.get("id") == "charter-response":
            doc.update(CHARTER_RESPONSE_EXAMPLE_COLLECTION_OVERRIDES)
        collection_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _compact_datetime(value: Any) -> Any:
    if isinstance(value, str) and re.search(r"\.\d+Z$", value):
        return re.sub(r"(\.\d*?[1-9])0+Z$", r"\1Z", value)
    return value


def _finalize_charter_example_items(output_dir: Path) -> None:
    for item_path in output_dir.glob("charter-*/*.json"):
        doc = json.loads(item_path.read_text(encoding="utf-8"))
        if doc.get("type") != "Feature":
            continue
        properties = doc.get("properties", {})
        if isinstance(properties, dict):
            for key in ("datetime", "start_datetime", "end_datetime"):
                if key in properties:
                    properties[key] = _compact_datetime(properties[key])
        item_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def iter_charter_stac_items(source_dir: Path, *, example_subset: bool = False) -> Iterator[Item]:
    """Yield STAC items for each ``act-*-activation.json`` under *source_dir* and sidecars."""
    api_dir = source_dir / "api-files"
    for act_path in sorted(source_dir.glob("act-*-activation.json")):
        act_id = act_path.stem.replace("-activation", "")
        data = json.loads(act_path.read_text(encoding="utf-8"))
        data["areas"] = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(source_dir.glob(f"{act_id}-area-*.json"))]
        vap_paths = list(source_dir.glob(f"{act_id}-vap-*.json"))
        if api_dir.is_dir():
            api_vap_paths = sorted(api_dir.glob(f"{act_id}-vap-*.json"))
            if example_subset:
                wanted_vaps = CHARTER_EXAMPLE_VAP_SOURCE_IDS.get(act_id, set())
                api_vap_paths = [path for path in api_vap_paths if path.stem.removeprefix(f"{act_id}-vap-") in wanted_vaps]
            vap_paths.extend(api_vap_paths)
        data["vaps"] = []
        for path in sorted(vap_paths):
            vap = json.loads(path.read_text(encoding="utf-8"))
            if api_dir.is_dir() and path.parent == api_dir:
                vap["_charter_api_sidecar"] = True
            data["vaps"].append(vap)
        data["calibrated_datasets"] = []
        if api_dir.is_dir():
            if example_subset:
                dataset_paths = [
                    api_dir / f"{dataset_id}.json"
                    for dataset_id in sorted(CHARTER_EXAMPLE_CALIBRATED_DATASET_IDS.get(act_id, set()))
                ]
            else:
                dataset_paths = sorted(api_dir.glob("*-calibrated.json"))
            call_ids = set(map(str, data.get("properties", {}).get("disaster:call_ids", [])))
            for path in dataset_paths:
                if not path.is_file():
                    continue
                dataset = json.loads(path.read_text(encoding="utf-8"))
                if example_subset or CharterTransformer._matches_activation_call_ids(dataset, call_ids):
                    data["calibrated_datasets"].append(dataset)
        source = CharterDataSource(
            data=GenericDataSource(
                source_url=f"{CHARTER_API_BASE}/activations/{act_id}",
                input_data=Memory(content=data, data_type=DataType.MEMORY),
            )
        )
        yield from CharterTransformer(source, None).get_stac_items()


def convert_charter_activations(source_dir: Path, output_dir: Path) -> None:
    """Read ``act-*-activation.json`` plus matching area/VAP sidecars from *source_dir*.

    The Charter model directory in the ``monty-stac-extension`` submodule
    (``docs/model/sources/Charter``) is a valid *source_dir* layout for end-to-end checks.
    """
    _export_charter_items(_CHARTER_BATCH, list(iter_charter_stac_items(source_dir)), output_dir)


def convert_charter_example_activations(source_dir: Path, output_dir: Path) -> None:
    """Regenerate the published Charter example layout from the upstream model fixtures."""
    items = list(iter_charter_stac_items(source_dir, example_subset=True))
    _export_charter_items(_CHARTER_EXAMPLES_BATCH, items, output_dir)
    _finalize_charter_example_items(output_dir)
    _finalize_charter_example_collections(output_dir)
