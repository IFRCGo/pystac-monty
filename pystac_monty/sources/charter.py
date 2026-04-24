"""Charter STAC source transformer

Transforms International Charter on Space and Major Disasters data into Monty STAC items.
Batch export uses :mod:`pystac_monty.exporter`;
this module provides :func:`convert_charter_activations` and :func:`iter_charter_stac_items`.

**Response items** (Charter acquisitions, calibrated datasets, VAPs) are not produced here: they require
partner/S3 or API access not available in open ingestion. Batch export still writes an empty
``charter-response`` collection so layout matches the ``monty-stac-extension`` examples; use
:meth:`CharterTransformer.get_response_collection` when implementing response items later.
"""

import datetime
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Optional, cast

import pytz
from pystac import Collection, Item, Link
from pystac.provider import Provider, ProviderRole

from pystac_monty.exporter import MONTY_STAC_EXAMPLES_BASE_URL, BatchExportConfig, export_collected_items_and_log
from pystac_monty.extension import HazardDetail, MontyEstimateType, MontyExtension
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
    "readyToDeliver": "secondary",
    "readyToArchive": "secondary",
}


@dataclass
class CharterDataSource(MontyDataSourceV3):
    """Charter data source containing activation and area data"""

    activation_data: dict
    areas_data: List[dict]

    def __init__(self, data: GenericDataSource, eoapi_url: Optional[str] = None):
        super().__init__(root=data, eoapi_url=eoapi_url)

        if data.input_data.data_type == DataType.MEMORY:
            self.activation_data = data.input_data.content
            self.areas_data = data.input_data.content.get("areas", [])
        elif data.input_data.data_type == DataType.FILE:
            with open(data.input_data.path, "r") as f:
                file_data = json.load(f)
                self.activation_data = file_data
                self.areas_data = file_data.get("areas", [])


class CharterTransformer(MontyDataTransformer[CharterDataSource]):
    """Transforms Charter activation data into STAC Items

    Following Charter mapping specification:
    - Activation → Event item
    - Area → Hazard item(s) (one per disaster:type for multi-hazard)
    """

    hazard_profiles = MontyHazardProfiles()
    source_name = "charter"

    def __init__(self, data_source: CharterDataSource, geocoder: MontyGeoCoder | None = None) -> None:
        # Charter does not use self.geocoder; the base class still records it. Pass None.
        super().__init__(data_source, cast(MontyGeoCoder, geocoder))
        self._charter_response_collection_cache: Collection | None = None
        ext_root = Path(__file__).resolve().parents[2] / "monty-stac-extension"
        ev = ext_root / "examples" / "charter-events" / "charter-events.json"
        if ev.is_file():
            self.events_collection_url = str(ev)
            self.hazards_collection_url = str(ext_root / "examples" / "charter-hazards" / "charter-hazards.json")
        rsp = ext_root / "charter-response.json"
        _root_url = MontyDataTransformer.base_collection_url.removesuffix("/examples")
        self._charter_response_collection_url = str(rsp) if rsp.is_file() else f"{_root_url}/charter-response.json"

    def get_response_collection(self) -> Collection:
        """Collection for Charter response items (``charter-response``); use when adding acquisition/dataset/VAP items."""
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

    def get_hazard_codes(self, disaster_types: List[str]) -> List[str]:
        """Map Charter disaster:type values to Monty hazard codes

        Args:
            disaster_types: List of Charter disaster type strings (e.g., ['flood', 'earthquake'])

        Returns:
            List of hazard codes [UNDRR-2025, EM-DAT, GLIDE] for all disaster types
        """
        codes = []
        for dtype in disaster_types:
            if dtype in CHARTER_HAZARD_CODES:
                codes.extend(CHARTER_HAZARD_CODES[dtype])
            else:
                logger.warning(f"Charter disaster type '{dtype}' not found in mapping")
        return codes

    def parse_area_description(self, description: str) -> tuple[Optional[float], Optional[int]]:
        """Parse radius and priority from area description

        Args:
            description: Area description text (e.g., "Radius (km): 10.0\nPriority: 1")

        Returns:
            Tuple of (radius_km, priority) where both may be None if not found
        """
        radius = None
        priority = None

        if not description:
            return radius, priority

        radius_match = re.search(r"Radius\s*\(km\)\s*:\s*(\d+\.?\d*)", description, re.IGNORECASE)
        if radius_match:
            radius = float(radius_match.group(1))

        priority_match = re.search(r"Priority\s*:\s*(\d+)", description, re.IGNORECASE)
        if priority_match:
            priority = int(priority_match.group(1))

        return radius, priority

    def make_event_item(self, activation: dict) -> Optional[Item]:
        """Create Event STAC item from Charter activation

        Args:
            activation: Charter activation dict with properties and geometry

        Returns:
            STAC Item with event role, or None if validation fails
        """
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

        monty.hazard_codes = self.get_hazard_codes(disaster_types)
        monty.hazard_codes = self.hazard_profiles.get_canonical_hazard_codes(item)

        monty.episode_number = 1
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        hazard_keywords = self.hazard_profiles.get_keywords(monty.hazard_codes)
        item.properties["keywords"] = list(set(hazard_keywords + monty.country_codes))

        item.add_link(Link("via", f"{CHARTER_API_BASE}/activations/act-{activation_id}", "application/json"))

        item.set_collection(self.get_event_collection())

        return item

    def make_hazard_items(self, activation: dict, areas: List[dict], event_corr_id: str) -> List[Item]:
        """Create Hazard STAC items from Charter areas

        Multi-hazard strategy: one Hazard item per area per disaster:type

        Args:
            activation: Charter activation dict with properties
            areas: List of area dicts with geometry and properties
            event_corr_id: Correlation ID from parent event item

        Returns:
            List of STAC Items with hazard role (one per disaster type per area)
        """
        items: List[Item] = []
        used_hazard_ids: set[str] = set()
        props = activation.get("properties", {})
        activation_id = props.get("disaster:activation_id")
        datetime_str = props.get("datetime")
        disaster_types = props.get("disaster:type", [])
        country = props.get("disaster:country")

        # Parse datetime
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

            # Parse area description
            description = area_props.get("description", "")
            radius, priority = self.parse_area_description(description)

            # Get CPE status and map to estimate_type
            cpe_status = area_props.get("cpe:status", {})
            stage = cpe_status.get("stage", "notificationNew")
            estimate_type = CPE_STATUS_MAPPING.get(stage, "primary")

            # Create one hazard item per disaster type
            for dtype in disaster_types:
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
                    detail = HazardDetail(
                        severity_value=radius,
                        severity_unit="km",
                        severity_label="Area radius",
                        estimate_type=MontyEstimateType(estimate_type),
                    )
                    monty.hazard_detail = detail

                if priority is not None:
                    item.properties["charter:area_priority"] = priority

                hazard_keywords = self.hazard_profiles.get_keywords(monty.hazard_codes)
                item.properties["keywords"] = list(set(hazard_keywords + monty.country_codes))

                item.set_collection(self.get_hazard_collection())

                items.append(item)

        return items

    def set_charter_item_hrefs(self, items: List[Item]) -> None:
        """Set each item's ``self`` href to the published static-examples URL (STAC 1.1.0 IRI).

        Using the same layout as :func:`pystac_monty.exporter.save_static_monty_collection` ensures
        ``related`` / cross-item links resolve to ``https://ifrcgo.org/...`` instead of relative API paths.
        """
        base = MONTY_STAC_EXAMPLES_BASE_URL.rstrip("/")
        for item in items:
            collection_id = item.collection_id or "test-collection"
            item.set_self_href(f"{base}/{collection_id}/{item.id}.json")

    def add_derived_from_links(self, event_item: Item, hazard_items: List[Item]):
        """Add derived_from links from hazard items to parent event item

        This implements STAC best practices for provenance tracking, as shown in
        the Charter hazard examples. The derived_from relationship indicates that
        hazard items were derived from processing the parent event (activation).

        Args:
            event_item: Parent event item
            hazard_items: List of hazard items that are derived from the event
        """
        if not hazard_items:
            return

        event_href = f"{MONTY_STAC_EXAMPLES_BASE_URL.rstrip('/')}/charter-events/{event_item.id}.json"

        for hazard_item in hazard_items:
            hazard_item.add_link(
                Link(rel="derived_from", target=event_href, media_type="application/json", title="Parent Charter Event")
            )

    def get_stac_items(self):
        """Generate STAC items from Charter activation data

        Yields:
            STAC Item: Event item followed by hazard items
        """
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

            all_items = [event_item] + hazard_items
            self.set_charter_item_hrefs(all_items)

            self.add_related_links(event_item=event_item, hazard_items=hazard_items if hazard_items else None)
            self.add_derived_from_links(event_item=event_item, hazard_items=hazard_items)

            yield event_item
            for hazard_item in hazard_items:
                yield hazard_item

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
    provider=Provider(
        "International Charter Space and Major Disasters",
        roles=[ProviderRole.PRODUCER],
        url="https://disasterscharter.org/",
    ),
    titles={
        "event": (
            "Charter Source Events",
            "International Charter Space and Major Disasters activation events",
        ),
        "hazard": ("Charter Source Hazards", "Areas of Interest from Charter activations"),
        "response": (
            "International Charter Source Response",
            "Response products (acquisitions, datasets, VAPs). Items require partner/S3 access; "
            "collection is emitted empty until that pipeline exists.",
        ),
    },
    emit_empty_response_subcatalog=True,
    public_href_base=MONTY_STAC_EXAMPLES_BASE_URL,
)


def iter_charter_stac_items(source_dir: Path) -> Iterator[Item]:
    """Yield STAC items for each ``act-*-activation.json`` under *source_dir* and its ``*-area-*.json`` sidecars."""
    for act_path in sorted(source_dir.glob("act-*-activation.json")):
        act_id = act_path.stem.replace("-activation", "")
        data = json.loads(act_path.read_text(encoding="utf-8"))
        data["areas"] = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(source_dir.glob(f"{act_id}-area-*.json"))]
        source = CharterDataSource(
            data=GenericDataSource(
                source_url=f"{CHARTER_API_BASE}/activations/{act_id}",
                input_data=Memory(content=data, data_type=DataType.MEMORY),
            )
        )
        yield from CharterTransformer(source, None).get_stac_items()


def convert_charter_activations(source_dir: Path, output_dir: Path) -> None:
    """Read ``act-*-activation.json`` plus matching ``act-*-area-*.json`` sidecars from *source_dir*.

    The Charter model directory in the ``monty-stac-extension`` submodule
    (``docs/model/sources/Charter``) is a valid *source_dir* layout for end-to-end checks.
    """
    export_collected_items_and_log(_CHARTER_BATCH, list(iter_charter_stac_items(source_dir)), output_dir)
