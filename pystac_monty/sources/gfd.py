import json
from datetime import datetime
from typing import Any, List

import pytz
import requests
from pystac import Collection, Item

from pystac_monty.extension import (
    HazardDetail,
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource

# Constants

STAC_EVENT_ID_PREFIX = "gfd-event-"
STAC_HAZARD_ID_PREFIX = "gfd-hazard-"
STAC_IMPACT_ID_PREFIX = "gfd-impact-"


class GFDDataSource(MontyDataSource):
    """GFD Data from the source"""

    def __init__(self, source_url: str, data: Any):
        super().__init__(source_url, data)
        self.data = json.loads(data)


class GFDTransformer:
    """Transform the source data into the STAC items"""

    gfd_events_collection_id = "gfd-events"
    gfd_events_collection_url = (
        "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/examples/gfd-events/gfd-events.json"
    )

    gfd_hazards_collection_id = "gfd-hazards"
    gfd_hazards_collection_url = (
        "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/examples/gfd-hazards/gfd-hazards.json"
    )

    gfd_impacts_collection_id = "gfd-impacts"
    gfd_impacts_collection_url = (
        "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/main/examples/gfd-impacts/gfd-impacts.json"
    )
    hazard_profiles = MontyHazardProfiles()

    def __init__(self, data: GFDDataSource):
        self.data = data

    def make_items(self) -> List[Item]:
        """Create Items"""
        items = []

        event_items = self.make_source_event_items()
        items.extend(event_items)

        hazard_items = self.make_hazard_items()
        items.extend(hazard_items)

        impact_items = self.make_impact_items()
        items.extend(impact_items)

        return items

    def get_event_collection(self, timeout: int = 30) -> Collection:
        """Get Event collection"""
        response = requests.get(self.gfd_events_collection_url, timeout=timeout)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def get_hazard_collection(self, timeout: int = 30) -> Collection:
        """Get Hazard collection"""
        response = requests.get(self.gfd_hazards_collection_url, timeout=timeout)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def get_impact_collection(self, timeout: int = 30) -> Collection:
        """Get Impact collection"""
        response = requests.get(self.gfd_impacts_collection_url, timeout=timeout)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def _get_bounding_box(self, polygon: list):
        """Get the bounding box from the polygon"""
        lons, lats = zip(*polygon)  # Separate longitudes and latitudes
        return [min(lons), min(lats), max(lons), max(lats)]

    def make_source_event_items(self) -> List[Item]:
        """Create the source event item"""
        items = []
        gfd_data = self.check_and_get_gfd_data()
        if not gfd_data:
            return []
        for data in gfd_data:
            item = self.make_source_event_item(data=data)
            items.append(item)
        return items

    def make_source_event_item(self, data: dict) -> Item:
        """Create the source event item"""

        footprint = data["system:footprint"]
        # Note: Convert LinearRing to Polygon as LinearRing is not supported in STAC spec.
        geometry = {"type": "Polygon", "coordinates": [footprint["coordinates"]]}

        description = data["dfo_main_cause"]

        bbox = self._get_bounding_box(data["system:footprint"]["coordinates"])
        # Episode number not in the source, so, set it to 1
        episode_number = 1

        startdate = pytz.utc.localize(datetime.fromtimestamp(data["system:time_start"] / 1000))
        enddate = pytz.utc.localize(datetime.fromtimestamp(data["system:time_end"] / 1000))

        item = Item(
            id=f'{STAC_EVENT_ID_PREFIX}{data["id"]}',
            geometry=geometry,
            bbox=bbox,
            datetime=startdate,
            properties={
                "title": description,
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
            },
        )

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = episode_number
        monty.country_codes = data["cc"].split(",")
        monty.hazard_codes = ["FL"]  # GFD is a Flood related source
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        return item

    def make_hazard_items(self) -> List[Item]:
        """Create hazard items"""
        hazard_items = []

        gfd_data = self.check_and_get_gfd_data()
        event_items = self.make_source_event_items()

        for event_item, src_data in zip(event_items, gfd_data):
            hazard_item = event_item.clone()
            hazard_item.id = event_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_HAZARD_ID_PREFIX)
            hazard_item.properties["roles"] = ["source", "hazard"]
            hazard_item.set_collection(self.get_hazard_collection())

            monty = MontyExtension.ext(hazard_item)
            # Hazard Detail
            monty.hazard_detail = HazardDetail(
                cluster=self.hazard_profiles.get_cluster_code(hazard_item),
                severity_value=src_data["dfo_severity"],
                severity_unit="GFD Flood Severity Score",
                severity_label=None,
                estimate_type=MontyEstimateType.PRIMARY,
            )
            hazard_items.append(hazard_item)
        return hazard_items

    def make_impact_items(self) -> List[Item]:
        """Create impact items"""
        items = []
        gfd_data = self.check_and_get_gfd_data()
        event_items = self.make_source_event_items()
        for event_item, src_data in zip(event_items, gfd_data):
            items.extend(self.make_type_based_impact_items(event_item, src_data))
        return items

    def make_type_based_impact_items(self, event_item: dict, src_data: dict) -> List[Item]:
        """Returns the impact details related to flood"""
        impact_fields = {
            "dfo_dead": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.DEATH),
            "dfo_displaced": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_DISPLACED_PERSONS),
        }

        impact_items = []

        for key_field, (category, impact_type) in impact_fields.items():
            impact_item = event_item.clone()
            impact_item.id = f"{STAC_IMPACT_ID_PREFIX}{src_data['id']}-{key_field}"
            impact_item.properties["title"] = f"{event_item.properties['title']}-{key_field}"
            impact_item.properties["roles"] = ["source", "impact"]
            impact_item.set_collection(self.get_impact_collection())

            monty = MontyExtension.ext(impact_item)
            monty.impact_detail = ImpactDetail(
                category=category,
                type=impact_type,
                value=src_data[key_field],
                unit="count",
                estimate_type=MontyEstimateType.PRIMARY,
            )
            impact_items.append(impact_item)
        return impact_items

    def check_and_get_gfd_data(self):
        """Get the GFD data"""
        return [item["properties"] for item in self.data.get_data()]
