import datetime
import json
import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List

import pytz
from markdownify import markdownify as md
from pystac import Asset, Item, Link
from shapely.geometry import Point, mapping

from pystac_monty.extension import (
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource, MontyDataTransformer

logger = logging.getLogger(__name__)

# Constants

STAC_EVENT_ID_PREFIX = "idmc-idu-event-"
STAC_IMPACT_ID_PREFIX = "idmc-idu-impact-"


class DisplacementType(Enum):
    """Displacement Types"""

    DISASTER_TYPE = "Disaster"
    CONFLICT_TYPE = "Conflict"
    OTHER_TYPE = "Other"


class ImpactMappings:
    """All Impact Mappings"""

    # TODO: For other types e.g. FORCED_TO_FLEE, IN_RELIEF_CAMP, DESTROYED_HOUSING,
    # PARTIALLY_DESTROYED_HOUSING, UNINHABITABLE_HOUSING, RETURNS, MULTIPLE_OR_OTHER
    # Handle them later.
    mappings = {
        "evacuated": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.EVACUATED),
        "displaced": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INTERNALLY_DISPLACED_PERSONS),
        "relocated": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.RELOCATED),
        "sheltered": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.EMERGENCY_SHELTERED),
        "homeless": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.HOMELESS),
        "affected": (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.TOTAL_AFFECTED),
    }


@dataclass
class IDUDataSource(MontyDataSource):
    """IDU Data directly from the source"""

    def __init__(self, source_url: str, data: Any):
        super().__init__(source_url, data)
        self.data = json.loads(data)


class IDUTransformer(MontyDataTransformer):
    """Transform the source data into the STAC items"""

    hazard_profiles = MontyHazardProfiles()

    def __init__(self, data: IDUDataSource):
        super().__init__("idmc-idu")
        self.data = data

    def make_items(self) -> List[Item]:
        """Create items"""
        items = []

        event_items = self.make_source_event_items()
        items.extend(event_items)

        impact_items = self.make_impact_items()
        items.extend(impact_items)

        return items

    def make_source_event_items(self) -> List[Item]:
        """Create the source event item"""
        items = []
        idu_data = self.check_and_get_idu_data()
        if not idu_data:
            return []

        for data in idu_data:
            item = self.make_source_event_item(data=data)
            items.append(item)
        return items

    def make_source_event_item(self, data: dict) -> Item:
        """Create an Event Item"""
        latitude = float(data.get("latitude") or 0)
        longitude = float(data.get("longitude") or 0)
        # Create the geojson point
        point = Point(longitude, latitude)
        geometry = mapping(point)
        bbox = [longitude, latitude, longitude, latitude]

        description = md(data["standard_popup_text"])

        # Episode number not in the source, so, set it to 1
        episode_number = 1

        startdate_str = data["event_start_date"]
        enddate_str = data["event_end_date"]

        startdate = pytz.utc.localize(datetime.datetime.fromisoformat(startdate_str))
        enddate = pytz.utc.localize(datetime.datetime.fromisoformat(enddate_str))

        item = Item(
            id=f'{STAC_EVENT_ID_PREFIX}{data["event_id"]}',
            geometry=geometry,
            bbox=bbox,
            datetime=startdate,
            properties={
                "title": data["event_name"],
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
                "location": data["locations_name"],
                "sources": data["sources"],
            },
        )

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        item.add_asset("report", Asset(href=data["source_url"], media_type="application/pdf", title="Report"))
        item.add_link(Link("via", self.data.get_source_url(), "application/json", "IDU Event Data"))

        hazard_tuple = (data["category"], data["subcategory"], data["type"], data["subtype"])

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = episode_number
        monty.country_codes = [data["iso3"]]
        monty.hazard_codes = self.map_idu_to_hazard_codes(hazard=hazard_tuple)
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        return item

    def map_idu_to_hazard_codes(self, hazard: tuple) -> list[str]:
        """Map IDU hazards to UNDRR-ISC 2020 Hazard Codes"""
        hazard = tuple((item.lower() if item else item for item in hazard))
        hazard_mapping = {
            ("geophysical", "geophysical", "earthquake", "earthquake"): ["nat-geo-ear-gro"],
            ("geophysical", "geophysical", "earthquake", "tsunami"): ["nat-geo-ear-tsu"],
            ("geophysical", "geophysical", "mass movement", "dry mass movement"): ["nat-geo-mmd-lan"],
            ("geophysical", "geophysical", "mass movement", "sinkhole"): ["nat-geo-mmd-sub"],
            ("geophysical", "geophysical", "volcanic activity", "volcanic activity"): ["nat-geo-vol-vol"],
            ("mixed disasters", "mixed disasters", "mixed disasters", "mixed disasters"): ["mix-mix-mix-mix"],
            ("weather related", "climatological", "desertification", "desertification"): ["EN0006", "nat-geo-env-des"],
            ("weather related", "climatological", "drought", "drought"): ["nat-cli-dro-dro"],
            ("weather related", "climatological", "erosion", "erosion"): ["EN0019", "nat-geo-env-soi"],
            ("weather related", "climatological", "salinisation", "salinization"): ["EN0007", "nat-geo-env-slr"],
            ("weather related", "climatological", "sea level rise", "sea level rise"): ["EN0023", "nat-geo-env-slr"],
            ("weather related", "climatological", "wildfire", "wildfire"): ["nat-cli-wil-wil"],
            ("weather related", "hydrological", "flood", "dam release flood"): ["tec-mis-col-col"],
            ("weather related", "hydrological", "flood", "flood"): ["nat-hyd-flo-flo"],
            ("weather related", "hydrological", "mass movement", "avalanche"): ["nat-hyd-mmw-ava"],
            ("weather related", "hydrological", "mass movement", "landslide/wet mass movement"): ["nat-hyd-mmw-lan"],
            ("weather related", "hydrological", "wave action", "rogue wave"): ["nat-hyd-wav-rog"],
            ("weather related", "meteorological", "extreme temperature", "cold wave"): ["nat-met-ext-col"],
            ("weather related", "meteorological", "extreme temperature", "heat wave"): ["nat-met-ext-hea"],
            ("weather related", "meteorological", "storm", "hailstorm"): ["nat-met-sto-hai"],
            ("weather related", "meteorological", "storm", "sand/dust storm"): ["nat-met-sto-san"],
            ("weather related", "meteorological", "storm", "storm surge"): ["nat-met-sto-sur"],
            ("weather related", "meteorological", "storm", "storm"): ["nat-met-sto-sto"],
            ("weather related", "meteorological", "storm", "tornado"): ["nat-met-sto-tor"],
            ("weather related", "meteorological", "storm", "typhoon/hurricane/cyclone"): ["nat-met-sto-tro"],
            ("weather related", "meteorological", "storm", "winter storm/blizzard"): ["nat-met-sto-bli"],
        }
        if hazard not in hazard_mapping:
            raise KeyError(f"Hazard {hazard} not found.")
        return hazard_mapping.get(hazard)

    def _get_impact_type_from_desc(self, description: str):
        """Get impact type from description using regex"""
        keywords = list(ImpactMappings.mappings.keys())
        # Get the first match
        match = re.findall(r"\((.*?)\)", description)
        # Use the first item only
        if match and match[0] in keywords:
            return match[0]
        logger.warning(f"Match {match} not found. Using the default value.")
        return "displaced"

    def make_impact_items(self) -> List[Item]:
        """Create impact items"""
        items = []
        idu_data = self.check_and_get_idu_data()
        event_items = self.make_source_event_items()
        for event_item, src_data in zip(event_items, idu_data):
            impact_item = event_item.clone()

            startdate_str = src_data["displacement_start_date"]
            enddate_str = src_data["displacement_end_date"]
            startdate = pytz.utc.localize(datetime.datetime.fromisoformat(startdate_str))
            enddate = pytz.utc.localize(datetime.datetime.fromisoformat(enddate_str))

            description = src_data["standard_popup_text"]
            impact_type = self._get_impact_type_from_desc(description=description)

            impact_item.id = (
                f"{impact_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_IMPACT_ID_PREFIX)}{src_data['id']}-{impact_type}"
            )
            impact_item.startdate = startdate
            impact_item.properties["start_datetime"] = startdate.isoformat()
            impact_item.properties["end_datetime"] = enddate.isoformat()
            impact_item.properties["roles"] = ["source", "impact"]
            impact_item.set_collection(self.get_impact_collection())

            monty = MontyExtension.ext(impact_item)
            monty.impact_detail = self.get_impact_details(idu_src_item=src_data, impact_type=impact_type)

            items.append(impact_item)
        return items

    def _get_impact_type(self, impact_type: str):
        """Get the impact related details"""
        return ImpactMappings.mappings.get(
            impact_type, (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INTERNALLY_DISPLACED_PERSONS)
        )

    def get_impact_details(self, idu_src_item: dict, impact_type: str):
        """Returns the impact details related to displacement"""
        category, category_type = self._get_impact_type(impact_type=impact_type)
        return ImpactDetail(
            category=category,
            type=category_type,
            value=idu_src_item["figure"],
            unit="count",
            estimate_type=MontyEstimateType.PRIMARY,
        )

    def check_and_get_idu_data(self) -> list[Any]:
        """Validate the source fields"""
        idu_data: List[Dict[str, Any]] = self.data.get_data()
        required_fields = ["latitude", "longitude", "event_id"]

        filtered_idu_data = []
        if not idu_data:
            logger.warning(f"No IDU data found in {self.data.get_source_url()}")
            return []

        for item in idu_data:
            if item["displacement_type"] not in DisplacementType._value2member_map_:
                logging.error("Unknown displacement type: {item['displacement_type']} found. Ignore the datapoint.")
                continue
            # Get the Disaster type data only
            if DisplacementType(item["displacement_type"]) == DisplacementType.DISASTER_TYPE:
                missing_fields = [field for field in required_fields if field not in item]
                if missing_fields:
                    raise ValueError(f"Missing required fields {missing_fields}.")
                filtered_idu_data.append(item)
        return filtered_idu_data
