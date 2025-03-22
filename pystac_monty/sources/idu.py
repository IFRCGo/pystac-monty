import datetime
import json
import logging
import re
from dataclasses import dataclass
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
from pystac_monty.sources.utils import IDMCUtils

logger = logging.getLogger(__name__)

# Constants

STAC_EVENT_ID_PREFIX = "idmc-idu-event-"
STAC_IMPACT_ID_PREFIX = "idmc-idu-impact-"


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
        # Get the latest item based on id(the last occurrence)
        # and get rid of duplicate items at event level
        event_items_unique = {item.id: item for item in event_items}
        event_items = list(event_items_unique.values())
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
            id=f"{STAC_EVENT_ID_PREFIX}{data['event_id']}",
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
        monty.hazard_codes = IDMCUtils.hazard_codes_mapping(hazard=hazard_tuple)
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        return item

    def _get_impact_type_from_desc(self, description: str):
        """Get impact type from description using regex"""
        keywords = list(IDMCUtils.mappings.keys())
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

    def get_impact_details(self, idu_src_item: dict, impact_type: str):
        """Returns the impact details related to displacement"""
        category, category_type = IDMCUtils.mappings.get(
            impact_type, (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INTERNALLY_DISPLACED_PERSONS)
        )
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
            if item["displacement_type"] not in IDMCUtils.DisplacementType._value2member_map_:
                logging.error("Unknown displacement type: {item['displacement_type']} found. Ignore the datapoint.")
                continue
            # Get the Disaster type data only
            if IDMCUtils.DisplacementType(item["displacement_type"]) == IDMCUtils.DisplacementType.DISASTER_TYPE:
                missing_fields = [field for field in required_fields if field not in item]
                if missing_fields:
                    raise ValueError(f"Missing required fields {missing_fields}.")
                filtered_idu_data.append(item)
        return filtered_idu_data
