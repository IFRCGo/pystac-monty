import datetime
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Tuple
from ast import literal_eval
import pytz
import requests
from markdownify import markdownify as md
from pystac import Asset, Collection, Item, Link
from shapely.geometry import Point, mapping

from pystac_monty.extension import (
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.hazard_profiles import HazardProfiles
from pystac_monty.sources.common import MontyDataSource

# Constants

STAC_EVENT_ID_PREFIX = "gfd-event-"
STAC_HAZARD_ID_PREFIX = "gfd-hazard-"
STAC_IMPACT_ID_PREFIX = "gfd-impact-"

class GFDDataSource(MontyDataSource):
    """GFD Data from the source"""
    def __init__(self, source_url: str, data: Any)
        super().__init__(source_url, data)
        self.data = json.loads(data)

class GFDTransformer:
    """Transform the source data into the STAC items"""
    hazard_profiles = HazardProfiles()

    def __init__(self, data: GFDDataSource):
        self.data = data
    
    def make_items(self) -> List[Item]:
        """Create Items"""
        items = []

        event_items = self.make_source_event_items()
        items.extend(event_items)

        return items
    
    def _get_bbox_from_coordinates(self,
        polygon: List[List[float, float]]
    ) -> List[float, float, float, float]:
        x_min = y_min = float("inf")
        x_max = y_max = float("-inf")
        for coor in polygon:
            if x_min > coor[0]:
                x_min = coor[0]
            if y_min > coor[1]:
                y_min = coor[1]
            if x_max < coor[0]:
                x_max = coor[0]
            if y_max < coor[1]:
                y_max = coor[1]
        
        return [x_min, y_min, x_max, y_max]

    
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
        geometry = data["system:footprint"]
        description = data["dfo_main_cause"]
        
        bbox = self._get_bbox_from_coordinates(data["system:footprint"]["coordinates"])
        # Episode number not in the source, so, set it to 1
        episode_number = 1

        startdate = pytz.utc.localize(datetime.datetime.fromisoformat(data["system:time_start"]))
        enddate = pytz.utc.localize(datetime.datetime.fromisoformat(data["system:time_end"]))

        item = Item(
            id=f'{STAC_EVENT_ID_PREFIX}{data["system:index"]}',
            geometry=geometry,
            bbox=bbox,
            datetime=startdate,
            properties={
                "title": description,
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
            }
        )

        item.set_collection()
        item.properties["roles"] = ["source", "event"]

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = episode_number
        monty.country_codes = [literal_eval(data["gfd_country_code"])]
        monty.hazard_codes = ["FL"]  # GFD is a Flood related source
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        return item



