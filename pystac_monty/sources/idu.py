import datetime
import json
import mimetypes
from dataclasses import dataclass
from typing import Any, List, Dict

import pytz
import requests
from markdownify import markdownify as md
from pystac import Asset, Collection, Item, Link
from shapely.geometry import Point, mapping

from pystac_monty.extension import (
    HazardDetail,
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.hazard_profiles import HazardProfiles
from pystac_monty.sources.common import MontyDataSource

# Constants

STAC_EVENT_ID_PREFIX = "idu-event-"
STAC_HAZARD_ID_PREFIX = "idu-hazard-"
STAC_IMPACT_ID_PREFIX = "idu-impact-"

@dataclass
class IDUDataSource(MontyDataSource):
    """IDU Data directly from the source"""
    def __init__(self, source_url: str, data: Any):
        super().__init__(source_url, data)
        self.data = json.loads(data)
    

class IDUTransformer:
    """Transform the source data into the STAC items"""
    idu_events_collection_id = "idu-events"
    idu_events_collection_url = ""
    idu_hazards_collection_id = "idu-hazards"
    idu_hazards_collection_url = ""
    idu_impacts_collection_id = "idu-impacts"
    idu_impacts_collection_url = ""

    hazard_profiles = HazardProfiles()

    def __init__(self, data: list[IDUDataSource]):
        self.data = data
    
    def get_event_collection(self, timeout: int=30) -> Collection:
        """Get the event collection"""
        response = requests.get(self.idu_events_collection_url, timeout=timeout)
        assert response.status_code == 200
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)
    
    def make_source_event_item(self):
        """Create the source event item"""
        items = []
        idu_data = self.check_and_get_idu_data()
        if not idu_data:
            print("Cannot create the event Item")
        
        for data in idu_data:
            latitude = float(data.get("latitude"))
            longitude = float(data.get("longitude"))
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
                id=id,
                geometry=geometry,
                bbox=bbox,
                datetime=startdate,
                properties={
                    "event_id": data["event_id"],
                    "title": data["event_name"],
                    "description": description,
                    "start_datetime": startdate.isoformat(), 
                    "end_datetime": enddate.isoformat(),
                    "location": data["location_name"],
                    "displacement_start_date": data["displacement_start_date"],
                    "displacement_end_date": data["displacement_end_date"],
                    "sources": data["sources"]
                }
            )

            item.set_collection(self.get_event_collection())
            item.properties["roles"] = ["source", "event"]

            item.add_asset(
                "report",
                Asset(
                    href=data["source_url"],
                    media_type=mimetypes.types_map[".pdf"],
                    title="Report"
                )
            )
            item.add_link(Link("via", self.data.get_source_url(), "application/json", "IDU Event Data"))

            MontyExtension.add_to(item)
            monty = MontyExtension.ext(item)
            monty.episode_number = episode_number
            monty.country_codes = data["iso3"]
            monty.hazard_codes = [data["type"]]

            items.append(item)
        return items
    
    def check_and_get_idu_data(self) -> list[Any]:
        """Validate the source fields"""
        idu_data: List[Dict[str, Any]] = self.data.get_data()
        required_fields = ["latitude", "longitude", "event_id"]
        
        if not idu_data:
            print(f"No IDU data found in {self.data.get_source_url()}")
            return []
        
        for item in idu_data:
            missing_fields = [field for field in required_fields if field not in item]
            if missing_fields:
                raise ValueError(f"Missing required fields {missing_fields} in glide number {obj.get('number')}")
        return idu_data
