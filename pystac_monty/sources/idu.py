import datetime
import json
from dataclasses import dataclass
from typing import Any, List, Dict

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

STAC_EVENT_ID_PREFIX = "idu-event-"
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
    idu_events_collection_url = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/feature/update-idu-documentation/examples/idu-events/idu-events.json"
    idu_impacts_collection_id = "idu-impacts"
    idu_impacts_collection_url = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/refs/heads/feature/update-idu-documentation/examples/idu-impacts/idu-impacts.json"

    hazard_profiles = HazardProfiles()

    def __init__(self, data: IDUDataSource):
        self.data = data

    def make_items(self) -> List[Item]:
        """Create items"""
        items = []

        event_items = self.make_source_event_items()
        items.extend(event_items)

        impact_items = self.make_impact_items()
        items.extend(impact_items)

        return items


    def get_event_collection(self, timeout: int=30) -> Collection:
        """Get the event collection"""
        response = requests.get(self.idu_events_collection_url, timeout=timeout)
        if response.status_code == 200:
            collection_dict = json.loads(response.text)
            return Collection.from_dict(collection_dict)
        return Collection.from_dict({})

    def get_impact_collection(self, timeout: int=30) -> Collection:
        """Get the impact collection"""
        response = requests.get(self.idu_events_collection_url, timeout=timeout)
        if response.status_code == 200:
            collection_dict = json.loads(response.text)
            return Collection.from_dict(collection_dict)
        return Collection.from_dict({})

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
                "sources": data["sources"]
            }
        )

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        item.add_asset(
            "report",
            Asset(
                href=data["source_url"],
                media_type="application/pdf",
                title="Report"
            )
        )
        item.add_link(Link("via", self.data.get_source_url(), "application/json", "IDU Event Data"))

        hazard_tuple = (
            data["category"],
            data["subcategory"],
            data["type"],
            data["subtype"]
        )

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = episode_number
        monty.country_codes = [data["iso3"]]
        monty.hazard_codes = self.map_idu_to_hazard_codes(hazard=hazard_tuple)
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        return item

    def map_idu_to_hazard_codes(self, hazard: tuple):
        """Map IDU hazards to UNDRR-ISC 2020 Hazard Codes"""
        hazard = tuple((item.lower() if item else item for item in hazard))
        hazard_mapping = {
            ('geophysical', 'geophysical', 'earthquake', 'earthquake'): ["GH0001", "GH0004"],
            ('geophysical', 'geophysical', 'mass movement', 'dry mass movement'): ["Dry Mass Movement"],
            ('geophysical', 'geophysical', 'mass movement', 'sinkhole'): ["GH0026"],
            ('geophysical', 'geophysical', 'volcanic activity', 'volcanic activity'): ["GH0020"],
            ('mixed disasters', 'mixed disasters', 'mixed disasters', 'mixed disasters'): "Mixed Disaster",
            ('weather related', 'climatological', 'drought', 'drought'): ["MH0035"],
            ('weather related', 'climatological', 'erosion', 'erosion'): ["EN0019"],
            ('weather related', 'climatological', 'sea level rise', 'sea level rise'): ["EN0023"],
            ('weather related', 'climatological', 'wildfire', 'wildfire'): ["EN0013"],
            ('weather related', 'hydrological', 'flood', 'dam release flood'): ["TL0009"],
            ('weather related', 'hydrological', 'flood', 'flood'): ["FL"],
            ('weather related', 'hydrological', 'mass movement', 'avalanche'): ["MH0050"],
            ('weather related', 'hydrological', 'mass movement', 'landslide/wet mass movement'): ["Landslide/Wet Mass Movement"],
            ('weather related', 'meteorological', 'extreme temperature', 'cold wave'): ["MH0040"],
            ('weather related', 'meteorological', 'storm', 'hailstorm'): ["MH0036"],
            ('weather related', 'meteorological', 'storm', 'storm surge'): ["MH0027"],
            ('weather related', 'meteorological', 'storm', 'storm'): ["MH0058"],
            ('weather related', 'meteorological', 'storm', 'tornado'): ["MH0059"],
            ('weather related', 'meteorological', 'storm', 'typhoon/hurricane/cyclone'): ["MH0057"],
            ('weather related', 'meteorological', 'storm', 'winter storm/blizzard'): ["MH0034"]
        }
        return hazard_mapping.get(hazard, [hazard[-1]])

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

            impact_item.id = f"{STAC_IMPACT_ID_PREFIX}{impact_item.id.replace(STAC_EVENT_ID_PREFIX, '')}"
            impact_item.startdate = startdate
            impact_item.properties["start_datetime"] = startdate.isoformat()
            impact_item.properties["end_datetime"] = enddate.isoformat()
            impact_item.properties["roles"] = ["source", "impact"]
            impact_item.set_collection(self.get_event_collection())

            monty = MontyExtension.ext(impact_item)
            monty.impact_detail = self.get_impact_details(src_data)

            items.append(impact_item)
        return items


    def get_impact_details(self, idu_src_item: dict):
        """Returns the impact details related to displacement"""
        return ImpactDetail(
            category=MontyImpactExposureCategory.ALL_PEOPLE,
            type=MontyImpactType.INTERNALLY_DISPLACED_PERSONS,
            value=idu_src_item["figure"],
            unit="count",
            estimate_type=MontyEstimateType.PRIMARY
        )


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
                raise ValueError(f"Missing required fields {missing_fields}.")
        return idu_data
