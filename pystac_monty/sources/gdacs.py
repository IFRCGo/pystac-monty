import datetime
import json
from typing import Any

import pytz
import requests
from markdownify import markdownify as md
from pystac import Collection, Item

from pystac_monty.extension import MontyExtension

# Constants

GDACS_EVENT_STARTDATETIME_PROPERTY = "fromdate"
GDACS_EVENT_ENDDATETIME_PROPERTY = "todate"


class GDACSTransformer:
    
    gdacs_event_collection_id = "gdacs-events"
    gdacs_event_collection_url = "https://github.com/IFRCGo/monty-stac-extension/raw/refs/heads/main/examples/gdacs-events/gdacs-events.json"
    
    def __init__(self, event_data: dict[str, Any]) -> None:
        self.event_data = event_data
        
    def make_items(self) -> list[Item]:
        items = []
        
        """ 1. Create the source event item """
        source_event_item = self.make_source_event_item()
        
        items.append(source_event_item)
        
        return items
    
    def get_event_collection(self) -> Collection:
        response = requests.get(self.gdacs_event_collection_url)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)
        
    def check_event_data(self) -> bool:
        if not self.event_data:
            raise ValueError("event_data is required")
        if "geometry" not in self.event_data:
            raise ValueError("event_data must contain a geometry")
        # check that the geometry is only a point
        if self.event_data["geometry"]["type"] != "Point":
            raise ValueError("Geometry must be a point")
        # check the properties
        if "properties" not in self.event_data:
            raise ValueError("event_data must contain properties")
        # check the datetime
        if GDACS_EVENT_STARTDATETIME_PROPERTY not in self.event_data["properties"]:
            raise ValueError("event_data must contain a 'fromdate' property")
        
        
    def make_source_event_item(self) -> Item:
        
        # check event_data
        self.check_event_data()
        
        # Build the identifier for the item
        id = self.event_data["properties"]["eventid"].__str__()
        episode_number = 0
        if "episodeid" in self.event_data["properties"]:
            episode_number = self.event_data["properties"]["episodeid"]
            
        id += "-" + episode_number.__str__()
            
        # Select the description
        if "htmldescription" in self.event_data["properties"]:
            # translate the description to markdown
            description = md(self.event_data["properties"]["htmldescription"])
        else:
            description = self.event_data["properties"]["description"]
            
        startdate_str = self.event_data["properties"][GDACS_EVENT_STARTDATETIME_PROPERTY]
        startdate = pytz.utc.localize(datetime.datetime.fromisoformat(startdate_str))
        enddate_str = self.event_data["properties"][GDACS_EVENT_ENDDATETIME_PROPERTY]
        enddate = pytz.utc.localize(datetime.datetime.fromisoformat(enddate_str))
                
        item = Item(
            id=id,
            geometry=self.event_data["geometry"],
            bbox=self.event_data["bbox"],
            datetime=startdate,
            properties= {
                "title": self.event_data["properties"]["name"],
                "description": description,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
            }
        )
        
        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = episode_number
        monty.hazard_codes = [self.event_data["properties"]["eventtype"]]
        monty.country_codes = [self.event_data["properties"]["iso3"]]
        monty.compute_and_set_correlation_id()
        
        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]
        
        return item
        
        