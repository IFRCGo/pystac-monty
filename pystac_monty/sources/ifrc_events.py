import json
from typing import Any, Dict, List
from pystac import Collection, Item

import requests

from pystac_monty.extension import (
    MontyExtension,
)

from pystac_monty.hazard_profiles import HazardProfiles
from pystac_monty.geocoding import MontyGeoCoder
from pystac_monty.sources.common import MontyDataSource

STAC_EVENT_ID_PREFIX = "ifrc-event-"
STAC_IMPACT_ID_PREFIX = "ifrc-impact-"


class IfrcEventsDataSource(MontyDataSource):
    event_url: str

    def __init__(self, event_url: str):
        self.event_url = event_url


class IfrcEventsTransformer():
    data_source: IfrcEventsDataSource

    ifrc_events_collection_id = "ifrc-events"
    ifrc_events_collection_url = ""  # TODO
    ifrc_impacts_collection_id = "ifrc-impacts"
    ifrc_impacts_collection_url = ""  # TODO
    hazard_profiles = HazardProfiles()

    def __init__(self, data_source: IfrcEventsDataSource, geocoder: MontyGeoCoder):
        self.data_source = data_source
        self.geocoder = geocoder

    def get_items(self):
        print("\n", self.data_source.event_url)
        return []

    def get_event_collection(self) -> Collection:
        """Get event collection"""
        response = requests.get(self.ifrc_events_collection_url)
        collection_dict = json.loads(response.text)
        return Collection.from_dict(collection_dict)

    def make_items(self) -> List[Item]:
        """Create items"""
        items = []

        event_items = self.make_source_event_items()
        items.extend(event_items)

        # impact_items = self.make_impact_items()
        # items.extend(impact_items)

        return items

    def make_source_event_items(self) -> List[Item]:
        """Create ifrc event item"""
        items = []
        ifrc_data: List[Dict[str, Any]] = self.data_source.get_data()
        if not ifrc_data:
            return []

        for data in ifrc_data:
            item = self.make_source_event_item(data=data)
            items.append(item)
        return items

    def make_source_event_item(self, data: dict) -> Item:
        """Create ane event item"""
        geometry = None
        bbox = None
        geom_data = self.geocoder.get_geometry_by_country_name(data["country"]["iso3"])

        # Filter out relevant disaster types
        monty_accepted_disaster_types = {
            "Earthquake", "Cyclone", "Volcanic Eruption", "Tsunami", "Flood",
            "Cold Wave", "Fire", "Heat Wave", "Drought", "Storm Surge",
            "Landslide", "Flash Flood"
        }

        if data["dtype"]["name"] not in monty_accepted_disaster_types:
            return []

        if data["aid"] not in {0, 1}:
            return []

        if geom_data:
            geometry = geom_data["geometry"]
            bbox = geom_data["bbox"]

        # Create item
        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{data["aid"]}",
            geometry=geometry,
            bbox=bbox,
            datetime=data["start_date"],
            start_datetime=data["start_date"],
            end_datetime=data["end_date"],
            properties={
                "title": data["name"],
            },
        )

        # Add Monty extension
        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = 1  # IFRC DREF doesn't have episodes
        monty.hazard_codes = data["dtype"]["name"]
        monty.country_codes = data["country"]["iso3"]
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        # Set collection and roles
        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        return item

    def map_ifrc_to_hazard_codes(self, classification_key: str) -> List[str]:
        """
        Map IFRC DREF & EA classification key to UNDRR-ISC 2020 Hazard codes

        Args:
            classification_key: dtype name (e.g., 'Flood')

        Returns:
            List of UNDRR-ISC hazard codes
        """

        if not classification_key:
            return []

        key = classification_key.lower()

        # IFRC DREF hazards classification mapping to UNDRR-ISC codes
        mapping = {
            "Earthquake": ["GH0001", "GH0002", "GH0003", "GH0004", "GH0005"],
            "Cyclone": ["MH0030", "MH0031", "MH0032"],
            "Volcanic Eruption": ["GH009", "GH0013", "GH0014", "GH0015", "GH0016"],
            "Tsunami": ["MH0029", "GH0006"],
            "Flood": ["FL"],  # General flood
            "Cold Wave": "MH0040",
            "Fire": ["FR"],
            "Heat Wave": ["MH0047"],
            "Drought": ["MH0035"],
            "Storm Surge": ["MH0027"],
            "Landslide": ["GH0007"],
            "Flash Flood": ["MH0006"],
            "Epidemic": ["EP"],  # General epidemic
        }

        if key in mapping:
            return mapping[key]

        return []
