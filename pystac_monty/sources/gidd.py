import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

import pytz
from pystac import Asset, Item, Link

from pystac_monty.extension import (
    ImpactDetail,
    MontyEstimateType,
    MontyExtension,
    MontyImpactExposureCategory,
    MontyImpactType,
)
from pystac_monty.hazard_profiles import MontyHazardProfiles
from pystac_monty.sources.common import MontyDataSource, MontyDataTransformer

STAC_EVENT_ID_PREFIX = "gidd-event-"
STAC_IMPACT_ID_PREFIX = "idmc-gidd-impact-"


@dataclass
class GIDDDataSource(MontyDataSource):
    """GIDD data source that can handle Json data"""

    def __init__(self, source_url: str, data: Any):
        super().__init__(source_url, data)
        self.data = json.loads(data)


class GIDDTransformer(MontyDataTransformer):
    """Transforms GIDD event data into STAC Items"""

    gidd_events_collection_id = "gidd-events"
    gidd_events_collection_url = ("../../monty-stac-extension/examples/idmc-events/idmc-events.json")
    # TODO: Make these events collection id consistent
    gidd_hazards_collection_id = "idmc-gidd-impacts"
    gidd_hazards_collection_url = ("../../monty-stac-extension/examples/idmc-gidd-impacts/idmc-gidd-impacts.json")

    hazard_profiles = MontyHazardProfiles()

    def __init__(self, data: GIDDDataSource) -> None:
        """
        Initialize GIDDTransformer

        Args:
            data: GIDDDataSource containing the gidd data
        """
        super().__init__("idmc-gidd")
        self.data = data

    def get_data(self) -> dict:
        """Get the event detail data."""
        return self.data

    def make_items(self) -> List[Item]:
        """Create all STAC items from GIDD data"""
        items = []
        # Create event items
        event_items = self.make_source_event_items()
        items.extend(event_items)
        # Create impact items
        impact_items = self.make_impact_items()
        items.extend(impact_items)

        return items

    def make_source_event_items(self) -> List[Item]:
        """Create the source event items"""
        items = []
        gidd_data = self.check_and_get_gidd_data()

        if not gidd_data:
            return []

        for data in gidd_data:
            item = self.make_source_event_item(data=data)
            items.append(item)

        return items

    def make_bbox(self, coordinates) -> List[float]:
        """
        Calculate bounding box from coordinates

        Args:
            coordinates: List of coordinate pairs

        Returns:
            List containing [min_x, min_y, max_x, max_y]
        """
        # Extract longitudes and latitudes
        longitudes = [coord[0] for coord in coordinates]
        latitudes = [coord[1] for coord in coordinates]

        # Calculate bounding box
        min_x, max_x = min(longitudes), max(longitudes)
        min_y, max_y = min(latitudes), max(latitudes)

        return [min_x, min_y, max_x, max_y]

    def make_source_event_item(self, data: dict) -> Item:
        """Create an Event Item"""
        # Create the geojson point
        geometry = data.get("geometry")
        coordinates = geometry["coordinates"]
        bbox = self.make_bbox(coordinates)

        # Episode number not in the source, so set it to 1
        episode_number = 1
        properties = data["properties"]

        startdate_str = properties.get("Event start date")
        enddate_str = properties.get("Event end date")
        startdate = pytz.utc.localize(datetime.fromisoformat(startdate_str))
        enddate = pytz.utc.localize(datetime.fromisoformat(enddate_str))

        item = Item(
            id=f'{STAC_EVENT_ID_PREFIX}{properties["ID"]}',
            geometry=geometry,
            bbox=bbox,
            datetime=startdate,
            properties={
                "title": properties.get("Event name"),
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
                "location": properties.get("Locations name"),
                "location_accuracy": properties.get("Locations accuracy"),
                "location_type": properties.get("Locations type"),
                "displacement_occured": properties.get("Displacement occurred"),
                "sources": properties.get("Sources"),
                "publishers": properties.get("Publishers"),
            },
        )

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = episode_number
        monty.country_codes = [properties.get("ISO3")]

        if properties.get("Figure cause") == "Disaster":
            hazard_tuple = (
                properties["Hazard category"],
                properties["Hazard sub category"],
                properties["Hazard type"],
                properties["Hazard sub type"],
            )
            monty.hazard_codes = self.map_gidd_to_hazard_codes(hazard=hazard_tuple)
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        item.add_asset(
            "source",
            Asset(
                href=self.data.get_source_url(), media_type="application/geo+json", title="GIDD GeoJson Source", roles=["source"]
            ),
        )

        item.add_link(Link("via", self.data.get_source_url(), "application/json", "GIDD Event Data"))

        return item

    def make_impact_items(self) -> List[Item]:
        """Create impact items"""
        items = []
        gidd_data = self.check_and_get_gidd_data()
        event_items = self.make_source_event_items()

        for event_item, src_data in zip(event_items, gidd_data):
            impact_item = event_item.clone()
            properties = src_data["properties"]
            startdate_str = properties.get("Event start date")
            enddate_str = properties.get("Event end date")
            startdate = pytz.utc.localize(datetime.fromisoformat(startdate_str))
            enddate = pytz.utc.localize(datetime.fromisoformat(enddate_str))

            impact_item.id = (
                impact_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_IMPACT_ID_PREFIX)
                + "-"
                + str(properties["ID"])
                + "-"
                + "displaced"
            )

            impact_item.datetime = startdate
            impact_item.properties["title"] = (
                f"{properties.get('Figure category')}-{properties.get('Figure unit')} " f"for {properties.get('Event name')}"
            )
            impact_item.properties.update(
                {
                    "start_datetime": startdate.isoformat(),
                    "end_datetime": enddate.isoformat(),
                    "figure_category": properties.get("Figure category"),
                    "figure_unit": properties.get("Figure unit"),
                    "figure_cause": properties.get("Figure cause"),
                    "geographical_region": properties.get("Geographical region"),
                    "country": properties.get("Country"),
                    "roles": ["source", "impact"],
                }
            )

            impact_item.set_collection(self.get_impact_collection())
            monty = MontyExtension.ext(impact_item)
            monty.impact_detail = self.get_impact_details(properties)
            items.append(impact_item)

        return items

    def get_impact_details(self, gidd_src_item: dict) -> ImpactDetail:
        """Returns the impact details related to displacement"""
        return ImpactDetail(
            category=MontyImpactExposureCategory.ALL_PEOPLE,
            type=MontyImpactType.INTERNALLY_DISPLACED_PERSONS,
            value=gidd_src_item["Total figures"],
            unit="count",
            estimate_type=MontyEstimateType.PRIMARY,
        )

    def map_gidd_to_hazard_codes(self, hazard: tuple) -> List[str]:
        """
        Map gidd hazards to UNDRR-ISC 2020 Hazard Codes

        Args:
            hazard: Tuple of (category, subcategory, type, subtype)

        Returns:
            List of hazard codes
        """
        hazard = tuple(item.lower() if item else item for item in hazard)

        hazard_mapping = {
            ("geophysical", "geophysical", "earthquake", "earthquake"): ["GH0001", "GH0004"],
            ("geophysical", "geophysical", "earthquake", "tsunami"): ["GH0006"],
            ("geophysical", "geophysical", "mass movement", "dry mass movement"): ["GH0007", "GH0014"],
            ("geophysical", "geophysical", "mass movement", "sinkhole"): ["GH0026"],
            ("geophysical", "geophysical", "volcanic activity", "volcanic activity"): ["GH0020"],
            ("mixed disasters", "mixed disasters", "mixed disasters", "mixed disasters"): ["mix-mix-mix-mix"],
            ("weather related", "climatological", "desertification", "desertification"): ["EN0014"],
            ("weather related", "climatological", "drought", "drought"): ["MH0035"],
            ("weather related", "climatological", "erosion", "erosion"): ["EN0019"],
            ("weather related", "climatological", "salinisation", "salinization"): ["Salinization"],
            ("weather related", "climatological", "sea level rise", "sea level rise"): ["EN0023"],
            ("weather related", "climatological", "wildfire", "wildfire"): ["EN0013"],
            ("weather related", "hydrological", "flood", "dam release flood"): ["TL0009"],
            ("weather related", "hydrological", "flood", "flood"): ["FL"],
            ("weather related", "hydrological", "mass movement", "avalanche"): ["MH0050"],
            ("weather related", "hydrological", "mass movement", "landslide/wet mass movement"): ["MH0051"],
            ("weather related", "hydrological", "wave action", "rogue wave"): ["MH0027"],
            ("weather related", "meteorological", "extreme temperature", "cold wave"): ["MH0040"],
            ("weather related", "meteorological", "extreme temperature", "heat wave"): ["MH0047"],
            ("weather related", "meteorological", "storm", "hailstorm"): ["MH0036"],
            ("weather related", "meteorological", "storm", "sand/dust storm"): ["MH0015"],
            ("weather related", "meteorological", "storm", "storm surge"): ["MH0027"],
            ("weather related", "meteorological", "storm", "storm"): ["MH0058"],
            ("weather related", "meteorological", "storm", "tornado"): ["MH0059"],
            ("weather related", "meteorological", "storm", "typhoon/hurricane/cyclone"): ["MH0057"],
            ("weather related", "meteorological", "storm", "winter storm/blizzard"): ["MH0034"],
        }

        return hazard_mapping.get(hazard, [hazard[-1]])

    def check_and_get_gidd_data(self) -> List[Dict[str, Any]]:
        """
        Validate the source fields

        Returns:
            List of validated GIDD data dictionaries
        """
        gidd_data: List[Dict[str, Any]] = self.data.get_data()
        if not gidd_data:
            print(f"No gidd data found in {self.data.get_source_url()}")
            return []

        data = gidd_data.get("features")
        disaster_data = []
        for item in data:
            item_properties = item.get("properties", {})
            if not item_properties.get("Figure cause") == "Conflict":  # skip conflict data
                required_properties = ["Event ID", "ISO3", "Event start date"]
                missing_properties = [field for field in required_properties if field not in item_properties]
                if missing_properties:
                    raise ValueError(f"Missing required properties {missing_properties}.")
                disaster_data.append(item)

        return disaster_data
