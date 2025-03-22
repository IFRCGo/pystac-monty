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

STAC_EVENT_ID_PREFIX = "idmc-gidd-event-"
STAC_IMPACT_ID_PREFIX = "idmc-gidd-impact-"


@dataclass
class GIDDDataSource(MontyDataSource):
    """GIDD data source that can handle Json data"""

    def __init__(self, source_url: str, data: Any):
        super().__init__(source_url, data)
        self.data = json.loads(data)


class GIDDTransformer(MontyDataTransformer[GIDDDataSource]):
    """Transforms GIDD event data into STAC Items"""

    hazard_profiles = MontyHazardProfiles()
    source_name = 'idmc-gidd'

    def get_data(self) -> dict:
        """Get the event detail data."""
        return self.data_source

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
                href=self.data_source.get_source_url(), media_type="application/geo+json", title="GIDD GeoJson Source", roles=["source"]
            ),
        )

        item.add_link(Link("via", self.data_source.get_source_url(), "application/json", "GIDD Event Data"))

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
        return hazard_mapping.get(hazard, [hazard[-1]])

    def check_and_get_gidd_data(self) -> List[Dict[str, Any]]:
        """
        Validate the source fields

        Returns:
            List of validated GIDD data dictionaries
        """
        gidd_data: List[Dict[str, Any]] = self.data_source.get_data()
        if not gidd_data:
            print(f"No gidd data found in {self.data_source.get_source_url()}")
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
