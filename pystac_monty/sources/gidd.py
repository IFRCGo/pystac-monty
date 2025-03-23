import itertools
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Generator, List

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
from pystac_monty.sources.utils import IDMCUtils
from pystac_monty.validators.gidd import GiddValidator

logger = logging.getLogger(__name__)

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
    source_name = "idmc-gidd"

    def get_stac_items(self) -> Generator[Item, None, None]:
        """Creates the STAC Items"""
        self.transform_summary.mark_as_started()

        gidd_data = self.check_and_get_gidd_data()
        gidd_data.sort(key=lambda x: x["properties"]["Event ID"])
        for event_id, data_iterator in itertools.groupby(gidd_data, key=lambda x: x["properties"].get("Event ID", " ")):
            gidd_data_items = list(data_iterator)
            self.transform_summary.increment_rows(len(gidd_data_items))

            try:
                def get_validated_data(items: list[dict]) -> List[GiddValidator]:
                    validated_data: list[GiddValidator] = []
                    for item in items:
                        obj = GiddValidator(**item)
                        validated_data.append(obj)
                    return validated_data
                validated_data = get_validated_data(gidd_data_items)

                if event_item := self.make_source_event_item(event_id=event_id, data_items=validated_data):
                    yield event_item
                    yield from self.make_impact_items(event_item, validated_data)
                else:
                    self.transform_summary.increment_failed_rows(len(gidd_data_items))
            except Exception:
                self.transform_summary.increment_failed_rows(len(gidd_data_items))
                logger.error("Failed to process the GIDD data", exc_info=True)
        self.transform_summary.mark_as_complete()

    # FIXME: This is deprecated
    def make_items(self) -> List[Item]:
        """Get the STAC items"""
        return list(self.get_stac_items())

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

    def make_source_event_item(self, event_id: int, data_items: List[GiddValidator]) -> Item | None:
        """Create an Event Item"""
        # Get the first item to create the STAC event item
        # as only one event item can create multiple impact items
        data_item = data_items[0]
        # Create the geojson point
        # FIXME: We might need to get this from aggregating the figures
        geometry = data_item.geometry
        coordinates = geometry.coordinates
        bbox = self.make_bbox(coordinates)

        # Episode number not in the source, so set it to 1
        episode_number = 1

        # FIXME: We might need to get this from aggregating the figures
        startdate_str = data_item.properties.Event_start_date.strftime("%Y-%m-%d")
        enddate_str = data_item.properties.Event_end_date.strftime("%Y-%m-%d")
        startdate = pytz.utc.localize(datetime.fromisoformat(startdate_str))
        enddate = pytz.utc.localize(datetime.fromisoformat(enddate_str))

        item = Item(
            id=f"{STAC_EVENT_ID_PREFIX}{data_item.properties.Event_ID}",
            geometry=dict(geometry),
            bbox=bbox,
            datetime=startdate,
            properties={
                "title": data_item.properties.Event_name,
                "start_datetime": startdate.isoformat(),
                "end_datetime": enddate.isoformat(),
                "location": data_item.properties.Locations_name,
                "location_accuracy": data_item.properties.Locations_accuracy,
                "location_type": data_item.properties.Locations_type,
                "displacement_occured": data_item.properties.Displacement_occurred,
                "sources": data_item.properties.Sources,
                "publishers": data_item.properties.Publishers,
            },
        )

        MontyExtension.add_to(item)
        monty = MontyExtension.ext(item)
        monty.episode_number = episode_number
        monty.country_codes = [data_item.properties.ISO3]

        if IDMCUtils.DisplacementType(
            data_item.properties.Figure_cause
        ) == IDMCUtils.DisplacementType.DISASTER_TYPE:
            hazard_tuple = (
                data_item.properties.Hazard_category,
                data_item.properties.Hazard_sub_category,
                data_item.properties.Hazard_type,
                data_item.properties.Hazard_sub_type,
            )
            monty.hazard_codes = IDMCUtils.hazard_codes_mapping(hazard=hazard_tuple)
        monty.compute_and_set_correlation_id(hazard_profiles=self.hazard_profiles)

        item.set_collection(self.get_event_collection())
        item.properties["roles"] = ["source", "event"]

        item.add_asset(
            "source",
            Asset(
                href=self.data_source.get_source_url(),
                media_type="application/geo+json",
                title="GIDD GeoJson Source",
                roles=["source"]
            ),
        )

        item.add_link(Link("via", self.data_source.get_source_url(), "application/json", "GIDD Event Data"))

        return item

    def make_impact_items(self, event_item: Item, data_items: List[GiddValidator]) -> List[Item]:
        """Create impact items"""
        items = []
        for data_item in data_items:
            impact_item = event_item.clone()

            actual_start_date = data_item.properties.Start_date or data_item.properties.Stock_date
            startdate_str = actual_start_date.strftime("%Y-%m-%d") if actual_start_date else None
            # FIXME: this should work
            startdate = pytz.utc.localize(datetime.fromisoformat(startdate_str)) if startdate_str else None

            actual_end_date = data_item.properties.End_date or data_item.properties.Stock_reporting_date
            enddate_str = actual_end_date.strftime("%Y-%m-%d") if actual_end_date else None
            enddate = pytz.utc.localize(datetime.fromisoformat(enddate_str)) if enddate_str else None

            if not startdate:
                raise Exception('Start date is not defined')

            impact_type = data_item.properties.Figure_category or "displaced"

            impact_item.id = (
                impact_item.id.replace(STAC_EVENT_ID_PREFIX, STAC_IMPACT_ID_PREFIX) +
                str(data_item.properties.ID) +
                "-" +
                impact_type
            )

            impact_item.datetime = startdate
            impact_item.properties["title"] = (
                f"{data_item.properties.Figure_category}-{data_item.properties.Figure_unit}-{data_item.properties.Figure_unit}-{data_item.properties.Event_name}"
            )
            impact_item.properties.update(
                {
                    # FIXME: Do we need to store if the figure is FLOW or STOCK
                    "start_datetime": startdate.isoformat() if startdate else None,
                    "end_datetime": enddate.isoformat() if enddate else None,
                    "figure_category": data_item.properties.Figure_category,
                    "figure_unit": data_item.properties.Figure_unit,
                    "figure_cause": data_item.properties.Figure_cause,
                    "geographical_region": data_item.properties.Geographical_region,
                    "country": data_item.properties.Country,
                    "roles": ["source", "impact"],
                }
            )

            impact_item.set_collection(self.get_impact_collection())
            monty = MontyExtension.ext(impact_item)
            monty.impact_detail = self.get_impact_details(data_item=data_item, impact_type=impact_type)
            items.append(impact_item)

        return items

    def get_impact_details(self, data_item: GiddValidator, impact_type: str) -> ImpactDetail:
        """Returns the impact details related to displacement"""
        category, category_type = IDMCUtils.mappings.get(
            impact_type, (MontyImpactExposureCategory.ALL_PEOPLE, MontyImpactType.INTERNALLY_DISPLACED_PERSONS)
        )
        return ImpactDetail(
            category=category,
            type=category_type,
            value=data_item.properties.Total_figures,
            unit="count",
            estimate_type=MontyEstimateType.PRIMARY,
        )

    def check_and_get_gidd_data(self) -> List[Dict[str, Any]]:
        """
        Validate the source fields

        Returns:
            List of validated GIDD data dictionaries
        """
        gidd_data: Dict[str, Any] = self.data_source.get_data()
        if not gidd_data:
            print(f"No gidd data found in {self.data_source.get_source_url()}")
            return []

        # FIXME: Only pass disaster
        data = gidd_data["features"]
        disaster_data = []
        for item in data:
            item_properties = item.get("properties", {})
            if (IDMCUtils.DisplacementType(item_properties.get("Figure cause")) == IDMCUtils.DisplacementType.DISASTER_TYPE):
                disaster_data.append(item)
        return disaster_data
